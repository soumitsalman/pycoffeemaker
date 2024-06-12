from icecream import ic
from retry import retry
from functools import reduce
import tiktoken

def count_tokens(texts: list[str]) -> int:
    return reduce(lambda a,b: a+b, [len(enc) for enc in encoding.encode_batch(texts)])

def truncate(input: str|list[str]) -> str|list[str]:
    if isinstance(input, str):
        return encoding.decode(encoding.encode(input)[:EMBEDDER_CTX])
    else:
        return [encoding.decode(enc[:EMBEDDER_CTX]) for enc in encoding.encode_batch(input)]
    
def combine_texts(texts: list[str], ctx: int, delimiter: str = "```") -> list[str]:
    if count_tokens(texts) > ctx:
        half = len(texts) // 2
        return combine_texts(texts[:half], ctx, delimiter) + combine_texts(texts[half:], ctx, delimiter)
    else:
        return [delimiter.join(texts)]
 
####################
## NOMIC EMBEDDER ##
####################

from llama_cpp import Llama
from langchain_core.embeddings import Embeddings

EMBEDDER_MODEL_PATH = "models/nomic.gguf"
EMBEDDER_CTX = 2047
SEARCH_DOCUMENT = "search_document"
SEARCH_QUERY = "search_query"

encoding = tiktoken.get_encoding("cl100k_base")

class LocalNomic(Embeddings):
    def __init__(self, model_path: str = EMBEDDER_MODEL_PATH):        
        self.model = Llama(model_path=model_path, n_ctx=EMBEDDER_CTX, embedding=True, verbose=False)
       
    def embed_documents(self, texts: list[str]):
        return self._embed(texts, SEARCH_DOCUMENT)
    
    def embed_query(self, text: str):    
        return self._embed(text, SEARCH_QUERY)

    def embed_queries(self, texts: list[str]):    
        return self._embed(texts, SEARCH_QUERY)
    
    @retry(tries=5)
    def _embed(self, input: str|list[str], task_type: str):
        texts = LocalNomic._prep_input(input, task_type)
        result = [self.model.create_embedding(text)['data'][0]['embedding'] for text in texts]
        if any(not res for res in result):
            raise Exception("None value returned by embedder")
        return result[0] if isinstance(input, str) else result 
    
    def _prep_input(input: str|list[str], task_type: str):
        texts = [input] if isinstance(input, str) else input
        return truncate([f"{task_type}: {t}" for t in texts])

################
## SUMMARIZER ##
################

import re
from langchain_groq import ChatGroq
from langchain.chains.summarize import load_summarize_chain
from langchain.schema import Document  

SUMMARIZER_MODEL = "llama3-8b-8192"
SUMMARIZER_CTX = 6144
MIN_SUMMARIZER_LEN = 1000
class Summarizer:
    def __init__(self, api_key):
        self.chain = load_summarize_chain(ChatGroq(api_key=api_key, model=SUMMARIZER_MODEL, temperature=0.1), chain_type="refine", verbose=False)

    @retry(tries=5, jitter=5, delay=10)
    def summarize(self, text: str) -> str:
        res = self.chain.invoke({"input_documents": [Document(text)]})['output_text'] if len(text) > MIN_SUMMARIZER_LEN else text
        #  the regex is a hack for llama3
        return re.sub(r'(?i)Here is a concise summary:', '', res).strip()

######################
## NUGGET EXTRACTOR ##
######################

from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from itertools import chain
from pydantic import BaseModel, Field
    
NUGGETS_MODEL = "llama3-8b-8192"
NUGGETS_CTX = 4096
NUGGETS_SYSTEM_MSG = """You are provided with one or more excerpts from news article or social media posts delimitered by ```
For each input you will extract the main `message` represented by the excerpt.
Each message will contain a `keyphrase`, an `event` and a `description` field.
The final output MUST BE a list of `messages` represented by array of json objects representing the structure of each `message`.
OUTPUT FORMAT: {format_instruction}"""
NUGGET_USER_MSG = "```\n{input}\n```"
RETRY_INSTRUCTION = "Format the INPUT content in JSON format"

K_MESSAGES = "messages"

class NuggetData(BaseModel):
    keyphrase: str = Field(descrition="keyphrase can be the name of a company, product, person, place, security vulnerability, entity, location, organization, object, condition, acronym, documents, service, disease, medical condition, vehicle, polical group, ID etc.")
    event: str = Field(description="'event' can be action, state or condition associated to the 'keyphrase' such as: what is the 'keyphrase' doing OR what is happening to the 'keyphrase' OR how is 'keyphrase' being impacted.")
    description: str = Field(description="An event description is the concise summary of the 'event' associated to the 'keyphrase'")

class NuggetList(BaseModel):
    messages: list[NuggetData] = Field(description="list of messages in the excerpts. Each message has keyphrase, event and description field")

class NuggetExtractor:
    def __init__(self, api_key):
        parser = JsonOutputParser(name = "nugget parser", pydantic_object=NuggetList)
        prompt = PromptTemplate(template=NUGGETS_SYSTEM_MSG+"\n"+NUGGET_USER_MSG, input_variables=["input"], partial_variables={"format_instruction": parser.get_format_instructions()})
        llm = ChatGroq(api_key=api_key, model=NUGGETS_MODEL, temperature=0.1)
        self.chain = prompt | llm | parser
    
    def extract(self, texts: str|list[str]):
        texts = combine_texts([texts] if isinstance(texts, str) else texts, NUGGETS_CTX)        
        return list(chain(*[self._extract(t) for t in texts]))
    
    @retry(tries=5, jitter=5, delay=10)
    def _extract(self, text: str):        
        res = self.chain.invoke({"input":text})
        return res[K_MESSAGES] if (K_MESSAGES in res) else res
