from icecream import ic
from retry import retry
from functools import reduce
import tiktoken
from shared.utils import create_logger
from langchain_groq import ChatGroq
from langchain_core.documents import Document
from langchain_core.output_parsers import StrOutputParser, JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langchain.chains.summarize import load_summarize_chain

encoding = tiktoken.get_encoding("cl100k_base")

def count_tokens(texts: list[str]) -> int:
    return reduce(lambda a,b: a+b, [len(enc) for enc in encoding.encode_batch(texts)])
   
def combine_texts(texts: list[str], batch_size: int, delimiter: str = "```") -> list[str]:
    if count_tokens(texts) > batch_size:
        half = len(texts) // 2
        return combine_texts(texts[:half], batch_size, delimiter) + combine_texts(texts[half:], batch_size, delimiter)
    else:
        return [delimiter.join(texts)]
 

################
## SUMMARIZER ##
################

import re

SUMMARIZER_MODEL = "llama3-8b-8192"
SUMMARIZER_BATCH_SIZE = 6144
MIN_SUMMARIZER_LEN = 1000

class Summarizer:
    def __init__(self, api_key):        
        llm = ChatGroq(api_key=api_key, model=SUMMARIZER_MODEL, temperature=0.1, verbose=False, streaming=False, max_tokens=384)
        self.chain = load_summarize_chain(llm=llm, chain_type="stuff", verbose=False)

    @retry(tries=5, jitter=5, delay=10, logger=create_logger("summarizer"))
    def summarize(self, text: str) -> str:
        res = self.chain.invoke({"input_documents": [Document(page_content=text)]}) if len(text) > MIN_SUMMARIZER_LEN else text
        #  the regex is a hack for llama3
        return re.sub(r'(?i)Here is a concise summary:', '', res['output_text']).strip()

######################
## NUGGET EXTRACTOR ##
######################

from itertools import chain
from pydantic import BaseModel, Field

NUGGETS_TEMPLATE = """You are provided with one or more excerpts from news article or social media posts delimitered by ```
For each input you will extract the main `message` represented by the excerpt.
Each message will contain a `keyphrase`, an `event` and a `description` field.
The final output MUST BE a list of `messages` represented by array of json objects representing the structure of each `message`.
OUTPUT FORMAT: {format_instruction}
```\n{input}\n```"""
NUGGETS_MODEL = "llama3-8b-8192"
NUGGETS_BATCH_SIZE = 5120
K_MESSAGES = "messages"

class NuggetData(BaseModel):
    keyphrase: str = Field(descrition="keyphrase can be the name of a company, product, person, place, security vulnerability, entity, location, organization, object, condition, acronym, documents, service, disease, medical condition, vehicle, polical group, ID etc.")
    event: str = Field(description="'event' can be action, state or condition associated to the 'keyphrase' such as: what is the 'keyphrase' doing OR what is happening to the 'keyphrase' OR how is 'keyphrase' being impacted.")
    description: str = Field(description="An event description is the concise summary of the 'event' associated to the 'keyphrase'")

class NuggetList(BaseModel):
    messages: list[NuggetData] = Field(description="list of key points and messages. Each message has keyphrase, event and description field")

class NuggetExtractor:
    def __init__(self, api_key):
        parser = JsonOutputParser(name = "nugget parser", pydantic_object=NuggetList)
        prompt = PromptTemplate.from_template(
            template=NUGGETS_TEMPLATE, 
            partial_variables={"format_instruction": parser.get_format_instructions()})
        llm = ChatGroq(api_key=api_key, model=NUGGETS_MODEL, temperature=0.1, verbose=False, streaming=False)
        self.chain = prompt | llm | parser
    
    def extract(self, texts: str|list[str]):
        texts = combine_texts([texts] if isinstance(texts, str) else texts, NUGGETS_BATCH_SIZE)        
        return list(chain(*[self._extract(t) for t in texts]))
    
    @retry(tries=5, jitter=5, delay=10, logger=create_logger("nuggetor"))
    def _extract(self, text: str):        
        res = self.chain.invoke({"input":text})
        return res[K_MESSAGES] if (K_MESSAGES in res) else res
