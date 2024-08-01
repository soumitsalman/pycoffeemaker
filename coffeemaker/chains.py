from icecream import ic
from retry import retry
from pybeansack import utils
from langchain_core.documents import Document
from langchain_core.output_parsers import JsonOutputParser, PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain.chains.summarize import load_summarize_chain
from typing import Optional
from itertools import chain
from pydantic import BaseModel, Field
import re

DIGEST_TEMPLATE="""TASK: Extract a title, top keyphrases and highlights from the following {kind}. 
OUTPUT FORMAT: {format_instruction}.
INPUT: {text}"""
class Digest(BaseModel):
    title: str = Field(description="title of the content")
    # summary: str = Field(description="summary of the content")
    keyphrases: list[str] = Field(descrition="A list of the main keyphrases mentioned here such as company, organization, group, entity, product, person, object, place, technology, stock ticker etc.")
    highlights: list[str] = Field(description="A list of the main one-liner highlights and key takeways from the content")

class DigestExtractor:
    def __init__(self, llm, context_len: int):
        parser = PydanticOutputParser(name = "digest parser", pydantic_object=Digest)
        prompt = PromptTemplate.from_template(
            template=DIGEST_TEMPLATE, 
            partial_variables={"format_instruction": parser.get_format_instructions()})
        self.chain = prompt | llm | parser
        self.context_len = context_len
    
    @retry(tries=2, jitter=5, delay=10, logger=utils.create_logger("digestor"))
    def run(self, kind: str, text: str) -> Digest:
        return self.chain.invoke({"kind": kind, "text": utils.truncate(text, self.context_len)})
        
    def __call__(self, kind: str, text: str) -> Digest:        
        return self.run(kind, text)
        

MIN_SUMMARIZER_LEN = 1000
class Summarizer:
    def __init__(self, llm, context_len: int):                
        self.chain = load_summarize_chain(llm=llm, chain_type="stuff", verbose=False)
        self.context_len = context_len

    @retry(tries=3, jitter=5, delay=10, logger=utils.create_logger("summarizer"))
    def run(self, text: str) -> str:
        if len(text) <= MIN_SUMMARIZER_LEN:
            return text
        res = self.chain.invoke({"input_documents": [Document(page_content=utils.truncate(text, self.context_len))]})
        #  the regex is a hack for llama3
        return re.sub(r'(?i)Here is a concise summary:', '', res['output_text']).strip()


NUGGETS_TEMPLATE = """You are provided with one or more excerpts from news article or social media posts delimitered by ```
For each input you will extract the main `message` represented by the excerpt.
Each message will contain a `keyphrase`, an `event` and a `description` field.
The final output MUST BE a list of `messages` represented by array of json objects representing the structure of each `message`.
OUTPUT FORMAT: {format_instruction}
```\n{input}\n```"""
# NUGGETS_MODEL = "llama3-8b-8192"
NUGGETS_BATCH_SIZE = 5120
K_MESSAGES = "messages"

class NuggetData(BaseModel):
    keyphrase: str = Field(descrition="keyphrase can be the name of a company, product, person, place, security vulnerability, entity, location, organization, object, condition, acronym, documents, service, disease, medical condition, vehicle, polical group, ID etc.")
    event: str = Field(description="'event' can be action, state or condition associated to the 'keyphrase' such as: what is the 'keyphrase' doing OR what is happening to the 'keyphrase' OR how is 'keyphrase' being impacted.")
    description: str = Field(description="An event description is the concise summary of the 'event' associated to the 'keyphrase'")

class NuggetList(BaseModel):
    messages: list[NuggetData] = Field(description="list of key points and messages. Each message has keyphrase, event and description field")

class NuggetExtractor:
    def __init__(self, llm):
        parser = JsonOutputParser(name = "nugget parser", pydantic_object=NuggetList)
        prompt = PromptTemplate.from_template(
            template=NUGGETS_TEMPLATE, 
            partial_variables={"format_instruction": parser.get_format_instructions()})
        self.chain = prompt | llm | parser
    
    def extract(self, texts: str|list[str]):
        texts = combine_texts([texts] if isinstance(texts, str) else texts, NUGGETS_BATCH_SIZE)        
        return list(chain(*[self._extract(t) for t in texts]))
    
    @retry(tries=5, jitter=5, delay=10, logger=utils.create_logger("nuggetor"))
    def _extract(self, text: str):        
        res = self.chain.invoke({"input":text})
        # this is some voodoo magic because LLM can return dumb shits
        if isinstance(res, dict):
            return res[K_MESSAGES] if (K_MESSAGES in res) else res
        elif isinstance(res, list):
            return list(chain(*(res_item[K_MESSAGES] for res_item in res))) if (K_MESSAGES in res[0]) else res


def combine_texts(texts: list[str], batch_size: int, delimiter: str = "```") -> list[str]:
    if utils.count_tokens(texts) > batch_size:
        half = len(texts) // 2
        return combine_texts(texts[:half], batch_size, delimiter) + combine_texts(texts[half:], batch_size, delimiter)
    else:
        return [delimiter.join(texts)]