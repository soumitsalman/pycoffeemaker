import json
import logging
import math
import os
import random
import string
from typing import Optional
from icecream import ic
from openai import OpenAI
from retry import retry
from pybeansack import utils
from llama_cpp import Llama
from itertools import chain
from pydantic import BaseModel, Field

class Digest(BaseModel):
    title: str = Field(description="title of the content")
    summary: str = Field(description="A summary of the content")
    tags: list[str] = Field(description="A list of tags that describe the content")

DIGESTOR_PROMPT = """TASK: Create a concise summary and title for the INPUT content. If the content is in 1st person, keep the narrative style as prose. In addition extract the names of the main entities such as: company, organization, person, catastrophic event, product, technology etc. as tags
title should be 10 - 20 words, summary should be 100 - 200 words and tags (entities) should a string of comma separated phrases
INPUT:
```{kind}
{text}
```
OUTPUT FORMAT: A json object with fields 'title', 'summary' and 'tags'"""
class LocalDigestor:
    model_path = None
    context_len = None
    model = None
    
    def __init__(self, model_path: str, context_len: int = 8192):
        self.model_path = model_path
        self.context_len = context_len

    @retry(tries=2, logger=logging.getLogger('local digestor'))
    def run(self, text: str) -> Digest:
        if not self.model:
            self.model = Llama(model_path=self.model_path, n_ctx=self.context_len, n_threads=os.cpu_count(), embedding=False, verbose=False)  
        # TODO: start taking kind as input
        resp = ic(self.model.create_completion(
            prompt=DIGESTOR_PROMPT.format(kind="news", text=utils.truncate(text, self.context_len//2)), 
            temperature=0.1, 
            max_tokens=496, 
            frequency_penalty=0.5)).choices[0].text
        resp = json.loads(ic(resp[resp.find('{'):resp.rfind('}')+1]))
        return Digest(
            title=resp['title'],
            summary=resp['summary'],
            tags=[tag.strip() for tag in resp['tags'].split(',')]
        )
    
    def __call__(self, text: str) -> str:
        return self.run(text)
    
class RemoteDigestor:
    client = None
    model_name = None
    context_len = None

    def __init__(self, base_url: str, api_key: str, model_name: str, context_len: int = 8192):
        self.client = OpenAI(api_key=api_key, base_url=base_url, timeout=10)
        self.model_name = model_name
        self.context_len = context_len
    
    @retry(tries=2, delay=5, logger=logging.getLogger("remote digestor"))
    def run(self, text: str) -> Digest:
        resp = self.client.completions.create(
            model=self.model_name,
            prompt=DIGESTOR_PROMPT.format(kind="news", text=utils.truncate(text, self.context_len//2)),
            temperature=0.1,
            max_tokens=496,
            frequency_penalty=0.5,
            stream=False,
            seed=42
        ).choices[0].text

        resp = json.loads(resp[resp.find('{'):resp.rfind('}')+1])
        return Digest(
            title=resp['title'],
            summary=resp['summary'],
            tags=[tag.strip() for tag in resp['tags'].split(',')]
        )
        
    def __call__(self, kind: str, text: str) -> Digest:        
        return self.run(kind, text)


# KEYPHRASE_TYPES = ["company", "organization", "political group", "person",  "geographic location", "product", "software", "technology", "vulnerability", "vehicle", "stock ticker symbol"]
# MIN_KEYPHRASE_LEN = 3
# class LocalKeyphraseExtractor:
#     model_path = None
#     cache_dir = None
#     model = None

#     def __init__(self, model_path: str="numind/NuNerZero_span", context_len: int=384, cache_dir=".models"):
#         self.model_path = model_path
#         self.cache_dir = cache_dir
#         self.context_len = context_len

#     def run(self, text: str) -> list[str]:
#         if not self.model:
#             self.model = GLiNER.from_pretrained(self.model_path, cache_dir=self.cache_dir)
#         chunks = utils.chunk(text, context_len=self.context_len)
#         chunks = random.sample(chunks, k=min(2, len(chunks))) # select any 3 random chunks
#         entities = chain(*(self.model.predict_entities(c, KEYPHRASE_TYPES) for c in chunks)) # this is an approximation assuming that there isn't much new after the first 1 pages
#         return sorted({entity["text"].lower():entity['text'] for entity in entities if len(entity["text"])>=MIN_KEYPHRASE_LEN}.values(), key=lambda x: len(x), reverse=True)
        
#     def __call__(self, text: str) -> list[str]:        
#         return self.run(text)

# class LocalTitleExtractor:
#     model_path = None
#     context_len = None
#     cache_dir = None
#     tokenizer = None
#     model = None

#     def __init__(self, model_path: str = "Ateeqq/news-title-generator", context_len: int = 512, cache_dir: str = ".models"):        
#         self.model_path = model_path
#         self.context_len = context_len
#         self.cache_dir = cache_dir

#     def run(self, text: str) -> dict:
#         if not self.model:
#             self.tokenizer = AutoTokenizer.from_pretrained("Ateeqq/news-title-generator", cache_dir=self.cache_dir)
#             self.model = AutoModelForSeq2SeqLM.from_pretrained("Ateeqq/news-title-generator", cache_dir=self.cache_dir)

#         return self.tokenizer.decode(
#             self.model.generate(
#                 self.tokenizer.encode(
#                     text, 
#                     return_tensors="pt")[:self.context_len-1])[0], 
#                 skip_special_tokens=True)
    
#     def __call__(self, text: str) -> dict:
#         return self.run(text)

# DIGEST_TEMPLATE="""TASK: Extract a one-liner highlight/tldr and extract the top five key phrases.
# OUTPUT FORMAT: {format_instruction}.
# ```{kind}
# {text}
# ```"""



        

# MIN_SUMMARIZER_LEN = 1000
# class RemoteSummarizer:
#     def __init__(self, llm, context_len: int):                
#         self.chain = load_summarize_chain(llm=llm, chain_type="stuff", verbose=False)
#         self.context_len = context_len

#     @retry(tries=3, jitter=5, delay=10, logger=utils.create_logger("summarizer"))
#     def run(self, text: str) -> str:
#         res = self.chain.invoke({"input_documents": [Document(page_content=utils.truncate(text, self.context_len))]})
#         #  the regex is a hack for llama3
#         return re.sub(r'(?i)Here(?:\s+\w+)*\s+is(?:\s+\w+)*\s+a(?:\s+\w+)*\s+concise(?:\s+\w+)*\s+summary(?:\s+\w+)*:', '', res['output_text']).strip()
    
# LOCAL_SUMMARIZER_TEMPLATE = """<|system|>The user text represents a news articles or social media posts. Your task is to create a concise summary of less than 300 words and a one-liner highlights/tldr. You MUST respond using JSON format with one field called 'summary' and one field called 'highlight'.<|end|><|user|>{text}<|end|><|assistant|>"""
# class LocalDigestor:
#     model_path = None
#     context_len = None
#     model = None

#     def __init__(self, model_path: str, context_len: int):        
#         self.model_path = model_path
#         self.context_len = context_len

#     @retry(tries=2, jitter=5, delay=5, logger=utils.create_logger("local digestor"))
#     def run(self, text: str) -> dict:
#         if not self.model:
#             self.model = Llama(model_path=self.model_path, n_ctx=4096, n_threads=max(1, os.cpu_count()-1), embedding=False, verbose=False)
#         result =self.model.create_completion(
#             prompt=LOCAL_SUMMARIZER_TEMPLATE.format(text=utils.truncate(text, self.context_len)), 
#             temperature=0.5, 
#             max_tokens=496)["choices"][0]['text']
#         return json.loads(ic(result[result.find("{"):result.rfind("}")+1]))
    
#     def __call__(self, text: str) -> dict:
#         return self.run(text)



# NUGGETS_TEMPLATE = """You are provided with one or more excerpts from news article or social media posts delimitered by ```
# For each input you will extract the main `message` represented by the excerpt.
# Each message will contain a `keyphrase`, an `event` and a `description` field.
# The final output MUST BE a list of `messages` represented by array of json objects representing the structure of each `message`.
# OUTPUT FORMAT: {format_instruction}
# ```\n{input}\n```"""
# # NUGGETS_MODEL = "llama3-8b-8192"
# NUGGETS_BATCH_SIZE = 5120
# K_MESSAGES = "messages"

# class NuggetData(BaseModel):
#     keyphrase: str = Field(descrition="keyphrase can be the name of a company, product, person, place, security vulnerability, entity, location, organization, object, condition, acronym, documents, service, disease, medical condition, vehicle, polical group, ID etc.")
#     event: str = Field(description="'event' can be action, state or condition associated to the 'keyphrase' such as: what is the 'keyphrase' doing OR what is happening to the 'keyphrase' OR how is 'keyphrase' being impacted.")
#     description: str = Field(description="An event description is the concise summary of the 'event' associated to the 'keyphrase'")

# class NuggetList(BaseModel):
#     messages: list[NuggetData] = Field(description="list of key points and messages. Each message has keyphrase, event and description field")

# class NuggetExtractor:
#     def __init__(self, llm):
#         parser = JsonOutputParser(name = "nugget parser", pydantic_object=NuggetList)
#         prompt = PromptTemplate.from_template(
#             template=NUGGETS_TEMPLATE, 
#             partial_variables={"format_instruction": parser.get_format_instructions()})
#         self.chain = prompt | llm | parser
    
#     def extract(self, texts: str|list[str]):
#         texts = combine_texts([texts] if isinstance(texts, str) else texts, NUGGETS_BATCH_SIZE)        
#         return list(chain(*[self._extract(t) for t in texts]))
    
#     @retry(tries=5, jitter=5, delay=10, logger=utils.create_logger("nuggetor"))
#     def _extract(self, text: str):        
#         res = self.chain.invoke({"input":text})
#         # this is some voodoo magic because LLM can return dumb shits
#         if isinstance(res, dict):
#             return res[K_MESSAGES] if (K_MESSAGES in res) else res
#         elif isinstance(res, list):
#             return list(chain(*(res_item[K_MESSAGES] for res_item in res))) if (K_MESSAGES in res[0]) else res


def combine_texts(texts: list[str], batch_size: int, delimiter: str = "```") -> list[str]:
    if utils.count_tokens(texts) > batch_size:
        half = len(texts) // 2
        return combine_texts(texts[:half], batch_size, delimiter) + combine_texts(texts[half:], batch_size, delimiter)
    else:
        return [delimiter.join(texts)]
    
def chunk_tokens(input: str, context_len: int, encode_fn) -> list[str]:
    tokens = encode_fn(input)
    num_chunks = math.ceil(len(tokens) / context_len)
    chunk_size = math.ceil(len(tokens) / num_chunks)
    return [tokens[start : start+chunk_size] for start in range(0, len(tokens), chunk_size)]
  