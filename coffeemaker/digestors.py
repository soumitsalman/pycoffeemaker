import json
import logging
import math
import os
from icecream import ic
from openai import OpenAI
from retry import retry
from pybeansack import utils
from llama_cpp import Llama
from pydantic import BaseModel, Field

class Digest(BaseModel):
    title: str = Field(description="title of the content")
    summary: str = Field(description="A summary of the content")
    tags: list[str] = Field(description="A list of tags that describe the content")

DIGESTOR_PROMPT = """<|begin_of_text|><|start_header_id|>system<|end_header_id|>\nresponse_format:json_object\n<|eot_id|>
<|start_header_id|>user<|end_header_id|>
TASK: generate summary, title, tags (e.g. company, organization, person, catastrophic event, product, technology, security vulnerability, stock ticker symbol, geographic location).
INPUT:\n```\n{text}\n```
OUTPUT FORMAT: A json object with fields title (string), summary (string) and tags (string of comma separated phrases)<|eot_id|>
<|start_header_id|>assistant<|end_header_id|>"""
    
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
        resp = self.model.create_completion(
            prompt=DIGESTOR_PROMPT.format(text=utils.truncate(text, self.context_len//2)),
            max_tokens=384, 
            frequency_penalty=0.3,
            temperature=0.2,
            seed=42
        )['choices'][0]['text']
        resp = json.loads(resp[resp.find('{'):resp.rfind('}')+1])
        return Digest(
            title=resp['title'],
            summary=resp['summary'],
            tags=[tag.strip() for tag in resp['tags'].split(',')] if isinstance(resp['tags'], str) else resp['tags']
        )
    
    def __call__(self, text: str) -> str:
        return self.run(text)
    
class RemoteDigestor:
    client = None
    model_name = None
    context_len = None

    def __init__(self, base_url: str, api_key: str, model_name: str, context_len: int = 8192):
        self.client = OpenAI(api_key=api_key, base_url=base_url, timeout=5, max_retries=2)
        self.model_name = model_name
        self.context_len = context_len
    
    @retry(tries=2, delay=5, logger=logging.getLogger("remote digestor"))
    def run(self, text: str) -> Digest:
        resp = self.client.completions.create(
            model=self.model_name,
            prompt=DIGESTOR_PROMPT.format(text=utils.truncate(text, self.context_len//2)),
            temperature=0.2,
            max_tokens=384,
            frequency_penalty=0.3
        ).choices[0].text

        resp = ic(json.loads(resp[resp.find('{'):resp.rfind('}')+1]))
        return Digest(
            title=resp['title'],
            summary=resp['summary'],
            tags=[tag.strip() for tag in resp['tags'].split(',')]
        )
        
    def __call__(self, kind: str, text: str) -> Digest:        
        return self.run(kind, text)

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
  