from abc import ABC, abstractmethod
import json
import os
from typing import Optional
from retry import retry
from .utils import LLAMA_CPP_PREFIX, API_URL_PREFIX, truncate
from pydantic import BaseModel, Field
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from llama_cpp import Llama
from openai import OpenAI
import logging

logger = logging.getLogger(__name__)

class Digest(BaseModel):
    title: Optional[str] = Field(description="title of the content", default=None)
    summary: Optional[str] = Field(description="A summary of the content", default=None)
    tags: Optional[list[str]] = Field(description="A list of tags that describe the content", default=None)

    def from_json_text(text: str):        
        text = json.loads(text[text.find('{'):text.rfind('}')+1])
        return Digest(
            title=text.get('title'),
            summary=text.get('summary'),
            tags=[tag.strip() for tag in text.get('tags', '').split(',')] if isinstance(text.get('tags'), str) else text.get('tags')
        )

# DIGESTOR_PROMPT = """<|begin_of_text|><|start_header_id|>system<|end_header_id|>response_format:json_object<|eot_id|>
# <|start_header_id|>user<|end_header_id|>
# TASK: generate summary, title, tags (e.g. company, organization, person, catastrophic event, product, technology, security vulnerability, stock ticker symbol, geographic location).
# INPUT:\n```\n{text}\n```
# OUTPUT FORMAT: A json object with fields title (string), summary (string) and tags (string of comma separated phrases)<|eot_id|>
# <|start_header_id|>assistant<|end_header_id|>"""
DIGESTOR_PROMPT = """<|im_start|>system\n\nresponse_format:json_object | language:en_US<|im_end|>

<|im_start|>user

TASK: translate the content into english and generate summary, title, tags (such as company, organization, person). 
INPUT:\n```\n{text}\n```
OUTPUT FORMAT: A json object with fields title (string), summary (string) and tags (string of comma separated phrases)

<|im_end|>

<|im_start|>assistant\n\n"""

class Digestor(ABC):
    @abstractmethod
    def run(self, text: str) -> Digest:
        raise NotImplementedError("Subclass must implement abstract method")

    def __call__(self, text: str) -> Digest:
        return self.run(text)

class LlamaCppDigestor(Digestor):
    model_path = None
    context_len = None
    model = None
    
    def __init__(self, model_path: str, context_len: int = 16384):
        self.model_path = model_path
        self.context_len = context_len
        self.model = Llama(model_path=self.model_path, n_ctx=self.context_len, n_gpu_layers=-1, n_threads=os.cpu_count(), embedding=False, verbose=False)  

    # @retry(tries=2, logger=logger)
    def run(self, text: str) -> Digest:
        resp = self.model.create_completion(
            prompt=DIGESTOR_PROMPT.format(text=truncate(text, self.context_len//2)),
            max_tokens=384, 
            frequency_penalty=0.3,
            temperature=0.3,
            seed=42
        )['choices'][0]['text']
        return Digest.from_json_text(resp)
    
    def __call__(self, text: str) -> str:
        return self.run(text)
    
class RemoteDigestor(Digestor):
    client = None
    model_name = None
    context_len = None

    def __init__(self, base_url: str, api_key: str, model_name: str, context_len: int = 8192):
        self.client = OpenAI(api_key=api_key, base_url=base_url, timeout=5, max_retries=2)
        self.model_name = model_name
        self.context_len = context_len
    
    @retry(tries=2, delay=5, logger=logger)
    def run(self, text: str) -> Digest:
        resp = self.client.completions.create(
            model=self.model_name,
            prompt=DIGESTOR_PROMPT.format(text=truncate(text, self.context_len//2)),
            temperature=0,
            max_tokens=384,
            frequency_penalty=0.3
        ).choices[0].text
        return Digest.from_json_text(resp)
        
    def __call__(self, kind: str, text: str) -> Digest:        
        return self.run(kind, text)
    
class NewspaperDigestor:       
    def __init__(self, language: str = "en"):
        from newspaper import nlp
        nlp.load_stopwords(language)
        
    def run(self, text: str, title: str) -> Digest:   
        from newspaper import nlp

        summary_lines = [' '.join(line.strip().split("\n")) for line in nlp.summarize(title=title, text=text)]
        return Digest(
            title=title,
            summary=' '.join(summary_lines),
            tags=[] # leaving this empty intentionally because nlp.keywords() is dumb
        )
    
_RESPONSE_START = "<|im_start|>assistant\n\n"
_RESPONSE_END = "<|im_end|>"
class TransformerDigestor(Digestor):
    model = None
    tokenizer = None
    device = None
    context_len = None

    def __init__(self, model_id, context_len=16384):
        self.context_len = context_len
        self.device = "cuda" if torch.cuda.is_available() else "cpu"        
        self.tokenizer = AutoTokenizer.from_pretrained(model_id, padding=True, truncation=True, max_length=context_len)
        self.model = AutoModelForCausalLM.from_pretrained(model_id, trust_remote_code=True).to(self.device)
        # if self.device == "cuda":
        #     from unsloth import FastLanguageModel

        #     model, tokenizer = FastLanguageModel.from_pretrained(
        #         model_name=model_id,
        #         max_seq_length=self.context_len,
        #         dtype=None,
        #         load_in_4bit=True
        #     )
        #     self.model = model
        #     self.tokenizer = tokenizer
        #     FastLanguageModel.for_inference(self.model)
        # else:
        #     from transformers import AutoModelForCausalLM, AutoTokenizer

        #     self.tokenizer = AutoTokenizer.from_pretrained(model_id, padding=True, truncation=True, max_length=context_len)
        #     self.model = AutoModelForCausalLM.from_pretrained(model_id, trust_remote_code=True).to(self.device)

    def run(self, text: str):
        inputs = self.tokenizer(DIGESTOR_PROMPT.format(text=truncate(text, self.context_len//2)), return_tensors="pt").to(self.device)
        outputs = self.model.generate(**inputs, max_new_tokens=384)
        generated = self.tokenizer.decode(outputs[0])
        # strip out the response braces
        start_index = generated.find(_RESPONSE_START)
        end_index = generated.find(_RESPONSE_END, start_index)
        resp = generated[start_index+len(_RESPONSE_START):end_index]
        # take the json part
        return Digest.from_json_text(resp)

def from_path(llm_path) -> Digestor:
    # intialize embedder
    if LLAMA_CPP_PREFIX in llm_path:
        return LlamaCppDigestor(llm_path[len(LLAMA_CPP_PREFIX):], os.getenv("LLM_N_CTX", 16384))
    elif API_URL_PREFIX in llm_path:
        return RemoteDigestor(llm_path, os.getenv("API_KEY"), os.getenv("LLM_NAME"), os.getenv("LLM_N_CTX", 16384))
    else:
        return TransformerDigestor(llm_path)
  