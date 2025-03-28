from abc import ABC, abstractmethod
import json
import os
import threading
from typing import Optional
from icecream import ic
from retry import retry
from .utils import LLAMA_CPP_PREFIX, API_URL_PREFIX, truncate
from pydantic import BaseModel, Field
import logging

logger = logging.getLogger(__name__)

class Digest(BaseModel):
    title: Optional[str] = Field(description="title of the content", default=None)
    summary: Optional[str] = Field(description="A summary of the content", default=None)
    tags: Optional[list[str]] = Field(description="A list of tags that describe the content", default=None)

    def from_json_text(text: str):  
        try:      
            text = json.loads(text[text.find('{'):text.rfind('}')+1])
            return Digest(
                title=text.get('title'),
                summary=text.get('summary'),
                tags=[tag.strip() for tag in text.get('tags', '').split(',')] if isinstance(text.get('tags'), str) else text.get('tags')
            )
        except json.JSONDecodeError as e:
            ic(e, text)
            return None

# DIGESTOR_PROMPT = """<|begin_of_text|><|start_header_id|>system<|end_header_id|>response_format:json_object<|eot_id|>
# <|start_header_id|>user<|end_header_id|>
# TASK: generate summary, title, tags (e.g. company, organization, person, catastrophic event, product, technology, security vulnerability, stock ticker symbol, geographic location).
# INPUT:\n```\n{text}\n```
# OUTPUT FORMAT: A json object with fields title (string), summary (string) and tags (string of comma separated phrases)<|eot_id|>
# <|start_header_id|>assistant<|end_header_id|>"""
DIGESTOR_PROMPT = """<|im_start|>system\n\nresponse_format:json_object<|im_end|>

<|im_start|>user

TASK: generate summary, title, tags (such as company, organization, person). 
INPUT:\n```\n{text}\n```
OUTPUT FORMAT: A json object with fields title (string), summary (string) and tags (string of comma separated phrases)

<|im_end|>

<|im_start|>assistant\n\n"""

class Digestor(ABC):
    @abstractmethod
    def run(self, text: str) -> Digest:
        raise NotImplementedError("Subclass must implement abstract method")
    
    def run_batch(self, texts: list[str]) -> list[Digest]:
        return [self.run(text) for text in texts]

    def __call__(self, input: str|list[str]) -> Digest|list[Digest]:
        if isinstance(input, str): return self.run(input)
        else: return self.run_batch(input)

class LlamaCppDigestor(Digestor):
    model_path = None
    context_len = None
    model = None
    lock = None
    
    def __init__(self, model_path: str, context_len: int = 8192):
        self.lock = threading.Lock()
        self.model_path = model_path
        self.context_len = context_len

        from llama_cpp import Llama
        self.model = Llama(model_path=self.model_path, n_ctx=self.context_len, n_batch=self.context_len//2, n_threads_batch=os.cpu_count(), n_threads=os.cpu_count(), embedding=False, verbose=False)  
  
    def run(self, text: str) -> Digest:
        prompt = DIGESTOR_PROMPT.format(text=truncate(text, self.context_len//4))
        with self.lock:
            resp = self.model.create_completion(
                prompt=prompt,
                max_tokens=512, 
                frequency_penalty=0.3,
                temperature=0.2,
                seed=42
            )['choices'][0]['text']
        return Digest.from_json_text(resp)
    
class RemoteDigestor(Digestor):
    client = None
    model_name = None
    context_len = None

    def __init__(self, base_url: str, api_key: str, model_name: str, context_len: int = 8192):
        from openai import OpenAI

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
    lock = None

    def __init__(self, model_id, context_len=8192):
        self.lock = threading.Lock()
        self.context_len = context_len

        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer
        from unsloth import FastLanguageModel
        
        self.device = "cuda:0" if torch.cuda.is_available() else "cpu"
  
        if self.device == "cuda:0":
            self.model, self.tokenizer = FastLanguageModel.from_pretrained(
                model_name=model_id,
                max_seq_length=self.context_len,
                load_in_4bit=False,
                device_map=self.device
            )
            self.model = FastLanguageModel.for_inference(self.model)
        else:
            self.tokenizer = AutoTokenizer.from_pretrained(model_id, padding=True, truncation=True, max_length=context_len)
            self.model =  AutoModelForCausalLM.from_pretrained(model_id, device_map="auto", torch_dtype="auto")

    def run(self, text: str):
        prompt = DIGESTOR_PROMPT.format(text=truncate(text, self.context_len//4))

        with self.lock:
            inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
            outputs = self.model.generate(**inputs, max_new_tokens=384, do_sample=True)
            generated = self.tokenizer.decode(outputs[0])
            # strip out the response braces
        start_index = generated.find(_RESPONSE_START)
        end_index = generated.find(_RESPONSE_END, start_index)
        resp = generated[start_index+len(_RESPONSE_START):end_index]
        # take the json part
        return Digest.from_json_text(resp)
    
    def run_batch(self, texts: list[str]):
        prompts = [DIGESTOR_PROMPT.format(text=truncate(text, self.context_len//4)) for text in texts]
        digests = []

        DIGESTOR_BATCH_SIZE = 16
        for i in range(0, len(texts), DIGESTOR_BATCH_SIZE):
            with self.lock:
                inputs = self.tokenizer(prompts[i:i+DIGESTOR_BATCH_SIZE], return_tensors="pt", padding=True, truncation=True, max_length=self.context_len//2).to(self.device)
                outputs = self.model.generate(**inputs, max_new_tokens=384, do_sample=True)
                generated = self.tokenizer.batch_decode(outputs)
            
            for g in generated:
                # strip out the response braces
                start_index = g.find(_RESPONSE_START)
                end_index = g.find(_RESPONSE_END, start_index)
                resp = g[start_index+len(_RESPONSE_START):end_index]
                digests.append(Digest.from_json_text(resp))

        return digests


def from_path(llm_path) -> Digestor:
    # intialize embedder
    if llm_path.startswith(LLAMA_CPP_PREFIX):
        return LlamaCppDigestor(llm_path[len(LLAMA_CPP_PREFIX):], os.getenv("LLM_N_CTX", 16384))
    elif llm_path.startswith(API_URL_PREFIX):
        return RemoteDigestor(llm_path, os.getenv("API_KEY"), os.getenv("LLM_NAME"), os.getenv("LLM_N_CTX", 16384))
    else:
        return TransformerDigestor(llm_path)
  