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

CONTEXT_LEN = 8192
NUM_THREADS = os.cpu_count()
DIGESTOR_BATCH_SIZE = os.getenv("DIGESTOR_BATCH_SIZE", 16)
MIN_WORDS_THRESHOLD_FOR_SUMMARY = 200 # min words needed to use the generated summary

SUMMARY_TEMPLATE = """TASK: Rewrite the article/post text using less than 250 words.

ARTICLE/POST:
{input_text}
"""

EXTRACTION_TEMPLATE = """TASKS:
    - Create a one sentence gist of the article/post. This will be the 'title'.
    - Extract names of the top 1 - 4 people, products, companies, organizations, or stock tickers mentioned in the article/post that influence the content. These will be the 'names'.
    - Identify 1 or 2 domains that the subject matter of the article/post aligns closest to, such as: Cybersecurity, Business & Finance, Health & Wellness, Astrophysics, Smart Automotive, IoT and Gadgets, etc. These will be the 'domains'.

RESPONSE FORMAT: 
Response MUST be a json object of the following structure
```json
{{
    "title": string,
    "names": [string, string, string, string],
    "domains": [string, string]
}}
```

ARTICLE/POST:
{input_text}
"""

class Digest(BaseModel):
    summary: Optional[str] = Field(description="Rewrite the article/post using less that 250 words.", default=None)
    # highlights: Optional[str|list[str]] = Field(description="A list of sentences that are most relevant to the main points and core meaning of the entire content", default=None)
    title: Optional[str] = Field(description="Create a one sentence gist of article/post.", default=None)
    names: Optional[list[str]] = Field(description="Extract names of the top 1 - 4 people, product, company, organization or stock ticker mentioned in the article/post that influences the content.", default=None)
    domains: Optional[list[str]] = Field(description="Identify 1 or 2 domains that the subject matter of the article/post aligns closest to, such as: Cybersecurity, Business & Finance, Health & Wellness, Astrophysics, Smart Automotive, IoT and Gadgets etc.", default=None)


create_prompt = lambda input_text, template, max_tokens: [
    {
        "role": "system",
        "content": "response_format:json_object"
    },
    {
        "role": "user",
        "content":  template.format(input_text=truncate(input_text, max_tokens))
    }
]

needs_summary = lambda text: text and len(text.split()) >= MIN_WORDS_THRESHOLD_FOR_SUMMARY # if the body is large enough
parse_list = lambda field: [item.strip() for item in field.split(',')] if isinstance(field, str) else field
unique_items = lambda items: list({item.strip().lower(): item for item in items}.values()) if items else items

def parse_digest(response: str):  
    try:      
        response = json.loads(response[response.find('{'):response.rfind('}')+1])
        return Digest(
            summary=response.get('summary'),
            highlights=response.get('highlights'),
            title=response.get('title'),
            names=unique_items(parse_list(response.get('names'))),
            domains=unique_items(parse_list(response.get('domains'))),
        )
    except json.JSONDecodeError as e:
        ic(e, response)
        return None

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
    
    def __init__(self, model_path: str, context_len: int = int(os.getenv("LLM_N_CTX", CONTEXT_LEN))):
        self.lock = threading.Lock()
        self.model_path = model_path
        self.context_len = context_len

        from llama_cpp import Llama
        self.model = Llama(
            model_path=self.model_path, n_ctx=self.context_len, 
            n_batch=self.context_len//2, n_threads_batch=NUM_THREADS, n_threads=NUM_THREADS, 
            embedding=False, verbose=False
        )  

    def _generate_response(self, input_text: str, template: str, max_new_tokens: int = 256, response_format = None) -> str:
        return self.model.create_chat_completion(
            messages=create_prompt(input_text=input_text, template=template, max_tokens=self.context_len//2),
            max_tokens=max_new_tokens,
            seed=666,
            # response_format={"type": response_format} if response_format else None,
            # temperature=0.1, # if response_format=="json_object" else 0.3,
            # frequency_penalty=0.2,
            repeat_penalty=1 if response_format=="json_object" else 1.5
        )['choices'][0]['message']['content'].strip()          
  
    def run(self, text: str) -> Digest:
        with self.lock:
            resp = self._generate_response(input_text=text, template=EXTRACTION_TEMPLATE, max_new_tokens=192, response_format="json_object")
            digest = parse_digest(resp)
            if digest and needs_summary(text):
                summary = self._generate_response(input_text=text, template=SUMMARY_TEMPLATE, max_new_tokens=400)
                if summary and (summary[0].isalnum() or summary[0] in ['-', '*']): 
                    digest.summary = summary
        return digest

    
class RemoteDigestor(Digestor):
    client = None
    model_name = None
    context_len = None

    def __init__(self, base_url: str, api_key: str, model_name: str, context_len: int = int(os.getenv("LLM_N_CTX", CONTEXT_LEN))):
        from openai import OpenAI

        self.client = OpenAI(api_key=api_key, base_url=base_url, timeout=5, max_retries=2)
        self.model_name = model_name
        self.context_len = context_len
    
    @retry(tries=2, delay=5, logger=logger)
    def run(self, text: str) -> Digest:
        resp = self.client.chat.completions.create(
            messages=create_prompt(truncate(text, self.context_len//2)),
            max_tokens=512, 
            frequency_penalty=0.3,
            temperature=0.7,
            seed=22,
            response_format={"type": "json_object"}
        ).choices[0].message.content
        return parse_digest(resp)
    
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
            names=[] # leaving this empty intentionally because nlp.keywords() is dumb
        )
    
_RESPONSE_START = "<|im_start|>assistant\n\n"
_RESPONSE_END = "<|im_end|>"
class TransformerDigestor(Digestor):
    model = None
    tokenizer = None
    device = None
    context_len = None
    lock = None

    def __init__(self, model_id, context_len=int(os.getenv("LLM_N_CTX", CONTEXT_LEN))):
        self.lock = threading.Lock()
        self.context_len = context_len

        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer
        from unsloth import FastLanguageModel
        
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
  
        if self.device == "cuda":
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

    def _generate_response(self, text: str, template: str, max_new_tokens: int = 256) -> str:
        prompt = template.format(input_text=truncate(text, self.context_len//2))
        inputs = self.tokenizer.apply_chat_template(prompt, tokenize=True, add_generation_prompt=True, truncation=False, return_tensors="pt").to(self.device)
        outputs = self.model.generate(**inputs, max_new_tokens=max_new_tokens, do_sample=True)
        generated = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        start_index = generated.find(_RESPONSE_START)
        end_index = generated.find(_RESPONSE_END, start_index)
        return generated[start_index + len(_RESPONSE_START):end_index].strip()

    def run(self, text: str) -> Digest:
        with self.lock:
            # prompt = EXTRACTION_TEMPLATE.format(input_text=truncate(text, self.context_len//4))
            # inputs = self.tokenizer(prompt, return_tensors="pt", truncation=True, max_length=self.context_len//2).to(self.device)
            # outputs = self.model.generate(**inputs, max_new_tokens=384, do_sample=True)
            # generated = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
            # start_index = generated.find(_RESPONSE_START)
            # end_index = generated.find(_RESPONSE_END, start_index)
            # resp = generated[start_index + len(_RESPONSE_START):end_index]
            resp = self._generate_response(text, EXTRACTION_TEMPLATE, max_new_tokens=192)
            digest = parse_digest(resp) or Digest()
            if needs_summary(text):
                # summary_prompt = SUMMARY_TEMPLATE.format(input_text=truncate(text, self.context_len//4))
                # inputs = self.tokenizer(summary_prompt, return_tensors="pt", truncation=True, max_length=self.context_len//2).to(self.device)
                # outputs = self.model.generate(**inputs, max_new_tokens=464, do_sample=True)
                # summary_generated = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
                digest.summary = self._generate_response(text, SUMMARY_TEMPLATE, max_new_tokens=384)
        return digest

    # def run_batch(self, texts: list[str]) -> list[Digest]:
        
    #     digests = []        
    #     for i in range(0, len(prompts), DIGESTOR_BATCH_SIZE):
    #         with self.lock:
    #             inputs = self.tokenizer(prompts[i:i + DIGESTOR_BATCH_SIZE], return_tensors="pt", padding=True, truncation=True, max_length=self.context_len//2).to(self.device)
    #             outputs = self.model.generate(**inputs, max_new_tokens=384, do_sample=True)
    #             generated = self.tokenizer.batch_decode(outputs, skip_special_tokens=True)
    #         for g, text in zip(generated, texts[i:i + DIGESTOR_BATCH_SIZE]):
    #             start_index = g.find(_RESPONSE_START)
    #             end_index = g.find(_RESPONSE_END, start_index)
    #             resp = g[start_index + len(_RESPONSE_START):end_index]
    #             digest = parse_digest(resp) or Digest()
    #             if needs_summary(text):
    #                 summary_prompt = SUMMARY_TEMPLATE.format(input_text=truncate(text, self.context_len//4))
    #                 inputs = self.tokenizer(summary_prompt, return_tensors="pt", truncation=True, max_length=self.context_len//2).to(self.device)
    #                 outputs = self.model.generate(**inputs, max_new_tokens=464, do_sample=True)
    #                 summary_generated = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
    #                 digest.summary = summary_generated
    #             digests.append(digest)
    #     return digests


def from_path(llm_path) -> Digestor:
    # intialize embedder
    if llm_path.startswith(LLAMA_CPP_PREFIX):
        return LlamaCppDigestor(llm_path[len(LLAMA_CPP_PREFIX):])
    elif llm_path.startswith(API_URL_PREFIX):
        return RemoteDigestor(llm_path, os.getenv("API_KEY"), os.getenv("LLM_NAME"))
    else:
        return TransformerDigestor(llm_path)
  