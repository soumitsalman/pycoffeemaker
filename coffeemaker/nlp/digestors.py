from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
import json
import os
import re
import threading
from typing import Callable, Optional
from icecream import ic
from retry import retry
from .prompts import *
from .utils import *
from pydantic import BaseModel, Field
from openai import OpenAI
import logging

logger = logging.getLogger(__name__)

CONTEXT_LEN = 8192
BATCH_SIZE = int(os.getenv("DIGESTOR_BATCH_SIZE", os.cpu_count()))
NUM_THREADS = int(os.getenv("DIGESTOR_NUM_THREADS", os.cpu_count()))


class Digest(BaseModel):
    expr: str
    keypoints: Optional[list[str]] = Field(default=None)
    keyevents: Optional[list[str]] = Field(default=None)
    datapoints: Optional[list[str]] = Field(default=None)
    categories: Optional[list[str]] = Field(default=None)
    entities: Optional[list[str]] = Field(default=None)
    regions: Optional[list[str]] = Field(default=None)
    sentiments: Optional[list[str]] = Field(default=None)

    gist: Optional[str] = Field(default=None)
    topic: Optional[str] = Field(default=None)
    summary: Optional[str] = Field(default=None)
    insight: Optional[str] = Field(default=None)    

class Digestor(ABC):
    @abstractmethod
    def run(self, text: str) -> Digest:
        raise NotImplementedError("Subclass must implement abstract method")
    
    def run_batch(self, texts: list[str]) -> list[Digest]:
        return [self.run(text) for text in texts]

    def __call__(self, input: str|list[str]) -> Digest|list[Digest]:
        if isinstance(input, str): return self.run(input)
        else: return self.run_batch(input)



def parse_json(response: str):  
    try:      
        response = json.loads(response[response.find('{'):response.rfind('}')+1])
        return Digest(
            summary=response.get('summary'),
            highlights=response.get('keypoints'),
            title=response.get('gist'),
            names=distinct_items(split_parts(response.get('entities'))),
            domains=distinct_items(split_parts(response.get('categories'))),
        )
    except json.JSONDecodeError as e:
        ic(e, response)
        return None
    
M_GIST = "# GIST"
M_CATEGORIES = "# DOMAINS"
M_ENTITIES = "# ENTITIES"
M_TOPIC = "# TOPIC"
M_REGIONS = "# REGIONS"
M_SUMMARY = "# SUMMARY"
M_KEYPOINTS = "# KEY POINTS"
M_KEYEVENTS = "# KEY EVENTS"
M_DATAPOINTS = "# KEY POINTS"
M_INSIGHT = "# ACTIONABLE INSIGHT"
M_FIELDS = [M_GIST, M_CATEGORIES, M_ENTITIES, M_TOPIC, M_REGIONS, M_SUMMARY, M_KEYPOINTS, M_KEYEVENTS, M_DATAPOINTS, M_INSIGHT]
M_START = "```markdown"
M_END="```"
UNDETERMINED = "N/A"
def parse_markdown(response: str):
    digest = Digest()
    response = response.strip().removeprefix(M_START).removesuffix(M_END).strip()
    last = None
    for line in response.splitlines():
        line = line.strip()
        if not line or line == UNDETERMINED: continue

        if any(field in line for field in M_FIELDS):
            last = line
        elif M_GIST in last:
            digest.gist = line
        elif M_CATEGORIES in last:
            digest.categories = split_parts(line)
        elif C_ENTITIES in last:
            digest.entities = split_parts(line)
        elif M_TOPIC in last:
            digest.topic = line 
        elif C_REGIONS in last:
            digest.regions = split_parts(line)
        elif M_SUMMARY in last:
            digest.summary = (digest.summary+"\n"+line) if digest.summary else line
        elif C_KEYPOINTS in last:
            if not digest.keypoints: digest.keypoints = []
            digest.keypoints.append(line.removeprefix("- ").removeprefix("* "))
        elif M_INSIGHT in last:
            digest.insight = line

    return digest   

C_KEYPOINTS = "P:"
C_KEYEVENTS = "E:"
C_DATAPOINTS = "D:"
C_REGIONS = "R:"
C_ENTITIES = "N:"
C_CATEGORIES = "C:"
C_SENTIMENTS = "S:"
COMPRESSED_FIELDS = [C_KEYPOINTS, C_KEYEVENTS, C_DATAPOINTS, C_REGIONS, C_ENTITIES, C_CATEGORIES, C_SENTIMENTS]

def parse_compressed_digest(response: str) -> Digest:
    response = response.strip()
    if not response: return
    
    digest = Digest(expr = "")
    parts = [part for part in split_parts(response, r'[;\|\n]+') if part != UNDETERMINED]
    last = None
    for part in parts:
        prefix = next((field for field in COMPRESSED_FIELDS if part.startswith(field)), None)

        if prefix:
            part = part.removeprefix(prefix)
            last = prefix
            if part == UNDETERMINED: continue # skip
            digest.expr += f";{part}" if digest.expr else part
        else:
            digest.expr += f"|{part}"

        if last == C_REGIONS:
            if not digest.regions: digest.regions = []
            digest.regions.append(part)
        if last == C_ENTITIES:
            if not digest.entities: digest.entities = []
            digest.entities.append(part)
        if last == C_CATEGORIES:
            if not digest.categories: digest.categories = []
            digest.categories.append(part)
        if last == C_SENTIMENTS:
            if not digest.sentiments: digest.sentiments = []
            digest.sentiments.append(part)

    return digest

def create_prompt_for_tuned_model(input_text: str, use_short_digest: bool): 
    template = TUNED_MODEL_DIGEST_INST if use_short_digest else TUNED_MODEL_DIGEST_INST
    return [
        {
            "role": "user",
            "content":  template.format(input_text=input_text)
        }
    ]

class LlamaCppDigestor(Digestor):
    model_path = None
    context_len = None
    model = None
    lock = None
    use_short_digest: Callable
    
    def __init__(self, model_path: str, context_len: int, use_short_digest: Callable = None):
        self.lock = threading.Lock()
        self.model_path = model_path
        self.context_len = context_len
        self.use_short_digest = use_short_digest or (lambda text: False)

        from llama_cpp import Llama
        self.model = Llama(
            model_path=self.model_path, n_ctx=self.context_len+(self.context_len//4), # this extension is needed to accommodate occasional overflows
            n_batch=self.context_len//2, n_threads_batch=NUM_THREADS, n_threads=NUM_THREADS, 
            embedding=False, verbose=False
        )  

    def _run(self, text: str) -> str:
        max_tokens = 256 if self.use_short_digest(text) else 512

        return self.model.create_chat_completion(
            messages=create_prompt_for_tuned_model(input_text=text),
            max_tokens=max_tokens,
            temperature=0.3,
            seed=666
        )['choices'][0]['message']['content'].strip()              
  
    def run(self, text: str) -> Digest:
        with self.lock:
            resp = self._run(truncate(text, self.context_len//2))
            digest = parse_markdown(resp)  
        return digest
    
    def run_batch(self, texts: list[str]) -> list[Digest]:
        texts = batch_truncate(texts, self.context_len//2)
        with self.lock:
            results = [self._run(text) for text in texts]
        return batch_run(parse_markdown, results)
    
_RESPONSE_START = "<|im_start|>assistant\n"
_RESPONSE_END = "<|im_end|>"
class TransformerDigestor(Digestor):
    model = None
    tokenizer = None
    device = None
    context_len = None
    lock = None
    use_short_digest = None

    def __init__(self, 
        model_id: str, 
        context_len: int,
        use_short_digest: Callable = None
    ):
        self.lock = threading.Lock()
        self.context_len = context_len
        self.use_short_digest = use_short_digest or (lambda text: False)

        from transformers import AutoModelForCausalLM, AutoTokenizer
        import torch

        self.device = "cuda" if torch.cuda.is_available() else "cpu"

        self.tokenizer = AutoTokenizer.from_pretrained(model_id, padding=True, truncation=True, max_length=context_len)
        self.model =  AutoModelForCausalLM.from_pretrained(model_id, device_map="auto", torch_dtype="auto")
        
    def _extract_response(self, generated: str) -> Digest:
        generated = remove_before(generated, _RESPONSE_START)
        generated = remove_after(generated, _RESPONSE_END)
        return parse_markdown(generated.strip())

    def run(self, text: str) -> Digest:
        max_tokens = 256 if self.use_short_digest(text) else 512
        prompt = create_prompt_for_tuned_model(truncate(text, self.context_len//2))

        with self.lock:            
            inputs = self.tokenizer.apply_chat_template(prompt, tokenize=True, add_generation_prompt=True, return_tensors="pt").to(self.device)
            outputs = self.model.generate(**inputs, max_new_tokens=max_tokens)
            generated = self.tokenizer.decode(outputs[0], skip_special_tokens=False)

        return self._extract_response(generated)

    def run_batch(self, texts: list[str]) -> list[Digest]:
        max_tokens = 512
        prompts = batch_run(create_prompt_for_tuned_model, batch_truncate(texts, self.context_len//2))

        generated = [] 
        with self.lock:
            for i in range(0, len(texts), BATCH_SIZE):    
                inputs = self.tokenizer.apply_chat_template(prompts, tokenize=True, add_generation_prompt=True, padding=True, truncation=True, max_length=(self.context_len*3)//4, return_dict=True, return_tensors="pt").to(self.device)
                outputs = self.model.generate(**inputs, max_new_tokens=max_tokens, num_beams=BATCH_SIZE, top_k=25)
                generated.extend(
                    self.tokenizer.batch_decode(outputs, skip_special_tokens=False)
                )
       
        return batch_run(self._extract_response, generated)

create_prompt_for_generic_model = lambda text: [
    {
        "role": "user",
        "content":  GENERIC_MODEL_DIGEST_INST.format(input_text=text)
    }
]

class RemoteDigestor(Digestor):
    client = None
    model_name = None
    context_len = None
    use_short_digest = None

    def __init__(self, 
        model_name: str,
        base_url: str, 
        api_key: str, 
        context_len: int,
        use_short_digest: Callable
    ):
        self.client = OpenAI(api_key=api_key, base_url=base_url, timeout=30, max_retries=2)
        self.model_name = model_name
        self.context_len = context_len
        self.use_short_digest = use_short_digest or (lambda text: False)
    
    @retry(tries=2, delay=5, logger=logger)
    def _run(self, text: str) -> Digest:
        max_tokens = 400

        resp = self.client.chat.completions.create(
            messages=create_prompt_for_generic_model(text),
            model=self.model_name,
            max_tokens=max_tokens, 
            temperature=0.2,
            seed=666
        ).choices[0].message.content
        return parse_compressed_digest(resp) 

    def run(self, text: str) -> Digest:
        return self._run(truncate(text, self.context_len//2))
    
    def run_batch(self, texts: list[str]) -> list[Digest]:
        return batch_run(self._run, batch_truncate(texts, self.context_len//2), BATCH_SIZE)

MARKDOWN_HEADERS = ["# ", "## ", "### ", "#### ", "**"]
def cleanup_markdown(text: str) -> str:
    # remove all \t with
    text = text.replace("\t", "")
    
    # removing the first line if it looks like a header
    text = text.strip()
    if any(text.startswith(tag) for tag in MARKDOWN_HEADERS):
        text = remove_before(text, "\n") 

    # replace remaining headers with "**"
    text = re.sub(r"(#+ )(.*?)(\n|$)", replace_header_tag, text)
    # Replace "\n(any number of spaces)\n" with "\n\n"
    text = re.sub(r"\n\s*\n", "\n\n", text)
    # # Remove any space after "\n"
    # text = re.sub(r"\n\s+", "\n", text)
    # Replace "\n\n\n" with "\n\n"
    # text = re.sub(r"\n\n\n", "\n\n", text)
    # # remove > right after \n
    # text = re.sub(r"\n>", "\n", text)
    # replace every single \n with \n\n
    text = re.sub(r'(?<!\n)\n(?!\n)', '\n\n', text)
    # Add a space after every "+" if there is no space
    text = re.sub(r'\+(?!\s)', '+ ', text)

    return text.strip()

def replace_header_tag(match):
    header_content = match.group(2).strip()  # The content after "# " or "## "
    newline = match.group(3)  # Preserve the newline or end of string
    return f"\n**{header_content}**{newline}"

def remove_before(text: str, sub: str) -> str:
    index = text.find(sub)
    if index > 0: return text[index:]
    return text

def remove_after(text: str, sub: str) -> str:
    index = text.find(sub)
    if index > 0: return text[:index]
    return text


def from_path(
    digestor_path: str,
    context_len: int = CONTEXT_LEN, 
    base_url: str = None, 
    api_key: str = None,
    use_short_digest = None
) -> Digestor:
    if digestor_path.startswith(LLAMA_CPP_PREFIX):
        return LlamaCppDigestor(digestor_path.removeprefix(LLAMA_CPP_PREFIX), context_len=context_len, use_short_digest=use_short_digest)
    elif base_url:
        return RemoteDigestor(digestor_path, base_url, api_key, context_len=context_len, use_short_digest=use_short_digest)
    else:
        return TransformerDigestor(digestor_path, context_len=context_len, use_short_digest=use_short_digest)
  