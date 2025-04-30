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
from .utils import LLAMA_CPP_PREFIX, batch_truncate, truncate
from pydantic import BaseModel, Field
from openai import OpenAI
import logging

logger = logging.getLogger(__name__)

CONTEXT_LEN = 8192
BATCH_SIZE = int(os.getenv("DIGESTOR_BATCH_SIZE", os.cpu_count()))
NUM_THREADS = int(os.getenv("DIGESTOR_NUM_THREADS", os.cpu_count()))

class Digest(BaseModel):
    gist: Optional[str] = Field(default=None)
    domains: Optional[list[str]] = Field(default=None)
    entities: Optional[list[str]] = Field(default=None)
    locations: Optional[list[str]] = Field(default=None)
    topic: Optional[str] = Field(default=None)
    summary: Optional[str] = Field(default=None)
    takeways: Optional[list[str]] = Field(default=None)
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


create_prompt_for_tuned_model = lambda input_text, template: [
    {
        "role": "user",
        "content":  template.format(input_text=input_text)
    }
]

parse_list = lambda field: [item.strip() for item in field.split(',')] if isinstance(field, str) else field
unique_items = lambda items: list({item.strip().lower(): item for item in items}.values()) if items else items

def json_to_digest(response: str):  
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
    
_MARKDOWN_START = "```markdown"
_MARKDOWN_END="```"
def markdown_to_digest(response: str):
    digest = Digest()
    response = response.strip().removeprefix(_MARKDOWN_START).removesuffix(_MARKDOWN_END).strip()
    last = None
    for line in response.splitlines():
        line = line.strip()
        if not line or line == UNDETERMINED: continue

        if any(field in line for field in DIGEST_FIELDS):
            last = line
        elif GIST in last:
            digest.gist = line
        elif DOMAINS in last:
            digest.domains = parse_list(line)
        elif ENTITIES in last:
            digest.entities = parse_list(line)
        elif TOPIC in last:
            digest.topic = line 
        elif LOCATION in last:
            digest.locations = parse_list(line)
        elif SUMMARY in last:
            digest.summary = (digest.summary+"\n"+line) if digest.summary else line
        elif TAKEAWAYS in last:
            if not digest.takeways: digest.takeways = []
            digest.takeways.append(line.removeprefix("- ").removeprefix("* "))
        elif INSIGHT in last:
            digest.insight = line

    return digest    

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
            model_path=self.model_path, n_ctx=self.context_len+(self.context_len//4), # this extension is needed to accommodate occasional overflows
            n_batch=self.context_len//2, n_threads_batch=NUM_THREADS, n_threads=NUM_THREADS, 
            embedding=False, verbose=False
        )  

    def _generate_response(self, input_text: str, template: str, max_new_tokens: int = 256, response_format = None) -> str:
        return self.model.create_chat_completion(
            messages=create_prompt(input_text=input_text, template=template, max_tokens=self.context_len//2),
            max_tokens=max_new_tokens,
            seed=666,
            response_format={"type": response_format} if response_format else None,
            # temperature=0.1, # if response_format=="json_object" else 0.3,
            # frequency_penalty=0.2,
            # top_k=40,
            # repeat_penalty=1 if response_format=="json_object" else 1.3
        )['choices'][0]['message']['content'].strip() 

    def _run_one(self, text: str) -> Digest:
        digest = json_to_digest(
            self._generate_response(
                input_text=text, 
                template=TUNED_MODEL_DIGEST_INST, 
                max_new_tokens=600,
                response_format="json_object"
            )
        )
        # resp = self._generate_response(input_text=text, template=EXTRACTION_TEMPLATE, max_new_tokens=192, response_format="json_object")
        # digest = parse_digest(resp)
        # if digest and needs_summary(text):
        #     digest.summary = self._generate_response(input_text=text, template=SUMMARY_TEMPLATE, max_new_tokens=360)
        return digest         
  
    def run(self, text: str) -> Digest:
        with self.lock:
            digest = self._run_one(text)
        return digest
    
    def run_batch(self, texts: list[str]) -> list[Digest]:
        with self.lock:
            digests = [self._run_one(text) for text in texts]
        return digests
    
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
        self.client = OpenAI(api_key=api_key, base_url=base_url, timeout=20, max_retries=2)
        self.model_name = model_name
        self.context_len = context_len
        self.use_short_digest = use_short_digest or (lambda text: False)

    def create_prompt(self, text: str): 
        template = GENERIC_MODEL_SHORT_DIGEST_INST \
            if self.use_short_digest(text) else \
                GENERIC_MODEL_DIGEST_INST
        return [
            {
                "role": "system",
                "content": GENERIC_MODEL_SYSTEM_INST
            },
            {
                "role": "user",
                "content":  template.format(input_text=text)
            }
        ]
    
    # @retry(tries=2, delay=5, logger=logger)
    def _run(self, text: str) -> Digest:
        max_tokens = 300 if self.use_short_digest(text) else 512

        resp = self.client.chat.completions.create(
            messages=self.create_prompt(text),
            model=self.model_name,
            max_tokens=max_tokens, 
            temperature=0.3,
            seed=666
        ).choices[0].message.content
        return markdown_to_digest(resp)

    def run(self, text: str) -> Digest:
        return self._run(truncate(text, self.context_len//2))
    
    def run_batch(self, input_texts: list[str]) -> list[Digest]:
        input_texts = batch_truncate(input_texts, self.context_len//2)
        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="remote-digestor") as executor:
            results = list(executor.map(self._run, input_texts))
        return results
    
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
    
_RESPONSE_START = "<|im_start|>assistant\n"
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
        
        # from unsloth import FastLanguageModel
        from transformers import AutoModelForCausalLM, AutoTokenizer
        import torch

        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        # if self.device == "cuda":
        #     self.model, self.tokenizer = FastLanguageModel.from_pretrained(
        #         model_name=model_id,
        #         max_seq_length=self.context_len,
        #         load_in_4bit=False,
        #         # load_in_8bit=True,
        #         device_map=self.device
        #     )
        #     self.model = FastLanguageModel.for_inference(self.model)
        #     self.tokenizer = self.tokenizer
        # else:
        self.tokenizer = AutoTokenizer.from_pretrained(model_id, padding=True, truncation=True, max_length=context_len)
        self.model =  AutoModelForCausalLM.from_pretrained(model_id, device_map="auto", torch_dtype="auto")
        

    def _extract_response(self, generated: str) -> str:
        start_index = generated.find(_RESPONSE_START)
        end_index = generated.find(_RESPONSE_END, start_index)
        return generated[start_index + len(_RESPONSE_START):end_index].strip()

    def _generate_response(self, text: str, template: str, max_new_tokens: int = 256) -> str:
        prompt = create_prompt(text, template, self.context_len//2)
        inputs = self.tokenizer.apply_chat_template(prompt, tokenize=True, add_generation_prompt=True, max_length=self.context_len, return_tensors="pt").to(self.device)
        outputs = self.model.generate(**inputs, max_new_tokens=max_new_tokens)
        generated = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        return self._extract_response(generated)

    def run(self, text: str) -> Digest:
        with self.lock:
            digest = json_to_digest(
                self._generate_response(
                    input_text=text, 
                    template=TUNED_MODEL_DIGEST_INST, 
                    max_new_tokens=600
                )
            )
            # resp = self._generate_response(text, EXTRACTION_TEMPLATE, max_new_tokens=192)
            # digest = parse_digest(resp) or Digest()
            # if digest and needs_summary(text):
            #     digest.summary = self._generate_response(text, SUMMARY_TEMPLATE, max_new_tokens=360)
        return digest
    
    def _generate_response_batch(self, texts: list[str], template: str, max_new_tokens: int = 256) -> list[str]:
        prompts = [create_prompt(text, template, self.context_len//2) for text in texts]
        inputs = self.tokenizer.apply_chat_template(prompts, tokenize=True, add_generation_prompt=True, padding=True, truncation=True, max_length=self.context_len, return_dict=True, return_tensors="pt").to(self.device)
        outputs = self.model.generate(**inputs, max_new_tokens=max_new_tokens, num_beams=BATCH_SIZE, top_k=25)
        generated = self.tokenizer.batch_decode(outputs, skip_special_tokens=False)
        return [self._extract_response(g) for g in generated]

    def run_batch(self, texts: list[str]) -> list[Digest]:
        digests = []    
        with self.lock:
            for i in range(0, len(texts), BATCH_SIZE):               
                responses = self._generate_response_batch(texts[i:i + BATCH_SIZE], TUNED_MODEL_DIGEST_INST, max_new_tokens=600)
                digests.extend([json_to_digest(response) for response in responses])
            # for i in range(0, len(texts), BATCH_SIZE):               
            #     extracts = self._generate_response_batch(texts[i:i + BATCH_SIZE], EXTRACTION_TEMPLATE, max_new_tokens=192)
            #     summaries = self._generate_response_batch(texts[i:i + BATCH_SIZE], SUMMARY_TEMPLATE, max_new_tokens=360)
            #     for ext, summ in zip(extracts, summaries):
            #         digest = parse_digest(ext)
            #         if digest: digest.summary = summ
            #         digests.append(digest)
        return digests
    
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
  