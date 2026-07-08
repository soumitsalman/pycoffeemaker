import base64
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
import json
import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Optional, Type
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_random
from .models import *
from .runtime import *
from .text import split_parts
from icecream import ic

log = logging.getLogger("digestor")

try: import torch
except: log.warning("PyTorch Not Available", extra={'source': __file__, 'num_items': 1})

_DEFAULT_SAMPLING_PARAMS = {
    "temperature": 0.5,
    "top_k": 50,
    "top_p": 1.0,
    "repetition_penalty": 1.15,
}
# DEFAULT_CONTEXT_LEN = 32768

class TextAnalystBase(ABC):
    def __init__(
        self,
        model_name: str,
        context_len: int,        
        instruction: str,
        input_template: str,
        output_model: Type[BaseModel],
        enable_thinking: bool,
        max_new_tokens: int,
        **sampling_params,
    ):
        self.model_name = model_name
        self.context_len = context_len
        self.instruction = instruction
        self.input_template = input_template
        self.output_model = output_model
        self.response_mode = "json" if output_model else None
        self.enable_thinking = enable_thinking
        self.max_new_tokens = max_new_tokens
        self.max_prompt_len = context_len - max_new_tokens - TOKEN_MARGIN
        self.sampling_params = sampling_params
        self._llm = None

    @abstractmethod
    def __enter__(self):
        raise NotImplementedError()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._llm:
            del self._llm
            self._llm = None
            # del self._sampling_params
            # self._sampling_params = None
            clear_gpu_cache()
        return False

    def create_prompt(self, msg: str):
        prompt = []        
        input_text = msg[:self.max_prompt_len<<2] # this is a heuristic
        if self.instruction: prompt.append({"role": "system", "content": self.instruction})
        prompt.append({
            "role": "user", 
            "content": self.input_template.format(
                description=self.output_model.model_text_schema(), 
                input_text=input_text
            ) if self.input_template else input_text
        })
        return prompt

    def parse_output(self, response: str):
        response = _strip_fences(response)
        try:
            if self.response_mode == "json":
                return self.output_model.model_validate_json(response)
            if self.response_mode == "compressed":
                return parse_compressed(response)
            if self.response_mode == "markdown":
                return parse_markdown(response)
            if self.response_mode == "tool_call":
                raise NotImplementedError
            return response
        except:
            log.warning("failed parsing: %s", response, exc_info=True)

    @abstractmethod
    def run_batch(self, input_messages: list[str]) -> list[BaseModel]:
        raise NotImplementedError()


class LocalTokenizer:
    tokenizer = None
    context_len = None
    device = None

    def __init__(
        self,
        model_id,
        context_len: int,
        device: str,
        max_prompt_len: int,
        enable_thinking: bool,
    ):
        from transformers import AutoTokenizer

        self.tokenizer = AutoTokenizer.from_pretrained(model_id, max_length=context_len, use_fast=True, trust_remote_code=True, padding_side="left")
        self.context_len = context_len
        self.device = device        
        if not self.tokenizer.pad_token:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        self.end_think_token_id = self.tokenizer.convert_tokens_to_ids("</think>")
        self.max_prompt_len = max_prompt_len
        self.enable_thinking = enable_thinking

    def tokenize_prompts(self, prompts):
        return self.tokenizer.apply_chat_template(
            prompts,
            padding=True,
            truncation=True,
            max_length=self.max_prompt_len,
            return_tensors="pt",
            return_dict=True,
            add_generation_prompt=True,
            enable_thinking=self.enable_thinking
        ).to(self.device)

    def _strip_after_thinking(self, tokens):
        if not self.end_think_token_id or self.end_think_token_id <= 0:
            return tokens
        matches = (tokens == self.end_think_token_id).nonzero(as_tuple=True)[0]
        if matches.numel():
            return tokens[matches[0] + 1:]
        return tokens

    def decode(self, tokens, input_tokens = None):
        if input_tokens is not None:
            prompt_len = input_tokens["input_ids"].shape[1]
            tokens = tokens[prompt_len:]
        tokens = self._strip_after_thinking(tokens)
        return self.tokenizer.decode(tokens, skip_special_tokens=True)

    def batch_decode(self, tokens, input_tokens = None):
        if input_tokens is not None:
            prompt_len = input_tokens["input_ids"].shape[1]
            tokens = tokens[:, prompt_len:]
        if self.end_think_token_id and self.end_think_token_id > 0 and (tokens == self.end_think_token_id).any():
            tokens = [self._strip_after_thinking(row) for row in tokens]
        return self.tokenizer.batch_decode(tokens, skip_special_tokens=True)

    @property
    def pad_token_id(self):
        return self.tokenizer.pad_token_id

    @property
    def eos_token_id(self):
        return self.tokenizer.eos_token_id


# needs 1.3 for repetition penalty support, and max_new_tokens = 384
# "no_repeat_ngram_size": 3,
class TransformerTextAnalyst(TextAnalystBase):
    _tokenizer = None
                
    def __enter__(self):
        if not self._llm:
            from transformers import AutoModelForCausalLM

            self.device = "cuda" if torch.cuda.is_available() else "cpu"
            self.dtype = (
                torch.bfloat16
                if self.device == "cuda" and torch.cuda.is_bf16_supported()
                else torch.float16
                if self.device == "cuda"
                else torch.float32
            )
            self._llm = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                dtype=self.dtype,
                device_map=self.device,
                trust_remote_code=True,
            )
            self._tokenizer = LocalTokenizer(self.model_name, self.context_len, self.device, self.max_prompt_len, self.enable_thinking)
            self.sampling_params.update({
                "max_new_tokens": self.max_new_tokens,
                "do_sample": True,
            })

            if self.response_mode == "json":
                from lmformatenforcer import JsonSchemaParser
                from importlib import import_module
                from transformers.tokenization_utils_base import PreTrainedTokenizerBase

                tokenization_utils = import_module("transformers.tokenization_utils")
                if not hasattr(tokenization_utils, "PreTrainedTokenizerBase"):
                    tokenization_utils.PreTrainedTokenizerBase = PreTrainedTokenizerBase

                from lmformatenforcer.integrations.transformers import build_transformers_prefix_allowed_tokens_fn
                
                parser = JsonSchemaParser(self.output_model.model_json_schema())
                self._sampling_params["prefix_allowed_tokens_fn"] = (
                    build_transformers_prefix_allowed_tokens_fn(
                        self._tokenizer.tokenizer, 
                        parser
                    )
                )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._tokenizer:
            del self._tokenizer
            self._tokenizer = None
        return super().__exit__(exc_type, exc_val, exc_tb)

    def _run_batch(self, prompts, **kwargs):
        with torch.inference_mode(), torch.amp.autocast(self.device, self.dtype):
            input_tokens = self._tokenizer.tokenize_prompts(prompts)
            output_tokens = self._llm.generate(**input_tokens, **self.sampling_params)
            generated_texts = self._tokenizer.batch_decode(output_tokens, input_tokens)
        return generated_texts

    def run_batch(self, input_messages: list[str]) -> list[Digest | None]:
        if not self._llm: self.__enter__()

        generated_texts = self._run_batch([self.create_prompt(msg) for msg in input_messages])
        return [self.parse_output(text) for text in generated_texts]


VLLM_MAX_NUM_BATCHED_TOKENS = int(os.getenv("VLLM_MAX_NUM_BATCHED_TOKENS", 0))
VLLM_MAX_NUM_SEQS = int(os.getenv("VLLM_MAX_NUM_SEQS", 0))
VLLM_GPU_MEMORY_UTILIZATION = float(os.getenv("VLLM_GPU_MEMORY_UTILIZATION", 0.92))
VLLM_ATTENTION_BACKEND=os.getenv("VLLM_ATTENTION_BACKEND")

class VLLMTextAnalyst(TextAnalystBase):
    def __enter__(self):
        if not self._llm:
            from vllm import LLM, SamplingParams
            from vllm.sampling_params import StructuredOutputsParams
            
            self._llm = LLM(
                model=self.model_name,
                max_model_len=self.context_len,
                max_num_seqs=VLLM_MAX_NUM_SEQS if VLLM_MAX_NUM_SEQS > 0 else None,
                max_num_batched_tokens=VLLM_MAX_NUM_BATCHED_TOKENS if VLLM_MAX_NUM_BATCHED_TOKENS > 0 else None,
                gpu_memory_utilization=VLLM_GPU_MEMORY_UTILIZATION,
                enable_prefix_caching=True,
                enable_chunked_prefill=True,        
                trust_remote_code=True,
                attention_config={"backend": VLLM_ATTENTION_BACKEND} if VLLM_ATTENTION_BACKEND else None,
            )
            self.sampling_params = SamplingParams(
                **self.sampling_params,
                max_tokens=self.max_new_tokens,
                # stop=["}\n", "\n\n", "\t\t", "\n \n", "\n\t\n"],
                structured_outputs=StructuredOutputsParams(
                    json=self.output_model.model_json_schema()
                ),
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._llm:
            try:
                engine_core = self._llm.llm_engine.engine_core
                engine_core.shutdown()
            except Exception:
                log.warning("Failed to shutdown vLLM engine cleanly", exc_info=True)
        return super().__exit__(exc_type, exc_val, exc_tb)

    def run_batch(self, input_messages: list[str]) -> list[BaseModel]:
        responses = self._llm.chat(
            [self.create_prompt(msg) for msg in input_messages], 
            sampling_params=self.sampling_params,             
            chat_template_kwargs={"enable_thinking": self.enable_thinking},
            tokenization_kwargs={"truncate_prompt_tokens": self.max_prompt_len},
            use_tqdm=False, 
        )
        return [self.parse_output(resp.outputs[0].text) if resp.outputs else None for resp in responses]


class RemoteTextAnalyst(TextAnalystBase):
    def __init__(
        self,
        model_name: str,
        base_url: str,
        api_key: str,
        context_len: int,
        instruction: str,
        input_template: str,
        output_model: Type[BaseModel],
        enable_thinking: bool,
        max_new_tokens: int,
        **sampling_params,
    ):
        super().__init__(
            model_name=model_name,
            context_len=context_len,
            instruction=instruction,
            input_template=input_template,
            output_model=output_model,
            enable_thinking=enable_thinking,
            max_new_tokens=max_new_tokens,
            **sampling_params,
        )
        self.base_url = base_url
        self.api_key = api_key
        self.sampling_params.pop('top_k', None)
        self.sampling_params.pop('repetition_penalty', None)
        self.sampling_params["max_completion_tokens"] = self.max_new_tokens
        if self.enable_thinking:
            self.sampling_params["extra_body"] = {"reasoning_budget": self.context_len, "chat_template_kwargs": {"enable_thinking": self.enable_thinking}}
        

    def __enter__(self):
        if not self._llm:
            from openai import OpenAI
            self._llm = OpenAI(api_key=self.api_key, base_url=self.base_url, timeout=180, max_retries=3)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._llm:
            del self._llm
            self._llm = None
        return False

    @retry(stop=stop_after_attempt(REMOTE_RETRY_COUNT), wait=wait_random(*REMOTE_RETRY_JITTER), reraise=True)
    def _run_single(self, msg: str) -> BaseModel:
        response = self._llm.chat.completions.parse(
            model=self.model_name,
            messages=self.create_prompt(msg),
            response_format=self.output_model,
            **self.sampling_params
        )
        return response.choices[0].message.parsed
        
    def run_batch(self, input_messages: list[str]) -> list[BaseModel]:
        if not self._llm: self.__enter__()
        with ThreadPoolExecutor(max_workers=len(input_messages)) as exec:
            results = list(exec.map(self._run_single, input_messages))
        return results


class EntityExtractor:
    model_path: str
    confidence = 0.5
    _splitter = None

    _LABELS = [
        "person",
        "people",
        "organization",
        "company",
        "institution",
        "business",
        "city",
        "state",
        "country",
        "location",
        "stock",
        "ticker",
        "stockticker",
        "product",
    ]
    _LABEL_FIELD_MAPPINGS = {
        "person": "people",
        "people": "people",
        "organization": "companies",
        "company": "companies",
        "institution": "companies",
        "business": "companies",
        "city": "regions",
        "state": "regions",
        "country": "regions",
        "location": "regions",
        "stock": "stock_tickers",
        "ticker": "stock_tickers",
        "stockticker": "stock_tickers",
        "product": "products",
    }   
        
    def __init__(self, model_path: str, context_len: int, threshold=0.5, batch_size: int = 16) -> None:
        # super().__init__(model_name=model_path, context_len=context_len, instruction=None, input_template=None, output_model=None)
        self.model_name = model_path
        self.context_len = context_len
        self.threshold = threshold
        self.batch_size = batch_size
        self._llm = None
        self._label_embeddings = None        
        self._splitter = None
    
    def __enter__(self):
        if not self._llm:
            import torch
            from gliner import GLiNER
            from llama_index.core.text_splitter import TokenTextSplitter

            # config.fx_graph_cache = True
            self._llm = GLiNER.from_pretrained(
                self.model_name,
                max_length=self.context_len,
                map_location="cuda" if torch.cuda.is_available() else "cpu",
            )
            self._label_embeddings = self._llm.encode_labels(
                self._LABELS, batch_size=len(self._LABELS)
            )
            self._splitter = TokenTextSplitter(
                chunk_size=self.context_len - TOKEN_MARGIN,
                chunk_overlap=TOKEN_MARGIN<<1,
                include_metadata=False,
                include_prev_next_rel=False,
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._llm:
            del self._llm
            self._llm = None
            del self._label_embeddings
            self._label_embeddings = None        
            del self._splitter
            self._splitter = None        
        clear_gpu_cache()
        return False    

    def parse_output(self, response):
        res = defaultdict(list)
        for ent in response:
            res[self._LABEL_FIELD_MAPPINGS[ent["label"]]].append(ent["text"])
        for k, v in res.items():
            res[k] = list({item.lower(): item for item in v}.values())
        return Entities(**res)

    def _split(self, text: str):
        chunks = self._splitter.split_text(text)
        if len(chunks) > 1 and len(chunks[-1]) < (TOKEN_MARGIN<<2): chunks = chunks[:-1]
        return chunks

    def _create_chunks(self, texts: list[str]) -> tuple[list[str], list[int], list[int]]:
        texts = texts if isinstance(texts, list) else [texts]
        
        chunks = list(map(self._split, texts))
        counts = list[int](map(len, chunks))

        start_idx = [0]*len(chunks)
        for i in range(1,len(counts)):
            start_idx[i] = start_idx[i-1]+counts[i-1]
        return list(chain(*chunks)), start_idx, counts

    def _merge_chunks(self, entities: list[Entities]):
        entities = [e for e in entities if e]
        if entities:
            return Entities(
                regions=list(set(chain(*[e.regions for e in entities if e.regions]))),
                people=list(set(chain(*[e.people for e in entities if e.people]))),
                products=list(set(chain(*[e.products for e in entities if e.products]))),
                companies=list(set(chain(*[e.companies for e in entities if e.companies]))),
                stock_tickers=list(set(chain(*[e.stock_tickers for e in entities if e.stock_tickers]))),
            )

    def run_batch(self, input_messages: list[str]):
        chunks, start_idx, counts = self._create_chunks(input_messages)
        entities = self._llm.batch_predict_with_embeds(
            chunks,
            labels_embeddings=self._label_embeddings,
            labels=self._LABELS,
            threshold=self.threshold,
            batch_size=self.batch_size,
        )
        entities = [self.parse_output(group) if group else None for group in entities]        
        return [self._merge_chunks(entities[start:start+count]) for start, count in zip(start_idx, counts)]


def create_text_analyst(
    model_path: str, 
    context_len: int, 
    instruction: str = None, 
    input_template: str = None, 
    output_model: Type[BaseModel] = Digest, 
    enable_thinking: bool = False, 
    max_new_tokens: int = 2048, 
    **kwargs,
) -> TextAnalystBase:
    if model_path.startswith(VLLM_PREFIX):
        # remove these two if they exist
        kwargs.pop('base_url', None) 
        kwargs.pop('api_key', None)
        return VLLMTextAnalyst(
            model_path.removeprefix(VLLM_PREFIX),
            context_len=context_len,
            instruction=instruction,
            input_template=input_template,
            output_model=output_model,
            enable_thinking=enable_thinking,
            max_new_tokens=max_new_tokens,
            **kwargs,
        )
    elif kwargs.get("base_url") and kwargs.get("api_key"):
        return RemoteTextAnalyst(
            model_path,
            base_url=kwargs.pop("base_url"),
            api_key=kwargs.pop("api_key"),
            context_len=context_len,
            instruction=instruction,
            input_template=input_template,
            output_model=output_model,
            enable_thinking=enable_thinking,
            max_new_tokens=max_new_tokens,
            **kwargs,
        )
    else:
        kwargs.pop('base_url', None) 
        kwargs.pop('api_key', None)
        return TransformerTextAnalyst(
            model_path, 
            context_len=context_len, 
            instruction=instruction,
            input_template=input_template,
            output_model=output_model, 
            enable_thinking=enable_thinking,
            max_new_tokens=max_new_tokens,
            **kwargs
        )

# ---------------------
# PARSING UTILITIES
# ---------------------

def _strip_fences(text: str) -> str:
    return (
        text.strip()
        .removeprefix("```json")
        .removeprefix("```markdown")
        .removeprefix("```")
        .removesuffix("```")
        .strip()
    )

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
M_FIELDS = [
    M_GIST,
    M_CATEGORIES,
    M_ENTITIES,
    M_TOPIC,
    M_REGIONS,
    M_SUMMARY,
    M_KEYPOINTS,
    M_KEYEVENTS,
    M_DATAPOINTS,
    M_INSIGHT,
]
M_START = "```markdown"
M_END = "```"
MARKDOWN_HEADERS = ["# ", "## ", "### ", "#### ", "**"]


def parse_markdown(response: str):
    digest = Digest(raw=response)
    response = response.strip().removeprefix(M_START).removesuffix(M_END).strip()
    last = None
    for line in response.splitlines():
        line = line.strip()
        if not line:
            continue

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
            digest.summary = (digest.summary + "\n" + line) if digest.summary else line
        elif C_KEYPOINTS in last:
            if not digest.keypoints:
                digest.keypoints = []
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
COMPRESSED_FIELDS = [
    C_KEYPOINTS,
    C_KEYEVENTS,
    C_DATAPOINTS,
    C_REGIONS,
    C_ENTITIES,
    C_CATEGORIES,
    C_SENTIMENTS,
]


def parse_compressed(response: str):
    if not response:
        return response

    results = {"P:": [], "E:": [], "D:": [], "N:": [], "R:": []}
    current_pos = 0
    while current_pos < len(response):
        key = response[current_pos : current_pos + 2]
        if key in results:
            next_key_pos = [
                response.find(";" + next_key, current_pos + 2)
                for next_key in results.keys()
            ]
            end = min([pos for pos in next_key_pos if pos > -1], default=len(response))
            if response[end - 1] == ";":
                ext = response[current_pos + 2 : end - 1]
            else:
                ext = response[current_pos + 2 : end]

            results[key].extend(
                chain(*(item.strip().split(";") for item in ext.strip().split("|")))
            )
            current_pos = end

        current_pos += 1

    response = ""
    for key, value in results.items():
        if not value:
            continue
        response += key + "|".join(v.strip() for v in value) + ";"

    return Digest(
        raw=response,
        keypoints=results.get("P:") or None,
        keyevents=results.get("E:") or None,
        datapoints=results.get("D:") or None,
        entities=results.get("N:") or None,
        regions=results.get("R:") or None,
    )
