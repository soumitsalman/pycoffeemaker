import os
import logging
from typing import Callable
from retry import retry
from abc import ABC, abstractmethod
from coffeemaker.nlp.utils import *

DEFAULT_TEMPERATURE = 0.2
DEFAULT_MAX_COMPLETION_TOKENS = 512
BATCH_SIZE = int(os.getenv('BATCH_SIZE', os.cpu_count()))

log = logging.getLogger(__name__)

class TextGenerationClient(ABC):
    @abstractmethod
    def run(self, prompt: list[dict[str, str]]) -> str:
        raise NotImplementedError("Subclass must implement abstract method")

    def run_batch(self, prompts: list[list[dict[str, str]]]) -> list[str]:
        return list(map(self.run, prompts))

class RemoteClient(TextGenerationClient):
    openai_client = None
    model_name: str = None
    max_output_tokens: int = None
    temperature: float = None

    def __init__(self, 
        model_name: str,
        base_url: str, 
        api_key: str,
        max_output_tokens: int,
        temperature: float,
        json_mode: bool
    ):
        from openai import OpenAI

        self.openai_client = OpenAI(api_key=api_key, base_url=base_url, timeout=30, max_retries=2)
        self.model_name = model_name
        self.max_output_tokens = max_output_tokens
        self.temperature = temperature
        self.json_mode = json_mode

    @retry(tries=2, delay=5, logger=log)
    def run(self, prompt: list[dict[str, str]]) -> str:
        return self.openai_client.chat.completions.create(
            messages=prompt,
            model=self.model_name,
            max_completion_tokens=self.max_output_tokens,
            response_format={ "type": "json_object" } if self.json_mode else None,
            temperature=self.temperature,
            seed=666
        ).choices[0].message.content
    
    def run_batch(self, prompts: list[list[dict[str, str]]]) -> list[str]:
        return batch_run(self.run, prompts, BATCH_SIZE)
    
DEFAULT_RESPONSE_START = "<|im_start|>assistant\n"
DEFAULT_RESPONSE_END = "<|im_end|>"
class TransformerClient(TextGenerationClient):
    model = None
    tokenizer = None
    device = None
    # context_len = None

    def __init__(self, 
        model_id: str,
        max_output_tokens: int,
        temperature: float,
        response_start: str,
        response_end: str
    ):
        from transformers import AutoModelForCausalLM, AutoTokenizer
        import torch

        # self.context_len = context_len
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.tokenizer = AutoTokenizer.from_pretrained(model_id, padding=True, truncation=True)
        self.model =  AutoModelForCausalLM.from_pretrained(model_id, device_map="auto", torch_dtype="auto")
        self.max_output_tokens = max_output_tokens
        self.temperature = temperature
        self.response_start = response_start
        self.response_end = response_end

    def _extract_response(self, generated: str) -> str:
        generated = remove_before(generated, self.response_start)
        return remove_after(generated, self.response_end)

    def run(self, prompt):
        input_tokens = self.tokenizer.apply_chat_template(prompt, tokenize=True, add_generation_prompt=True, return_tensors="pt").to(self.device)
        output_tokens = self.model.generate(**input_tokens, max_new_tokens=self.max_output_tokens, temperature=self.temperature)
        generated_text = self.tokenizer.decode(output_tokens[0], skip_special_tokens=False)
        return self._extract_response(generated_text)

    def run_batch(self, prompts):
        input_tokens = self.tokenizer.apply_chat_template(prompts, tokenize=True, add_generation_prompt=True, padding=True, truncation=True, return_dict=True, return_tensors="pt").to(self.device)
        output_tokens = self.model.generate(**input_tokens, max_new_tokens=self.max_output_tokens)
        generated_texts = self.tokenizer.batch_decode(output_tokens, skip_special_tokens=False)
        return batch_run(self._extract_response, generated_texts, BATCH_SIZE)
    
class SimpleAgent:
    client: TextGenerationClient
    max_input_tokens: int = None
    system_prompt: str = None
    output_parser: Callable = None

    def __init__(self, client, max_input_tokens: int, system_prompt: str, output_parser: Callable):
        self.client = client
        self.max_input_tokens = max_input_tokens
        self.system_prompt = system_prompt
        self.output_parser = output_parser

    def _make_prompt(self, input_msg: str):
        if self.system_prompt: return [
            {
                "role": "system",
                "content": self.system_prompt
            },
            {
                "role": "user",
                "content": input_msg
            }
        ]
        else: return [
            {
                "role": "user",
                "content": input_msg
            }
        ]

    def run(self, input_msg: str):
        if self.max_input_tokens: input_msg = truncate(input_msg, self.max_input_tokens)
        response = self.client.run(self._make_prompt(input_msg))
        if self.output_parser: return self.output_parser(response)
        return response
    
    def run_batch(self, input_messages: list[str]):
        if self.max_input_tokens: input_messages = batch_truncate(input_messages, self.max_input_tokens)
        prompts = batch_run(self._make_prompt, input_messages, BATCH_SIZE)
        responses = self.client.run_batch(prompts)
        if self.output_parser: return batch_run(self.output_parser, responses, BATCH_SIZE)
        return responses

def from_path(
    model_path: str,
    base_url: str = None, 
    api_key: str = None,
    max_input_tokens: int = None, 
    max_output_tokens: int = None,
    system_prompt: str = None,
    output_parser: Callable = None,
    temperature: float = DEFAULT_TEMPERATURE,
    json_mode: bool = False
) -> SimpleAgent:
    
    if base_url: client = RemoteClient(model_path, base_url, api_key, max_output_tokens, temperature, json_mode)
    elif model_path.startswith(LLAMA_CPP_PREFIX): raise NotImplementedError("LlamaCpp client not implemented yet")
    else: client = TransformerClient(model_path, max_output_tokens, temperature, DEFAULT_RESPONSE_START, DEFAULT_RESPONSE_END)
    
    return SimpleAgent(client, max_input_tokens, system_prompt, output_parser)