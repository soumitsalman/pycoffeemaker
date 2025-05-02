from concurrent.futures import ThreadPoolExecutor
import os
from typing import Callable
import tiktoken
import math

LLAMA_CPP_PREFIX = "llama-cpp://"
API_URL_PREFIX = "https://"

_encoding = tiktoken.get_encoding("cl100k_base")
NUM_THREADS = os.cpu_count()

def chunk(input: str, context_len: int) -> list[str]:
    tokens = _encoding.encode(input)
    num_chunks = math.ceil(len(tokens) / context_len)
    chunk_size = math.ceil(len(tokens) / num_chunks)
    return [_encoding.decode(tokens[start : start+chunk_size]) for start in range(0, len(tokens), chunk_size)]

def combine_texts(texts: list[str], batch_size: int, delimiter: str = "```") -> list[str]:
    if count_tokens(texts) > batch_size:
        half = len(texts) // 2
        return combine_texts(texts[:half], batch_size, delimiter) + combine_texts(texts[half:], batch_size, delimiter)
    else:
        return [delimiter.join(texts)]
    
def chunk_tokens(input: str, context_len: int, encode_fn) -> list[str]:
    tokens = encode_fn(input)
    num_chunks = math.ceil(len(tokens) / context_len)
    chunk_size = math.ceil(len(tokens) / num_chunks)
    return [tokens[start : start+chunk_size] for start in range(0, len(tokens), chunk_size)]

truncate = lambda input, n_ctx: _encoding.decode(_encoding.encode(input)[:n_ctx]) 
count_tokens = lambda input: len(_encoding.encode(input))

def batch_truncate(input_texts: list[str], n_ctx):
    tokenlist = _encoding.encode_batch(input_texts, num_threads=os.cpu_count())
    tokenlist = [tokens[:n_ctx] for tokens in tokenlist]
    return _encoding.decode_batch(tokenlist, num_threads=os.cpu_count())

def batch_run(func: Callable, items, num_threads: int = os.cpu_count()):
    results = None
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = list(executor.map(func, items))
    return results  