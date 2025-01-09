from datetime import datetime as dt, timedelta, timezone
import math
import tiktoken

DEFAULT_VECTOR_SEARCH_SCORE = 0.75
DEFAULT_VECTOR_SEARCH_LIMIT = 1000

_encoding = tiktoken.get_encoding("cl100k_base")

def chunk(input: str, context_len: int) -> list[str]:
    tokens = _encoding.encode(input)
    num_chunks = math.ceil(len(tokens) / context_len)
    chunk_size = math.ceil(len(tokens) / num_chunks)
    return [_encoding.decode(tokens[start : start+chunk_size]) for start in range(0, len(tokens), chunk_size)]

truncate = lambda input, n_ctx: _encoding.decode(_encoding.encode(input)[:n_ctx]) 
count_tokens = lambda input: len(_encoding.encode(input))

now = lambda: dt.now(tz=timezone.utc)
ndays_ago = lambda ndays: now() - timedelta(days=ndays)