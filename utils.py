import sys
import logging
import tiktoken

_LOGGER_PATH = None

def set_logger_path(logger_path: str):
    global _LOGGER_PATH
    _LOGGER_PATH = logger_path

def create_logger(name: str):
    logger=logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(_LOGGER_PATH) if _LOGGER_PATH else logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(f'[%(asctime)s] {name} - %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M"))
    logger.addHandler(handler)
    return logger

_encoding = tiktoken.get_encoding("cl100k_base")

def chunk(input: str, n_ctx) -> list[str]:
    tokens = _encoding.encode(input)
    return [_encoding.decode(tokens[start:start+n_ctx]) for start in range(0, len(tokens), n_ctx)]

def truncate(input: str, n_ctx) -> str:
    tokens = _encoding.encode(input)
    return _encoding.decode(tokens[:n_ctx]) 

def count_tokens(input: str) -> int:
    return len(_encoding.encode(input))