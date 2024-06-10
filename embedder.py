## LOCAL EMBEDDER ##
from llama_cpp import Llama
from langchain_core.embeddings import Embeddings
import tiktoken
from functools import reduce
from retry import retry
import logging

MODEL_PATH = "models/nomic.gguf"
CTX = 2047
SEARCH_DOCUMENT = "search_document"
SEARCH_QUERY = "search_query"
encoding = tiktoken.get_encoding("cl100k_base")

class LocalNomic(Embeddings):
    def __init__(self):
        self.model = Llama(model_path=LocalNomic.MODEL_PATH, n_ctx=LocalNomic.CTX, embedding=True, verbose=False)
       
    def embed_documents(self, texts: list[str]):
        return self._embed(texts, LocalNomic.SEARCH_DOCUMENT)
    
    def embed_query(self, text: str):    
        return self._embed(text, LocalNomic.SEARCH_QUERY)

    def embed_queries(self, texts: list[str]):    
        return self._embed(texts, LocalNomic.SEARCH_QUERY)
    
    @retry(tries=5)
    def _embed(self, input: str|list[str], task_type: str):
        texts = _prep_input(input, task_type)
        result = [self.model.create_embedding(text) for text in texts]
        if any(not res for res in result):
            raise Exception("None value returned by embedder")
        return result[0] if isinstance(input, str) else result 
            
def _count_tokens(texts: list[str]) -> int:
    return reduce(lambda a,b: a+b, [len(enc) for enc in encoding.encode_batch(texts)])

def _truncate(input: str|list[str]) -> str|list[str]:
    if isinstance(input, str):
        return encoding.decode(encoding.encode(input)[:CTX])
    else:
        return [encoding.decode(enc[:CTX]) for enc in encoding.encode_batch(input)]

def _prep_input(input: str|list[str], task_type: str):
    texts = [input] if isinstance(input, str) else input
    return _truncate([f"{task_type}: {t}" for t in texts])
