import logging
from retry import retry
import os
from .utils import truncate
from abc import ABC, abstractmethod
from memoization import cached
from icecream import ic
import logging

logger = logging.getLogger(__name__)

class Embeddings(ABC):
    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if texts:
            return self.embed(texts)

    def embed_query(self, text: str) -> list[float]:
        if text:
            return self.embed("query: "+text)
        
    def __call__(self, input):
        if input:
            return self.embed(input)

    @abstractmethod
    def embed(self, input: str|list[str]):
        raise NotImplementedError("Subclass must implement abstract method")

# local embeddings from llama.cpp
class LlamaCppEmbeddings(Embeddings):
    model_path = None
    context_len = None
    model = None

    def __init__(self, model_path: str, context_len: int = 8192):  
        from llama_cpp import Llama

        self.model_path = model_path
        self.context_len = context_len
        self.model = Llama(model_path=self.model_path, n_ctx=self.context_len, n_gpu_layers=-1, n_threads=os.cpu_count(), embedding=True, verbose=False)
    
    @retry(tries=2, logger=logger)
    def embed(self, input):
        result = self.model.create_embedding(_prep_input(input, self.context_len))
        if isinstance(input, str):
            return result['data'][0]['embedding']
        return [data['embedding'] for data in result['data']]
    
class RemoteEmbeddings(Embeddings):
    openai_client = None
    model_name: str
    context_len: int

    def __init__(self, base_url: str, api_key: str, model_name: str, context_len: int):
        from openai import OpenAI
        self.openai_client = OpenAI(base_url=base_url, api_key=api_key, max_retries=3, timeout=10)
        self.model_name = model_name
        self.context_len = context_len    
       
    @retry(tries=2, delay=5, logger=logger)
    @cached(max_size=100, ttl=600)
    def embed(self, input):
        result = self.openai_client.embeddings.create(model=self.model_name, input=_prep_input(input, self.context_len), encoding_format="float")
        if isinstance(input, str):
            return result.data[0].embedding
        return [data.embedding for data in result.data]
    
_TOKENIZER_KWARGS = {
    "padding": True,
    "truncation": True
}
class TransformerEmbeddings(Embeddings):
    model = None

    def __init__(self, model_id: str):
        import torch
        from sentence_transformers import SentenceTransformer
        
        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(model_id, trust_remote_code=True, device=device, tokenizer_kwargs=_TOKENIZER_KWARGS)

    def __call__(self, input: str|list[str]):
        return self.embed(input)
    
    @retry(tries=2, logger=logger)
    def embed(self, input):
        return self.model.encode(input).tolist()

def _prep_input(input, context_len):
    if isinstance(input, str):
        return truncate(input, context_len)
    return [truncate(t, context_len) for t in input]

LLAMA_CPP_PREFIX = "llama-cpp://"
API_URL_PREFIX = "https://"

def from_path(emb_path) -> Embeddings:
    # initialize digestor
    if LLAMA_CPP_PREFIX in emb_path:
        return LlamaCppEmbeddings(emb_path[len(LLAMA_CPP_PREFIX):], os.getenv("EMBEDDER_N_CTX", 512))
    elif API_URL_PREFIX in emb_path:
        return RemoteEmbeddings(emb_path, os.getenv("API_KEY"), os.getenv("EMBEDDER_MODEL_NAME"), os.getenv("EMBEDDER_N_CTX", 512))
    else:
        return TransformerEmbeddings(emb_path)