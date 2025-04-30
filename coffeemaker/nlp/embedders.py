import logging
import threading
from openai import OpenAI
from sentence_transformers import SentenceTransformer
from retry import retry
import os
from abc import ABC, abstractmethod
from .utils import truncate, LLAMA_CPP_PREFIX, API_URL_PREFIX

logger = logging.getLogger(__name__)

CONTEXT_LEN = 512

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
    lock = None
    def __init__(self, model_path: str, context_len: int = CONTEXT_LEN):  
        from llama_cpp import Llama

        self.lock = threading.Lock()
        self.model_path = model_path
        self.context_len = context_len
        self.model = Llama(model_path=self.model_path, n_ctx=self.context_len, n_batch=self.context_len, n_threads_batch=os.cpu_count(), n_threads=os.cpu_count(), embedding=True, verbose=False)
    
    @retry(tries=2, logger=logger)
    def embed(self, input):
        with self.lock:
            result = self.model.create_embedding(_prep_input(input, self.context_len))

        if isinstance(input, str):
            return result['data'][0]['embedding']
        return [data['embedding'] for data in result['data']]
    
class RemoteEmbeddings(Embeddings):
    openai_client = None
    model_name: str
    context_len: int

    def __init__(self, model_name: str, base_url: str, api_key: str, context_len: int):        
        self.openai_client = OpenAI(base_url=base_url, api_key=api_key, max_retries=3, timeout=10)
        self.model_name = model_name
        self.context_len = context_len    
       
    @retry(tries=2, delay=5, logger=logger)
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
    lock = None
    def __init__(self, model_id: str):
        self.lock = threading.Lock()

        import torch
        
        device = "cuda" if torch.cuda.is_available() else "cpu"
        backend = "torch" if torch.cuda.is_available() else "onnx"
        self.model = SentenceTransformer(model_id, trust_remote_code=True, device=device, backend=backend, tokenizer_kwargs=_TOKENIZER_KWARGS)
    
    # TODO: move this out
    # @retry(tries=2, logger=logger)
    def embed(self, input):
        with self.lock:
            result = self.model.encode(input)
        return result.tolist()

def _prep_input(input, context_len):
    if isinstance(input, str):
        return truncate(input, context_len)
    return [truncate(t, context_len) for t in input]

def from_path(
    embedder_path: str, 
    context_len: str,
    base_url: str = None,
    api_key: str = None
) -> Embeddings:
    # initialize digestor
    if embedder_path.startswith(LLAMA_CPP_PREFIX):
        return LlamaCppEmbeddings(embedder_path.removeprefix(LLAMA_CPP_PREFIX), context_len)
    elif base_url:
        return RemoteEmbeddings(embedder_path, base_url, api_key, context_len)
    else:
        return TransformerEmbeddings(embedder_path)