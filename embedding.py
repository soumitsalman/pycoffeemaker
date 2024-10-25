import logging
from retry import retry
import os
from .utils import truncate
from llama_cpp import Llama
from openai import OpenAI
from abc import ABC, abstractmethod

class Embeddings(ABC):
    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if texts:
            return self.embed(texts)

    def embed_query(self, text: str) -> list[float]:
        if text:
            return self.embed(text)
        
    def __call__(self, input):
        if input:
            return self.embed(input)

    @abstractmethod
    def embed(self, input):
        raise NotImplementedError("Subclass must implement abstract method")

# local embeddings from llama.cpp
class BeansackEmbeddings(Embeddings):
    model_path = None
    context_len = None
    model = None

    def __init__(self, model_path: str, context_len: int):        
        self.model_path = model_path
        self.context_len = context_len    
    
    @retry(tries=2, logger=logging.getLogger("local embedder"))
    def embed(self, input):
        if not self.model:
            self.model = Llama(model_path=self.model_path, n_ctx=self.context_len, n_threads=os.cpu_count(), embedding=True, verbose=False)

        result = self.model.create_embedding(_prep_input(input, self.context_len))
        if isinstance(input, str):
            return result['data'][0]['embedding']
        return [data['embedding'] for data in result['data']]
    
class RemoteEmbeddings(Embeddings):
    openai_client = None
    model_name: str
    context_len: int

    def __init__(self, base_url: str, api_key: str, model_name: str, context_len: int):
        self.openai_client = OpenAI(base_url=base_url, api_key=api_key, max_retries=3, timeout=10)
        self.model_name = model_name
        self.context_len = context_len    
       
    @retry(tries=3, logger=logging.getLogger("remote embedder"))
    def embed(self, input):
        result = self.openai_client.embeddings.create(model=self.model_name, input=_prep_input(input, self.context_len), encoding_format="float")
        if isinstance(input, str):
            return result.data[0].embedding
        return [data.embedding for data in result.data]

def _prep_input(input, context_len):
    if isinstance(input, str):
        return truncate(input, context_len)
    return [truncate(t, context_len) for t in input]
