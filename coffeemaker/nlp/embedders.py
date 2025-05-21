import logging
import random
import threading
import os
import torch
import numpy as np
from abc import ABC, abstractmethod
from llama_index.core.text_splitter import SentenceSplitter
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from retry import retry
from .utils import LLAMA_CPP_PREFIX

logger = logging.getLogger(__name__)

CONTEXT_LEN = 512
MAX_CHUNKS = 4 # this is an approximation assuming that getting 4 (max) chunks are good enough gauge for generating embedding

class _TextSplitter:
    splitter = None

    def __init__(self, context_len: int):
        self.splitter = SentenceSplitter.from_defaults(
            chunk_size=context_len, 
            chunk_overlap=0, 
            paragraph_separator="\n", 
            include_metadata=False, 
            include_prev_next_rel=False
        )

    def _split(self, text: str):
        splits = self.splitter.split_text(text)
        return random.sample(splits, k=min(len(splits), MAX_CHUNKS))        

    def split_texts(self, texts: list[str]) -> tuple[list[str], list[int], list[int]]:
        texts = texts if isinstance(texts, list) else [texts]
        
        chunks = list(map(self._split, texts))
        counts = list(map(len, chunks))
        start_idx = [0]*len(chunks)
        for i in range(1,len(counts)):
            start_idx[i] = start_idx[i-1]+counts[i-1]
        return list(chain(*chunks)), start_idx, counts
    
    def merge_embeddings(self, embeddings, start_idx: list[int], counts: list[int]):
        merged_embeddings = lambda start, count: np.median(embeddings[start:start+count], axis=0).tolist()
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            embeddings = list(executor.map(merged_embeddings, start_idx, counts))
        return embeddings   

class Embeddings(ABC):
    splitter: _TextSplitter = None

    def __init__(self, context_len: int):
        self.splitter = _TextSplitter(context_len)    

    @abstractmethod
    def _embed(self, texts: list[str]):
        raise NotImplementedError("Subclass must implement abstract method")
    
    def embed(self, texts: str|list[str]):
        chunks, start_idx, counts = self.splitter.split_texts(texts)
        embeddings = self._embed(chunks)
        embeddings = self.splitter.merge_embeddings(embeddings, start_idx, counts)
        return embeddings[0] if isinstance(texts, str) else embeddings
    
    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if texts: return self.embed(texts)

    def embed_query(self, query: str) -> list[float]:
        if query: return self.embed("query: "+query)
        
    def __call__(self, texts: str|list[str]):
        if texts: return self.embed(texts)


# local embeddings from llama.cpp
class LlamaCppEmbeddings(Embeddings):
    model_path = None
    context_len = None
    model = None
    lock = None
    def __init__(self, model_path: str, context_len: int):  
        from llama_cpp import Llama

        super().__init__(context_len)
        self.lock = threading.Lock()
        self.model_path = model_path
        self.context_len = context_len
        self.model = Llama(model_path=self.model_path, n_ctx=self.context_len, n_batch=self.context_len, n_threads_batch=os.cpu_count(), n_threads=os.cpu_count(), embedding=True, verbose=False)
    
    def _embed(self, texts: list[str]):
        with self.lock:
            embeddings = self.model.create_embedding(texts)
        return [data['embedding'] for data in embeddings['data']]

class RemoteEmbeddings(Embeddings):
    openai_client = None
    model_name: str
    context_len: int

    def __init__(self, model_name: str, base_url: str, api_key: str, context_len: int): 
        from openai import OpenAI

        super().__init__(context_len)
        self.openai_client = OpenAI(base_url=base_url, api_key=api_key, max_retries=3, timeout=10)
        self.model_name = model_name
        self.context_len = context_len    
       
    @retry(tries=2, delay=5, logger=logger)
    def _embed(self, texts):
        embeddings = self.openai_client.embeddings.create(model=self.model_name, input=texts, encoding_format="float")
        return [data.embedding for data in embeddings.data]
    
class TransformerEmbeddings(Embeddings):
    model = None
    tokenizer = None
    context_len: int = None
    lock = None
    splitter = None

    def __init__(self, model_path: str, context_len: int):
        from optimum.onnxruntime import ORTModelForFeatureExtraction
        from transformers import AutoTokenizer

        super().__init__(context_len)
        # self.lock = threading.Lock()
        provider = "CUDAExecutionProvider" if torch.cuda.is_available() else "CPUExecutionProvider"
        self.model = ORTModelForFeatureExtraction.from_pretrained(model_path, provider=provider)
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        self.context_len = context_len

        # tokenizer_kwargs = {
        #     "truncation": True,
        #     "max_length": context_len
        # }
        # NOTE: temporarily disabling this
        # self.model = SentenceTransformer(model_id, trust_remote_code=True, device="cuda", tokenizer_kwargs=tokenizer_kwargs) \
        #     if torch.cuda.is_available() else \
        #     SentenceTransformer(model_id, trust_remote_code=True, backend="onnx", model_kwargs={"file_name": "model_quantized.onnx"}, tokenizer_kwargs=tokenizer_kwargs)
        # self.model = SentenceTransformer(model_id, trust_remote_code=True, device="cpu", backend="onnx", model_kwargs={"file_name": "model_quantized.onnx"}, tokenizer_kwargs=tokenizer_kwargs)
        # self.model = SentenceTransformer(model_path, trust_remote_code=True, tokenizer_kwargs=tokenizer_kwargs)

    def _embed(self, texts: str|list[str]):
        # with self.lock:
        #     embeddings = self.model.encode(texts, batch_size=len(texts))
        # return embeddings
        inputs = self.tokenizer(texts, return_tensors='pt', padding=True, max_length=self.context_len, truncation=True)
        outputs = self.model(**inputs)
        return outputs.last_hidden_state.mean(dim=1).detach().tolist()

# def _prep_input(input, context_len):
#     if isinstance(input, str):
#         return truncate(input, context_len)
#     return [truncate(t, context_len) for t in input]

def from_path(
    embedder_path: str, 
    context_len: int,
    base_url: str = None,
    api_key: str = None
) -> Embeddings:
    # initialize digestor
    if embedder_path.startswith(LLAMA_CPP_PREFIX):
        return LlamaCppEmbeddings(embedder_path.removeprefix(LLAMA_CPP_PREFIX), context_len)
    elif base_url:
        return RemoteEmbeddings(embedder_path, base_url, api_key, context_len)
    else:
        return TransformerEmbeddings(embedder_path, context_len)
    




    

