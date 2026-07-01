import logging
import threading
import os
import numpy as np
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_random
from .runtime import *
from icecream import ic

log = logging.getLogger(__name__)

try: import torch
except: log.warning("PyTorch Not Available", extra={'source': __file__, 'num_items': 1})

VECTOR = list[float]

is_cuda_usable = lambda: torch.cuda.is_available()

# def is_cuda_usable() -> bool:
#     if not torch.cuda.is_available():
#         return False
#     try:
#         return torch.cuda.get_device_capability()[0] >= 7
#     except Exception:
#         return False

class EmbedderBase(ABC):
    splitter = None
    context_len: int = None
    tokenizer_fn = None
    # _SPECIAL_TOKEN_MARGIN = 8  # BOS/EOS/CLS/SEP overhead; chunk_size = context_len - this
    # _OVERLAP_MARGIN = 20  # to ensure that we don't lose important context when merging chunk embeddings

    def __init__(self, context_len: int, tokenizer_fn=None):
        self.context_len = context_len
        self.tokenizer_fn = tokenizer_fn

    def _split(self, text: str):
        # NOTE: moving the import inside the function so that there is no need to install llama-index if the embedder is used only for small texts
        from llama_index.core.text_splitter import TokenTextSplitter
        if not self.splitter:
            self.splitter = TokenTextSplitter(
                chunk_size=self.context_len,
                chunk_overlap=TOKEN_MARGIN<<1,
                tokenizer=self.tokenizer_fn,
                include_metadata=False,
                include_prev_next_rel=False,
            )
        chunks = self.splitter.split_text(text)
        if len(chunks) > 1 and len(chunks[-1]) < (TOKEN_MARGIN<<2): chunks = chunks[:-1]
        return chunks

    def _create_chunks(self, texts: list[str]) -> tuple[list[str], list[int], list[int]]:
        texts = texts if isinstance(texts, list) else [texts]
        
        chunks = list(map(self._split, texts)) # NOTE: batch running will mess this up
        counts = list(map(len, chunks))
        start_idx = [0]*len(chunks)
        for i in range(1,len(counts)):
            start_idx[i] = start_idx[i-1]+counts[i-1]
        return list(chain(*chunks)), start_idx, counts
    
    def _merge_chunks(self, embeddings, start_idx: list[int], counts: list[int]):
        merged_embeddings = lambda start, count: np.mean(embeddings[start:start+count], axis=0).tolist()
        return list(map(merged_embeddings, start_idx, counts))

    @abstractmethod
    def _embed(self, texts: str|list[str]):
        raise NotImplementedError("Subclass must implement abstract method")
    
    def embed_documents(self, texts: str|list[str]) -> VECTOR|list[VECTOR]:
        """Takes a list of strings/large documents as input and chunks them into smaller pieces as needed based on the context length.
        For each document it returns a mean of the embeddings of the chunks."""
        if not texts: return
        
        chunks, start_idx, counts = self._create_chunks(texts)
        embeddings = self._embed(chunks)
        embeddings = self._merge_chunks(embeddings, start_idx, counts)
        return embeddings[0] if isinstance(texts, str) else embeddings
    
    def embed_query(self, query: str) -> VECTOR:
        """Embeds a single string as a query. It prepends `query: ` to the input. 
        It processes the string without chunking or truncation for faster response."""
        if not query: return
        vec = self._embed("query: "+query)
        return vec if isinstance(vec, list) else vec.tolist()        
   
    def __call__(self, texts: str|list[str]):
        """This takes a string or an list of strings as an input.
        This calls the embedder directly without chunking or truncation for faster response"""
        if texts: 
            res = self._embed(texts)
            if hasattr(res, "tolist"): return res.tolist()
            return res

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

class RemoteEmbeddings(EmbedderBase):
    model_client = None
    model_name: str
    context_len: int

    def __init__(self, model_name: str, base_url: str, api_key: str, context_len: int): 
        from openai import OpenAI

        super().__init__(context_len)
        self.model_client = OpenAI(base_url=base_url, api_key=api_key, max_retries=3, timeout=10)
        self.model_name = model_name
        self.context_len = context_len    
       
    @retry(stop=stop_after_attempt(REMOTE_RETRY_COUNT), wait=wait_random(*REMOTE_RETRY_JITTER), reraise=True)
    def _embed(self, texts):
        embeddings = self.model_client.embeddings.create(model=self.model_name, input=texts, encoding_format="float")
        return [data.embedding for data in embeddings.data]    

# local embeddings from llama.cpp
class LlamaCppEmbeddings(EmbedderBase):
    model_path = None
    context_len = None
    _model = None
    lock = None
    def __init__(self, model_path: str, context_len: int): 
        super().__init__(context_len)
        self.lock = threading.Lock()
        self.model_path = model_path
        self.context_len = context_len

    def _embed(self, texts):
        with self.lock:
            embeddings = self.model.create_embedding(texts)
        if isinstance(texts, str): return embeddings['data'][0]['embedding']
        return [data['embedding'] for data in embeddings['data']]
    
    @property
    def model(self):
        if not self._model:
            from llama_cpp import Llama
            n_threads = min(1, os.cpu_count()-1)
            self._model = Llama(model_path=self.model_path, n_ctx=self.context_len, n_threads_batch=n_threads, n_threads=n_threads, embedding=True, verbose=False)
        return self._model

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._model:
            del self._model
            self._model = None
        return False
        
class TransformerEmbeddings(EmbedderBase):
    _model = None
    model_path = None
    tokenizer_kwargs = None

    def __init__(self, model_path: str, context_len: int):
        from transformers import AutoTokenizer

        super().__init__(context_len, tokenizer_fn=AutoTokenizer.from_pretrained(model_path, truncation=False, use_fast=True).encode)
        self.model_path = model_path
        self.tokenizer_kwargs = {
            "truncation": True,
            "max_length": context_len,
            "padding": True
        }
        self.device = "cuda" if is_cuda_usable() else "cpu"
        self._model = None

    def _embed(self, texts: str|list[str]):
        if not self._model: self.__enter__()
        with torch.inference_mode(), torch.no_grad():
            embs = self._model.encode(texts, batch_size=len(texts), convert_to_numpy=True)
        return embs    
    
    def __enter__(self):
        if not self._model:         
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_path, processor_kwargs=self.tokenizer_kwargs, device=self.device)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._model:
            self._model = None
        clear_gpu_cache()
        return False
    
class OVEmbeddings(EmbedderBase):
    _model = None
    model_path = None
    context_len = None

    def __init__(self, model_path: str, context_len: int):
        from transformers import AutoTokenizer

        _tokenizer = AutoTokenizer.from_pretrained(model_path, max_length=context_len, use_fast=True)
        super().__init__(context_len, tokenizer_fn=_tokenizer.encode)
        self.model_path = model_path
        self.context_len = context_len

    def _embed(self, texts: str|list[str]):
        with torch.no_grad(), torch.inference_mode():
            embs = self.model.encode(texts, batch_size=len(texts), convert_to_numpy=True)
        return embs
    
    @property
    def model(self):
        if not self._model:
            raise ImportError(
                "openvino:// embedders require optimum-intel (removed). "
                "Use infinity:// for local embeddings instead."
            )
        return self._model

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._model:
            del self._model
            self._model = None
        return False
    
class ORTEmbeddings(EmbedderBase):
    def __init__(self, model_path: str, context_len: int):
        from transformers import AutoTokenizer

        super().__init__(context_len, tokenizer_fn=AutoTokenizer.from_pretrained(model_path, truncation=False, use_fast=True).encode)
        self.model_path = model_path
        self.tokenizer_kwargs = {
            "truncation": True,
            "max_length": context_len,
            "padding": True
        }
        self.device = "CUDAExecutionProvider" if is_cuda_usable() else "CPUExecutionProvider"
        self._model = None

    def _embed(self, texts: str|list[str]):
        with torch.inference_mode(), torch.no_grad():
            embs = self._model.encode(texts, batch_size=len(texts), convert_to_numpy=True)
        return embs

    def __enter__(self):
        if not self._model:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(
                self.model_path, 
                backend="onnx",
                model_kwargs={"provider": self.device, "file_name": "onnx/model.onnx"},
                processor_kwargs=self.tokenizer_kwargs,
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._model: 
            self._model = None
        clear_gpu_cache()
        return False

class VLLMEmbedder(EmbedderBase):
    def __init__(self, model_path: str, context_len: int):
        from transformers import AutoTokenizer

        _tokenizer = AutoTokenizer.from_pretrained(model_path, max_length=context_len, use_fast=True)
        super().__init__(context_len, tokenizer_fn=_tokenizer.encode)
        self.model_path = model_path
        self._llm = None

    def _embed(self, texts: str|list[str]):
        embs = self._llm.embed(texts, use_tqdm=False)
        return [emb.outputs.embedding for emb in embs]
    
    def __enter__(self):
        if not self._llm:
            from vllm import LLM
            self._llm = LLM(model=self.model_path)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._llm:
            del self._llm
            self._llm = None            
        clear_gpu_cache()
        return False


class InfinityEmbeddings(EmbedderBase):
    _engine = None
    model_path = None
    _model_name = None

    def __init__(self, model_path: str, context_len: int):
        from transformers import AutoTokenizer

        super().__init__(
            context_len,
            tokenizer_fn=AutoTokenizer.from_pretrained(
                model_path, truncation=False, use_fast=True
            ).encode,
        )
        self.model_path = model_path
        self.engine = "torch"
        self.device = "cuda" if is_cuda_usable() else "cpu"
        self.batch_size = int(os.getenv("INFINITY_BATCH_SIZE", 32))

    def _embed(self, texts: str | list[str]):
        single = isinstance(texts, str)
        if single:
            texts = [texts]
        embeddings, _usage = self._engine.embed(model=self._model_name, sentences=texts).result()
        arr = np.asarray(embeddings, dtype=np.float32)
        return arr[0] if single else arr

    def __enter__(self):
        if not self._engine:
            from infinity_emb import EngineArgs, SyncEngineArray

            engine_args = EngineArgs(
                model_name_or_path=self.model_path,
                engine=self.engine,
                device=self.device,
                embedding_dtype="float32",
                dtype="bfloat16" if self.device == "cuda" else "auto",
                batch_size=self.batch_size,
                model_warmup=True,
                bettertransformer=False,
            )
            self._model_name = engine_args.served_model_name
            self._engine = SyncEngineArray.from_args([engine_args])
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._engine:
            self._engine.stop()
            self._engine = None
        clear_gpu_cache()
        return False


def create_embedder(
    model_path: str, 
    context_len: int = 512,
    base_url: str = None,
    api_key: str = None
) -> EmbedderBase:
    # initialize embedder
    if base_url: return RemoteEmbeddings(model_path, base_url, api_key, context_len)
    if model_path.startswith(LLAMACPP_PREFIX): return LlamaCppEmbeddings(model_path.removeprefix(LLAMACPP_PREFIX), context_len)
    if model_path.startswith(OPENVINO_PREFIX): return OVEmbeddings(model_path.removeprefix(OPENVINO_PREFIX), context_len)
    if model_path.startswith(ONNX_PREFIX): return ORTEmbeddings(model_path.removeprefix(ONNX_PREFIX), context_len)
    if model_path.startswith(VLLM_PREFIX): return VLLMEmbedder(model_path.removeprefix(VLLM_PREFIX), context_len)
    if model_path.startswith(INFINITY_PREFIX): return InfinityEmbeddings(model_path.removeprefix(INFINITY_PREFIX), context_len)
    return TransformerEmbeddings(model_path, context_len)

    




    

