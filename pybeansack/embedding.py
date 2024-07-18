from retry import retry
import tiktoken
from .utils import create_logger
from llama_cpp import Llama
from langchain_core.embeddings import Embeddings

EMBEDDER_MODEL_PATH = "models/nomic.gguf"
EMBEDDER_CTX = 2047
SEARCH_DOCUMENT = "search_document"
SEARCH_QUERY = "search_query"

class LocalEmbedder(Embeddings):
    def __init__(self, model_path: str = EMBEDDER_MODEL_PATH):        
        self.model = Llama(model_path=model_path, n_ctx=EMBEDDER_CTX, embedding=True, verbose=False)
       
    def embed_documents(self, texts):
        return self._embed(texts, SEARCH_DOCUMENT)
    
    def embed_query(self, text):    
        return self._embed(text, SEARCH_QUERY)

    # def embed_queries(self, texts: list[str]):    
    #     return self._embed(texts, SEARCH_QUERY)
    
    @retry(tries=5, logger=create_logger("local embedder"))
    def _embed(self, input: str|list[str], task_type: str):
        if input:
            texts = LocalEmbedder._prep_input(input, task_type)
            result = [self.model.create_embedding(text)['data'][0]['embedding'] for text in texts]
            if any(not res for res in result):
                raise Exception("None value returned by embedder")
            return result[0] if isinstance(input, str) else result 
    
    def _prep_input(input: str|list[str], task_type: str):
        texts = [input] if isinstance(input, str) else input
        return [f"{task_type}: {t}" for t in texts]
    
