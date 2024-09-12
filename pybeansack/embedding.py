from retry import retry
import os
from .utils import create_logger, truncate
from llama_cpp import Llama

class BeansackEmbeddings:
    model_path = None
    context_len = None
    model = None

    def __init__(self, model_path: str, context_len: int):        
        self.model_path = model_path
        self.context_len = context_len    
       
    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if texts:
            return self.embed(texts)
    
    def embed_query(self, text: str) -> list[float]:    
        if text:
            return self.embed(text)
        
    def __call__(self, input):
        if input:
            return self.embed(input)
    
    @retry(tries=3, logger=create_logger("local embedder"))
    def embed(self, input):
        if not self.model:
            self.model = Llama(model_path=self.model_path, n_ctx=self.context_len, n_threads=os.cpu_count(), embedding=True, verbose=False)

        result = [self.model.create_embedding(text)['data'][0]['embedding'] for text in self._prep_input(input)]
        if any(not res for res in result):
            raise Exception("None value returned by embedder")
        return result[0] if isinstance(input, str) else result
    
    def _prep_input(self, input):
        texts = [input] if isinstance(input, str) else input
        return [truncate(t, self.context_len) for t in texts]
    
