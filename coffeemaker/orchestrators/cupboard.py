import os
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import register, TextEmbeddingFunction, get_registry
from functools import cached_property
from datetime import datetime, timedelta
from pydantic import Field
from typing import Optional
import numpy as np
from coffeemaker.nlp import embedders
from coffeemaker.pybeansack.models import K_EMBEDDING, K_ID, VECTOR_LEN
from icecream import ic

# @register("cafecito-embedding-adapter")
# class EmbeddingAdapter(TextEmbeddingFunction):
#     model_path: str = None
#     context_len: int = None

#     def compute_query_embeddings(self, query: str, *args, **kwargs):
#         return self.embedder._embed(query)

#     def generate_embeddings(self, texts: list[str], *args, **kwargs):
#         response = self.embedder.embed_documents(list(texts))
#         return [np.array(embedding, dtype=np.float32) for embedding in response]
    
#     def ndims(self) -> int:        
#         return VECTOR_LEN

#     @cached_property
#     def embedder(self):
#         return embedders.from_path(self.model_path, self.context_len)
    
class CupboardModel(LanceModel):
    id: str = Field(...)
    title: Optional[str] = Field(None, description="This is the title")
    content: Optional[str] = Field(None, description="This is the content")
    embedding: Vector(VECTOR_LEN, nullable=True) = Field(None, description="This is the embedding vector of title+content")
    created: Optional[datetime] = Field(None, description="This is the created timestamp")
    updated: Optional[datetime] = Field(None, description="This is the updated timestamp")

class Sip(CupboardModel):
    past_sips: Optional[list[str]] = Field(None, description="These are the slugs to related past sips")
    source_beans: Optional[list[str]] = Field(None, description="These are the urls to the beans")

class Mug(CupboardModel):
    sips: Optional[list[str]] = Field(None, description="These are the slugs to the sips/sections")
    highlights: Optional[list[str]] = Field(None, description="These are the highlights")   
    tags: Optional[list[str]] = Field(None, description="These are the tags")
  
class CupboardDB: 
    db = None
    allmugs = None
    allsips = None
    embedder: embedders.Embeddings = None

    def __init__(self, db_path: str):
        self.db = lancedb.connect(uri=db_path, read_consistency_interval = timedelta(hours=1))
        self.allmugs = self.db.create_table("mugs", schema=Mug, exist_ok=True)
        self.allsips = self.db.create_table("sips", schema=Sip, exist_ok=True)
        self.embedder = embedders.from_path(model_path=os.getenv("EMBEDDER_PATH"), context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN")))

    def _rectify(self, items: list[Mug]|list[Sip]):
        get_content = lambda item: f"{item.title}\n\n{item.content}"
        contents = [get_content(item) for item in items]
        vecs = self.embedder.embed_documents(contents)
        for item, vec in zip(items, vecs):
            item.embedding = vec
        return items

    def add(self, items: Mug|Sip|list[Mug]|list[Sip]):
        to_store = items if isinstance(items, list) else [items]
        to_store = self._rectify(to_store)
        if isinstance(to_store[0], Mug): self.allmugs.add(to_store)
        elif isinstance(to_store[0], Sip): self.allsips.add(to_store)

    def query_sips(self, query_text: str, distance: float = 0, limit: int = 5):
        return self.allsips.search(self.embedder.embed_query(query_text)).limit(limit).to_pydantic(Sip)