import os
import chromadb
from chromadb import Documents, EmbeddingFunction, Embeddings
from datetime import datetime
from pydantic import BaseModel, Field, field_serializer, field_validator
from typing import Optional
import numpy as np
from coffeemaker.nlp import embedders
from coffeemaker.pybeansack.models import K_EMBEDDING, K_ID
from icecream import ic


class CupboardBaseModel(BaseModel):
    id: str = Field(..., description="This is the slug")
    title: Optional[str] = Field(None, description="This is the title")
    content: Optional[str] = Field(None, description="This is the content")
    created: Optional[datetime] = Field(None, description="This is the created timestamp")
    updated: Optional[datetime] = Field(None, description="This is the updated timestamp")

    @field_validator('created', 'updated', mode='before')
    @classmethod
    def parse_created(cls, v):
        if v is None: return None
        if isinstance(v, (int, float)): return datetime.fromtimestamp(v)
        return v

    @field_serializer('created', 'updated')
    def serialize_created(self, dt: Optional[datetime], _info):
        return dt.timestamp() if dt else None

    class Config:
        arbitrary_types_allowed = False
        exclude = {K_EMBEDDING, K_ID}
        exclude_none = True
        

class Sip(CupboardBaseModel):
    beans: Optional[list[str]] = Field(None, description="These are the urls to the beans")
    related: Optional[list[str]] = Field(None, description="These are the slugs to related past sips")

class Mug(CupboardBaseModel):
    sips: Optional[list[str]] = Field(None, description="These are the slugs to the sips/sections")
    highlights: Optional[list[str]] = Field(None, description="These are the highlights")   
    tags: Optional[list[str]] = Field(None, description="These are the tags")
  

class EmbeddingAdapter(EmbeddingFunction):
    embedder = None

    def __init__(self, model_path: str, context_len: int):
        self.embedder = embedders.from_path(model_path, context_len)

    def embed_query(self, input):
        return self.embedder._embed(input)

    def __call__(self, input: Documents) -> Embeddings:
        response = self.embedder.embed_documents(list(input))
        return [np.array(embedding, dtype=np.float32) for embedding in response]
    
    def name(self) -> str:
        return "cafecito-embedding-adapter"

doc_template = lambda item: f"# {item.title}\n\n{item.content}"

class CupboardDB: 
    db = None
    allmugs = None
    allsips = None

    def __init__(self, db_path: str):
        self.db = chromadb.PersistentClient(path=db_path)
        em_function = EmbeddingAdapter(model_path=os.getenv("EMBEDDER_PATH"), context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN")))
        self.allmugs = self.db.get_or_create_collection(name="mugs", embedding_function=em_function)
        self.allsips = self.db.get_or_create_collection(name="sips", embedding_function=em_function)  

    def add(self, item: Mug|Sip):
        from icecream import ic
        docs = ic([doc_template(item)])
        metadatas = ic([item.model_dump(exclude=[K_ID], exclude_none=True)])
        ids = [item.id]
        if isinstance(item, Mug):
            self.allmugs.add(
                documents=docs,
                metadatas=metadatas,
                ids=ids
            )
        elif isinstance(item, Sip):
            self.allsips.add(
                documents=docs,
                metadatas=metadatas,
                ids=ids
            )

    def add_sips(self, sips: list[Sip]):
        self.allsips.add(
            documents=[doc_template(sip) for sip in sips],
            metadatas=[sip.model_dump(exclude={K_EMBEDDING, K_ID}) for sip in sips],
            ids=[sip.id for sip in sips]
        )

    def query_sips(self, query_text: str, distance: float = 0, limit: int = 5):
        from icecream import ic
        ic(self.allsips.get(limit=5))
        result = ic(self.allsips.query(
            query_texts=[query_text],
            n_results=limit,
            include=["metadatas", "distances"]
        ))
        return [Sip(id=id, **metadata) for id, metadata, d in zip(result.ids[0], result.metadatas[0], result.distances[0]) if d <= distance]
