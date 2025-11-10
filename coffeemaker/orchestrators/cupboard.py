import os
import chromadb
from chromadb import Documents, EmbeddingFunction, Embeddings
from pydantic import BaseModel, Field
from typing import Optional
import numpy as np
from coffeemaker.nlp import embedders
from coffeemaker.pybeansack.models import K_EMBEDDING, K_ID

class Sip(BaseModel):
    id: str = Field(..., description="This is the slug")    
    title: Optional[str] = Field(None, description="This is the title")
    content: Optional[str] = Field(None, description="This is the content")
    embedding: Optional[list[float]] = Field(None, description="These are the embeddings")
    created: Optional[str] = Field(None, description="This is the created timestamp")
    beans: Optional[list[str]] = Field(None, description="These are the urls to the beans")

class Mug(BaseModel):
    id: str = Field(..., description="This is the slug")    
    title: Optional[str] = Field(None, description="This is the title")
    content: Optional[str] = Field(None, description="This is the content")
    embedding: Optional[list[float]] = Field(None, description="These are the embeddings")
    created: Optional[str] = Field(None, description="This is the created timestamp")
    updated: Optional[str] = Field(None, description="This is the updated timestamp")
    sips: Optional[list[str]] = Field(None, description="These are the slugs to the sips/sections")
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
        docs = [doc_template(item)]
        metadatas = [item.model_dump(exclude={K_EMBEDDING, K_ID})]
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