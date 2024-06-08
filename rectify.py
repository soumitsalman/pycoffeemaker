## DATA MODELS ##
from pydantic import BaseModel
from typing import Optional
from enum import Enum

CHANNEL = "social media group/forum"
POST = "social media post"
COMMENT = "social media comment"
ARTICLE = "news article/blog"

class Bean(BaseModel):
    _id: Optional[str] = None
    url: str
    updated: Optional[int] = None
    source: Optional[str] = None
    title: Optional[str] = None
    kind: Optional[str] = None
    text: Optional[str] = None
    author: Optional[str] = None    
    created: Optional[int] = None    
    keywords: Optional[list[str]] = None
    summary: Optional[str] = None
    topic: Optional[str] = None
    embedding: Optional[list[float]] = None
    search_score: Optional[float|int] = None

    def digest(self):
        return f"{self.kind} from {self.source}\nTitle: {self.title}\nBody: {self.text}"

class Nugget(BaseModel):
    _id: Optional[str] = None
    keyphrase: Optional[str] = None
    event: Optional[str] = None
    description: Optional[str] = None
    updated: Optional[int] = None
    embedding: Optional[list[float]] = None
    urls: Optional[list[str]] = None
    trend_score: Optional[int] = None


## LOCAL EMBEDDER ##
from llama_cpp import Llama
import tiktoken
from functools import reduce
from retry import retry
import logging

MODEL_PATH = "models/nomic.gguf"
CTX = 2047
SEARCH_DOCUMENT = "search_document: "
SEARCH_QUERY = "search_query"

embedder = Llama(model_path=MODEL_PATH, embedding=True, verbose=False, n_ctx=CTX)
encoding = tiktoken.get_encoding("cl100k_base")

def embed_documents(input: str|list[str]):
    return _embed(input, SEARCH_DOCUMENT)

def embed_query(input: str|list[str]):    
    return _embed(input, SEARCH_QUERY)

def _embed(input: str|list[str], task_type: str):
    texts = [input] if isinstance(input, str) else input
    try:        
        result = _create_embedding(texts, task_type)
    except:
        result = [None]*len(texts)
    ic(len(result), result[0][:2])
    return result[0] if isinstance(input, str) else result 

@retry(tries=5)
def _create_embedding(texts: str, task_type: str):
    results = [embedder.create_embedding(text)['data'][0]['embedding'] for text in truncate([task_type+t for t in texts])]
    if any(res is None for res in results):
        raise Exception("None value returned by embedder")
    return results

        
def _count_tokens(texts: list[str]) -> int:
    return reduce(lambda a,b: a+b, [len(enc) for enc in encoding.encode_batch(texts)])

def truncate(input: str|list[str]) -> str|list[str]:
    if isinstance(input, str):
        return encoding.decode(encoding.encode(input)[:CTX])
    else:
        return [encoding.decode(enc[:CTX]) for enc in encoding.encode_batch(input)]

def prep_documents(input: str|list[str]):
    texts = [input] if isinstance(input, str) else input
    return truncate([SEARCH_DOCUMENT+t for t in texts])

def prep_query(input: str|list[str]):
    texts = [input] if isinstance(input, str) else input
    return truncate([SEARCH_QUERY+t for t in texts])


## BEAN SACK DB OPERATIONS ##
from pymongo import UpdateOne
from langchain_community.vectorstores import MongoDBAtlasVectorSearch
from langchain_community.embeddings import LlamaCppEmbeddings

BEANSACK = "beansack"
BEANS = "beans"
NUGGETS = "concepts"
NOISES = "noises"

ID="_id"
URL="url"
TEXT = "text"
EMBEDDING = "embedding"
UPDATED = "updated"

class Beansack:
    def __init__(self, conn_str: str):
        self.embedder = LlamaCppEmbeddings(model_path=MODEL_PATH, n_ctx=CTX, verbose=False)
        
        self.beans_vsearch = MongoDBAtlasVectorSearch.from_connection_string(
            conn_str,
            f"{BEANSACK}.{BEANS}",
            embedding=embedder,
            index_name="beans_vector_search",
            embedding_key=EMBEDDING
        )
        self.beanstore = self.beans_vsearch._collection
        
        self.nuggets_vsearch = MongoDBAtlasVectorSearch.from_connection_string(
            conn_str,
            f"{BEANSACK}.{NUGGETS}",
            embedding=embedder,
            index_name="concept_vector_search",
            embedding_key=EMBEDDING
        )
        self.nuggetstore = self.nuggets_vsearch._collection

        self.noises_vsearch = MongoDBAtlasVectorSearch.from_connection_string(
            conn_str,
            f"{BEANSACK}.{NOISES}",
            embedding=embedder,
            index_name="noise_vector_search",
            embedding_key=EMBEDDING
        )
        self.noisestore = self.noises_vsearch._collection

    def rectify(self, last_ndays:int):
        BATCH_SIZE = 10
        update_bean = lambda bean: UpdateOne(
            {URL: bean.url},
            {"$set": {EMBEDDING: embed_documents(bean.digest())}}
        )

        while True:
            # update beans
            beans = list(self.beanstore.find(
                {
                    EMBEDDING: { "$exists": False },
                    UPDATED: { "$gte": get_time(last_ndays) }
                }
            ).limit(BATCH_SIZE))
            if not len(beans):
                break
            ic(self.beanstore.bulk_write([update_bean(Bean(**res)) for res in beans]))            

def get_time(last_ndays: int):
    return int((datetime.now() - timedelta(days=last_ndays)).timestamp())



        
## MAIN FUNC ##
from dotenv import load_dotenv
import os
from icecream import ic
from datetime import datetime, timedelta

load_dotenv()

beansack = Beansack(ic(os.getenv('DB_CONNECTION_STRING')))
beansack.rectify(4)

