import os
from dotenv import load_dotenv
from pybeansack import utils
from pybeansack.beansack import Beansack, Localsack
from pybeansack.embedding import BeansackEmbeddings
from persistqueue import Queue
from langchain_groq import ChatGroq
import chromadb
from pymongo import MongoClient


CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR+"/.env")

WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)
CHROMA_DIR=WORKING_DIR+"/.chromadb"
INDEXING_EMBEDDER = WORKING_DIR+"/.models/gte-large-Q4.gguf"
QUERY_EMBEDDER = WORKING_DIR+"/.models/snowflake-arctic-Q4.GGUF"
LOG_FILE = os.getenv('LOG_FILE')
if LOG_FILE:
    utils.set_logger_path(WORKING_DIR+"/"+LOG_FILE)

indexing_embedder = BeansackEmbeddings(INDEXING_EMBEDDER, 4095)
# this is used ONLY FOR clustering
localsack = Localsack(CHROMA_DIR, indexing_embedder)
# this is used ONLY FOR categorizationn
local_categorystore = chromadb.PersistentClient(CHROMA_DIR).get_or_create_collection("categories", embedding_function=indexing_embedder,  metadata={"hnsw:space": "cosine"})
# this is the source of truth
remotesack = Beansack(os.getenv('DB_CONNECTION_STRING'))
remote_categorystore = MongoClient(os.getenv('DB_CONNECTION_STRING'))['espresso']['categories']

llm = ChatGroq(
    api_key=os.getenv('GROQ_API_KEY'), 
    model="llama3-8b-8192", 
    temperature=0.1, 
    verbose=False, 
    streaming=False)

