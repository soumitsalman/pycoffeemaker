import json
import os
from dotenv import load_dotenv
from pybeansack import utils

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR+"/.env")
WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)

from pybeansack.embedding import BeansackEmbeddings
from pybeansack.datamodels import K_SOURCE, K_DESCRIPTION, K_TEXT, K_EMBEDDING
from pymongo import MongoClient

categorystore = MongoClient(os.getenv("DB_CONNECTION_STRING"))['espresso']["categories"]
embedder = BeansackEmbeddings(WORKING_DIR+"/.models/"+os.getenv("EMBEDDER_PATH"), 4096)

def setup_categories():    
    with open("factory_settings.json", 'r') as file:
        categories = json.load(file)['categories']
    cat_names = list(categories.keys())
    cat_desc = list(map(lambda keywords: ", ".join(keywords), categories.values()))
    cat_embs = embedder.embed_documents(cat_desc)
   
    
    categorystore.delete_many({K_SOURCE: "__SYSTEM__"})
    categorystore.insert_many(list(map(
        lambda key, desc, emb: {K_TEXT: key, K_DESCRIPTION: desc, K_EMBEDDING: emb, K_SOURCE: "__SYSTEM__"},
        cat_names, cat_desc, cat_embs)))
    
setup_categories()

# def sync_localsack():
#     local_urls = localsack.beanstore.get(include=[])['ids']
#     beans = remotesack.get_beans(
#         filter={
#             K_URL: {"$nin": local_urls}, 
#             K_TEXT: {"$exists": True}
#         }, 
#         projection={K_URL: 1, K_TEXT: 1})
#     localsack.store_beans(beans)   

# def sync_localcategories():
#     local_categories = local_categorystore.get(include=[])["ids"]
#     categories = remote_categorystore.find(filter={K_TEXT: {"$nin": local_categories}})
#     local_categorystore.upsert(
#         ids=[cat[K_TEXT] for cat in categories], 
#         documents=[cat.get(K_DESCRIPTION, cat[K_TEXT]) for cat in categories],
#         metadatas=[{K_SOURCE: cat.get(K_SOURCE)} for cat in local_categories]
#     )

# def sync_remotecategories():
#     local_categories = local_categorystore.get()
#     remote_categories = [cat[K_ID] for cat in remote_categorystore.find(filter={K_ID: {"$in": local_categories["ids"]}}, projection={K_ID: 1})]    
#     make_cat = lambda i: {**{K_ID: local_categories['ids'][i], K_DESCRIPTION: local_categories['documents'][i]}, **local_categories['metadatas'][i]}
#     remote_categorystore.insert_many([make_cat(i) for i in range(len(local_categories["ids"])) if local_categories['ids'][i] not in remote_categories])

# sync_localsack()