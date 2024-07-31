from __init__ import *
import json
from pymongo import UpdateOne
from pybeansack.datamodels import *

def setup_categories():    
    with open("factory_settings.json", 'r') as file:
        categories = json.load(file)['categories']
    cat_names = list(categories.keys())
    cat_desc = list(map(lambda keywords: ", ".join(keywords), categories.values()))
    local_categorystore.upsert(
        ids=cat_names, 
        documents=cat_desc,
        metadatas=[{K_SOURCE: "__SYSTEM__"}]*len(categories)
    )
    remote_categorystore.delete_many({K_SOURCE: "__SYSTEM__"})
    remote_categorystore.insert_many(list(map(
        lambda key, desc: {K_TEXT: key, K_DESCRIPTION: desc, K_SOURCE: "__SYSTEM__"},
        cat_names, cat_desc)))
    
def sync_localsack():
    local_urls = localsack.beanstore.get(include=[])['ids']
    beans = remotesack.get_beans(
        filter={
            K_URL: {"$nin": local_urls}, 
            K_TEXT: {"$exists": True}
        }, 
        projection={K_URL: 1, K_TEXT: 1})
    localsack.store_beans(beans)   

def sync_localcategories():
    local_categories = local_categorystore.get(include=[])["ids"]
    categories = remote_categorystore.find(filter={K_TEXT: {"$nin": local_categories}})
    local_categorystore.upsert(
        ids=[cat[K_TEXT] for cat in categories], 
        documents=[cat.get(K_DESCRIPTION, cat[K_TEXT]) for cat in categories],
        metadatas=[{K_SOURCE: cat.get(K_SOURCE)} for cat in local_categories]
    )

# def sync_remotecategories():
#     local_categories = local_categorystore.get()
#     remote_categories = [cat[K_ID] for cat in remote_categorystore.find(filter={K_ID: {"$in": local_categories["ids"]}}, projection={K_ID: 1})]    
#     make_cat = lambda i: {**{K_ID: local_categories['ids'][i], K_DESCRIPTION: local_categories['documents'][i]}, **local_categories['metadatas'][i]}
#     remote_categorystore.insert_many([make_cat(i) for i in range(len(local_categories["ids"])) if local_categories['ids'][i] not in remote_categories])

setup_categories()
sync_localsack()