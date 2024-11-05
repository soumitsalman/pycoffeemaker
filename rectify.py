from itertools import chain
import json
import os
import random
import re
from dotenv import load_dotenv
from icecream import ic

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR+"/.env")
WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)

from pybeansack.datamodels import *
from pymongo import DeleteOne, InsertOne, UpdateOne
from coffeemaker import orchestrator as orch

make_id = lambda text: re.sub(r'[^a-zA-Z0-9]', '-', text.lower())

def setup_categories():   
    updates = []
    def _make_category_entry(predecessors, entry):        
        if isinstance(entry, str):
            path = predecessors + [entry]
            id = make_id(entry)
            updates.append({
                K_ID: id,
                K_TEXT: entry, 
                "related": list({make_id(item) for item in path}),
                K_DESCRIPTION: " >> ".join(path), 
                K_EMBEDDING:  orch.remotesack.embedder.embed( "topic: " + (" >> ".join(path))), 
                K_SOURCE: "__SYSTEM__"
            })
            return [id]
        if isinstance(entry, list):
            return list(chain(*(_make_category_entry(predecessors, item) for item in entry)))
        if isinstance(entry, dict):
            res = []
            for key, value in entry.items():
                id = make_id(key)
                related = list(set(_make_category_entry(predecessors + [key], value)))
                updates.append({
                    K_ID: id,
                    K_TEXT: key,
                    "related": related,
                    K_SOURCE: "__SYSTEM__"
                })
                res.extend(related+[id])
            return res    
        
    with open("factory_settings.json", 'r') as file:
        _make_category_entry([], json.load(file)['categories'])
    orch.categorystore.delete_many({K_SOURCE: "__SYSTEM__"})
    orch.categorystore.insert_many(list({item[K_ID]: item for item in updates}.values()))   

def setup_baristas():   
    updates = []
    def make_barista_entry(entry):        
        if isinstance(entry, str):
            barista = {
                K_ID: make_id(entry),
                K_TITLE: entry, 
                K_DESCRIPTION: "Trending news, blogs and posts on {entry}.",
                K_TAGS: [entry],                
                "owner": "__SYSTEM__"
            }
            updates.append(barista)
            return barista[K_TAGS]
        if isinstance(entry, list):
            return list(chain(*(make_barista_entry(item) for item in entry)))
        if isinstance(entry, dict):
            res = []
            for key, value in entry.items():
                barista = {
                    K_ID: make_id(key),
                    K_TITLE: key,
                    K_DESCRIPTION: "Trending news, blogs and posts on {entry}.",
                    K_TAGS: list(set(make_barista_entry(value))) + [key],
                    "owner": "__SYSTEM__"
                }
                updates.append(barista)
                res.extend(barista[K_TAGS])
            return res    
        
    with open("factory_settings.json", 'r') as file:
        make_barista_entry(json.load(file)['categories'])

    # with open(".test/baristas.json", 'w') as file:
    #     json.dump(updates, file)
    # orch.baristastore.delete_many({K_SOURCE: "__SYSTEM__"})
    # orch.baristastore.insert_many(list({item[K_ID]: item for item in updates}.values())) 

def embed_categories():
    cats = orch.categorystore.find({K_EMBEDDING: {"$exists": False}})
    ic(orch.categorystore.bulk_write(
        [UpdateOne(filter={K_ID: cat[K_ID]}, update={"$set": {K_EMBEDDING: orch.remotesack.embedder.embed("topic: " + cat[K_TEXT])}}) for cat in cats],
        ordered=False
    ).modified_count)

def rectify_categories():
    beans = orch.remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL: 1, K_TITLE: 1, K_EMBEDDING: 1})
    updates = []
    for bean in beans:
        cats = orch._find_categories(bean)
        if cats:
            updates.append(UpdateOne(
                filter = {K_URL: bean.url},
                update = {"$set": {K_CATEGORIES: cats}}
            ))
        else:
            updates.append(UpdateOne(
                filter = {K_URL: bean.url},
                update = {"$unset": {K_CATEGORIES: ""}}
            ))
    return orch.remotesack.beanstore.bulk_write(updates, False).modified_count

def rectify_ranking():
    orch.trend_queue.put(orch.remotesack.get_beans(filter={}, projection={K_URL: 1, K_UPDATED: 1, K_TRENDSCORE: 1, K_LIKES: 1, K_COMMENTS: 1}))
    orch.run_trend_ranking()

orch.initialize(
    os.getenv("DB_CONNECTION_STRING"),
    os.getenv("SB_CONNECTION_STRING"), 
    WORKING_DIR, 
    os.getenv("EMBEDDER_PATH"),    
    os.getenv("LLM_BASE_URL"),
    os.getenv("LLM_API_KEY"),
    os.getenv("LLM_MODEL"),
    float(os.getenv('CATEGORY_EPS')),
    float(os.getenv('CLUSTER_EPS'))
)

# _patch_upper_categories()
# setup_categories()
# embed_categories()
# rectify_categories()
# orch.run_clustering()
rectify_ranking()
# setup_baristas()