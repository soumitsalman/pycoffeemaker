from itertools import chain
import json
import os
import random
from dotenv import load_dotenv
from icecream import ic

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR+"/.env")
WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)

from pybeansack.datamodels import *
from pymongo import DeleteOne, InsertOne, UpdateOne
from coffeemaker import orchestrator as orch

category_id = lambda entry: entry.lower().replace(" & ", "-").replace(" ", "-").replace("&", "").replace("(", "").replace(")", "").replace("/", "-").replace("\\", "-")

def setup_categories():   
    updates = []
    def _make_category_entry(predecessors, entry):        
        if isinstance(entry, str):
            path = predecessors + [entry]
            id = category_id(entry)
            updates.append({
                K_ID: id,
                K_TEXT: entry, 
                "related": list({category_id(item) for item in path}),
                K_DESCRIPTION: " >> ".join(path), 
                K_EMBEDDING:  orch.embedder.embed( "topic: " + (" >> ".join(path))), 
                K_SOURCE: "__SYSTEM__"
            })
            return [id]
        if isinstance(entry, list):
            return list(chain(*(_make_category_entry(predecessors, item) for item in entry)))
        if isinstance(entry, dict):
            res = []
            for key, value in entry.items():
                id = category_id(key)
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
    orch.trend_queue.put(orch.remotesack.get_beans(filter={}))
    orch.run_trend_ranking()

orch.initialize(
    os.getenv("DB_CONNECTION_STRING"), 
    WORKING_DIR, 
    os.getenv("EMBEDDER_FILE"),
    os.getenv("GROQ_API_KEY"),   
    float(os.getenv('CATEGORY_EPS'))
)

# _patch_upper_categories()
setup_categories()
# rectify_categories()
# orch.run_clustering()
# rectify_ranking()
