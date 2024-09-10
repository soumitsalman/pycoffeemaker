from itertools import chain
import json
import os
from dotenv import load_dotenv
from icecream import ic

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR+"/.env")
WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)

from pybeansack.embedding import BeansackEmbeddings
from pybeansack.datamodels import *
from pymongo import UpdateOne
from coffeemaker import orchestrator as orch


def setup_categories():    
    with open("factory_settings.json", 'r') as file:
        categories = json.load(file)['categories']

    def _make_category_entry(predecessors, entry):
        if isinstance(entry, str):
            path = predecessors + [entry]
            return [
                {
                    K_TEXT: entry, 
                    K_CATEGORIES: path,
                    K_DESCRIPTION: " > ".join(path), 
                    K_EMBEDDING:  orch.embedder.embed( "topic: " + (" > ".join(path))), 
                    K_SOURCE: "__SYSTEM__"
                }
            ]
        elif isinstance(entry, list):
            return list(chain(*(_make_category_entry(predecessors, item) for item in entry)))
        else: # it is a dict
            return list(chain(*(_make_category_entry(predecessors+[key], value) for key, value in entry.items())))    

    items = _make_category_entry([], categories)
    orch.categorystore.delete_many({K_SOURCE: "__SYSTEM__"})
    orch.categorystore.insert_many(items)

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
# setup_categories()
# rectify_categories()
# orch.run_clustering()
rectify_ranking()
