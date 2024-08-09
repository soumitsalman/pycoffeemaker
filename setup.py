from itertools import chain
import json
import os
from dotenv import load_dotenv
from pybeansack import utils

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR+"/.env")
WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)

from pybeansack.embedding import BeansackEmbeddings
from pybeansack.datamodels import *
from pymongo import MongoClient, UpdateOne
from coffeemaker import orchestrator as orch

# categorystore = MongoClient(os.getenv("DB_CONNECTION_STRING"))['espresso']["categories"]
# embedder = BeansackEmbeddings(WORKING_DIR+"/.models/"+os.getenv("EMBEDDER_FILE"), 4096)


def setup_categories():    
    with open("factory_settings.json", 'r') as file:
        categories = json.load(file)['categories']

    embedder = BeansackEmbeddings(orch.embedder_path, 4096)
    orch.categorystore.delete_many({K_SOURCE: "__SYSTEM__"})
    items = chain(*list(map(lambda cat_cluster: [{K_TEXT: cat_cluster, K_DESCRIPTION: desc, K_EMBEDDING:  embedder.embed(f"category/topic: {desc}"), K_SOURCE: "__SYSTEM__"} for desc in categories[cat_cluster]], categories.keys())))
    orch.categorystore.insert_many(items)

def rectify_beans():
    beans = orch.remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL: 1, K_EMBEDDING: 1})
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
                update = {"$unset": {K_CATEGORIES: None}}
            ))
    orch.remotesack.beanstore.bulk_write(updates, False)


orch.initialize(
    os.getenv("DB_CONNECTION_STRING"), 
    WORKING_DIR, 
    os.getenv("EMBEDDER_FILE"),
    os.getenv("GROQ_API_KEY"),    
    float(os.getenv('CLUSTER_EPS')),
    float(os.getenv('CATEGORY_EPS'))
)

setup_categories()
rectify_beans()
