import asyncio
from dotenv import load_dotenv
load_dotenv()

from itertools import chain
import json
import os
import re
from icecream import ic
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from coffeemaker.collectors import ychackernews, redditor
from coffeemaker.pybeansack.models import *
from coffeemaker.orchestrator import Orchestrator

K_RELATED = "related"
LIMIT=40000

ndays_ago = lambda n: (datetime.now() - timedelta(days=n))
make_id = lambda text: re.sub(r'[^a-zA-Z0-9]', '-', text.lower())
create_orch = lambda: Orchestrator(
    os.getenv("REMOTE_DB_CONNECTION_STRING"),
    os.getenv("LOCAL_DB_PATH"),
    os.getenv("AZSTORAGE_CONNECTION_STRING"), 
    os.getenv("EMBEDDER_PATH"),    
    os.getenv("LLM_PATH"),
    float(os.getenv('CLUSTER_EPS')))

def setup_categories():   
    updates = []
    def _make_category_entry(predecessors, entry):        
        if isinstance(entry, str):
            path = predecessors + [entry]
            id = make_id(entry)
            updates.append({
                K_ID: id,
                K_TEXT: entry, 
                K_RELATED: list({make_id(item) for item in path}),
                K_DESCRIPTION: " >> ".join(path), 
                K_EMBEDDING:  orch.remotesack.embedder.embed( "category: " + (" >> ".join(path))), 
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
                    K_RELATED: related,
                    K_SOURCE: "__SYSTEM__"
                })
                res.extend(related+[id])
            return res    
        
    with open("factory_settings.json", 'r') as file:
        _make_category_entry([], json.load(file)['categories'])
    # orch.categorystore.delete_many({K_SOURCE: "__SYSTEM__"})
    # orch.categorystore.insert_many(list({item[K_ID]: item for item in updates}.values()))   
    orch.localsack.store_categories(list({item[K_ID]: item for item in updates}.values()))

def setup_baristas():   
    baristas = MongoClient(os.getenv("DB_CONNECTION_STRING"))['espresso']['baristas']
    updates = [
        {
            K_ID: "hackernews", 
            K_TITLE: "Hackernews (by Y Combinator)", 
            K_DESCRIPTION: "News, blogs and posts shared in Y Combinator's Hackernews.", 
            K_SOURCE: ychackernews.YC,
            "owner": "__SYSTEM__"
        },
        {
            K_ID: "reddit", 
            K_TITLE: "Reddit", 
            K_DESCRIPTION: "News, blogs and posts shared in Reddit.", 
            K_SOURCE: redditor.REDDIT,
            "owner": "__SYSTEM__"
        }
    ]
    # updates = []
    # def make_barista_entry(entry) -> list[str]:        
    #     if isinstance(entry, str):
    #         return [entry]
    #     if isinstance(entry, list):
    #         return list(chain(*(make_barista_entry(item) for item in entry)))
    #     if isinstance(entry, dict):
    #         res = []
    #         for key, value in entry.items():
    #             tags = list(set([key] + make_barista_entry(value)))
    #             barista = {
    #                 K_ID: make_id(key),
    #                 K_TITLE: key,
    #                 K_DESCRIPTION: f"News, blogs and posts on {', '.join(tags)}.",
    #                 K_TAGS: tags,
    #                 K_EMBEDDING: orch.remotesack.embedder.embed(f"News, blogs and posts on domain/genre/topics such as {', '.join(tags)}."),
    #                 "owner": "__SYSTEM__"
    #             }
    #             updates.append(barista)
    #             res.extend(tags)
    #         return res    
        
    # with open("factory_settings.json", 'r') as file:
    #     make_barista_entry(json.load(file)['categories'])

    # with open(".test/baristas.json", 'w') as file:
    #     json.dump(updates, file)
    
    # baristas.delete_many({"owner": "__SYSTEM__"})
    baristas.insert_many(list({item[K_ID]: item for item in updates}.values())) 

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
    orch.trend_rank_beans()

def port_categories_to_localsack():
    orch.localsack.store_categories(list(orch.categorystore.find()))


def port_beans_to_localsack():
    orch = create_orch()
    beans = orch.remotesack.get_beans(
        filter={
            K_EMBEDDING: {"$exists": True},
            K_COLLECTED: {"$gte": ndays_ago(30)}
        }
    )
    print(datetime.now(), "porting beans|%d", len(beans))
    BATCH_SIZE = 2000
    
    async def store_locally(beans: list[Bean]):
        tasks = [asyncio.to_thread(orch.localsack.store_beans, beans[i:i+BATCH_SIZE]) for i in range(0, len(beans), BATCH_SIZE)]
        await asyncio.gather(*tasks) 
           
    asyncio.run(store_locally(beans))   
    orch.close()
    
    print(datetime.now(), "finished porting beans")

def port_chatters_to_localsack():
    orch = create_orch()
    
    print("finished porting beans")

def port_chatters_to_localsack():
    chatters = []
    items = list(orch.remotesack.chatterstore.find(filter={K_UPDATED: {"$exists": True}}, sort={K_UPDATED: -1}))
    print("porting chatters|%d", len(items))
    for item in items:
        if isinstance(item.get(K_UPDATED), int):
            item[K_COLLECTED] = dt.fromtimestamp(item[K_UPDATED])
        item["chatter_url"] = item[K_CONTAINER_URL]
        chatters.append(Chatter(**item))

    orch.localsack.store_chatters(chatters)
    print("finished porting chatters")

# adding data porting logic
if __name__ == "__main__":
    port_beans_to_localsack()