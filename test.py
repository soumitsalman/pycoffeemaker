## MAIN FUNC ##
from icecream import ic
import os
from dotenv import load_dotenv
    
load_dotenv()

import json
from datetime import datetime as dt
from pybeansack.beansack import *
from pybeansack.datamodels import *
from collectors import rssfeed, ychackernews, individual, redditor
from coffeemaker import orchestrator as orch
from coffeemaker.chains import *

def write_datamodels(items, file_name: str = None):
    if items:
        with open(f".test/{file_name or items[0].source}.json", 'w') as file:
            json.dump([bean.model_dump(exclude_unset=True, exclude_none=True) for bean in items], file)
            
def write_text(text, file_name):
    with open(f".test/{file_name}", 'w') as file:
        file.write(text)

def test_collection():
    sources = [
        # "https://www.prnewswire.com/rss/environment-latest-news/environment-latest-news-list.rss",
        # "https://chaski.huffpost.com/us/auto/vertical/world-news",
        "https://theaviationist.com/feed/"
    ]
    rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._download_beans(beans)))
    # redditor.collect(store_func=lambda items: write_datamodels(orch._download_beans([item[0] for item in items])))

def test_chains():
    sources = [
        "https://www.finsmes.com/feed"
    ]
    rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._augment(beans), "TEST-CHAIN-"+beans[0].source))
  
def test_search():
    query = "profession: pilot"
    # write_datamodels(ic(orch.remotesack.query_unique_beans(filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "QUERY_BEANS")
    # write_datamodels(ic(orch.remotesack.text_search_beans(query=query, filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "TEXT_SEARCH")
    write_datamodels(orch.remotesack.vector_search_beans(query=query, filter=timewindow_filter(3), sort_by=LATEST, projection={K_EMBEDDING: 0, K_ID: 0}), "VECTOR_SEARCH")

def test_clustering(): 
    res = orch._run_clustering(orch.remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL:1, K_TITLE: 1, K_EMBEDDING: 1}), orch.N_DEPTH)
    make_update = lambda group, header: print("[", len(group), "]", header, "====\n", "\n\t".join(group) if isinstance(group[0], str) else group,"\n") # print("[", len(group), "] ====\n", "\n\t".join(group),"\n")
    list(map(make_update, res, [items[0] for items in res]))


# orch.initialize(
#     os.getenv("DB_CONNECTION_STRING"), 
#     os.getenv("WORKING_DIR", "."), 
#     os.getenv("EMBEDDER_FILE"),
#     os.getenv("GROQ_API_KEY"),    
#     float(os.getenv('CLUSTER_EPS')),
#     float(os.getenv('CATEGORY_EPS')))
   

### TEST CALLS
# test_writing()
# test_chains()
test_collection()
# test_clustering()
# test_search()

