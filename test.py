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
import random

def write_datamodels(items, file_name: str = None):
    if items:
        with open(f".test/{file_name or ic(items[0].source)}.json", 'w') as file:
            json.dump([bean.model_dump(exclude_unset=True, exclude_none=True) for bean in items], file)
            
def write_text(text, file_name):
    with open(f".test/{file_name}", 'w') as file:
        file.write(text)

def test_collection():
    sources = [
        "https://www.buzzhint.com/feed/"
    ]
    
    rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._download_beans(beans[:5])))
    redditor.collect(store_func=lambda items: write_datamodels(orch._download_beans([item[0] for item in random.sample(items, k=20)]), file_name="REDDIT"))
    ychackernews.collect(store_func=lambda items: write_datamodels(orch._download_beans([item[0] for item in random.sample(items, k=20)]), file_name="YC"))

def test_whole_path_live():
    sources = [
        "https://www.nationalreview.com/feed/",
        "https://www.moneytalksnews.com/feed/"
    ]
    # rssfeed.collect(sources=sources, store_func=orch._collect)
    redditor.collect(store_func=orch._collect)
    orch.run_indexing()
    orch.run_clustering()
    orch.run_trend_ranking()
    orch.run_augmentation()

def test_chains():
    source = "https://eletric-vehicles.com/feed/"
    write_datamodels(orch._augment(rssfeed.collect_from(source)), "TEST-CHAIN")
  
def test_search():
    query = "profession: pilot"
    # write_datamodels(ic(orch.remotesack.query_unique_beans(filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "QUERY_BEANS")
    # write_datamodels(ic(orch.remotesack.text_search_beans(query=query, filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "TEXT_SEARCH")
    write_datamodels(orch.remotesack.vector_search_beans(query=query, filter=updated_in(3), sort_by=LATEST, projection={K_EMBEDDING: 0, K_ID: 0}), "VECTOR_SEARCH")

def test_clustering(): 
    res = orch._cluster(orch.remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL:1, K_TITLE: 1, K_EMBEDDING: 1}))
    make_update = lambda group, header: print("[", len(group), "]", header, "====\n", "\n\t".join(group) if isinstance(group[0], str) else group,"\n")
    list(map(make_update, res, [items[0] for items in res]))

def test_clustering_live(): 
    orch.run_clustering()

def test_trend_ranking():
    selected_urls = [
        "https://www.nature.com/articles/d41586-024-02526-y", 
        "https://www.livescience.com/space/black-holes/final-parsec-problem-supermassive-black-holes-impossible-solution", 
        "https://scitechdaily.com/hubble-captures-a-supernova-spiral-a-galaxys-tale-of-birth-beauty-and-explosions/", 
        "https://www.livescience.com/space/cosmology/catastrophic-collision-between-milky-way-and-andromeda-galaxies-may-not-happen-after-all-new-study-hints", 
        "https://www.iflscience.com/astronomers-place-5050-odds-on-andromeda-colliding-with-us-75543"
    ]
    items = orch.remotesack.get_beans(filter={K_URL: {"$in": selected_urls}})
    urls = [item.url for item in items]
    chatters=orch.remotesack.get_latest_chatter_stats(urls)
    cluster_sizes=orch.remotesack.count_related_beans(urls)
    ic([orch._make_trend_update(item, chatters, cluster_sizes) for item in items])


orch.initialize(
    os.getenv("DB_CONNECTION_STRING"), 
    os.getenv("WORKING_DIR", "."), 
    os.getenv("EMBEDDER_FILE"),
    os.getenv("GROQ_API_KEY"),
    float(os.getenv('CATEGORY_EPS')))
   

### TEST CALLS
# test_writing()
# test_chains()
# test_collection()
# test_clustering()
# test_clustering_live()
test_whole_path_live()
# test_search()
# test_trend_ranking()


