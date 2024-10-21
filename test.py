## MAIN FUNC ##
import time
from icecream import ic
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.WARNING, 
    datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s|%(name)s|%(levelname)s|%(message)s"
)
logging.getLogger("app").setLevel(logging.INFO)
logging.getLogger("orchestrator").setLevel(logging.INFO)
logging.getLogger("local digestor").setLevel(logging.INFO)
logging.getLogger("remote digestor").setLevel(logging.INFO)
logging.getLogger("local embedder").setLevel(logging.INFO)
logging.getLogger("remote embedder").setLevel(logging.INFO)
logging.getLogger("beansack").setLevel(logging.INFO)
    
load_dotenv()

import json
from datetime import datetime as dt
from pybeansack.beansack import *
from pybeansack.datamodels import *
from collectors import rssfeed, ychackernews, individual, redditor, espresso
from coffeemaker import orchestrator as orch
from coffeemaker.digestors import *
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import random

def write_datamodels(items, file_name: str = None):
    if items:
        with open(f".test/{file_name or items[0].source}.json", 'w') as file:
            json.dump([bean.model_dump(exclude_unset=True, exclude_none=True) for bean in items], file)
        return ic(len(items))
            
def write_text(text, file_name):
    with open(f".test/{file_name}", 'w') as file:
        file.write(text)

def test_collection():
    sources = [
        "https://www.buzzhint.com/feed/"
    ]
    
    # rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._download_beans(beans[:5])))
    # redditor.collect(store_func=lambda items: write_datamodels(orch._download_beans([item[0] for item in random.sample(items, k=20)]), file_name="REDDIT"))
    # ychackernews.collect(store_func=lambda items: write_datamodels(orch._download_beans([item[0] for item in random.sample(items, k=20)]), file_name="YC"))

    # with ServiceBusClient.from_connection_string(orch.sb_connection_str).get_queue_sender("index-queue") as index_queue:
    #     to_json = lambda bean: ServiceBusMessage(json.dumps({K_ID: f"TEST:{bean.url}", K_SOURCE: "TEST", K_URL: bean.url, K_CREATED: int(time.time())}))
    #     rssfeed.collect(sources=sources, store_func=lambda beans: index_queue.send_messages([to_json(bean) for bean in beans]))

    espresso.collect(orch.sb_connection_str, store_func=lambda beans: write_datamodels(orch._download(beans), "ESPRESSO-QUEUE"))

def test_index_and_augment():
    sources = [
        "https://regtechtimes.com/feed/",
        "https://fedoramagazine.org/feed/"
    ]
    rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._augment(beans[:2])))
    # rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._index(beans)))
  
def test_search():
    query = "profession: pilot"
    # write_datamodels(ic(orch.remotesack.query_unique_beans(filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "QUERY_BEANS")
    # write_datamodels(ic(orch.remotesack.text_search_beans(query=query, filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "TEXT_SEARCH")
    write_datamodels(orch.remotesack.vector_search_beans(query=query, filter=updated_in(3), sort_by=LATEST, projection={K_EMBEDDING: 0, K_ID: 0}), "VECTOR_SEARCH")

def test_clustering():  
    existing = orch.remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL:1, K_TITLE: 1, K_CLUSTER_ID: 1, K_EMBEDDING: 1})    
    sources = [
        "https://accessvector.net/rss.xml",
        "https://androidoffsec.withgoogle.com/index.xml",
        "https://www.runzero.com/blog/index.xml",
        "https://blog.stratumsecurity.com/rss/",
        "https://blog.assetnote.io/feed.xml",
        "https://www.atredis.com/blog?format=rss",
        "https://starlabs.sg/blog/index.xml",
        "https://labs.watchtowr.com/rss/",
        "http://feeds.feedburner.com/positiveTechnologiesResearchLab",
        "http://googleprojectzero.blogspot.com/feeds/posts/default"
    ]
    rssfeed.collect(sources = sources, store_func=lambda beans: existing.extend(orch._index(orch._download(orch.remotesack.filter_unstored_beans(beans)))))
    url_set = set()
    duplicate_urls = []
    for bean in orch._cluster(existing):
        if bean.url in url_set:
            duplicate_urls.append(bean.url)
        else:
            url_set.add(bean.url)
    if duplicate_urls:
        print(f"Duplicate URLs found: {duplicate_urls}")
    else:
        print("No duplicate URLs found.")

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

def test_whole_path_live():
    # sources = [
    #     "https://www.nationalreview.com/feed/",
    #     "https://www.moneytalksnews.com/feed/"
    # ]
    # rssfeed.collect(sources=sources, store_func=orch._collect)
    ychackernews.collect(store_func=orch._collect)
    # redditor.collect(store_func=orch._collect)
    # espresso.collect(orch.sb_connection_str, orch._collect)
    orch.run_indexing_and_augmenting()
    orch.run_clustering()
    orch.run_trend_ranking()

orch.initialize(
    os.getenv("DB_CONNECTION_STRING"), 
    os.getenv("SB_CONNECTION_STRING"),
    os.getenv("WORKING_DIR", "."), 
    os.getenv("EMBEDDER_PATH"),
    # os.getenv("LLM_PATH"),
    None,
    os.getenv("LLM_BASE_URL"),
    os.getenv("LLM_API_KEY"),
    os.getenv("LLM_MODEL"),
    float(os.getenv('CATEGORY_EPS')),
    float(os.getenv('CLUSTER_EPS')))
   

### TEST CALLS
start = time.time()
# test_collection()
# test_clustering()
test_index_and_augment()
# test_whole_path_live()
# test_search()
# test_trend_ranking()
logging.getLogger("test").info("execution time|%s|%d", "__batch__", int(time.time() - start))

