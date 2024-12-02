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
from pybeansack.mongosack import *
from pybeansack.datamodels import *
from collectors import rssfeed, ychackernews, individual, redditor, espresso
from coffeemaker import orchestrator as orch
from coffeemaker.digestors import *
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import random

def write_datamodels(items: list[Bean|Chatter], file_name: str = None):
    if items:
        with open(f".test/{file_name or items[0].source}.json", 'w') as file:
            json.dump([item.model_dump(exclude_unset=True, exclude_none=True) for item in items], file)
        return ic(len(items))
            
def write_text(text, file_name):
    with open(f".test/{file_name}", 'w') as file:
        file.write(text)

def test_collection():
    sources = [
        "https://www.buzzhint.com/feed/"
    ]
    
    # rssfeed.collect(store_beans=lambda beans: write_datamodels(orch._download(beans[:5])), sources=sources)
    redditor.collect(
        store_beans=lambda items: print(len(items), "beans collected from", items[0].source),
        store_chatters=orch._collect_chatters)
    ychackernews.collect(
        store_beans=lambda items: print(len(items), "beans collected from", items[0].source),
        store_chatters=orch._collect_chatters)

    # with ServiceBusClient.from_connection_string(orch.sb_connection_str).get_queue_sender("index-queue") as index_queue:
    #     to_json = lambda bean: ServiceBusMessage(json.dumps({K_ID: f"TEST:{bean.url}", K_SOURCE: "TEST", K_URL: bean.url, K_CREATED: int(time.time())}))
    #     rssfeed.collect(sources=sources, store_func=lambda beans: index_queue.send_messages([to_json(bean) for bean in beans]))

    # espresso.collect(orch.sb_connection_str, store_func=lambda beans: write_datamodels(orch._download(beans), "ESPRESSO-QUEUE"))

def test_index_and_augment():
    sources = [
        # "https://www.freethink.com/feed/all",
        # "https://www.testingcatalog.com/rss/",
        # "https://startupnews.fyi/feed/",
        # "https://spectrum.ieee.org/customfeeds/feed/all-topics/rss",
        # "https://dailyhodl.com/feed/",
        # "https://www.investors.com/feed/",
        # "https://www.datacenterknowledge.com/rss.xml",
        # "https://www.gamedeveloper.com/rss.xml",
        # "https://singularityhub.com/feed/",
        # "https://www.nextbigfuture.com/feed",        
        # "https://blog.mozilla.org/attack-and-defense/feed/",
        # "https://www.nccgroup.com/us/research-blog/feed/",
        # "https://blog.oversecured.com/feed.xml",
        # "https://rtx.meta.security/feed.xml",
        # "https://windows-internals.com/feed/",
        # "https://secret.club/feed.xml",
        # "https://research.securitum.com/feed/",
        # "https://feeds.feedburner.com/positiveTechnologiesResearchLab",
        # "https://microsoftedge.github.io/edgevr/feed.xml",
        "https://github.blog/tag/github-security-lab/feed/"
    ]
    rssfeed.collect(sources=sources, store_beans=lambda beans: write_datamodels(orch._augment(orch._index(beans[:3]))))
    # rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._index(orch._download(beans[:3]))))
  
def test_search():
    query = "profession: pilot"
    # write_datamodels(ic(orch.remotesack.query_unique_beans(filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "QUERY_BEANS")
    # write_datamodels(ic(orch.remotesack.text_search_beans(query=query, filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "TEXT_SEARCH")
    write_datamodels(orch.remotesack.vector_search_beans(query=query, filter=updated_after(3), sort_by=LATEST, projection={K_EMBEDDING: 0, K_ID: 0}), "VECTOR_SEARCH")

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
    rssfeed.collect(sources = sources, store_beans=lambda beans: existing.extend(orch._index(orch._download(orch.remotesack.new_beans(beans)))))
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
    rssfeed.collect(
        store_beans=lambda beans: orch._collect_beans(random.sample(beans, 3)),
        sources=["https://www.buzzhint.com/feed/"])
    redditor.collect(
        store_beans=lambda beans: orch._collect_beans(random.sample(beans, 3)),
        store_chatters=orch._collect_chatters)
    ychackernews.collect(
        store_beans=lambda beans: orch._collect_beans(random.sample(beans, 3)),
        store_chatters=orch._collect_chatters)
    
    orch.run_indexing_and_augmenting()
    orch.run_clustering()
    orch.run_trend_ranking()

orch.initialize(
    os.getenv("DB_CONNECTION_STRING"), 
    os.getenv("SB_CONNECTION_STRING"),
    os.getenv("WORKING_DIR", "."), 
    os.getenv("EMBEDDER_PATH"),
    os.getenv("LLM_BASE_URL"),
    os.getenv("LLM_API_KEY"),
    os.getenv("LLM_MODEL"),
    float(os.getenv('CATEGORY_EPS')),
    float(os.getenv('CLUSTER_EPS')))
   

### TEST CALLS
start = now()
# test_collection()
# test_clustering()
# test_index_and_augment()
test_whole_path_live()
# test_search()
# test_trend_ranking()
# orch.run_trend_ranking()
logging.getLogger("test").info("execution time|%s|%d", "__batch__", now() - start)
