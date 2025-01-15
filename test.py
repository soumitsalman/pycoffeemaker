import asyncio
import os
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.ERROR, format="%(asctime)s|%(name)s|%(levelname)s|%(message)s|%(source)s|%(num_items)s")
logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
logging.getLogger("orchestrator").setLevel(logging.INFO)
logging.getLogger("jieba").propagate = False
logging.getLogger("local digestor").propagate = False
logging.getLogger("local embedder").propagate = False
logging.getLogger("__package__").propagate = False

import json
from datetime import datetime as dt
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.pybeansack.datamodels import *
from coffeemaker.collectors import rssfeed, ychackernews, individual, redditor, espresso
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
    orch.run_collection()
    # sources = [
    #     "https://www.buzzhint.com/feed/"
    # ]
    
    # rssfeed.collect(store_beans=lambda items: print(len(items), "beans collected from", items[0].source), sources=sources)
    # redditor.collect(
    #     store_beans=lambda items: print(len(items), "beans collected from", items[0].source),
    #     store_chatters=orch.store_chatters)
    # ychackernews.collect(
    #     store_beans=lambda items: print(len(items), "beans collected from", items[0].source),
    #     store_chatters=orch.store_chatters)

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
        "https://feeds.feedburner.com/positiveTechnologiesResearchLab",
        # "https://microsoftedge.github.io/edgevr/feed.xml",
        "https://github.blog/tag/github-security-lab/feed/"
    ]
    rssfeed.collect(sources=sources, store_beans=lambda beans: [print(bean.tags, "\n", bean.summary) for bean in orch._augment(orch._index(random.sample(beans, 3)))])
    # rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._index(orch._download(beans[:3]))))
  
def test_search():
    query = "profession: pilot"
    # write_datamodels(ic(orch.remotesack.query_unique_beans(filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "QUERY_BEANS")
    # write_datamodels(ic(orch.remotesack.text_search_beans(query=query, filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "TEXT_SEARCH")
    write_datamodels(orch.remotesack.vector_search_beans(query=query, filter=updated_after(3), sort_by=LATEST, projection={K_EMBEDDING: 0, K_ID: 0}), "VECTOR_SEARCH")

def test_clustering():  
    def _collect(beans: list[Bean]):
        ic(len(beans))
        beans = orch.localsack.not_exists(beans)
        if beans:
            beans = orch._index(orch.deep_collect(random.sample(beans, min(3, ic(len(beans))))))
            orch.localsack.store_beans(beans)
            ic(orch._cluster([bean.url for bean in beans]))        
    
    sources = [
        "https://www.ghacks.net/feed/",
        "https://thenewstack.io/feed",
        "https://scitechdaily.com/feed/"
    ]
    rssfeed.collect(store_beans=_collect, sources = sources)

def test_clustering_live(): 
    orch.cluster_beans()

def test_trend_ranking():    
    # redditor.collect(
    #     store_beans=lambda beans: print(len(beans), "beans collected from", beans[0].source),
    #     store_chatters=orch._collect_chatters)
    # ychackernews.collect(
    #     store_beans=lambda beans: print(len(beans), "beans collected from", beans[0].source),
    #     store_chatters=orch._collect_chatters)    
    orch._trend_rank()

def test_whole_path_live():    
    rssfeed.collect(
        store_beans=lambda beans: orch.extract_new(random.sample(beans, 2)),
        sources=[
            "https://www.techradar.com/feeds/articletype/news",
            "https://www.geekwire.com/feed/",
            "https://investorplace.com/content-feed/",
            "https://dev.to/feed"
        ])
    redditor.collect(
        store_beans=lambda beans: orch.extract_new(random.sample(beans, 2)),
        store_chatters=orch.store_chatters)
    ychackernews.collect(
        store_beans=lambda beans: orch.extract_new(random.sample(beans, 2)),
        store_chatters=orch.store_chatters)
    
    orch.run_indexing_and_augmenting()
    orch.cluster_beans()
    orch.trend_rank_beans()

if __name__ == "__main__":
    start_time = dt.now()
    run_id = start_time.strftime('%Y-%m-%d %H')
    
    logger.info("starting", extra={"source": run_id, "num_items": 0})
    
    orch.initialize(
        os.getenv("DB_CONNECTION_STRING"),
        os.getenv("SB_CONNECTION_STRING"), 
        os.getenv("WORKING_DIR", "."), 
        os.getenv("EMBEDDER_PATH"),    
        os.getenv("LLM_PATH"),
        float(os.getenv('CATEGORY_EPS')),
        float(os.getenv('CLUSTER_EPS')))
    
    # asyncio.run(orch.run_collection_async())
    asyncio.run(orch.run_async())
    orch.close()
    # test_collection()
    # test_collection_async()
    # test_clustering()
    # test_index_and_augment()
    # test_whole_path_live()
    # test_search()
    # test_trend_ranking()
    logger.info("execution time", extra={"source": run_id, "num_items": dt.now()-start_time})
    
