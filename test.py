import os
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.WARNING, format="%(asctime)s|%(name)s|%(levelname)s|%(message)s|%(source)s|%(num_items)s")
logger = logging.getLogger("TEST")
logger.setLevel(logging.INFO)

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
    sources = [
        "https://www.buzzhint.com/feed/"
    ]
    
    rssfeed.collect(store_beans=lambda items: print(len(items), "beans collected from", items[0].source), sources=sources)
    redditor.collect(
        store_beans=lambda items: print(len(items), "beans collected from", items[0].source),
        store_chatters=orch.store_chatters)
    ychackernews.collect(
        store_beans=lambda items: print(len(items), "beans collected from", items[0].source),
        store_chatters=orch.store_chatters)

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
            beans = orch._index(orch.download_beans(random.sample(beans, min(3, ic(len(beans))))))
            orch.localsack.store_beans(beans)
            ic(orch._cluster([bean.url for bean in beans]))        
    
    sources = [
        "https://www.ghacks.net/feed/",
        "https://thenewstack.io/feed",
        "https://scitechdaily.com/feed/"
    ]
    rssfeed.collect(store_beans=_collect, sources = sources)

def test_clustering_live(): 
    orch.run_clustering()

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
        store_beans=lambda beans: orch.collect_beans(random.sample(beans, 2)),
        sources=[
            "https://www.techradar.com/feeds/articletype/news",
            "https://www.geekwire.com/feed/",
            "https://investorplace.com/content-feed/",
            "https://dev.to/feed"
        ])
    redditor.collect(
        store_beans=lambda beans: orch.collect_beans(random.sample(beans, 2)),
        store_chatters=orch.store_chatters)
    ychackernews.collect(
        store_beans=lambda beans: orch.collect_beans(random.sample(beans, 2)),
        store_chatters=orch.store_chatters)
    
    orch.run_indexing_and_augmenting()
    orch.run_clustering()
    orch.run_trend_ranking()

if __name__ == "__main__":
    orch.initialize(
        os.getenv("DB_CONNECTION_STRING"),
        os.getenv("SB_CONNECTION_STRING"), 
        os.getenv("WORKING_DIR", "."), 
        os.getenv("EMBEDDER_PATH"),    
        os.getenv("LLM_PATH"),
        float(os.getenv('CATEGORY_EPS')),
        float(os.getenv('CLUSTER_EPS')))
    
    start_time = dt.now()
    total_new_beans = 0
    
    # test_collection()
    # test_clustering()
    # test_index_and_augment()
    # test_whole_path_live()
    # test_search()
    test_trend_ranking()
    
    # num_items = orch.cleanup()
    # logger.info("cleaned up", extra={"source": "__batch__", "num_items": num_items})
    
    # run_collection()
    
    # num_items = orch.update_trend_rank(None, start_time)
    # logger.info("trend ranked", extra={"source": "__batch__", "num_items": num_items})
    
    # while not collected_beans_queue.empty():
    #     beans = orch.store_beans(
    #         orch.augment_beans(
    #             orch.index_beans(
    #                 orch.new_beans(collected_beans_queue.get()))))
    #     if beans:
    #         total_new_beans += len(beans)
    #         urls = [bean.url for bean in beans]
    #         logger.info("stored", extra={"source": beans[0].source, "num_items": len(beans)})
            
            
    #         num_items = orch.update_clusters(orch.cluster_beans(urls))
    #         logger.info("clustered", extra={"source": beans[0].source, "num_items": num_items})
            
    #         num_items = orch.update_trend_rank(orch.trend_rank_beans(urls), start_time)
    #         logger.info("trend ranked", extra={"source": beans[0].source, "num_items": num_items})

    # orch.close()
    logger.info("finished", extra={"source": "__batch__", "num_items": total_new_beans, "execution_time": dt.now()-start_time})
  



