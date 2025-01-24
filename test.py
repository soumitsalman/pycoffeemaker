import os
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.WARNING, format="%(asctime)s|%(name)s|%(levelname)s|%(message)s|%(source)s|%(num_items)s")
logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrator").setLevel(logging.INFO)
logging.getLogger("jieba").propagate = False
logging.getLogger("digestor.local").propagate = False
logging.getLogger("embedder.local").propagate = False
logging.getLogger("asyncprawcore").propagate = False
logging.getLogger("asyncpraw").propagate = False
logging.getLogger("dammit").propagate = False
logging.getLogger("UnicodeDammit").propagate = False
logging.getLogger("urllib3").propagate = False
logging.getLogger("connectionpool").propagate = False

import json
import asyncio
from datetime import datetime as dt
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.pybeansack.models import *
from coffeemaker.collectors import collector, rssfeed, ychackernews, individual, redditor, espresso
from coffeemaker.orchestrator import Orchestrator
from coffeemaker.digestors import *
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import random
import re, json, random

def url_to_filename(url: str) -> str:
    return "./.test/" + re.sub(r'[^a-zA-Z0-9]', '-', url)

def save_markdown(url, markdown):
    filename = url_to_filename(url)+".md"
    with open(filename, 'w') as file:
        file.write(markdown)

def save_json(url, items):
    filename = url_to_filename(url)+".json"
    with open(filename, 'w') as file:
        json.dump(items, file)

def save_models(items: list[Bean|Chatter], file_name: str = None):
    if items:
        with open(f".test/{file_name or items[0].source}.json", 'w') as file:
            json.dump([item.model_dump(exclude_unset=True, exclude_none=True) for item in items], file)
        return ic(len(items))

def _create_orchestrator():
    return Orchestrator(
        os.getenv("DB_CONNECTION_STRING"),
        None, 
        os.getenv("WORKING_DIR", "."), 
        os.getenv("EMBEDDER_PATH"),    
        os.getenv("LLM_PATH"),
        float(os.getenv('CATEGORY_EPS')),
        float(os.getenv('CLUSTER_EPS')))

def test_collection():
    # orch.run_collection()
    # urls = [
    #     "https://www.huffpost.com/entry/vivek-ramaswamy-ohio-senate_n_6788136ee4b0ebaad44e9932",
    #     "https://financebuzz.com/hotel-etiquette-rules-7"
    # ]
    # for url in urls:
    #     res = individual.collect_url(url)
    #     ic(res.text)
        
    #     response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    #     soup = BeautifulSoup(response.text, 'html.parser')
    #     primary_cli_items = soup.find_all("div", class_=["primary-cli", "slideshow-container"])        
    #     ic("\n\n".join(item.get_text(strip=True) for item in primary_cli_items))
    # sources = [
    #     "https://feeds.washingtonpost.com/rss/national",
    #     "https://www.buzzhint.com/feed/"
    # ]
    # collected = lambda beans: logger.info("collected", extra={"source": beans[0][0].source if isinstance(beans[0], tuple) else beans[0].source, "num_items": len(beans)}) if beans else None
    # rssfeed.collect(collected, sources=sources)
    # redditor.collect(collected)
    asyncio.run(orch.run_collection_async())
    
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
    collected = lambda beans: print(f"collected {len(beans)} from {beans[0].source}" if beans else "no beans collected")
    rssfeed.collect(collected, sources=sources)
    redditor.collect(collected)
    # rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._index(orch._download(beans[:3]))))
  
def test_search():
    query = "profession: pilot"
    # write_datamodels(ic(orch.remotesack.query_unique_beans(filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "QUERY_BEANS")
    # write_datamodels(ic(orch.remotesack.text_search_beans(query=query, filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "TEXT_SEARCH")
    save_models(orch.remotesack.vector_search_beans(query=query, filter=updated_after(3), sort_by=LATEST, projection={K_EMBEDDING: 0, K_ID: 0}), "VECTOR_SEARCH")

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

def test_run_async():
    orch = _create_orchestrator()
    asyncio.run(orch.run_async())
    orch.close()

if __name__ == "__main__":
    test_run_async()

    
