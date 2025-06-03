import os, sys
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
logging.basicConfig(level=logging.WARNING, format="%(asctime)s|%(name)s|%(levelname)s|%(message)s|%(source)s|%(num_items)s")
logger = logging.getLogger("test")
logger.setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.collectororch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.analyzerorch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.composerorch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.fullstack").setLevel(logging.INFO)
# logging.getLogger("coffeemaker.collectors.collector").setLevel(logging.INFO)
logging.getLogger("jieba").propagate = False
logging.getLogger("coffeemaker.nlp.digestors").propagate = False
logging.getLogger("coffeemaker.nlp.embedders").propagate = False
logging.getLogger("asyncprawcore").propagate = False
logging.getLogger("asyncpraw").propagate = False
logging.getLogger("dammit").propagate = False
logging.getLogger("UnicodeDammit").propagate = False
logging.getLogger("urllib3").propagate = False
logging.getLogger("connectionpool").propagate = False
logging.getLogger("asyncio").propagate = False

DB_REMOTE_TEST="mongodb://localhost:27017/"
DB_NAME_TEST="test3"
AZSTORAGE_PATH_TEST="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
INDEXER_IN_QUEUE="indexing-queue"
DIGESTOR_IN_QUEUE="digesting-queue"
COLLECTOR_OUT_QUEUES=[INDEXER_IN_QUEUE, DIGESTOR_IN_QUEUE]
EMBEDDER_CONTEXT_LEN=512

import json, re, random
import asyncio
from datetime import datetime
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.pybeansack.models import *
from coffeemaker.collectors import collector
from coffeemaker.orchestrators.utils import log_runtime

os.makedirs(".test", exist_ok=True)

def url_to_filename(url: str) -> str:
    return "./.test/" + re.sub(r'[^a-zA-Z0-9]', '-', url)

def load_json(filename):
    with open(filename, 'r') as file:
        return json.load(file)

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

def _create_fullstack_orchestrator():
    from coffeemaker.orchestrators.fullstack import Orchestrator
    return Orchestrator(
        os.getenv("DB_REMOTE_TEST"),
        os.getenv("DB_LOCAL"),
        os.getenv("DB_NAME"),
        embedder_path=os.getenv("EMBEDDER_PATH"),
        digestor_path=os.getenv("DIGESTOR_PATH"),
        clus_eps=0.5
    )


  
# def test_index_and_augment():
#     sources = [
#         # "https://www.freethink.com/feed/all",
#         # "https://www.testingcatalog.com/rss/",
#         # "https://startupnews.fyi/feed/",
#         # "https://spectrum.ieee.org/customfeeds/feed/all-topics/rss",
#         # "https://dailyhodl.com/feed/",
#         # "https://www.investors.com/feed/",
#         # "https://www.datacenterknowledge.com/rss.xml",
#         # "https://www.gamedeveloper.com/rss.xml",
#         # "https://singularityhub.com/feed/",
#         # "https://www.nextbigfuture.com/feed",        
#         # "https://blog.mozilla.org/attack-and-defense/feed/",
#         # "https://www.nccgroup.com/us/research-blog/feed/",
#         # "https://blog.oversecured.com/feed.xml",
#         # "https://rtx.meta.security/feed.xml",
#         # "https://windows-internals.com/feed/",
#         # "https://secret.club/feed.xml",
#         # "https://research.securitum.com/feed/",
#         "https://feeds.feedburner.com/positiveTechnologiesResearchLab",
#         # "https://microsoftedge.github.io/edgevr/feed.xml",
#         "https://github.blog/tag/github-security-lab/feed/"
#     ]
#     collected = lambda beans: print(f"collected {len(beans)} from {beans[0].source}" if beans else "no beans collected")
#     rssfeed.collect(collected, sources=sources)
#     redditor.collect(collected)
#     # rssfeed.collect(sources=sources, store_func=lambda beans: write_datamodels(orch._index(orch._download(beans[:3]))))
  
# def test_search():
#     query = "profession: pilot"
#     # write_datamodels(ic(orch.remotesack.query_unique_beans(filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "QUERY_BEANS")
#     # write_datamodels(ic(orch.remotesack.text_search_beans(query=query, filter=timewindow_filter(3), sort_by=LATEST, limit=3, projection={K_EMBEDDING: 0, K_ID:0})), "TEXT_SEARCH")
#     save_models(orch.remotesack.vector_search_beans(query=query, filter=updated_after(3), sort_by=LATEST, projection={K_EMBEDDING: 0, K_ID: 0}), "VECTOR_SEARCH")

# def test_clustering():  
#     def _collect(beans: list[Bean]):
#         ic(len(beans))
#         beans = orch.localsack.not_exists(beans)
#         if beans:
#             beans = orch._index(orch.deep_collect(random.sample(beans, min(3, ic(len(beans))))))
#             orch.localsack.store_beans(beans)
#             ic(orch._cluster([bean.url for bean in beans]))        
    
#     sources = [
#         "https://www.ghacks.net/feed/",
#         "https://thenewstack.io/feed",
#         "https://scitechdaily.com/feed/"
#     ]
#     rssfeed.collect(store_beans=_collect, sources = sources)

# def test_clustering_live(): 
#     orch.cluster_beans()

# def test_trend_ranking():    
#     # redditor.collect(
#     #     store_beans=lambda beans: print(len(beans), "beans collected from", beans[0].source),
#     #     store_chatters=orch._collect_chatters)
#     # ychackernews.collect(
#     #     store_beans=lambda beans: print(len(beans), "beans collected from", beans[0].source),
#     #     store_chatters=orch._collect_chatters)    
#     orch._trend_rank()

def test_collection_and_download():
    from coffeemaker.orchestrators.fullstack import Orchestrator, END_OF_STREAM
    async def _run(orch: Orchestrator):
        orch._init_run()
        await orch.run_collections_async("/home/soumitsr/codes/pycoffeemaker/tests/test-sources-1.yaml")
        await orch.download_queue.put(END_OF_STREAM)
        await orch.run_downloading_async()

    orch = _create_fullstack_orchestrator()
    asyncio.run(_run(orch))
    orch.close()


@log_runtime
def test_run_async():
    orch = _create_fullstack_orchestrator() 
    sources = "/home/soumitsr/codes/pycoffeemaker/tests/sources-2.yaml"
    asyncio.run(orch.run_async(sources))
    orch.close()

lower_case = lambda items: {"$in": [item.lower() for item in items]} if isinstance(items, list) else items.lower()
case_insensitive = lambda items: {"$in": [re.compile(item, re.IGNORECASE) for item in items]} if isinstance(items, list) else re.compile(items, re.IGNORECASE)

def download_test_data(output_path):
    from coffeemaker.orchestrators.fullstack import Orchestrator
    orch = Orchestrator(
        os.getenv("DB_REMOTE_TEST"),
        os.getenv("DB_LOCAL"),
        os.getenv("DB_NAME"), 
        embedder_path=os.getenv("EMBEDDER_PATH"),    
        digestor_path=os.getenv("DIGESTOR_PATH")
    )
    beans = orch.remotesack.query_beans(
        filter = {
            K_COLLECTED: { "$gte": (datetime.now() - timedelta(days=10))},
            K_KIND: {"$ne": [NEWS, BLOG]},
            K_CONTENT: {"$exists": True}
        },
        projection = {K_EMBEDDING: 0},
        limit=512
    )
    to_write = [bean.model_dump_json(by_alias=True, exclude_none=True, exclude_unset=True, exclude_defaults=True) for bean in beans]
    with open(output_path, "w") as file:
        json.dump(to_write, file)

def download_markdown(q: str = None, accuracy = DEFAULT_VECTOR_SEARCH_SCORE, keywords: str|list[str] = None, limit = 100):
    orch = _create_fullstack_orchestrator()
    filter = {K_KIND: { "$ne": POST}}
    if keywords: 
        filter.update(
            { 
                "tags": case_insensitive(keywords) 
            }
        )
    projection = {K_SUMMARY: 1, K_URL: 1}

    if q:
        beans = orch.remotesack.vector_search_beans(
            embedding=orch.embedder.embed_query(f"query: {q}"),
            min_score=accuracy,
            filter=filter,
            sort_by=NEWEST,
            skip=0,
            limit=limit,
            projection=projection
        )
    else:
        beans = orch.remotesack.query_sample_beans(filter, NEWEST, limit, projection)

    markdown = "\n\n".join([bean.digest() for bean in beans])

    filename = q or datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    if keywords: filename = "-".join(keywords)
    save_markdown(filename, markdown)

def test_trend_analysis():
    from coffeemaker.orchestrators.collectororch import Orchestrator
    orch = Orchestrator(
        os.getenv('MONGODB_CONN_STR'),
        "test"
    )
    items = orch.db.get_latest_chatters()
    ic(random.sample(items, 5))

def test_collector_orch():
    from coffeemaker.orchestrators.collectororch import Orchestrator
    orch = Orchestrator(
        DB_REMOTE_TEST,
        now().strftime("%Y%m%d")
    )
    # sources = """/home/soumitsr/codes/pycoffeemaker/coffeemaker/collectors/feeds.yaml"""
    # sources = """/home/soumitsr/codes/pycoffeemaker/tests/sources-2.yaml"""
    sources = """
sources:
  rss:
    - https://newatlas.com/index.rss
    - https://www.channele2e.com/feed/topic/latest
    - https://www.ghacks.net/feed/
    - https://thenewstack.io/feed
    - https://scitechdaily.com/feed/
    - https://www.techradar.com/feeds/articletype/news
    - https://www.geekwire.com/feed/
    - https://investorplace.com/content-feed/
  ychackernews:
    - https://hacker-news.firebaseio.com/v0/newstories.json
    - https://hacker-news.firebaseio.com/v0/askstories.json
  reddit:
    - news
    - worldnews
    - InternationalNews
    - GlobalNews
    - GlobalMarketNews
    - FinanceNews
    - StockNews
    - CryptoNews
    - energyStocks
"""
    asyncio.run(orch.run_async(sources))
    # orch.run(sources)

def test_indexer_orch():
    from coffeemaker.orchestrators.analyzerorch import Orchestrator
    orch = Orchestrator(
        DB_REMOTE_TEST,
        now().strftime("%Y%m%d"), 
        embedder_path=os.getenv("EMBEDDER_PATH"),
        embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN")),
        cluster_distance=0.03,
        category_defs="./factory/categories.parquet",
        sentiment_defs="./factory/sentiments.parquet"
    )
    orch.run_indexer()

def test_digestor_orch():
    from coffeemaker.orchestrators.analyzerorch import Orchestrator
    orch = Orchestrator(
        DB_REMOTE_TEST,
        now().strftime("%Y%m%d"),
        digestor_path=os.getenv("DIGESTOR_PATH"), 
        digestor_base_url=os.getenv("DIGESTOR_BASE_URL"),
        digestor_api_key=os.getenv("DIGESTOR_API_KEY"),
        digestor_context_len=int(os.getenv("DIGESTOR_CONTEXT_LEN"))
    )
    orch.run_digestor()

def test_static_db():
    from coffeemaker.pybeansack.staticdb import StaticDB
    from coffeemaker.nlp.src.embedders import from_path
    print("starting")
    categories = StaticDB("/home/soumitsr/codes/pycoffeemaker/coffeemaker/nlp/categories.parquet")
    sentiments = StaticDB("/home/soumitsr/codes/pycoffeemaker/coffeemaker/nlp/sentiments.parquet")
    print("db loaded")
    embedder = from_path(os.getenv('EMBEDDER_PATH'), 512)
    print("embedder loaded")
    ic(categories.vector_search(embedder.embed("category/domain classification: artificial intelligence"), limit=10))
    ic(sentiments.vector_search(embedder.embed("sentiment classification: ecstatic"), limit=5))

def test_composer_orch():
    from coffeemaker.orchestrators.composerorch import Orchestrator
    from coffeemaker.nlp.src.models import GeneratedArticle
    orch = Orchestrator(
        DB_REMOTE_TEST,
        "20250601",
        composer_path="google/gemma-3-27b-it", 
        composer_base_url=os.getenv("DIGESTOR_BASE_URL"),
        composer_api_key=os.getenv("DIGESTOR_API_KEY"),
        composer_context_len=120000,
        backup_azstorage_conn_str=os.getenv("AZSTORAGE_CONN_STR")
    )
    orch.run()

if __name__ == "__main__":
    # test_static_db()
    # test_trend_analysis()
    test_collector_orch()
    test_indexer_orch()
    # test_digestor_orch()
    # test_composer_orch()
    # download_test_data("/home/soumitsr/codes/pycoffeemaker/tests/texts-for-nlp.json")

    # test_run_async()
    # test_embedder()
    # test_digestor()
    # test_digest_parser()
    # test_collection_and_download()
    # test_run_async()
    # topics['topics'][0]
    # download_markdown(limit = 110)
    # download_markdown(keywords =  ["Cardano", "$0.62 support", "RSI oversold", "Fibonacci retracement", "volume confirmation"], limit=110)
    # download_markdown("North Korean operatives generate $250 million to $600 million annually through remote IT job fraud (May 2, 2025).", accuracy=0.8)
    # [download_markdown(q = topic['verdict'], limit = 50) for topic in topics['topics']]
    
