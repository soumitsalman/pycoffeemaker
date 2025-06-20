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

DB_LOCAL_TEST="mongodb://localhost:27017/"
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
            json.dump([item.model_dump(by_alias=True, exclude_none=True, exclude_unset=True, exclude_defaults=True) for item in items], file)
        return ic(len(items))

def test_collector_and_scraper():
    from coffeemaker.collectors import APICollector

    coll = APICollector()
    feeds = [
        "https://newsfeed.zeit.de/index",
        "https://newsletter.canopy.is/feed",
        "https://www.cio.com/feed/"
    ]
    [coll.collect_rssfeed(f) for f in feeds]

def test_fullstack_orch():
    from coffeemaker.orchestrators.fullstack import Orchestrator
    orch = Orchestrator(
        DB_LOCAL_TEST,
        now().strftime("%Y%m%d"),
        now().strftime("%Y%m%d"),
        embedder_path=os.getenv("EMBEDDER_PATH"),
        digestor_path=os.getenv("DIGESTOR_PATH"),
        clus_eps=0.5
    )
    sources = "/home/soumitsr/codes/pycoffeemaker/tests/sources-2.yaml"
    asyncio.run(orch.run_async(sources))
    orch.close()

def create_test_data_file(output_path):
    from coffeemaker.orchestrators.collectororch import Orchestrator

    orch = Orchestrator(
        os.getenv('MONGODB_CONN_STR'),
        "test"
    )
    beans = orch.db.sample_beans(
        filter = {
            K_COLLECTED: { "$gte": (datetime.now() - timedelta(days=10))},
            K_KIND: {"$in": [NEWS, BLOG]},
            K_CONTENT: VALUE_EXISTS,
            K_GIST: VALUE_EXISTS,
            K_EMBEDDING: VALUE_EXISTS
        },
        limit=512,
        project = {K_EMBEDDING: 0}
    )
    save_models(beans, output_path)

def hydrate_test_db():
    from factory.rectify import migrate_mongodb
    migrate_mongodb("test", now().strftime("%Y%m%d"), from_db_conn=os.getenv('MONGODB_CONN_STR'), to_db_conn=DB_LOCAL_TEST)


def test_trend_analysis():
    from coffeemaker.orchestrators.collectororch import Orchestrator
    orch = Orchestrator(
        os.getenv('MONGODB_CONN_STR'),
        "test"
    )
    items = orch.db.get_latest_chatters()
    ic(random.sample(items, 5))

def test_static_db():
    from coffeemaker.pybeansack.staticdb import StaticDB
    from coffeemaker.nlp import embedders
    print("starting")
    categories = StaticDB("./factory/categories.parquet")
    sentiments = StaticDB("./factory/sentiments.parquet")
    print("db loaded")
    embedder = embedders.from_path(os.getenv('EMBEDDER_PATH'), 512)
    print("embedder loaded")
    ic(categories.vector_search(embedder.embed("category/domain classification: artificial intelligence"), limit=10))
    ic(sentiments.vector_search(embedder.embed("sentiment classification: ecstatic"), limit=5))


def test_collector_orch():
    from coffeemaker.orchestrators.collectororch import Orchestrator
    orch = Orchestrator(
        DB_LOCAL_TEST,
        now().strftime("%Y%m%d")
    )
    # sources = """/home/soumitsr/codes/pycoffeemaker/factory/feeds.yaml"""
    # sources = """/home/soumitsr/codes/pycoffeemaker/tests/sources-1.yaml"""
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
    orch.db.beanstore.drop()
    asyncio.run(orch.run_async(sources))
    # orch.run(sources)

def test_indexer_orch():
    from coffeemaker.orchestrators.analyzerorch import Orchestrator
    orch = Orchestrator(
        DB_LOCAL_TEST,
        now().strftime("%Y%m%d"),
        # embedder_path="avsolatorio/GIST-small-Embedding-v0", 
        embedder_path="openvino:///home/soumitsr/codes/pycoffeemaker/.models/gist-small-embedding-v0-openvino",
        embedder_context_len=512,
        category_defs="./factory/categories.parquet",
        sentiment_defs="./factory/sentiments.parquet"
    )
    orch.run_indexer()

def test_digestor_orch():
    from coffeemaker.orchestrators.analyzerorch import Orchestrator
    orch = Orchestrator(
        DB_LOCAL_TEST,
        now().strftime("%Y%m%d"),
        digestor_path="soumitsr/led-base-article-digestor",
        digestor_context_len=4096,
        backup_azstorage_conn_str=os.getenv("AZSTORAGE_CONN_STR")
        # digestor_path="google/gemma-3-12b-it", 
        # digestor_base_url=os.getenv("DIGESTOR_BASE_URL"),
        # digestor_api_key=os.getenv("DIGESTOR_API_KEY"),
        # digestor_context_len=4096
    )
    orch.run_digestor()

def test_composer_orch():
    from coffeemaker.orchestrators.composerorch import Orchestrator
    from coffeemaker.nlp import GeneratedArticle, agents, NEWSRECAP_SYSTEM_PROMPT
    orch = Orchestrator(
        DB_LOCAL_TEST,
        now().strftime("%Y%m%d"),
        # "20250615",
        composer_path="o4-mini",
        composer_base_url=os.getenv("COMPOSER_BASE_URL"),
        composer_api_key=os.getenv("COMPOSER_API_KEY"),
        composer_context_len=40000,
        embedder_path=os.getenv("EMBEDDER_PATH"),
        embedder_context_len=512,
        backup_azstorage_conn_str=os.getenv("AZSTORAGE_CONN_STR")
    )
#     topics = """
#   Artificial Intelligence:
#     kind: blog
#     last_ndays: 1
#     tags:
#       - Artificial General Intelligence
#       - Artificial Intelligence
#       - Artificial Intelligence Safety
#       - Artificial Intelligence Ethics and Governance
#       - Artificial Neural Networks
#       - Artificial Vision
#       - Machine Learning and AI Applications
#       - Data Science and Analytics
#       - Automation and Robotics
#       - Computing and Information Technology

#   Cybersecurity:
#     kind: news
#     last_ndays: 1
#     tags:
#       - Cybersecurity and Cybercrime
#       - Privacy and Data Protection
#       - Security and Defense Technology
#       - Internet and Web Technologies
#       - Computing and Information Technology
#       - Cloud Computing
#       - Blockchain and Cryptocurrency

#   Software Engineering:
#     kind: blog
#     last_ndays: 1
#     tags:
#       - Software Development
#       - Coding and Programming Languages
#       - Algorithm and Computation
#       - Cloud Computing
#       - Computing and Information Technology
#       - Internet and Web Technologies
#       - High Performance Computing
#       - Data Science and Analytics

#   Career & Professional Growth:
#     kind: blog
#     last_ndays: 1
#     tags:
#       - Career and Employment
#       - Professional Development
#       - Business and Management
#       - Workplace and Employment Law
#       - Labor and Workforce
#       - Innovation and Startups
#       - Education
#       - Youth and Education

#   Startups & Entrepreneurship:
#     kind: news
#     last_ndays: 1
#     tags:
#       - Innovation and Startups
#       - Business and Management
#       - Technology and Innovation
#       - Product Development and Technology
#       - Research and Development
#       - Industry and Manufacturing
#     """
    topics = """/home/soumitsr/codes/pycoffeemaker/factory/composer-topics.yaml"""
    for bean in orch.run(topics):
        print(">>>>>>>>>>>>>>>>")
        print(bean.title)
        print(bean.verdict)
        print(bean.analysis)
        print(bean.insights)
        print(bean.predictions)
        print(bean.entities)
        print("<<<<<<<<<<<<<<<<")
    ## test cluster
    # orch.run_id = now().strftime("%Y-%m-%d-%H-%M-%S")
    # clusters = orch.get_topic_clusters(topics)
    # [ic(c[0], c[1], len(c[2])) for c in clusters]
    # beans = orch.get_beans(filter = {K_KIND: NEWS})

    # for idx, cl in enumerate(orch.cluster_beans(beans, method="HDBSCAN")):
    #     print(len(cl))
    #     save_markdown(f"{orch.run_id}-{idx}", "\n".join([b.gist for b in cl]))

    ## test generation
    # infilename = "/home/soumitsr/codes/pycoffeemaker/.test/2025-06-04-20-41-00-5.md"
    # outfilename = f"/home/soumitsr/Dropbox/ObsidianNotes/cafecito.tech/test-md/{infilename.split('/')[-1]}"
    # with open(infilename, "r") as infile:
    #     resp = orch.news_writer.run(infile.read())
    # with open(outfilename, "w") as outfile:
    #     outfile.write(resp.raw)
       

if __name__ == "__main__":
    hydrate_test_db()
    # test_static_db()
    # test_trend_analysis()
    # test_collector_and_scraper()

    # test_collector_orch()
    # test_indexer_orch()
    # test_digestor_orch()
    test_composer_orch()
    # test_run_async()
    # download_test_data("/home/soumitsr/codes/pycoffeemaker/tests/texts-for-nlp.json")

    
   
  