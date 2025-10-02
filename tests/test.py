import os, sys
import time
from dotenv import load_dotenv
import requests
import logging
from icecream import ic

load_dotenv()
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


logging.basicConfig(level=logging.WARNING, format="%(asctime)s|%(name)s|%(levelname)s|%(message)s|%(source)s|%(num_items)s")
logger = logging.getLogger("test")
logger.setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.collectororch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.analyzerorch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.composerorch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.refresherorch").setLevel(logging.INFO)
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
from coffeemaker.pybeansack.warehouse import *
from coffeemaker.pybeansack.models import *
from coffeemaker.collectors import collector
from coffeemaker.orchestrators.utils import log_runtime
from concurrent.futures import ThreadPoolExecutor


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

def test_scraper():
    from coffeemaker.collectors import WebScraperLite
    urls = [
        "https://financebuzz.com/retirees-should-buy-at-bjs-4",
        "https://financebuzz.com/southern-lake-towns-afford-social-security",
        "https://financebuzz.com/professional-skills-more-valuable-after-60",
        "https://financebuzz.com/avoid-buying-rv-in-retirement",
        "https://financebuzz.com/trader-joes-pantry-items-june-2025",
        "https://financebuzz.com/cities-getting-caseys-june-2025",
        "https://financebuzz.com/costco-home-decor-guests-want-june-2025"
    ]

    async def run():
        async with WebScraperLite() as scraper:
            for url in urls:
                ic(await scraper.scrape_url(url, False))
    
    asyncio.run(run())

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
    from coffeemaker.pybeansack.mongosack import VALUE_EXISTS

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
    migrate_mongodb("test", "master", from_db_conn=os.getenv('MONGODB_CONN_STR'), to_db_conn=DB_LOCAL_TEST)
    migrate_mongodb("espresso", "master", from_db_conn=os.getenv('MONGODB_CONN_STR'), to_db_conn=DB_LOCAL_TEST)


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
        db_conn_str=(os.getenv("PG_CONNECTION_STRING"), ".beansack/"),
        batch_size=128
    )
    # sources = """/home/soumitsr/codes/pycoffeemaker/factory/feeds.yaml"""
    sources = f"{os.path.dirname(__file__)}/sources-1.yaml"
    # sources = """
    # sources:
    #     rss:
    #         - https://newatlas.com/index.rss
    #         - https://www.channele2e.com/feed/topic/latest
    #         - https://www.ghacks.net/feed/
    #         - https://thenewstack.io/feed
    #         - https://scitechdaily.com/feed/
    #         - https://www.techradar.com/feeds/articletype/news
    #         - https://www.geekwire.com/feed/
    #         - https://investorplace.com/content-feed/
    #     ychackernews:
    #         - https://hacker-news.firebaseio.com/v0/newstories.json
    #     reddit:
    #         - news
    #         - worldnews
    #         - InternationalNews
    #         - GlobalNews
    #         - GlobalMarketNews
    #         - FinanceNews
    #         - StockNews
    #         - CryptoNews
    #         - energyStocks
    # """
    asyncio.run(orch.run_async(sources))
    # orch.run(sources)

def test_indexer_orch():
    from coffeemaker.orchestrators.analyzerorch import Orchestrator
    orch = Orchestrator(
        db_conn_str=(os.getenv("PG_CONNECTION_STRING"), ".beansack/"),
        embedder_path="avsolatorio/GIST-small-Embedding-v0",
        embedder_context_len=512,
        batch_size=8
    )
    orch.run_indexer()

def test_digestor_orch():
    from coffeemaker.orchestrators.analyzerorch import Orchestrator
    orch = Orchestrator(
        db_conn_str=(os.getenv("PG_CONNECTION_STRING"), ".beansack/"),
        digestor_path="soumitsr/led-base-article-digestor",
        digestor_context_len=4096,
        batch_size=8
    )
    orch.run_digestor()

def test_composer_orch():
    from coffeemaker.orchestrators.composerorch import Orchestrator
    from coffeemaker.nlp import ArticleMetadata, agents, NEWSRECAP_SYSTEM_PROMPT
    orch = Orchestrator(
        db_conn_str=(os.getenv("PG_CONNECTION_STRING"), ".beansack/"),
        cdn_endpoint=os.getenv("DOSPACES_ENDPOINT"),
        cdn_access_key=os.getenv("DOSPACES_ACCESS_KEY"),
        cdn_secret_key=os.getenv("DOSPACES_SECRET_KEY"),
        composer_path="openai/gpt-oss-20b",
        extractor_path="openai/gpt-oss-20b",
        composer_base_url=os.getenv("DIGESTOR_BASE_URL"),
        composer_api_key=os.getenv("DIGESTOR_API_KEY"),
        composer_context_len=50000,
        banner_model="black-forest-labs/FLUX-1-dev",
        banner_base_url=os.getenv('DEEPINFRA_BASE_URL'),
        banner_api_key=os.getenv('DEEPINFRA_API_KEY'),
        # backup_azstorage_conn_str=os.getenv("AZSTORAGE_CONN_STR")
    )

    topics = f"{os.path.dirname(__file__)}/composer-topics.json"
    orch.run(topics)
    # for bean in orch.run(topics):
    #     print(">>>>>>>>>>>>>>>>")
    #     print(bean.title)
    #     print(bean.url)
    #     print(bean.image_url)
    #     if bean.highlights: print("ANALYSIS:\n", "\n".join(bean.highlights))
    #     if bean.insights: print("INSIGHTS:\n", "\n".join(bean.insights))
    #     if bean.entities: print("TAGS:\n", ", ".join(bean.entities))
    #     print(bean.content)
    #     print("<<<<<<<<<<<<<<<<")
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

def test_refresher_orch():
    from coffeemaker.orchestrators.refresherorch import Orchestrator
    orch = Orchestrator(
        master_conn_str=(os.getenv("PG_CONNECTION_STRING"), ".beansack"),
        replica_conn_str=("mongodb://localhost:27017", "replica1")
    )
    orch.run()

def hydrate_local_gobeansack():
    import requests
    from concurrent.futures import ThreadPoolExecutor
    from coffeemaker.pybeansack.mongosack import Beansack   
    beansack = Beansack(os.getenv("MONGODB_CONN_STR"), "test")
    gobeansack_url = "http://localhost:8080/publisher"
    test_api_key = "contributor"

    def store_beans(skip: int, limit: int):
        beans = beansack.query_beans(project={K_RELATED: 0}, skip=skip, limit=limit)
        beans = [json.loads(b.model_dump_json(by_alias=True, exclude_none=True, exclude_unset=True, exclude_defaults=True)) for b in beans]
        ic(skip, len(beans))
        ic(requests.post(f"{gobeansack_url}/beans", json=beans, headers={"X-API-KEY": test_api_key}))
        ic(requests.post(f"{gobeansack_url}/beans/embeddings", json=beans, headers={"X-API-KEY": test_api_key}))
        ic(requests.post(f"{gobeansack_url}/beans/tags", json=beans, headers={"X-API-KEY": test_api_key}))

    def store_chatters(skip: int, limit: int):
        chatters = [json.loads(Chatter(**c).model_dump_json(by_alias=True, exclude_none=True, exclude_unset=True, exclude_defaults=True)) for c in beansack.chatterstore.find(projection={K_ID: 0}, skip=skip, limit=limit)]
        ic(skip, len(chatters))
        ic(requests.post(f"{gobeansack_url}/chatters", json=chatters, headers={"X-API-KEY": test_api_key}))

    batch_size = 256
    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        executor.map(lambda x: store_beans(x, batch_size), range(0, 158000, batch_size))
        executor.map(lambda x: store_chatters(x, batch_size), range(0, 318000, batch_size))

def test_local_gobeansack_query():
    import requests
    from rfc3339 import rfc3339
    gobeansack_url = "http://localhost:8080/privileged"
    test_api_key = "regapp"

    def vector_search_beans(embedding):
        search_params = {
            "embedding": embedding, 
            "max_distance": 0.3, 
            "limit": 200, 
            "kind": "news",
            "created_since": rfc3339(datetime.now() - timedelta(days=1))
        }
        resp = requests.get(
            f"{gobeansack_url}/beans/trending/gists",
            json=search_params,
            headers={"X-API-KEY": test_api_key}
        )
        ic(len(resp.json()))

    from coffeemaker.orchestrators.composerorch import _parse_topics


    topics = """/workspaces/beansack/pycoffeemaker/tests/composer-topics.json"""
    topics = _parse_topics(topics)    
    
    with ThreadPoolExecutor(max_workers=32) as executor:
        embeddings = [t[K_EMBEDDING] for t in topics]*2000
        executor.map(lambda x: vector_search_beans(x), embeddings)

def test_dbcache():
    from dbcache.api import kvstore

    
    cache = kvstore(os.getenv('PG_CONNECTION_STRING'))
    # cache.set("current_snapshot", 15509)
    print(cache.get("current_snapshot")+10)

def s3store_test():
    from coffeemaker.pybeansack.s3store import S3Store
    # import os

    store = S3Store(
        endpoint_url=os.environ.get('PUBLICATIONS_S3_ENDPOINT'),
        region=os.environ.get('S3_REGION'),
        access_key_id=os.environ.get('S3_ACCESS_KEY_ID'),
        secret_key=os.environ.get('S3_SECRET_ACCESS_KEY'),
        bucket=os.environ.get('PUBLICATIONS_S3_BUCKET'),
        public_url=os.environ.get('PUBLICATIONS_PUBLIC_URL')
    )

    print(store.upload_bytes(b"<html><body><b>hello</b> world</body></html>", ext='html'))
    print(store.upload_file("/workspaces/beansack/espresso/images/linkedin.png"))

    
import argparse
import subprocess
parser = argparse.ArgumentParser(description="Run pycoffeemaker tests")
parser.add_argument("--hydrate", action="store_true", help="Hydrate local gobeansack database")
# parser.add_argument("--test-local-gobeansack-query", action="store_true", help="Test local gobeansack query")
# parser.add_argument("--hydrate-test-db", action="store_true", help="Hydrate test database")
# parser.add_argument("--test-static-db", action="store_true", help="Test static database")
# parser.add_argument("--test-trend-analysis", action="store_true", help="Test trend analysis")
parser.add_argument("--collect", action="store_true", help="Test collector and scraper")
parser.add_argument("--scrape", action="store_true", help="Test web scraper")
parser.add_argument("--runcollector", action="store_true", help="Test collector orchestrator")
parser.add_argument("--runindexer", action="store_true", help="Test indexer orchestrator")
parser.add_argument("--rundigestor", action="store_true", help="Test digestor orchestrator")
parser.add_argument("--runcomposer", action="store_true", help="Test composer orchestrator")
parser.add_argument("--runrefresher", action="store_true", help="Test refresher orchestrator")
parser.add_argument("--dbcache", action="store_true", help="Test dbcache")
parser.add_argument("--s3store", action="store_true", help="Test S3 store")
# parser.add_argument("--test-fullstack-orch", action="store_true", help="Test fullstack orchestrator")
# parser.add_argument("--create-test-data-file", metavar="OUTPUT_PATH", help="Create test data file at OUTPUT_PATH")

def main():
    
    args = parser.parse_args()

    # if args.hydrate:
    #     hydrate_local_gobeansack()
    # if args.test_local_gobeansack_query:
    #     test_local_gobeansack_query()
    # if args.hydrate_test_db:
    #     hydrate_test_db()
    # if args.test_static_db:
    #     test_static_db()
    # if args.test_trend_analysis:
    #     test_trend_analysis()
    if args.collect:
        test_collector_and_scraper()
    if args.scrape:
        test_scraper()
    if args.runcollector:
        test_collector_orch()
    if args.runindexer:
        test_indexer_orch()
    if args.rundigestor:
        test_digestor_orch()
    if args.runcomposer:
        test_composer_orch()
    if args.runrefresher:
        test_refresher_orch()
    if args.dbcache:
        test_dbcache()
    if args.s3store:
        s3store_test()
    # if args.test_fullstack_orch:
    #     test_fullstack_orch()
    # if args.create_test_data_file:
    #     create_test_data_file(args.create_test_data_file)

if __name__ == "__main__":
    main()
