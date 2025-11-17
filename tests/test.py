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

def test_publisher_scraper():
    from coffeemaker.collectors import PublisherScraper
    from coffeemaker.pybeansack import models, warehouse
    urls = [
        "https://financebuzz.com/retirees-should-buy-at-bjs-4",
        "https://financebuzz.com/southern-lake-towns-afford-social-security",
        "https://financebuzz.com/professional-skills-more-valuable-after-60",
        "https://www.cyberark.com/blog/distinguishing-authn-and-authz/",
        "http://jalammar.github.io/ai-image-generation-tools/",
        "https://aws.amazon.com/blogs/aws/introducing-aws-capabilities-by-region-for-easier-regional-planning-and-faster-global-deployments/",
        "https://www.camilleroux.com/la-veille-technologique-ma-methode-complete-pour-rester-a-jour/",
        "https://www.neelc.org/posts/freebsd-signal-proxy/",
        "https://aws.amazon.com/blogs/storage/cross-account-amazon-s3-bulk-transfers-with-enhanced-aws-kms-support/",
        "https://aws.amazon.com/blogs/storage/cross-account-disaster-recovery-setup-using-aws-elastic-disaster-recovery-in-secured-networks-part-1-architecture-and-network-setup/",
        "https://aws.amazon.com/blogs/storage/mastering-cross-account-amazon-efs-seamlessly-mount-amazon-efs-on-amazon-eks-cluster/"
    ]
    db = warehouse.Beansack(
        catalogdb=os.getenv("PG_CONNECTION_STRING"),
        storagedb=os.getenv("STORAGE_DATAPATH"),
        factory_dir="factory"
    )   

    async def run():
        async with PublisherScraper() as scraper:
            ic(await scraper.scrape_urls(urls))
            pubs = db.query_publishers(conditions=["rss_feed IS NULL", "favicon IS NULL", "site_name IS NULL"], limit=5)
            ic(await scraper.scrape_publishers(pubs))            
    
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
    # migrate_mongodb("test", "master", from_db_conn=os.getenv('MONGODB_CONN_STR'), to_db_conn=DB_LOCAL_TEST)
    migrate_mongodb("espresso", "espresso1", from_db_conn=os.getenv('MONGO_CONNECTION_STRING'), to_db_conn=DB_LOCAL_TEST)


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
    orch = Orchestrator(db_conn_str=(os.getenv("PG_CONNECTION_STRING"), os.getenv("STORAGE_DATAPATH")))
    # sources = """/home/soumitsr/codes/pycoffeemaker/factory/feeds.yaml"""
    # sources = f"{os.path.dirname(__file__)}/sources-1.yaml"
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
    asyncio.run(orch.run_async(sources, batch_size=128))
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
    from coffeemaker.orchestrators.composerorch import Orchestrator, parse_topics
    from coffeemaker.pybeansack import lancesack as ls
    from coffeemaker.pybeansack.models import Mug, Sip, Bean

    orch = Orchestrator(
        db_conn=(
            os.getenv("PG_CONNECTION_STRING"), 
            os.getenv("STORAGE_DATAPATH")
        ),
        embedder_model="avsolatorio/GIST-small-Embedding-v0",
        analyst_model="openai/gpt-oss-20b",
        writer_model="openai/gpt-oss-120b",
        composer_conn=(
            os.getenv("COMPOSER_BASE_URL"),
            os.getenv("COMPOSER_API_KEY")
        ),
        cupboard_conn_str=os.getenv("RAGDB_STORAGE_DATAPATH")
    )
    articles = asyncio.run(orch.run_async(os.path.dirname(__file__) + "/composer-topics.json"))
    if not articles: raise Exception("No articles composed")
    [print(a[K_CONTENT], "\n======================\n") for a in articles]

    # domains = parse_topics(os.path.dirname(__file__) + "/composer-topics.json")  
    # domains = orch._create_topic_embeddings(domains)
    # cupboard = ls.Beansack(".beansack/lancesack")

    # # sips = cupboard.allsips.search().limit(5).to_pydantic(ls._Sip)
    # # ic([(sip.title, sip.embedding[:5]) for sip in sips])

    # async def run():
    #     for domain in domains:
    #         print("============ DOMAIN:", domain["_id"], "=============")
    #         beans = cupboard.allbeans \
    #             .search(domain[K_EMBEDDING], query_type="vector", vector_column_name=K_EMBEDDING, ordering_field_name=K_CREATED) \
    #             .distance_type("cosine") \
    #             .distance_range(upper_bound=0.3) \
    #             .where(f"created >= date '{ndays_ago_str(2)}'") \
    #             .limit(10) \
    #             .to_pydantic(ls._Bean)
    #         for bean in beans:                
    #             sips = cupboard.allsips \
    #                 .search(bean.embedding, query_type="vector", vector_column_name=K_EMBEDDING, ordering_field_name=K_CREATED) \
    #                 .distance_type("cosine") \
    #                 .distance_range(upper_bound=0.15) \
    #                 .limit(5) \
    #                 .to_pydantic(ls._Sip)
    #             if sips: ic(bean.title, [sip.title for sip in sips])

    # asyncio.run(run())
  
def test_refresher_orch():
    from coffeemaker.orchestrators.refresherorch import Orchestrator
    orch = Orchestrator(
        masterdb_conn_str=(os.getenv("PG_CONNECTION_STRING"), ".beansack/prod/storage"),
        # espressodb_conn_str=("mongodb://localhost:27017", "replica1")
        espressodb_conn_str=(None),
        ragdb_conn_str=".beansack/lancesack/"
    )
    orch.run()

def test_warehouse():
    from coffeemaker.pybeansack.warehouse import Beansack, DIGEST_COLUMNS
    from coffeemaker.pybeansack.models import K_URL, K_CREATED, K_CONTENT, Bean
    from coffeemaker.collectors.collector import APICollector, parse_sources
    from coffeemaker.orchestrators.composerorch import parse_topics
    from coffeemaker.nlp.src import embedders, agents
    from tqdm import tqdm
   
    db = Beansack(
        catalogdb="sqlite:.test/beansack/catalogdb.db",
        storagedb=".test/beansack/storage",
        factory_dir="factory"
    )
    col = APICollector(batch_size=128)
    sources = parse_sources(f"{os.path.dirname(__file__)}/sources-2.yaml")
    embedder = embedders.from_path(os.getenv('EMBEDDER_PATH'), EMBEDDER_CONTEXT_LEN)
    
    async def run_collector():
        async with col:
            for rss in sources['rss'][:5]:
                beans = await col.collect_rssfeed_async(rss)
                ic(db.store_beans(beans))
    if False: asyncio.run(run_collector())

    def run_indexer():
        while beans := db.query_latest_beans(exprs=["embedding IS NULL", "content_length >= 10"], limit=8, select = [K_URL, K_CONTENT, K_CREATED]):
            vecs = embedder.embed_documents([bean.content for bean in beans])
            beans = [Bean(url=bean.url, embedding=vec) for bean, vec in zip(beans, vecs) if vec]
            ic(db.update_beans(beans, columns=[K_EMBEDDING]))
    if False: run_indexer()

    if False: ic(db.refresh_classifications())
    if False: ic(db.refresh_clusters())

    def run_vector_query():
        topics = parse_topics("/workspaces/beansack/pycoffeemaker/tests/composer-topics.json")
        vecs = embedder.embed_documents([topic[K_DESCRIPTION] for topic in topics])*100
        
        def query(vec):
            limit = random.randint(24,256)
            beans=db.query_trending_beans(updated=ndays_ago(7), embedding=vec, distance=0.3, limit=limit, columns=DIGEST_COLUMNS)
            pbar.update(1)
            # ic(limit, len(beans))
        
        with tqdm(total=len(vecs)) as pbar:
            with ThreadPoolExecutor(max_workers=30) as executor:
                executor.map(query, vecs)
            # list(map(query, vecs))
    if False: run_vector_query()

    if True: ic(db.query_aggregated_chatters(updated=ndays_ago(7), limit=16))

    db.close()

def test_dbcache():
    from dbcache.api import kvstore    
    cache = kvstore(os.getenv('PG_CONNECTION_STRING'))
    # cache.set("current_snapshot", 15509)
    print(cache.get("current_snapshot")+10)

def test_orch_on_lancesack():
    from coffeemaker.orchestrators.collectororch import parse_sources
    from coffeemaker.collectors.collector import APICollector
    from coffeemaker.pybeansack.lancesack import Beansack, _Bean
    from coffeemaker.nlp.src import embedders, digestors, Digest

    db = Beansack(".beansack/lancesack")
    if False:
        feeds = parse_sources(f"{os.path.dirname(__file__)}/sources-1.yaml")
        collector = APICollector(batch_size=64)
        for rss in random.sample(feeds['rss'], 5):
            items = collector.collect_rssfeed(rss)
            ic(db.store_beans([item['bean'] for item in items if item.get('bean')]))
            ic(db.store_publishers([item['publisher'] for item in items if item.get("publisher")]))
            ic(db.store_chatters([item['chatter'] for item in items if item.get("chatter")]))

    if True:
        with embedders.from_path(os.getenv('EMBEDDER_PATH'), EMBEDDER_CONTEXT_LEN) as embedder:
            while beans := db.allbeans.search().where("embedding IS NULL AND content IS NOT NULL AND content <> ''").limit(16).select([K_URL, K_CONTENT, K_SOURCE]).to_pydantic(_Bean):
                beans = [bean for bean in beans if bean.content]
                vectors = embedder.embed_documents([bean.content for bean in beans])
                updates = [Bean(url=bean.url, embedding=vec) for bean, vec in zip(beans, vectors) if vec]
                # ic(db.update_beans(beans, columns=["embedding"]))
                ic(db.update_embeddings(updates))

    if True:
        with digestors.from_path(os.getenv('DIGESTOR_PATH'), max_input_tokens=4096, max_output_tokens=384, output_parser=Digest.parse_compressed) as digestor:
            while beans := db.allbeans.search().where("gist IS NULL AND content IS NOT NULL AND content <> ''").limit(2).select([K_URL, K_CONTENT, K_SOURCE]).to_pydantic(_Bean):
                beans = [bean for bean in beans if bean.content]
                digests = digestor.run_batch([bean.content for bean in beans])
                updates = [Bean(url=bean.url, gist=d.raw, regions=d.regions, entities=d.entities) for bean, d in zip(beans, digests) if d]
                ic(db.update_beans(updates, columns=[K_GIST, K_REGIONS, K_ENTITIES]))
   
import argparse
import subprocess
parser = argparse.ArgumentParser(description="Run pycoffeemaker tests")
parser.add_argument("--hydrate", action="store_true", help="Hydrate local gobeansack database")
# parser.add_argument("--hydrate-test-db", action="store_true", help="Hydrate test database")
# parser.add_argument("--test-static-db", action="store_true", help="Test static database")
# parser.add_argument("--test-trend-analysis", action="store_true", help="Test trend analysis")
parser.add_argument("--collect", action="store_true", help="Test collector and scraper")
parser.add_argument("--scrapepubs", action="store_true", help="Test publisher scraper")
parser.add_argument("--scrape", action="store_true", help="Test web scraper")
parser.add_argument("--runcollector", action="store_true", help="Test collector orchestrator")
parser.add_argument("--runindexer", action="store_true", help="Test indexer orchestrator")
parser.add_argument("--rundigestor", action="store_true", help="Test digestor orchestrator")
parser.add_argument("--runcomposer", action="store_true", help="Test composer orchestrator")
parser.add_argument("--runrefresher", action="store_true", help="Test refresher orchestrator")
parser.add_argument("--dbcache", action="store_true", help="Test dbcache")
parser.add_argument("--readonly", action="store_true", help="Test readonly warehouse")
parser.add_argument("--warehouse", action="store_true", help="Test warehouse v2")
parser.add_argument("--cupboard", action="store_true", help="Test cupboard orchestrator")
parser.add_argument("--orchonlance", action="store_true", help="Test lancesack orchestrator")

# parser.add_argument("--test-fullstack-orch", action="store_true", help="Test fullstack orchestrator")
# parser.add_argument("--create-test-data-file", metavar="OUTPUT_PATH", help="Create test data file at OUTPUT_PATH")

def main():
    
    args = parser.parse_args()

    if args.hydrate:
        hydrate_test_db()
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
    if args.scrapepubs:
        test_publisher_scraper()
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
    if args.warehouse:
        test_warehouse()
    if args.orchonlance: test_orch_on_lancesack()
    # if args.test_fullstack_orch:
    #     test_fullstack_orch()
    # if args.create_test_data_file:
    #     create_test_data_file(args.create_test_data_file)

if __name__ == "__main__":
    main()
