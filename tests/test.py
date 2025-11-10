import os, sys
import time
from dotenv import load_dotenv
import requests
import logging
from icecream import ic
from slugify import slugify
from faker import Faker

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
    from coffeemaker.nlp.src.models import Metadata

    orch = Orchestrator(
        db_conn=(
            os.getenv("PG_CONNECTION_STRING"), 
            ".beansack/"
        ),
        embedder_model="avsolatorio/GIST-small-Embedding-v0",
        analyst_model="openai/gpt-oss-20b",
        writer_model="openai/gpt-oss-120b",
        composer_conn=(
            os.getenv("COMPOSER_BASE_URL"),
            os.getenv("COMPOSER_API_KEY")
        ),
        publisher_conn=(
            os.getenv("PUBLISHER_BASE_URL"),
            os.getenv("PUBLISHER_API_KEY")
        )
    )

    domains = parse_topics("/workspaces/beansack/pycoffeemaker/tests/composer-topics.json")   

    for domain in domains:
        print("============ DOMAIN:", domain["_id"], "=============")
        res = asyncio.run(orch.compose_article(domain))
        if not res:
            print("No article composed")
        else:
            print(res[1])
            print("Title:", res[0].headline)
            print("Tags:", res[0].keywords)
        # res = asyncio.run(orch._get_beans_for_domain(domain))
        # [print(bean.digest(),"\n------------------------------") for bean in res]
        # res = asyncio.run(orch._get_beans_for_domain(domain))
        # print(domain["_id"], ":", len(res) if res else 0)

     # topics = [
    #     "Microsoft expands Copilot Studio with GPT‑5 and enterprise agent runtime",
    #     "Nation-State Hackers Breach F5 Networks, Exfiltrate BIG‑IP Source Code and Vulnerability Data",
    #     "Digital Marketing Spend Shifts Toward Direct Channels for Banks",
    #     "October 2025 Government Shutdown Fires Over 4,500 Employees, Exposes RIF and Wage Issues",
    #     "Deel Secures $300M Series E Funding, Valued at $17.3B Amid Talent and Pay Growth"
    # ]
    # for topic in topics:
    #     print("TOPIC:", topic)
    #     res = asyncio.run(
    #         orch._query_beans(
    #             kind=None,
    #             last_ndays=1,
    #             query_text=f"Topic: {topic}",
    #             query_emb=None,
    #             distance=0.2,
    #             limit=8
    #         )
    #     )
    #     [print(bean.digest(),"\n------------------------------") for bean in res]
    #     print("==============================\n")

  
def test_refresher_orch():
    from coffeemaker.orchestrators.refresherorch import Orchestrator
    orch = Orchestrator(
        master_conn_str=(os.getenv("PG_CONNECTION_STRING"), ".beansack"),
        replica_conn_str=("mongodb://localhost:27017", "replica1")
    )
    orch.run()

def test_readonly_warehouse():
    from coffeemaker.pybeansack.warehouse_readonly import Beansack
    from coffeemaker.pybeansack.models import K_URL, K_CREATED
    from coffeemaker.nlp.src import embedders
   
    db = Beansack(
        catalogdb=os.getenv("PG_CONNECTION_STRING"),
        storagedb=os.getenv("STORAGE_DATAPATH")    
    )
    embedder = embedders.from_path(os.getenv('EMBEDDER_PATH'), EMBEDDER_CONTEXT_LEN)
    topics = []
    
    beans = db.query_processed_beans(limit=5, columns = [K_URL, K_CREATED, "gist", "categories", "sentiments"])
    [print(bean.digest()) for bean in beans]

def test_warehousev2():
    from coffeemaker.pybeansack.warehousev2 import Beansack, DIGEST_COLUMNS
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

def test_cupboard():
    from coffeemaker.orchestrators.cupboard import CupboardDB, Mug, Sip, EmbeddingAdapter
    
    db = CupboardDB(db_path=".test/cupboard0/")
    fake = Faker()

    def store_sips():
        for _ in range(fake.random_int(min=2, max=5)):
            title = fake.sentence(nb_words=8)
            date = fake.date_between(start_date='-1y', end_date='today')
            sip = Sip(
                id=slugify(f"{title} {date}"),
                created=date,
                title=title,
                content=fake.paragraph(nb_sentences=3)
            )
            db.add(sip)
            yield sip

    def store_mugs():
        for _ in range(fake.random_int(min=3, max=8)):
            sips = list(store_sips())
            title = fake.sentence(nb_words=6)
            date = fake.date_between(start_date='-1y', end_date='today')
            mug = Mug(
                id=slugify(f"{title} {date}"),
                title=title,
                content=fake.paragraph(nb_sentences=5),
                created=date,
                updated=fake.date_between(start_date=date, end_date='today'),
                sips=[sip.id for sip in sips],
                tags=[fake.word() for _ in range(fake.random_int(min=1, max=4))],
                highlights=[sip.title for sip in sips]
            )
            db.add(mug)
            yield mug


    list(store_mugs())

    ic(db.query_sips(query_text=fake.sentence(nb_words=6), distance=0.9, limit=5))
    
    
   
    
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
parser.add_argument("--warehousev2", action="store_true", help="Test warehouse v2")
parser.add_argument("--cupboard", action="store_true", help="Test cupboard orchestrator")

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
    if args.readonly:
        test_readonly_warehouse()
    if args.warehousev2:
        test_warehousev2()
    if args.cupboard:
        test_cupboard()
    # if args.test_fullstack_orch:
    #     test_fullstack_orch()
    # if args.create_test_data_file:
    #     create_test_data_file(args.create_test_data_file)

if __name__ == "__main__":
    main()
