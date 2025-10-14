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

    domains = [
        {
            "_id": "Artificial Intelligence",
            "kind": "blog",
            "description": "Machine learning algorithms performance benchmarks, neural networks architecture training methods, AI ethics safety guidelines implementation, computer vision object recognition systems, natural language processing models applications, robotics automation industrial deployment, AI research papers methodology results, deep learning model optimization techniques."
        },
        {
            "_id": "Business",
            "kind": "news",
            "description": "Financial markets stock trading analysis, corporate earnings quarterly reports mergers acquisitions, economic indicators GDP employment statistics, retail industry sales trends consumer behavior, workplace automation digital transformation implementation, banking investment strategies risk management, marketing advertising campaign effectiveness data, real estate property market valuations development."
        },
        # {
        #     "_id": "Career & Professional Growth",
        #     "kind": "blog",
        #     "description": "Professional skills development certification programs, job market demand analysis trends, workplace culture remote work practices, career advancement strategies mentorship, leadership management training methods, salary compensation benefits analysis, work-life balance employee wellbeing research, continuing education professional training programs."
        # }
    ]
    res = ic(asyncio.run(orch.run_async(domains)))

    # data = [
    #     (
    #         d,
    #         Metadata(
    #             headline=f"Latest updates in {d['_id']}",
    #             question=f"What are the latest updates in {d['_id']}?",
    #             summary="blah blah",
    #             highlights=["1", "2", "3"],
    #             keywords=[d['_id'], "trends", "latest", "news"]

    #         ),
    #         "sample body",
    #         None
    #     ) for d in domains
    # ]

    # asyncio.run(orch.bulk_publish_articles(data))


def test_refresher_orch():
    from coffeemaker.orchestrators.refresherorch import Orchestrator
    orch = Orchestrator(
        master_conn_str=(os.getenv("PG_CONNECTION_STRING"), ".beansack"),
        replica_conn_str=("mongodb://localhost:27017", "replica1")
    )
    orch.run()

def test_dbcache():
    from dbcache.api import kvstore

    
    cache = kvstore(os.getenv('PG_CONNECTION_STRING'))
    # cache.set("current_snapshot", 15509)
    print(cache.get("current_snapshot")+10)

def cdnstore_test():
    from coffeemaker.pybeansack.cdnstore import CDNStore
    # import os

    store = CDNStore(
        endpoint_url=os.environ.get('PUBLICATIONS_S3_ENDPOINT'),
        region=os.environ.get('S3_REGION'),
        access_key_id=os.environ.get('S3_ACCESS_KEY_ID'),
        secret_key=os.environ.get('S3_SECRET_ACCESS_KEY'),
        bucket=os.environ.get('PUBLICATIONS_S3_BUCKET'),
        public_url=os.environ.get('PUBLICATIONS_PUBLIC_URL')
    )

    article = """<p>Our review of recent technical reports confirms that the acceleration of generative‑AI capabilities is accompanied by a parallel increase in privacy‑related risk vectors, compliance complexity, and governance gaps across sectors.</p>

<p>Data‑harvesting analyses reveal that more than 173 000 YouTube videos from 48 000 channels have been systematically extracted for model training without uniform opt‑out mechanisms. Similar practices are evident on LinkedIn, Quora, and X (Grok), where user‑generated content is ingested at scale. In the Philippines, 85 % of organizations have reported AI‑related cyber incidents, yet only 6 % demonstrate mature cybersecurity readiness. The Cisco 2025 study and Bitdefender 2025 report jointly indicate a high incidence of breach concealment (58 %) and an elevated exposure to prompt‑injection and adversarial‑training attacks.</p>

<p>Technical attack surfaces have broadened as cybercriminals hijack AI pipelines to deploy “evil LLMs” such as FraudGPT and WormGPT, which are available on dark‑web marketplaces for as little as US $100. These models enable automated ransomware, phishing, and credential‑harvesting operations. Microsoft Copilot data in H1 2025 exposed three million sensitive records per organization, illustrating the magnitude of “zombie” data retained in persistent prompt caches. The resulting “enormous data trail” facilitates profiling, black‑mail, and unauthorized data resale.</p>

<p>Regulatory environments are fragmenting. The Philippines’ NPC advisory (2024) establishes AI system standards with fines up to US $90 000 per breach, while the EU AI Act, California SB 53, and Colorado’s pending legislation impose divergent compliance requirements. Assuming a 15 % breach probability across the global internet user base and the US $90 k fine ceiling, aggregate penalties could exceed US $1.6 trillion over five years.</p>

<p>Enterprise adoption of AI is driven by projected economic output of up to US $15 trillion globally by 2030, with the Philippines targeting a P2.8 trillion uplift. However, the same analyses indicate that organizations lacking robust data‑governance frameworks experience a 30 % project abandonment rate, reducing net gains. IDC data show that only 40 % of firms currently invest in “trustworthy AI” guardrails, despite a 60 % higher probability of doubling ROI for those that do.</p>

<p>Model releases such as Claude Sonnet 4.5 and Gemini 2.0 Flash illustrate the industry’s shift toward token efficiency and multimodal capabilities. Both models achieve approximately 15 % token reductions relative to prior generations, translating into estimated compute cost savings of US $150 k per month for enterprises processing ten million tokens daily. Sonnet 4.5’s integration of Sora‑2 video‑audio generation (1080p output at 20 seconds per clip) expands the agentic AI ecosystem, supported by eleven new tutorials for rapid agent development. Gemini 2.0 Flash offers lower inference latency (≤100 ms for text) and broad cloud integration, positioning it as a direct competitor in enterprise deployments.</p>

<p>In education, token‑based pricing models and data‑compression techniques enable campus‑wide LLM deployments with a projected total cost of ownership of US $1.2 million per semester, offset by up to 30 % reductions in manual grading time. Nevertheless, 71 % of workforces and 58 % of faculty report insufficient AI fluency, creating a skills gap that hampers effective integration. Governance frameworks that embed trustworthy‑AI safeguards increase the likelihood of achieving measurable productivity gains, with a projected 35 % reduction in compliance incidents for institutions that implement token‑level provenance tracking.</p>

<p>Technical recommendations emerging from the reports converge on four pillars:</p>

<p>1. <strong>Data Governance:</strong> Enforce provenance tagging, immutable audit logs, and automated data‑minimization pipelines before model training to limit exposure of “zombie” data and satisfy proportionality requirements.</p>

<p>2. <strong>Access Controls:</strong> Deploy role‑based, least‑privilege APIs for LLMs and enforce token‑level revocation for cached prompts in hosted services, mitigating permission sprawl and unauthorized data retrieval.</p>

<p>3. <strong>Adversarial Robustness:</strong> Integrate continuous adversarial training, poisoned‑data detection, and explainable‑AI modules to monitor refusal‑rate decay and prevent self‑evolving agent drift.</p>

<p>4. <strong>Regulatory Alignment:</strong> Map internal policies to the most stringent jurisdiction (e.g., EU AI Act, US SB 53), adopt privacy‑by‑design architectures, and prepare for the US $90 k fine ceiling.</p>

<p>Additional actions include establishing AI‑specific incident response playbooks to raise cybersecurity readiness from the current 6 % to above 50 %, and conducting third‑party risk assessments for all external data sources to prevent inadvertent harvesting. Implementing these controls will reduce the projected incident rate of 85 % in high‑adoption sectors and align AI deployments with emerging global standards.</p>

<p>In summary, the confluence of expansive data harvesting, commoditized malicious LLMs, and fragmented regulatory regimes creates a high‑risk environment that outweighs short‑term economic incentives for organizations lacking robust governance. Immediate adoption of provenance‑driven data governance, stringent access controls, continuous adversarial robustness, and harmonized compliance frameworks is essential to mitigate privacy erosion, avoid multi‑trillion‑dollar penalties, and sustain public trust in generative‑AI systems.</p>"""
    print(store.upload_article(article))

    
import argparse
import subprocess
parser = argparse.ArgumentParser(description="Run pycoffeemaker tests")
parser.add_argument("--hydrate", action="store_true", help="Hydrate local gobeansack database")
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
parser.add_argument("--cdnstore", action="store_true", help="Test S3 store")
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
    if args.cdnstore:
        cdnstore_test()
    # if args.test_fullstack_orch:
    #     test_fullstack_orch()
    # if args.create_test_data_file:
    #     create_test_data_file(args.create_test_data_file)

if __name__ == "__main__":
    main()
