import asyncio
import os
import argparse
from datetime import datetime as dt
import logging
from dotenv import load_dotenv

EMBEDDER_CONTEXT_LEN = 512
DIGESTOR_CONTEXT_LEN = 4096
COMPOSER_CONTEXT_LEN = 110760

# Set up argument parser
parser = argparse.ArgumentParser(description='Run the coffee maker application')
parser.add_argument('--batch_size', type=int, help='Batch size for processing')
parser.add_argument('--mode', type=str, choices=['COLLECTOR', 'INDEXER', 'DIGESTOR', 'COMPOSER', 'REFRESHER'], 
                    help='Operation mode (COLLECTOR, INDEXER, DIGESTOR, COMPOSER, REFRESHER)')
parser.add_argument('--max_articles', type=int, help='Maximum number of articles to process')

args = parser.parse_args()

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR+"/.env")

log_dir, log_file = os.getenv("LOG_DIR"), None
if log_dir:
    os.makedirs(log_dir, exist_ok=True)
    log_file = f"{log_dir}/coffeemaker-{dt.now().strftime('%Y-%m-%d-%H')}.log"
    
logging.basicConfig(
    level=logging.WARNING, 
    filename=log_file, 
    format="%(asctime)s||%(name)s||%(levelname)s||%(message)s||%(source)s||%(num_items)s")

log = logging.getLogger("app")
log.setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.collectororch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.analyzerorch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.composerorch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.refresherorch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.fullstack").setLevel(logging.INFO)
logging.getLogger("jieba").propagate = False
logging.getLogger("coffeemaker.nlp.agents").propagate = False
logging.getLogger("coffeemaker.nlp.embedders").propagate = False
logging.getLogger("asyncprawcore").propagate = False
logging.getLogger("asyncpraw").propagate = False
logging.getLogger("dammit").propagate = False
logging.getLogger("UnicodeDammit").propagate = False
logging.getLogger("urllib3").propagate = False
logging.getLogger("connectionpool").propagate = False
# logging.getLogger("asyncio").propagate = False

### WORKER SCHEDULING ###
# Collector can run 3 times a day for 1 hour -- 6 AM, 2 PM, 10 PM
# Indexer can run 3 times a day for 30 mins -- 7 AM, 3 PM, 11 PM
# Digestor can run 3 times a day for 30 mins (in GPU) -- 7 AM, 3 PM, 11 PM
# Composer can run 1 time a day for 30 mins -- 5:30 AM (this way the contents will get picked up by indexer and digestor as needed)
# All UI clients can do their porting as they please

if __name__ == "__main__":    
    # Use command line args if provided, otherwise fall back to env vars    
    mode = args.mode or os.getenv("MODE")
    batch_size = int(args.batch_size or os.getenv('BATCH_SIZE') or os.cpu_count())
    
    if mode == "COLLECTOR":
        from coffeemaker.orchestrators.collectororch import Orchestrator
        orch = Orchestrator(
            db_conn_str=(os.getenv("PG_CONNECTION_STRING"), os.getenv("CACHE_DIR")),
            batch_size=batch_size
        )
        asyncio.run(orch.run_async(os.getenv("COLLECTOR_SOURCES", "./factory/feeds.yaml")))
    elif mode == "INDEXER":
        from coffeemaker.orchestrators.analyzerorch import Orchestrator
        orch = Orchestrator(
            db_conn_str=(os.getenv("PG_CONNECTION_STRING"), os.getenv("CACHE_DIR")),
            embedder_path=os.getenv("EMBEDDER_PATH"),
            embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN)),
            batch_size=batch_size
        )
        orch.run_indexer()
    elif mode == "DIGESTOR":
        from coffeemaker.orchestrators.analyzerorch import Orchestrator
        orch = Orchestrator(
            db_conn_str=(os.getenv("PG_CONNECTION_STRING"), os.getenv("CACHE_DIR")),
            digestor_path=os.getenv("DIGESTOR_PATH"), 
            digestor_context_len=int(os.getenv("DIGESTOR_CONTEXT_LEN", DIGESTOR_CONTEXT_LEN)),
            batch_size=batch_size
        )
        orch.run_digestor()
    elif mode == "COMPOSER":
        from coffeemaker.orchestrators.composerorch import Orchestrator
        orch = Orchestrator(
            os.getenv("MONGODB_CONN_STR"),
            os.getenv("DB_NAME"),
            cdn_endpoint=os.getenv("DOSPACES_ENDPOINT"),
            cdn_access_key=os.getenv("DOSPACES_ACCESS_KEY"),
            cdn_secret_key=os.getenv("DOSPACES_SECRET_KEY"),
            composer_path=os.getenv("COMPOSER_PATH"), 
            extractor_path=os.getenv("EXTRACTOR_PATH"),
            composer_base_url=os.getenv("COMPOSER_BASE_URL"),
            composer_api_key=os.getenv("COMPOSER_API_KEY"),
            composer_context_len=int(os.getenv("COMPOSER_CONTEXT_LEN", COMPOSER_CONTEXT_LEN)),
            banner_model=os.getenv("BANNER_MODEL"),
            banner_base_url=os.getenv("BANNER_BASE_URL"),
            banner_api_key=os.getenv("BANNER_API_KEY"),
            embedder_path=os.getenv("EMBEDDER_PATH"),
            embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN)),
            backup_azstorage_conn_str=os.getenv("AZSTORAGE_CONN_STR"),
            max_articles = args.max_articles or int(os.getenv('MAX_ARTICLES', os.cpu_count()))
        )
        orch.run(os.getenv("COMPOSER_TOPICS", "./factory/composer-topics.yaml"))
    elif mode == "REFRESHER":
        from coffeemaker.orchestrators.refresherorch import Orchestrator
        orch = Orchestrator(os.getenv("MONGO_CONN_STR"), os.getenv("MONGO_DATABASE"))
        orch.run()
    else:
        from coffeemaker.orchestrators.fullstack import Orchestrator
        orch = Orchestrator(
            os.getenv("MONGODB_CONN_STR"),
            os.getenv("DB_LOCAL"),
            os.getenv("DB_NAME"),
            os.getenv("AZQUEUE_CONN_STR")
        )
        
        try: asyncio.run(orch.run_async())
        except Exception as e: log.error("failed run", extra={"source": "__BATCH__", "num_items": 1})
        orch.close()
    
 