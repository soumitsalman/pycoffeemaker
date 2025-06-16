import asyncio
import os
from datetime import datetime as dt
import logging
from dotenv import load_dotenv

EMBEDDER_CONTEXT_LEN = 512
DIGESTOR_CONTEXT_LEN = 4096
COMPOSER_CONTEXT_LEN = 110760

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

if __name__ == "__main__":    
    mode = os.getenv("MODE")
    if mode == "COLLECTOR":
        from coffeemaker.orchestrators.collectororch import Orchestrator
        orch = Orchestrator(
            os.getenv("MONGODB_CONN_STR"),
            os.getenv("DB_NAME")
        )
        asyncio.run(orch.run_async(os.getenv("COLLECTOR_SOURCES", "./factory/feeds.yaml")))
    elif mode == "INDEXER":
        from coffeemaker.orchestrators.analyzerorch import Orchestrator
        orch = Orchestrator(
            os.getenv("MONGODB_CONN_STR"),
            os.getenv("DB_NAME"),
            embedder_path=os.getenv("EMBEDDER_PATH"),
            embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN)),
            category_defs=os.getenv('INDEXER_CATEGORIES', "./factory/categories.parquet"),
            sentiment_defs=os.getenv('INDEXER_SENTIMENTS', "./factory/sentiments.parquet")
        )
        orch.run_indexer()
    elif mode == "DIGESTOR":
        from coffeemaker.orchestrators.analyzerorch import Orchestrator
        orch = Orchestrator(
            os.getenv("MONGODB_CONN_STR"),
            os.getenv("DB_NAME"),
            digestor_path=os.getenv("DIGESTOR_PATH"), 
            digestor_base_url=os.getenv("DIGESTOR_BASE_URL"),
            digestor_api_key=os.getenv("DIGESTOR_API_KEY"),
            digestor_context_len=int(os.getenv("DIGESTOR_CONTEXT_LEN", DIGESTOR_CONTEXT_LEN))
        )
        orch.run_digestor()
    elif mode == "COMPOSER":
        from coffeemaker.orchestrators.composerorch import Orchestrator
        orch = Orchestrator(
            os.getenv("MONGODB_CONN_STR"),
            os.getenv("DB_NAME"),
            composer_path=os.getenv("COMPOSER_PATH"), 
            composer_base_url=os.getenv("COMPOSER_BASE_URL"),
            composer_api_key=os.getenv("COMPOSER_API_KEY"),
            composer_context_len=int(os.getenv("COMPOSER_CONTEXT_LEN", COMPOSER_CONTEXT_LEN)),
            embedder_path=os.getenv("EMBEDDER_PATH"),
            embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN)),
            backup_azstorage_conn_str=os.getenv("AZSTORAGE_CONN_STR")
        )
        orch.run("./factory/composer-topics.yaml")
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
    
 