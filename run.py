import asyncio
import os
from datetime import datetime as dt
import logging
from dotenv import load_dotenv
from icecream import ic

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
logging.getLogger("coffeemaker.orchestrators.collectoronly").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.indexeronly").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.digestoronly").setLevel(logging.INFO)
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
# logging.getLogger("asyncio").propagate = False

if __name__ == "__main__":    
    mode = os.getenv("MODE")
    if mode == "COLLECTOR_ONLY":
        from coffeemaker.orchestrators.collectoronly import Orchestrator
        orch = Orchestrator(
            os.getenv("DB_REMOTE"),
            os.getenv("DB_NAME"),
            os.getenv("QUEUE_PATH"),
            [queue_name.strip() for queue_name in os.getenv("COLLECTOR_OUT_QUEUES").split(",")],
        )
        orch.run()
    elif mode == "INDEXER_ONLY":
        from coffeemaker.orchestrators.indexeronly import Orchestrator
        orch = Orchestrator(
            os.getenv("DB_REMOTE"),
            os.getenv("DB_NAME"),
            os.getenv("QUEUE_PATH"),
            os.getenv("INDEXER_IN_QUEUE")
        )
        orch.run()
    elif mode == "DIGESTOR_ONLY":
        from coffeemaker.orchestrators.digestoronly import Orchestrator
        orch = Orchestrator(
            os.getenv("DB_REMOTE"),
            os.getenv("DB_NAME"),
            os.getenv("QUEUE_PATH"),
            os.getenv("DIGESTOR_IN_QUEUE")
        )
        orch.run()
    else:
        from coffeemaker.orchestrators.fullstack import Orchestrator
        orch = Orchestrator(
            os.getenv("DB_REMOTE"),
            os.getenv("DB_LOCAL"),
            os.getenv("DB_NAME"),
            os.getenv("AZSTORAGE_CONNECTION_STRING")
        )
        
        try: asyncio.run(orch.run_async())
        except Exception as e: log.error("failed run", extra={"source": "__BATCH__", "num_items": 1})
        orch.close()
    
 