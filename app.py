import asyncio
import os
from datetime import datetime as dt
import logging
from dotenv import load_dotenv
from icecream import ic

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR+"/.env")

if not os.path.exists(f"{CURR_DIR}/.logs"): os.makedirs(f"{CURR_DIR}/.logs")
logging.basicConfig(
    level=logging.WARNING, 
    filename=f"{CURR_DIR}/.logs/coffeemaker-{dt.now().strftime('%Y-%m-%d-%H')}.log", 
    format="%(asctime)s||%(name)s||%(levelname)s||%(message)s||%(source)s||%(num_items)s")

log = logging.getLogger("app")
log.setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.simplecollector").setLevel(logging.INFO)
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
        from coffeemaker.orchestrators.simplecollector import Orchestrator
        orch = Orchestrator(
            os.getenv("DB_REMOTE"),
            os.getenv("DB_NAME"),
            os.getenv("INDEXING_QUEUE_PATH"),
            os.getenv("INDEXING_QUEUE_NAME")
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
    
 