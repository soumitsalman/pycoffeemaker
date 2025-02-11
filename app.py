import asyncio
import os
from datetime import datetime as dt

CURR_DIR = os.path.dirname(os.path.abspath(__file__))

from dotenv import load_dotenv
load_dotenv(CURR_DIR+"/.env")

WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)

import logging
logging.basicConfig(
    level=logging.WARNING, 
    filename=f"{WORKING_DIR}/.logs/coffeemaker-{dt.now().strftime('%Y-%m-%d-%H')}.log", 
    format="%(asctime)s||%(name)s||%(levelname)s||%(message)s||%(source)s||%(num_items)s")
log = logging.getLogger("app")
log.setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrator").setLevel(logging.INFO)
logging.getLogger("jieba").propagate = False
logging.getLogger("digestor.local").propagate = False
logging.getLogger("embedder.local").propagate = False
logging.getLogger("asyncprawcore").propagate = False
logging.getLogger("asyncpraw").propagate = False
logging.getLogger("dammit").propagate = False
logging.getLogger("UnicodeDammit").propagate = False
logging.getLogger("urllib3").propagate = False
logging.getLogger("connectionpool").propagate = False

from coffeemaker.orchestrator import Orchestrator

if __name__ == "__main__":    
    orch = Orchestrator(
        os.getenv("DB_CONNECTION_STRING"),
        os.getenv("AZSTORAGE_CONNECTION_STRING"), 
        WORKING_DIR, 
        os.getenv("EMBEDDER_PATH"),    
        os.getenv("LLM_PATH"),
        float(os.getenv('CATEGORY_EPS')),
        float(os.getenv('CLUSTER_EPS')))
    asyncio.run(orch.run_async())
    orch.close()
    
 