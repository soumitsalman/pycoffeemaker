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
logging.getLogger("local digestor").propagate = False
logging.getLogger("local embedder").propagate = False
logging.getLogger("prawcore").propagate = False
logging.getLogger("dammit").propagate = False
logging.getLogger("UnicodeDammit").propagate = False
logging.getLogger("urllib3").propagate = False
logging.getLogger("connectionpool").propagate = False

from coffeemaker import orchestrator as orch

if __name__ == "__main__":    
    start_time = dt.now()
    batch_id = "batch "+start_time.strftime('%Y-%m-%d %H')
    
    log.info("starting", extra={"source": batch_id, "num_items": 0})
    
    orch.initialize(
        os.getenv("DB_CONNECTION_STRING"),
        os.getenv("AZSTORAGE_CONNECTION_STRING"), 
        WORKING_DIR, 
        os.getenv("EMBEDDER_PATH"),    
        os.getenv("LLM_PATH"),
        float(os.getenv('CATEGORY_EPS')),
        float(os.getenv('CLUSTER_EPS')))
    asyncio.run(orch.run_async())
    orch.close()
    
    log.info("execution time", extra={"source": batch_id, "num_items": int((dt.now()-start_time).total_seconds())})

