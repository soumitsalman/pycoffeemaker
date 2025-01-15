import asyncio
import os
import random
from datetime import datetime as dt

CURR_DIR = os.path.dirname(os.path.abspath(__file__))


from dotenv import load_dotenv
load_dotenv(CURR_DIR+"/.env")

WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)

import logging
logging.basicConfig(
    level=logging.WARNING, 
    filename=f"{WORKING_DIR}/.logs/coffeemaker-{dt.now().strftime('%Y-%m-%d-%H')}.log", 
    format="%(asctime)s|%(name)s|%(levelname)s|%(message)s|%(source)s|%(num_items)s")
logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
logging.getLogger("orchestrator").setLevel(logging.INFO)
logging.getLogger("jieba").propagate = False
logging.getLogger("local digestor").propagate = False
logging.getLogger("local embedder").propagate = False
logging.getLogger("__package__").propagate = False

from coffeemaker import orchestrator as orch

if __name__ == "__main__":    
    start_time = dt.now()
    batch_id = start_time.strftime('%Y-%m-%d %H')
    
    logger.info("starting", extra={"source": batch_id, "num_items": 0})
    
    orch.initialize(
        os.getenv("DB_CONNECTION_STRING"),
        os.getenv("SB_CONNECTION_STRING"), 
        WORKING_DIR, 
        os.getenv("EMBEDDER_PATH"),    
        os.getenv("LLM_PATH"),
        float(os.getenv('CATEGORY_EPS')),
        float(os.getenv('CLUSTER_EPS')))
    asyncio.run(orch.run_async())
    orch.close()
    
    logger.info("execution time", extra={"source": batch_id, "num_items": dt.now()-start_time})

