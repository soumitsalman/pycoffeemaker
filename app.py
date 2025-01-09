import os
import random
from datetime import datetime as dt

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)

from dotenv import load_dotenv
load_dotenv(CURR_DIR+"/.env")

import logging
logging.basicConfig(
    level=logging.WARNING, 
    filename=f"{WORKING_DIR}/coffeemaker-{dt.now().strftime('%Y-%m-%d-%H')}.log", 
    format="%(asctime)s|%(name)s|%(levelname)s|%(message)s|%(source)s|%(num_items)s")
logger = logging.getLogger("app")
logger.setLevel(logging.INFO)

from coffeemaker import orchestrator as orch

if __name__ == "__main__":    
    start_time = dt.now()
    batch_id = start_time.strftime('%Y-%m-%d %H')
    
    logger.info("started", extra={"source": batch_id, "num_items": 0})
    orch.logger.setLevel(logging.INFO)
    
    orch.initialize(
        os.getenv("DB_CONNECTION_STRING"),
        os.getenv("SB_CONNECTION_STRING"), 
        WORKING_DIR, 
        os.getenv("EMBEDDER_PATH"),    
        os.getenv("LLM_PATH"),
        float(os.getenv('CATEGORY_EPS')),
        float(os.getenv('CLUSTER_EPS')))
    num_items = orch.run(batch_id)
    
    logger.info("finished", extra={"source": batch_id, "num_items": num_items, "execution_time": dt.now()-start_time})

