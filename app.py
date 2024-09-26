import os
from datetime import datetime as dt
from dotenv import load_dotenv
from pybeansack import utils


CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR+"/.env")
WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)
LOG_FILE = os.getenv('LOG_FILE')
if LOG_FILE:
    utils.set_logger_path(WORKING_DIR+"/"+LOG_FILE)

from coffeemaker import orchestrator as orch

orch.initialize(
    os.getenv("DB_CONNECTION_STRING"),
    os.getenv("SB_CONNECTION_STRING"), 
    WORKING_DIR, 
    os.getenv("EMBEDDER_PATH"),
    None,   
    float(os.getenv('CATEGORY_EPS')),
    float(os.getenv('CLUSTER_EPS')))

start_time = dt.now()
orch.run_collection()
orch.run_indexing()
orch.run_clustering()
orch.run_trend_ranking()
orch.run_augmentation()
orch.logger.info("Run Completed! Total Time: %s", str(dt.now()-start_time))