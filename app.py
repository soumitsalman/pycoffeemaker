import logging
import os
from datetime import datetime as dt
import time
from dotenv import load_dotenv

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR+"/.env")
WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)
logging.basicConfig(
    filename=f"{WORKING_DIR}/coffeemaker-{time.strftime('%Y-%m-%d-%H', time.localtime())}.log", 
    level=logging.WARNING, 
    datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s|%(name)s|%(levelname)s|%(message)s"
)
# logging.getLogger('httpx').setLevel(logging.ERROR)
# logging.getLogger('openai._base_client').setLevel(logging.ERROR)
# logging.getLogger('persistqueue').setLevel(logging.ERROR)
# logging.getLogger('persistqueue.serializers.pickle').setLevel(logging.ERROR)
# logging.getLogger('pymongo.client').setLevel(logging.ERROR)
# logging.getLogger('azure.servicebus._pyamqp._connection').setLevel(logging.ERROR)
# logging.getLogger('azure.servicebus._pyamqp.session').setLevel(logging.ERROR)
# logging.getLogger('azure.servicebus._pyamqp.link').setLevel(logging.ERROR)
# logging.getLogger('azure.servicebus._pyamqp.management_link').setLevel(logging.ERROR)
logging.getLogger("app").setLevel(logging.INFO)
logging.getLogger("orchestrator").setLevel(logging.INFO)
logging.getLogger("local digestor").setLevel(logging.INFO)
logging.getLogger("remote digestor").setLevel(logging.INFO)
logging.getLogger("local embedder").setLevel(logging.INFO)
logging.getLogger("remote embedder").setLevel(logging.INFO)
logging.getLogger("beansack").setLevel(logging.INFO)

from coffeemaker import orchestrator as orch

orch.initialize(
    os.getenv("DB_CONNECTION_STRING"),
    os.getenv("SB_CONNECTION_STRING"), 
    WORKING_DIR, 
    os.getenv("EMBEDDER_PATH"),
    # os.getenv("LLM_PATH"),
    None,
    os.getenv("LLM_BASE_URL"),
    os.getenv("LLM_API_KEY"),
    os.getenv("LLM_MODEL"),
    None, None, None,
    float(os.getenv('CATEGORY_EPS')),
    float(os.getenv('CLUSTER_EPS')))

start_time = time.time()
orch.run_cleanup()
orch.run_collection()
orch.run_indexing_and_augmenting()
orch.run_clustering()
orch.run_trend_ranking()
# orch.run_augmentation()
logging.getLogger("app").info("execution time|%s|%d", "__batch__", int(time.time()-start_time))
