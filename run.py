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
logging.getLogger("coffeemaker.orchestrators.chainable").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.fullstack").setLevel(logging.INFO)
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
    if mode == "COLLECTOR":
        from coffeemaker.orchestrators.collectororch import Orchestrator
        orch = Orchestrator(
            os.getenv("MONGODB_CONN_STR"),
            os.getenv("DB_NAME")
            # azqueue_conn_str=os.getenv("AZQUEUE_CONN_STR"),
            # output_queue_names=[queue_name.strip() for queue_name in os.getenv("OUTPUT_QUEUE_NAMES").split(",")],
        )
        asyncio.run(orch.run_async())
    elif mode == "INDEXER":
        from coffeemaker.orchestrators.analyzerorch import Orchestrator
        orch = Orchestrator(
            os.getenv("MONGODB_CONN_STR"),
            os.getenv("DB_NAME"),
            # azqueue_conn_str=os.getenv("AZQUEUE_CONN_STR"),
            # input_queue_name=os.getenv("INPUT_QUEUE_NAME"),
            embedder_path=os.getenv("EMBEDDER_PATH"),
            embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN")),
            cluster_distance=float(os.getenv("CLUSTER_EPS", 0))
        )
        orch.run_indexer()
    elif mode == "DIGESTOR":
        from coffeemaker.orchestrators.analyzerorch import Orchestrator
        orch = Orchestrator(
            os.getenv("MONGODB_CONN_STR"),
            os.getenv("DB_NAME"),
            # azqueue_conn_str=os.getenv("AZQUEUE_CONN_STR"),
            # input_queue_name=os.getenv("INPUT_QUEUE_NAME"),
            digestor_path=os.getenv("DIGESTOR_PATH"), 
            digestor_base_url=os.getenv("DIGESTOR_BASE_URL"),
            digestor_api_key=os.getenv("DIGESTOR_API_KEY"),
            digestor_context_len=int(os.getenv("DIGESTOR_CONTEXT_LEN"))
        )
        orch.run_digestor()
    elif mode == "COMPOSER":
        from writers import Orchestrator
        orch = Orchestrator(
            os.getenv("MONGODB_CONN_STR"),
            os.getenv("DB_NAME"),
            embedder_path=os.getenv("EMBEDDER_PATH"),
            embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN")),
            bean_distance=float(os.getenv("BEAN_DISTANCE", 0)),
            writer_path=os.getenv("WRITER_PATH"), 
            writer_base_url=os.getenv("WRITER_BASE_URL"),
            writer_api_key=os.getenv("WRITER_API_KEY"),
            writer_context_len=int(os.getenv("WRITER_CONTEXT_LEN"))
        )
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
    
 