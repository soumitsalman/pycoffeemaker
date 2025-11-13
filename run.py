import asyncio
import os
import argparse
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
logging.getLogger("coffeemaker.orchestrators.refresherorch").setLevel(logging.INFO)
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

### WORKER SCHEDULING ###
# Collector can run 2 times a day for 1-1.5 hours -- 6 AM / 6 PM
# Indexer can run 3 times a day for 30 mins -- 7 AM, 3 PM, 11 PM
# Digestor can run 3 times a day for 30 mins (in GPU) -- 7 AM, 3 PM, 11 PM
# Composer can run 1 time a day for 30 mins -- 5:30 AM (this way the contents will get picked up by indexer and digestor as needed)
# All UI clients can do their porting as they please

# Set up argument parser
parser = argparse.ArgumentParser(description='Run the coffee maker application')
parser.add_argument('--batch_size', type=int, help='Batch size for processing')
parser.add_argument('--embedder_batch_size', type=int, help='Batch size for processing')
parser.add_argument('--digestor_batch_size', type=int, help='Batch size for processing')
parser.add_argument('--mode', type=str, choices=['COLLECTOR', 'INDEXER', 'DIGESTOR', 'ANALYZER', 'COMPOSER', 'REFRESHER'], 
                    help='Operation mode (COLLECTOR, INDEXER, DIGESTOR, COMPOSER, REFRESHER)')
parser.add_argument('--max_articles', type=int, help='Maximum number of articles to process')

if __name__ == "__main__":    
    # Use command line args if provided, otherwise fall back to env vars  
    args = parser.parse_args()  
    mode = args.mode or os.getenv("MODE")
    batch_size = int(args.batch_size or os.getenv('BATCH_SIZE') or os.cpu_count())
    db_conn_str = (os.getenv("PG_CONNECTION_STRING"), os.getenv("STORAGE_DATAPATH"))
    
    if mode == "COLLECTOR":
        from coffeemaker.orchestrators.collectororch import Orchestrator
        orch = Orchestrator(db_conn_str=db_conn_str)
        asyncio.run(orch.run_async(
            os.getenv("COLLECTOR_SOURCES", "./factory/feeds.yaml"),
            batch_size=batch_size
        ))
    elif mode == "INDEXER":
        from coffeemaker.orchestrators.analyzerorch import Orchestrator
        orch = Orchestrator(
            db_conn_str=db_conn_str,
            embedder_path=os.getenv("EMBEDDER_PATH"),
            embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN))
        )
        orch.run_indexer(batch_size=batch_size)
        orch.close()
    elif mode == "DIGESTOR":
        from coffeemaker.orchestrators.analyzerorch import Orchestrator
        orch = Orchestrator(
            db_conn_str=db_conn_str,
            digestor_path=os.getenv("DIGESTOR_PATH"), 
            digestor_context_len=int(os.getenv("DIGESTOR_CONTEXT_LEN", DIGESTOR_CONTEXT_LEN))
        )
        orch.run_digestor(batch_size=batch_size)
        orch.close()
    # this combines both indexer and digestor
    elif mode == "ANALYZER":
        from coffeemaker.orchestrators.analyzerorch import Orchestrator
        orch = Orchestrator(
            db_conn_str=db_conn_str,
            embedder_path=os.getenv("EMBEDDER_PATH"),
            embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN)),
            digestor_path=os.getenv("DIGESTOR_PATH"), 
            digestor_context_len=int(os.getenv("DIGESTOR_CONTEXT_LEN", DIGESTOR_CONTEXT_LEN))
        )
        orch.run(
            embedder_batch_size=int(args.embedder_batch_size or batch_size), 
            digestor_batch_size=int(args.digestor_batch_size or batch_size)
        )
        orch.close()

    elif mode == "COMPOSER":
        from coffeemaker.orchestrators.composerorch import Orchestrator
        orch = Orchestrator(
            db_conn=db_conn_str,           
            embedder_model=os.getenv("EMBEDDER_PATH"),
            analyst_model=os.getenv("ANALYST_MODEL"),
            writer_model=os.getenv("WRITER_MODEL"),
            composer_conn=(
                os.getenv("COMPOSER_BASE_URL"),
                os.getenv("COMPOSER_API_KEY")
            ),
            publisher_conn=(
                os.getenv("PUBLISHER_BASE_URL"),
                os.getenv("PUBLISHER_API_KEY")
            )
        )
        asyncio.run(orch.run_async(os.getenv("COMPOSER_TOPICS", "./factory/composer-topics.yaml")))
    elif mode == "REFRESHER":
        from coffeemaker.orchestrators.refresherorch import Orchestrator
        orch = Orchestrator(
            masterdb_conn_str=db_conn_str,
            espressodb_conn_str=(os.getenv("MONGO_CONNECTION_STRING"), os.getenv("MONGO_DATABASE")),
            ragdb_conn_str=os.getenv("RAGDB_STORAGE_DATAPATH")
        )
        orch.run()
    else:
        raise ValueError("Invalid mode. Please choose from COLLECTOR, INDEXER, DIGESTOR, COMPOSER, REFRESHER.")
 