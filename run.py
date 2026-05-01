import argparse
import asyncio
import logging
import os
from datetime import datetime as dt

from dotenv import load_dotenv

EMBEDDER_CONTEXT_LEN = 512
EXTRACTOR_CONTEXT_LEN = 4096
DIGESTOR_CONTEXT_LEN = 4096

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR + "/.env")

log_dir, log_file = os.getenv("LOG_DIR"), None
if log_dir:
    os.makedirs(log_dir, exist_ok=True)
    log_file = f"{log_dir}/coffeemaker-{dt.now().strftime('%Y-%m-%d-%H')}.log"

logging.basicConfig(
    level=logging.WARNING,
    filename=log_file,
    format="%(asctime)s||%(name)s||%(levelname)s||%(message)s||%(source)s||%(num_items)s",
)

log = logging.getLogger("app")
log.setLevel(logging.INFO)
logging.getLogger("collectorworker").setLevel(logging.INFO)
logging.getLogger("analyzerworker").setLevel(logging.INFO)
logging.getLogger("porterworker").setLevel(logging.INFO)
logging.getLogger("processingcache").setLevel(logging.INFO)
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
parser = argparse.ArgumentParser(description="Run the coffee maker application")
parser.add_argument("--batch_size", type=int, help="Batch size for processing")
parser.add_argument("--embedder_batch_size", type=int, help="Batch size for processing")
parser.add_argument("--extractor_batch_size", type=int, help="Batch size for processing")
parser.add_argument("--digestor_batch_size", type=int, help="Batch size for processing")
parser.add_argument(
    "--mode",
    type=str,
    choices=[
        "COLLECTOR",
        "EMBEDDER",
        "DIGESTOR",
        "EXTRACTOR",
        "ANALYZER",
        "CLASSIFIER",
        "CLUSTERER",
        "PORTER",
    ],
    help="Operation mode (COLLECTOR, EMBEDDER, DIGESTOR, EXTRACTOR, ANALYZER, CLASSIFIER, CLUSTERER, PORTER)",
)

from coffeemaker.processingcache.pgcache import AsyncStateCache, StateCache
from pybeansack import create_client, BEANS, PUBLISHERS, CHATTERS, K_URL, K_BASE_URL

if __name__ == "__main__":
    # Use command line args if provided, otherwise fall back to env vars
    args = parser.parse_args()
    mode = args.mode or os.getenv("MODE")
    batch_size = int(args.batch_size or os.getenv("BATCH_SIZE") or os.cpu_count())
    
    cache_path = os.getenv("PROCESSING_CACHE")
    cache_settings = {
        BEANS: {"id_key": K_URL},
        PUBLISHERS: {"id_key": K_BASE_URL},
        CHATTERS: {"id_key": "id"}
    }
    cache_store = StateCache(cache_path, cache_settings)
    async_cache_store = AsyncStateCache(cache_path, cache_settings)    

    if mode == "COLLECTOR":
        from coffeemaker.orchestrators.collectororch import Collector

        orch = Collector(cache=async_cache_store)
        asyncio.run(
            orch.run(
                os.getenv("COLLECTOR_SOURCES", "./factory/feeds.yaml"),
                batch_size=batch_size,
            )
        )

    elif mode == "EMBEDDER":
        from coffeemaker.orchestrators.analyzerorch import Indexer

        orch = Indexer(
            cache=cache_store,
            embedder_path=os.getenv("EMBEDDER_PATH"),
            embedder_context_len=int(
                os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN)
            ),
        )
        while orch.run_embedder(batch_size=batch_size):
            # keep running it while there is something to process
            pass

    elif mode == "CLASSIFIER":
        from coffeemaker.orchestrators.analyzerorch import Indexer
        from coffeemaker.processingcache.firecache import ClassificationCache
        
        cls_cache = ClassificationCache(
            # TODO: change this later
            os.getenv('CLASSIFICATION_CACHE', '.cache')+"/clsstore", 
            table_settings={
                BEANS: {"id_key": K_URL, "distance_func": "l2"},
                "categories": {"id_key": "category", "distance_func": "cosine"},
                "sentiments": {"id_key": "sentiment", "distance_func": "cosine"}
            }
        )

        orch = Indexer(cache=cache_store, cls_cache=cls_cache)
        orch.run_classifier(batch_size=batch_size)
        orch.run_clusterer(batch_size=batch_size)
        cls_cache.close()

    # elif mode == "CLUSTERER":
    #     from coffeemaker.orchestrators.analyzerorch import Indexer

    #     orch = Indexer(cache=cache_store, cls_cache=cls_cache)
    #     orch.run_clusterer(batch_size=batch_size)

    elif mode == "EXTRACTOR":
        from coffeemaker.orchestrators.analyzerorch import Indexer

        orch = Indexer(
            cache=cache_store,
            extractor_path=os.getenv("EXTRACTOR_PATH"),
            extractor_context_len=int(
                os.getenv("EXTRACTOR_CONTEXT_LEN", EXTRACTOR_CONTEXT_LEN)
            ),
        )
        while orch.run_extractor(batch_size=batch_size):
            # keep running it while there is something to process
            pass

    elif mode == "DIGESTOR":
        from coffeemaker.orchestrators.analyzerorch import Indexer

        orch = Indexer(
            cache=cache_store,
            digestor_path=os.getenv("DIGESTOR_PATH"),
            digestor_context_len=int(
                os.getenv("DIGESTOR_CONTEXT_LEN", DIGESTOR_CONTEXT_LEN)
            ),
        )
        while orch.run_digestor(batch_size=batch_size):
            # keep running it while there is something to process
            pass

    # TODO: better naming
    elif mode == "ANALYZER":
        # this combines both embedder, extractor, and digestor
        from coffeemaker.orchestrators.analyzerorch import Indexer

        orch = Indexer(
            cache=cache_store,
            embedder_path=os.getenv("EMBEDDER_PATH"),
            embedder_context_len=int(
                os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN)
            ),
            extractor_path=os.getenv("EXTRACTOR_PATH"),
            extractor_context_len=int(
                os.getenv("EXTRACTOR_CONTEXT_LEN", EXTRACTOR_CONTEXT_LEN)
            ),
            digestor_path=os.getenv("DIGESTOR_PATH"),
            digestor_context_len=int(
                os.getenv("DIGESTOR_CONTEXT_LEN", DIGESTOR_CONTEXT_LEN)
            ),
        )
        orch.run_embedder(batch_size=int(args.embedder_batch_size or batch_size))
        orch.run_extractor(batch_size=int(args.extractor_batch_size or batch_size))
        # orch.run_digestor(batch_size=int(args.digestor_batch_size or batch_size))        

    elif mode == "PORTER":
        from coffeemaker.orchestrators.porterorch import Porter

        db_kwargs = {
            "db_type": os.getenv("DB_TYPE"),
            "mongo_connection_string": os.getenv("MONGO_CONNECTION_STRING"),
            "mongo_database": os.getenv("MONGO_DATABASE"),
            "pg_connection_string": os.getenv("PG_CONNECTION_STRING"),
            "duckdb_storage": os.getenv("DUCKDB_STORAGE"),
            "lancedb_storage": os.getenv("LANCEDB_STORAGE"),
            "ducklake_catalog": os.getenv("DUCKLAKE_CATALOG"),
            "ducklake_storage": os.getenv("DUCKLAKE_STORAGE"),
        }
        db = create_client(**db_kwargs)

        orch = Porter(cache=cache_store)
        while orch.hydrate_beansacks(db, batch_size):
            # keep running it while there is something to port
            pass

        db.close()

    else:
        raise ValueError(
            "Invalid mode. Please choose from COLLECTOR, INDEXER, DIGESTOR, EXTRACTOR, ANALYZER, CLASSIFIER, CLUSTERER."
        )
    
    cache_store.close()
    
