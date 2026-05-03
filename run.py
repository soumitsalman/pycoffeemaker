import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from datetime import datetime as dt

from dotenv import load_dotenv

MAX_WORKERS = os.cpu_count() * os.cpu_count()
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
parser.add_argument("--classifier_batch_size", type=int, help="Batch size for processing")
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
        "PORTER",
        "BEANSACK"
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

    if mode == "COLLECTOR":
        from coffeemaker.orchestrators.collectororch import Collector
        async def run_collector():
            async with AsyncStateCache(cache_path, cache_settings) as cache:
                await Collector(cache=cache).run(
                    os.getenv("COLLECTOR_SOURCES", "./factory/feeds.yaml"),
                    batch_size=batch_size,
                )
        asyncio.run(run_collector())

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
            pass

    elif mode == "CLASSIFIER":
        from coffeemaker.orchestrators.analyzerorch import Indexer
        from coffeemaker.processingcache.firecache import ClassificationCache
        
        cls_cache = ClassificationCache(
            # TODO: change this later
            os.getenv('CLASSIFICATION_CACHE', '.cache/clsstore'), 
            table_settings={
                BEANS: {"id_key": K_URL, "distance_func": "l2"},
                "categories": {"id_key": "category", "distance_func": "cosine"},
                "sentiments": {"id_key": "sentiment", "distance_func": "cosine"}
            }
        )

        orch = Indexer(cache=cache_store, cls_cache=cls_cache)
        while (
            orch.run_classifier(batch_size=batch_size) + \
            orch.run_clusterer(batch_size=batch_size)
        ):
            pass
        cls_cache.close()

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
            )
        )
        
        while (
            orch.run_embedder(batch_size=args.embedder_batch_size or batch_size) + \
            orch.run_extractor(batch_size=args.extractor_batch_size or batch_size)
        ):
            pass
       

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
        orch.hydrate_beansacks(db)
        db.close()

    elif mode == "BEANSACK":
        from coffeemaker.orchestrators.analyzerorch import Indexer
        from coffeemaker.orchestrators.porterorch import Porter
        from coffeemaker.processingcache.firecache import ClassificationCache
        
        cls_cache = ClassificationCache(
            # TODO: change this later
            os.getenv('CLASSIFICATION_CACHE', '.cache/clsstore'), 
            table_settings={
                BEANS: {"id_key": K_URL, "distance_func": "l2"},
                "categories": {"id_key": "category", "distance_func": "cosine"},
                "sentiments": {"id_key": "sentiment", "distance_func": "cosine"}
            }
        )
        indexer = Indexer(
            cache=cache_store,
            embedder_path=os.getenv("EMBEDDER_PATH"),
            embedder_context_len=int(os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN)),
            extractor_path=os.getenv("EXTRACTOR_PATH"),
            extractor_context_len=int(os.getenv("EXTRACTOR_CONTEXT_LEN", EXTRACTOR_CONTEXT_LEN)),
            cls_cache=cls_cache
        )

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
        porter = Porter(cache=cache_store)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
            while True:
                total_indexed = 0
                total_indexed += indexer.run_embedder(batch_size=args.embedder_batch_size or batch_size)
                extractor_future = exec.submit(
                    indexer.run_extractor,
                    batch_size=args.extractor_batch_size or batch_size,
                )
                
                run_classifier_then_clusterer = lambda: \
                    indexer.run_classifier(batch_size=args.classifier_batch_size or batch_size) + \
                    indexer.run_clusterer(batch_size=args.classifier_batch_size or batch_size)
                classifier_clusterer_future = exec.submit(run_classifier_then_clusterer)

                total_indexed += extractor_future.result()
                total_indexed += classifier_clusterer_future.result()

                # nothing to port just break
                if not total_indexed: 
                    break
                
                exec.submit(porter.hydrate_beansacks, db)                
            
        with ThreadPoolExecutor() as exec:
            exec.submit(db.close)
            exec.submit(cls_cache.close)

    else:
        raise ValueError(
            "Invalid mode. Please choose from COLLECTOR, INDEXER, DIGESTOR, EXTRACTOR, ANALYZER, CLASSIFIER, CLUSTERER."
        )
    
    cache_store.close()
    
