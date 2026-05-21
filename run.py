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
CONSOLIDATOR_CONTEXT_LEN = 65536

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
# Embedder / Extractor / Classifier can run 3 times a day for 30 mins -- 7 AM, 3 PM, 11 PM
# Digestor can run 3 times a day for 30 mins (in GPU) -- 7 AM, 3 PM, 11 PM
# Porter clients can run as needed

# Set up argument parser
parser = argparse.ArgumentParser(description="Run the coffee maker application")
parser.add_argument("--batch_size", type=int, help="Batch size for processing")
# parser.add_argument("--embedder_batch_size", type=int, help="Batch size for processing")
# parser.add_argument("--extractor_batch_size", type=int, help="Batch size for processing")
# parser.add_argument("--digestor_batch_size", type=int, help="Batch size for processing")
# parser.add_argument("--classifier_batch_size", type=int, help="Batch size for processing")
parser.add_argument(
    "--mode",
    type=str,
    choices=[
        "COLLECTOR",
        "EMBEDDER",
        "DIGESTOR",
        "EXTRACTOR",
        "CLASSIFIER",
        "CONSOLIDATOR",
        "PORTER",
    ],
    help="Operation mode (COLLECTOR, EMBEDDER, DIGESTOR, EXTRACTOR, CLASSIFIER, CONSOLIDATOR, PORTER)",
)

from coffeemaker.processingcache.pgcache import AsyncStateCache, StateCache
from coffeemaker.orchestrators.utils import BEANSACKED, CATEGORIES, CUPBOARDED, SENTIMENTS, SIGNALS, BEANS, PUBLISHERS, CHATTERS, ID
from datacollectors.utils import URL, BASE_URL
from pybeansack import create_client

if __name__ == "__main__":
    # Use command line args if provided, otherwise fall back to env vars
    args = parser.parse_args()
    mode = args.mode or os.getenv("MODE")
    batch_size = int(args.batch_size or os.getenv("BATCH_SIZE") or os.cpu_count())
    
    cache_path = os.getenv("PROCESSING_CACHE")
    cache_settings = {
        BEANS: {"id_key": URL},
        PUBLISHERS: {"id_key": BASE_URL},
        CHATTERS: {"id_key": ID},
        SIGNALS: {"id_key": ID}
    }
    cache = StateCache(cache_path, cache_settings)
    async_cache = AsyncStateCache(cache_path, cache_settings)

    if mode == "COLLECTOR":
        from coffeemaker.orchestrators.collectororch import Collector

        asyncio.run(
            Collector(cache=async_cache).run(
                os.getenv("COLLECTOR_SOURCES", f"{CURR_DIR}/factory/feeds.yaml"),
                batch_size=batch_size,
            )
        )

    elif mode == "EMBEDDER":
        from coffeemaker.orchestrators.analyzerorch import Embedder

        Embedder(
            cache=cache,
            embedder_model_path=os.environ["EMBEDDER_PATH"],
            embedder_context_len=int(
                os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN)
            ),
        ).run(batch_size=batch_size)

    elif mode == "CLASSIFIER":
        from coffeemaker.orchestrators.analyzerorch import Classifier
        from coffeemaker.processingcache.clscache import ClassificationCache
        
        cls_cache = ClassificationCache(
            os.getenv('CLASSIFICATION_CACHE', f'{CURR_DIR}/.cache/clsstore'), 
            table_settings={
                BEANS: {"id_key": URL, "distance_func": "l2"},
                CATEGORIES: {"id_key": "category", "distance_func": "cosine"},
                SENTIMENTS: {"id_key": "sentiment", "distance_func": "cosine"}
            }
        )
        Classifier(cache=cache, cls_cache=cls_cache).run(batch_size=batch_size)
        cls_cache.close()

    elif mode == "EXTRACTOR":
        from coffeemaker.orchestrators.analyzerorch import Extractor

        Extractor(
            cache=cache,
            extractor_model_path=os.environ["EXTRACTOR_PATH"],
            extractor_context_len=int(
                os.getenv("EXTRACTOR_CONTEXT_LEN", EXTRACTOR_CONTEXT_LEN)
            ),
        ).run(batch_size=batch_size)

    elif mode == "DIGESTOR":
        from coffeemaker.orchestrators.analyzerorch import Digestor

        Digestor(
            cache=cache,
            digestor_model_path=os.environ["DIGESTOR_PATH"],
            digestor_context_len=int(
                os.getenv("DIGESTOR_CONTEXT_LEN", DIGESTOR_CONTEXT_LEN)
            ),
        ).run(batch_size=batch_size)     

    elif mode == "CONSOLIDATOR":
        from coffeemaker.orchestrators.analyzerorch import Consolidator
        
        Consolidator(
            cache=cache,
            consolidator_model_path=os.environ["CONSOLIDATOR_PATH"],
            consolidator_context_len=int(
                os.getenv("CONSOLIDATOR_CONTEXT_LEN", CONSOLIDATOR_CONTEXT_LEN)
            ),
            base_url=os.getenv("CONSOLIDATOR_BASE_URL"),
            api_key=os.getenv("CONSOLIDATOR_API_KEY"),
        ).run(batch_size=batch_size)

    elif mode == "PORTER":
        from coffeemaker.orchestrators.porterorch import BeansackPorter, CupboardPorter
        from pycupboard.pgcupboard import Cupboard

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

        async def run_porter():
            beansack_db = create_client(**db_kwargs)
            cupboard_db = Cupboard(os.getenv("CUPBOARD_CONNECTION_STRING"))
            try:
                async with async_cache:
                    await asyncio.gather(
                        BeansackPorter(cache=async_cache).hydrate_beansack(beansack_db, BEANSACKED),
                        CupboardPorter(cache=async_cache).hydrate_cupboard(cupboard_db, CUPBOARDED)
                    )
            finally:
                beansack_db.close()

        asyncio.run(run_porter())

    else:
        raise ValueError(
            "Invalid mode. Choose COLLECTOR, EMBEDDER, EXTRACTOR, DIGESTOR, CLASSIFIER, CONSOLIDATOR, or PORTER."
        )
    
    cache.close()
    
