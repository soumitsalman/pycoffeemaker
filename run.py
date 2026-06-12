import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime as dt

from dotenv import load_dotenv

MAX_WORKERS = os.cpu_count() * os.cpu_count()
EMBEDDER_CONTEXT_LEN = 512
EXTRACTOR_CONTEXT_LEN = 4096
DIGESTOR_CONTEXT_LEN = 32768
CONSOLIDATOR_CONTEXT_LEN = 65536

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(CURR_DIR + "/.env")

log_dir, log_file = os.getenv("LOG_DIR"), None
if log_dir:
    os.makedirs(log_dir, exist_ok=True)
    log_file = f"{log_dir}/coffeemaker-{dt.now().strftime('%Y-%m-%d-%H')}.log"

from utils.logs import configure_logging, get_logger

configure_logging(log_file=log_file)
log = get_logger("app")

### WORKER SCHEDULING ###
# Collector can run 2 times a day for 1-1.5 hours -- 6 AM / 6 PM
# Embedder / Extractor / Classifier can run 3 times a day for 30 mins -- 7 AM, 3 PM, 11 PM
# Digestor can run 3 times a day for 30 mins (in GPU) -- 7 AM, 3 PM, 11 PM
# Porter clients can run as needed

# Set up argument parser
parser = argparse.ArgumentParser(description="Run the coffee maker application")
parser.add_argument("--batch_size", type=int, help="Batch size for processing")
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

from workers.workercache.pgcache import AsyncStateCache, StateCache
from workers.utils import BEANSACKED, CATEGORIES, CUPBOARDED, SENTIMENTS, COMPOSITES, BEANS, PUBLISHERS, CHATTERS, ID
from datacollectors import URL, BASE_URL
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
        COMPOSITES: {"id_key": ID}
    }
    cache = StateCache(cache_path, cache_settings)
    async_cache = AsyncStateCache(cache_path, cache_settings)

    if mode == "COLLECTOR":
        from workers.collectororch import Collector

        feeds = os.getenv("COLLECTOR_SOURCES", f"{CURR_DIR}/factory/feeds.yaml")
        asyncio.run(
            Collector(cache=async_cache, batch_size=batch_size).run(feeds)
        )

    elif mode == "EMBEDDER":
        from workers.analyzerorch import Embedder

        Embedder(
            cache=cache,
            model_path=os.environ["EMBEDDER_PATH"],
            context_len=int(
                os.getenv("EMBEDDER_CONTEXT_LEN", EMBEDDER_CONTEXT_LEN)
            ),
            batch_size=batch_size,
        ).run()

    elif mode == "CLASSIFIER":
        from workers.analyzerorch import Classifier
        from workers.workercache.clscache import ClassificationCache
        
        cls_cache = ClassificationCache(
            os.getenv('CLASSIFICATION_CACHE', f'{CURR_DIR}/.cache/clsstore'), 
            table_settings={
                BEANS: {"id_key": URL, "distance_func": "l2"},
                CATEGORIES: {"id_key": "category", "distance_func": "cosine"},
                SENTIMENTS: {"id_key": "sentiment", "distance_func": "cosine"}
            }
        )
        Classifier(cache=cache, cls_cache=cls_cache, batch_size=batch_size).run()
        cls_cache.close()

    elif mode == "EXTRACTOR":
        from workers.analyzerorch import Extractor

        Extractor(
            cache=cache,
            model_path=os.environ["EXTRACTOR_PATH"],
            context_len=int(
                os.getenv("EXTRACTOR_CONTEXT_LEN", EXTRACTOR_CONTEXT_LEN)
            ),
            batch_size=batch_size,
        ).run()

    elif mode == "DIGESTOR":
        from workers.analyzerorch import Digestor

        Digestor(
            cache=cache,
            model_path=os.environ["DIGESTOR_PATH"],
            context_len=int(
                os.getenv("DIGESTOR_CONTEXT_LEN", DIGESTOR_CONTEXT_LEN)
            ),
            batch_size=batch_size,
        ).run()     

    elif mode == "CONSOLIDATOR":
        from workers.analyzerorch import Consolidator
        
        Consolidator(
            cache=cache,
            model_path=os.environ["CONSOLIDATOR_PATH"],
            context_len=int(
                os.getenv("CONSOLIDATOR_CONTEXT_LEN", CONSOLIDATOR_CONTEXT_LEN)
            ),
            batch_size=batch_size,
            base_url=os.getenv("CONSOLIDATOR_BASE_URL"),
            api_key=os.getenv("CONSOLIDATOR_API_KEY"),
        ).run()

    elif mode == "PORTER":
        from workers.porterorch import BeansackPorter, CupboardPorter
        from pycupboard.pgcupboard import Cupboard

        async def run_porter():
            beansack_db = create_client("pg", pg_connection_string=os.getenv("BEANSACK_CONNECTION_STRING"))
            cupboard_db = Cupboard(os.getenv("CUPBOARD_CONNECTION_STRING"))
            try:
                async with async_cache:
                    await asyncio.gather(
                        BeansackPorter(cache=async_cache).run(beansack_db, BEANSACKED),
                        CupboardPorter(cache=async_cache).run(cupboard_db, CUPBOARDED)
                    )
            finally:
                beansack_db.close()

        asyncio.run(run_porter())

    else:
        raise ValueError(
            "Invalid mode. Choose COLLECTOR, EMBEDDER, EXTRACTOR, DIGESTOR, CLASSIFIER, CONSOLIDATOR, or PORTER."
        )
    
    cache.close()
    
