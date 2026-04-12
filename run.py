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
logging.getLogger("coffeemaker.orchestrators.collectororch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.analyzerorch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.composerorch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.refresherorch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.porterorch").setLevel(logging.INFO)
logging.getLogger("coffeemaker.orchestrators.statemachines_pg").setLevel(logging.INFO)
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
        "CDN",
        "PORTER",
    ],
    help="Operation mode (COLLECTOR, EMBEDDER, DIGESTOR, EXTRACTOR, ANALYZER, CLASSIFIER, CDN, PORTER)",
)

from coffeemaker.processingcache.sqlitecache import AsyncStateMachine, StateMachine, ClassificationStore
from pybeansack import create_client, CDNStore, BEANS, PUBLISHERS, K_URL, K_BASE_URL

if __name__ == "__main__":
    # Use command line args if provided, otherwise fall back to env vars
    args = parser.parse_args()
    mode = args.mode or os.getenv("MODE")
    batch_size = int(args.batch_size or os.getenv("BATCH_SIZE") or os.cpu_count())
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
    classification_store = ClassificationStore(
        os.getenv("CACHE_DIR"), 
        table_id_keys={BEANS: K_URL}
    )
    state_store = StateMachine(
        os.getenv("CACHE_DIR"),
        object_id_keys={BEANS: K_URL, PUBLISHERS: K_BASE_URL},
    )
    async_state_store = AsyncStateMachine(
        os.getenv("CACHE_DIR"),
        object_id_keys={BEANS: K_URL, PUBLISHERS: K_BASE_URL},
    )

    if mode == "COLLECTOR":
        from coffeemaker.orchestrators.collectororch import Collector

        orch = Collector(state_store=async_state_store, db=db)
        asyncio.run(
            orch.run(
                os.getenv("COLLECTOR_SOURCES", "./factory/feeds.yaml"),
                batch_size=batch_size,
            )
        )

    elif mode == "EMBEDDER":
        from coffeemaker.orchestrators.analyzerorch import Indexer

        orch = Indexer(
            state_store=state_store,
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

        orch = Indexer(
            state_store=state_store, 
            classification_store=classification_store
        )
        while orch.run_classifier(batch_size=batch_size):
            # keep running it while there is something to process
            pass

    elif mode == "EXTRACTOR":
        from coffeemaker.orchestrators.analyzerorch import Indexer

        orch = Indexer(
            state_store=state_store,
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
            state_store=state_store,
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
            state_store=state_store,
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
        while orch.run(
            embedder_batch_size=int(args.embedder_batch_size or batch_size),
            extractor_batch_size=int(args.extractor_batch_size or batch_size),
            digestor_batch_size=int(args.digestor_batch_size or batch_size),
        ):
            # keep running it while there is something to process
            pass

    elif mode == "CDN":
        from coffeemaker.orchestrators.analyzerorch import Indexer

        orch = Indexer(
            state_store=state_store,
            cdn=CDNStore(os.getenv("CDN_BUCKET"), os.getenv("CDN_PUBLIC_ACCESS_URL")),
        )
        while orch.run_cdn(batch_size=batch_size):
            # keep running it while there is something to cdn
            pass

    elif mode == "PORTER":
        from coffeemaker.orchestrators.porterorch import Porter

        orch = Porter(state_store=state_store)
        # add a backup db to store 
        while orch.hydrate_beansacks(db):
            # keep running it while there is something
            pass

    else:
        raise ValueError(
            "Invalid mode. Please choose from COLLECTOR, INDEXER, DIGESTOR, EXTRACTOR, ANALYZER, CLASSIFIER."
        )

    state_store.close()
    classification_store.close()
    db.close()
