import sys
import os


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.env import load_coffeemaker_env

load_coffeemaker_env()

from icecream import ic
from pybeansack.models import *
from pybeansack import BEANS, CHATTERS, PUBLISHERS, URL, BASE_URL

def create_classification_embeddings():
    import yaml
    import pandas as pd
    from nlp import create_embedder

    dir_name = os.path.dirname(__file__)
    
    with open(f"{dir_name}/classifications.yaml", "r") as file:
        classifications = yaml.safe_load(file)

    with create_embedder(os.getenv('EMBEDDER_PATH'), os.getenv('EMBEDDER_CONTEXT_LEN')) as embedder:
        categories = pd.DataFrame(
            {
                ID: [category[ID] for category in classifications[CATEGORIES]],
                EMBEDDING: embedder([f"Instruct: Given a question, retrieve passages that can help answer the question.\nQuery: Content related to {category['description']}" for category in classifications[CATEGORIES]])
            }
        )
        ic(categories.sample(n=3))

        sentiments = pd.DataFrame(
            {
                ID: [sentiment[ID] for sentiment in classifications[SENTIMENTS]],
                EMBEDDING: embedder([f"Instruct: Given a question, retrieve passages that can help answer the question.\nQuery: Content with {sentiment['description']}" for sentiment in classifications[SENTIMENTS]])
            }
        )
        ic(sentiments.sample(n=3))

    return categories, sentiments

def create_classification_files():
    dir_name = os.path.dirname(__file__)
    categories, sentiments = create_classification_embeddings()
    categories.to_parquet(f"{dir_name}/categories.parquet", engine='pyarrow')
    sentiments.to_parquet(f"{dir_name}/sentiments.parquet", engine='pyarrow')

def create_processing_cache(db_path: str):
    """Seed cache with classification embeddings"""

    from processingcache.pgcache import create_db
    from workers.states import COMPOSITES, BEANS, PUBLISHERS, CHATTERS
    res = create_db(
        db_path, 
        {
            BEANS: {"id_key": URL},
            PUBLISHERS: {"id_key": BASE_URL},
            CHATTERS: {"id_key": ID},
            COMPOSITES: {"id_key": ID},
        }
    )
    print("Created new processing cache at", res)

def hydrate_classification_cache(window: int = 90):
    from pybeansack import PGSack
    from processingcache import ClassificationCache

    db = PGSack(os.getenv('BEANSACK_CONNECTION_STRING'))
    cls_cache = ClassificationCache(os.getenv('CLASSIFICATION_CACHE'), {BEANS: {"id_key": URL}})
    
    if beans := db.query_latest_beans(created=ndays_ago(window), conditions=["embedding IS NOT NULL"], columns=[URL, EMBEDDING]):
        beans = [bean for bean in beans]
        print("hydrating:cls_cache", len(beans))
        beans = [{ID: bean.url, EMBEDDING: bean.embedding} for bean in beans]
        print("hydrated:cls_cache", cls_cache.store(BEANS, beans))
    cls_cache.close()


def create_beansack(db_type: str, *connection_args: str):
    from pybeansack import create_db
    if db_type in ["lancedb", "lancesack", "lance"]:
        db = create_db(db_type=db_type, lancedb_storage=connection_args[0])
        print("Created new lancesack at", db.db.uri)
    elif db_type in ["pg", "postgres", "postgresql"]:
        db = create_db(db_type=db_type, pg_connection_string=connection_args[0])
        print("Created new pgsack at", db.pool.conninfo)
    elif db_type in ["duckdb", "duck"]:
        db = create_db(db_type=db_type, duckdb_storage=connection_args[0])
        print("Created new ducksack at", db.storage_path)
    elif db_type in ["ducklake", "dl"]:
        db = create_db(db_type=db_type, ducklake_catalog=connection_args[0], ducklake_storage=connection_args[1])
        print("Created new lakehouse at catalog:", connection_args[0], " storage:", connection_args[1])
    else:
        raise ValueError("unsupported db type")

def create_cupboard(connection_string: str):
    from pycupboard import create_db
    db = create_db(connection_string)
    if db: print("Created new cupboard at", connection_string)
    else: print("Failed to create cupboard at", connection_string)
    
import argparse
parser = argparse.ArgumentParser(description="Setup coffeemaker and beansack")
parser.add_argument('--beansack', type=str, nargs='+', metavar=('DB_TYPE', 'CONNECTION'), help='Database type followed by optional connection args (e.g. pg "postgresql://..."); falls back to env vars when omitted')
parser.add_argument('--cupboard', type=str, help="Postgres connection string for Cupboard")
parser.add_argument('--pgcache', type=str, help='Initialize PG State Cache')
parser.add_argument('--clscache', action='store_true', help='Initialize Classification Cache with Seed Value')
parser.add_argument('--hydrate_clscache', type=int, nargs='?', const=90, default=None, help='Hydrate Classification Cache with Seed Value (default window 90)')
parser.add_argument('--cls_files', action='store_true', help='Create classification files with embeddings for categories and sentiments')

if __name__ == "__main__":
    args = parser.parse_args()
    if args.beansack: create_beansack(*args.beansack)
    if args.cupboard: create_cupboard(args.cupboard)
    if args.pgcache: create_processing_cache(args.pgcache)
    if args.hydrate_clscache: hydrate_classification_cache(args.hydrate_clscache)
    if args.cls_files: create_classification_files()