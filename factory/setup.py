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

    with create_embedder(os.getenv('EMBEDDER_PATH'), 512) as embedder:
        categories = pd.DataFrame(
            {
                "id": classifications[CATEGORIES],
                EMBEDDING: embedder([f"topic/domain={cat}" for cat in classifications[CATEGORIES]])
            }
        )
        ic(categories.sample(n=3))

        sentiments = pd.DataFrame(
            {
                "id": classifications[SENTIMENTS],
                EMBEDDING: embedder([f"sentiment={s}" for s in classifications[SENTIMENTS]])
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

    from workers.workercache.pgcache import StateCache
    StateCache(
        db_path, 
        {
            BEANS: {"id_key": URL},
            PUBLISHERS: {"id_key": BASE_URL},
            CHATTERS: {"id_key": "id"}
        }
    ).close()

def create_classification_cache():
    from workers.workercache.clscache import ClassificationCache
    cls_cache = ClassificationCache(
        os.getenv('CLASSIFICATION_CACHE'), 
        {
            BEANS: {"id_key": URL, "vector_length": 384, "distance_func": "l2"},
            "categories": {"id_key": "category", "vector_length": 384, "distance_func": "cosine"},
            "sentiments": {"id_key": "sentiment", "vector_length": 384, "distance_func": "cosine"}
        }
    )
    categories, sentiments = create_classification_embeddings()
    cls_cache.store("categories", categories.to_dict(orient="records"))
    cls_cache.store("sentiments", sentiments.to_dict(orient="records"))
    cls_cache.close()
    

def hydrate_classification_cache(window: int = 90):
    from workers.workercache.pgcache import StateCache
    from workers.workercache.clscache import ClassificationCache

    proc_cache = StateCache(os.getenv('PROCESSING_CACHE'), {BEANS: {"id_key": URL}})
    cls_cache = ClassificationCache(os.getenv('CLASSIFICATION_CACHE'), {BEANS: {"id_key": URL}})
    
    if beans := proc_cache.get(BEANS, states="embedded", window=window):
        beans = [bean for bean in beans if bean.get("embedding")]
        print("hydrating:cls_cache", len(beans))
        print("hydrated:cls_cache", cls_cache.store(BEANS, beans))
    cls_cache.close()


def create_beansack(db_type: str, *connection_args: str):
    from pybeansack import create_db
    if db_type in ["lancedb", "lancesack", "lance"]:
        db = create_db(db_type=db_type, lancedb_storage=connection_args[0] if connection_args else os.getenv('LANCEDB_STORAGE'))
        print("Created new lancesack at", db.db.uri)
    elif db_type in ["pg", "postgres", "postgresql"]:
        db = create_db(db_type=db_type, pg_connection_string=connection_args[0] if connection_args else os.getenv('BEANSACK_CONNECTION_STRING'))
        print("Created new pgsack at", db.pool.conninfo)
    elif db_type in ["duckdb", "duck"]:
        db = create_db(db_type=db_type, duckdb_storage=connection_args[0] if connection_args else os.getenv('DUCKDB_STORAGE'), catalogs_dir=os.getenv('FACTORY_DIR'))
        print("Created new ducksack at", db.storage_path)
    elif db_type in ["ducklake", "dl"]:
        ducklake_catalog = connection_args[0] if len(connection_args) > 0 else os.getenv('DUCKLAKE_CATALOG')
        ducklake_storage = connection_args[1] if len(connection_args) > 1 else os.getenv('DUCKLAKE_STORAGE')
        db = create_db(db_type=db_type, ducklake_catalog=ducklake_catalog, ducklake_storage=ducklake_storage, catalogs_dir=os.getenv('FACTORY_DIR'))
        print("Created new lakehouse at catalog:", ducklake_catalog, " storage:", ducklake_storage)
    else:
        raise ValueError("unsupported db type")
    
import argparse
parser = argparse.ArgumentParser(description="Setup coffeemaker and beansack")
parser.add_argument('--beansack', type=str, nargs='+', metavar=('DB_TYPE', 'CONNECTION'), help='Database type followed by optional connection args (e.g. pg "postgresql://..."); falls back to env vars when omitted')
parser.add_argument('--pgcache', type=str, help='Initialize PG State Cache')
parser.add_argument('--clscache', action='store_true', help='Initialize Classification Cache with Seed Value')
parser.add_argument('--hydrate_clscache', type=int, nargs='?', const=90, default=None, help='Hydrate Classification Cache with Seed Value (default window 90)')
parser.add_argument('--cls_files', action='store_true', help='Create classification files with embeddings for categories and sentiments')

if __name__ == "__main__":
    args = parser.parse_args()
    if args.beansack: create_beansack(*args.beansack)
    if args.pgcache: create_processing_cache(args.pgcache)
    if args.clscache: create_classification_cache()
    if args.hydrate_clscache: hydrate_classification_cache(args.hydrate_clscache)
    if args.cls_files: create_classification_files()