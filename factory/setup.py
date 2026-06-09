import sys
import os
from dotenv import load_dotenv



sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

from icecream import ic
from pybeansack.models import *
from pybeansack import BEANS, CHATTERS, PUBLISHERS, K_URL, K_BASE_URL

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
                "category": classifications[K_CATEGORIES],
                K_EMBEDDING: embedder([f"topic/category: {cat}" for cat in classifications[K_CATEGORIES]])
            }
        )
        ic(categories.sample(n=3))

        sentiments = pd.DataFrame(
            {
                "sentiment": classifications[K_SENTIMENTS],
                K_EMBEDDING: embedder([f"sentiment: {s}" for s in classifications[K_SENTIMENTS]])
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

    from coffeemaker.processingcache.pgcache import StateCache
    StateCache(
        db_path, 
        {
            BEANS: {"id_key": K_URL},
            PUBLISHERS: {"id_key": K_BASE_URL},
            CHATTERS: {"id_key": "id"}
        }
    ).close()

def create_classification_cache():
    from workers.workercache.clscache import ClassificationCache
    cls_cache = ClassificationCache(
        os.getenv('CLASSIFICATION_CACHE'), 
        {
            BEANS: {"id_key": K_URL, "vector_length": 384, "distance_func": "l2"},
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

    proc_cache = StateCache(os.getenv('PROCESSING_CACHE'), {BEANS: {"id_key": K_URL}})
    cls_cache = ClassificationCache(os.getenv('CLASSIFICATION_CACHE'), {BEANS: {"id_key": K_URL}})
    
    if beans := proc_cache.get(BEANS, states="embedded", window=window):
        beans = [bean for bean in beans if bean.get("embedding")]
        print("hydrating:cls_cache", len(beans))
        print("hydrated:cls_cache", cls_cache.store(BEANS, beans))
    cls_cache.close()


def create_db(db_type: str):
    from pybeansack import create_db
    if db_type in ["lancedb", "lancesack", "lance"]:
        db = create_db(db_type=db_type, lancedb_storage=os.getenv('LANCEDB_STORAGE'), catalogs_dir=os.getenv('FACTORY_DIR'))
        print("Created new lancesack at", db.db.uri)
    elif db_type in ["pg", "postgres", "postgresql"]:
        db = create_db(db_type=db_type, pg_connection_string=os.getenv('PG_CONNECTION_STRING'), catalogs_dir=os.getenv('FACTORY_DIR'))
        print("Created new pgsack at", db.pool.conninfo)
    elif db_type in ["duckdb", "duck"]:
        db = create_db(db_type=db_type, duckdb_storage=os.getenv('DUCKDB_STORAGE'), catalogs_dir=os.getenv('FACTORY_DIR'))
        print("Created new ducksack at", db.storage_path)
    elif db_type in ["ducklake", "dl"]:
        db = create_db(db_type=db_type, ducklake_catalog=os.getenv('DUCKLAKE_CATALOG'), ducklake_storage=os.getenv('DUCKLAKE_STORAGE'), catalogs_dir=os.getenv('FACTORY_DIR'))
        print("Created new lakehouse at catalog:", os.getenv('DUCKLAKE_CATALOG'), " storage:", os.getenv('DUCKLAKE_STORAGE'))
    else:
        raise ValueError("unsupported db type")
    
import argparse
parser = argparse.ArgumentParser(description="Setup coffeemaker and beansack")
parser.add_argument('--create', type=str, help='Type of database to create')
parser.add_argument('--update', type=str, help='Update the lancedb')
parser.add_argument('--pg_pcache', type=str, help='Initialize PG State Cache')
parser.add_argument('--create_clscache', action='store_true', help='Initialize Classification Cache with Seed Value')
parser.add_argument('--hydrate_clscache', type=int, default=90, help='Hydrate Classification Cache with Seed Value')
parser.add_argument('--cls_files', action='store_true', help='Create classification files with embeddings for categories and sentiments')

if __name__ == "__main__":
    args = parser.parse_args()
    if args.create: create_db(args.create)
    if args.pg_pcache: create_processing_cache(args.pg_pcache)
    if args.create_clscache: create_classification_cache()
    if args.hydrate_clscache: hydrate_classification_cache(args.hydrate_clscache)
    if args.cls_files: create_classification_files()