import sys
import os
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

from icecream import ic
from pybeansack.models import *
from coffeemaker.orchestrators.processingcache import PROCESSING_CACHE_DIR, ProcessingCache

def create_classification_embeddings():
    import yaml
    import pandas as pd
    from nlp import embedders

    dir_name = os.path.dirname(__file__)
    
    with open(f"{dir_name}/classifications.yaml", "r") as file:
        classifications = yaml.safe_load(file)

    with embedders.from_path(os.getenv('EMBEDDER_PATH'), 512) as embedder:
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

def update_db(db_type: str):
    from pybeansack import LanceDB

    if db_type in ["lancedb", "lancesack", "lance"]:
        db = LanceDB(os.getenv('LANCEDB_STORAGE'))
        db.allpublishers.alter_columns({"path": K_COLLECTED, "nullable": True})
        db.allchatters.drop_columns([K_SHARES, K_UPDATED])

def create_db(db_type: str):
    from pybeansack import create_db
    if db_type in ["lancedb", "lancesack", "lance"]:
        db = create_db(db_type=db_type, lancedb_store=os.getenv('LANCEDB_STORAGE'), catalogs_dir=os.getenv('FACTORY_DIR'))
        print("Created new lancesack at", db.db.uri)
    elif db_type in ["pg", "postgres", "postgresql"]:
        db = create_db(db_type=db_type, pg_connection_string=os.getenv('PG_CONNECTION_STRING'), catalogs_dir=os.getenv('FACTORY_DIR'))
        print("Created new pgsack at", db.db.dsn)
    elif db_type in ["duckdb", "duck"]:
        db = create_db(db_type=db_type, duckdb_storage=os.getenv('DUCKDB_STORAGE'), catalogs_dir=os.getenv('FACTORY_DIR'))
        print("Created new ducksack at", db.storage_path)
    elif db_type in ["ducklake", "dl"]:
        db = create_db(db_type=db_type, ducklake_catalog=os.getenv('DUCKLAKE_CATALOG'), ducklake_storage=os.getenv('DUCKLAKE_STORAGE'), catalogs_dir=os.getenv('FACTORY_DIR'))
        print("Created new lakehouse at catalog:", os.getenv('DUCKLAKE_CATALOG'), " storage:", os.getenv('DUCKLAKE_STORAGE'))
    else:
        raise ValueError("unsupported db type")
    
def initialize_processingcache() -> ProcessingCache:
    """Seed cache with classification embeddings"""
    categories, sentiments = create_classification_embeddings()
    with ProcessingCache(PROCESSING_CACHE_DIR, "beans", K_URL) as db:
        # TODO: add some indexing?        
        # TODO: move delete this to the cache
        db.db.delete("fixed_categories")
        db.db.delete("fixed_sentiments")
        db.store("fixed_categories", categories.rename(columns={"category": K_URL}).to_dict("records"))
        db.store("fixed_sentiments", sentiments.rename(columns={"sentiment": K_URL}).to_dict("records"))

import argparse
parser = argparse.ArgumentParser(description="Setup coffeemaker and beansack")
parser.add_argument('--create', type=str, help='Type of database to create')
parser.add_argument('--update', type=str, help='Update the lancedb')
parser.add_argument('--initcache', action="store_true", help='Initialize Processing Cache with Seed Value')

if __name__ == "__main__":
    args = parser.parse_args()
    if args.create: create_db(args.create)
    if args.update: update_db(args.update)
    if args.initcache: initialize_processingcache()