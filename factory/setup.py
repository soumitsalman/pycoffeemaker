import sys
import os
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

from icecream import ic
from pybeansack.models import *

def create_classification_files():
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
                K_EMBEDDING: embedder(["topic/domain: "+cat for cat in classifications[K_CATEGORIES]])
            }
        )
        ic(categories.sample(n=3))
        categories.to_parquet(f"{dir_name}/categories.parquet", engine='pyarrow')

        sentiments = pd.DataFrame(
            {
                "sentiment": classifications[K_SENTIMENTS],
                K_EMBEDDING: embedder(["sentiment: "+s for s in classifications[K_SENTIMENTS]])
            }
        )
        ic(sentiments.sample(n=3))
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

import argparse
parser = argparse.ArgumentParser(description="Setup coffeemaker and beansack")
parser.add_argument('--create', type=str, help='Type of database to create')
parser.add_argument('--update', type=str, help='Update the lancedb')

if __name__ == "__main__":
    args = parser.parse_args()
    if args.create: create_db(args.create)
    if args.update: update_db(args.update)