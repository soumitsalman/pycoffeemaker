import sys
import os
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

from icecream import ic
from coffeemaker.pybeansack.models import *

def create_classification_files():
    import yaml
    import pandas as pd
    from coffeemaker.nlp import embedders

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

def create_new_lancesack():
    from coffeemaker.pybeansack.lancesack import Beansack

    db = Beansack.create_db(os.getenv('STORAGE_DATAPATH'), os.getenv('FACTORY_DIR'))
    print("Created new lancesack at", db.db.uri)


import argparse
parser = argparse.ArgumentParser(description="Setup coffeemaker and beansack")
parser.add_argument("--lancedb", action="store_true", help="Setup new lancedb beansack")
parser.add_argument("--ducklake", action="store_true", help="Setup new ducklake beansack")
parser.add_argument("--duckdb", action="store_true", help="Setup new duckdb beansack")
parser.add_argument("--mongodb", action="store_true", help="Setup new mongodb beansack")

if __name__ == "__main__":
    args = parser.parse_args()
    if args.lancedb: create_new_lancesack()