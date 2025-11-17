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

def establish_new_lancesack():
    from coffeemaker.pybeansack.lancesack import establish_db

    dir_name = os.path.dirname(__file__)
    storage_path = ".beansack/lancesack/"
    db = establish_db(storage_path, dir_name)
    print("Created new lancesack at", db.uri)

if __name__ == "__main__":
    establish_new_lancesack()