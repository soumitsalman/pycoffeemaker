import sys
import json
import os
import re
import asyncio
from dotenv import load_dotenv



sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
from itertools import chain
from icecream import ic
from tqdm import tqdm
from datetime import datetime, timedelta
from pybeansack.models import *
from pybeansack.database import *
# from coffeemaker.pybeansack.mongosack import *
# from coffeemaker.pybeansack.ducksack import *
# from coffeemaker.orchestrators.fullstack import Orchestrator

K_RELATED = "related"
LIMIT=40000

ndays_ago = lambda n: (datetime.now() - timedelta(days=n))
make_id = lambda text: re.sub(r'[^a-zA-Z0-9]', '-', text.lower())
create_orch = lambda: Orchestrator(
    os.getenv("DB_REMOTE"),
    os.getenv("DB_LOCAL"),
    os.getenv("DB_NAME"),
    embedder_path = os.getenv("EMBEDDER_PATH"),    
    digestor_path = os.getenv("DIGESTOR_PATH")
)


def merge_feeds():
    from coffeemaker.orchestrators.collectororch import Orchestrator
    from coffeemaker.collectors.collector import parse_sources
    import yaml

    local = Orchestrator(
        mongodb_conn_str="mongodb://localhost:27017/",
        db_name="test3"
    )
    
    # SCRAPE FOR DETAILS
    rss_feeds = local.db.sourcestore.distinct("rss_feed", filter = {"rss_feed": {"$nin": to_ignore}})
    
    existing = parse_sources("/home/soumitsr/codes/pycoffeemaker/coffeemaker/collectors/feeds.yaml")
    existing['rss'] = list(set(existing['rss']+rss_feeds))
    existing = {"sources": existing}
    
    with open("/home/soumitsr/codes/pycoffeemaker/coffeemaker/collectors/feeds.yaml", "w") as file:
        yaml.dump(existing, file)

def remove_non_functional_feeds():
    import csv, yaml
    csv_path = "/home/soumitsr/codes/pycoffeemaker/coffeemaker/collectors/non-functional-rss.csv"
    yaml_path = "/home/soumitsr/codes/pycoffeemaker/coffeemaker/collectors/feeds.yaml"

    # Read non-functional RSS feeds from CSV (handling quoted values)
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        non_functional = [row[0].strip().strip('"') for row in reader if row and row[0].strip()]

    ic(len(non_functional))
    # Load YAML feeds
    with open(yaml_path, 'r') as file:
        feeds_data = yaml.safe_load(file)

    # Remove non-functional feeds from the rss list
    if 'sources' in feeds_data and 'rss' in feeds_data['sources']:
        original_count = len(feeds_data['sources']['rss'])
        feeds_data['sources']['rss'] = sorted([
            feed for feed in feeds_data['sources']['rss'] if feed not in non_functional
        ])
        print(f"Removed {original_count - len(feeds_data['sources']['rss'])} feeds.")


    # Save the updated YAML
    with open(yaml_path, 'w') as file:
        yaml.dump(feeds_data, file)

def port_pages_locally():
    from coffeemaker.orchestrators.analyzerorch import Orchestrator
    orch = Orchestrator(
        os.getenv('DB_REMOTE'),
        "beansackV2",
        os.getenv('QUEUE_PATH_TEST'),
        "test"
    )
    local_orch = Orchestrator(
        os.getenv('DB_REMOTE_TEST'),
        "test",
        os.getenv('QUEUE_PATH_TEST'),
        "test"
    )

    pages = list(orch.db.db['baristas'].find({}))
    for page in pages:
        if K_EMBEDDING in page:
            page[K_EMBEDDING] = local_orch.embedder.embed("category/classification/domain: "+page[K_DESCRIPTION])
    local_orch.db.db['pages'].insert_many(pages)

    print(datetime.now(), "ported pages|%d", len(pages))

def create_composer_topics_locally():  
    import yaml, json
    import pandas as pd
    from coffeemaker.nlp import embedders
    
    with open("/home/soumitsr/codes/pycoffeemaker/factory/composer-topics.yaml", "r") as file:
        topics = yaml.safe_load(file)

    embedder = embedders.from_path(os.getenv('EMBEDDER_PATH'), 512)
    vecs = embedder(["topic: "+topics[key][K_DESCRIPTION] for key in topics.keys()])
    for key, vec in zip(topics.keys(), vecs):
        topics[key][K_EMBEDDING] = vec

    with open("/home/soumitsr/codes/pycoffeemaker/factory/composer-topics.json", "w") as file:
        json.dump(topics, file, indent=2)

def migrate_users(from_db, to_db):
    from coffeemaker.orchestrators.collectororch import Orchestrator

    old_prod = Orchestrator(
        os.getenv('MONGODB_CONN_STR'),
        from_db
    )
    new_prod = Orchestrator(
        os.getenv('MONGODB_CONN_STR'),
        to_db
    )
    
    new_prod.db.userstore.insert_many(old_prod.db.userstore.find({}), ordered=False)

def swap_gist_regions_entities(batch_size: int = 256):
    """Query all rows from warehouse.bean_gists, swap the `entities` and
    `regions` values for each row and update the table in batches.

    This uses the warehouse Beansack.query and Beansack.execute APIs so it
    behaves like other functions in this module.
    """
    from coffeemaker.pybeansack.warehouse import Beansack

    db = Beansack(
        os.getenv('PG_CONNECTION_STRING'),
        os.getenv('CACHE_DIR')
    )

    # Fetch all gists with their entities/regions
    query_expr = "SELECT url, gist, entities, regions FROM warehouse.bean_gists"
    rows = db.query(query_expr)
    total = len(rows)
    print(datetime.now(), f"fetched bean_gists|{total}")

    for item in rows:
        temp = item.get('entities')
        item['entities'] = item.get('regions')
        item['regions'] = temp

    import pandas as pd
    pd.DataFrame(rows).to_parquet(os.getenv("CACHE_DIR") + "/main/bean_gists/bean-gists-rectified.parquet")
    db.close()


def split_parquet_into_chunks(src_path: str = "/home/soumitsr/.beansack/main/bean_gists/bean-gists-rectified.parquet",
                              chunk_size: int = 1024,
                              dest_dir: str | None = None,
                              prefix: str = "bean-gists-chunk") -> list[str]:
    """Read a Parquet file, split it into chunks of `chunk_size` rows and
    write each chunk as a separate Parquet file.

    Returns the list of written file paths.
    """
    import pandas as pd

    if not os.path.exists(src_path):
        raise FileNotFoundError(f"source parquet not found: {src_path}")

    if dest_dir is None:
        dest_dir = os.path.dirname(src_path)

    os.makedirs(dest_dir, exist_ok=True)

    # Load dataframe (assumes it fits in memory). If this becomes a problem
    # we can switch to pyarrow.dataset scanning.
    df = pd.read_parquet(src_path)
    total = len(df)
    ic(f"loaded parquet|rows={total}")

    written = []
    for i in range(0, total, chunk_size):
        chunk = df.iloc[i : i + chunk_size]
        idx = i // chunk_size
        out_name = f"{prefix}-{idx:04d}.parquet"
        out_path = os.path.join(dest_dir, out_name)
        # Use pyarrow engine for compatibility with other code in this repo
        chunk.to_parquet(out_path, engine="pyarrow")
        written.append(out_path)
        ic(f"wrote chunk|{out_path}|rows={len(chunk)}")

    ic(f"wrote_total_chunks|{len(written)}")
    return written

def register_new_gist_files():
    from coffeemaker.pybeansack.warehouse import Beansack, SQL_INSERT_PARQUET
    db = Beansack(
        os.getenv('PG_CONNECTION_STRING'),
        os.getenv('STORAGE_DATAPATH')
    )

    import glob
    files = glob.glob(os.getenv("STORAGE_DATAPATH") + "/main/bean_gists/bean-gists-chunk-*.parquet")
    for f in files:
        db.execute(SQL_INSERT_PARQUET, ('bean_gists', f,))

    db.close()

def cleanup_bean_tags():
    from pybeansack import create_client

    db = create_client(db_type="postgres", pg_connection_string=os.getenv('PG_CONNECTION_STRING'))

    strip_non_alphanumeric = lambda text: re.sub(r'^\W+|\W+$', '', text)
    clean_list = lambda items: list(set(filter(None, map(strip_non_alphanumeric, items)))) if items else items

    conditions = ["gist IS NOT NULL", "embedding IS NOT NULL"]

    # get total rows
    total = db.count_rows(BEANS, conditions=conditions)
    batch_size = 256
    for offset in tqdm(range(0, total, batch_size), desc="Processing batches"):
        beans = db.query_latest_beans(conditions=conditions, limit=batch_size, offset=offset, columns=[K_URL, K_CATEGORIES, K_SENTIMENTS, K_ENTITIES, K_REGIONS])

        for bean in beans:
            bean.categories = clean_list(bean.categories)
            bean.sentiments = clean_list(bean.sentiments)
            bean.entities = clean_list(bean.entities)
            bean.regions = clean_list(bean.regions)
        
        db.update_beans(beans, columns=[K_CATEGORIES, K_SENTIMENTS, K_ENTITIES, K_REGIONS])

    db.close()


# adding data porting logic
if __name__ == "__main__":
    cleanup_bean_tags()
    # create_classification_data_files()
    # asyncio.run(prefill_publishers())
    # swap_gist_regions_entities()
    # rectify_parquets()
    # merge_to_classification()
    # create_composer_topics_locally()
    # create_categories_locally()
    # create_sentiments_locally()
    # port_beans_locally()
    # migrate_users("beansackV2", "test")
    # migrate_mongodb("test", "espresso")