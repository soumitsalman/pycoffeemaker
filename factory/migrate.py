from concurrent.futures import ThreadPoolExecutor
import os
import logging
from pathlib import Path
import sys
import os
from icecream import ic
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

import pandas as pd

def register_file(beansack, filename):
    from coffeemaker.pybeansack.warehouse import Beansack, SQL_INSERT_PARQUET

    table = Path(filename.path).parent.name
    beansack.execute(SQL_INSERT_PARQUET, (table, str(filename.path),))
    log.info(f"[{table}] {filename.path}")

TABLES = [       
    "bean_embeddings",
    "bean_gists",
    "chatters",       
    "computed_bean_clusters",
    "computed_bean_categories",
    "computed_bean_sentiments",
    "publishers",
    "bean_cores",
]

def compact_files(root_dir: str, dest_dir: str = None):
    for table in TABLES:
        
        dest_table_dir = Path(dest_dir) / "main" / table
        source_table_dir = Path(root_dir) / "main" / table

        # Load all parquet files into DataFrames
        dfs = [pd.read_parquet(f.path) for f in list(os.scandir(source_table_dir))]
        # Merge all DataFrames
        dfs = pd.concat(dfs, ignore_index=True)
        # Deduplicate by 'url' column
        if table in ["bean_embeddings", "bean_gists", "computed_bean_categories", "computed_bean_sentiments", "bean_cores",]: dfs = dfs.drop_duplicates(subset=["url"])
        # Save each chunk as a separate parquet file
        os.makedirs(dest_table_dir, exist_ok=True)
        batch_size = 16384<<1
        for i in range(0, len(dfs), batch_size):
            chunk = dfs.iloc[i:i + batch_size]
            chunk.to_parquet(dest_table_dir / f"{table}_chunk_{i}.parquet", index=False)

def register_parquets(catalogdb: str, storagedb: str, factory_dir: str = "factory"):
    from coffeemaker.pybeansack.warehouse import Beansack, SQL_INSERT_PARQUET

    beansack = Beansack(catalogdb, storagedb, factory_dir=factory_dir)

    root_dir = Path(storagedb) / "main"   
    for table in TABLES:
        # directory where parquet files for this table live
        parquet_dir = root_dir / table
        filenames = list(os.scandir(parquet_dir))
        log.info(f"Found {len(filenames)} parquet files in {table}")
        with ThreadPoolExecutor(max_workers=56) as executor:
            executor.map(lambda filename: register_file(beansack, filename), filenames)
            
    beansack.close()

def migrate_from_v1_to_v2(v1_conn, v2_conn):
    from coffeemaker.pybeansack.models import Bean, Publisher, Chatter
    from coffeemaker.pybeansack.warehouse import Beansack as BSv1
    from coffeemaker.pybeansack.warehousev2 import Beansack as BSv2

    v1db = BSv1(v1_conn[0], v1_conn[1], os.getenv("FACTORY_DIR"))
    target_db = BSv2(v2_conn[0], v2_conn[1], os.getenv("FACTORY_DIR"))

    batch_size = 1<<16

    if False:
        for offset in range(0, ic(v1db.count_items("bean_cores")), batch_size):
            res = v1db.get_items("bean_cores", offset=offset, limit=batch_size)
            target_db.store_beans([Bean(**row) for row in res])
            ic(offset)

    if False:
        for offset in range(0, ic(v1db.count_items("bean_embeddings")), batch_size):
            res = v1db.get_items("bean_embeddings", offset=offset, limit=batch_size)
            target_db.update_beans([Bean(**row) for row in res], columns=["embedding"])
            target_db.update_classifications()
            ic(offset)

    if False:
        for offset in range(0, ic(v1db.count_items("bean_gists")), batch_size):
            res = v1db.get_items("bean_gists", offset=offset, limit=batch_size)
            target_db.update_beans([Bean(**{k:v for k,v in row.items() if v}) for row in res], columns=["gist", "regions", "entities"])
            ic(offset)

    if False:  
        for offset in range(0, ic(v1db.count_items("chatters")), batch_size):
            res = v1db.get_items("chatters", offset=offset, limit=batch_size)
            target_db.store_chatters([Chatter(**row) for row in res])
            target_db.refresh_chatter_aggregates()
            ic(offset)

    if True:
        for offset in range(0, ic(v1db.count_items("publishers")), batch_size):
            res = v1db.get_items("publishers", offset=offset, limit=batch_size)
            target_db.store_publishers([Publisher(**row) for row in res])
            ic(offset)

    if True:
        for offset in range(0, ic(v1db.count_items("computed_bean_clusters")), batch_size):
            res = v1db.get_items("computed_bean_clusters", offset=offset, limit=batch_size)
            target_db._store_related_beans(res)
            ic(offset)

    v1db.close()
    target_db.close()

if __name__ == "__main__":
    catalog = os.getenv("PG_CONNECTION_STRING")
    storage = os.getenv("STORAGE_DATAPATH")
    factory = os.getenv("FACTORY_DIR")
    if not catalog:
        log.warning("PG_CONNECTION_STRING not set; Beansack will try to initialize without a catalog.")
    # register_parquets(catalog, storage, factory)
    # compact_files(".beansack", ".beansackv2")

    migrate_from_v1_to_v2(
        (os.getenv("PG_CONNECTION_STRING"), os.getenv("STORAGE_DATAPATH")),
        ("sqlite:/workspaces/beansack/pycoffeemaker/.test/beansack/catalogdb.db", "/workspaces/beansack/pycoffeemaker/.test/beansack/storage/"),
    )