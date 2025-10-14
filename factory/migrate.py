from concurrent.futures import ThreadPoolExecutor
import os
import logging
from pathlib import Path
import sys
import os
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from coffeemaker.pybeansack.warehouse import Beansack, SQL_INSERT_PARQUET
import pandas as pd

def register_file(beansack, filename):
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
    beansack = Beansack(catalogdb, storagedb, factory_dir=factory_dir)

    root_dir = Path(storagedb) / "main"   
    for table in TABLES:
        # directory where parquet files for this table live
        parquet_dir = root_dir / table
        filenames = list(os.scandir(parquet_dir))
        log.info(f"Found {len(filenames)} parquet files in {table}")
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(lambda filename: register_file(beansack, filename), filenames)
            
    beansack.close()

if __name__ == "__main__":
    catalog = os.getenv("PG_CONNECTION_STRING")
    storage = os.getenv("STORAGE_DATAPATH")
    factory = os.getenv("FACTORY_DIR")
    if not catalog:
        log.warning("PG_CONNECTION_STRING not set; Beansack will try to initialize without a catalog.")
    register_parquets(catalog, ".beansackv2", factory)
    # compact_files(".beansack", ".beansackv2")