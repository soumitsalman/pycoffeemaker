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

def register_file(beansack, filename):
    table = Path(filename.path).parent.name
    beansack.execute(SQL_INSERT_PARQUET, (table, str(filename.path),))
    log.info(f"[{table}] {filename.path}")

def register_parquets(catalogdb: str, storagedb: str, factory_dir: str = "factory"):
    beansack = Beansack(catalogdb, storagedb, factory_dir=factory_dir)

    root_dir = Path(storagedb) / "main"
    directories = [
        "bean_cores",
        "bean_embeddings",
        "bean_gists",
        "chatters",       
        "computed_bean_clusters",
        "computed_bean_categories",
        "computed_bean_sentiments",
        "publishers",
    ]
    with ThreadPoolExecutor(max_workers=16) as executor:
        for table in directories:
            # directory where parquet files for this table live
            parquet_dir = root_dir / table
            filenames = list(os.scandir(parquet_dir))
            log.info(f"Found {len(filenames)} parquet files in {table}")
            executor.map(lambda filename: register_file(beansack, filename), filenames)
            
            

    beansack.close()

if __name__ == "__main__":
    catalog = os.getenv("PG_CONNECTION_STRING")
    storage = os.getenv("STORAGE_DATAPATH")
    factory = os.getenv("FACTORY_DIR")
    if not catalog:
        log.warning("PG_CONNECTION_STRING not set; Beansack will try to initialize without a catalog.")
    register_parquets(catalog, storage, factory)