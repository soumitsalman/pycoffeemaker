from concurrent.futures import ThreadPoolExecutor
import os
import logging
import sys
import os
from dotenv import load_dotenv
from tqdm import tqdm

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from pybeansack.models import *
from pybeansack import *

def db_instance(db_type: str) -> Beansack:
    if db_type in ["lancedb", "lancesack", "lance"]:
        return LanceDB(os.getenv('LANCEDB_STORAGE'))
    elif db_type in ["pg", "postgres", "postgresql"]:
        return Postgres(os.getenv('PG_CONNECTION_STRING'))
    elif db_type in ["duckdb", "duck"]:
        return DuckDB(os.getenv('DUCKDB_STORAGE'))
    elif db_type in ["ducklake", "dl"]:
        return Ducklake(os.getenv('DUCKLAKE_CATALOG'), os.getenv('DUCKLAKE_STORAGE'))
    else:
        raise ValueError(f"Unsupported db type: {db_type}")

def migrate(from_db: str, to_db: str, batch_size: int, window: int, *requested_items):
    print(f"[{now().strftime('%A, %Y-%m-%d')}] Migrating from {from_db} to {to_db} with batch size {batch_size} and window {window} for items: {requested_items or 'all'}")

    from_db_instance = db_instance(from_db)
    to_db_instance = db_instance(to_db)

    BEAN_CONDITIONS = [
        "gist IS NOT NULL",
        "embedding IS NOT NULL"
    ]
    PUBLISHER_CONDITIONS = ["""(
        rss_feed IS NOT NULL 
        OR favicon IS NOT NULL 
        OR site_name IS NOT NULL
    )"""]

    CHATTER_CONDITIONS = []

    if window: 
        collected_expr = f"collected >= CURRENT_TIMESTAMP - interval '{window} days'"
        BEAN_CONDITIONS.append(collected_expr)
        PUBLISHER_CONDITIONS.append(collected_expr)
        CHATTER_CONDITIONS.append(collected_expr)

    items_map = {
        "beans": {
            "count_table": BEANS,
            "port_fn": lambda offset: to_db_instance.store_beans(
                from_db_instance.query_latest_beans(conditions=BEAN_CONDITIONS, offset=offset, limit=batch_size)
            ),
            "conditions": BEAN_CONDITIONS,
            "tqdm_desc": "Porting Beans"
        },
        "contents": {
            "count_table": BEANS,
            "port_fn": lambda offset: to_db_instance.update_beans(
                from_db_instance.query_latest_beans(conditions=BEAN_CONDITIONS, offset=offset, limit=batch_size, columns=[K_URL, K_CONTENT, K_CONTENT_LENGTH, K_GIST, K_SENTIMENTS]),
                columns=[K_CONTENT, K_CONTENT_LENGTH, K_GIST, K_SENTIMENTS]
            ),
            "conditions": BEAN_CONDITIONS,
            "tqdm_desc": "Porting Contents"
        },
        "publishers": {
            "count_table": PUBLISHERS,
            "port_fn": lambda offset: to_db_instance.store_publishers(
                from_db_instance.query_publishers(conditions=PUBLISHER_CONDITIONS, offset=offset, limit=batch_size)
            ),
            "conditions": PUBLISHER_CONDITIONS,            
            "tqdm_desc": "Porting Publishers"
        },
        "chatters": {
            "count_table": CHATTERS,
            "port_fn": lambda offset: to_db_instance.store_chatters(
                from_db_instance.query_chatters(conditions=CHATTER_CONDITIONS, offset=offset, limit=batch_size)
            ),
            "conditions": CHATTER_CONDITIONS,
            "tqdm_desc": "Porting Chatters"
        },
    }
    
    for name, cfg in items_map.items():
        if not requested_items or name in set(requested_items):
            total = from_db_instance.count_rows(cfg["count_table"], conditions=cfg["conditions"])
            for offset in tqdm(range(0, total, batch_size), desc=cfg["tqdm_desc"], unit=cfg['count_table']):
                cfg["port_fn"](offset)                

    from_db_instance.close()
    to_db_instance.close()    

def hydrate(from_db, states_cache, classifier_cache, batch_size):
    from coffeemaker.orchestrators.statemachines import StateMachine
    from pybeansack import SimpleVectorDB

    db = db_instance(from_db)
    states = StateMachine(states_cache, {"beans": K_URL, "publishers": K_BASE_URL})
    classifications = SimpleVectorDB(classifier_cache, {"beans": K_URL})

    if False:
        offset = 0
        with tqdm(desc="Hydrating State Caches", unit="publishers") as pbar:
            while pubs := db.query_publishers(
                conditions=["(rss_feed IS NOT NULL) OR (favicon IS NOT NULL) OR (site_name IS NOT NULL) OR (description IS NOT NULL)"],
                limit=batch_size, offset=offset,
            ):
                publisher_states = [pub.model_dump(exclude_none=True) for pub in pubs]
                for state in ["collected"]:
                    states.set("publishers", state, publisher_states)
                offset += len(pubs)
                pbar.update(len(pubs))

    
    if True:
        # offset = 139264+24576
        with tqdm(desc="Hydrating State Caches", unit="beans") as pbar:
            while beans := db.query_latest_beans(
                conditions=["gist IS NOT NULL", "embedding IS NOT NULL"],
                limit=batch_size, offset=offset,
                columns=[K_URL, K_EMBEDDING, K_COLLECTED]
            ):
                with ThreadPoolExecutor() as executor:
                    bean_states = [bean.model_dump(include={K_URL, K_COLLECTED}) for bean in beans]
                    executor.map(lambda state: states.set("beans", state, bean_states), ["collected", "embedded", "extracted", "digested"])
                    executor.submit(classifications.store, "beans", [bean.model_dump(include={K_URL, K_EMBEDDING}) for bean in beans])
                offset += len(beans)
                pbar.update(len(beans))
        classifications.optimize()


import argparse
parser = argparse.ArgumentParser(description="Setup coffeemaker and beansack")
parser.add_argument('--hydrate', action='store_true', help='Hydrate state caches instead of doing a full migration')
parser.add_argument('--from_db', type=str, help='Type of database to create')
parser.add_argument('--to_db', type=str, help='Update the lancedb')
parser.add_argument('--states_cache', type=str, help='State cache path for hydration')
parser.add_argument('--classifier_cache', type=str, help='Classifier cache path for hydration')
parser.add_argument('--batch_size', type=int, default=2048, help='Batch size for migration')
parser.add_argument("--window", type=int, default=0, help="Window size for migration")
parser.add_argument('--items', type=str, nargs='*', help='Items to migrate (beans, publishers, chatters)')

if __name__ == "__main__":
    args = parser.parse_args()
    if args.hydrate: hydrate(args.from_db, args.states_cache, args.classifier_cache, args.batch_size)
    else: migrate(args.from_db, args.to_db, args.batch_size, args.window, *(args.items or []))