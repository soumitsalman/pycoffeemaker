import asyncio
import json
import logging
import os
import re
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from itertools import batched, chain
from urllib.parse import urljoin, urlparse

from icecream import ic
from tqdm import tqdm

from pybeansack import create_client
from pybeansack.database import *
from pybeansack.models import *
from coffeemaker.processingcache.pgcache import ClassificationCache as PGClassificationCache
from coffeemaker.processingcache.firecache import (
    ClassificationCache as FireClassificationCache,
)

K_RELATED = "related"
LIMIT = 40000


log_dir, log_file = os.getenv("LOG_DIR"), None
if log_dir:
    os.makedirs(log_dir, exist_ok=True)
    log_file = f"{log_dir}/rectify-{now().strftime('%Y-%m-%d-%H')}.log"

logging.basicConfig(
    level=logging.INFO,
    filename=log_file,
    format="%(asctime)s||%(name)s||%(levelname)s||%(message)s||%(source)s||%(num_items)s",
)

ndays_ago = lambda n: datetime.now() - timedelta(days=n)
make_id = lambda text: re.sub(r"[^a-zA-Z0-9]", "-", text.lower())


def create_composer_topics_locally():
    import json

    import pandas as pd
    import yaml
    from nlp import embedders

    with open(
        "/home/soumitsr/codes/pycoffeemaker/factory/composer-topics.yaml", "r"
    ) as file:
        topics = yaml.safe_load(file)

    embedder = embedders.from_path(os.getenv("EMBEDDER_PATH"), 512)
    vecs = embedder(["topic: " + topics[key][K_DESCRIPTION] for key in topics.keys()])
    for key, vec in zip(topics.keys(), vecs):
        topics[key][K_EMBEDDING] = vec

    with open(
        "/home/soumitsr/codes/pycoffeemaker/factory/composer-topics.json", "w"
    ) as file:
        json.dump(topics, file, indent=2)


def migrate_users(from_db, to_db):
    from coffeemaker.orchestrators.collectororch import Collector

    old_prod = Collector(os.getenv("MONGODB_CONN_STR"), from_db)
    new_prod = Collector(os.getenv("MONGODB_CONN_STR"), to_db)

    new_prod.db.userstore.insert_many(old_prod.db.userstore.find({}), ordered=False)


def split_parquet_into_chunks(
    src_path: str = "/home/soumitsr/.beansack/main/bean_gists/bean-gists-rectified.parquet",
    chunk_size: int = 1024,
    dest_dir: str | None = None,
    prefix: str = "bean-gists-chunk",
) -> list[str]:
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



def migrate_classification_cache(from_lance, to_pg):
    from pybeansack import SimpleVectorDB
    from coffeemaker.processingcache.firecache import ClassificationCache

    from_cache = SimpleVectorDB(from_lance, {BEANS: K_URL})
    to_cache = ClassificationCache(
        to_pg,
        table_settings={
            BEANS: {"id_key": K_URL},
            "categories": {"id_key": "category"},
            "sentiments": {"id_key": "sentiment"},
        },
    )

    batch_size = 256
    total = from_cache.db[BEANS].count_rows()
    with tqdm(total=total, desc="Migrating classification cache") as pbar:

        def transfer(limit, offset):
            items = from_cache.db[BEANS].search().limit(limit).offset(offset).to_list()
            if items:
                pbar.update(to_cache.store(BEANS, items))

        list(
            ThreadPoolExecutor(max_workers=os.cpu_count()).map(
                lambda off: transfer(batch_size, off), range(0, total, batch_size)
            )
        )

    from_cache.close()
    to_cache.close()


def migrate_classification_cache_pg_to_fire(
    pg_conn_str: str = None,
    fire_db_path: str = "./firecache",
    batch_size: int = 256,
    vector_length: int = 384,
    distance_func: str = "l2",
):
    """Migrate ClassificationCache from PostgreSQL to Firebird/zvec.

    Args:
        pg_conn_str: PostgreSQL connection string (default: PROCESSING_CACHE env)
        fire_db_path: Target Firebird/zvec database path
        batch_size: Number of rows per batch
        vector_length: Embedding dimension
        distance_func: Distance function (l2 or cosine)
    """
    if pg_conn_str is None:
        pg_conn_str = os.getenv("PROCESSING_CACHE")

    if not pg_conn_str:
        raise ValueError("PG connection string not provided")

    table_settings={
        BEANS: {
            "id_key": K_URL,
            "vector_length": vector_length,
            "distance_func": distance_func,
        },
    }

    from_cache = PGClassificationCache(pg_conn_str, table_settings)
    to_cache = FireClassificationCache(fire_db_path, table_settings)

    with from_cache.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM beans")
            total = cur.fetchone()[0]


    def fetch_batch(offset):
        with from_cache.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT url, embedding FROM beans WHERE embedding IS NOT NULL LIMIT {batch_size} OFFSET {offset}"
                )
                rows = cur.fetchall()
                items = [
                    {
                        "url": row[0],
                        "embedding": row[1].tolist() if hasattr(row[1], "tolist") else row[1],
                    }
                    for row in rows
                ]
                if items:
                    return to_cache.store(BEANS, items)
        return 0

    # with tqdm(total=total, desc="Migrating PG→Fire") as pbar:
    #     with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
    #         futures = [
    #             executor.submit(fetch_batch, offset)
    #             for offset in range(0, total, batch_size)
    #         ]
    #         for future in futures:
    #             pbar.update(future.result())

    list(map(
        fetch_batch, 
        tqdm(range(0, total, batch_size), desc="Migrated", total=(total//batch_size)+1)
    ))

    from_cache.close()
    to_cache.close()
    ic("Migration complete")


def cleanup_bean_tags():
    from pybeansack import create_client

    db = create_client(
        db_type="postgres", pg_connection_string=os.getenv("PG_CONNECTION_STRING")
    )

    strip_non_alphanumeric = lambda text: re.sub(r"^\W+|\W+$", "", text)
    clean_list = lambda items: (
        list(set(filter(None, map(strip_non_alphanumeric, items)))) if items else items
    )

    def cleanup_beans(beans: list[Bean]) -> list[Bean]:
        for bean in beans:
            bean.categories = clean_list(bean.categories)
            bean.sentiments = clean_list(bean.sentiments)
            bean.entities = clean_list(bean.entities)
            bean.regions = clean_list(bean.regions)

        return beans

    CONDITION = ["gist IS NOT NULL", "embedding IS NOT NULL"]

    # get total rows
    start = int(os.getenv("MIGRATE_OFFSET", 0))
    total = db.count_rows(BEANS, conditions=CONDITION)
    batch_size = 1024
    offsets = list(range(start, total, batch_size))

    with tqdm(total=total - start, desc="Updating batches") as pbar:
        with ThreadPoolExecutor(
            max_workers=8, thread_name_prefix="UPDATER"
        ) as executor:
            for offset in offsets:
                beans = cleanup_beans(
                    db._fetch_all(
                        table=BEANS,
                        conditions=CONDITION,
                        order="collected",
                        limit=batch_size,
                        offset=offset,
                        columns=[
                            K_URL,
                            K_CATEGORIES,
                            K_SENTIMENTS,
                            K_ENTITIES,
                            K_REGIONS,
                        ],
                    )
                )
                future = executor.submit(
                    db.update_beans,
                    beans,
                    columns=[K_CATEGORIES, K_SENTIMENTS, K_ENTITIES, K_REGIONS],
                )
                future.add_done_callback(lambda f: pbar.update(f.result()))
            # executor waits for all submitted updates to finish before exiting
    db.close()


def hydrate_processing_cache(cache_dir, batch_size):
    """Hydrates local processing cache with beans and publishers from production/backup db

    Loading Publishers: processed publishers that already has either site_name, favicon or description as `collected` and `beansacked`
    Loading Beans:
    - processed beans that already has both gist and embedding as `collected`, `embedded`, `extracted`, `digested` (since we don't want to repeat any of those steps for existing beans). Query only the `url` and `embedding`. Store the embedding in classification cache
    - unprocessed beans that do not have `gist`. Store those in `collected` state in processing cache so they can be picked up by the pipeline and processed as usual.
    """
    import os
    from coffeemaker.processingcache.sqlitecache import StateCache
    from coffeemaker.processingcache.base import ClassificationStore
    from pybeansack import (
        K_URL,
        K_EMBEDDING,
        K_BASE_URL,
        BEANS,
        PUBLISHERS,
        RELATED_BEANS,
        create_client,
    )

    db = create_client(
        db_type="pg", pg_connection_string=os.getenv("PG_CONNECTION_STRING")
    )
    state_store = StateCache(
        cache_dir, {BEANS: {"id_key": K_URL}, PUBLISHERS: {"id_key": K_BASE_URL}}
    )

    if False:
        offset = 0
        while pubs := db.query_publishers(
            conditions=[
                "(favicon IS NOT NULL) OR (site_name IS NOT NULL) OR (description IS NOT NULL)"
            ],
            limit=batch_size,
            offset=offset,
            columns=[K_BASE_URL, K_SOURCE],
        ):
            publisher_states = [
                pub.model_dump(exclude_none=True, exclude_unset=True) for pub in pubs
            ]
            list(
                map(
                    lambda state: state_store.set(PUBLISHERS, state, publisher_states),
                    ["collected", "beansacked"],
                )
            )
            offset += len(pubs)
            logging.info(
                "hydrated", extra={"source": "publishers", "num_items": offset}
            )

    if False:
        offset = 0
        with ThreadPoolExecutor() as exec:
            while beans := db.query_latest_beans(
                conditions=["gist IS NOT NULL", "embedding IS NOT NULL"],
                limit=batch_size,
                offset=offset,
                columns=[K_URL],
            ):
                basic_vals = [{K_URL: bean.url} for bean in beans]
                exec.map(
                    lambda state: state_store.set(BEANS, state, basic_vals),
                    [
                        "collected",
                        "extracted",
                        "digested",
                        "cdned",
                        "classified",
                        "embedded",
                        "beansacked",
                    ],
                )
                offset += len(beans)
                logging.info(
                    "hydrated", extra={"source": "processed beans", "num_items": offset}
                )

    if True:
        offset = 0
        cls_cache = ClassificationStore(cache_dir, {BEANS: K_URL})
        while beans := db.query_latest_beans(
            conditions=["embedding IS NOT NULL"],
            limit=batch_size,
            offset=offset,
            columns=[K_URL, K_EMBEDDING],
        ):
            offset += len(beans)
            count = cls_cache.store(
                BEANS,
                [
                    bean.model_dump(exclude_none=True, exclude_unset=True)
                    for bean in beans
                ],
            )
            logging.info(
                "hydrated:cls_cache", extra={"source": offset, "num_items": count}
            )
        cls_cache.close()

    state_store.close()
    db.close()


# adding data porting logic
if __name__ == "__main__":
    migrate_classification_cache_pg_to_fire(
        pg_conn_str=os.getenv("PROCESSING_CACHE")+"/clsstore",
        fire_db_path=".cache/clsstore",
        batch_size=512,
        vector_length=384,
        distance_func="l2",
    )
    # migrate_classification_cache(
    #     from_lance=".cache/",
    #     to_pg=os.getenv("PROCESSING_CACHE") + "/clsstore",
    # )
    # hydrate_processing_cache(os.getenv("CACHE_DIR"), batch_size=4096)
    # cleanup_bean_tags()
    # cluster_existing_beans()

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
