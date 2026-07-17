import asyncio
from itertools import batched
import json
import logging
import os
import re
import sys
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.env import load_coffeemaker_env

load_coffeemaker_env()

from concurrent.futures import ThreadPoolExecutor

from icecream import ic
from tqdm import tqdm

from pybeansack import create_client
from pybeansack.database import *
from pybeansack.models import *



log_dir, log_file = os.getenv("LOG_DIR"), None
if log_dir:
    os.makedirs(log_dir, exist_ok=True)
    log_file = f"{log_dir}/rectify-{now().strftime('%Y-%m-%d-%H')}.log"

# logging.basicConfig(
#     level=logging.INFO,
#     filename=log_file,
#     format="%(asctime)s||%(name)s||%(levelname)s||%(message)s||%(source)s||%(num_items)s",
# )

from utils.dates import ndays_ago, now
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
    vecs = embedder(["topic: " + topics[key][DESCRIPTION] for key in topics.keys()])
    for key, vec in zip(topics.keys(), vecs):
        topics[key][EMBEDDING] = vec

    with open(
        "/home/soumitsr/codes/pycoffeemaker/factory/composer-topics.json", "w"
    ) as file:
        json.dump(topics, file, indent=2)


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

    from_cache = SimpleVectorDB(from_lance, {BEANS: URL})
    to_cache = ClassificationCache(
        to_pg,
        table_settings={
            BEANS: {"id_key": URL},
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
                            URL,
                            CATEGORIES,
                            SENTIMENTS,
                            ENTITIES,
                            REGIONS,
                        ],
                    )
                )
                future = executor.submit(
                    db.update_beans,
                    beans,
                    columns=[CATEGORIES, SENTIMENTS, ENTITIES, REGIONS],
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
        URL,
        EMBEDDING,
        BASE_URL,
        BEANS,
        PUBLISHERS,
        RELATED_BEANS,
        create_client,
    )

    db = create_client(
        db_type="pg", pg_connection_string=os.getenv("PG_CONNECTION_STRING")
    )
    state_store = StateCache(
        cache_dir, {BEANS: {"id_key": URL}, PUBLISHERS: {"id_key": BASE_URL}}
    )

    if False:
        offset = 0
        while pubs := db.query_publishers(
            conditions=[
                "(favicon IS NOT NULL) OR (site_name IS NOT NULL) OR (description IS NOT NULL)"
            ],
            limit=batch_size,
            offset=offset,
            columns=[BASE_URL, SOURCE],
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
                columns=[URL],
            ):
                basic_vals = [{URL: bean.url} for bean in beans]
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

def hydrate_classification_cache(cache_dir):
    from workers.workercache.clscache import ClassificationCache
    from pybeansack import create_client

    db = create_client(db_type="pg", pg_connection_string=os.getenv("BEANSACK_CONNECTION_STRING"))
    cls_cache = ClassificationCache(cache_dir, table_settings={BEANS: {"id_key": URL, "distance_func": "l2"}})

    QUERY = """
    SELECT url, embedding FROM beans 
    WHERE url > %s AND embedding IS NOT NULL 
    ORDER BY url LIMIT 16384
    """
    def get_beans(last_url: str):
        with db.pool.connection() as conn:
            with conn.execute(QUERY, (last_url, )) as cur:
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
                items = [dict(zip(cols, row)) for row in rows]
                for item in items:
                    item[ID] = item.pop("url")
        return items

    last_url = ""
    while beans := get_beans(last_url):
        count = cls_cache.store(BEANS, beans)
        ic(last_url, count)
        last_url = beans[-1][ID]
    
    
    db.close()


def recitify_embeddings_and_classifications():
    from nlp import embedders
    db = create_client(
        db_type="pg", pg_connection_string=os.getenv("PG_CONNECTION_STRING")
    )
    cls_cache =  FireClassificationCache(
        os.getenv('CLASSIFICATION_CACHE', f'.cache/clsstore'), 
        table_settings={
            BEANS: {"id_key": URL, "distance_func": "l2"},
            "categories": {"id_key": "category", "distance_func": "cosine"},
            "sentiments": {"id_key": "sentiment", "distance_func": "cosine"}
        }
    )
    with embedders.from_path(os.getenv("EMBEDDER_PATH"), int(os.getenv("EMBEDDER_CONTEXT_LEN"))) as embedder:
        while beans := db.query_latest_beans(
            conditions=["embedding IS NOT NULL"],
            limit=16,
            offset=0,
            columns=[URL, CONTENT],
        ):
            embeddings = embedder.embed_documents([bean.content for bean in beans])
            categories = cls_cache.batch_search("categories", embeddings, top_n=2)
            sentiments = cls_cache.batch_search("sentiments", embeddings, top_n=2)
            cls_cache.store(BEANS, [{"url": bean.url, "embedding": embedding} for bean, embedding in zip(beans, embeddings)])
            db.update_beans(
                [ 
                    Bean(url=bean.url, embedding=emb, categories=cats, sentiments=sents)
                    for bean, emb, cats, sents in zip(beans, embeddings, categories, sentiments)
                ], 
                columns=[EMBEDDING, CATEGORIES, SENTIMENTS]
            )
            print("rectified %d" % offset)
            offset += len(beans)
    db.close()
    cls_cache.close()

def _sip_to_str(sip: dict) -> str:
    from workers.analyzerorch import _PRIORITY_DIGEST_KEYS, _OUTLOOK_KEYS, _ENTITY_KEYS, _entity_tags, _value_to_str
    lines = []
    if digest := sip.get(DIGEST):        
        for key in _PRIORITY_DIGEST_KEYS:
            if v := digest.get(key):
                lines.append(f"{key}:{_value_to_str(v)}")
        for key, v in digest.items():
            if not v or key in _PRIORITY_DIGEST_KEYS or key in _OUTLOOK_KEYS:
                continue
            lines.append(f"{key}:{_value_to_str(v)}")
        for key in _OUTLOOK_KEYS:
            if v := digest.get(key):
                lines.append(f"{key}:{_value_to_str(v)}")
    return "\n".join(lines)


def rectify_beans_id_and_embeddings():
    import psycopg
    from nlp import create_embedder
    from psycopg_pool import ConnectionPool
    from pgvector.psycopg import register_vector
    from utils import generate_uuid
    from workers.workercache.clscache import ClassificationCache

    beansack_pool = ConnectionPool(
        os.getenv("BEANSACK_CONNECTION_STRING"),
        min_size=1, max_size=4, timeout=60, max_idle=60,
        configure=register_vector,
    )
    cupboard_pool = ConnectionPool(
        os.getenv("CUPBOARD_CONNECTION_STRING"),
        min_size=1, max_size=4, timeout=60, max_idle=60,
        configure=register_vector,
    )

    BEAN_EXPR = """
    SELECT url, content FROM beans b
    WHERE embedding IS NULL
    LIMIT %(limit)s
    """
    SIP_EXPR = """
    SELECT id, digest FROM sips
    WHERE kind = 'signal' AND embedding IS NULL
    LIMIT %(limit)s
    """
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 16))
    MAX_DOCUMENT_LEN = int(os.getenv("MAX_DOCUMENT_LEN", 8192)) << 1

    @retry(
        retry=retry_if_exception_type((psycopg.OperationalError, psycopg.InterfaceError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _update_beans_batch(data):
        with beansack_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE beans SET id = %s, embedding = %s::vector WHERE url = %s",
                    data,
                )
            conn.commit()

    @retry(
        retry=retry_if_exception_type((psycopg.OperationalError, psycopg.InterfaceError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _update_sips_batch(data):
        with cupboard_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE sips SET embedding = %s::vector WHERE id = %s",
                    data,
                )
            conn.commit()

    def _get_beans(limit: int):
        with beansack_pool.connection() as bconn:
            beans = bconn.execute(BEAN_EXPR, {"limit": limit}).fetchall()
        return beans

    with create_embedder(os.getenv("EMBEDDER_PATH"), int(os.getenv("EMBEDDER_CONTEXT_LEN"))) as embedder:
        # ── Loop 1: Beans ──
        batches = batched(_get_beans(50000), BATCH_SIZE)
        for chunk in tqdm(iterable=batches, desc="Rectifying beans", unit="bean_batch"):
            urls = [b[0] for b in chunk]
            ids = [generate_uuid(url) for url in urls]
            texts = [b[1][:MAX_DOCUMENT_LEN] for b in chunk]
            try:
                vectors = embedder.embed_documents(texts)
                _update_beans_batch([(id, vec, url) for id, vec, url in zip(ids, vectors, urls)])
                _update_sips_batch([(vec, id) for id, vec in zip(ids, vectors)])
            except:
                pass            

        # ── Loop 2: Cupboard signal sips ──
        with cupboard_pool.connection() as conn:
            total_sips = conn.execute("SELECT count(*) FROM sips WHERE kind = 'signal' AND embedding IS NULL").fetchone()[0]
        with tqdm(total=total_sips, desc="Rectifying signal sips", unit="sips") as pbar:
            while True:
                with cupboard_pool.connection() as conn:
                    sips = conn.execute(SIP_EXPR, {"limit": BATCH_SIZE}).fetchall()
                if not sips:
                    break
                texts = [_sip_to_str({DIGEST: s[1]}) for s in sips]
                vectors = embedder.embed_documents(texts)

                _update_sips_batch([(vec.tolist(), sip[0]) for sip, vec in zip(sips, vectors)])
                pbar.update(len(sips))

        beansack_pool.close()
        cupboard_pool.close()    


# adding data porting logic
if __name__ == "__main__":
    rectify_beans_id_and_embeddings()
    # hydrate_classification_cache(os.getenv("CLASSIFICATION_CACHE"))
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
    # migrate_mongodb("test", "espresso")
