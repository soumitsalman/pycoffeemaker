from concurrent.futures import ThreadPoolExecutor
from itertools import chain, batched
import os
import sys
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.env import load_coffeemaker_env

load_coffeemaker_env()

from tqdm import tqdm

WORKERS = 4
FETCH_SIZE = 8192


def rectify_bean_ids(batch_size: int):
    import psycopg
    from psycopg_pool import ConnectionPool
    from utils import generate_uuid

    pool = ConnectionPool(
        os.getenv("BEANSACK_CONNECTION_STRING"),
        min_size=1, max_size=WORKERS, timeout=120, max_idle=120,
    )

    @retry(
        retry=retry_if_exception_type((psycopg.OperationalError, psycopg.InterfaceError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _update_beans(data):
        data = list(data)
        if not data:
            return
        with pool.connection() as conn:
            conn.execute(
                f"UPDATE publishers AS p SET id = v.id::uuid FROM (VALUES {','.join(['(%s, %s)'] * len(data))}) AS v(id, url) WHERE p.base_url = v.url",
                list(chain.from_iterable(data)),
            )
            conn.commit()

    @retry(
        retry=retry_if_exception_type((psycopg.OperationalError, psycopg.InterfaceError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _get_beans():
        with pool.connection() as conn:
            return conn.execute(
                "SELECT base_url FROM publishers WHERE id IS NULL LIMIT %s",
                [FETCH_SIZE],
                binary=True,
            ).fetchall()

    count = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        while beans := _get_beans():
            rows = [(generate_uuid(url), url) for (url,) in beans]
            list(executor.map(_update_beans, batched(rows, batch_size)))
            count += len(beans)
            print(f"BEANS={count}")

    pool.close()


def rectify_sip_source_ids(batch_size: int):
    import psycopg
    from psycopg_pool import ConnectionPool
    from utils import generate_uuid

    pool = ConnectionPool(
        os.getenv("CUPBOARD_CONNECTION_STRING"),
        min_size=1, max_size=WORKERS, timeout=120, max_idle=120,
    )

    @retry(
        retry=retry_if_exception_type((psycopg.OperationalError, psycopg.InterfaceError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _update_sips(data):
        data = list(data)
        if not data:
            return
        with pool.connection() as conn:
            conn.execute(
                f"UPDATE sips AS s SET source = v.source::uuid FROM (VALUES {','.join(['(%s, %s)'] * len(data))}) AS v(source, id) WHERE s.id = v.id::uuid",
                list(chain.from_iterable(data)),
            )
            conn.commit()

    @retry(
        retry=retry_if_exception_type((psycopg.OperationalError, psycopg.InterfaceError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _get_sips():
        with pool.connection() as conn:
            return conn.execute(
                "SELECT id, base_url FROM sips WHERE source IS NULL LIMIT %s",
                [FETCH_SIZE],
                binary=True,
            ).fetchall()

    count = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        while sips := _get_sips():
            rows = [(generate_uuid(base_url), sip_id) for sip_id, base_url in sips]
            list(executor.map(_update_sips, batched(rows, batch_size)))
            count += len(sips)
            print(f"SIPS={count}")

    pool.close()


if __name__ == "__main__":
    batch_size = 512
    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(lambda rectify: rectify(batch_size), (rectify_bean_ids, rectify_sip_source_ids)))
