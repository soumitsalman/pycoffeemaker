from concurrent.futures import ThreadPoolExecutor
from itertools import chain, batched
import os
import sys
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.env import load_coffeemaker_env

load_coffeemaker_env()

WORKERS = 4
FETCH_SIZE = 16384
BATCH_SIZE = 4096


def rectify_chatter_bean_ids(batch_size: int = BATCH_SIZE):
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
    def _update_chatters(data):
        data = list(data)
        if not data:
            return 0
        with pool.connection() as conn:
            result = conn.execute(
                f"UPDATE chatters AS c SET bean_id = v.bean_id::uuid FROM (VALUES {','.join(['(%s, %s)'] * len(data))}) AS v(bean_id, url) WHERE c.url = v.url AND c.bean_id IS NULL",
                list(chain.from_iterable(data)),
            )
            conn.commit()
            return result.rowcount

    @retry(
        retry=retry_if_exception_type((psycopg.OperationalError, psycopg.InterfaceError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _get_urls():
        with pool.connection() as conn:
            return conn.execute(
                "SELECT DISTINCT url FROM chatters WHERE bean_id IS NULL LIMIT %s",
                [FETCH_SIZE],
                binary=True,
            ).fetchall()

    url_count = 0
    row_count = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        while urls := _get_urls():
            rows = [(generate_uuid(url), url) for (url,) in urls]
            updated = list(executor.map(_update_chatters, batched(rows, batch_size)))
            url_count += len(urls)
            row_count += sum(updated)
            print(f"URLS={url_count} ROWS={row_count}")

    pool.close()


if __name__ == "__main__":
    rectify_chatter_bean_ids(batch_size=BATCH_SIZE)
