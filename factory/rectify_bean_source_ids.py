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


def rectify_bean_source_ids(batch_size: int = BATCH_SIZE):
    import psycopg
    from psycopg_pool import ConnectionPool
    from datacollectors.normalize import cleanup_url, extract_base_url
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
            return 0
        with pool.connection() as conn:
            result = conn.execute(
                f"UPDATE beans AS b SET source_id = v.source_id::uuid, base_url = v.base_url FROM (VALUES {','.join(['(%s, %s, %s)'] * len(data))}) AS v(source_id, base_url, id) WHERE b.id = v.id::uuid AND (b.source_id IS NULL OR b.base_url IS NULL)",
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
    def _get_beans(after_id):
        with pool.connection() as conn:
            if after_id is None:
                return conn.execute(
                    "SELECT id, url FROM beans WHERE source_id IS NULL OR base_url IS NULL ORDER BY id LIMIT %s",
                    [FETCH_SIZE],
                    binary=True,
                ).fetchall()
            return conn.execute(
                "SELECT id, url FROM beans WHERE (source_id IS NULL OR base_url IS NULL) AND id > %s ORDER BY id LIMIT %s",
                [after_id, FETCH_SIZE],
                binary=True,
            ).fetchall()

    bean_count = 0
    row_count = 0
    after_id = None
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        while beans := _get_beans(after_id):
            after_id = beans[-1][0]
            rows = []
            for bean_id, url in beans:
                base_url = cleanup_url(extract_base_url(url)) if url else None
                if not base_url:
                    continue
                rows.append((generate_uuid(base_url), base_url, bean_id))
            if not rows:
                continue
            updated = list(executor.map(_update_beans, batched(rows, batch_size)))
            bean_count += len(rows)
            row_count += sum(updated)
            print(f"BEANS={bean_count} ROWS={row_count}")

    pool.close()


if __name__ == "__main__":
    rectify_bean_source_ids(batch_size=BATCH_SIZE)
