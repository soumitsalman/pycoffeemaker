import argparse
from concurrent.futures import ThreadPoolExecutor
from itertools import chain, batched
import os
import sys
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.env import load_coffeemaker_env

load_coffeemaker_env()

from processingcache import StateCache
from utils.fields import ARTICLE_LANGUAGE, LANGUAGE, URL
from workers.states import BEANS, COLLECTED

WORKERS = 4
FETCH_SIZE = 16384
BATCH_SIZE = 4096


def rectify_language(
    processing_cache: str | None = None,
    beansack_connection_string: str | None = None,
    batch_size: int = BATCH_SIZE,
):
    import psycopg
    from psycopg_pool import ConnectionPool

    processing_cache = processing_cache or os.getenv("PROCESSING_CACHE")
    beansack_connection_string = beansack_connection_string or os.getenv("BEANSACK_CONNECTION_STRING")
    if not processing_cache or not beansack_connection_string:
        raise ValueError("processing_cache and beansack_connection_string are required")

    cache = StateCache(processing_cache, {BEANS: {"id_key": URL}})
    pool = ConnectionPool(
        beansack_connection_string,
        min_size=1, max_size=WORKERS, timeout=120, max_idle=120,
        num_workers=WORKERS,
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
                f"UPDATE beans AS b SET language = v.language FROM (VALUES {','.join(['(%s, %s)'] * len(data))}) AS v(url, language) WHERE b.url = v.url",
                list(chain.from_iterable(data)),
            )
            conn.commit()
            return result.rowcount

    bean_count = 0
    row_count = 0
    offset = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        while beans := cache.get(BEANS, states=COLLECTED, window=0, limit=FETCH_SIZE, offset=offset):
            offset += len(beans)
            rows = []
            for bean in beans:
                url = bean.get(URL)
                language = bean.get(LANGUAGE) or bean.get(ARTICLE_LANGUAGE)
                if url and isinstance(language, str) and language:
                    rows.append((url, language.lower()))
            if not rows:
                continue
            updated = list(executor.map(_update_beans, batched(rows, batch_size)))
            bean_count += len(rows)
            row_count += sum(updated)
            print(f"BEANS={bean_count} ROWS={row_count}")

    cache.close()
    pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy collected bean language into Beansack")
    parser.add_argument("--processing_cache", type=str, default=os.getenv("PROCESSING_CACHE"), help="Processing cache connection string (default: PROCESSING_CACHE)")
    parser.add_argument("--beansack_connection_string", type=str, default=os.getenv("BEANSACK_CONNECTION_STRING"), help="Beansack connection string (default: BEANSACK_CONNECTION_STRING)")
    parser.add_argument("--batch_size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()
    rectify_language(
        processing_cache=args.processing_cache,
        beansack_connection_string=args.beansack_connection_string,
        batch_size=args.batch_size,
    )
