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
from utils.fields import ARTICLE_LANGUAGE, ID, LANGUAGE, SUMMARY, TITLE, URL
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



def rectify_missing_languages(
    beansack_connection_string: str | None = None,
    batch_size: int = BATCH_SIZE,
):
    """Detect and store languages for Beansack beans where language is NULL."""
    import psycopg
    from ftlangdetect import detect
    from psycopg_pool import ConnectionPool

    beansack_connection_string = beansack_connection_string or os.getenv("BEANSACK_CONNECTION_STRING")
    if not beansack_connection_string:
        raise ValueError("beansack_connection_string is required")

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
        placeholders = ",".join(["(%s, %s)"] * len(data))
        with pool.connection() as conn:
            result = conn.execute(
                f"UPDATE beans AS b SET language = v.language FROM (VALUES {placeholders}) AS v(id, language) WHERE b.id = v.id::uuid AND b.language IS NULL",
                list(chain.from_iterable(data)),
            )
            conn.commit()
            return result.rowcount

    def _detect_language(row):
        bean_id, title, summary = row
        text = "\n\n".join(
            value.strip()
            for value in (title, summary)
            if isinstance(value, str) and value.strip()
        )
        if not text:
            return None

        language = detect(text=text, low_memory=True).get("lang")
        if not isinstance(language, str) or not language:
            return None
        return bean_id, language.lower()

    selected_count = 0
    updated_count = 0
    query = f"SELECT {ID}, {TITLE}, {SUMMARY} FROM beans WHERE language IS NULL ORDER BY {ID}"
    with pool.connection() as conn:
        with conn.execute(query) as cursor:
            while rows := cursor.fetchmany(FETCH_SIZE):
                selected_count += len(rows)
                detected = [row for row in map(_detect_language, rows) if row]
                updated_count += sum(
                    _update_beans(chunk) for chunk in batched(detected, batch_size)
                )
                print(f"BEANS={selected_count} ROWS={updated_count}")

    pool.close()
    return updated_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy collected bean language into Beansack")
    parser.add_argument("--missing_languages", action="store_true", help="Detect and fill NULL bean languages from title and summary")
    parser.add_argument("--processing_cache", type=str, default=os.getenv("PROCESSING_CACHE"), help="Processing cache connection string (default: PROCESSING_CACHE)")
    parser.add_argument("--beansack_connection_string", type=str, default=os.getenv("BEANSACK_CONNECTION_STRING"), help="Beansack connection string (default: BEANSACK_CONNECTION_STRING)")
    parser.add_argument("--batch_size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()
    if args.missing_languages:
        rectify_missing_languages(
            beansack_connection_string=args.beansack_connection_string,
            batch_size=args.batch_size,
        )
    else:
        rectify_language(
            processing_cache=args.processing_cache,
            beansack_connection_string=args.beansack_connection_string,
            batch_size=args.batch_size,
        )
