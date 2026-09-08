import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.env import load_coffeemaker_env

load_coffeemaker_env()

from processingcache import StateCache
from pybeansack.models import Publisher
from pybeansack.pgsack import PGSack
from utils.fields import BASE_URL, DOMAIN_NAME
from workers.states import COLLECTED, PUBLISHERS


FETCH_SIZE = 8192
BATCH_SIZE = 4096


def rectify_publishers(
    processing_cache: str | None = None,
    beansack_connection_string: str | None = None,
    batch_size: int = BATCH_SIZE,
):
    processing_cache = processing_cache or os.getenv("PROCESSING_CACHE")
    beansack_connection_string = beansack_connection_string or os.getenv(
        "BEANSACK_CONNECTION_STRING"
    )
    if not processing_cache or not beansack_connection_string:
        raise ValueError("processing_cache and beansack_connection_string are required")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    cache = StateCache(processing_cache, {PUBLISHERS: {"id_key": BASE_URL}})
    db = PGSack(beansack_connection_string)

    publisher_count = 0
    stored_count = 0
    offset = 0
    try:
        while publishers := cache.get(
            PUBLISHERS,
            states=COLLECTED,
            window=0,
            limit=FETCH_SIZE,
            offset=offset,
        ):
            offset += len(publishers)
            prepared = []
            for publisher in publishers:
                if source := publisher.pop("source", None):
                    publisher[DOMAIN_NAME] = publisher.get(DOMAIN_NAME, source)
                prepared.append(Publisher(**publisher))

            for start in range(0, len(prepared), batch_size):
                batch = prepared[start : start + batch_size]
                publisher_count += len(batch)
                stored_count += db.store_publishers(batch)
                print(f"PUBLISHERS={publisher_count} ROWS={stored_count}")
    finally:
        cache.close()
        db.close()

    return stored_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Copy collected publishers from processing cache into Beansack"
    )
    parser.add_argument(
        "--processing_cache",
        type=str,
        default=os.getenv("PROCESSING_CACHE"),
        help="Processing cache connection string (default: PROCESSING_CACHE)",
    )
    parser.add_argument(
        "--beansack_connection_string",
        type=str,
        default=os.getenv("BEANSACK_CONNECTION_STRING"),
        help="Beansack connection string (default: BEANSACK_CONNECTION_STRING)",
    )
    parser.add_argument("--batch_size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()
    rectify_publishers(
        processing_cache=args.processing_cache,
        beansack_connection_string=args.beansack_connection_string,
        batch_size=args.batch_size,
    )
