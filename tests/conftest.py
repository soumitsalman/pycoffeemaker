import os
import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from utils.env import load_coffeemaker_env

load_coffeemaker_env(ROOT)

from utils.logs import configure_logging

configure_logging()

os.makedirs(os.path.join(ROOT, ".test"), exist_ok=True)

TESTS_DIR = Path(__file__).resolve().parent


@pytest.fixture
def pg_db():
    conn = os.getenv("PG_CONNECTION_STRING")
    if not conn:
        pytest.skip("PG_CONNECTION_STRING not set")
    from pybeansack import create_client

    db = create_client("pg", pg_connection_string=conn)
    yield db
    db.close()


@pytest.fixture
def duck_db():
    from pybeansack import create_client

    catalog = os.getenv("DUCKLAKE_CATALOG")
    storage = os.getenv("DUCKLAKE_STORAGE")
    if catalog and storage:
        db = create_client("dl", ducklake_catalog=catalog, ducklake_storage=storage)
    else:
        path = os.getenv("DUCKDB_STORAGE", "/tmp/beansack-test.duckdb")
        db = create_client("duck", duckdb_storage=path)
    yield db
    db.close()


@pytest.fixture
def lance_db():
    from pybeansack import create_client

    root = os.getenv("TEST_STORAGE", "/tmp/beansack-lance")
    path = f"{root}/{datetime.now().strftime('%Y-%m-%d')}-lancedb"
    db = create_client("lance", lancedb_storage=path)
    yield db
    db.close()


@pytest.fixture
def db(request):
    """Indirect parametrization: request.param is pg_db | duck_db | lance_db."""
    return request.getfixturevalue(request.param)
