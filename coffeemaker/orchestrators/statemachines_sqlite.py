import logging
import os
from datetime import datetime, timezone
import threading
from typing import Any
from functools import cached_property
from icecream import contextmanager
import msgpack
from pydantic import BaseModel
from retry import retry
import sqlite3
from icecream import ic

# ID = "id"
# STATE = "state"
# TS = "ts"
# DATA = "data"

DB_JITTER = (1, 5)

_INIT_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA cache_size=-64000;
PRAGMA busy_timeout=300000;
PRAGMA mmap_size=268435456;
"""

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    id TEXT NOT NULL,
    state TEXT NOT NULL,
    ts TEXT NOT NULL,
    data BLOB DEFAULT NULL,
    PRIMARY KEY (id, state)
);
CREATE INDEX IF NOT EXISTS {table}_id_idx ON {table}(id);
CREATE INDEX IF NOT EXISTS {table}_state_idx ON {table}(state);
"""

class StateMachine:
    id_keys: dict[str, str]
    db_path: str
    write_lock: threading.Lock

    def __init__(self, db_path: str, object_id_keys: dict[str, str]):
        self.id_keys = object_id_keys.copy()
        self.db_path = db_path
        self.write_lock = threading.Lock()
        self._conn = sqlite3.connect(
            self.db_path, 
            timeout=300,
            check_same_thread=False,
            isolation_level=None
        )
        self._conn.executescript(_INIT_SQL)
        with self._write_connection() as conn:
            conn.executescript("\n".join([_CREATE_TABLE_SQL.format(table=table) for table in self.id_keys.keys()]))

    @contextmanager
    def _write_connection(self):
        with self.write_lock:
            try:
                cur = self._conn.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                yield cur
                self._conn.commit()
            except Exception as e:
                ic(e.__class__.__name__, e)
                self._conn.rollback()
                raise e
            finally:
                cur.close()    
    
    def _read(self, expr: str, params = None):
        cur = self._conn.cursor()
        result = cur.execute(expr, params).fetchall()
        cur.close()
        return result
    
    @retry(tries=3, jitter=DB_JITTER, logger=logging.getLogger("app")) 
    def set(self, object_type: str, state: str, items: list[dict[str, Any]] | list[BaseModel]):
        if not items: return 0

        with self._write_connection() as conn:
            rowcount = conn.executemany(
                f"INSERT OR IGNORE INTO {object_type} (id, state, ts, data) VALUES (?, ?, ?, ?)",
                _create_rows(self.id_keys[object_type], state, items)
            ).rowcount
        return rowcount

    def get(self, object_type: str, states: str | list[str], exclude_states: str | list[str], limit: int = 0, offset: int = 0):
        expr, params = create_query_expr(object_type, states, exclude_states, limit, offset)
        result = self._read(expr, params)
        return [decode_data(r[0]) for r in result]

    def deduplicate(self, object_type: str, state: str, items: list):
        if not items: return items

        ids = get_ids(items, self.id_keys[object_type])
        placeholders = ",".join(["?"] * len(ids))
        result = self._read(
            f"SELECT id FROM {object_type} WHERE state = ? AND id IN ({placeholders})", 
            [state] + ids
        )
        existing_ids = {r[0] for r in result}

        return [item for id, item in zip(ids, items) if id not in existing_ids]


encode_data = lambda data: msgpack.packb(data, datetime=True)
decode_data = lambda data: msgpack.unpackb(data, timestamp=3)
get_id = (
    lambda item, id_key: getattr(item, id_key)
    if isinstance(item, BaseModel)
    else item[id_key]
)
get_ids = lambda items, id_key: [get_id(item, id_key) for item in items]

def _create_rows(id_key: str, state: str, items: list[dict[str, Any]] | list[BaseModel]):
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return [
        (
            id,
            state,
            ts,
            encode_data(data),
        )
        for id, data in zip(get_ids(items, id_key), items)
    ]

def _create_single_state_query_expr(table: str, include_state: str, exclude_state: str):
    expr = f"""SELECT data FROM {table} 
    WHERE state = ?
    AND NOT EXISTS (
        SELECT 1 FROM {table} t2 WHERE t2.id = {table}.id AND t2.state = ?
    )
    """
    return expr, [include_state, exclude_state]


def _create_multi_state_query_expr(
    table: str, include_states: list[str], exclude_states: list[str]
):
    include_placeholders = ",".join(["?"] * len(include_states))
    exclude_placeholders = ",".join(["?"] * len(exclude_states))
    expr = f"""WITH filtered AS (
        SELECT id FROM {table}
        WHERE state NOT IN ({exclude_placeholders})
        GROUP BY id
        HAVING COUNT(DISTINCT CASE WHEN state IN ({include_placeholders}) THEN state END) >= ?
    )
    SELECT data FROM {table}
    WHERE EXISTS (SELECT 1 FROM filtered WHERE filtered.id = {table}.id)
    """
    params = exclude_states + include_states + [len(include_states)]
    return expr, params


def create_query_expr(
    table: str,
    states: str | list[str],
    exclude_states: str | list[str],
    limit: int = 0,
    offset: int = 0,
):
    if (isinstance(states, list) and len(states) > 1) or (
        isinstance(exclude_states, list) and len(exclude_states) > 1
    ):
        if isinstance(states, str):
            states = [states]
        if isinstance(exclude_states, str):
            exclude_states = [exclude_states]
        expr, params = _create_multi_state_query_expr(table, states, exclude_states)
    else:
        if isinstance(states, list):
            states = states[0]
        if isinstance(exclude_states, list):
            exclude_states = exclude_states[0]
        expr, params = _create_single_state_query_expr(table, states, exclude_states)

    if limit:
        expr = f"{expr} LIMIT ?"
        params.append(limit)
    if offset:
        expr = f"{expr} OFFSET ?"
        params.append(offset)

    return expr, params
