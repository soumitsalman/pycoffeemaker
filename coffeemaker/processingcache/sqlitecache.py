from datetime import datetime, timezone
import threading
from typing import Any
from pydantic import BaseModel
from retry import retry
import sqlite3
import asyncio
import aiosqlite
from .base import *
from icecream import ic
import os
from pathlib import Path

DEFAULT_DB_PATH = "statestore.db"
DB_JITTER = (1, 5)

_INIT_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA cache_size=64000;
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

def _rectify_path(db_path: str) -> str:
    path = Path(db_path)
    if path.is_dir() or (not path.exists() and not path.suffix):
        # If it's a directory or doesn't exist and has no suffix, treat as directory
        path = path / DEFAULT_DB_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
    else:
        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)

class StateMachine(StateStoreBase):
    id_keys: dict[str, str]
    db_path: str
    write_lock: threading.Lock

    def __init__(self, db_path: str, object_id_keys: dict[str, str]):
        self.db_path = _rectify_path(db_path)
        self.id_keys = object_id_keys.copy()
        self.write_lock = threading.Lock()
        self._conn = None

    @property
    def conn(self):
        if not self._conn:
            self._conn = sqlite3.connect(self.db_path, timeout=300, check_same_thread=False, isolation_level=None)
            self._conn.executescript(_INIT_SQL)
            self._conn.executescript(create_table_expr(self.id_keys))
        return self._conn

    def _read(self, expr: str, params=None):
        cur = self.conn.cursor()
        result = cur.execute(expr, params).fetchall()
        cur.close()
        return result

    @retry(exceptions=sqlite3.OperationalError, tries=3, jitter=DB_JITTER)
    def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        if not items:
            return 0

        with self.write_lock:
            cur = self.conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            rowcount = cur.executemany(
                insert_expr(object_type),
                create_rows(self.id_keys[object_type], state, items),
            ).rowcount
            self.conn.commit()
            cur.close()

        return rowcount

    def get(
        self,
        object_type: str,
        states: str | list[str],
        exclude_states: str | list[str],
        limit: int = 0,
        offset: int = 0,
    ):
        expr, params = query_expr(
            object_type, states, exclude_states, limit, offset
        )
        result = self._read(expr, params)
        return [decode_data(r[0]) for r in result]

    def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = get_ids(items, self.id_keys[object_type])
        result = self._read(
            exists_expr(object_type, ids),
            [state] + ids,
        )
        existing_ids = {r[0] for r in result}

        return [item for id, item in zip(ids, items) if id not in existing_ids]

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None


class AsyncStateMachine(AsyncStateStoreBase):
    id_keys: dict[str, str]
    db_path: str
    write_lock: asyncio.Lock
    conn: aiosqlite.Connection | None

    def __init__(self, db_path: str, object_id_keys: dict[str, str]):
        self.db_path = _rectify_path(db_path)
        self.id_keys = object_id_keys.copy()
        self.write_lock = asyncio.Lock()
        self.conn = None

    async def __aenter__(self):
        self.conn = await aiosqlite.connect(self.db_path, timeout=300, check_same_thread=False, isolation_level=None)
        await self.conn.executescript(_INIT_SQL)
        await self.conn.executescript(create_table_expr(self.id_keys))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            await self.conn.close()
            self.conn = None

    # @asynccontextmanager
    # async def _write_transaction(self):
    #     if not self.conn:
    #         raise RuntimeError(
    #             "AsyncStateMachine not initialized. Use 'async with' context manager."
    #         )
    #     async with self.write_lock:
    #         try:
    #             cur = await self.conn.cursor()
    #             # await cur.execute("BEGIN IMMEDIATE;")
    #             yield cur                
    #             await self.conn.commit()
    #         except Exception as e:
    #             ic(e.__class__.__name__, e)
    #             # await self.conn.rollback()
    #             raise e
    #         finally:
    #             await cur.close()

    async def _read(self, expr: str, params=None):
        cur = await self.conn.execute(expr, params)
        result = await cur.fetchall()
        await cur.close()
        return result

    @retry(exceptions=aiosqlite.OperationalError, tries=10, jitter=DB_JITTER)
    async def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        if not items:
            return 0

        async with self.write_lock:
            cur = await self.conn.cursor()
            await cur.execute("BEGIN IMMEDIATE;")
            result = await cur.executemany(
                insert_expr(object_type),
                create_rows(self.id_keys[object_type], state, items),
            )
            rowcount = result.rowcount
            await self.conn.commit()
            await cur.close()
        return rowcount

    async def get(
        self,
        object_type: str,
        states: str | list[str],
        exclude_states: str | list[str],
        limit: int = 0,
        offset: int = 0,
    ):
        expr, params = query_expr(
            object_type, states, exclude_states, limit, offset
        )
        result = await self._read(expr, params)
        return [decode_data(r[0]) for r in result]

    async def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = get_ids(items, self.id_keys[object_type])
        result = await self._read(
            exists_expr(object_type, ids),
            [state] + ids,
        )
        existing_ids = {r[0] for r in result}

        return [item for id, item in zip(ids, items) if id not in existing_ids]


get_id = (
    lambda item, id_key: getattr(item, id_key)
    if isinstance(item, BaseModel)
    else item[id_key]
)
get_ids = lambda items, id_key: [get_id(item, id_key) for item in items]


def create_rows(
    id_key: str, state: str, items: list[dict[str, Any]] | list[BaseModel]
):
    ts = datetime.now(tz=timezone.utc)
    return [
        (
            id,
            state,
            ts,
            encode_data(data),
        )
        for id, data in zip(get_ids(items, id_key), items)
    ]


def _single_state_query_expr(table: str, include_state: str, exclude_state: str):
    expr = f"""SELECT data FROM {table} 
    WHERE state = ?
    AND NOT EXISTS (
        SELECT 1 FROM {table} t2 WHERE t2.id = {table}.id AND t2.state = ?
    )
    """
    return expr, [include_state, exclude_state]


def _multi_state_query_expr(
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


def query_expr(
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
        expr, params = _multi_state_query_expr(table, states, exclude_states)
    else:
        if isinstance(states, list):
            states = states[0]
        if isinstance(exclude_states, list):
            exclude_states = exclude_states[0]
        expr, params = _single_state_query_expr(table, states, exclude_states)

    if limit:
        expr = f"{expr} LIMIT ?"
        params.append(limit)
    if offset:
        expr = f"{expr} OFFSET ?"
        params.append(offset)

    return expr, params


insert_expr = lambda table: f"INSERT OR IGNORE INTO {table} (id, state, ts, data) VALUES (?, ?, ?, ?)"
exists_expr = lambda table, ids: f"SELECT id FROM {table} WHERE state = ? AND id IN ({','.join(['?'] * len(ids))})"
create_table_expr = lambda id_keys: "\n".join([_CREATE_TABLE_SQL.format(table=table) for table in id_keys.keys()])
