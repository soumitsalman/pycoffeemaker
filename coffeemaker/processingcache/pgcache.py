import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Any

# from psycopg import sql
from psycopg_pool import AsyncConnectionPool, ConnectionPool
from pgvector.psycopg import register_vector, Vector

from pydantic import BaseModel

from .base import *

TIMEOUT = 600

class StateMachine(StateStoreBase):
    id_keys: dict[str, str]
    conn_str: str
    pool: ConnectionPool

    def __init__(self, conn_str: str, tables: dict[str, str]):
        self.conn_str = conn_str
        self.id_keys = tables.copy()
        self.pool = ConnectionPool(
            conn_str,
            min_size=4,
            max_size=64,
            timeout=TIMEOUT,
            max_idle=TIMEOUT,
            num_workers=os.cpu_count() or 1,
            # configure=register_vector
        )
        self.pool.open()
        self._init_db()

    def _init_db(self):
        if not self.id_keys: return
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_create_tables_sql(self.id_keys))

    def close(self):
        self.pool.close()

    def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        if not items:
            return 0

        expr = _INSERT_SQL.format(table=object_type)
        rows = create_rows(self.id_keys[object_type], state, items)
        with self.pool.connection() as conn:
            with conn.cursor(binary=True) as cur:
                cur.executemany(expr, rows)
                total = cur.rowcount
        return total

    def get(
        self,
        object_type: str,
        states: str | list[str],
        exclude_states: str | list[str],
        limit: int = 0,
        offset: int = 0,
    ):
        expr, params = create_query_expr(object_type, states, exclude_states, limit, offset)
        with self.pool.connection() as conn:
            with conn.cursor(binary=True) as cur:
                rows = cur.execute(expr, params).fetchall()
        return [decode_data(row[0]) for row in rows]

    def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = get_ids(items, self.id_keys[object_type])
        expr = _EXISTS_SQL.format(table=object_type)
        with self.pool.connection() as conn:
            with conn.cursor(binary=True) as cur:
                rows = cur.execute(expr, {"state": state, "ids": ids}).fetchall()
        existing_ids = {row[0] for row in rows}
        return [item for item, item_id in zip(items, ids) if item_id not in existing_ids]

    def optimize(self, cleanup_older_than: int = 7):
        threshold = datetime.now(tz=timezone.utc) - timedelta(days=cleanup_older_than)
        for table in self.id_keys:
            expr = _CLEANUP_OLD_SQL.format(table=table)
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(expr, {"threshold": threshold})


class AsyncStateMachine(AsyncStateStoreBase):
    id_keys: dict[str, str]
    vector_tables: set[str]
    conn_str: str
    pool: AsyncConnectionPool
    _ready: bool
    _setup_lock: asyncio.Lock

    def __init__(self, conn_str: str, tables: dict[str, str]):
        self.conn_str = conn_str
        self.id_keys = {tab: id_field for tab, id_field in tables.items()}

    async def _init_db(self):
        if not self.id_keys: return

        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(_create_tables_sql(self.id_keys))

    async def __aenter__(self):
        self.pool = AsyncConnectionPool(
            self.conn_str,
            min_size=4,
            max_size=64,
            timeout=TIMEOUT,
            max_idle=TIMEOUT,
            num_workers=os.cpu_count() or 1,
            # configure=register_vector
        )
        await self.pool.open()
        await self._init_db()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self.pool.close()

    async def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        if not items:
            return 0

        expr = _INSERT_SQL.format(table=object_type)
        rows = create_rows(self.id_keys[object_type], state, items)
        async with self.pool.connection() as conn:
            async with conn.cursor(binary=True) as cur:
                await cur.executemany(expr, rows)
                total = cur.rowcount
        # logset(total)
        return total

    async def get(
        self,
        object_type: str,
        states: str | list[str],
        exclude_states: str | list[str],
        limit: int = 0,
        offset: int = 0,
    ):
        expr, params = create_query_expr(object_type, states, exclude_states, limit, offset)
        async with self.pool.connection() as conn:
            async with conn.cursor(binary=True) as cur:
                result = await cur.execute(expr, params)
                rows = await result.fetchall()
        return [decode_data(row[0]) for row in rows]

    async def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = get_ids(items, self.id_keys[object_type])
        expr = _EXISTS_SQL.format(table=object_type)
        async with self.pool.connection() as conn:
            async with conn.cursor(binary=True) as cur:
                result = await cur.execute(expr, {"state": state, "ids": ids})
                rows = await result.fetchall()
        existing_ids = {row[0] for row in rows}
        return [item for item, item_id in zip(items, ids) if item_id not in existing_ids]

    async def optimize(self, cleanup_older_than: int = 7):
        threshold = datetime.now(tz=timezone.utc) - timedelta(days=cleanup_older_than)
        for table in self.id_keys:
            expr = _CLEANUP_OLD_SQL.format(table=table)
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(expr, {"threshold": threshold})   
    
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    id TEXT NOT NULL,
    state TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    data BYTEA DEFAULT NULL,
    PRIMARY KEY (id, state)
);
CREATE INDEX IF NOT EXISTS {table}_id_idx ON {table}(id);
CREATE INDEX IF NOT EXISTS {table}_state_idx ON {table}(state);
"""

_INSERT_SQL = """
    INSERT INTO {table} (id, state, ts, data) VALUES (%(id)s, %(state)s, %(ts)s, %(data)s)
    ON CONFLICT (id, state) DO NOTHING
"""
_EXISTS_SQL = "SELECT id FROM {table} WHERE state = %(state)s AND id = ANY(%(ids)s)"
_CLEANUP_OLD_SQL = "UPDATE {table} SET data = NULL WHERE ts < %(threshold)s"

def _create_tables_sql(id_keys):
    return "\n".join(_CREATE_TABLE_SQL.format(table=table) for table in id_keys)

def create_rows(
    id_key: str,
    state: str,
    items: list[dict[str, Any]] | list[BaseModel],
):
    ts = datetime.now(tz=timezone.utc)
    return [
        {
            "id": item_id,
            "state": state,
            "ts": ts,
            "data": encode_data(item),
        }
        for item_id, item in zip(get_ids(items, id_key), items)
    ]


def create_query_expr(
    table: str,
    states: str | list[str],
    exclude_states: str | list[str],
    limit: int = 0,
    offset: int = 0,
):
    include_states = _normalize_states(states)
    excluded_states = _normalize_states(exclude_states)

    if len(include_states) > 1 or len(excluded_states) > 1:
        expr, params = _create_multi_state_query_expr(table, include_states, excluded_states)
    else:
        expr, params = _create_single_state_query_expr( table, include_states[0], excluded_states[0])

    if limit:
        expr += "LIMIT %(limit)s"
        params["limit"] = limit
    if offset:
        expr += " OFFSET %(offset)s"
        params["offset"] = offset

    return expr, params


def _normalize_states(states: str | list[str] | None) -> list[str]:
    if states is None:
        return []
    if isinstance(states, str):
        return [states]
    return [state for state in states if state]


_QUERY_SINGLE_STATE_SQL = """
SELECT data FROM {table}
WHERE state = %(include_state)s
AND NOT EXISTS (SELECT 1 FROM {table} t2 WHERE t2.id = {table}.id AND t2.state = %(exclude_state)s)
"""
def _create_single_state_query_expr(
    table: str,
    include_state: str | None,
    exclude_state: str | None,
):
    return _QUERY_SINGLE_STATE_SQL.format(table=table), {"include_state": include_state, "exclude_state": exclude_state}


_QUERY_MULTI_STATE_SQL = """
WITH filtered AS (
    SELECT id FROM {table} incl
    WHERE state = ANY(%(include_states)s)
        AND NOT EXISTS (
            SELECT 1 FROM {table} excl 
            WHERE excl.id = incl.id AND excl.state = ANY(%(exclude_states)s)
        )
    GROUP BY id
    HAVING COUNT(*) >= %(min_count)s
)
SELECT data FROM {table}
WHERE EXISTS (
    SELECT 1 FROM filtered WHERE filtered.id = {table}.id
)
"""
def _create_multi_state_query_expr(
    table: str,
    include_states: list[str],
    exclude_states: list[str],
):
    return _QUERY_MULTI_STATE_SQL.format(table=table), {"include_states": include_states, "exclude_states": exclude_states, "min_count": len(include_states)}

