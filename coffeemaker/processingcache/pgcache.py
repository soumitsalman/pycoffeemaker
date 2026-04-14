import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from pydantic import BaseModel
from .base import *
import pandas as pd
from psycopg_pool import AsyncConnectionPool, ConnectionPool
from pgvector.psycopg import register_vector, Vector

from icecream import ic

TIMEOUT = 600

class ProcessingCache(ProcessingCacheBase):
    conn_str: str
    table_settings: dict[str, dict[str, Any]]
    id_keys: dict[str, str]
    pool: ConnectionPool

    def __init__(self, conn_str: str, table_settings: dict[str, dict[str, Any]]):
        """Initialize the ProcessingCache with a PostgreSQL connection string and table settings.
        
        Parameters:
            conn_str: The connection string to the PostgreSQL database.
            table_settings: A dictionary of table settings. table_settings should be formatted as 
            ```
            {
                table_name: {id_key: str},
                ...
            }
            ```
            It will `CREATE TABLE <table_name> (id, state, ts, data)` for each table in table_settings.
        """
        self.conn_str = conn_str
        self.table_settings = table_settings
        self.id_keys = {tab: setting["id_key"] for tab, setting in table_settings.items() if "id_key" in setting}
        
        self.pool = ConnectionPool(conn_str, min_size=4, max_size=64, timeout=TIMEOUT, max_idle=TIMEOUT, num_workers=os.cpu_count() or 1)
        self.pool.open()
        self._init_db()

    def _init_db(self):
        _execute(self.pool, _create_state_tables_sql(self.table_settings))
        
    def close(self):
        self.pool.close()

    def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        rows = create_rows(self.id_keys[object_type], state, items)
        if not rows: return 0
        
        return _execute(self.pool, _INSERT_STATE_TABLE_SQL.format(table=object_type), rows)

    def get(
        self,
        object_type: str,
        states: str | list[str],
        exclude_states: str | list[str],
        limit: int = 0,
        offset: int = 0,
    ):
        expr, params = create_query_expr(object_type, states, exclude_states, limit, offset)
        rows = _read(self.pool, expr, params)
        return [decode_data(row) for row in rows]

    def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = get_field_vals(items, self.id_keys[object_type])
        expr = _EXISTS_SQL.format(table=object_type)
        existing_ids = _read(self.pool, expr, {"state": state, "ids": ids})
        return [item for item, item_id in zip(items, ids) if item_id not in existing_ids]

    def optimize(self, cleanup_older_than: int = 7):
        threshold = datetime.now(tz=timezone.utc) - timedelta(days=cleanup_older_than)
        for table in self.id_keys:
            expr = _CLEANUP_OLD_SQL.format(table=table)
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(expr, {"threshold": threshold})


class AsyncProcessingCache(AsyncProcessingCacheBase):
    conn_str: str
    table_settings: dict[str, dict[str, Any]]
    id_keys: dict[str, str]
    pool: AsyncConnectionPool

    def __init__(self, conn_str: str, table_settings: dict[str, dict[str, Any]]):
        self.conn_str = conn_str
        self.table_settings = table_settings
        self.id_keys = {tab: setting["id_key"] for tab, setting in table_settings.items() if "id_key" in setting}

    async def _init_db(self):
        await _execute_async(self.pool, _create_state_tables_sql(self.table_settings))

    async def __aenter__(self):
        self.pool = AsyncConnectionPool(
            self.conn_str,
            min_size=4,
            max_size=64,
            timeout=TIMEOUT,
            max_idle=TIMEOUT,
            num_workers=os.cpu_count() or 1
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
        rows = create_rows(self.id_keys[object_type], state, items)
        if not rows: return 0

        return await _execute_async(self.pool, _INSERT_STATE_TABLE_SQL.format(table=object_type), rows)

    async def get(
        self,
        object_type: str,
        states: str | list[str],
        exclude_states: str | list[str],
        limit: int = 0,
        offset: int = 0,
    ):
        expr, params = create_query_expr(object_type, states, exclude_states, limit, offset)
        rows = await _read_async(self.pool, expr, params)
        return [decode_data(row) for row in rows]

    async def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = get_field_vals(items, self.id_keys[object_type])
        expr = _EXISTS_SQL.format(table=object_type)
        existing_ids = await _read_async(self.pool, expr, {"state": state, "ids": ids})
        return [item for item, item_id in zip(items, ids) if item_id not in existing_ids]

    async def optimize(self, cleanup_older_than: int = 7):
        threshold = datetime.now(tz=timezone.utc) - timedelta(days=cleanup_older_than)
        for table in self.id_keys:
            expr = _CLEANUP_OLD_SQL.format(table=table)
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(expr, {"threshold": threshold})   
    

class ClassificationCache:
    def __init__(self, conn_str: str, table_settings: dict[str, dict[str, Any]]):
        """Initialize the ProcessingCache with a PostgreSQL connection string and table settings.
        
        Parameters:
            conn_str: The connection string to the PostgreSQL database.
            table_settings: A dictionary of table settings. table_settings should be formatted as 
            ```
            {
                table_name: {id_key: str, vector_length: int, data: Optional[pd.DataFrame]},
                ...
            }
            ```
            if `id_key` & 'vector_length` is present then it will create a table with `CREATE TABLE <table_name> (id, embedding)`            
            if `data` is present then it will ignore `id_key` and `vector_length` and use the data to create a fixed table
        """
        self.conn_str = conn_str
        self.table_settings = table_settings
        self.id_keys = {tab: setting["id_key"] for tab, setting in table_settings.items() if "id_key" in setting}
        
        self.pool = ConnectionPool(conn_str, min_size=4, max_size=64, timeout=TIMEOUT, max_idle=TIMEOUT, num_workers=os.cpu_count() or 1, configure=register_vector)
        self.pool.open()
        self._init_db()

    def _init_db(self):
        _execute(self.pool, _create_emb_tables_sql(self.table_settings))

    def store(self, object_type: str, items: list[dict[str, Any]] | list[BaseModel] | pd.DataFrame):
        rows = create_embedding_rows(self.id_keys[object_type], items)
        if not rows: return 0

        return _execute(self.pool, _INSERT_EMB_TABLE_SQL.format(table=object_type, id_key=self.id_keys[object_type]), rows)

    def search(self, object_type: str, embedding: list[float], distance_func: str = "l2", distance: Optional[float] = None, top_n: Optional[int] = None):    
        expr, params = create_vector_search_expr(object_type, self.id_keys[object_type], embedding, distance_func, distance, top_n)
        return _read(self.pool, expr, params)
    
    def close(self):
        self.pool.close()


def _execute(pool, expr: str, rows = None):
    with pool.connection() as conn:
        with conn.cursor(binary=bool(rows)) as cur:
            cur.executemany(expr, rows) if rows else cur.execute(expr)
            total = cur.rowcount
    return total

async def _execute_async(pool, expr: str, rows = None):
    async with pool.connection() as conn:
        async with conn.cursor(binary=bool(rows)) as cur:
            if rows: await cur.executemany(expr, rows)
            else: await cur.execute(expr)
            total = cur.rowcount
    return total

def _read(pool, expr: str, params = None):
    with pool.connection() as conn:
        with conn.cursor(binary=bool(params)) as cur:
            rows = cur.execute(expr, params).fetchall()
    return [row[0] for row in rows]
    
async def _read_async(pool, expr: str, params = None):
    async with pool.connection() as conn:
        async with conn.cursor(binary=bool(params)) as cur:
            result = await cur.execute(expr, params)
            rows = await result.fetchall()
    return [row[0] for row in rows]

_CREATE_STATE_TABLE_SQL = """
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
_INSERT_STATE_TABLE_SQL = """
    INSERT INTO {table} (id, state, ts, data) VALUES (%(id)s, %(state)s, %(ts)s, %(data)s)
    ON CONFLICT (id, state) DO NOTHING
"""

def _create_state_tables_sql(table_settings: dict[str, dict[str, Any]]):
    exprs = [_CREATE_STATE_TABLE_SQL.format(table=name) for name, settings in table_settings.items() if "id_key" in settings]
    return "\n".join(exprs)

_CREATE_EMB_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    {id_key} TEXT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    embedding VECTOR({vector_length})
);
CREATE INDEX IF NOT EXISTS {table}_embeddings_idx 
    ON {table} USING ivfflat (embedding vector_l2_ops) 
    WITH (lists = 100);
"""
_INSERT_EMB_TABLE_SQL = """
    INSERT INTO {table} ({id_key}, ts, embedding) VALUES (%({id_key})s, %(ts)s, %(embedding)s)
    ON CONFLICT ({id_key}) DO NOTHING
"""

def _create_emb_tables_sql(table_settings: dict[str, dict[str, Any]]):
    exprs = [_CREATE_EMB_TABLE_SQL.format(table=name, id_key=settings["id_key"], vector_length=settings["vector_length"]) for name, settings in table_settings.items() if "vector_length" in settings]
    return "\n".join(exprs)
               

def create_rows(
    id_key: str,
    state: str,
    items: list[dict[str, Any]] | list[BaseModel],
):
    if not items: return

    ts = datetime.now(tz=timezone.utc)
    return [
        {
            "id": get_field_val(item, id_key),
            "state": state,
            "ts": ts,
            "data": encode_data(item),
        }
        for item in items
    ]

get_embeddings = lambda items: [get_field_val(item, "embedding") for item in items]
def create_embedding_rows(id_key: str, items: list[dict[str, Any]]):
    data = items # for future extension
    if not data: return
    
    ts = datetime.now(tz=timezone.utc)
    return [
        {
            id_key: item[id_key],
            "ts": ts,
            "embedding": Vector(item["embedding"])
        }
        for item in data
    ]

def create_vector_search_expr(object_type: str, id_key: str, query_embedding, distance_func: str = "l2", distance: float | None = None, top_n: int | None = None):
    expr = f"""SELECT {id_key} FROM {object_type}"""
    params = {"query_embedding": Vector(query_embedding)}
    distance_op = "<->" if distance_func == "l2" else "<=>"
    
    if distance:
        expr += f"\nWHERE embedding {distance_op} %(query_embedding)s <= %(distance)s"
        params["distance"] = distance
    
    if top_n:
        expr += f"\nORDER BY embedding {distance_op} %(query_embedding)s LIMIT %(top_n)s"
        params["top_n"] = top_n
    return expr, params

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

_EXISTS_SQL = "SELECT id FROM {table} WHERE state = %(state)s AND id = ANY(%(ids)s)"
_CLEANUP_OLD_SQL = "UPDATE {table} SET data = NULL WHERE ts < %(threshold)s"