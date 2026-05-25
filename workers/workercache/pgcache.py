import asyncio
from itertools import batched, chain
import os
import queue
import threading
from retry import retry
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from pydantic import BaseModel
from .base import *
from psycopg_pool import AsyncConnectionPool, ConnectionPool
from pgvector.psycopg import register_vector, Vector
from icecream import ic

TIMEOUT = 270
MAX_WORKERS = 16
BATCH_SIZE = 128

PROCESSING_WINDOW = int(os.getenv('PROCESSING_WINDOW', 60))

##############
# STATE CACHE
##############

class StateCache(StateCacheBase):
    conn_str: str
    table_settings: dict[str, dict[str, Any]]
    id_keys: dict[str, str]
    pool: ConnectionPool
    write_queue: queue.Queue
    writer_thread: threading.Thread

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
        self.id_keys = {
            tab: setting["id_key"]
            for tab, setting in table_settings.items()
            if "id_key" in setting
        }
        self.pool = ConnectionPool(
            self.conn_str,
            min_size=0,
            max_size=16,
            timeout=TIMEOUT,
            max_idle=TIMEOUT,
            num_workers=MAX_WORKERS,
        )
        self.pool.open()
        self._init_db()

    def _init_db(self):
        with self.pool.connection() as conn:
            conn.execute(_create_state_tables_sql(self.table_settings))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @retry(tries=3, delay=15)
    def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        rows = _create_rows(self.id_keys[object_type], state, items)
        if not rows:
            return

        def insert_chunk(chunk: list):
            with self.pool.connection() as conn:
                return conn.execute(
                    _insert_state_multivalues_sql(object_type, len(chunk)),
                    list(chain.from_iterable(chunk)),
                ).rowcount

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
            counts = list(exec.map(insert_chunk, batched(rows, BATCH_SIZE)))
        return sum(counts)

    def get(
        self,
        object_type: str,
        states: str | list[str],
        exclude_states: str | list[str] = NULL_STATE,
        ids: list[str] = None,
        window: int = PROCESSING_WINDOW,
        limit: int = 0,
        offset: int = 0,
    ):
        expr, params = create_query_expr(object_type, states, exclude_states, ids, window, limit, offset)
        with self.pool.connection() as conn:
            rows = _read(conn, expr, params)
        return deserialize_data_rows(rows) 

    def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = get_field_vals(items, self.id_keys[object_type])
        expr = _EXISTS_SQL.format(table=object_type)
        with self.pool.connection() as conn:
            existing_ids = _read(conn, expr, {"state": state, "ids": ids})
        return [
            item for item, item_id in zip(items, ids) if item_id not in existing_ids
        ]

    def optimize(self, cleanup_older_than: int = 7):
        threshold = datetime.now(tz=timezone.utc) - timedelta(days=cleanup_older_than)
        with self.pool.connection() as conn:
            [conn.execute(_CLEANUP_OLD_SQL.format(table=table), {"threshold": threshold}) for table in self.id_keys]

    def close(self):
        self.pool.close()


class AsyncStateCache(AsyncStateCacheBase):
    conn_str: str
    table_settings: dict[str, dict[str, Any]]
    id_keys: dict[str, str]
    pool: AsyncConnectionPool
    write_queue: asyncio.Queue
    write_task: asyncio.Task | None

    def __init__(self, conn_str: str, table_settings: dict[str, dict[str, Any]]):
        self.conn_str = conn_str
        self.table_settings = table_settings
        self.id_keys = {
            tab: setting["id_key"]
            for tab, setting in table_settings.items()
            if "id_key" in setting
        }
        self.pool = None

    async def _init_db(self):
        async with self.pool.connection() as conn:
            await conn.execute(_create_state_tables_sql(self.table_settings))

    async def __aenter__(self):
        self.pool = AsyncConnectionPool(
            self.conn_str,
            min_size=0,
            max_size=32,
            timeout=TIMEOUT,
            max_idle=TIMEOUT,
            num_workers=MAX_WORKERS,
        )
        await self.pool.open()
        await self._init_db()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    @retry(tries=3, delay=15)
    async def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        rows = _create_rows(self.id_keys[object_type], state, items)
        if not rows:
            return

        async with self.pool.connection() as conn:
            results = await asyncio.gather(*(
                conn.execute(
                    _insert_state_multivalues_sql(object_type, len(chunk)),
                    list(chain.from_iterable(chunk)),
                )
                for chunk in batched(rows, BATCH_SIZE)
            ))
            return sum(result.rowcount for result in results)

    async def get(
        self,
        object_type: str,
        states: str | list[str],
        exclude_states: str | list[str] = NULL_STATE,
        ids: list[str] = None,
        window: int = PROCESSING_WINDOW,
        limit: int = 0,
        offset: int = 0,
    ):
        expr, params = create_query_expr(object_type, states, exclude_states, ids, window, limit, offset)
        async with self.pool.connection() as conn:
            rows = await _read_async(conn, expr, params)
        return deserialize_data_rows(rows)         

    async def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = get_field_vals(items, self.id_keys[object_type])
        expr = _EXISTS_SQL.format(table=object_type)
        async with self.pool.connection() as conn:
            existing_ids = await _read_async(conn, expr, {"state": state, "ids": ids})
        return [
            item for item, item_id in zip(items, ids) if item_id not in existing_ids
        ]

    async def optimize(self, cleanup_older_than: int = 7):
        threshold = datetime.now(tz=timezone.utc) - timedelta(days=cleanup_older_than)
        for table in self.id_keys:
            expr = _CLEANUP_OLD_SQL.format(table=table)
            async with self.pool.connection() as conn:
                await conn.execute(expr, {"threshold": threshold})

    async def close(self):
        await self.pool.close()


def _read(conn, expr: str, params=None):
    rows = conn.execute(expr, params, binary=True).fetchall()
    return [row[0] for row in rows]

async def _read_async(conn, expr: str, params=None):    
    result = await conn.execute(expr, params, binary=True)
    rows = await result.fetchall()
    return [row[0] for row in rows]

def merge(group: list[dict[str, Any]]):
    if not group: return
    if not (isinstance(group, list) and all(isinstance(item, dict) for item in group if item)):
        raise ValueError("Group must be a list of dictionaries")
    pack = {}    
    [pack.update({k: v for k, v in fields.items() if v}) for fields in group if fields]
    return pack

def deserialize_data_rows(rows: list[bytes]):
    return [
        decode_data(row) if not isinstance(row, list) 
        else merge([decode_data(data) for data in row]) 
        for row in rows
    ]

# STATE TABLES
_insert_state_multivalues_sql = (
    lambda table, rowcount: f"""
INSERT INTO {table} (id, state, ts, data) 
VALUES {", ".join(["(%s, %s, %s, %s)"] * rowcount)} 
ON CONFLICT (id, state) DO NOTHING
"""
)

_INSERT_STATE_TEMP_SQL = """
CREATE TEMP TABLE {table}_stage (
    id TEXT NOT NULL,
    state TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    data BYTEA DEFAULT NULL
) ON COMMIT DROP
"""
_INSERT_STATE_COPY_SQL = "COPY {table}_stage (id, state, ts, data) FROM STDIN"
_INSERT_STATE_PORT_SQL = """
INSERT INTO {table} (id, state, ts, data)
SELECT id, state, ts, data FROM {table}_stage
ON CONFLICT (id, state) DO NOTHING
"""


def _copy_insert_state_rows(pool: ConnectionPool, work_batch: dict[str, list]):
    """Insert rows using COPY + staging table for optimal performance on large batches.

    Uses PostgreSQL's COPY FROM STDIN for fast bulk loading, with ON CONFLICT handling
    via final INSERT ... SELECT statement.
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            for table, rows in work_batch.items():
                # CREATE temporary staging table with ON COMMIT DROP
                cur.execute(_INSERT_STATE_TEMP_SQL.format(table=table))
                # COPY data into staging table
                with conn.cursor().copy(
                    _INSERT_STATE_COPY_SQL.format(table=table)
                ) as copy:
                    [copy.write_row(row) for row in rows]
                # INSERT from staging to real table with conflict handling
                cur.execute(_INSERT_STATE_PORT_SQL.format(table=table))


async def _copy_insert_state_rows_async(
    pool: AsyncConnectionPool, work_batch: dict[str, list]
):
    """Async version: Insert rows using COPY + staging table for optimal performance.

    Uses PostgreSQL's COPY FROM STDIN for fast bulk loading, with ON CONFLICT handling
    via final INSERT ... SELECT statement.
    """
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            for table, rows in work_batch.items():
                # CREATE temporary staging table with ON COMMIT DROP
                await cur.execute(_INSERT_STATE_TEMP_SQL.format(table=table))
                # COPY data into staging table
                async with conn.cursor().copy(
                    _INSERT_STATE_COPY_SQL.format(table=table)
                ) as copy:
                    await asyncio.gather(*[copy.write_row(row) for row in rows])
                # INSERT from staging to real table with conflict handling
                await cur.execute(_INSERT_STATE_PORT_SQL.format(table=table))


def _create_rows(
    id_key: str,
    state: str,
    items: list[dict[str, Any]] | list[BaseModel],
):
    if not items:
        return

    ts = datetime.now(tz=timezone.utc)
    return [
        (get_field_val(item, id_key), state, ts, encode_data(item)) for item in items
    ]


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

def _create_state_tables_sql(table_settings: dict[str, dict[str, Any]]):
    exprs = [
        _CREATE_STATE_TABLE_SQL.format(table=name)
        for name, settings in table_settings.items()
        if "id_key" in settings
    ]
    return "\n".join(exprs)


def create_query_expr(
    table: str,
    states: str | list[str],
    exclude_states: str | list[str],
    ids: list[str] = None,
    window: int = DEFAULT_WINDOW,
    limit: int = 0,
    offset: int = 0,
):
    include_states = _normalize_states(states)
    excluded_states = _normalize_states(exclude_states)

    if len(include_states) > 1 or len(excluded_states) > 1:
        expr, params = _create_multi_state_query_expr(
            table, include_states, excluded_states, ids, window
        )
    else:
        expr, params = _create_single_state_query_expr(
            table, include_states[0], excluded_states[0], ids, window
        )

    if limit:
        expr += "\nLIMIT %(limit)s"
        params["limit"] = limit
    if offset:
        expr += "\nOFFSET %(offset)s"
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
{ts_expr}
{ids_expr}
"""
def _create_single_state_query_expr(
    table: str,
    include_state: str | None,
    exclude_state: str | None,
    ids: list[str] = None,
    window: int = None,
):
    params = {
        "include_state": include_state,
        "exclude_state": exclude_state
    }
    return _add_ts_and_ids_expr(_QUERY_SINGLE_STATE_SQL, table=table, params=params, window=window, ids=ids)


_QUERY_MULTI_STATE_SQL = """
SELECT ARRAY_AGG(data) AS data FROM {table} incl
WHERE state = ANY(%(include_states)s)
    AND NOT EXISTS (
        SELECT 1 FROM {table} excl 
        WHERE excl.id = incl.id AND excl.state = ANY(%(exclude_states)s)
    )
    {ts_expr}
    {ids_expr}
GROUP BY id
HAVING COUNT(*) >= %(min_count)s
"""
def _create_multi_state_query_expr(
    table: str,
    include_states: list[str],
    exclude_states: list[str],
    ids: list[str] = None,
    window: int = None,
):
    params = {
        "include_states": include_states,
        "exclude_states": exclude_states,
        "min_count": len(include_states)
    }
    return _add_ts_and_ids_expr(_QUERY_MULTI_STATE_SQL, table=table, params=params, window=window, ids=ids)

def _add_ts_and_ids_expr(template: str, table: str, params: dict, window: int, ids: list[str]):
    ts_expr, ids_expr = "", ""
    if window:
        ts_expr = f"AND ts >= CURRENT_TIMESTAMP - INTERVAL '{window} days'"
    if ids:
        ids_expr = f"AND id = ANY(%(ids)s)"
        params["ids"] = ids
    return template.format(table=table, ts_expr=ts_expr, ids_expr=ids_expr), params

_EXISTS_SQL = "SELECT id FROM {table} WHERE state = %(state)s AND id = ANY(%(ids)s)"
_CLEANUP_OLD_SQL = "UPDATE {table} SET data = NULL WHERE ts < %(threshold)s"
