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

TIMEOUT = 600
MAX_WORKERS = os.cpu_count() * os.cpu_count()
MAX_WRITE_ITEMS = 1024
MAX_READ_ITEMS = 8192

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
            max_size=16,
            timeout=TIMEOUT,
            max_idle=TIMEOUT,
            num_workers=os.cpu_count() or 1,
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

    def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        rows = _create_rows(self.id_keys[object_type], state, items)
        if not rows:
            return

        with self.pool.connection() as conn, ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
            res = list(exec.map(
                lambda chunk: conn.execute(
                    _insert_state_multivalues_sql(object_type, len(chunk)),
                    list(chain.from_iterable(chunk)),
                ),
                batched(rows, MAX_WRITE_ITEMS)
            ))
            conn.commit()
            count = sum(item.rowcount for item in res)
        return count

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
            rows = _read(conn, expr, params)
        return [decode_data(row) for row in rows]

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
            max_size=32,
            timeout=TIMEOUT,
            max_idle=TIMEOUT,
            num_workers=os.cpu_count() or 1,
        )
        await self.pool.open()
        await self._init_db()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

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
            res = await asyncio.gather(*(
                conn.execute(
                    _insert_state_multivalues_sql(object_type, len(chunk)),
                    list(chain.from_iterable(chunk)),
                )
                for chunk in batched(rows, MAX_WRITE_ITEMS)
            ))
            await conn.commit()
            return sum(item.rowcount for item in res)

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
            rows = await _read_async(conn, expr, params)
        return [decode_data(row) for row in rows]

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
    limit: int = 0,
    offset: int = 0,
):
    include_states = _normalize_states(states)
    excluded_states = _normalize_states(exclude_states)

    if len(include_states) > 1 or len(excluded_states) > 1:
        expr, params = _create_multi_state_query_expr(
            table, include_states, excluded_states
        )
    else:
        expr, params = _create_single_state_query_expr(
            table, include_states[0], excluded_states[0]
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
AND ts >= CURRENT_TIMESTAMP - INTERVAL '7 days'
"""


def _create_single_state_query_expr(
    table: str,
    include_state: str | None,
    exclude_state: str | None,
):
    return _QUERY_SINGLE_STATE_SQL.format(table=table), {
        "include_state": include_state,
        "exclude_state": exclude_state,
    }


_QUERY_MULTI_STATE_SQL = """
WITH filtered AS (
    SELECT id FROM {table} incl
    WHERE state = ANY(%(include_states)s)
        AND NOT EXISTS (
            SELECT 1 FROM {table} excl 
            WHERE excl.id = incl.id AND excl.state = ANY(%(exclude_states)s)
        )
        AND ts >= CURRENT_TIMESTAMP - INTERVAL '7 days'
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
    return _QUERY_MULTI_STATE_SQL.format(table=table), {
        "include_states": include_states,
        "exclude_states": exclude_states,
        "min_count": len(include_states),
    }


_EXISTS_SQL = "SELECT id FROM {table} WHERE state = %(state)s AND id = ANY(%(ids)s)"
_CLEANUP_OLD_SQL = "UPDATE {table} SET data = NULL WHERE ts < %(threshold)s"


###########################
# Classification Cache
###########################

_DISTANCE_OPS = {
    "l2": "<->",
    "cosine": "<=>"
}

class ClassificationCache(ClassificationCacheBase):
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
        self.distance_funcs = {tab: setting["distance_func"] for tab, setting in table_settings.items() if "distance_func" in setting}
        
        self.pool = ConnectionPool(conn_str, min_size=1, max_size=32, timeout=TIMEOUT, max_idle=TIMEOUT, num_workers=os.cpu_count() or 1, configure=register_vector)
        self.pool.open()
        self._init_db()

    def _init_db(self):
        with self.pool.connection() as conn:
            conn.execute(_create_emb_tables_sql(self.table_settings))

    def store(self, object_type: str, items: list[dict[str, Any]]):
        data = items # for future extension
        if not data: return 0

        id_key = self.id_keys[object_type]
        expr = f"INSERT INTO {object_type} ({id_key}, ts, embedding) VALUES {', '.join(['(%s, %s, %s)']*len(data))} ON CONFLICT ({id_key}) DO NOTHING"

        ts = datetime.now(tz=timezone.utc)
        rows = list(chain.from_iterable([
            [item[id_key], ts, Vector(item["embedding"])]
            for item in data
        ]))
        with self.pool.connection() as conn:
            res = conn.execute(expr, rows)
            return res.rowcount

    def distance_op(self, object_type):
        return _DISTANCE_OPS[self.distance_funcs.get(object_type, "l2")]

    @retry(tries=5, delay=2)
    def search(self, object_type: str, embedding: list[float], distance: Optional[float] = None, top_n: int = DEFAULT_TOPN):        
        expr, params = create_vector_search_expr(object_type, self.id_keys[object_type], embedding, self.distance_op(object_type), distance, top_n)
        with self.pool.connection() as conn:
            return _read(conn, expr, params)
    
    @retry(tries=5, delay=2)
    def batch_search(self, object_type: str, embeddings: list[list[float]], distance: Optional[float] = None, top_n: int = DEFAULT_TOPN):
        expr_and_params = [create_vector_search_expr(object_type, self.id_keys[object_type], embedding, self.distance_op(object_type), distance, top_n) for embedding in embeddings]
        with self.pool.connection() as conn:            
            results = list(ThreadPoolExecutor(max_workers=16).map(lambda expr_param: _read(conn, *expr_param), expr_and_params))
        return results
    
    def close(self):
        self.pool.close()

def create_vector_search_expr(object_type: str, id_key: str, query_embedding, distance_op: str, distance: float | None = None, top_n: int | None = None):
    expr = f"""SELECT {id_key} FROM {object_type}"""
    params = {"query_embedding": Vector(query_embedding)}
    
    if distance:
        expr += f"\nWHERE embedding {distance_op} %(query_embedding)s <= %(distance)s"
        params["distance"] = distance
    
    if top_n:
        expr += f"\nORDER BY embedding {distance_op} %(query_embedding)s LIMIT %(top_n)s"
        params["top_n"] = top_n
    return expr, params

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
def _create_emb_tables_sql(table_settings: dict[str, dict[str, Any]]):
    exprs = [_CREATE_EMB_TABLE_SQL.format(table=name, id_key=settings["id_key"], vector_length=settings["vector_length"]) for name, settings in table_settings.items() if "vector_length" in settings]
    return "\n".join(exprs)
