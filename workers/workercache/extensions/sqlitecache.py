from datetime import datetime, timedelta, timezone
import queue
import threading
from typing import Any
from duckdb import execute
from pydantic import BaseModel
from retry import retry
import turso
import asyncio
import aiosqlite
from .base import *
from icecream import ic
from pathlib import Path

DEFAULT_DB_PATH = "statestore.db"
DB_JITTER = (1, 5)
TIMEOUT = 300

_INIT_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA cache_size=64000;
PRAGMA busy_timeout=300000;
PRAGMA mmap_size=268435456;
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

class StateCache(StateCacheBase):
    id_keys: dict[str, str]
    db_path: str
    write_queue: queue.Queue

    def __init__(self, db_path: str, table_settings: dict[str, dict[str, Any]]):
        self.db_path = _rectify_path(db_path)
        self.table_settings = table_settings
        self.id_keys = {tab: setting["id_key"] for tab, setting in table_settings.items() if "id_key" in setting}

        self.read_conn = None
        self.write_queue = queue.Queue()        
        self.writer_thread = threading.Thread(target=self._run_write)
        self.writer_thread.start()
        self._init_db()
    
    def _init_db(self):        
        self.write_queue.put_nowait((_create_state_tables_sql(self.table_settings), None))
    
    def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        if not items: return
        
        self.write_queue.put_nowait((
            _create_insert_state_multivalues_sql(object_type, len(items)), 
            create_rows(self.id_keys[object_type], state, items)
        ))
    
    def _run_write(self):
        # @retry(tries=20, jitter=DB_JITTER)
        def start_write(items):
            expr, rows = items

            writer_conn = turso.connect(self.db_path, experimental_features="mvcc")
            writer_conn.executescript("PRAGMA journal_mode = 'mvcc';\nPRAGMA busy_timeout = 300000;")
            writer_conn.executescript("BEGIN CONCURRENT;")
            
            execute = lambda expr, rows: writer_conn.execute(expr, rows) if rows else writer_conn.executescript(expr)         
            total = execute(expr, rows).rowcount
            while not self.write_queue.empty():
                items = self.write_queue.get_nowait()
                if not items:
                    break
                expr, rows = items
                total += execute(expr, rows).rowcount

            writer_conn.commit()
            writer_conn.close()

            return total

        while True:
            items = self.write_queue.get()
            if not items:
                break
            res = start_write(items)
            logset(res)

    def get(
        self,
        object_type: str,
        states: str | list[str],
        exclude_states: str | list[str],
        limit: int = 0,
        offset: int = 0,
    ):
        expr, params = _create_state_query_expr(
            object_type, states, exclude_states, limit, offset
        )
        result = self._read(expr, params)
        return [decode_data(r[0]) for r in result]

    def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = get_field_vals(items, self.id_keys[object_type])
        result = self._read(
            _create_id_exists_expr(object_type, ids),
            [state] + ids,
        )
        existing_ids = {r[0] for r in result}

        return [item for id, item in zip(ids, items) if id not in existing_ids]
    
    def _read(self, expr: str, params=None):
        conn = turso.connect(self.db_path, experimental_features="mvcc")
        cur = conn.cursor()
        result = cur.execute(expr, params).fetchall()
        cur.close()
        conn.close()
        return result
    
    def optimize(self, cleanup_older_than: int = 7):
        for table in self.id_keys.keys():
            self.write_queue.put_nowait((f"UPDATE {table} SET data = NULL WHERE ts < ?", datetime.now() - timedelta(days=cleanup_older_than)))        

    def close(self):
        if self.writer_thread.is_alive():
            [self.write_queue.put_nowait(None) for _ in range(5)]
            self.writer_thread.join()
        if self.read_conn:
            self.read_conn.close()
            self.read_conn = None


class AsyncStateCache(AsyncStateCacheBase):
    db_path: str
    table_settings: dict[str, dict[str, Any]]
    id_keys: dict[str, str]
    write_queue: asyncio.Queue
    write_task: asyncio.Task | None

    def __init__(self, db_path: str, table_settings: dict[str, dict[str, Any]]):
        self.db_path = _rectify_path(db_path)
        self.table_settings = table_settings
        self.id_keys = {tab: setting["id_key"] for tab, setting in table_settings.items() if "id_key" in setting}
        self.write_queue = None
        self.write_task = None
    
    def _init_db(self):        
        self.write_queue.put_nowait((_create_state_tables_sql(self.table_settings), None))

    def _read(self, expr: str, params=None):
        cur = self.read_conn.cursor()
        result = cur.execute(expr, params).fetchall()
        cur.close()
        return result

    async def __aenter__(self):
        self.read_conn = turso.connect(self.db_path)
        self.write_queue = asyncio.Queue()
        self.write_task = asyncio.create_task(self._run_write())
        self._init_db()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        [self.write_queue.put_nowait(None) for _ in range(5)]  # Signal the write task to exit
        await self.write_task
        if self.read_conn:
            self.read_conn.close()
            self.read_conn = None  

    async def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        if not items: return

        self.write_queue.put_nowait((_create_insert_state_multivalues_sql(object_type, len(items)), create_rows(self.id_keys[object_type], state, items)))
    
    async def _run_write(self):
        # @retry(tries=20, jitter=DB_JITTER)
        async def start_write(items):
            expr, rows = items

            conn = turso.connect(self.db_path, experimental_features="mvcc")            
            await asyncio.to_thread(lambda: conn.execute("PRAGMA journal_mode = 'mvcc';").fetchone())
            await asyncio.to_thread(lambda: conn.executescript("BEGIN CONCURRENT"))
            
            execute = lambda expr, rows: asyncio.to_thread(conn.execute, expr, rows) if rows else asyncio.to_thread(conn.executescript, expr)
            total = (await execute(expr, rows)).rowcount
            while not self.write_queue.empty():
                items = self.write_queue.get_nowait()
                if not items:
                    break
                expr, rows = items
                total += (await execute(expr, rows)).rowcount

            await asyncio.to_thread(conn.commit)            
            conn.close()

            return total

        while True:
            items = await self.write_queue.get()
            if not items:
                break
            res = await start_write(items)
            logset(res)

    async def get(
        self,
        object_type: str,
        states: str | list[str],
        exclude_states: str | list[str],
        limit: int = 0,
        offset: int = 0,
    ):
        expr, params = _create_state_query_expr(
            object_type, states, exclude_states, limit, offset
        )
        result = await asyncio.to_thread(self._read, expr, params)
        return [decode_data(r[0]) for r in result]

    async def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = get_field_vals(items, self.id_keys[object_type])
        result = await asyncio.to_thread(
            self._read,
            _create_id_exists_expr(object_type, ids),
            [state] + ids,
        )
        existing_ids = {r[0] for r in result}

        return [item for id, item in zip(ids, items) if id not in existing_ids]
    
    async def optimize(self, cleanup_older_than: int = 7):
        for table in self.id_keys.keys():
            self.write_queue.put_nowait((
                f"UPDATE {table} SET data = NULL WHERE ts < ?", 
                (datetime.now() - timedelta(days=cleanup_older_than)).strftime("%Y-%m-%dT%H:%M:%S%Z"),
            )) 


### INITIALIZE
_CREATE_STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    id TEXT NOT NULL,
    state TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    data BLOB DEFAULT NULL,
    PRIMARY KEY (id, state)
);
CREATE INDEX IF NOT EXISTS {table}_id_idx ON {table}(id);
CREATE INDEX IF NOT EXISTS {table}_state_idx ON {table}(state);
"""

def _create_state_tables_sql(table_settings: dict[str, dict[str, Any]]):
    exprs = [_CREATE_STATE_TABLE_SQL.format(table=name) for name, settings in table_settings.items() if "id_key" in settings]
    return "\n".join(exprs)

### SET VALUES
_create_insert_state_multivalues_sql = lambda table, rowcount: f"""
INSERT OR IGNORE INTO {table} (id, state, ts, data) 
VALUES {', '.join(['(?, ?, ?, ?)']*rowcount)}
"""

get_field_val = (
    lambda item, id_key: getattr(item, id_key)
    if isinstance(item, BaseModel)
    else item[id_key]
)
get_field_vals = lambda items, id_key: [get_field_val(item, id_key) for item in items]

def create_rows(
    id_key: str, state: str, items: list[dict[str, Any]] | list[BaseModel]
):
    ts = datetime.now(tz=timezone.utc).isoformat()
    rows = [
        (
            id,
            state,
            ts,
            encode_data(data),
        )
        for id, data in zip(get_field_vals(items, id_key), items)
    ]
    return [value for row in rows for value in row]


### QUERY VALUES
def _create_single_state_query_expr(table: str, include_state: str, exclude_state: str):
    expr = f"""SELECT data FROM {table} 
    WHERE state = ?
    AND NOT EXISTS (
        SELECT 1 FROM {table} t2 
        WHERE t2.id = {table}.id AND t2.state = ?
    )
    AND ts >= date('now', '-14 days')
    """
    return expr, [include_state, exclude_state]

_QUERY_MULTI_STATE_SQL = """
WITH filtered AS (
    SELECT id FROM {table} incl
    WHERE state IN ({include_placeholders})
        AND NOT EXISTS (
            SELECT 1 FROM {table} excl 
            WHERE excl.id = incl.id AND excl.state IN ({exclude_placeholders})
        )
        AND ts >= date('now', '-14 days')
    GROUP BY id
    HAVING COUNT(*) >= ?
)
SELECT data FROM {table}
WHERE EXISTS (
    SELECT 1 FROM filtered WHERE filtered.id = {table}.id
)
"""

def _create_multi_state_query_expr(
    table: str, include_states: list[str], exclude_states: list[str]
):
    include_placeholders = ",".join(["?"] * len(include_states))
    exclude_placeholders = ",".join(["?"] * len(exclude_states))
    expr = _QUERY_MULTI_STATE_SQL.format(table=table, include_placeholders=include_placeholders, exclude_placeholders=exclude_placeholders)
    params = include_states + exclude_states +[len(include_states)]
    return expr, params


def _create_state_query_expr(
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
        expr = f"{expr}\nLIMIT ?"
        params.append(limit)
    if offset:
        expr = f"{expr}\nOFFSET ?"
        params.append(offset)

    return expr, params

### DEDUPLICATE
_create_id_exists_expr = lambda table, ids: f"SELECT id FROM {table} WHERE state = ? AND id IN ({','.join(['?'] * len(ids))})"
