import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import os
import queue
import threading
from typing import Any, Optional
from pathlib import Path
import hashlib
import zvec
from firebird.driver import connect, create_database
from pydantic import BaseModel

from .base import *
from icecream import ic


#################
## State Cache ##
#################

DEFAULT_DB_PATH = "statestore.fdb"
DEFAULT_USER = "SYSDBA"
DEFAULT_PASSWORD = "masterkey"


def _rectify_path(db_path: str) -> str:
    path = Path(db_path)
    if path.is_dir() or (not path.exists() and not path.suffix):
        path = path / DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _fb_connect(
    db_path: str, user: str = DEFAULT_USER, password: str = DEFAULT_PASSWORD
):
    return connect(db_path, user=user, password=password)


def _ensure_database(
    db_path: str, user: str = DEFAULT_USER, password: str = DEFAULT_PASSWORD
):
    if not os.path.exists(db_path):
        conn = create_database(db_path, user=user, password=password)
        conn.close()


def _executescript(cur, script: str):
    for stmt in script.split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)
    return cur


class StateCache(StateCacheBase):
    id_keys: dict[str, str]
    db_path: str
    user: str
    password: str
    write_queue: queue.Queue
    writer_thread: threading.Thread

    def __init__(
        self,
        db_path: str,
        table_settings: dict[str, dict[str, Any]],
        user: str = DEFAULT_USER,
        password: str = DEFAULT_PASSWORD,
    ):
        self.db_path = _rectify_path(db_path)
        self.user = user
        self.password = password
        self.table_settings = table_settings
        self.id_keys = {
            tab: setting["id_key"]
            for tab, setting in table_settings.items()
            if "id_key" in setting
        }

        self.write_queue = queue.Queue()
        self.writer_thread = threading.Thread(target=self._run_write, daemon=True)
        self.writer_thread.start()
        _ensure_database(self.db_path, user, password)
        self._init_db()

    def _init_db(self):
        self.write_queue.put_nowait(
            (_create_state_tables_sql(self.table_settings), None)
        )

    def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        if not items:
            return

        self.write_queue.put_nowait(
            (
                _merge_state_sql(object_type),
                _create_rows(self.id_keys[object_type], state, items),
            )
        )

    def _run_write(self):
        def start_write(items):
            expr, rows = items

            conn = _fb_connect(self.db_path, self.user, self.password)
            cur = conn.cursor()

            if rows is None:
                _executescript(cur, expr)
                conn.commit()
                total = 0
            else:
                cur.executemany(expr, rows)
                conn.commit()
                total = cur.rowcount if cur.rowcount > 0 else len(rows)

            while not self.write_queue.empty():
                items = self.write_queue.get_nowait()
                if not items:
                    break

                expr, rows = items
                if rows is None:
                    _executescript(cur, expr)
                    conn.commit()
                else:
                    cur.executemany(expr, rows)
                    conn.commit()
                    total += cur.rowcount if cur.rowcount > 0 else len(rows)

            cur.close()
            conn.close()
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
        conn = _fb_connect(self.db_path, self.user, self.password)
        cur = conn.cursor()
        result = cur.execute(expr, params).fetchall()
        cur.close()
        conn.close()
        return result

    def optimize(self, cleanup_older_than: int = 7):
        threshold = datetime.now(tz=timezone.utc) - timedelta(days=cleanup_older_than)
        for table in self.id_keys.keys():
            self.write_queue.put_nowait(
                (
                    _cleanup_sql(table),
                    [(threshold,)],
                )
            )

    def close(self):
        if self.writer_thread.is_alive():
            [self.write_queue.put_nowait(None) for _ in range(5)]
            self.writer_thread.join()


class AsyncStateCache(AsyncStateCacheBase):
    db_path: str
    table_settings: dict[str, dict[str, Any]]
    id_keys: dict[str, str]
    user: str
    password: str
    write_queue: asyncio.Queue
    write_task: asyncio.Task | None

    def __init__(
        self,
        db_path: str,
        table_settings: dict[str, dict[str, Any]],
        user: str = DEFAULT_USER,
        password: str = DEFAULT_PASSWORD,
    ):
        self.db_path = _rectify_path(db_path)
        self.user = user
        self.password = password
        self.table_settings = table_settings
        self.id_keys = {
            tab: setting["id_key"]
            for tab, setting in table_settings.items()
            if "id_key" in setting
        }
        self.write_queue = None
        self.write_task = None

    def _init_db(self):
        self.write_queue.put_nowait(
            (_create_state_tables_sql(self.table_settings), None)
        )

    def _read(self, expr: str, params=None):
        conn = _fb_connect(self.db_path, self.user, self.password)
        cur = conn.cursor()
        result = cur.execute(expr, params).fetchall()
        cur.close()
        conn.close()
        return result

    async def __aenter__(self):
        await asyncio.to_thread(
            _ensure_database, self.db_path, self.user, self.password
        )
        self.write_queue = asyncio.Queue()
        self.write_task = asyncio.create_task(self._run_write())
        self._init_db()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        [self.write_queue.put_nowait(None) for _ in range(5)]
        if self.write_task:
            await self.write_task

    async def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        if not items:
            return

        self.write_queue.put_nowait(
            (
                _merge_state_sql(object_type),
                _create_rows(self.id_keys[object_type], state, items),
            )
        )

    async def _run_write(self):
        async def start_write(items):
            expr, rows = items

            conn = await asyncio.to_thread(
                _fb_connect, self.db_path, self.user, self.password
            )
            cur = conn.cursor()

            if rows is None:
                await asyncio.to_thread(_executescript, cur, expr)
                await asyncio.to_thread(conn.commit)
                total = 0
            else:
                await asyncio.to_thread(cur.executemany, expr, rows)
                await asyncio.to_thread(conn.commit)
                total = cur.rowcount if cur.rowcount > 0 else len(rows)

            while not self.write_queue.empty():
                items = self.write_queue.get_nowait()
                if not items:
                    break

                expr, rows = items
                if rows is None:
                    await asyncio.to_thread(_executescript, cur, expr)
                    await asyncio.to_thread(conn.commit)
                else:
                    await asyncio.to_thread(cur.executemany, expr, rows)
                    await asyncio.to_thread(conn.commit)
                    total += cur.rowcount if cur.rowcount > 0 else len(rows)

            cur.close()
            await asyncio.to_thread(conn.close)
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
        threshold = datetime.now(tz=timezone.utc) - timedelta(days=cleanup_older_than)
        for table in self.id_keys.keys():
            self.write_queue.put_nowait(
                (
                    _cleanup_sql(table),
                    [(threshold,)],
                )
            )


_CREATE_STATE_TABLE_SQL = """
CREATE TABLE {table} (
    id VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL,
    ts TIMESTAMP NOT NULL,
    data BLOB SUB_TYPE 0 DEFAULT NULL,
    PRIMARY KEY (id, state)
)
"""

_CREATE_STATE_INDEX_SQL = """
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'CREATE INDEX {table}_id_idx ON {table}(id)';
    WHEN ANY DO BEGIN END
END
"""

_CREATE_STATE_INDEX2_SQL = """
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'CREATE INDEX {table}_state_idx ON {table}(state)';
    WHEN ANY DO BEGIN END
END
"""


def _create_state_tables_sql(table_settings: dict[str, dict[str, Any]]):
    parts = []
    for name, settings in table_settings.items():
        if "id_key" in settings:
            parts.append(_CREATE_STATE_TABLE_SQL.format(table=name))
            parts.append(_CREATE_STATE_INDEX_SQL.format(table=name))
            parts.append(_CREATE_STATE_INDEX2_SQL.format(table=name))
    return ";\n".join(parts)


_MERGE_STATE_SQL = """
MERGE INTO {table} AS tgt
USING (SELECT ? AS id, ? AS state, ? AS ts, ? AS data FROM RDB$DATABASE) AS src
ON tgt.id = src.id AND tgt.state = src.state
WHEN NOT MATCHED THEN
    INSERT (id, state, ts, data) VALUES (src.id, src.state, src.ts, src.data)
"""


def _merge_state_sql(table: str) -> str:
    return _MERGE_STATE_SQL.format(table=table)


def _create_rows(
    id_key: str, state: str, items: list[dict[str, Any]] | list[BaseModel]
):
    if not items:
        return None

    ts = datetime.now(tz=timezone.utc)
    rows = [
        (
            get_field_val(item, id_key),
            state,
            ts,
            encode_data(item),
        )
        for item in items
    ]
    return rows


_QUERY_SINGLE_STATE_SQL = """
SELECT data FROM {table}
WHERE state = ?
AND NOT EXISTS (
    SELECT 1 FROM {table} t2
    WHERE t2.id = {table}.id AND t2.state = ?
)
AND ts >= CURRENT_TIMESTAMP - 14
"""


def _create_single_state_query_expr(table: str, include_state: str, exclude_state: str):
    return _QUERY_SINGLE_STATE_SQL.format(table=table), [include_state, exclude_state]


_QUERY_MULTI_STATE_SQL = """
WITH filtered AS (
    SELECT id FROM {table} incl
    WHERE state IN ({include_placeholders})
    AND NOT EXISTS (
        SELECT 1 FROM {table} excl
        WHERE excl.id = incl.id AND excl.state IN ({exclude_placeholders})
    )
    AND ts >= CURRENT_TIMESTAMP - 14
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
    expr = _QUERY_MULTI_STATE_SQL.format(
        table=table,
        include_placeholders=include_placeholders,
        exclude_placeholders=exclude_placeholders,
    )
    params = include_states + exclude_states + [len(include_states)]
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
        expr = f"{expr}\nROWS ?"
        params.append(limit)
    if offset:
        expr = f"{expr}\nSKIP ?"
        params.append(offset)

    return expr, params


def _create_id_exists_expr(table: str, ids: list) -> str:
    id_placeholders = ",".join(["?"] * len(ids))
    return f"SELECT id FROM {table} WHERE state = ? AND id IN ({id_placeholders})"


def _cleanup_sql(table: str) -> str:
    return f"UPDATE {table} SET data = NULL WHERE ts < ?"


##########################
## Classification Cache ##
##########################


_METRIC_MAP = {"l2": zvec.MetricType.L2, "cosine": zvec.MetricType.COSINE}

class ClassificationCache(ClassificationCacheBase):
    def __init__(self, db_path: str, table_settings: dict[str, dict[str, Any]]):
        self.db_path = db_path
        self.table_settings = table_settings
        self.id_keys = {
            tab: setting["id_key"]
            for tab, setting in table_settings.items()
            if "id_key" in setting
        }

        Path(db_path).mkdir(parents=True, exist_ok=True)
        zvec.init(query_threads=os.cpu_count(), optimize_threads=max(1, os.cpu_count()>>1))
        self.collections: dict[str, zvec.Collection] = {}
        for tab, setting in table_settings.items():            
            path = os.path.join(db_path, tab)
            if os.path.exists(path): 
                coll = zvec.open(path=path)
            else:
                schema = zvec.CollectionSchema(
                    name=tab,
                    vectors=zvec.VectorSchema(
                        "embedding", 
                        data_type=zvec.DataType.VECTOR_FP32, 
                        dimension=setting["vector_length"],
                        index_param=zvec.HnswIndexParam(_METRIC_MAP.get(setting.get("distance_func"), zvec.MetricType.L2))
                    ),
                    fields=[zvec.FieldSchema(setting["id_key"], zvec.DataType.STRING)],
                )                
                coll = zvec.create_and_open(path=path, schema=schema)
            self.collections[tab] = coll

    def store(self, object_type: str, items: list[dict[str, Any]]) -> int:
        if not items:
            return 0
        id_key = self.id_keys[object_type]
        docs = [
            zvec.Doc(
                id=hashlib.sha256(item[id_key].encode('utf-8')).hexdigest(),
                vectors={"embedding": item["embedding"]},
                fields={id_key: item[id_key]},
            )
            for item in items
        ]
        results = self.collections[object_type].insert(docs)
        return len([r for r in results if r.code == 0])

    def search(self, object_type: str, embedding: list[float], distance: Optional[float] = None, top_n: int = DEFAULT_TOPN) -> list[str]:        
        results = self.collections[object_type].query(
            zvec.VectorQuery("embedding", vector=embedding),
            topk=top_n,
            output_fields=[self.id_keys[object_type]]
        )
        filter_distance = lambda r: not distance or (r.score <= distance)    
        return [r.fields[self.id_keys[object_type]] for r in filter(filter_distance, results)]

    def batch_search(self, object_type: str, embeddings: list[list[float]], distance: Optional[float] = None, top_n: int = DEFAULT_TOPN) -> list[list[str]]:
        return list(ThreadPoolExecutor().map(lambda emb: self.search(object_type, emb, distance, top_n), embeddings))        

    def close(self):
        [coll.optimize() for coll in self.collections.values()]
        self.collections.clear()
