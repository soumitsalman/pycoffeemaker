import asyncio
import logging
import re
import os
from datetime import datetime, timezone
from typing import Any, Optional
from icecream import contextmanager
from psycopg import sql
from psycopg_pool import ConnectionPool
import msgpack
from pydantic import BaseModel
from icecream import ic

ID = "id"
STATE = "state"
TS = "ts"
DATA = "data"

TIMEOUT = 600

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    id TEXT NOT NULL,
    state TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    data BYTEA DEFAULT NULL,
    PRIMARY KEY (id, state)
);
CREATE INDEX IF NOT EXISTS {table}_id_idx ON {table}(id);
CREATE INDEX IF NOT EXISTS {table}_state_idx ON {table}(state);
"""

log = logging.getLogger(__name__)

class StateMachine:
    id_keys: dict[str, str]
    conn_str: str
    pool: ConnectionPool

    def __init__(self, conn_str: str, object_id_keys: dict[str, str]):
        self.id_keys = object_id_keys.copy()
        self.conn_str = conn_str
        self.pool = ConnectionPool(
            conn_str,
            min_size=4,
            max_size=64,
            timeout=TIMEOUT,
            max_idle=TIMEOUT,
            num_workers=os.cpu_count(),
        )
        self.pool.open()

        table_exprs = [_CREATE_TABLE_SQL.format(table=table) for table in object_id_keys.keys()]
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("\n".join(table_exprs)))

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

        expr = sql.SQL(
            "INSERT INTO {table} (id, state, ts, data) VALUES (%(id)s, %(state)s, %(ts)s, %(data)s) ON CONFLICT (id, state) DO NOTHING;"
        ).format(table=sql.Identifier(object_type))
        rows = self._create_rows(object_type, state, items)

        with self.pool.connection() as conn:
            with conn.cursor(binary=True) as cur:
                cur.executemany(expr, rows)
                rowcount = cur.rowcount
        return rowcount

    def _create_rows(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        ts = datetime.now(tz=timezone.utc)
        return [
            {
                "id": id,
                "state": state,
                "ts": ts,
                "data": encode_data(data),
            }
            for id, data in zip(get_ids(items, self.id_keys[object_type]), items)
        ]

    def get(
        self,
        object_type: str,
        states: str | list[str],
        exclude_states: str | list[str],
        limit: int = 0,
        offset: int = 0,
    ):
        query_expr, params = create_query_expr(
            object_type, states, exclude_states, limit, offset
        )
        with self.pool.connection() as conn:
            with conn.cursor(binary=True) as cur:
                result = cur.execute(query_expr, params).fetchall()
        return [decode_data(r[0]) for r in result]

    def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = [get_id(item, self.id_keys[object_type]) for item in items]
        expr = sql.SQL(
            "SELECT id FROM {table} WHERE state = %(state)s AND id = ANY(%(ids)s)"
        ).format(table=sql.Identifier(object_type))
        params = {"state": state, "ids": ids}
        with self.pool.connection() as conn:
            with conn.cursor(binary=True) as cur:
                result = cur.execute(expr, params).fetchall()
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


def _create_single_state_query_expr(table: str, include_state: str, exclude_state: str):
    """
    SELECT data FROM table
    WHERE state = $include_state
    AND NOT EXISTS (
        SELECT 1 FROM table t2 WHERE t2.id = table.id AND t2.state = $exclude_state
    )
    """
    expr = sql.SQL("""SELECT data FROM {table} 
    WHERE state = %(include_state)s
    AND NOT EXISTS (
        SELECT 1 FROM {table} t2 WHERE t2.id = {table}.id AND t2.state = %(exclude_state)s
    )
    """).format(table=sql.Identifier(table))
    return expr, {"include_state": include_state, "exclude_state": exclude_state}


def _create_multi_state_query_expr(
    table: str, include_states: list[str], exclude_states: list[str]
):
    """
    WITH filtered AS (
        SELECT id FROM table
        WHERE state != ALL(%(exclude_states)s::varchar[])
        GROUP BY id
        HAVING COUNT(DISTINCT CASE WHEN state = ANY(%(include_states)s::varchar[]) THEN state END) >= %(min_count)s
    )
    SELECT data FROM table
    WHERE EXISTS (SELECT 1 FROM filtered WHERE filtered.id = table.id)
    """
    expr = sql.SQL("""WITH filtered AS (
        SELECT id FROM {table}
        WHERE state != ALL(%(exclude_states)s::varchar[])
        GROUP BY id
        HAVING COUNT(DISTINCT CASE WHEN state = ANY(%(include_states)s::varchar[]) THEN state END) >= %(min_count)s
    )
    SELECT data FROM {table}
    WHERE EXISTS (SELECT 1 FROM filtered WHERE filtered.id = {table}.id)
    """).format(table=sql.Identifier(table))
    params = {
        "include_states": include_states,
        "exclude_states": exclude_states,
        "min_count": len(include_states),
    }
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
        expr = sql.SQL("{expr} LIMIT %(limit)s").format(expr=expr)
        params["limit"] = limit
    if offset:
        expr = sql.SQL("{expr} OFFSET %(offset)s").format(expr=expr)
        params["offset"] = offset

    return expr, params
