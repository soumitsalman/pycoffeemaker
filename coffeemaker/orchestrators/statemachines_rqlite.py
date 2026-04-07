import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional
from icecream import contextmanager
import msgpack
from pydantic import BaseModel
import pyrqlite.dbapi2 as rqlite

ID = "id"
STATE = "state"
TS = "ts"
DATA = "data"

TIMEOUT = 600

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

log = logging.getLogger(__name__)


class StateMachine:
    id_keys: dict[str, str]
    conn_str: str
    conn: rqlite.Connection

    def __init__(self, conn_str: str, object_id_keys: dict[str, str]):
        self.id_keys = object_id_keys.copy()
        self.conn_str = conn_str
        self.conn = rqlite.connect(conn_str, timeout=TIMEOUT)
        self.conn.execute("PRAGMA journal_mode=WAL;")

        table_exprs = [
            _CREATE_TABLE_SQL.format(table=table) for table in object_id_keys.keys()
        ]
        with self.conn.cursor() as cur:
            for expr in table_exprs:
                cur.execute(expr)

    def close(self):
        self.conn.close()

    def set(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        if not items:
            return 0

        expr = f"INSERT OR IGNORE INTO {object_type} (id, state, ts, data) VALUES (?, ?, ?, ?)"
        rows = self._create_rows(object_type, state, items)

        with self.conn.cursor() as cur:
            cur.executemany(expr, rows)
            rowcount = cur.rowcount
        return rowcount

    def _create_rows(
        self,
        object_type: str,
        state: str,
        items: list[dict[str, Any]] | list[BaseModel],
    ):
        ts = datetime.now(tz=timezone.utc).isoformat()
        return [
            (
                id,
                state,
                ts,
                encode_data(data),
            )
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
        with self.conn.cursor() as cur:
            result = cur.execute(query_expr, params).fetchall()
        return [decode_data(r[0]) for r in result]

    def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        ids = [get_id(item, self.id_keys[object_type]) for item in items]
        placeholders = ",".join(["?"] * len(ids))
        expr = (
            f"SELECT id FROM {object_type} WHERE state = ? AND id IN ({placeholders})"
        )
        params = [state] + ids
        with self.conn.cursor() as cur:
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
