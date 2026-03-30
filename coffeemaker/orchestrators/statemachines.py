import asyncio
from datetime import datetime, timezone
import random
import re
from typing import Any, Optional
from pydantic import BaseModel
from surrealdb import BlockingEmbeddedSurrealConnection, AsyncEmbeddedSurrealConnection
from icecream import ic

NS = "__coffeemaker__"
DB = "__statestore__"
ID = "id"
TS = "ts"
STATE = "state"
DATA = "data"

class StateMachine:
    def __init__(self, db_path: str, object_id_keys: dict[str, Optional[str]]):
        self.id_keys = object_id_keys.copy()
        self.db = BlockingEmbeddedSurrealConnection(db_path)
        self.db.connect()
        self.db.use(NS, DB)
        [self.db.query(create_table_expr(table, id_key)) for table, id_key in self.id_keys.items()]

    def close(self):
        self.db.query(create_optimize_expr(self.id_keys))
        self.db.close()

    def set(self, object_type: str, state: str, items: list[dict[str, Any]]|list[BaseModel]):
        id_key = self.id_keys.get(object_type)
        inserted = self.db.query(
            f"INSERT IGNORE INTO {object_type} $data RETURN VALUE id",
            {"data": create_data_to_store(items, state, id_key)},
        )
        return len(inserted)

    def get(self, object_type: str, states: str|list[str], exclude_states: str|list[str] = None, conditions: dict[str, Any]|list[str] = None, columns: list[str] = None, limit: int = 0, offset: int = 0):
        query_expr, params = create_query_expr(object_type, self.id_keys.get(object_type), states, exclude_states, conditions, columns, limit, offset)
        result = self.db.query(query_expr, params)
        return [r[DATA] for r in result]


class AsyncStateMachine:
    def __init__(self, db_path: str, object_id_keys: dict[str, Optional[str]]):
        self.id_keys = object_id_keys.copy()
        self.db = AsyncEmbeddedSurrealConnection(db_path)

    async def __aenter__(self):        
        await self.db.connect()
        await self.db.use(NS, DB)
        await asyncio.gather(*[self.db.query(create_table_expr(table, id_key)) for table, id_key in self.id_keys.items()])

    async def set(self, object_type: str, state: str, items: list[dict[str, Any]]|list[BaseModel]):
        id_key = self.id_keys.get(object_type)
        inserted = await self.db.query(
            f"INSERT IGNORE INTO {object_type} $data RETURN VALUE id",
            {"data": create_data_to_store(items, state, id_key)},
        )
        return len(inserted)

    async def get(self, object_type: str, states: str|list[str], exclude_states: str|list[str] = None, conditions: dict[str, Any]|list[str] = None, columns: list[str] = None, limit: int = 0, offset: int = 0):
        query_expr, params = create_query_expr(object_type, self.id_keys.get(object_type), states, exclude_states, conditions, columns, limit, offset)
        result = await self.db.query(query_expr, params)
        return [r[DATA] for r in result]

    async def __aexit__(self, exc_type, exc, tb):
        await self.db.query(create_optimize_expr(self.id_keys))
        await self.db.__aexit__(exc_type, exc, tb)


def create_table_expr(table: str, id_key: Optional[str]):
    expr = f"""
    DEFINE TABLE IF NOT EXISTS {table} SCHEMAFULL;
    DEFINE FIELD IF NOT EXISTS data ON {table} FLEXIBLE TYPE object;
    DEFINE FIELD IF NOT EXISTS ts ON {table} TYPE datetime;
    DEFINE FIELD IF NOT EXISTS state ON {table} TYPE string;
    DEFINE INDEX IF NOT EXISTS {table}_state_idx ON {table} FIELDS state;    
    """
    if id_key:
        expr += f"""
        DEFINE FIELD IF NOT EXISTS {id_key} ON {table} TYPE string;
        DEFINE INDEX IF NOT EXISTS {table}_{id_key}_idx ON {table} FIELDS {id_key};
        """
    return expr


def create_query_expr(table: str, id_key: str, states: list[str], exclude_states: list[str], conditions: dict|list = None, columns: list[str] = None, limit: int = 0, offset: int = 0):
    expr = f"""SELECT {"*" if not columns else ", ".join(["data."+col for col in columns])} FROM {table}"""
    params = {}

    where_items = []
    
    if isinstance(states, list) or isinstance(exclude_states, list):
        # TODO: group by may be a better way to do this instead of subqueries, need to test performance on larger datasets
        # else:
        #     where_items.extend(f"(SELECT VALUE {id_key} FROM {table} WHERE {id_key} = $parent.{id_key} AND state = '{st}')" for st in states)
        pass
    else:
        if states:
            where_items.append("state = $states")
            params["states"] = states
        if exclude_states:        
            # where_items.append(f"{id_key} NOTINSIDE (SELECT VALUE {id_key} FROM {table} WHERE state IN $exlude_states)")
            where_items.append(f"!(SELECT VALUE 1 FROM {table} WHERE {id_key} = $parent.{id_key} AND state = $exlude_states)")
            params["exlude_states"] = exclude_states

    if conditions:
        if isinstance(conditions, list):
            where_items.extend([f"{DATA}.{cond}" for cond in conditions])
        if isinstance(conditions, dict):
            make_param_key = lambda k: re.sub(r"\W+", "_", k.strip()).lower()
            for k, v in conditions.items():
                param_key = make_param_key(k)
                where_items.append(f"{DATA}.{k} ${param_key}")
                params[param_key] = v

    if where_items:
        expr += "\nWHERE " + " AND ".join(where_items)

    if limit:
        expr += "\nLIMIT $limit"
        params["limit"] = limit
    
    if offset:
        expr += "\nSTART $offset"
        params["offset"] = offset
    
    return ic(expr), params


def create_data_to_store(items: list[dict[str, Any]]|list[BaseModel], state: str, id_key: str):        
    data_items = items
    if isinstance(items[0], BaseModel): 
        data_items = [d.model_dump(exclude_none=True, exclude_unset=True) for d in items]
    ts = datetime.now(tz=timezone.utc)
    return [{ID: f"{state}:{data[id_key]}", TS: ts, STATE: state, id_key: data[id_key], DATA: data} for data in data_items]


def create_optimize_expr(object_id_keys: dict[str, Optional[str]]):
    expr_template = """
    REBUILD INDEX IF EXISTS {table}_state_idx ON {table};
    REBUILD INDEX IF EXISTS {table}_{id_key}_idx ON {table};
    """
    return "\n".join(expr_template.format(table=table, id_key=id_key) for table, id_key in object_id_keys.items() if id_key)

