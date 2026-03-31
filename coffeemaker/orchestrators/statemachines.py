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

    def get(self, object_type: str, states: str|list[str], exclude_states: str|list[str] = None, conditions: dict[str, Any]|list[str] = None, limit: int = 0, offset: int = 0, columns: list[str] = None):
        query_expr, params = create_query_expr(object_type, self.id_keys.get(object_type), states, exclude_states, conditions, limit, offset, columns)
        result = self.db.query(query_expr, params)
        return [r[DATA] for r in result]
    
    def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items
        
        expr, params, id_key, id_func = create_deduplicate_query_expr(self.id_keys, object_type, state, items)
        result = self.db.query(expr, params)
        result = set(r[id_key] for r in result)
        # ic("duplicate", len(items)-len(result))
        return [item for item in items if id_func(item) in result]

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

    async def get(self, object_type: str, states: str|list[str], exclude_states: str|list[str] = None, conditions: dict[str, Any]|list[str] = None, limit: int = 0, offset: int = 0, columns: list[str] = None):
        query_expr, params = create_query_expr(object_type, self.id_keys.get(object_type), states, exclude_states, conditions, limit, offset, columns)
        result = await self.db.query(query_expr, params)
        return [r[DATA] for r in result]

    async def deduplicate(self, object_type: str, state: str, items: list):
        if not items:
            return items

        expr, params, id_key, id_func = create_deduplicate_query_expr(self.id_keys, object_type, state, items)
        result = await self.db.query(expr, params)
        result = set(r[id_key] for r in result)
        # ic("duplicate", len(items)-len(result))
        return [item for item in items if id_func(item) in result]

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

def create_deduplicate_query_expr(id_keys: dict, table: str, state: str, items: list):
    id_key = id_keys[table]
    id_func = lambda item: getattr(item, id_key) if isinstance(item, BaseModel) else item[id_key]

    expr = f"""
    SELECT {id_key} FROM $data
    WHERE !(SELECT VALUE 1 FROM {table} WHERE state = $state AND {id_key} = $parent.{id_key})
    """
    params = {
        "data": [{id_key: id_func(d)} for d in items],
        "state": state
    }
    return expr, params, id_key, id_func

def _create_multi_state_query_expr(table: str, id_key: str, states: list[str], exclude_states: list[str], conditions: dict|list = None, columns: list[str] = None):
    """
    SELECT data FROM (
        SELECT url, array::group(state) as states, array::group(data) as data FROM table
        WHERE conditions...
        GROUP BY url
    ) 
    WHERE state CONTAINSALL $states
        AND state CONTAINSNONE $exclude_states
    """
    params = {}
    inner_where_items, outer_where_items = [], []
    if states:
        outer_where_items.append("states CONTAINSALL $states")
        params["states"] = states
    if exclude_states:
        outer_where_items.append("states CONTAINSNONE $exclude_states")
        params["exclude_states"] = exclude_states    
    if conditions:
        if isinstance(conditions, list):
            inner_where_items.extend([f"{DATA}.{cond}" for cond in conditions])
        if isinstance(conditions, dict):
            make_param_key = lambda k: re.sub(r"\W+", "_", k.strip()).lower()
            for k, v in conditions.items():
                param_key = make_param_key(k)
                inner_where_items.append(f"{DATA}.{k} ${param_key}")
                params[param_key] = v

    fields_expr = "data" if not columns else "{" + ",\n".join([f"{col}: data.{col}" for col in columns]) + "}"
    expr = f"""SELECT data FROM (
        SELECT {id_key}, array::group(state) as states, array::group({fields_expr}) as data FROM {table}
        {"WHERE "+ " AND ".join(inner_where_items) if inner_where_items else ""}
        GROUP BY {id_key}
    )"""
    if outer_where_items:
        expr += "\nWHERE " + "\nAND ".join(outer_where_items)

    return expr, params
    
def _create_single_state_query_expr(table: str, id_key: str, states: str, exclude_states: str, conditions: dict|list = None, columns: list[str] = None):
    """
    SELECT data.field1, data.field2 FROM beans
    WHERE state = $states
        AND !(SELECT VALUE 1 FROM beans WHERE url = $parent.url AND state = $exclude_states)
        AND conditions...
    """
    params = {}
    where_items = []
    if states:
        where_items.append("state = $states")
        params["states"] = states if isinstance(states, str) else states[0]
    if exclude_states:
        where_items.append(f"!(SELECT VALUE 1 FROM {table} WHERE {id_key} = $parent.{id_key} AND state = $exlude_states)")
        params["exlude_states"] = exclude_states if isinstance(exclude_states, str) else exclude_states[0]
    if conditions:
        if isinstance(conditions, list):
            where_items.extend([f"{DATA}.{cond}" for cond in conditions])
        if isinstance(conditions, dict):
            make_param_key = lambda k: re.sub(r"\W+", "_", k.strip()).lower()
            for k, v in conditions.items():
                param_key = make_param_key(k)
                where_items.append(f"{DATA}.{k} ${param_key}")
                params[param_key] = v

    expr = f"""SELECT {"*" if not columns else ", ".join(["data."+col for col in columns])} FROM {table}"""
    if where_items:
        expr += "\nWHERE " + "\nAND ".join(where_items)

    return expr, params

def create_query_expr(table: str, id_key: str, states: str|list[str], exclude_states: str|list[str], conditions: dict|list = None, limit: int = 0, offset: int = 0, columns: list[str] = None):
    if (isinstance(states, list) and len(states) > 1) or (isinstance(exclude_states, list) and len(exclude_states) > 1):
        # if there is more than 1 state to include or exclude, we need to use a group by for faster/easier querying
        if isinstance(states, str): states = [states]
        if isinstance(exclude_states, str): exclude_states = [exclude_states]
        expr, params = _create_multi_state_query_expr(table, id_key, states, exclude_states, conditions, columns)
       
    else:
        if isinstance(states, list): states = states[0]
        if isinstance(exclude_states, list): exclude_states = exclude_states[0]
        expr, params = _create_single_state_query_expr(table, id_key, states, exclude_states, conditions, columns)
    
    if limit:
        expr += "\nLIMIT $limit"
        params["limit"] = limit    
    if offset:
        expr += "\nSTART $offset"
        params["offset"] = offset
    
    return expr, params

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

