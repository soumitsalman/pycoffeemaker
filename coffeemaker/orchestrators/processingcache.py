import re
from typing import Optional
import logging
from datetime import datetime, timezone
from surrealdb import Surreal, AsyncSurreal, AsyncEmbeddedSurrealConnection, BlockingEmbeddedSurrealConnection
from .utils import merge_lists
from icecream import ic

PROCESSING_CACHE_DIR = "file://.cache"
CACHE_NS = "default"
CACHE_DB = "default"

ID = "id"
TS = "ts"
DIST_FUNC = "euclidean"

log = logging.getLogger(__name__)

# TODO: setting up some kind of relationship may be helpful
class ProcessingCache:
    db: BlockingEmbeddedSurrealConnection
    db_path: str
    db_name: str
    id_key: Optional[str]
    model_cls: type
    
    def __init__(self, db_path=PROCESSING_CACHE_DIR, db_name=CACHE_DB, id_key=None, model_cls=None):
        self.db_path = db_path
        self.db_name = db_name
        self.id_key = id_key
        self.model_cls = model_cls
        self._db_ctx = None

    def __enter__(self):
        self._db_ctx = Surreal(self.db_path)
        self.db = self._db_ctx.__enter__()
        self.db.use(CACHE_NS, self.db_name)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._db_ctx is not None:
                return self._db_ctx.__exit__(exc_type, exc, tb)
            return False
        finally:
            self.db = None
            self._db_ctx = None

    def store(self, table: str, items: list, columns=None):
        if not items:
            return
        stored = self.db.query(
            f"INSERT IGNORE INTO {table} $data RETURN VALUE id",
            {"data": _create_data(items, columns, self.id_key)},
        )
        log.info("cached", extra={"source": table, "num_items": len(stored)})
        return stored
        
    def get(self, table: str, notin_tables: str|list[str] = None, in_tables: str|list[str] = None, embedding: list[float] = None, distance_func: str = DIST_FUNC, distance: float = None, conditions: dict|list[str] = None, columns: list[str] = None, limit: int = 0, offset: int = 0) -> list:
        expr, params = _to_sql(table, notin_tables, in_tables, embedding, distance_func, distance,conditions, columns, limit, offset)
        vals = self.db.query(expr, params)
        if self.model_cls and vals:
            return [self.model_cls(**v) for v in vals]
        return vals
    
    def stream(self, table: str, notin_tables: str|list[str] = None, in_tables: str|list[str] = None, embedding: list[float] = None, distance_func: str = DIST_FUNC, distance: float = None, conditions: dict|list[str] = None, columns: list[str] = None, batch_size: int = 0):
        offset = 0
        while batch := self.get(table, notin_tables, in_tables, embedding, distance_func, distance, conditions, columns, batch_size, offset=offset):            
            yield batch
            offset += len(batch)

class AsyncProcessingCache:
    db: AsyncEmbeddedSurrealConnection
    db_path: str
    db_name: str
    id_key: Optional[str]
    model_cls: Optional[type]
    
    def __init__(self, db_path=PROCESSING_CACHE_DIR, db_name=CACHE_DB, id_key=None, model_cls=None):
        self.db_path = db_path
        self.db_name = db_name
        self.id_key = id_key
        self.model_cls = model_cls
        self._db_ctx = None

    async def __aenter__(self):
        self._db_ctx = AsyncSurreal(self.db_path)
        self.db = await self._db_ctx.__aenter__()
        await self.db.use(CACHE_NS, self.db_name)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if self._db_ctx is not None:
                return await self._db_ctx.__aexit__(exc_type, exc, tb)
            return False
        finally:
            self.db = None
            self._db_ctx = None

    async def store(self, table: str, items: list, columns=None):
        if not items:
            return
        stored = await self.db.query(
            f"INSERT IGNORE INTO {table} $data RETURN VALUE id",
            {"data": _create_data(items, columns, self.id_key)},
        )
        log.info("cached", extra={"source": table, "num_items": len(stored)})
        return stored
        
    async def get(self, table: str, notin_tables: str|list[str] = None, in_tables: str|list[str] = None, embedding: list[float] = None, distance_func: str = DIST_FUNC, distance: float = None, conditions: dict|list[str] = None, columns: list[str] = None, limit: int = 0, offset: int = 0) -> list:
        expr, params = _to_sql(table, notin_tables, in_tables, embedding, distance_func, distance, conditions, columns, limit, offset)
        vals = await self.db.query(expr, params)
        if self.model_cls and vals:
            return [self.model_cls(**v) for v in vals]
        return vals
    
    async def stream(self, table: str, notin_tables: str|list[str] = None, in_tables: str|list[str] = None, embedding: list[float] = None, distance_func: str = DIST_FUNC, distance: float = None, conditions: dict|list[str] = None, columns: list[str] = None, batch_size: int = 0):
        offset = 0
        while batch := await self.get(table, notin_tables, in_tables, embedding, distance_func, distance, conditions, columns, batch_size, offset=offset):            
            yield batch
            offset += len(batch)

def _create_data(items: list, columns=None, id_key=None):
    current_time = datetime.now(timezone.utc)
    # default treat it as dict
    data = items
    id_func = lambda item: item[id_key]
    # if it's not dict then try to convert using model_dump
    if not isinstance(items[0], dict):
        data = [item.model_dump(exclude_none=True, exclude_unset=True, include=columns) for item in items]
        id_func = lambda item: getattr(item, id_key)
    
    for d, item in zip(data, items):
        d[TS] = current_time
        if id_key: d[ID] = id_func(item)
    return data

def _to_sql(table: str, notin_tables: list[str], in_tables: list[str], embedding: list[float] = None, distance_func: str = DIST_FUNC, distance: float = None, conditions: dict|list = None, columns: list[str] = None, limit: int = 0, offset: int = 0):
    field_clause = "*"
    if columns:
        field_clause = ", ".join(merge_lists([ID, TS], columns))

    expr = f"SELECT {field_clause} FROM {table}"
    params = {}

    where_items = []
        
    if notin_tables:
        if isinstance(notin_tables, str):
            notin_tables = [notin_tables]
        where_items.extend(f"record::id(id) NOTINSIDE (SELECT VALUE record::id(id) FROM {frm})" for frm in notin_tables)

    if in_tables:
        if isinstance(in_tables, str):
            in_tables = [in_tables]
        where_items.extend(f"record::id(id) INSIDE (SELECT VALUE record::id(id) FROM {frm})" for frm in in_tables)

    if embedding:
        # this filters by distance
        if distance:
            where_items.append(f"vector::distance::{distance_func}(embedding, $embedding) <= $distance")
            params.update({"embedding": embedding, "distance": distance})
        # if distance is not given then take knn
        else:
            if not limit: 
                raise ValueError("limit must be provided when distance is not given for embedding search")
            where_items.append(f"embedding <| {limit}, {distance_func.upper()} |> $embedding")
            params["embedding"] = embedding

    if conditions:
        if isinstance(conditions, list):
            where_items.extend(conditions)
        if isinstance(conditions, dict):
            make_param_key = lambda k: re.sub(r"\W+", "_", k.strip()).lower()
            for k, v in conditions.items():
                param_key = make_param_key(k)
                where_items.append(f"{k} ${param_key}")
                params[param_key] = v

    if where_items:
        expr += "\nWHERE " + " AND ".join(where_items)

    if limit:
        expr += "\nLIMIT $limit"
        params["limit"] = limit
    
    if offset:
        expr += "\nSTART $offset"
        params["offset"] = offset
    
    return expr, params