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

log = logging.getLogger(__name__)

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
        
    def get(self, unprocessed_table: str, processed_table: str, filters: list[str] = None, columns: list[str] = None, limit: int = 0, offset: int = 0) -> list:
        expr, params = _to_sql(unprocessed_table, processed_table, filters, columns, limit, offset)
        vals = self.db.query(expr, params)
        if self.model_cls:
            return [self.model_cls(**v) for v in vals] if vals else []
        return vals
    
    def stream(self, unprocessed_table: str, processed_table: str, filters: list[str] = None, columns: list[str] = None, batch_size: int = 0):
        offset = 0
        while batch := self.get(unprocessed_table, processed_table, filters, columns, limit=batch_size, offset=offset):            
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
        
    async def get(self, unprocessed_table: str, processed_table: str, filters: list[str] = None, columns: list[str] = None, limit: int = 0, offset: int = 0) -> list:
        expr, params = _to_sql(unprocessed_table, processed_table, filters, columns, limit, offset)
        vals = await self.db.query(expr, params)
        if self.model_cls:
            return [self.model_cls(**v) for v in vals] if vals else []
        return vals
    
    async def stream(self, unprocessed_table: str, processed_table: str, filters: list[str], columns: list[str], batch_size: int = 0):
        offset = 0
        while batch := await self.get(unprocessed_table, processed_table, filters, columns, limit=batch_size, offset=offset):            
            yield batch
            offset += len(batch)

def _create_data(items: list, columns=None, id_key=None):
    data = [item.model_dump(exclude_none=True, exclude_unset=True, include=columns) for item in items]
    current_time = datetime.now(timezone.utc)
    for d, item in zip(data, items):
        d[TS] = current_time
        if id_key:
            d[ID] = getattr(item, id_key, None) 
    return data

def _to_sql(unprocessed_table, processed_table, filters, columns, limit, offset):
    params = {}
    if filters:
        added_where_clause = "AND " + " AND ".join(filters)        
    else:
        added_where_clause = ""
    if limit:
        limit_clause = f"LIMIT $limit"
        params["limit"] = limit
    else:
        limit_clause = ""
    if offset:
        offset_clause = f"START $offset"
        params["offset"] = offset
    else:
        offset_clause = ""
    expr = f"""SELECT {", ".join(merge_lists([ID, TS], columns))} FROM {unprocessed_table}
    WHERE record::id(id) NOTINSIDE (SELECT VALUE record::id(id) FROM {processed_table})
        {added_where_clause}
    {limit_clause} {offset_clause}
    """
    return expr, params