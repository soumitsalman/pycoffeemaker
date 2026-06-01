import pandas as pd
from pydantic import BaseModel
from typing import Any, Optional
from abc import ABC, abstractmethod
from datetime import datetime, timezone
import msgpack
from utils.logs import get_logger

NOT_IMPLEMENTED = NotImplementedError("Method not implemented")
NULL_STATE = "__NULL__"

log = get_logger("processingcache")
logset = lambda total: log.info(event="state set", num_items=total, source="__default__")

DEFAULT_WINDOW = 7

class StateCacheBase(ABC):
    @abstractmethod
    def set(self, object_type: str, state: str, items: list[dict[str, Any]]):
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def get(self, object_type: str, states: str | list[str], exclude_states: str | list[str] = NULL_STATE, ids: list[str] = None, window: int = DEFAULT_WINDOW, limit: int = 0, offset: int = 0):
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def deduplicate(self, object_type: str, state: str, items: list):
        raise NOT_IMPLEMENTED
    
    def close(self):
        pass

class AsyncStateCacheBase(ABC):
    @abstractmethod
    async def set(self, object_type: str, state: str, items: list[dict[str, Any]] | list[BaseModel],):
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    async def get(self, object_type: str, states: str | list[str], exclude_states: str | list[str] = NULL_STATE, ids: list[str] = None, window: int = DEFAULT_WINDOW, limit: int = 0, offset: int = 0):
        raise NOT_IMPLEMENTED

    @abstractmethod
    async def deduplicate(self, object_type: str, state: str, items: list):
        raise NOT_IMPLEMENTED
    
    async def close(self):
        pass

def encode_data(data):
    assign_timezone = lambda obj: obj.replace(tzinfo=timezone.utc) if (isinstance(obj, datetime) and not obj.tzinfo) else obj
    return msgpack.packb(data, datetime=True, default=assign_timezone)

def decode_data(data):
    return msgpack.unpackb(data, timestamp=3)

get_field_val = (
    lambda item, id_key: getattr(item, id_key)
    if isinstance(item, BaseModel)
    else item[id_key]
)
get_field_vals = lambda items, id_key: [get_field_val(item, id_key) for item in items]
