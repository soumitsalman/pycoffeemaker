from pydantic import BaseModel
from typing import Any
from abc import ABC, abstractmethod
from datetime import datetime, timezone
import msgpack

NOT_IMPLEMENTED = NotImplementedError("Method not implemented")

class StateStoreBase(ABC):
    @abstractmethod
    def set(self, object_type: str, state: str, items: list[dict[str, Any]] | list[BaseModel],):
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def get(self, object_type: str, states: str | list[str], exclude_states: str | list[str], limit: int = 0, offset: int = 0):
        raise NOT_IMPLEMENTED

    @abstractmethod
    def deduplicate(self, object_type: str, state: str, items: list):
        raise NOT_IMPLEMENTED
    
    def close(self):
        pass

class AsyncStateStoreBase(ABC):
    @abstractmethod
    async def set(self, object_type: str, state: str, items: list[dict[str, Any]] | list[BaseModel],):
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    async def get(self, object_type: str, states: str | list[str], exclude_states: str | list[str], limit: int = 0, offset: int = 0):
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

get_id = (
    lambda item, id_key: getattr(item, id_key)
    if isinstance(item, BaseModel)
    else item[id_key]
)
get_ids = lambda items, id_key: [get_id(item, id_key) for item in items]