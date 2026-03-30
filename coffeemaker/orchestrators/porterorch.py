import json
import logging
import os
from typing import Any
from .statemachines import AsyncStateMachine
from .utils import *
from pybeansack import Beansack, create_client
from pybeansack.models import (
    K_BASE_URL,
    K_CATEGORIES,
    K_SENTIMENTS,
    K_URL,
    K_KIND,
    K_SOURCE,
    K_TITLE,
    K_TITLE_LENGTH,
    K_SUMMARY,
    K_SUMMARY_LENGTH,
    K_CONTENT,
    K_CONTENT_LENGTH,
    K_RESTRICTED_CONTENT,
    K_IMAGEURL,
    K_AUTHOR,
    K_CREATED,
    K_COLLECTED,
    K_EMBEDDING,
    K_ENTITIES,    
    K_REGIONS,
    Bean
)
from pybeansack.utils import *
from icecream import ic

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count()))
PORT_CONDITIONS = [
    
]
PORT_COLUMNS = [
    K_URL,
    K_KIND,
    K_SOURCE,
    K_TITLE,
    K_TITLE_LENGTH,
    K_SUMMARY,
    K_SUMMARY_LENGTH,
    K_CONTENT,
    K_CONTENT_LENGTH,
    K_RESTRICTED_CONTENT,
    K_IMAGEURL,
    K_AUTHOR,
    K_CREATED,
    K_COLLECTED,
    K_EMBEDDING,
    K_ENTITIES,    
    K_REGIONS,
]

class Orchestrator:
    db: Beansack    
    states: AsyncStateMachine
    run_total: int = 0

    def __init__(self, cache_kwargs: dict, db_kwargs: dict):
        self.states = AsyncStateMachine(cache_kwargs["statemachine_cache"], object_id_keys={"beans": K_URL, "publishers": K_BASE_URL})
        self.db = create_client(**db_kwargs)

    def _store(self, beans: list[dict]):
        if not beans:
            return
        
        with open(f".test/{int(datetime.now().timestamp())}.json", "w") as f:
            json.dump([Bean(**bean).model_dump(mode="json", exclude_none=True, exclude_unset=True) for bean in beans], f, indent=4)

    async def run(self):
        async with self.states:
            beans = await self.states.get("beans", states=["collected", "embedded", "classified", "extracted"], exclude_states="ported")
            ic(len(beans))
            # extracts = cache.get(
            #     table="extracted_beans", 
            #     notin_tables="ported_beans",
            #     columns=[K_URL, K_ENTITIES, K_REGIONS]
            # )
            # ic(len(extracts))
            # vectors = cache.get(
            #     table="embedded_beans", 
            #     notin_tables="ported_beans", 
            #     in_tables=["extracted_beans"],
            #     columns=[K_URL, K_EMBEDDING]
            # )
            # ic(len(vectors))
            # classifications = cache.get(
            #     table="classified_beans", 
            #     notin_tables="ported_beans", 
            #     in_tables=["extracted_beans"],
            #     columns=[K_URL, K_CATEGORIES, K_SENTIMENTS]
            # )
            # ic(len(classifications))
            # contents = cache.get(
            #     table="collected_beans", 
            #     notin_tables="ported_beans", 
            #     in_tables=["extracted_beans"]
            # )
            # ic(len(contents))
            beans = merge(K_URL, beans)   
            ic(len(beans))         
            self._store(beans)
    
    def close(self):
        self.db.close()


def merge(key, items: list[dict[str, Any]]):
    merged = {}
    for bean in items:
        if bean[key] not in merged:
            merged[bean[key]] = bean
        else:                
            merged[bean[key]].update(bean)    
    return list(merged.values())        
