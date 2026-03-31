import json
import logging
import os
from typing import Any
from .statemachines import AsyncStateMachine, StateMachine
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
    states: StateMachine
    run_total: int = 0

    def __init__(self, cache_kwargs: dict, db_kwargs: dict):
        self.states = StateMachine(cache_kwargs["statemachine_cache"], object_id_keys={"beans": K_URL, "publishers": K_BASE_URL})
        self.db = create_client(**db_kwargs)

    def _store(self, beans: list[dict]):
        if not beans:
            return
        ic(len(beans))               
        with open(f".test/{int(datetime.now().timestamp())}.json", "w") as f:
            json.dump([Bean(**bean).model_dump(mode="json", exclude_none=True, exclude_unset=True) for bean in beans], f, indent=4)

    def run(self):
        beans = self.states.get(
            "beans", 
            states=["collected", "embedded", "classified"], 
            exclude_states="ported", 
            # limit=10,
            # columns=[K_URL, K_EMBEDDING, K_CONTENT, K_REGIONS, K_ENTITIES, K_CATEGORIES, K_SENTIMENTS, "related"]
        )
        beans = merge(K_URL, beans)
        self._store(beans)
    
    def close(self):
        self.db.close()


def merge(key, items: list[dict[str, Any]]):
    merged = {}
    for group in items:
        for data in group:
            if data[key] not in merged:
                merged[data[key]] = data
            else:              
                # NOTE: it is imported to update only the keys for which there is a value  
                merged[data[key]].update({k: v for k, v in data.items() if v})
    return list(merged.values())        
