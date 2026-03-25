import json
import logging
import os
from typing import Optional

from .processingcache import ProcessingCache
from .utils import *
from pybeansack import Beansack, create_client
from pybeansack.models import (
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
    run_total: int = 0

    def __init__(self, db_kwargs: dict):
        self.db = create_client(**db_kwargs)

    def _store(self, beans):
        if not beans:
            return
        with open(f".test/{int(datetime.now().timestamp())}.json", "w") as f:
            json.dump(beans, f, indent=4)

    def run(self):
        with ProcessingCache(db_name="beans", id_key=K_URL) as cache:
            
            extracts = cache.get(
                table="extracted_beans", 
                notin_tables="ported_beans",
                columns=[K_URL, K_ENTITIES, K_REGIONS]
            )
            ic(len(extracts))
            vectors = cache.get(
                table="embedded_beans", 
                notin_tables="ported_beans", 
                in_tables=["extracted_beans"],
                columns=[K_URL, K_EMBEDDING]
            )
            ic(len(vectors))
            classifications = cache.get(
                table="classified_beans", 
                notin_tables="ported_beans", 
                in_tables=["extracted_beans"],
                columns=[K_URL, K_CATEGORIES, K_SENTIMENTS]
            )
            ic(len(classifications))
            contents = cache.get(
                table="collected_beans", 
                notin_tables="ported_beans", 
                in_tables=["extracted_beans"]
            )
            ic(len(contents))
            beans = merge(K_URL, extracts, vectors, contents)
            [self._store(bean) for bean in beans]
    
    def close(self):
        self.db.close()


def merge(key, *lists):
    merged = {}
    for bean in (item for items in lists if items for item in items if item and key in item):        
        if bean[key] not in merged:
            merged[bean[key]] = bean
        else:
            merged[bean[key]].update(bean)
    return list(merged.values())        
