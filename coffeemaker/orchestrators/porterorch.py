import json
import logging
import os
import random
from itertools import batched
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional
from coffeemaker.processingcache.base import StateCacheBase
from pybeansack import Beansack, Bean, Publisher, now
from pybeansack.models import K_BASE_URL, K_CONTENT, K_URL
from icecream import ic

log = logging.getLogger("porterworker")

BATCH_SIZE = 2048
MAX_WORKERS = os.cpu_count()*os.cpu_count()

def unpack_related(beans: list[dict]):
    items = {}
    for bean in beans:
        url = bean[K_URL]
        related = bean.get("related")
        if related:
            items.update({f"{url}#{r}": {K_URL: url, "related_url": r} for r in related})
            items.update({f"{r}#{url}": {K_URL: r, "related_url": url} for r in related})
    return list(items.values())
    
def merge(key, items: list[dict[str, Any]]):
    merged = {}
    for data in items:
        # NOTE: it is important to update only the keys for which there is a value
        update = {k: v for k, v in data.items() if v}
        if data[key] not in merged:
            merged[data[key]] = update
        else:
            merged[data[key]].update(update)
    return list(merged.values())

def prep_bean_items_for_beansack(beans: list[dict]):
    """Merges beans, replaces content with cdn url"""
    beans = merge(K_URL, beans)
    # for b in beans:
    #     if not b.get("content_url"): print("--- PORTING ERROR ---", list(b.keys()))
    #     else: b[K_CONTENT] = b["content_url"]
    return beans


class Porter:
    cache: StateCacheBase

    def __init__(self, cache: StateCacheBase):
        self.cache = cache

    def hydrate_beansacks(self, db: Beansack):
        """Ports beans, publishers and related beans to 1 or more Beansacks"""
        total_ported = 0

        # move beans
        if beans := self.cache.get(
            "beans",
            states=["collected", "embedded", "classified", "extracted"],
            exclude_states=["beansacked"],
        ):  
            beans = prep_bean_items_for_beansack(beans)
            log.info("porting", extra={"source": "portable:beans", "num_items": len(beans)})  
            count = db.store_beans([Bean(**b) for b in beans])
            log.info("ported", extra={"source": "beansack:beans", "num_items": count})                
            self.cache.set("beans", "beansacked", [{K_URL: b[K_URL]} for b in beans])
            total_ported += count

        # related beans go to a separate table
        if related_beans := self.cache.get(
            "beans", states="classified", exclude_states="related_beansacked"
        ):
            log.info("porting", extra={"source": "portable:related_beans", "num_items": len(related_beans)})
            count = db.store_related(unpack_related(related_beans))
            log.info("ported", extra={"source": "beansack:related_beans", "num_items": count})
            self.cache.set("beans", "related_beansacked", [{K_URL: b[K_URL]} for b in related_beans])
            total_ported += count

        # move the publishers
        if publishers := self.cache.get(
            "publishers", states="collected", exclude_states="beansacked"
        ):
            log.info("porting", extra={"source": "portable:publishers", "num_items": len(publishers)})
            count = db.store_publishers([Publisher(**pub) for pub in publishers])
            log.info("ported",extra={"source": "beansack:publishers", "num_items": count})
            self.cache.set("publishers", "beansacked", [{K_BASE_URL: p[K_BASE_URL]} for p in publishers])
            total_ported += count
        
        db.optimize()
        log.info("hydration complete", extra={"source": "beansack", "num_items": total_ported})
        return total_ported
