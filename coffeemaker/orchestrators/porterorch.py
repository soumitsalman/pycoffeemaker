import json
import logging
import os
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional
from coffeemaker.processingcache.base import StateStoreBase
from pybeansack import Beansack, Bean, Publisher, now
from pybeansack.models import K_BASE_URL, K_CONTENT, K_URL
from icecream import ic

log = logging.getLogger(__name__)

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
    for b in beans:
        b[K_CONTENT] = b["content_url"]
    with open(f".cache/{now().strftime('%Y%m%d-%H%M%S')}.json", "w") as f:
        json.dump(random.sample(beans, min(5, len(beans))), f, indent=2, default=str)
    return beans


class Porter:
    state_store: StateStoreBase

    def __init__(self, state_store: StateStoreBase):
        self.state_store = state_store

    def hydrate_beansacks(self, dbs: list[Beansack]):
        """Ports beans, publishers and related beans to 1 or more Beansacks"""
        # TODO: increase this to collected, embedded, classified, extracted, digested, cdned
        beans = self.state_store.get(
            "beans",
            states=["collected", "embedded", "classified", "extracted", "cdned", "digested"],
            exclude_states="beansacked",
        )
        beans = prep_bean_items_for_beansack(beans)
        related_beans = unpack_related(beans)
        publishers = self.state_store.get(
            "publishers", states="collected", exclude_states="beansacked"
        )
        with ThreadPoolExecutor() as exec:
            if beans:
                beans_stored = exec.map(lambda db: db.store_beans([Bean(**b) for b in beans]), dbs)
            if related_beans:
                rel_beans_stored = exec.map(lambda db: db.store_related(related_beans), dbs)
            if publishers:                
                pubs_stored = exec.map(lambda db: db.store_publishers([Publisher(**pub) for pub in publishers]), dbs)
            
            if beans:
                log.info(
                    "ported",
                    extra={"source": "beansack:beans", "num_items": sum(list(beans_stored))},
                )                
                self.state_store.set("beans", "beansacked", [{K_URL: b[K_URL]} for b in beans])
            if related_beans:
                log.info(
                    "ported",
                    extra={"source": "beansack:related_beans", "num_items": sum(list(rel_beans_stored))},
                )
            if publishers:
                log.info(
                    "ported",
                    extra={"source": "beansack:publishers", "num_items": sum(list(pubs_stored))},
                )
                self.state_store.set("publishers", "beansacked", [{K_BASE_URL: p[K_BASE_URL]} for p in publishers])
            
            exec.map(lambda db: db.optimize(), dbs)
        
        log.info(
            "hydration complete", 
            extra={"source": "beansack", "num_items": len(dbs)}
        )
