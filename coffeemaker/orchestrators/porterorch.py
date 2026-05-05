import json
import logging
import os
import asyncio
from itertools import batched, chain
from typing import Any, Optional
from coffeemaker.processingcache.base import AsyncStateCacheBase
from pybeansack import Beansack, Bean, Chatter, Publisher, BEANS, CHATTERS, PUBLISHERS
from pybeansack.models import K_BASE_URL, K_CONTENT, K_URL
from icecream import ic

log = logging.getLogger("porterworker")

MAX_ITEMS = 256
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
    return merge(K_URL, beans)

class Porter:
    cache: AsyncStateCacheBase

    def __init__(self, cache: AsyncStateCacheBase):
        self.cache = cache

    async def hydrate_beansacks(self, db: Beansack):
        """Ports beans, publishers and related beans to 1 or more Beansacks"""
        total_ported = 0

        async def store(func, items):
            counts = await asyncio.gather(
                *(
                    asyncio.to_thread(func, chunk) 
                    for chunk in batched(items, MAX_ITEMS)
                )
            )
            return sum(counts)

        async def hydrate_beans():
            # move beans
            if beans := await self.cache.get(
                BEANS,
                states=["collected", "embedded", "classified", "extracted"],
                exclude_states=["beansacked"]
            ):  
                beans = prep_bean_items_for_beansack(beans)
                log.info("porting", extra={"source": "portable:beans", "num_items": len(beans)})  
                count = await store(db.store_beans, [Bean(**b) for b in beans])
                log.info("ported", extra={"source": "beansack:beans", "num_items": count})                
                await self.cache.set(BEANS, "beansacked", [{K_URL: b[K_URL]} for b in beans])
                return count
            return 0

        async def hydrate_publishers():
            if publishers := await self.cache.get(
                PUBLISHERS, states="collected", exclude_states="beansacked"
            ):
                log.info("porting", extra={"source": "portable:publishers", "num_items": len(publishers)})
                count = await asyncio.to_thread(db.store_publishers, [Publisher(**pub) for pub in publishers])
                log.info("ported",extra={"source": "beansack:publishers", "num_items": count})
                await self.cache.set(PUBLISHERS, "beansacked", [{K_BASE_URL: p[K_BASE_URL]} for p in publishers])
                return count
            return 0

        async def hydrate_related():
            if related_beans := await self.cache.get(
                BEANS, states="clustered", exclude_states="related_beansacked",
            ):
                log.info("porting", extra={"source": "portable:related_beans", "num_items": len(related_beans)})
                count = await store(db.store_related, unpack_related(related_beans))
                log.info("ported", extra={"source": "beansack:related_beans", "num_items": count})
                await self.cache.set(BEANS, "related_beansacked", [{K_URL: b[K_URL]} for b in related_beans])
                return count
            return 0

        async def hydrate_chatters():
            if chatters := await self.cache.get(
                CHATTERS, states="collected", exclude_states="beansacked"
            ):
                # save the ids for cache resetting
                ids = [{"id": pkg['id']} for pkg in chatters]
                chatters = list(chain(*(pkg['chatters'] for pkg in chatters)))
                log.info("porting", extra={"source": "portable:chatters", "num_items": len(chatters)})
                count = await asyncio.to_thread(db.store_chatters, [Chatter(**ch) for ch in chatters])
                log.info("ported",extra={"source": "beansack:chatters", "num_items": count})
                await self.cache.set(CHATTERS, "beansacked", ids)
                return count
            return 0

        async def hydrate_trends():
            counts = await asyncio.gather(*(hydrate_related(), hydrate_chatters()))
            await asyncio.to_thread(db.optimize)
            return sum(counts)

        async with self.cache:
            counts = await asyncio.gather(*[hydrate_beans(), hydrate_publishers(), hydrate_trends()])
            total_ported = sum(counts)
            log.info("hydration complete", extra={"source": "beansack", "num_items": total_ported})
        
        return total_ported
