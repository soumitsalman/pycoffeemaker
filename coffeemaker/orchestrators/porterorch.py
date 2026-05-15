import json
import logging
import os
import asyncio
from itertools import batched, chain
from typing import Any, Optional

from coffeemaker.processingcache.base import AsyncStateCacheBase
from pybeansack import Beansack, Bean, Chatter, Publisher, BEANS, CHATTERS, PUBLISHERS
from pybeansack.models import K_BASE_URL, K_CATEGORIES, K_CONTENT, K_CONTENT_LENGTH, K_CREATED, K_EMBEDDING, K_ENTITIES, K_KIND, K_REGIONS, K_RELATED, K_RESTRICTED_CONTENT, K_SENTIMENTS, K_SOURCE, K_SUMMARY, K_SUMMARY_LENGTH, K_TAGS, K_TITLE, K_TITLE_LENGTH, K_URL
from pycupboard.pgcupboard import *
from .utils import *
from icecream import ic

log = logging.getLogger("porterworker")

BATCH_SIZE = int(os.getenv('BATCH_SIZE', 256))
    
def merge(groups: list[dict[str, Any]]):
    merged = []
    for gr in groups:
        pack = {}
        for data in gr:
            pack.update({k: v for k, v in data.items() if v})
        merged.append(pack)
    return merged

class BeansackPorter:
    cache: AsyncStateCacheBase

    def __init__(self, cache: AsyncStateCacheBase):
        self.cache = cache

    @classmethod
    async def store_chunks(cls, store_func, items):
        counts = await asyncio.gather(
            *(
                asyncio.to_thread(store_func, chunk) 
                for chunk in batched(items, BATCH_SIZE)
            )
        )
        return sum(counts)

    @classmethod
    def prep_beans(cls, beans: list[dict]):
        """Merges beans, replaces content with cdn url"""
        beans = merge(beans)
        for bean in beans:
            if entities := bean.get(K_ENTITIES):
                bean.update({
                    K_ENTITIES: merge_lists(
                        entities.people,
                        entities.companies,
                        entities.products,
                        entities.stock_tickers,
                    ),
                    K_REGIONS: entities.regions
                })
        return [Bean(**bean) for bean in merge(beans)]

    @classmethod
    def prep_related(cls, beans: list[dict]):
        items = {}
        for bean in beans:
            url = bean[K_URL]
            if related := bean.get(K_RELATED):
                items.update({f"{url}#{r}": {K_URL: url, "related_url": r} for r in related})
                items.update({f"{r}#{url}": {K_URL: r, "related_url": url} for r in related})
        return list(items.values())

    async def hydrate_beans(self, db: Beansack, target_state: str):
        # move beans
        if beans := await self.cache.get(
            BEANS,
            states=["collected", "embedded", "classified", "extracted"],
            exclude_states=target_state
        ):  
            beans = self.prep_beans(beans)
            log.info("porting", extra={"source": "portable:beans", "num_items": len(beans)})  
            count = await self.store_chunks(db.store_beans, beans)
            log.info("ported", extra={"source": "beansack:beans", "num_items": count})                
            await self.cache.set(BEANS, target_state, [{K_URL: b.url} for b in beans])
            return count
        return 0

    async def hydrate_publishers(self, db: Beansack, target_state: str):
        if publishers := await self.cache.get(PUBLISHERS, states="collected", exclude_states=target_state):
            log.info("porting", extra={"source": "portable:publishers", "num_items": len(publishers)})
            count = await self.store_chunks(db.store_publishers, [Publisher(**pub) for pub in publishers])
            log.info("ported",extra={"source": "beansack:publishers", "num_items": count})
            await self.cache.set(PUBLISHERS, target_state, [{K_BASE_URL: p[K_BASE_URL]} for p in publishers])
            return count
        return 0

    async def hydrate_related(self, db: Beansack, target_state: str):
        target = target_state+":link"
        if related_beans := await self.cache.get(BEANS, states="clustered", exclude_states=target):
            log.info("porting", extra={"source": "portable:related_beans", "num_items": len(related_beans)})
            count = await self.store_chunks(db.store_related, self.prep_related(related_beans))
            log.info("ported", extra={"source": "beansack:related_beans", "num_items": count})
            await self.cache.set(BEANS, target, [{K_URL: b[K_URL]} for b in related_beans])
            return count
        return 0

    async def hydrate_chatters(self, db: Beansack, target_state: str):
        if chatters := await self.cache.get(CHATTERS, states="collected", exclude_states=target_state):
            # save the ids for cache resetting
            ids = [{"id": pkg['id']} for pkg in chatters]
            chatters = list(chain(*(pkg['chatters'] for pkg in chatters)))
            log.info("porting", extra={"source": "portable:chatters", "num_items": len(chatters)})
            count = await asyncio.to_thread(db.store_chatters, [Chatter(**ch) for ch in chatters])
            log.info("ported",extra={"source": "beansack:chatters", "num_items": count})
            await self.cache.set(CHATTERS, target_state, ids)
            return count
        return 0    

    async def hydrate_trends(self, db: Beansack, target_state: str):
        counts = await asyncio.gather(*(
            self.hydrate_related(db, target_state), 
            self.hydrate_chatters(db, target_state)
        ))
        await asyncio.to_thread(db.optimize)
        return sum(counts)

    async def hydrate_beansacks(self, db: Beansack, target_state: str = "beansacked"):
        """Ports beans, publishers and related beans to 1 or more Beansacks"""
        total_ported = 0
        async with self.cache:
            counts = await asyncio.gather(*[
                self.hydrate_beans(db, target_state), 
                self.hydrate_publishers(db, target_state), 
                self.hydrate_trends(db, target_state)
            ])
            total_ported = sum(counts)
            log.info("hydration complete", extra={"source": "beansack", "num_items": total_ported})        
        return total_ported


class CupboardPorter:
    cache: AsyncStateCacheBase

    def __init__(self, cache: AsyncStateCacheBase):
        self.cache = cache

    @classmethod
    def prep_events(cls, beans: list):
        beans = merge(beans)
        return [
            Sip(
                created=bean[K_CREATED],
                kind="event:"+bean[K_KIND],
                embedding=bean[K_EMBEDDING],
                tags=merge_lists(bean[K_CATEGORIES], bean[K_SENTIMENTS], bean.get(DIGEST, {}).get(TAGS, [])),
                digest=bean.get(DIGEST),
                url=bean[K_URL],
                base_url=bean[K_BASE_URL],
            )
            for bean in beans
        ]

    @classmethod
    def prep_sources(cls, sources: list[dict]):
        return [Source(**src, domain_name=src.get(K_SOURCE)) for src in sources]

    # @classmethod
    # def prep_links(cls, links: list):
    #     unpacked = []
    #     for item in links:
    #         url = item[K_URL]
    #         if related := item.get(K_RELATED):
    #             unpacked.extend([{K_URL: url, "related_url": r} for r in related])
    #     return unpacked

    async def hydrate_events(self, db: Cupboard, target_state: str):
        # move beans
        count = 0
        if beans := await self.cache.get(
            BEANS,
            states=["collected", "embedded", "classified", "digested"],
            exclude_states=target_state,
        ):  
            beans = self.prep_events(beans)
            log.info("porting", extra={"source": "portable:events", "num_items": len(beans)})             
            count = await db.store_sips(beans)
            log.info("ported", extra={"source": "cupboard:events", "num_items": count})                
            await self.cache.set(BEANS, target_state, [{K_URL: b.url} for b in beans])            
        return count

    async def hydrate_sources(self, db: Cupboard, target_state: str):
        count = 0
        if sources := await self.cache.get(
            PUBLISHERS, states="collected", exclude_states=target_state
        ):
            sources = self.prep_sources(sources)
            log.info("porting", extra={"source": "portable:sources", "num_items": len(sources)})
            count = await db.store_sources(sources)
            log.info("ported",extra={"source": "cupboard:sources", "num_items": count})
            await self.cache.set(PUBLISHERS, target_state, [{K_BASE_URL: p.base_url} for p in sources])
        return count

    async def hydrate_related(self, db: Cupboard, target_state: str):
        # related bean is pulling from the same well so distinguishing related vs regular
        target = target_state+":link"
        count = 0
        if related_beans := await self.cache.get(
            BEANS, states="clustered", exclude_states=target
        ):
            log.info("porting", extra={"source": "portable:related_beans", "num_items": len(related_beans)})
            count = await db.link_sips(related_beans, "SAME_AS")
            log.info("ported", extra={"source": "cupboard:related_beans", "num_items": count})
            await self.cache.set(BEANS, target, [{K_URL: b[K_URL]} for b in related_beans])
        return count

    async def hydrate_cupboard(self, db: Cupboard, target_state: str = "cupboarded"):
        async with self.cache, db:           
            counts = await asyncio.gather(*(
                self.hydrate_events(db, target_state),
                self.hydrate_sources(db, target_state),
                self.hydrate_related(db, target_state)
            ))
            await db.optimize()
        
        
        
