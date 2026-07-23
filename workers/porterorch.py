import random
from utils.logs import get_logger
from utils.fields import *
import os
import asyncio
from itertools import chain
from typing import Any

from processingcache import AsyncStateCacheBase
from pybeansack import Beansack, Bean, Chatter, Publisher, BEANS, CHATTERS, PUBLISHERS
from pybeansack.models import (
    BASE_URL, CATEGORIES, CONTENT, CONTENT_LENGTH, CREATED, EMBEDDING, ENTITIES,
    KIND, REGIONS, RELATED, RESTRICTED_CONTENT, SENTIMENTS, SOURCE, SUMMARY,
    SUMMARY_LENGTH, TAGS, TITLE, TITLE_LENGTH, URL,
)
from pycupboard.pgcupboard import Cupboard
from pycupboard.models import Sip, Source, DEFAULT_SOURCE
from utils import generate_uuid
from .states import *
from nlp import merge_lists, normalize_tags
from icecream import ic

log = get_logger("porterworker")

BATCH_SIZE = int(os.getenv('BATCH_SIZE', 512))

class BeansackPorter:
    cache: AsyncStateCacheBase

    def __init__(self, cache: AsyncStateCacheBase):
        self.cache = cache
    
    @classmethod
    def prep_beans(cls, beans: list[dict]):
        """Merges beans, replaces content with cdn url"""
        for bean in beans:
            if entity_pack := bean.pop(ENTITIES, None):                
                if entities := merge_lists(
                    entity_pack.get(PEOPLE), 
                    entity_pack.get(COMPANIES), 
                    entity_pack.get(PRODUCTS), 
                    entity_pack.get(STOCK_TICKERS)
                ):
                    bean[ENTITIES] = entities
                if regions := entity_pack.get(REGIONS):
                    bean[REGIONS] = regions
        return [Bean(**bean) for bean in beans]

    async def hydrate_beans(self, db: Beansack, target_state: str):
        if beans := await self.cache.get(
            BEANS,
            states=[COLLECTED, EMBEDDED, EXTRACTED],
            exclude_states=target_state,
        ):  
            log.info(event="porting:beans", to="beansack", num_items=len(beans))  
            count = await asyncio.to_thread(db.store_beans, self.prep_beans(beans))
            log.info(event="ported:beans", to="beansack", num_items=count)                
            await self.cache.set(BEANS, target_state, [{URL: b[URL]} for b in beans])
            return count
        return 0

    async def hydrate_publishers(self, db: Beansack, target_state: str):
        if publishers := await self.cache.get(PUBLISHERS, states=COLLECTED, exclude_states=target_state):
            log.info(event="porting:publishers", to="beansack", num_items=len(publishers))
            count = await asyncio.to_thread(db.store_publishers, [Publisher(**pub) for pub in publishers])
            log.info(event="ported:publishers", to="beansack", num_items=count)
            await self.cache.set(PUBLISHERS, target_state, [{BASE_URL: p[BASE_URL]} for p in publishers])
            return count
        return 0

    @classmethod
    def prep_related(cls, beans: list[dict]):
        items = {}
        for bean in beans:
            url = bean[URL]
            if related := bean.get(RELATED):
                items.update({f"{url}#{r}": {URL: url, "related_url": r} for r in related})
                items.update({f"{r}#{url}": {URL: r, "related_url": url} for r in related})
        return list(items.values())

    async def hydrate_related(self, db: Beansack, target_state: str):
        target = target_state+":link"
        if related_beans := await self.cache.get(BEANS, states=CLUSTERED, exclude_states=target):
            log.info(event="porting:bean_links", to="beansack", num_items=len(related_beans))
            count = await asyncio.to_thread(db.store_related, self.prep_related(related_beans))
            log.info(event="ported:bean_links", to="beansack", num_items=count)
            await self.cache.set(BEANS, target, [{URL: b[URL]} for b in related_beans])
            return count
        return 0

    async def hydrate_chatters(self, db: Beansack, target_state: str):
        if chatters := await self.cache.get(CHATTERS, states=COLLECTED, exclude_states=target_state):
            # save the ids for cache resetting
            ids = [{"id": pkg['id']} for pkg in chatters]
            chatters = list(chain(*(pkg['chatters'] for pkg in chatters)))
            log.info(event="porting:chatters", to="beansack", num_items=len(chatters))
            count = await asyncio.to_thread(db.store_chatters, [Chatter(**ch) for ch in chatters])
            log.info(event="ported:chatters", to="beansack", num_items=count)
            await self.cache.set(CHATTERS, target_state, ids)
            return count
        return 0    

    async def run(self, db: Beansack, target_state: str = BEANSACKED):
        """Ports beans, publishers and related beans to 1 or more Beansacks"""
        async def hydrate_trends():
            counts = await asyncio.gather(*(
                self.hydrate_related(db, target_state), 
                self.hydrate_chatters(db, target_state)
            ))
            if sum(counts): await asyncio.to_thread(db.optimize)
            return sum(counts)

        counts = await asyncio.gather(*[
            self.hydrate_beans(db, target_state),
            self.hydrate_publishers(db, target_state),
            hydrate_trends(),
        ])
        total_ported = sum(counts)
        log.info(event=f"total {target_state}", num_items=total_ported)
        return total_ported


CUPBOARD_EVENT_KIND = "event"
CUPBOARD_SIGNAL_KIND = "signal"
CUPBOARD_SIGNAL_URL_PREFIX = "https://api.cafecito.tech/espresso/signals/"
MAX_TAGS = 50
class CupboardPorter:
    cache: AsyncStateCacheBase

    def __init__(self, cache: AsyncStateCacheBase):
        self.cache = cache

    @classmethod
    def prep_events(cls, beans: list[dict[str, Any]]):
        beans = [bean for bean in beans if bean.get(DIGEST)]
        for bean in beans:            
            bean.pop(SOURCE)
            bean[KIND] = CUPBOARD_EVENT_KIND     
            entity_tags = []       
            if entity_pack := bean.get(ENTITIES):
                bean[DIGEST] |= entity_pack
                entity_tags = merge_lists(
                    entity_pack.get(PEOPLE), 
                    entity_pack.get(COMPANIES), 
                    entity_pack.get(PRODUCTS), 
                    entity_pack.get(STOCK_TICKERS),
                    entity_pack.get(REGIONS),
                )
            bean[TAGS] = merge_lists(
                normalize_tags(bean.get(TAGS)),
                bean.get(CATEGORIES),
                entity_tags,
                bean[DIGEST].get("macro_context")
            )
        return [Sip(**bean) for bean in beans]

    async def hydrate_events(self, db: Cupboard, target_state: str):
        if beans := await self.cache.get(
            BEANS,
            states=[COLLECTED, EMBEDDED, EXTRACTED, DIGESTED],
            exclude_states=target_state,
        ):  
            log.info(event="porting:events", to="cupboard", num_items=len(beans))             
            count = await db.store_sips(self.prep_events(beans))
            log.info(event="ported:events", to="cupboard", num_items=count)                
            await self.cache.set(BEANS, target_state, [{URL: b[URL]} for b in beans])       
            return count
        return 0

    @classmethod
    def prep_sources(cls, sources: list[dict]):
        return [Source(**src, domain_name=src.get(SOURCE)) for src in sources]

    async def hydrate_sources(self, db: Cupboard, target_state: str):
        count = 0
        if sources := await self.cache.get(PUBLISHERS, states=COLLECTED, exclude_states=target_state):
            sources = self.prep_sources(sources)
            log.info(event="porting:sources", to="cupboard", num_items=len(sources))
            count = await db.store_sources(sources)
            log.info(event="ported:sources", to="cupboard", num_items=count)
            await self.cache.set(PUBLISHERS, target_state, [{BASE_URL: p.base_url} for p in sources])
        return count

    async def hydrate_related(self, db: Cupboard, target_state: str):
        # related bean is pulling from the same well so distinguishing related vs regular
        target = target_state+":link"
        if related_beans := await self.cache.get(BEANS, states=CLUSTERED, exclude_states=target):
            log.info(event="porting:event_links", to="cupboard", num_items=len(related_beans))
            count = await db.link_sips(related_beans, "SAME_AS")
            log.info(event="ported:event_links", to="cupboard", num_items=count)
            await self.cache.set(BEANS, target, [{URL: b[URL]} for b in related_beans])
            return count
        return 0

    @classmethod
    def prep_signals(cls, composites: list[dict]):
        """Adding fields for easier conversion for storage."""
        for comp in composites:
            # this is just for convenient storage and linking ops
            url = CUPBOARD_SIGNAL_URL_PREFIX+comp[ID]
            comp.update({
                SOURCE: DEFAULT_SOURCE,
                KIND: CUPBOARD_SIGNAL_KIND,
                ID: generate_uuid(url),
                URL: url,                
            })  
            if tags := comp.get(TAGS):
                comp[TAGS] = random.sample(tags, min(len(tags), MAX_TAGS))
        return composites              

    async def hydrate_signals(self, db: Cupboard, target_state: str):
        count = 0
        if composites := await self.cache.get(COMPOSITES, states=COLLECTED, exclude_states=target_state):
            log.info(event="porting:signals", to="cupboard", num_items=len(composites))
            # saving this for cache resetting
            ids = [{ID: comp[ID]} for comp in composites]
            composites = self.prep_signals(composites)
            counts = await asyncio.gather(*[
                db.store_sips([Sip(**comp) for comp in composites]),
                db.link_sips(composites, "DERIVED_FROM"),
            ])
            log.info(event="ported:signals", to="cupboard", num_items=counts[0], links=counts[1])
            await self.cache.set(COMPOSITES, target_state, ids)
            count = sum(counts)
        return count

    async def run(self, db: Cupboard, target_state: str = CUPBOARDED):
        async with db:
            counts = await asyncio.gather(
                self.hydrate_events(db, target_state),
                self.hydrate_sources(db, target_state),
                self.hydrate_related(db, target_state),
                self.hydrate_signals(db, target_state),
            )
            await db.optimize()
            total_ported = sum(counts)
            log.info(event=f"total {target_state}", num_items=total_ported)
            return total_ported
        
        
        
