import json
import logging
import os
import asyncio
from itertools import batched, chain
from typing import Any, Optional

from pandas._testing import ThreadPoolExecutor
from coffeemaker.processingcache.base import AsyncStateCacheBase
from pybeansack import Beansack, Bean, Chatter, Publisher, BEANS, CHATTERS, PUBLISHERS
from pybeansack.models import K_BASE_URL, K_CATEGORIES, K_CONTENT, K_CREATED, K_EMBEDDING, K_RELATED, K_SOURCE, K_TAGS, K_URL
from icecream import ic

from pybeansack.utils import VECTOR_LEN

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

from grafeo import GrafeoDB


EVENTS = "Events"
SIGNALS = "Signals"
REPORTS = "Reports"
SOURCES = "Sources"

distinct = lambda items: list({item.strip().lower():item for item in items}.values())

class Cupboard:
    db_path: str
    db: GrafeoDB

    def __init__(self, db_path: str):
        self.db = GrafeoDB(db_path)

    @classmethod
    def create_db(cls, db_path: str):
        db = GrafeoDB(db_path)
        db.create_property_index(K_URL)
        db.create_property_index(K_SOURCE)
        db.create_property_index(K_CREATED)
        db.create_property_index(K_CATEGORIES)
        db.create_property_index(K_BASE_URL)
        db.create_text_index(EVENTS, K_TAGS)
        db.create_text_index(SIGNALS, K_TAGS)
        db.create_text_index(REPORTS, K_TAGS)
        db.create_vector_index(EVENTS, K_EMBEDDING, VECTOR_LEN, "cosine")
        db.create_vector_index(SIGNALS, K_EMBEDDING, VECTOR_LEN, "cosine")
        db.create_vector_index(REPORTS, K_EMBEDDING, VECTOR_LEN, "cosine")

    def store(self, data_type: str, items: list[dict[str, Any]]):
        if not items: return 0

        res = self.db.batch_create_nodes_with_props(data_type, items)
        
        if data_type == EVENTS:
            expr = """
            FOR evt IN $evts
            MATCH (e:Events {url: evt.url}), (s:Sources {base_url: evt.base_url})
            INSERT (s)-[:PUBLISHED]->(e)
            """
            ic(self.db.execute(expr, {"evts": items}))
        
        if data_type == SOURCES:
            expr = """
            FOR src in $base_urls
            MATCH (s:Sources {base_url: src}), (e:Events {base_url: src}), 
            INSERT (s)-[:PUBLISHED]->(e)
            """            
            ic(self.db.execute(expr, {"base_urls": distinct([item[K_BASE_URL] for item in items])}))

        return len(res)

    def link_events(self, items: list[dict[str, list[str]]]):
        expr =  """
        FOR rel in $related
        MATCH (e1:Events {url: $url}), (e2:Events {url: rel})                
        INSERT (e1)-[:SAME_AS]-(e2)
        """
        packs = [pack for pack in items if pack.get(K_RELATED)]
        from tqdm import tqdm
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
            list(exec.map(lambda pack: self.db.execute(expr, pack), tqdm(packs, total=len(packs), unit="related events")))
        
        

    def query_events(self, embedding: list[float], distance: float = None, limit: int = 0, fields: list[str] = None):
        params = {"embedding": embedding}
        query = "MATCH (e.Events)"

        if distance:
            query += "\nWHERE cosine_distance(e.embedding, $embedding) AS distance <= $distance"
            params["distance"] = distance

        return_expr = "e" if not fields else ", ".join(f"e.{f}" for f in fields)
        query += f"""
        RETURN {return_expr}, cosine_distance(e.embedding, $embedding) AS distance
        ORDER BY distance
        """
        
        if limit:
            query += "\nLIMIT $limit"
            params["limit"] = limit

        return self.db.execute(query, params)

    def close(self):
        self.db.close()


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

        async def hydrate_publishers():
            if publishers := await self.cache.get(
                PUBLISHERS, states="collected", exclude_states="beansacked"
            ):
                log.info("porting", extra={"source": "portable:publishers", "num_items": len(publishers)})
                count = await asyncio.to_thread(db.store_publishers, [Publisher(**pub) for pub in publishers])
                log.info("ported",extra={"source": "beansack:publishers", "num_items": count})
                await self.cache.set(PUBLISHERS, "beansacked", [{K_BASE_URL: p[K_BASE_URL]} for p in publishers])
                return count

        async def hydrate_related():
            if related_beans := await self.cache.get(
                BEANS, states="clustered", exclude_states="related_beansacked",
            ):
                log.info("porting", extra={"source": "portable:related_beans", "num_items": len(related_beans)})
                count = await store(db.store_related, unpack_related(related_beans))
                log.info("ported", extra={"source": "beansack:related_beans", "num_items": count})
                await self.cache.set(BEANS, "related_beansacked", [{K_URL: b[K_URL]} for b in related_beans])
                return count

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

        async def hydrate_trends():
            counts = await asyncio.gather(*(hydrate_related(), hydrate_chatters()))
            await asyncio.to_thread(db.optimize)
            return sum(counts)

        async with self.cache:
            counts = await asyncio.gather(*[hydrate_beans(), hydrate_publishers(), hydrate_trends()])
            total_ported = sum(counts)
            log.info("hydration complete", extra={"source": "beansack", "num_items": total_ported})
        
        return total_ported

    async def hydrate_cupboard(self, db: Cupboard):
        async def hydrate_events():
            # move beans
            TARGET = "cupboarded"
            if beans := await self.cache.get(
                BEANS,
                states=["collected", "embedded", "classified"],
                exclude_states=TARGET
            ):  
                beans = prep_bean_items_for_beansack(beans)
                log.info("porting", extra={"source": "portable:events", "num_items": len(beans)})  
                count = await asyncio.to_thread(db.store, EVENTS, beans)
                log.info("ported", extra={"source": "cupboard:events", "num_items": count})                
                await self.cache.set(BEANS, TARGET, [{K_URL: b[K_URL]} for b in beans])
                return count
            return 0

        async def hydrate_sources():
            TARGET = "cupboarded"
            if sources := await self.cache.get(
                PUBLISHERS, states="collected", exclude_states=TARGET
            ):
                log.info("porting", extra={"source": "portable:sources", "num_items": len(sources)})
                count = await asyncio.to_thread(db.store, SOURCES, sources)
                log.info("ported",extra={"source": "cupboard:sources", "num_items": count})
                await self.cache.set(PUBLISHERS, TARGET, [{K_BASE_URL: p[K_BASE_URL]} for p in sources])
                return count
            return 0

        async def hydrate_related():
            TARGET = "link:cupboarded"
            if related_beans := await self.cache.get(
                BEANS, states="clustered", exclude_states=TARGET, limit=1000
            ):
                log.info("porting", extra={"source": "portable:related_beans", "num_items": len(related_beans)})
                count = await asyncio.to_thread(db.link_events, related_beans)
                log.info("ported", extra={"source": "cupboard:related_beans", "num_items": count})
                await self.cache.set(BEANS, TARGET, [{K_URL: b[K_URL]} for b in related_beans])
                return count

        async with self.cache:
            # await hydrate_events()
            # await asyncio.gather(*(hydrate_related(), hydrate_sources()))
            await hydrate_related()
        
        db.close()
        
