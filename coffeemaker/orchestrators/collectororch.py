import asyncio
from itertools import batched
import logging
import os
import random
from datetime import datetime
from ..collectors import APICollectorAsync, WebScraperLite, PublisherScraper, parse_sources
from .statemachines import AsyncStateMachine
from .utils import *
from pybeansack import Beansack, create_client
from pybeansack.models import *
from icecream import ic

BATCH_SIZE = int(
    os.getenv("BATCH_SIZE", os.cpu_count() * os.cpu_count())
)
WORDS_THRESHOLD_FOR_STORING = int(
    os.getenv("WORDS_THRESHOLD_FOR_STORING", 160)
)  # min words needed to not download the body

is_bean_storable = lambda bean: bean and bean.get("content") and count_words(bean.get("content")) >= WORDS_THRESHOLD_FOR_STORING
scrapable_beans = lambda beans: [bean for bean in beans if not is_bean_storable(bean)] if beans else beans
storable_beans = lambda beans: list(filter(is_bean_storable, beans)) if beans else beans
is_publisher_storable = lambda publisher: publisher and (publisher.get("site_name") or publisher.get("rss_feed") or publisher.get("description"))
storable_publishers = lambda publishers: list(filter(is_publisher_storable, publishers)) if publishers else publishers

log = logging.getLogger(__name__)

class Orchestrator:
    cache_kwargs: dict
    db: Beansack
    states: AsyncStateMachine
    apicollector: APICollectorAsync
    webscraper: WebScraperLite
    pubscraper: PublisherScraper
    run_id: str
    beans_collected: int
    publishers_collected: int

    def __init__(self, cache_kwargs: dict, db_kwargs: dict):
        self.db = create_client(**db_kwargs)        
        self.states = AsyncStateMachine(cache_kwargs["statemachine_cache"], object_id_keys={"beans": K_URL, "publishers": K_BASE_URL})

    async def _collect(self, collect_func, *args, **kwargs):
        try:
            await self._triage(
                await collect_func(*args, **kwargs)
            )
        except Exception as e:
            log.warning(f"{collect_func.__name__}{args} failed", extra={"source": e, "num_items": 1})

    async def _triage(self, items: list[dict]):
        """Store storable collection results and persist the rest for scraping."""
        if not items:
            return

        beans = [item["bean"] for item in items if item and item.get("bean")]
        publishers = [item["publisher"] for item in items if item and item.get("publisher")]
        chatters = [Chatter(**item["chatter"]) for item in items if item and item.get("chatter")]

        async with asyncio.TaskGroup() as tg:
            if chatters:
                tg.create_task(asyncio.to_thread(self.db.store_chatters, chatters))
            if beans:
                to_store = storable_beans(beans)
                to_scrape = scrapable_beans(beans)
                if to_store:
                    tg.create_task(self._cache_beans(to_store))
                if to_scrape:
                    tg.create_task(self._scrape_beans(to_scrape))
            if publishers:
                to_store = storable_publishers(publishers)
                to_scrape = [pub for pub in publishers if not is_publisher_storable(pub)]
                if to_store:
                    tg.create_task(self._cache_publishers(to_store))
                if to_scrape:
                    tg.create_task(self._cache_publishers(to_scrape))


    async def _cache_beans(self, beans: list[dict]):
        count = await self.states.set("beans", "collected", beans)
        self.beans_collected += count
        # source is a heuristic
        if count: log.info("collected beans", extra={"source": beans[0]["source"], "num_items": count})
        return count 

    async def _cache_publishers(self, publishers: list[dict]):                
        count = await self.states.set("publishers", "collected", publishers)
        self.publishers_collected += count
        # source is a heuristic
        if count: log.info("collected publishers", extra={"source": publishers[0]["source"], "num_items": count})
        return count

    async def _scrape_beans(self, beans: list[dict]):
        to_scrape = await self.states.deduplicate("beans", "collected", beans)
        if to_scrape:
            await self._triage(
                await self.webscraper.scrape_beans(to_scrape)
            )

    async def _scrape_publishers(self, publishers: list[dict]):
        to_scrape = await self.states.deduplicate("publishers", "collected", publishers)
        if to_scrape:
            await self._triage(
                ic(await self.webscraper.scrape_publishers(to_scrape))
            )

    def _create_collection_funcs(self, sources):
        funcs = []
        for source_type, source_paths in parse_sources(sources).items():
            log.info("collecting", extra={"source": source_type, "num_items": len(source_paths)})
            if source_type == "ychackernews":
                func = self.apicollector.collect_ychackernews
            elif source_type == "reddit":
                func = self.apicollector.collect_subreddit
            elif source_type == "rss":
                func = self.apicollector.collect_rssfeed
            else:
                continue
            funcs.extend((func, source) for source in source_paths)
        random.shuffle(funcs)
        return funcs

    @log_runtime_async(logger=log)
    async def run(self, sources, batch_size: int = BATCH_SIZE):
        """Main entry point for collector orchestrator. Runs the complete bean collection pipeline and refreshes chatter data."""

        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.beans_collected, self.publishers_collected = 0, 0       

        log.info("starting collectors", extra={"source": self.run_id, "num_items": batch_size})
        async with self.states, \
            APICollectorAsync(batch_size) as self.apicollector, \
            WebScraperLite(batch_size) as self.webscraper, \
            PublisherScraper(batch_size) as self.pubscraper:
            await asyncio.gather(*(self._collect(func, source) for func, source in self._create_collection_funcs(sources)))
        
        log.info("total beans collected", extra={"source": self.run_id, "num_items": self.beans_collected})
        log.info("total publishers collected", extra={"source": self.run_id, "num_items": self.publishers_collected})
        self.db.refresh_chatters()
        

    def close(self):
        # Close database connection
        self.db.close()


