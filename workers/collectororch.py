import asyncio
import shutil
from itertools import batched
from utils.logs import get_logger, log_runtime_async
import os
import random
import uuid
import yaml
from datacollectors import (
    APICollectorAsync,
    AsyncWebScraper,
    ARTICLE_LANGUAGE,
    AUTHOR,
    AUTHOR_EMAIL,
    BASE_URL,
    CHATTER_URL,
    COLLECTED,
    COMMENTS,
    CONTENT,
    CREATED,
    DESCRIPTION,
    FAVICON,
    FORUM,
    IMAGEURL,
    KIND,
    LANGUAGE,
    LIKES,
    PLATFORM,
    RESTRICTED_CONTENT,
    RSS_FEED,
    SITE_LANGUAGE,
    SITE_NAME,
    SOURCE,
    SUMMARY,
    TAGS,
    TITLE,
    URL,
    POST
)
from .workercache.base import AsyncStateCacheBase
from .utils import *
from persistqueue import AsyncQueue, Empty
from icecream import ic

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count() * os.cpu_count()))
WORDS_THRESHOLD_FOR_STORING = int(os.getenv("WORDS_THRESHOLD_FOR_STORING", 160))  # min words needed to not download the body

IGNORE_WORD_GAMES = ['hurdle hints', 'nyt strands hints', 'wordle today', 'crossword today', 'crossword hints', 'nyt connections hints', 'spelling bee hints', 'wordle answers']
BEAN_EXCLUDED_FIELDS = {
    ARTICLE_LANGUAGE,
    CHATTER_URL,
    COMMENTS,
    DESCRIPTION,
    FAVICON,
    FORUM,
    LIKES,
    PLATFORM,
    RSS_FEED,
    SITE_LANGUAGE,
    SITE_NAME,
    "subscribers",
}
is_bean_storable = lambda bean: (
    bean
    and bean.get("content_length", 0) >= WORDS_THRESHOLD_FOR_STORING
    and not any(tag in bean.get(TITLE, '').lower() for tag in IGNORE_WORD_GAMES)
)
storable_beans = lambda beans: list(filter(is_bean_storable, beans)) if beans else beans
is_bean_scrapable = lambda bean: (
    bean
    and bean.get(KIND) != POST
    and bean.get("content_length", 0) < WORDS_THRESHOLD_FOR_STORING
    and not any(tag in bean.get(TITLE, '').lower() for tag in IGNORE_WORD_GAMES)
)
scrapable_beans = lambda beans: list(filter(is_bean_scrapable, beans)) if beans else beans

is_publisher_storable = lambda publisher: publisher and any(field in publisher for field in [SITE_NAME, FAVICON, DESCRIPTION])
storable_publishers = lambda publishers: list(filter(is_publisher_storable, publishers)) if publishers else publishers
is_publisher_scrapable = lambda publisher: publisher and not any(field in publisher for field in [SITE_NAME, FAVICON, DESCRIPTION])
scrapable_publishers = lambda publishers: list(filter(is_publisher_scrapable, publishers)) if publishers else publishers


def validate_bean_item(item: dict) -> bool:
    if not item:
        return False
    return bool(
        item.get(TITLE)
        and item.get(COLLECTED)
        and item.get(CREATED)
        and item.get(SOURCE)
        and item.get(KIND)
    )

def validate_chatter_item(item: dict) -> bool:
    if not item:
        return False
    return bool(
        item.get(CHATTER_URL)
        and item.get(URL)
        and (item.get(LIKES) or item.get(COMMENTS) or item.get("subscribers"))
    )

def validate_source_item(item: dict) -> bool:
    if not item:
        return False
    return bool(item.get(SOURCE) and item.get(BASE_URL))

def parse_sources(sources: str) -> dict:
    if os.path.exists(sources):
        with open(sources, 'r') as file:
            data = yaml.safe_load(file)
    else: data = yaml.safe_load(sources)
    return data['sources']

log = get_logger("collectorworker")

_COLLECTOR_CACHE = ".cache/collector"

class Collector:
    cache: AsyncStateCacheBase
    apicollector: APICollectorAsync
    webscraper: AsyncWebScraper
    beans_collected: int
    publishers_collected: int

    def __init__(self, cache: AsyncStateCacheBase, batch_size: int = BATCH_SIZE):
        self.cache = cache
        self.batch_size = batch_size
        self.apicollector = APICollectorAsync(batch_size<<6)
        self.webscraper = AsyncWebScraper(batch_size<<6)
        # self.scraper_queue = AsyncQueue(f"{_COLLECTOR_CACHE}/scraper", chunksize=self.batch_size<<1, tempdir=_COLLECTOR_CACHE)

    def _split_item(self, item: dict):
        if not item:
            return None, None, None

        chatter = {
            CHATTER_URL: item.get(CHATTER_URL),
            URL: item.get(URL),
            SOURCE: item.get(PLATFORM) or item.get(SOURCE),
            FORUM: item.get(FORUM),
            COLLECTED: item.get(COLLECTED),
            LIKES: item.get(LIKES),
            COMMENTS: item.get(COMMENTS),
            "subscribers": item.get("subscribers"),
        }
        [chatter.pop(key, None) for key in list(chatter) if not chatter[key]]

        publisher = {
            SOURCE: item.get(SOURCE),
            BASE_URL: item.get(BASE_URL),
            SITE_NAME: item.get(SITE_NAME),
            DESCRIPTION: item.get(DESCRIPTION),
            FAVICON: item.get(FAVICON),
            RSS_FEED: item.get(RSS_FEED),
            COLLECTED: item.get(COLLECTED),
            LANGUAGE: item.get(SITE_LANGUAGE) or item.get(LANGUAGE),
        }
        [publisher.pop(key, None) for key in list(publisher) if not publisher[key]]
        
        bean = item        
        if language := (item.get(ARTICLE_LANGUAGE) or item.get(LANGUAGE)):
            bean[LANGUAGE] = language
        if is_bean_scrapable(bean):
            bean.pop(CONTENT, None)      
        [bean.pop(key, None) for key in list(bean) if (key in BEAN_EXCLUDED_FIELDS) or (not bean[key])]

        return (
            bean if validate_bean_item(bean) else None,
            chatter if validate_chatter_item(chatter) else None,
            publisher if validate_source_item(publisher) else None,
        )

    def _split_items(self, items: list[dict]):
        beans, chatters, publishers = [], [], []

        for item in items or []:
            bean, chatter, publisher = self._split_item(item)
            if bean:
                beans.append(bean)
            if chatter:
                chatters.append(chatter)
            if publisher:
                publishers.append(publisher)

        return beans, chatters, publishers

    async def _triage(self, items: list[dict]):
        """Store storable collection results and persist the rest for scraping."""
        if not items: return

        beans, chatters, publishers = self._split_items(items)
        del items
        storable_bean_items = storable_beans(beans)
        scrapable_bean_items = scrapable_beans(beans)
        del beans
        storable_publisher_items = storable_publishers(publishers)
        scrapable_publisher_items = scrapable_publishers(publishers)
        del publishers

        async with asyncio.TaskGroup() as tg:
            if chatters:
                tg.create_task(self._cache_chatters(chatters))
            if storable_bean_items:
                tg.create_task(self._cache_beans(storable_bean_items))
            if scrapable_bean_items:
                tg.create_task(self._scrape_beans(scrapable_bean_items))
            if storable_publisher_items:
                tg.create_task(self._cache_publishers(storable_publisher_items))
            if scrapable_publisher_items:
                tg.create_task(self._scrape_publishers(scrapable_publisher_items))

    async def _cache_beans(self, beans: list[dict]):
        if not beans: return

        count = await self.cache.set(BEANS, COLLECTED, beans)
        if count is not None: 
            log.info(event="cached", source=beans[0][SOURCE], beans=count)
            self.beans_collected += count
        else: 
            log.info(event="caching", source=beans[0][SOURCE], beans=len(beans))
        
    async def _cache_publishers(self, publishers: list[dict]):
        if not publishers: return

        count = await self.cache.set(PUBLISHERS, COLLECTED, publishers)
        if count is not None: 
            log.info(event="cached", source=publishers[0][SOURCE], publishers=count)
            self.publishers_collected += count
        else: 
            log.info(event="caching", source=publishers[0][SOURCE], publishers=len(publishers))

    async def _cache_chatters(self, chatters: list[dict]):
        if not chatters: return

        pkg = [{"id": str(uuid.uuid4()), "chatters": chatters}]
        count = await self.cache.set(CHATTERS, COLLECTED, pkg)
        if count is not None: log.info(event="cached", source=chatters[0].get(FORUM, chatters[0][SOURCE]), chatters=count)
        else: log.info(event="caching", source=chatters[0].get(FORUM, chatters[0][SOURCE]), chatters=len(chatters))

    async def _queue_scrape(self, kind: str, items: list[dict]):
        if not items: return
        if items := await self.cache.deduplicate(kind, COLLECTED, items):            
            await self.scraper_queue.put_nowait((kind, items))
        return items

    async def _scrape_beans(self, beans: list[dict]):
        if not beans: return

        beans = await self.cache.deduplicate(BEANS, COLLECTED, beans)
        if not beans: return

        beans = storable_beans(await self.webscraper.scrape_beans(beans))
        if not beans: return

        log.info(event="scraped", source=beans[0][SOURCE], beans=len(beans))
        await self._cache_beans(beans)

    async def _scrape_publishers(self, publishers: list[dict]):        
        if not publishers: return

        publishers = await self.cache.deduplicate(PUBLISHERS, COLLECTED, publishers)
        if not publishers: return

        publishers = storable_publishers(await self.webscraper.scrape_publishers(publishers))
        if not publishers: return

        log.info(event="scraped", source=publishers[0][SOURCE], publishers=len(publishers))
        await self._cache_publishers(publishers)

    def _get_collector_funcs(self, sources):
        # shuffling the sources to introduce randomness in failures
        funcs = []
        for source_type, source_paths in parse_sources(sources).items():
            log.info(event="collecting", source=source_type, num_items=len(source_paths))            
            funcs.extend((source_type, source) for source in source_paths)
        random.shuffle(funcs)
        return funcs

    async def _collect(self, source_type, source):
        to_triage = None
        try:
            if source_type == "ychackernews": to_triage = await self.apicollector.collect_ychackernews(source)
            elif source_type == "reddit": to_triage = await self.apicollector.collect_subreddit(source)
            elif source_type == "rss": to_triage = await self.apicollector.collect_rssfeed(source)
        except Exception as e:
            log.warning(
                event="collection failed",
                source=source,
                error_type=e.__class__.__name__,
                error_details=str(e),
            )
        if to_triage: await self._triage(to_triage)

    async def _run_collectors(self, sources):
        """Run the collectors"""
        collector_funcs = self._get_collector_funcs(sources)

        async def work(offset: int):
            for func in collector_funcs[offset::self.batch_size]:
                await self._collect(*func)

        await asyncio.gather(*(work(offset) for offset in range(self.batch_size)), return_exceptions=True)        
        log.info(event="collectors completed")

        await self.scraper_queue.put_nowait(None)

    # async def _run_scraper(self):
    #     """Run the scrapers - flushing the buffers when full or worker is done."""      
    #     BUFFER_SIZE = self.batch_size<<1
    #     beans_buffer, publishers_buffer = [], []        
    #     while items := await self.scraper_queue.get():
    #         kind, items = items
    #         if kind == BEANS: beans_buffer.extend(items)
    #         elif kind == PUBLISHERS: publishers_buffer.extend(items)

    #         if len(beans_buffer) >= BUFFER_SIZE:
    #             await self._scrape_beans(beans_buffer)
    #             beans_buffer = []
    #         if len(publishers_buffer) >= BUFFER_SIZE:
    #             await self._scrape_publishers(publishers_buffer)
    #             publishers_buffer = []

    #     await asyncio.gather(
    #         self._scrape_beans(beans_buffer), 
    #         self._scrape_publishers(publishers_buffer)
    #     )

    def _init_run(self):
        self.beans_collected = 0
        self.publishers_collected = 0   

    @log_runtime_async(logger=log)
    async def run(self, sources):
        """Main entry point for collector orchestrator. Runs the complete bean collection pipeline and refreshes chatter data."""
        log.info(event="starting collectors")

        self._init_run()
        async with self.cache, self.apicollector, self.webscraper:
            await asyncio.gather(
                self._run_collectors(sources),
                # self._run_scraper(),
                return_exceptions=True
            )
        # await self.scraper_queue.close()
        # shutil.rmtree(_COLLECTOR_CACHE, ignore_errors=True)
        log.info(event="total collected", beans=self.beans_collected, publishers=self.publishers_collected)

async def _get_many(queue: AsyncQueue, batch_size: int):
    items = []
    while len(items) < batch_size and not (await queue.empty()):
        try: items.append(await queue.get_nowait())
        except Empty: break
    return items
