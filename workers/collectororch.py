import asyncio
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
from asyncio import Queue
from icecream import ic

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count() * os.cpu_count()))
WORDS_THRESHOLD_FOR_STORING = int(os.getenv("WORDS_THRESHOLD_FOR_STORING", 160))  # min words needed to not download the body

IGNORE_WORD_GAMES = ['hurdle hints', 'nyt strands hints', 'wordle today', 'crossword today', 'crossword hints', 'nyt connections hints', 'spelling bee hints', 'wordle answers']
is_bean_storable = (
    lambda bean: bean
    and bean.get("content_length", 0) >= WORDS_THRESHOLD_FOR_STORING
    and not any(tag in bean.get(TITLE, '').lower() for tag in IGNORE_WORD_GAMES)
)
scrapable_beans = (
    lambda beans: [bean for bean in beans if not is_bean_storable(bean) and bean[KIND] != POST]
    if beans
    else beans
)
storable_beans = lambda beans: list(filter(is_bean_storable, beans)) if beans else beans

is_publisher_storable = lambda publisher: publisher and any(field in publisher for field in [SITE_NAME, FAVICON, DESCRIPTION])
scrapable_publishers = lambda publishers: [publisher for publisher in publishers if not is_publisher_storable(publisher)] if publishers else publishers
storable_publishers = lambda publishers: list(filter(is_publisher_storable, publishers)) if publishers else publishers


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

class Collector:
    cache: AsyncStateCacheBase
    apicollector: APICollectorAsync
    webscraper: AsyncWebScraper
    beans_collected: int
    publishers_collected: int

    def __init__(self, cache: AsyncStateCacheBase):
        self.cache = cache        

    def _split_item(self, item: dict):
        if not item:
            return None, None, None

        bean = {
            URL: item.get(URL),
            KIND: item.get(KIND),
            SOURCE: item.get(SOURCE),
            TITLE: item.get(TITLE),
            SUMMARY: item.get(SUMMARY),
            CONTENT: item.get(CONTENT),
            AUTHOR: item.get(AUTHOR),
            CREATED: item.get(CREATED),
            COLLECTED: item.get(COLLECTED),
            BASE_URL: item.get(BASE_URL),
            IMAGEURL: item.get(IMAGEURL),
            RESTRICTED_CONTENT: item.get(RESTRICTED_CONTENT),
            TAGS: item.get(TAGS),
            AUTHOR_EMAIL: item.get(AUTHOR_EMAIL),
            LANGUAGE: item.get(ARTICLE_LANGUAGE) or item.get(LANGUAGE),
            "title_length": item.get("title_length"),
            "summary_length": item.get("summary_length"),
            "content_length": item.get("content_length"),
        }
        bean = {key: value for key, value in bean.items() if value}

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
        chatter = {key: value for key, value in chatter.items() if value}

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
        publisher = {key: value for key, value in publisher.items() if value}

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

        async with asyncio.TaskGroup() as tg:
            if chatters:
                tg.create_task(self._cache_chatters(chatters))
            if beans:                
                tg.create_task(self._cache_beans(storable_beans(beans)))
                tg.create_task(self._scrape_beans(scrapable_beans(beans)))
            if publishers:
                tg.create_task(self._cache_publishers(storable_publishers(publishers)))
                tg.create_task(self._scrape_publishers(scrapable_publishers(publishers)))

    async def _cache_beans(self, beans: list[dict]):
        if not beans: return

        count = await self.cache.set(BEANS, COLLECTED, beans)
        if count is not None: 
            log.info(event="cached beans", source=beans[0][SOURCE], num_items=count)
            self.beans_collected += count
        else: 
            log.info(event="caching beans", source=beans[0][SOURCE], num_items=len(beans))
        
    async def _cache_publishers(self, publishers: list[dict]):
        if not publishers: return

        count = await self.cache.set(PUBLISHERS, COLLECTED, publishers)
        if count is not None: 
            log.info(event="cached publishers", source=publishers[0][SOURCE], num_items=count)
            self.publishers_collected += count
        else: 
            log.info(event="caching publishers", source=publishers[0][SOURCE], num_items=len(publishers))

    async def _cache_chatters(self, chatters: list[dict]):
        if not chatters: return

        pkg = [{"id": str(uuid.uuid4()), "chatters": chatters}]
        count = await self.cache.set(CHATTERS, COLLECTED, pkg)
        if count is not None: log.info(event="cached chatters", source=chatters[0][SOURCE], num_items=count)
        else: log.info(event="caching chatters", source=chatters[0][SOURCE], num_items=len(chatters))

    async def _scrape_beans(self, beans: list[dict]):
        if not beans: return

        beans = await self.cache.deduplicate(BEANS, COLLECTED, beans)
        beans = await self.webscraper.scrape_beans(beans)
        if beans := storable_beans(beans): 
            log.info(event="scraped beans", source=beans[0][SOURCE], num_items=len(beans))
            await self._cache_beans(beans)

    async def _scrape_publishers(self, publishers: list[dict]):        
        if not publishers: return

        publishers = await self.cache.deduplicate(PUBLISHERS, COLLECTED, publishers)
        publishers = await self.webscraper.scrape_publishers(publishers)
        if publishers := storable_publishers(publishers): 
            log.info(event="scraped publishers", source=publishers[0][SOURCE], num_items=len(publishers))
            await self._cache_publishers(publishers)

    def _get_collector_funcs(self, sources):
        # shuffling the sources to introduce randomness in failures
        funcs = []
        for source_type, source_paths in parse_sources(sources).items():
            log.info(event="collecting", source=source_type, num_items=len(source_paths))            
            funcs.extend((source_type, source) for source in source_paths)
        random.shuffle(funcs)

        collector_queue = Queue()
        for func in funcs:
            collector_queue.put_nowait(func)        
        return collector_queue

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
        # set up the queue      
        collector_queue = self._get_collector_funcs(sources)

        async def work():
            while not collector_queue.empty():               
                await self._collect(*(await collector_queue.get()))

        await asyncio.gather(*(work() for _ in range(self.batch_size)))

    def _init_run(self, batch_size: int):
        self.batch_size = batch_size
        self.apicollector = APICollectorAsync(batch_size)
        self.webscraper = AsyncWebScraper(batch_size<<4)
        self.beans_collected = 0
        self.publishers_collected = 0        

    @log_runtime_async(logger=log)
    async def run(self, sources, batch_size: int = BATCH_SIZE):
        """Main entry point for collector orchestrator. Runs the complete bean collection pipeline and refreshes chatter data."""
        log.info(event="starting collectors")

        self._init_run(batch_size)        
        async with self.cache, self.apicollector, self.webscraper:
            await self._run_collectors(sources)

        log.info(event="total collected", beans=self.beans_collected, publishers=self.publishers_collected)
