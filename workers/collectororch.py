import asyncio
from utils import log_runtime_async
from utils.dates import now
from utils.logs import get_logger
import os
import random
import uuid
import yaml
from datetime import datetime
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
from persistqueue import AsyncQueue
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
storable_publishers = (
    lambda publishers: list(filter(is_publisher_storable, publishers))
    if publishers
    else publishers
)

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
        bean = {key: value for key, value in bean.items() if value is not None}

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
        chatter = {key: value for key, value in chatter.items() if value is not None}

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
        publisher = {
            key: value for key, value in publisher.items() if value is not None
        }

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

    async def _triage(self, items: list[dict], scrape_on_fail: bool):
        """Store storable collection results and persist the rest for scraping."""
        if not items:
            return

        beans, chatters, publishers = self._split_items(items)

        async with asyncio.TaskGroup() as tg:
            if chatters:
                tg.create_task(self._cache_chatters(chatters))
            if beans:
                to_store = storable_beans(beans)
                to_scrape = scrapable_beans(beans)
                if to_store:
                    tg.create_task(self._cache_beans(to_store))
                if scrape_on_fail and to_scrape:
                    tg.create_task(self._queue_bean_scraping(to_scrape))
            if publishers:
                to_store = storable_publishers(publishers)
                to_scrape = [pub for pub in publishers if not is_publisher_storable(pub)]
                if to_store:
                    tg.create_task(self._cache_publishers(to_store))
                if scrape_on_fail and to_scrape:
                    tg.create_task(self._queue_publisher_scraping(to_scrape))

    async def _cache_beans(self, beans: list[dict]):
        count = await self.cache.set("beans", "collected", beans)
        if count is not None: 
            log.info(event="cached beans", source=beans[0][SOURCE], num_items=count)
            self.beans_collected += count
        else: 
            log.info(event="caching beans", source=beans[0][SOURCE], num_items=len(beans))
        
    async def _cache_publishers(self, publishers: list[dict]):
        count = await self.cache.set("publishers", "collected", publishers)
        if count is not None: 
            log.info(event="cached publishers", source=publishers[0][SOURCE], num_items=count)
            self.publishers_collected += count
        else: 
            log.info(event="caching publishers", source=publishers[0][SOURCE], num_items=len(publishers))

    async def _cache_chatters(self, chatters: list[dict]):
        pkg = [{"id": str(uuid.uuid4()), "chatters": chatters}]
        count = await self.cache.set(CHATTERS, COLLECTED, pkg)
        if count is not None: log.info(event="cached chatters", source=chatters[0][SOURCE], num_items=count)
        else: log.info(event="caching chatters", source=chatters[0][SOURCE], num_items=len(chatters))

    async def _queue_bean_scraping(self, beans: list[dict]):
        to_scrape = await self.cache.deduplicate(BEANS, COLLECTED, beans)
        if not to_scrape: return
        await self.scrape_queue.put((BEANS, to_scrape))

    async def _queue_publisher_scraping(self, publishers: list[dict]):
        to_scrape = await self.cache.deduplicate(PUBLISHERS, COLLECTED, publishers)
        if not to_scrape: return
        await self.scrape_queue.put((PUBLISHERS, to_scrape))
    
    async def _queue_collectors(self, sources, batch_size: int):
        # shuffling the sources to introduce randomness in failures
        funcs = []
        for source_type, source_paths in parse_sources(sources).items():
            log.info(event="collecting", source=source_type, num_items=len(source_paths))            
            funcs.extend((source_type, source) for source in source_paths)
        random.shuffle(funcs)

        await asyncio.gather(*[self.collect_queue.put_nowait(item) for item in funcs])        
        await asyncio.gather(*(self.collect_queue.put_nowait(None) for _ in range(batch_size)))


    async def _run_collect(self, batch_size: int):
        """Run the collect functions until all collect functions are done. Then insert an _END_MARKER for the scraper."""
        async def collect_and_triage(source_type, source):
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
            if to_triage: await self._triage(to_triage, scrape_on_fail=True)
        
        async def work():
            while (item := await self.collect_queue.get()):
                collect_func, source = item
                await collect_and_triage(collect_func, source)
            await self.scrape_queue.put(None)

        await asyncio.gather(*(work() for _ in range(batch_size)))
        log.info(event="collection completed")


    async def _run_scrape(self, batch_size: int):
        async def scrape_and_triage(data_type, to_scrape):            
            to_triage = None
            if data_type == BEANS: to_triage = await self.webscraper.scrape_beans(to_scrape)
            elif data_type == PUBLISHERS: to_triage = await self.webscraper.scrape_publishers(to_scrape)
            if not to_triage: return

            if data_type == BEANS: log.info(event="scraped beans", source=to_scrape[0][SOURCE], num_items=len(to_triage))
            elif data_type == PUBLISHERS: log.info(event="scraped publishers", source=to_scrape[0][SOURCE], num_items=len(to_triage))
            await self._triage(to_triage, scrape_on_fail=False)

        async def work():
            while (item := await self.scrape_queue.get()):
                data_type, to_scrape = item
                await scrape_and_triage(data_type, to_scrape)

        await asyncio.gather(*(work() for _ in range(batch_size)))
        log.info(event="scraping completed")


    def _init_run(self, batch_size: int):
        DIR = ".cache"
        os.makedirs(DIR, exist_ok=True)
        self.collect_queue = AsyncQueue(f"{DIR}/collect-{now().strftime('%Y-%m-%d-%H-%M-%S')}", tempdir=DIR)
        self.scrape_queue = AsyncQueue(f"{DIR}/scrape-{now().strftime('%Y-%m-%d-%H-%M-%S')}", tempdir=DIR)

        self.apicollector = APICollectorAsync(batch_size)
        self.webscraper = AsyncWebScraper(batch_size)

        self.beans_collected, self.publishers_collected = 0, 0

    @log_runtime_async(logger=log)
    async def run(self, sources, batch_size: int = BATCH_SIZE):
        """Main entry point for collector orchestrator. Runs the complete bean collection pipeline and refreshes chatter data."""
        log.info(event="starting collectors")

        self._init_run(batch_size)
        async with self.cache, self.apicollector, self.webscraper:
            await asyncio.gather(
                self._queue_collectors(sources, batch_size),
                self._run_collect(batch_size),
                self._run_scrape(batch_size)
            )
        log.info(event="total collected", beans=self.beans_collected, publishers=self.publishers_collected)
