import asyncio
import os
import random
import uuid
import yaml
from datacollectors import RSSFeedCollector, GovInfoRSSCollector, RedditCollector, HackerNewsCollector, SECFilingCollector, AsyncWebScraper, POST
from utils.fields import (
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
    IMAGE_URL,
    KIND,
    LANGUAGE,
    LIKES,
    PLATFORM,
    RESTRICTED_CONTENT,
    RSS_FEED,
    SITE_LANGUAGE,
    SITE_NAME,
    DOMAIN_NAME,
    SUMMARY,
    TAGS,
    TITLE,
    URL,
)
from utils import now_str, get_logger, log_runtime_async
from persistqueue import AsyncQueue
from processingcache import AsyncStateCacheBase
from .states import *
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
is_bean_scrapable = lambda bean: (
    bean
    and bean.get(KIND) != POST
    and bean.get('content_length', 0) < WORDS_THRESHOLD_FOR_STORING
    and not any(tag in (bean.get(TITLE) or "").lower() for tag in IGNORE_WORD_GAMES)
)
is_publisher_storable = lambda publisher: publisher and any(field in publisher for field in [SITE_NAME, FAVICON, DESCRIPTION])
is_publisher_scrapable = lambda publisher: publisher and not any(field in publisher for field in [SITE_NAME, FAVICON, DESCRIPTION])

def filtered_list(items: list[dict], filter_func) -> list[dict]:
    if not items: return items
    return list(filter(filter_func, items))

def validate_bean_item(item: dict) -> bool:
    if not item:
        return False
    return bool(
        item.get(TITLE)
        and item.get(COLLECTED)
        and item.get(CREATED)
        and item.get(BASE_URL)
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
    return bool(item.get(DOMAIN_NAME) and item.get(BASE_URL))

def parse_sources(sources: str) -> dict:
    if os.path.exists(sources):
        with open(sources, 'r') as file:
            data = yaml.safe_load(file)
    else: data = yaml.safe_load(sources)
    return data['sources']

log = get_logger("collectorworker")

_SCRAPER_QUEUE = ".cache/scraper-queue"
_SCRAPER_CHUNK_SIZE = 32

class Collector:
    cache: AsyncStateCacheBase
    rss_collector: RSSFeedCollector
    govinfo_collector: GovInfoRSSCollector
    reddit_collector: RedditCollector
    hn_collector: HackerNewsCollector
    sec_filing_collector: SECFilingCollector
    webscraper: AsyncWebScraper
    beans_collected: int
    publishers_collected: int

    def __init__(self, cache: AsyncStateCacheBase, batch_size: int = BATCH_SIZE):
        self.cache = cache
        self.batch_size = batch_size
        self.rss_collector = RSSFeedCollector(batch_size)
        self.govinfo_collector = GovInfoRSSCollector(batch_size)
        self.reddit_collector = RedditCollector(batch_size)
        self.hn_collector = HackerNewsCollector(batch_size)
        self.sec_filing_collector = SECFilingCollector(batch_size)
        self.webscraper = AsyncWebScraper(batch_size*_SCRAPER_CHUNK_SIZE)

    def _split_item(self, item: dict):
        if not item:
            return None, None, None

        chatter = {
            CHATTER_URL: item.get(CHATTER_URL),
            URL: item.get(URL),
            PLATFORM: item.get(PLATFORM) or item.get(DOMAIN_NAME),
            FORUM: item.get(FORUM),
            COLLECTED: item.get(COLLECTED),
            LIKES: item.get(LIKES),
            COMMENTS: item.get(COMMENTS),
            "subscribers": item.get("subscribers"),
        }
        [chatter.pop(key, None) for key in list(chatter) if not chatter[key]]

        publisher = {
            DOMAIN_NAME: item.get(DOMAIN_NAME),
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

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._cache_chatters(chatters))
            tg.create_task(self._cache_beans(filtered_list(beans, is_bean_storable)))
            tg.create_task(self._queue_scrape(BEANS, filtered_list(beans, is_bean_scrapable)))
            del beans
            tg.create_task(self._cache_publishers(filtered_list(publishers, is_publisher_storable)))
            tg.create_task(self._queue_scrape(PUBLISHERS, filtered_list(publishers, is_publisher_scrapable)))
            del publishers

    async def _cache_beans(self, beans: list[dict]):
        if not beans: return

        source_marker, item_count = beans[0][DOMAIN_NAME], len(beans)
        cached_count = await self.cache.set(BEANS, COLLECTED, beans)
        beans[:] = []

        if cached_count is not None: 
            log.info(event="cached", source=source_marker, beans=cached_count)
            self.beans_collected += cached_count
        else: 
            log.info(event="caching", source=source_marker, beans=item_count)
        
    async def _cache_publishers(self, publishers: list[dict]):
        if not publishers: return

        source_marker, item_count = publishers[0][DOMAIN_NAME], len(publishers)
        cached_count = await self.cache.set(PUBLISHERS, COLLECTED, publishers)        
        publishers[:] = []

        if cached_count is not None: 
            log.info(event="cached", source=source_marker, publishers=cached_count)
            self.publishers_collected += cached_count
        else: 
            log.info(event="caching", source=source_marker, publishers=item_count)

    async def _cache_chatters(self, chatters: list[dict]):
        if not chatters: return

        source_marker, item_count = chatters[0].get(FORUM, chatters[0][PLATFORM]), len(chatters)
        pkg = [{"id": str(uuid.uuid4()), "chatters": chatters}]
        cached_count = await self.cache.set(CHATTERS, COLLECTED, pkg)
        chatters[:] = []
        del pkg

        if cached_count is not None: log.info(event="cached", source=source_marker, chatters=cached_count)
        else: log.info(event="caching", source=source_marker, chatters=item_count)

    async def _scrape_beans(self, beans: list[dict]):
        if not beans: return

        beans[:] = await self.cache.deduplicate(BEANS, COLLECTED, beans)
        if not beans: return

        beans[:] = filtered_list(await self.webscraper.scrape_beans(beans), is_bean_storable)
        if not beans: return

        log.info(event="scraped", source=beans[0][DOMAIN_NAME], beans=len(beans))
        await self._cache_beans(beans)

    async def _scrape_publishers(self, publishers: list[dict]):        
        if not publishers: return

        publishers[:] = await self.cache.deduplicate(PUBLISHERS, COLLECTED, publishers)
        if not publishers: return

        publishers[:] = filtered_list(await self.webscraper.scrape_publishers(publishers), is_publisher_storable)
        if not publishers: return

        log.info(event="scraped", source=publishers[0][DOMAIN_NAME], publishers=len(publishers))
        await self._cache_publishers(publishers)

    async def _queue_scrape(self, kind: str, items: list[dict]):
        _scrape = lambda items: self._scrape_beans(items) if kind == BEANS else self._scrape_publishers(items)
        while items:
            await _scrape(items[:_SCRAPER_CHUNK_SIZE])
            del items[:_SCRAPER_CHUNK_SIZE]

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
            if source_type == "ychackernews":
                to_triage = await self.hn_collector.collect(source)
            elif source_type == "reddit":
                to_triage = await self.reddit_collector.collect(source, mode="json")
            elif source_type == "rss":
                to_triage = await self.rss_collector.collect(source)
            elif source_type == "govinfo":
                to_triage = await self.govinfo_collector.collect(source)
            elif source_type == "sec_edgar":
                to_triage = await self.sec_filing_collector.collect(source)
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

        async with asyncio.TaskGroup() as tg:
            for offset in range(self.batch_size):
                tg.create_task(work(offset))
        
        log.info(event="collectors completed")

    async def _run_scrapers(self):
        async def work():
            while items := await self.scraper_queue.get():
                kind, items = items
                if kind == BEANS: await self._scrape_beans(items)
                elif kind == PUBLISHERS: await self._scrape_publishers(items)
                del items
                
        async with asyncio.TaskGroup() as tg:
            for _ in range(self.batch_size):
                tg.create_task(work())

        log.info(event="scrapers completed")
    
    def _init_run(self):
        self.beans_collected = 0
        self.publishers_collected = 0
        # self.scraper_queue = AsyncQueue(path=f"{_SCRAPER_QUEUE}-{now_str()}", tempdir="/tmp", chunksize=_SCRAPER_CHUNK_SIZE)

    # async def _end_run(self):
    #     import shutil
    #     await self.scraper_queue.close()
    #     shutil.rmtree(self.scraper_queue.path, ignore_errors=True)


    @log_runtime_async(logger=log)
    async def run(self, sources):
        """Main entry point for collector orchestrator. Runs the complete bean collection pipeline and refreshes chatter data."""
        log.info(event="starting collectors")

        self._init_run()

        async with (
            self.rss_collector,
            self.govinfo_collector,
            self.reddit_collector,
            self.hn_collector,
            self.sec_filing_collector,
            self.webscraper,
            self.cache
        ):
            await asyncio.gather(
                self._run_collectors(sources), 
                # self._run_scrapers()
            )

        # await self._end_run()
        log.info(event="collection completed", beans=self.beans_collected, publishers=self.publishers_collected)
        