import asyncio
import logging
import os
import random
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
)
from pybeansack import Beansack, Chatter
from coffeemaker.processingcache.base import AsyncStateStoreBase
from .utils import *
from icecream import ic

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count() * os.cpu_count()))
WORDS_THRESHOLD_FOR_STORING = int(os.getenv("WORDS_THRESHOLD_FOR_STORING", 160))  # min words needed to not download the body

is_bean_storable = (
    lambda bean: bean
    and bean.get("content_length", 0) >= WORDS_THRESHOLD_FOR_STORING
)
scrapable_beans = (
    lambda beans: [bean for bean in beans if not is_bean_storable(bean)]
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

log = logging.getLogger(__name__)

class Collector:
    db: Beansack
    state_store: AsyncStateStoreBase
    apicollector: APICollectorAsync
    webscraper: AsyncWebScraper
    run_id: str
    beans_collected: int
    publishers_collected: int

    def __init__(self, state_store: AsyncStateStoreBase, db: Beansack):
        self.db = db   
        self.state_store = state_store

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
        chatters = [Chatter(**item) for item in chatters]

        async with asyncio.TaskGroup() as tg:
            if chatters:
                tg.create_task(asyncio.to_thread(self.db.store_chatters, chatters))
            if beans:
                to_store = storable_beans(beans)
                to_scrape = scrapable_beans(beans)
                if to_store:
                    tg.create_task(self._cache_beans(to_store))
                if scrape_on_fail and to_scrape:
                    tg.create_task(self._scrape_beans(to_scrape))
            if publishers:
                to_store = storable_publishers(publishers)
                to_scrape = [
                    pub for pub in publishers if not is_publisher_storable(pub)
                ]
                if to_store:
                    tg.create_task(self._cache_publishers(to_store))
                if scrape_on_fail and to_scrape:
                    tg.create_task(self._scrape_publishers(to_scrape))

    async def _cache_beans(self, beans: list[dict]):
        await self.state_store.set("beans", "collected", beans)
        log.info("caching beans", extra={"source": beans[0]["source"], "num_items": len(beans)})
        
    async def _cache_publishers(self, publishers: list[dict]):
        await self.state_store.set("publishers", "collected", publishers)
        log.info("caching publishers", extra={"source": publishers[0]["source"], "num_items": len(publishers)})

    async def _scrape_beans(self, beans: list[dict]):
        to_scrape = await self.state_store.deduplicate("beans", "collected", beans)
        if not to_scrape: return

        to_triage = await self.webscraper.scrape_beans(to_scrape)
        if not to_triage: return 
        
        log.info("scraped beans", extra={"source": to_scrape[0]["source"], "num_items": len(to_triage)})
        await self._triage(to_triage, scrape_on_fail=False)
        

    async def _scrape_publishers(self, publishers: list[dict]):
        to_scrape = await self.state_store.deduplicate("publishers", "collected", publishers)
        if not to_scrape: return

        to_triage = await self.webscraper.scrape_publishers(to_scrape)
        if not to_triage: return

        log.info("scraped publishers", extra={"source": to_scrape[0]["source"], "num_items": len(to_triage)})
        await self._triage(to_triage, scrape_on_fail=False)


    async def _collect(self, collect_func, *args, **kwargs):
        try: await self._triage(await collect_func(*args, **kwargs), scrape_on_fail=True)
        except Exception as e: log.warning(f"{collect_func.__name__}{args} failed", extra={"source": f"{e.__class__.__name__}: {e}", "num_items": 1})

    def _create_collection_funcs(self, sources):
        funcs = []
        for source_type, source_paths in parse_sources(sources).items():
            log.info(
                "collecting",
                extra={"source": source_type, "num_items": len(source_paths)},
            )
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

        log.info(
            "starting collectors",
            extra={"source": self.run_id, "num_items": batch_size},
        )
        async with (
            self.state_store,
            APICollectorAsync(batch_size) as self.apicollector,
            AsyncWebScraper(batch_size) as self.webscraper,
        ):
            await asyncio.gather(
                *(
                    self._collect(func, source)
                    for func, source in self._create_collection_funcs(sources)
                )
            )
