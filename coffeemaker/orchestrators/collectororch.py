import asyncio
import logging
import os
import random
from surrealdb import AsyncEmbeddedSurrealConnection, AsyncSurreal, RecordID, Surreal

from coffeemaker.collectors import APICollector, WebScraperLite, parse_sources
from coffeemaker.collectors.scraper import PublisherScraper
from coffeemaker.orchestrators.processingcache import AsyncProcessingCache
from coffeemaker.orchestrators.utils import *
from pybeansack import BEANS, Beansack, create_client
from pybeansack.models import *

FILTER_KINDS = [NEWS, BLOG]
BATCH_SIZE = int(
    os.getenv("BATCH_SIZE", os.cpu_count() * os.cpu_count())
)
WORDS_THRESHOLD_FOR_STORING = int(
    os.getenv("WORDS_THRESHOLD_FOR_STORING", 160)
)  # min words needed to not download the body

is_storable = lambda bean: bean.content and count_words(bean.content) >= WORDS_THRESHOLD_FOR_STORING
scrapables = lambda beans: [bean for bean in beans if not is_storable(bean)] if beans else beans
storables = lambda beans: [bean for bean in beans if is_storable(bean)] if beans else beans

log = logging.getLogger(__name__)

class Orchestrator:
    cache_kwargs: dict
    db: Beansack
    cache: AsyncProcessingCache
    run_total: int = 0

    def __init__(self, cache_kwargs: dict, db_kwargs: dict):
        self.cache_kwargs = cache_kwargs
        self.db = create_client(**db_kwargs)        

    async def _triage_collection_async(self, source: str, items: list[dict]):
        if not items:
            return

        beans = [item["bean"] for item in items if item and item.get("bean")]
        publishers = [
            item["publisher"] for item in items if item and item.get("publisher")
        ]
        chatters = [item["chatter"] for item in items if item and item.get("chatter")]

        if beans:
            await self.cache_beans(source, beans)
        if publishers:
            await asyncio.to_thread(self.db.store_publishers, publishers)
        if chatters:
            await asyncio.to_thread(self.db.store_chatters, chatters)

        return await asyncio.to_thread(self.db.deduplicate, BEANS, scrapables(beans))

    async def _triage_scrape_async(self, source: str, items: list[dict]):
        beans = storables([item["bean"] for item in items if item and item.get("bean")])
        publishers = [
            item["publisher"] for item in items if item and item.get("publisher")
        ]

        log.info("scraped", extra={"source": source, "num_items": len(beans)})
        if beans:
            return await self.cache_beans(source, beans)
        if publishers:
            return await asyncio.to_thread(self.db.store_publishers, publishers)

    async def cache_beans(self, source: str, beans: list[Bean]):
        data = prepare_beans_for_store(storables(beans))
        if not data:
            return
        [print(bean.image_url) for bean in beans if bean.image_url]  # --- IGNORE ---
        stored = await self.cache.store("collected_beans", data)
        self.run_total += len(stored)
        return stored

    # @log_runtime_async(logger=log)
    async def collect_beans_async(self, sources, batch_size):
        def get_collection_tasks():
            tasks = []
            for source_type, source_paths in parse_sources(sources).items():
                log.info(
                    "collecting",
                    extra={"source": source_type, "num_items": len(source_paths)},
                )
                if source_type == "ychackernews":
                    func = apicollector.collect_ychackernews_async
                elif source_type == "reddit":
                    func = apicollector.collect_subreddit_async
                elif source_type == "rss":
                    func = apicollector.collect_rssfeed_async

                tasks.extend((source, func) for source in source_paths)
            random.shuffle(tasks)
            return tasks

        async def collect(source, func):
            try:
                beans = await self._triage_collection_async(
                    source, await func(source)
                )  # collect and triage which ones needs scraping
                if beans:
                    await self._triage_scrape_async(
                        source,
                        await webscraper.scrape_beans(beans, collect_metadata=True),
                    )  # scrape and triage which ones succeeded
            except Exception as e:
                log.warning(
                    f"collection failed - {e.__class__.__name__} {e}",
                    extra={"source": source, "num_items": 1},
                )

        async with (
            APICollector(batch_size) as apicollector,
            WebScraperLite(batch_size) as webscraper,
            AsyncProcessingCache(db_name="beans", id_key=K_URL, model_cls=Bean) as self.cache,
        ):
            await asyncio.gather(
                *[collect(source, func) for source, func in get_collection_tasks()]
            )

    async def scrape_publishers_async(self, batch_size: int = BATCH_SIZE):
        pubs = self.db.query_publishers(
            conditions=["rss_feed IS NULL", "favicon IS NULL", "site_name IS NULL"]
        )
        log.info(
            "scraping publishers", extra={"source": self.run_id, "num_items": len(pubs)}
        )

        async with PublisherScraper(batch_size) as scraper:
            result = await scraper.scrape_publishers(pubs)

        result = [pub for pub in result if pub]
        log.info(
            "scraped publishers",
            extra={"source": self.run_id, "num_items": len(result)},
        )
        if result:
            await asyncio.to_thread(self.db.update_publishers, result)

    @log_runtime_async(logger=log)
    async def run_async(self, sources, batch_size: int = BATCH_SIZE):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.run_total = 0

        log.info(
            "starting collector",
            extra={"source": self.run_id, "num_items": os.cpu_count()},
        )
                    
        await asyncio.gather(
            *[
                self.collect_beans_async(sources, batch_size=batch_size),
                self.scrape_publishers_async(batch_size=batch_size),
            ]
        )
        self.db.refresh_chatters()
        log.info(
            "total collected",
            extra={"source": self.run_id, "num_items": self.run_total},
        )

    def close(self):
        # Close database connection
        self.db.close()
        

    # def _get_collect_funcs(self, sources):
    #     tasks = []
    #     for source_type, source_paths in parse_sources(sources).items():
    #         log.info("collecting", extra={"source": source_type, "num_items": len(source_paths)})
    #         if source_type == 'ychackernews': func = self.apicollector.collect_ychackernews
    #         elif source_type == 'reddit': func = self.apicollector.collect_subreddit
    #         elif source_type == 'rss': func = self.apicollector.collect_rssfeed

    #         tasks.extend((source, func) for source in source_paths)
    #     random.shuffle(tasks)
    #     return tasks

    # @log_runtime(logger=log)
    # def run_collection(self, sources):
    #     def collect(task):
    #         source, func = task
    #         try:
    #             needs_scraping = self._triage_collection(source, func(source))
    #             if needs_scraping: self.scraping_queue.put((source, needs_scraping))
    #         except Exception as e:
    #             log.warning(f"collection failed - {e.__class__.__name__}: {e}", extra={"source": source, "num_items": 1})

    #     with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="collecting-") as executor:
    #         executor.map(collect, self._get_collect_funcs(sources))
    #     self.scraping_queue.put(_END_OF_STREAM)

    # @log_runtime(logger=log)
    # def run_scraping(self):
    #     # TODO: this is broken, need to fix it with asyncio context manager for webscraper
    #     def scrape(source, beans):
    #         loop = asyncio.new_event_loop()
    #         asyncio.set_event_loop(loop)
    #         try:
    #             beans = loop.run_until_complete(self.webscraper.scrape_beans(beans, True))
    #             if beans: self._triage_scrape(source, beans)
    #         except Exception as e:
    #             log.warning(f"scraping failed - {e.__class__.__name__}: {e}", extra={"source": source, "num_items": len(beans)})
    #         asyncio.set_event_loop(loop.close())

    #     with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="scraping-") as executor:
    #         while True:
    #             token = self.scraping_queue.get()
    #             if token == _END_OF_STREAM: break
    #             if not token: continue
    #             source, beans = token
    #             executor.submit(scrape, source, beans)
    #             self.scraping_queue.task_done()

    # @log_runtime(logger=log)
    # def run(self, sources):
    #     import persistqueue

    #     self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #     self.run_total = 0
    #     self.scraping_queue = persistqueue.Queue(".scrapingqueue", tempdir=os.curdir)

    #     log.info("starting collector", extra={"source": self.run_id, "num_items": 1})
    #     self.cleanup()
    #     self.run_collection(sources)
    #     self.run_scraping()
    #     self.run_trend_ranking()
    #     log.info("total collected", extra={"source": self.run_id, "num_items": self.run_total})
