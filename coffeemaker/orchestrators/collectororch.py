import os
import logging
import asyncio
import random
import persistqueue
from concurrent.futures import ThreadPoolExecutor
from icecream import ic
from datetime import timezone
from azure.storage.queue import QueueClient
from pymongo import UpdateOne
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.pybeansack.models import *
from coffeemaker.collectors.collector import APICollector, WebScraper, parse_sources
from coffeemaker.orchestrators.utils import *

END_OF_STREAM = "END_OF_STREAM"
FILTER_KINDS = [NEWS, BLOG]
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 16*os.cpu_count()))

log = logging.getLogger(__name__)

# is_indexable = lambda bean: above_threshold(bean.content, WORDS_THRESHOLD_FOR_INDEXING) and (bean.created > ndays_ago(2))
is_scrapable = lambda bean: not above_threshold(bean.content, WORDS_THRESHOLD_FOR_SCRAPING) # if there is no summary and embedding then no point storing
# indexables = lambda beans: list(filter(is_indexable, beans)) if beans else beans
scrapables = lambda beans: list(filter(is_scrapable, beans)) if beans else beans 

def _prepare_for_storing(beans: list[Bean]):
    for bean in beans:
        bean.id = bean.url
        bean.created = bean.created or bean.collected
        bean.updated = bean.updated or bean.collected
        if not bean.created.tzinfo: bean.created.replace(tzinfo=timezone.utc)
        bean.num_words_in_title = num_words(bean.title)
        bean.num_words_in_summary = num_words(bean.summary)
        bean.num_words_in_content = num_words(bean.content)
    return beans

class Orchestrator:
    db: Beansack = None
    queues: list[QueueClient] = None
    workers_semaphore: int = BATCH_SIZE
    collection_queue = None
    scraping_queue = None
    run_total: int = 0

    def __init__(self, mongodb_conn_str: str, db_name: str):
        self.db = Beansack(mongodb_conn_str, db_name)
        self.apicollector = APICollector()
        self.webscraper = WebScraper(os.getenv('REMOTE_CRAWLER_URL'), 16) # NOTE: this value is hard-coded for now

    def _filter_new(self, beans: list[Bean]) -> list[Bean]:
        if not beans: return beans
        try: exists = self.db.exists(beans)
        except: exists = [bean.url for bean in beans]
        return list({bean.url: bean for bean in beans if (bean.kind in FILTER_KINDS) and (bean.url not in exists)}.values())  
    
    def _triage_collection(self, source: str, collection):
        if not collection: return

        beans, chatters = zip(*collection)
        beans = self._filter_new([bean for bean in beans if bean] if beans else None)
        chatters = [chatter for chatter in chatters if chatter] if chatters else None

        if beans: self.store_beans(source, beans)
        if chatters: self.db.store_chatters(chatters)

        return scrapables(beans)
    
    def _triage_scrape(self, source, beans):
        beans = [bean for bean in beans if not is_scrapable(bean)]
        count = self.db.update_bean_fields(
            _prepare_for_storing(beans), 
            [
                K_CONTENT, K_SUMMARY, K_TITLE, K_IS_SCRAPED,
                K_NUM_WORDS_CONTENT, K_NUM_WORDS_SUMMARY, K_NUM_WORDS_TITLE,
                K_IMAGEURL, K_AUTHOR, K_CREATED, 
                K_SITE_RSS_FEED, K_SITE_NAME, K_SITE_FAVICON 
            ]
        )
        log.info("scraped", extra={"source": source, "num_items": count})

    def store_beans(self, source: str, beans: list[Bean]):
        if not beans: return
        count = self.db.store_beans(_prepare_for_storing(beans))
        log.info("stored", extra={"source": source, "num_items": count})
        self.run_total += count
    
    def run_trend_ranking(self):
        trends = self.db.get_latest_chatters(None)
        for trend in trends:
            trend.trend_score = calculate_trend_score(trend)
        updates = [UpdateOne(
            filter={K_ID: trend.url}, 
            update={
                "$set": {
                    K_LIKES: trend.likes,
                    K_COMMENTS: trend.comments,
                    K_SHARES: trend.shares,
                    K_SHARED_IN: trend.shared_in,
                    K_LATEST_LIKES: trend.likes_change,
                    K_LATEST_COMMENTS: trend.comments_change,
                    K_LATEST_SHARES: trend.shares_change,
                    K_TRENDSCORE: trend.trend_score,
                    K_UPDATED: trend.collected      
                }
            }
        ) for trend in trends if trend.trend_score] 
        count = self.db.update_beans(updates)
        log.info("trend ranked", extra={"source": self.run_id, "num_items": count})
    
    def _get_collect_funcs(self, sources):
        tasks = []
        for source_type, source_paths in parse_sources(sources).items():
            log.info("collecting", extra={"source": source_type, "num_items": len(source_paths)})
            if source_type == 'ychackernews': func = self.apicollector.collect_ychackernews
            elif source_type == 'reddit': func = self.apicollector.collect_subreddit
            elif source_type == 'rss': func = self.apicollector.collect_rssfeed

            tasks.extend((source, func) for source in source_paths)
        random.shuffle(tasks)
        return tasks
    
    @log_runtime(logger=log)
    def run_collection(self, sources):
        def collect(task):
            source, func = task
            try:
                needs_scraping = self._triage_collection(source, func(source))
                if needs_scraping: self.scraping_queue.put((source, needs_scraping))
            except Exception as e:
                log.warning("collection failed", extra={"source": source, "num_items": 1})
                log.error(e, extra={"source": source, "num_items": 1})

        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="collecting-") as executor:
            executor.map(collect, self._get_collect_funcs(sources))
        self.scraping_queue.put(END_OF_STREAM) 

    @log_runtime(logger=log)
    def run_scraping(self):
        def scrape(source, beans):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                beans = loop.run_until_complete(self.webscraper.scrape_beans(beans, True))
                if beans: self._triage_scrape(source, beans)    
            except Exception as e:
                log.warning("scraping failed", extra={"source": source, "num_items": len(beans)})
                log.error(e, extra={"source": source, "num_items": len(beans)})
            asyncio.set_event_loop(loop.close())        

        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="scraping-") as executor:
            while True:
                token = self.scraping_queue.get()
                if token == END_OF_STREAM: break
                if not token: continue
                source, beans = token
                executor.submit(scrape, source, beans)
                self.scraping_queue.task_done()

    def _get_async_collect_funcs(self, sources):
        tasks = []
        for source_type, source_paths in parse_sources(sources).items():
            log.info("collecting", extra={"source": source_type, "num_items": len(source_paths)})
            if source_type == 'ychackernews': func = self.apicollector.collect_ychackernews_async
            elif source_type == 'reddit': func = self.apicollector.collect_subreddit_async
            elif source_type == 'rss': func = self.apicollector.collect_rssfeed_async

            tasks.extend((source, func) for source in source_paths)
        random.shuffle(tasks)
        return tasks

    @log_runtime_async(logger=log)
    async def run_collection_async(self, sources):        
        workers_semaphore = asyncio.Semaphore(BATCH_SIZE)
        async def collect(source, func):
            async with workers_semaphore:
                try: 
                    needs_scraping = self._triage_collection(source, await func(source))
                    if needs_scraping: await self.scraping_queue.put((source, needs_scraping))
                except Exception as e:
                    log.warning("collection failed", extra={"source": source, "num_items": 1})
                    log.error(e, extra={"source": source, "num_items": 1})

        async with asyncio.TaskGroup() as tg:
            [tg.create_task(collect(source, beans), name = f"collecting-{source}") for source, beans in self._get_async_collect_funcs(sources)]     
        await self.scraping_queue.put(END_OF_STREAM)        

    @log_runtime_async(logger=log)
    async def run_scraping_async(self):
        workers_semaphore = asyncio.Semaphore(BATCH_SIZE)
        async def scrape(source, beans):
            async with workers_semaphore:
                try:
                    beans = await self.webscraper.scrape_beans(beans, collect_metadata=True)
                    if beans: self._triage_scrape(source, beans)
                except Exception as e:
                    log.warning("scraping failed", extra={"source": source, "num_items": len(beans)})
                    log.error(e, extra={"source": source, "num_items": len(beans)})

        async with asyncio.TaskGroup() as tg:
            while True:
                token = await self.scraping_queue.get()
                if token == END_OF_STREAM: break
                if not token: continue
                source, beans = token
                tg.create_task(scrape(source, beans), name=f"scraping-{source}")
                self.scraping_queue.task_done() 
    
    @log_runtime(logger=log)
    def run(self, sources = os.getenv("COLLECTOR_SOURCES", "./coffeemaker/collectors/feeds.yaml")):
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.run_total = 0
        self.scraping_queue = persistqueue.Queue(".scrapingqueue", tempdir=os.curdir)

        log.info("starting collector", extra={"source": run_id, "num_items": 1})

        self.run_collection(sources)
        self.run_trend_ranking(None)
        self.run_scraping()

        log.info("total collected", extra={"source": run_id, "num_items": self.run_total})

    @log_runtime_async(logger=log)
    async def run_async(self, sources = os.getenv("COLLECTOR_SOURCES", "./coffeemaker/collectors/feeds.yaml")):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.run_total = 0        
        self.scraping_queue = asyncio.Queue()

        log.info("starting collector", extra={"source": self.run_id, "num_items": 1})

        await self.apicollector.start_session()
        collection = asyncio.create_task(self.run_collection_async(sources))
        scraping = asyncio.create_task(self.run_scraping_async())
        await collection
        self.run_trend_ranking()
        await scraping
        await self.apicollector.close_session()

        log.info("total collected", extra={"source": self.run_id, "num_items": self.run_total})



