import os
import logging
import asyncio
import threading
from typing import Callable
from icecream import ic

from persistqueue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.queue import QueueClient
from coffeemaker.pybeansack.mongosack import Beansack as MongoSack
from coffeemaker.pybeansack.models import *
from coffeemaker.collectors.collector import APICollector, WebScraper, parse_sources
from coffeemaker.orchestrators.utils import *

FILTER_KINDS = [NEWS, BLOG]
BATCH_SIZE = int(os.getenv('COLLECTOR_BATCH_SIZE', 16*os.cpu_count()))

log = logging.getLogger(__name__)

is_indexable = lambda bean: above_threshold(bean.content, WORDS_THRESHOLD_FOR_INDEXING)
is_scrapable = lambda bean: not above_threshold(bean.content, WORDS_THRESHOLD_FOR_SCRAPING) # if there is no summary and embedding then no point storing
indexables = lambda beans: list(filter(is_indexable, beans)) if beans else beans
scrapables = lambda beans: list(filter(is_scrapable, beans)) if beans else beans 

def _prepare_new(beans: list[Bean]):
    for bean in beans:
        bean.id = bean.url
        bean.created = bean.created or bean.collected
        bean.updated = bean.updated or bean.collected
    return beans

class Orchestrator:
    db: MongoSack = None
    queues: QueueClient = None
    scraper_queue: Queue = None
    total: int = 0

    def __init__(self, db_path: str, db_name: str, queue_path: str = None, queue_names: list[str] = None):
        self.db = MongoSack(db_path, db_name)
        if queue_path: self.queues = [QueueClient.from_connection_string(queue_path, queue_name) for queue_name in queue_names]

        self.apicollector = APICollector(self.triage_beans)
        self.webscraper = WebScraper(BATCH_SIZE)
        self.scraper_queue = Queue(".scrapingqueue", tempdir=".")

    def _filter_new(self, beans: list[Bean]) -> list[Bean]:
        if not beans: return beans
        try: exists = self.db.exists(beans)
        except: exists = [bean.url for bean in beans]
        return list({bean.url: bean for bean in beans if (bean.kind in FILTER_KINDS) and (bean.url not in exists)}.values())  

    def _collect(self, collect_func: Callable, sources: list):
        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="collector") as executor: 
            results = list(executor.map(collect_func, sources))
        return results
            
    def triage_beans(self, source: str, collection: list[tuple[Bean, Chatter]]):
        if not collection: return

        beans, chatters = zip(*collection)
        # chatters = [chatter for chatter in chatters if chatter] if chatters else None
        # if chatters: self.localsack.store_chatters(chatters)
        beans = [bean for bean in beans if bean] if beans else None
        if not beans: return   
        log.info("triaged", extra={"source": source, "num_items": len(beans)})

        needs_scraping = scrapables(beans)
        if needs_scraping: self.scraper_queue.put_nowait((source, needs_scraping)) 
        self._commit_new(source, [bean for bean in beans if bean not in needs_scraping])

    def queue_beans(self, source, beans: list[Bean]) -> None:
        if not beans or not self.queues: return
        urls = [bean.url for bean in beans]
        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="queuing") as executor:
            [executor.map(queue.send_message, urls) for queue in self.queues]
        log.info(f"queued", extra={"source": source, "num_items": len(beans)})

    def store_beans(self, source: str, beans: list[Bean]):
        if not beans or not self.db: return
        count = self.db.store_beans(beans)
        log.info("stored", extra={"source": source, "num_items": count})
        self.total += count
    
    def _commit_new(self, source: str, beans: list[Bean]):
        beans = self._filter_new(beans)
        if not beans: return
        beans = _prepare_new(beans)
        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="commit") as executor:
            executor.submit(self.store_beans, source, beans),
            executor.submit(self.queue_beans, source, indexables(beans))
        return beans
    
    def _scrape(self, source: str, beans: list[Bean]):
        if not beans: return

        loop = asyncio.get_event_loop()
        beans = loop.run_until_complete(self.webscraper.scrape_beans(beans, True))
        log.info("scraped", extra={"source": source, "num_items": len(beans)})
        # nullify the contents for which the scraping failed to get anything substantial
        for bean in beans:
            if is_scrapable(bean): bean.content = None
        self._commit_new(source, beans) 
    
    @log_runtime(logger=log)
    def run(self, sources = os.getenv("COLLECTOR_SOURCES", "./coffeemaker/collectors/feeds.yaml")):
        self.total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting collector", extra={"source": run_id, "num_items": 1})

        # first collect
        for source_type, source_paths in parse_sources(sources).items():
            # awaiting on each group so that os is not overwhelmed by sockets
            log.info("collecting", extra={"source": source_type, "num_items": len(source_paths)})
            if source_type == 'ychackernews': self.triage_beans(source_type, self.apicollector.collect_ychackernews(source_paths))
            elif source_type == 'reddit': self._collect(self.apicollector.collect_subreddit, source_paths)
            elif source_type == 'rss': self._collect(self.apicollector.collect_rssfeed, source_paths)

        # then scrape
        while not self.scraper_queue.empty():
            source, beans = self.scraper_queue.get()
            self._scrape(source, beans)
            self.scraper_queue.task_done()

        log.info("completed collector", extra={"source": run_id, "num_items": self.total})

