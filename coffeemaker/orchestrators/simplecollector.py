from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import logging
import asyncio
from typing import Callable
from icecream import ic

from azure.storage.queue import QueueClient
from coffeemaker.pybeansack.mongosack import Beansack as MongoSack
from coffeemaker.pybeansack.models import *
from coffeemaker.collectors.collector import APICollector, WebScraper, parse_sources
from coffeemaker.orchestrators.utils import *

FILTER_KINDS = [NEWS, BLOG]
BATCH_SIZE = int(os.getenv('COLLECTOR_BATCH_SIZE', 16*os.cpu_count()))
MAX_TEXT_LEN = 40*1024

log = logging.getLogger(__name__)

is_storable = lambda bean: above_threshold(bean, WORDS_THRESHOLD_FOR_SCRAPING) # if there is no summary and embedding then no point storing
storables = lambda beans: [bean for bean in beans if is_storable(bean)] if beans else beans 

def _prepare_new(beans: list[Bean]):
    for bean in beans:
        bean.id = bean.url
        bean.created = bean.created or bean.collected
        bean.updated = bean.updated or bean.collected
        bean.tags = None
        bean.cluster_id = bean.url
        bean.text = bean.text[:MAX_TEXT_LEN] if bean.text else None
    return beans

class Orchestrator:
    db: MongoSack = None
    queue: QueueClient = None

    def __init__(self, db_path: str = None, db_name: str = None, queue_path: str = None, queue_name: str = None):
        self.db = MongoSack(db_path, db_name) if (db_name and db_path) else None
        self.queue = QueueClient.from_connection_string(queue_path, queue_name) if queue_path else None

        self.apicollector = APICollector()
        self.webscraper = WebScraper()

    def _filter_new(self, beans: list[Bean]) -> list[Bean]:
        if not beans: beans
        try: exists = self.db.exists(beans)
        except: exists = [bean.url for bean in beans]
        return list({bean.url: bean for bean in beans if (bean.kind in FILTER_KINDS) and (bean.url not in exists)}.values())  

    def _collect(self, collect_func: Callable, sources: list):
        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="collector") as executor: 
            futures = {executor.submit(collect_func, source): source for source in sources}
            [self.triage_beans(futures[future], future.result()) for future in as_completed(futures)]

    def _scrape(self, source: str, beans: list[Bean]):
        if not beans: return

        log.info("scraping", extra={"source": source, "num_items": len(beans)})
        loop = asyncio.get_event_loop()
        beans = loop.run_until_complete(self.webscraper.scrape_beans(beans))
        stored = self._commit_new(source, beans) or []

        if len(beans) > len(stored): log.warning("scraping failed", extra={"source": source, "num_items": len(beans) - len(stored)})
        
    def triage_beans(self, source: str, collection: list[tuple[Bean, Chatter]]):
        if not collection: return

        beans, chatters = zip(*collection)
        # chatters = [chatter for chatter in chatters if chatter] if chatters else None
        # if chatters: self.localsack.store_chatters(chatters)
        beans = [bean for bean in beans if bean] if beans else None
        if not beans: return   
        log.info("triaged", extra={"source": source, "num_items": len(beans)})

        self._commit_new(source, beans)
        self._scrape(source, scrapables(beans))

    def queue_beans(self, source, beans: list[Bean]) -> None:
        if not beans or not self.queue: return
        [self.queue.send_message(bean.model_dump_json(exclude_none = True, exclude_unset=True, by_alias=True)) for bean in beans]
        log.info(f"queued->{self.queue.queue_name}", extra={"source": source, "num_items": len(beans)})

    def store_beans(self, source: str, beans: list[Bean]):
        if not beans or not self.db: return
        count = self.db.store_beans(beans)
        log.info("stored", extra={"source": source, "num_items": count})
    
    def _commit_new(self, source: str, beans: list[Bean]):
        beans = self._filter_new(beans)
        if not beans: return
        beans = storables(beans)
        beans = _prepare_new(beans)
        self.store_beans(source, beans)
        self.queue_beans(source, beans)
        return beans
    
    @log_runtime(logger=log)
    def run(self, sources = os.getenv("COLLECTOR_SOURCES")):
        for source_type, source_paths in parse_sources(sources).items():
            # awaiting on each group so that os is not overwhelmed by sockets
            log.info("collecting", extra={"source": source_type, "num_items": len(source_paths)})
            if source_type == 'ychackernews': self.triage_beans(self.apicollector.collect_ychackernews(source_paths))
            elif source_type == 'reddit': self._collect(self.apicollector.collect_subreddit, source_paths)
            elif source_type == 'rss': self._collect(self.apicollector.collect_rssfeed, source_paths)

        

