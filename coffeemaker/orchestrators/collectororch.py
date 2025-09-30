import os
import logging
import asyncio
import random
from concurrent.futures import ThreadPoolExecutor
from icecream import ic
from coffeemaker.pybeansack import mongosack, warehouse
from coffeemaker.pybeansack.models import *
from coffeemaker.collectors import APICollector, WebScraperLite, parse_sources
from coffeemaker.orchestrators.utils import *

FILTER_KINDS = [NEWS, BLOG]
BATCH_SIZE = int(os.getenv('BATCH_SIZE', os.cpu_count()*os.cpu_count()))

_END_OF_STREAM = "END_OF_STREAM"

log = logging.getLogger(__name__)

is_scrapable = lambda bean: not above_threshold(bean.content, WORDS_THRESHOLD_FOR_SCRAPING) 
scrapables = lambda beans: list(filter(is_scrapable, beans)) if beans else beans 
storables = lambda beans: [bean for bean in beans if not is_scrapable(bean)]
cores = lambda beans: [BeanCore(**bean.model_dump()) for bean in beans if bean and bean.title]

class Orchestrator:
    db: warehouse.Beansack|mongosack.Beansack = None
    run_total: int = 0

    def __init__(self, db_conn_str: tuple[str,str], batch_size: int = BATCH_SIZE):
        self.db = initialize_db(db_conn_str)
        self.batch_size = batch_size     

    async def _triage_collection_async(self, source: str, beans: list[Bean]):
        if not beans: return

        chatters = [Chatter(**bean.chatter.model_dump(exclude={"shares"})) for bean in beans if bean and bean.chatter]
        if chatters: await asyncio.to_thread(self.db.store_chatters, chatters)

        # TODO: disabled for now
        # publishers = [bean.publisher for bean in beans if bean and bean.publisher]
        # if publishers: await asyncio.to_thread(self.db.store_publishers, publishers)

        # filtering out new beans        
        beans = await asyncio.to_thread(self.db.deduplicate, "bean_cores", "url", beans)
        if beans: await self.store_beans_async(source, storables(beans))
       
        return scrapables(beans)

    async def _triage_scrape_async(self, source: str, beans: list[Bean]):
        beans = storables(beans)
        log.info("scraped", extra={"source": source, "num_items": len(beans)})
        beans = await asyncio.to_thread(self.db.deduplicate, "bean_cores", "url", beans)
        if beans: return await self.store_beans_async(source, beans)

    async def store_beans_async(self, source: str, beans: list[Bean]):
        if not beans: return       
        items = await asyncio.to_thread(self.db.store_cores, cores(beans))
        count = len(items) if items else 0
        log.info("stored", extra={"source": source, "num_items": count})
        self.run_total += count
        return beans

    @log_runtime_async(logger=log)
    async def run_collection_async(self, sources):   
        def get_collection_tasks():
            tasks = []
            for source_type, source_paths in parse_sources(sources).items():
                log.info("collecting", extra={"source": source_type, "num_items": len(source_paths)})
                if source_type == 'ychackernews': func = apicollector.collect_ychackernews_async
                elif source_type == 'reddit': func = apicollector.collect_subreddit_async
                elif source_type == 'rss': func = apicollector.collect_rssfeed_async

                tasks.extend((source, func) for source in source_paths)
            random.shuffle(tasks)
            return tasks
     
        async def collect(source, func):
            try: 
                beans = await self._triage_collection_async(source, await func(source)) # collect and triage which ones needs scraping 
                if beans: await self._triage_scrape_async(source, await webscraper.scrape_beans(beans, collect_metadata=True)) # scrape and triage which ones succeeded
            except Exception as e:
                log.warning(f"collection failed - {e.__class__.__name__} {e}", extra={"source": source, "num_items": 1})

        async with APICollector(self.batch_size) as apicollector, WebScraperLite(self.batch_size) as webscraper:
            await asyncio.gather(*[collect(source, func) for source, func in get_collection_tasks()])    

    @log_runtime_async(logger=log)
    async def run_async(self, sources):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.run_total = 0        

        log.info("starting collector", extra={"source": self.run_id, "num_items": os.cpu_count()})
        await self.run_collection_async(sources)
        # self.run_trend_ranking() # trend rank from this collection if execution finished
        log.info("total collected", extra={"source": self.run_id, "num_items": self.run_total})

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



