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

def _prepare_for_storing(items: list[Bean]) -> list[BeanCore]:
    for item in items:
        item.title = clean_text(item.title)
        item.title_length = num_words(item.title)
        item.summary = clean_text(item.summary)
        item.summary_length = num_words(item.summary)
        item.content = clean_text(item.content)
        item.content_length = num_words(item.content)
        item.author = clean_text(item.author)
        item.image_url = clean_text(item.image_url)
        item.created = item.created or datetime.now()
        item.collected = item.collected or datetime.now()

    return list(map(lambda b: BeanCore(**b.model_dump()), items))

# def _prepare_for_storing(beans: list[Bean]):
#     for bean in beans:
#         bean.id = bean.url
#         bean.created = bean.created or bean.collected
#         bean.updated = bean.updated or bean.collected
#         if not bean.created.tzinfo: bean.created.replace(tzinfo=timezone.utc)
#         bean.num_words_in_title = num_words(bean.title)
#         bean.num_words_in_summary = num_words(bean.summary)
#         bean.num_words_in_content = num_words(bean.content)
#         if bean.author == "[no-author]": bean.author = None
#     return beans

class Orchestrator:
    db: warehouse.Beansack|mongosack.Beansack = None
    run_total: int = 0

    def __init__(self, ducklake_conn: tuple[str,str] = None, mongodb_conn: tuple[str, str] = None, batch_size: int = BATCH_SIZE):
        if ducklake_conn: self.db = warehouse.Beansack(catalogdb=ducklake_conn[0], storagedb=ducklake_conn[1])
        elif mongodb_conn: self.db = mongosack.Beansack(mongodb_conn[0], mongodb_conn[1])
        else: raise ValueError("Either mongodb_conn or ducklake_conn must be provided")
        self.batch_size = batch_size
         
    async def _filter_new(self, beans: list[Bean]) -> list[Bean]:
        if not beans: return beans
        exists = await asyncio.to_thread(self.db.exists, [bean.url for bean in beans])
        # try: exists = self.db.exists([bean.url for bean in beans])
        # except: exists = [bean.url for bean in beans]
        return list({bean.url: bean for bean in beans if (bean.kind in FILTER_KINDS) and (bean.url not in exists)}.values())  
    
    async def _triage_collection(self, source: str, collection):
        if not collection: return

        beans, chatters = zip(*collection)
        beans = await self._filter_new([bean for bean in beans if bean] if beans else None)
        chatters = [chatter for chatter in chatters if chatter] if chatters else None

        if beans: await self.store_beans(source, storables(beans))
        if chatters: await asyncio.to_thread(self.db.store_chatters, chatters)

        return scrapables(beans)

    async def _triage_scrape(self, source: str, beans: list[Bean]):
        beans = storables(beans)
        log.info("scraped", extra={"source": source, "num_items": len(beans)})
        beans = await self._filter_new(beans)
        if beans: return await self.store_beans(source, beans)

    async def store_beans(self, source: str, beans: list[Bean]):
        if not beans: return       
        # count = self.db.store_beans(_prepare_for_storing(beans))
        items = await asyncio.to_thread(self.db.store_cores, _prepare_for_storing(beans))
        count = len(items) if items else 0
        log.info("stored", extra={"source": source, "num_items": count})
        self.run_total += count
        return beans
    
    # def run_trend_ranking(self):
    #     # get ranking data from the master db
    #     trends = self.db.get_latest_chatters(None)
    #     for trend in trends:
    #         trend.trend_score = calculate_trend_score(trend)
    #     updates = [UpdateOne(
    #         filter={K_ID: trend.url}, 
    #         update={
    #             "$set": {
    #                 K_LIKES: trend.likes,
    #                 K_COMMENTS: trend.comments,
    #                 K_SHARES: trend.shares,
    #                 K_SHARED_IN: trend.shared_in,
    #                 K_LATEST_LIKES: trend.likes_change,
    #                 K_LATEST_COMMENTS: trend.comments_change,
    #                 K_LATEST_SHARES: trend.shares_change,
    #                 K_TRENDSCORE: trend.trend_score,
    #                 K_UPDATED: trend.collected      
    #             }
    #         }
    #     ) for trend in trends if trend.trend_score] 
    #     count = self.db.update_beans(updates)
    #     log.info("trend ranked", extra={"source": self.run_id, "num_items": count})

    # def cleanup(self):
    #     # NOTE: remove anything collected 7 days ago that did not get processed by analyzer
    #     # TODO: this is a temporary fix.
    #     _CLEANUP_WINDOW = 7
    #     _CLEANUP_FILTER = {
    #         K_COLLECTED: {"$lt": ndays_ago(_CLEANUP_WINDOW)},
    #         K_CLUSTER_ID: {"$exists": False},
    #         K_GIST: {"$exists": False},
    #         K_KIND: {"$ne": GENERATED}
    #     }
    #     count = self.db.beanstore.delete_many(_CLEANUP_FILTER).deleted_count
    #     log.info("cleaned up beans", extra={"source": self.run_id, "num_items": count})
    #     count = self.db.chatterstore.delete_many({K_COLLECTED: {"$lt": ndays_ago(_CLEANUP_WINDOW)}}).deleted_count
    #     log.info("cleaned up chatters", extra={"source": self.run_id, "num_items": count})

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
                beans = await self._triage_collection(source, await func(source)) # collect and triage which ones needs scraping 
                if beans: await self._triage_scrape(source, await webscraper.scrape_beans(beans, collect_metadata=True)) # scrape and triage which ones succeeded
            except Exception as e:
                log.warning(f"collection failed - {e.__class__.__name__} {e}", extra={"source": source, "num_items": 1})

        async with APICollector(self.batch_size) as apicollector, WebScraperLite(self.batch_size) as webscraper:
            await asyncio.gather(*[collect(source, func) for source, func in get_collection_tasks()])    
    
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

    @log_runtime_async(logger=log)
    async def run_async(self, sources):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.run_total = 0        

        log.info("starting collector", extra={"source": self.run_id, "num_items": os.cpu_count()})
        self.db.cleanup()
        await self.run_collection_async(sources)
        
        # self.run_trend_ranking() # trend rank from this collection if execution finished
        log.info("total collected", extra={"source": self.run_id, "num_items": self.run_total})



