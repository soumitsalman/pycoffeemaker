from typing import Awaitable
from icecream import ic
import logging
import asyncio
from asyncio import Queue, TaskGroup
from coffeemaker.pybeansack.ducksack import Beansack as DuckSack
from coffeemaker.pybeansack.mongosack import Beansack as MongoSack
from coffeemaker.pybeansack.embedding import BeansackEmbeddings
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.utils import now
from coffeemaker.collectors import RSSFEEDS, SUBREDDITS
from coffeemaker.collectors.collector import AsyncCollector, HACKERNEWS, REDDIT, TIMEOUT
from coffeemaker.digestors import *
from pymongo import UpdateOne

log = logging.getLogger(__name__)

# # if a bean.text is less than 75 words, it is not worth indexing
# ALLOWED_BODY_LEN = 50   
# allowed_body = lambda bean: bean.text and len(bean.text.split()) >= ALLOWED_BODY_LEN
# # if a bean.text is more than 150 words, then assume it is a good candidate for indexing and does not need to be downloaded again
# NEEDS_DOWNLOAD_BODY_LEN = 150 
# needs_download = lambda bean: not bean.text or len(bean.text.split()) < NEEDS_DOWNLOAD_BODY_LEN
# # if a bean.summary is less than 150 words, it is not worth summarizing again
# NEEDS_SUMMARY_BODY_LEN = 150
# needs_summary = lambda bean: len(bean.text.split()) >= NEEDS_SUMMARY_BODY_LEN

MIN_WORDS_THRESHOLD = 100
MAX_CATEGORIES = 5
END_OF_STREAM = "END_OF_STREAM"

is_text_above_threshold = lambda bean: bean.text and len(bean.text.split()) >= MIN_WORDS_THRESHOLD
is_storable = lambda bean: bean.embedding and bean.summary # if there is no summary and embedding then no point storing
is_indexable = lambda bean: bean.text and (bean.kind == POST or is_text_above_threshold(bean)) # it has to have some text to be indexed
is_downloadable = lambda bean: not (bean.kind == POST or is_text_above_threshold(bean)) # if it is a post dont download it or if the body is large enough
storables = lambda beans: [bean for bean in beans if is_storable(bean)] if beans else beans 
indexables = lambda beans: [bean for bean in beans if is_indexable(bean)] if beans else beans
downloadables = lambda beans: [bean for bean in beans if is_downloadable(bean)] if beans else beans
merge_tags = lambda bean, new_tags: list({tag.lower(): tag for tag in ((bean.tags + new_tags) if (bean.tags and new_tags) else (bean.tags or new_tags))}.values())

def log_runtime(func):
    async def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = await func(*args, **kwargs)
        log.info("execution time", extra={"source": func.__name__, "num_items": int((datetime.now() - start_time).total_seconds())})
        return result
    return wrapper

class Orchestrator:
    run_start_time: datetime = None
    run_id: str = None
    download_queue: Queue = None
    index_queue: Queue = None

    scraper: AsyncCollector = None

    az_storage_conn_str: str = None
    remotesack: MongoSack = None
    localsack: DuckSack = None
    category_eps: float = None
    cluster_eps: float = None

    embedder: BeansackEmbeddings = None
    digestor: NewspaperDigestor = None

    def __init__(self, db_conn_str: str, storage_conn_str: str, working_dir: str, emb_path: str, llm_path: str, cat_eps: float, clus_eps: float): 
        self.embedder = BeansackEmbeddings(model_path=emb_path)
        self.digestor = LocalDigestor(model_path=llm_path) 

        self.az_storage_conn_str = storage_conn_str
        self.remotesack = MongoSack(db_conn_str, self.embedder)
        self.localsack = DuckSack(db_dir=working_dir+"/.db")
        self.category_eps = cat_eps
        self.cluster_eps = clus_eps    
        self.scraper = AsyncCollector()
        
    def new_beans(self, beans: list[Bean]) -> list[Bean]:
        if not beans: beans

        new_items = {}
        exists = self.localsack.exists(beans)
        for bean in beans:
            if bean.url not in exists:
                bean.id = bean.url
                bean.created = bean.created or bean.collected
                bean.tags = None
                bean.cluster_id = bean.url
                new_items[bean.url] = bean
        return list(new_items.values())
 
    def embed_beans(self, beans: list[Bean]) -> list[Bean]|None:   
        if not beans: return beans

        for bean in beans:
            try: bean.embedding = self.embedder.embed(bean.text)  
            except Exception as err:
                ic(err) 
                log.error("failed embedding", extra={"source": bean.url, "num_items": 1})
        return [bean for bean in beans if bean.embedding]

    def find_categories(self, bean: Bean):    
        return self.localsack.search_categories(bean.embedding, self.category_eps, MAX_CATEGORIES)

    def augment_beans(self, beans: list[Bean]) -> list[Bean]|None:
        if not beans: return beans
        
        for bean in beans:             
            try:
                bean.summary = bean.text # this is the default if things fail
                if is_text_above_threshold(bean):
                    digest = self.digestor.run(bean.text)
                    if digest:
                        bean.summary = digest.summary or bean.summary
                        bean.title = digest.title or bean.title
                        bean.tags = merge_tags(bean, digest.tags)
            except: log.error("failed augmenting", extra={"source": bean.url, "num_items": 1})
        
        return [bean for bean in beans if bean.summary]  
        
    def store_beans(self, beans: list[Bean]) -> list[Bean]|None:
        beans = storables(beans)
        if beans:
            self.localsack.store_beans(beans)
            self.remotesack.store_beans(beans)
        return beans
            
    def store_chatters(self, chatters: list[Chatter]) -> list[Chatter]|None:
        if chatters:
            self.localsack.store_chatters(chatters)
            # logger.info("stored chatters", extra={"source": chatters[0].channel or chatters[0].source, "num_items": len(chatters)})
        return chatters

    # current clustering approach
    # new beans (essentially beans without cluster gets priority for defining cluster)
    # for each bean without a cluster_id (essentially a new bean) fine the related beans within cluster_eps threshold
    # override their current cluster_id (if any) with the new bean's url
    # a bean is already in the update list then skip that one to include as a related item
    # if a new bean is found related to the query new bean, then it should be skipped for finding related beans for itself
    # keep running this loop through the whole collection until there is no bean without cluster id left
    # every time we find a cluster (a set of related beans) we add it to the update collection and return
    def find_clusters(self, urls: list[str]) -> dict[str, list[str]]:
        return {url: self.localsack.search_similar_beans(url, self.cluster_eps) for url in urls}

    def update_clusters(self, clusters: dict[str, list[str]]):    
        updates = []
        for key, val in clusters.items():
            updates.extend([UpdateOne({ K_ID: url }, { "$set": { K_CLUSTER_ID: key } }) for url in val])
        return self.remotesack.update_beans(updates)
        
    def cluster_beans(self, source: str, beans: list[Bean]):
        if not beans: return
        
        clusters = self.find_clusters([bean.url for bean in beans])
        num_items = self.update_clusters(clusters)
        log.info("clustered", extra={"source": source, "num_items": num_items})    
                
    def find_trend_ranks(self, urls: list[str] = None) -> list[ChatterAnalysis]|None:
        calculate_trend_score = lambda bean: 100*(bean.latest_comments or 0) + 10*(bean.latest_shares or 0) + (bean.latest_likes or 0)
        trends = self.localsack.get_latest_chatters(1, urls)
        for trend in trends:
            trend.trend_score = calculate_trend_score(trend)
        return trends

    def update_trend_ranks(self, trends: list[ChatterAnalysis]):
        updates = [UpdateOne(
            filter={K_ID: trend.url}, 
            update={
                "$set": {
                    K_LIKES: trend.likes,
                    K_COMMENTS: trend.comments,
                    K_SHARES: trend.shares,
                    "shared_in": trend.shared_in,
                    K_LATEST_LIKES: trend.latest_likes,
                    K_LATEST_COMMENTS: trend.latest_comments,
                    K_LATEST_SHARES: trend.latest_shares,
                    K_TRENDSCORE: trend.trend_score,
                    K_UPDATED: self.run_start_time      
                }
            }
        ) for trend in trends] 
        return self.remotesack.update_beans(updates)
        
    def trend_rank_beans(self, source: str, beans: list[Bean] = None):
        trends = self.find_trend_ranks([bean.url for bean in beans] if beans else None)
        if not trends: return

        num_items = self.update_trend_ranks(trends)
        log.info("trend ranked", extra={"source": source or self.run_id, "num_items": num_items})

    def cleanup(self):
        # TODO: add delete from localsack
        num_items = self.remotesack.delete_old(window=30)
        log.info("cleaned up", extra={"source": self.run_id, "num_items": num_items})

    def close(self):
        self.localsack.close()
        if self.az_storage_conn_str: self.localsack.backup_azblob(self.az_storage_conn_str)

    @log_runtime
    async def run_collections_async(self):
        with open(RSSFEEDS, 'r') as file:
            feed_urls = [line.strip() for line in file.readlines() if line.strip()]
        with open(SUBREDDITS, 'r') as file:
            subreddits = [line.strip() for line in file.readlines() if line.strip()] 

        async with TaskGroup() as tg:
            log.info("collecting", extra={"source": "rssfeed", "num_items": 0})
            [tg.create_task(self.collect_async(source, self.scraper.collect_rssfeed(source)), name=source) for source in feed_urls]
            log.info("collecting", extra={"source": REDDIT, "num_items": 0})
            [tg.create_task(self.collect_async(source, self.scraper.collect_subreddit(source)), name=source) for source in subreddits]
            log.info("collecting", extra={"source": HACKERNEWS, "num_items": 0})
            tg.create_task(self.collect_async(HACKERNEWS, self.scraper.collect_ychackernews()), name=HACKERNEWS)

    async def collect_async(self, source: str, collect: Awaitable[list[tuple[Bean, Chatter]]]):
        collection = await collect
        if not collection: return

        beans, chatters = zip(*collection)
        chatters = [chatter for chatter in chatters if chatter] if chatters else None
        if chatters: 
            self.localsack.store_chatters(chatters)
        
        beans = self.new_beans([bean for bean in beans if bean]) if beans else None
        needs_download = downloadables(beans)
        if needs_download:
            await self.download_queue.put((source, needs_download))
            beans = [bean for bean in beans if bean not in needs_download]
        
        if not beans: return
        
        await self.index_queue.put((source, beans))
        log.info("collected", extra={"source": source, "num_items": len(beans)})

    @log_runtime
    async def run_downloading_async(self):
        async with TaskGroup() as tg:
            while True:
                items = await self.download_queue.get()
                if items == END_OF_STREAM: break
                if not items: continue

                source, beans = items
                tg.create_task(self.download_async(source, beans), name="downloading: "+source)

    async def download_async(self, source: str, beans: list[Bean]):
        if not beans: return
        
        downloaded_beans = indexables(await self.scraper.collect_beans(beans, collect_metadata=True))
        if downloaded_beans:
            await self.index_queue.put((source, downloaded_beans))
            log.info("downloaded", extra={"source": source, "num_items": len(downloaded_beans)})
        if len(downloaded_beans) < len(beans):
            log.info("failed downloading", extra={"source": source, "num_items": len(beans)-len(downloaded_beans)})
        
    @log_runtime
    async def run_indexing_async(self):
        total_new_beans = 0
        ranking_tasks = []
        while True:
            items = await self.index_queue.get()
            if items == END_OF_STREAM: break
            if not items: continue
            
            source, beans = items
            if not beans: continue

            beans = self.store_beans(
                self.augment_beans(
                    self.embed_beans(
                        self.new_beans(beans))))  
            if not beans: continue
            
            total_new_beans += len(beans)
            log.info("stored", extra={"source": source, "num_items": len(beans)}) 
            ranking_tasks.append(asyncio.to_thread(self.cluster_beans, source, beans))
            ranking_tasks.append(asyncio.to_thread(self.trend_rank_beans, source, beans))

        await asyncio.gather(*ranking_tasks)
        log.info("run complete", extra={"source": self.run_id, "num_items": total_new_beans}) 

    # 1. schedule a clean up
    # 2. start collection
    # 3. await collection to finish
    # 4. schedule a trend ranking for existing beans
    # 5. run through the collection queue
    # 6. for each set of beans run through the following:
    # 7. embed beans
    # 8. augment beans
    # 9. store beans
    # 10. schedule clustering and then update clusters
    # 11. schedule trend ranking and then update trend ranking new beans 
    @log_runtime
    async def run_async(self):    
        await self.scraper.warmup()

        self.download_queue = Queue()
        self.index_queue = Queue()
        self.run_start_time = now()
        self.run_id = self.run_start_time.strftime("%Y-%m-%d %H")

        # start the collection and downloading parallelly
        # so that if there is a wait in the collection cycle, downloading can still ruh
        collections_task = asyncio.create_task(self.run_collections_async())
        downloading_task = asyncio.create_task(self.run_downloading_async())
        indexing_task = asyncio.create_task(self.run_indexing_async())

        await collections_task
        await self.download_queue.put(END_OF_STREAM)

        # i can run cleanup and trend ranking here because all these need is the collection, not the download
        # i don't need to put these on thread 
        # because by this time downloading and indexing are already running
        self.cleanup()
        self.trend_rank_beans(None, None)

        # wait for the downloading to finish then close the indexing queue and the scraper
        await downloading_task
        await self.index_queue.put(END_OF_STREAM)
        await self.scraper.close()

        await indexing_task

### NOTE: commenting out sync version for now
# def run_collection():   
#     collected = []
#     trigger_collection = lambda items: collected.append(extract_new(items))
#     log.info("collecting", extra={"source": "rssfeed", "num_items": 0})
#     rssfeed.collect(trigger_collection)
#     log.info("collecting", extra={"source": "ychackernews", "num_items": 0})
#     ychackernews.collect(trigger_collection)
#     log.info("collecting", extra={"source": "redditor", "num_items": 0})
#     redditor.collect(trigger_collection)
#     # logger().info("collecting|%s", "espresso")
#     # espresso.collect(sb_conn_str=sb_connection_str, store_func=_collect)
#     # TODO: add collection from nextdoor
#     # TODO: add collection from linkedin
#     return collected

# def run():
#     global run_id, run_start_time
#     run_start_time = now()
#     run_id = run_start_time.strftime("%Y-%m-%d %H")
    
#     collected = run_collection() 
#     cleanup()        
#     trend_rank_beans()
    
#     total_new_beans = 0
#     for beans in collected:
#         beans = store_beans(
#             augment_beans(
#                 embed_beans(
#                     new_beans(beans))))
#         if beans:
#             total_new_beans += len(beans)
#             cluster_beans(beans)                
#             trend_rank_beans(beans)
#     log.info("finished", extra={"source": run_id, "num_items": total_new_beans})