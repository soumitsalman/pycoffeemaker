import random
from typing import Awaitable
from icecream import ic
import logging
import asyncio
from asyncio import Queue
from coffeemaker.pybeansack.ducksack import Beansack as DuckSack
from coffeemaker.pybeansack.mongosack import Beansack as MongoSack
from coffeemaker.pybeansack.models import *
from coffeemaker.collectors.collector import AsyncCollector, HACKERNEWS, REDDIT
from coffeemaker.nlp import digestors, embedders
from pymongo import UpdateOne
import os

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

QUEUE_BATCH_SIZE = 100
END_OF_STREAM = "END_OF_STREAM"
MAX_CLUSTER_SIZE=20

# MIN_WORDS_THRESHOLD = 150
MIN_WORDS_THRESHOLD_FOR_SUMMARY = 150 # min words needed to use the generated summary
MIN_WORDS_THRESHOLD_FOR_DOWNLOADING = 200 # min words needed to not download the body
MIN_WORDS_THRESHOLD_FOR_INDEXING = 70 # mininum words needed to put it through indexing

is_text_above_threshold = lambda bean, threshold: bean.text and len(bean.text.split()) >= threshold
is_storable = lambda bean: bean.embedding and bean.summary # if there is no summary and embedding then no point storing
is_indexable = lambda bean: is_text_above_threshold(bean, MIN_WORDS_THRESHOLD_FOR_INDEXING) # it has to have some text and the text has to be large enough
is_downloadable = lambda bean: not (bean.kind == POST or is_text_above_threshold(bean, MIN_WORDS_THRESHOLD_FOR_DOWNLOADING)) # if it is a post dont download it or if the body is large enough
is_summaryable = lambda bean: bean.kind != POST or is_text_above_threshold(bean, MIN_WORDS_THRESHOLD_FOR_SUMMARY) # if the body is large enough
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

def _read_sources(file: str) -> list[str]:
    with open(file, 'r') as file:
        return [line.strip() for line in file.readlines() if line.strip()]

async def _enqueue_beans(queue: Queue, source: str, beans: list[Bean]):
    for i in range(0, len(beans), QUEUE_BATCH_SIZE):
        batch = beans[i:i+QUEUE_BATCH_SIZE]
        await queue.put((source, batch))

class Orchestrator:
    run_id: str = None
    download_queue: Queue = None
    index_queue: Queue = None

    scraper: AsyncCollector = None

    az_storage_conn_str: str = None
    remotesack: MongoSack = None
    localsack: DuckSack = None
    cluster_eps: float = None

    embedder = None
    digestor = None

    def __init__(self, remote_db_conn_str: str, local_db_path: str, storage_conn_str: str, emb_path: str, llm_path: str, clus_eps: float): 
        self.embedder = embedders.from_path(emb_path)
        self.digestor = digestors.from_path(llm_path)

        self.az_storage_conn_str = storage_conn_str
        self.remotesack = MongoSack(remote_db_conn_str, os.getenv("DB_NAME"))
        self.localsack = DuckSack(local_db_path, os.getenv("DB_NAME"))
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

        try:
            embeddings = self.embedder.embed([bean.digest() for bean in beans])
            for bean, embedding in zip(beans, embeddings):
                bean.embedding = embedding
        except Exception as e:
            log.error("failed embedding", extra={"source": beans[0].source, "num_items": len(beans)})
            ic(e.__class__.__name__, e) # NOTE: this is for local debugging

        return [bean for bean in beans if bean.embedding]

    # def find_baristas(self, bean: Bean):    
    #     return self.localsack.search_categories(bean.embedding, self.category_eps)

    def digest_beans(self, beans: list[Bean]) -> list[Bean]|None:
        if not beans: return beans

        try:
            digests = self.digestor.run_batch([bean.text for bean in beans])
            for bean, digest in zip(beans, digests):
                if not digest: 
                    log.error("failed digesting", extra={"source": bean.url, "num_items": 1})
                    continue

                bean.summary = digest.summary if is_summaryable(bean) else bean.text
                bean.title = digest.title or bean.title
                bean.tags = merge_tags(bean, digest.tags)
        except Exception as e:
            log.error("failed digesting", extra={"source": beans[0].source, "num_items": len(beans)})
            ic(e.__class__.__name__, e)

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
    # def find_clusters(self, urls: list[str]) -> dict[str, list[str]]:
    #     return {url: self.localsack.search_bean_cluster(url, self.cluster_eps, limit=100) for url in urls}

    def update_clusters(self, clusters: dict[str, list[str]]):    
        updates = []
        for key, val in clusters.items():
            updates.extend([UpdateOne({ K_ID: url }, { "$set": { K_CLUSTER_ID: key } }) for url in val])
        return self.remotesack.update_beans(updates)
        
    def cluster_beans(self, beans: list[Bean]):
        if not beans: return
        
        # clusters = self.find_clusters([bean.url for bean in beans])
        clusters = {bean.url: self.localsack.search_bean_cluster(bean.url, self.cluster_eps, limit=MAX_CLUSTER_SIZE) for bean in beans}
        return self.update_clusters(clusters)
                
    def find_trend_ranks(self, urls: list[str] = None) -> list[ChatterAnalysis]|None:
        calculate_trend_score = lambda chatter_delta: 100*(chatter_delta.comments_change or 0) + 10*(chatter_delta.shares_change or 0) + (chatter_delta.likes_change or 0)
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
                    K_LATEST_LIKES: trend.likes_change,
                    K_LATEST_COMMENTS: trend.comments_change,
                    K_LATEST_SHARES: trend.shares_change,
                    K_TRENDSCORE: trend.trend_score,
                    K_UPDATED: trend.last_collected      
                }
            }
        ) for trend in trends if trend.trend_score] 
        return self.remotesack.update_beans(updates)
        
    def trend_rank_beans(self, beans: list[Bean] = None):
        trends = self.find_trend_ranks([bean.url for bean in beans] if beans else None)
        if not trends: return

        return self.update_trend_ranks(trends)

    def cleanup(self):
        # TODO: add delete from localsack
        num_items =  self.remotesack.delete_old(window=30)
        log.info("cleaned up", extra={"source": self.run_id, "num_items": num_items})

    def close(self):
        self.localsack.close()
        if self.az_storage_conn_str: 
            self.localsack.backup_azblob(self.az_storage_conn_str)
            log.info("local db backed up", extra={"source": self.run_id, "num_items": 1})

    @log_runtime
    async def run_collections_async(self):
        current_directory = os.path.dirname(os.path.abspath(__file__))
        rssfeeds = _read_sources(os.path.join(current_directory, "collectors/rssfeedsources.txt"))
        subreddits = _read_sources(os.path.join(current_directory, "collectors/redditsources.txt"))
        random.shuffle(rssfeeds) # shuffling out to avoid failure of the same things
        random.shuffle(subreddits) # shuffling out to avoid failure of the same things

        # awaiting on each group so that os is not overwhelmed by sockets
        log.info("collecting", extra={"source": REDDIT, "num_items": len(subreddits)})
       
        await asyncio.gather(*[self.collect_async(source, self.scraper.collect_subreddit(source)) for source in subreddits])
        log.info("collecting", extra={"source": HACKERNEWS, "num_items": 1})
        await self.collect_async(HACKERNEWS, self.scraper.collect_ychackernews())
        log.info("collecting", extra={"source": "rssfeed", "num_items": len(rssfeeds)})
        
        await asyncio.gather(*[self.collect_async(source, self.scraper.collect_rssfeed(source)) for source in rssfeeds])
        
        await self.download_queue.put(END_OF_STREAM)

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
            await _enqueue_beans(self.download_queue, source, needs_download)
            beans = [bean for bean in beans if bean not in needs_download]
        if not beans: return

        await _enqueue_beans(self.index_queue, source, beans)
        log.info("collected", extra={"source": source, "num_items": len(beans)})

    @log_runtime
    async def run_downloading_async(self):
        while True:
            items = await self.download_queue.get()
            if items == END_OF_STREAM: break
            if not items: continue

            source, beans = items
            await self.download_async(source, beans)
            self.download_queue.task_done()

        await self.index_queue.put(END_OF_STREAM)

    async def download_async(self, source: str, beans: list[Bean]):
        if not beans: return
        
        downloaded_beans = indexables(await self.scraper.collect_beans(beans, collect_metadata=True))
        if downloaded_beans:
            await self.index_queue.put((source, downloaded_beans))
            log.info("downloaded", extra={"source": source, "num_items": len(downloaded_beans)})
        if len(downloaded_beans) < len(beans):
            log.info("download failed", extra={"source": source, "num_items": len(beans)-len(downloaded_beans)})
        
    @log_runtime
    async def run_indexing_async(self):
        total_new_beans = 0
        while True:
            items = await self.index_queue.get()
            if items == END_OF_STREAM: break
            if not items: continue
            
            source, beans = items
            if not beans: continue

            # changing the sequence so that it embeds the digest/summary instead of the whole text
            # since the digest/summary is smaller and got generated through a text-gen model so it is more clean. 
            beans = self.new_beans(indexables(beans))
            beans = self.embed_beans(self.digest_beans(beans))
            beans = self.store_beans(beans) 

            if not beans: continue

            total_new_beans += len(beans)
            log.info("stored", extra={"source": source, "num_items": len(beans)})  
            self.cluster_and_rank_beans(source, beans)            

            self.index_queue.task_done()

        log.info("run complete", extra={"source": self.run_id, "num_items": total_new_beans}) 

    def cluster_and_rank_beans(self, source: str, beans: list[Bean]):
        if not beans: return

        clustered_count = self.cluster_beans(beans)
        if clustered_count > len(beans): log.info("clustered", extra={"source": source, "num_items": clustered_count})
        ranked_count = self.trend_rank_beans(beans)
        if ranked_count: log.info("trend ranked", extra={"source": source, "num_items": ranked_count})  

    async def cluster_and_rank_beans_async(self, source: str, beans: list[Bean]):
        if not beans: return

        loop = asyncio.get_event_loop()
        clustered_count = await loop.run_in_executor(None, self.cluster_beans, beans)
        if clustered_count > len(beans): log.info("clustered", extra={"source": source, "num_items": clustered_count})
        ranked_count = await loop.run_in_executor(None, self.trend_rank_beans, beans)
        if ranked_count: log.info("trend ranked", extra={"source": source, "num_items": ranked_count})  

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
        # instantiate the run specific work
        self.download_queue = Queue()
        self.index_queue = Queue()
        self.run_id = datetime.now().strftime("%Y-%m-%d %H")

        # 1. start all the scrapers
        # 2. once the chatter collection is done
        # 3. clean up and trend rank the database
        # 4. then kick off indexing
        # 5. wait for the downloading to finish and then put the end of stream for indexing
        # 6. wait for indexing to finish
        # await self.scraper.web_crawler.start() # this is very important otherwise people die
        await self.run_collections_async()        

        # clean up the old stuff from the db before adding new crap
        self.cleanup()
        ranked_count = self.trend_rank_beans()
        log.info("trend ranked", extra={"source": self.run_id, "num_items": ranked_count})

        downloading_task = asyncio.create_task(self.run_downloading_async())
        indexing_task = asyncio.create_task(self.run_indexing_async())       

        await downloading_task
        await self.index_queue.put(END_OF_STREAM)
        await self.scraper.close() # this is for closing out the open sessions
        await indexing_task
  