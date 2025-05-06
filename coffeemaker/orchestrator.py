import random
from typing import Awaitable
from icecream import ic
import logging
import asyncio
from asyncio import Queue
from coffeemaker.pybeansack.ducksack import Beansack as DuckSack
from coffeemaker.pybeansack.mongosack import Beansack as MongoSack
from coffeemaker.pybeansack.models import *
from coffeemaker.collectors.collector import APICollector, HACKERNEWS, REDDIT, WebScraper
from coffeemaker.nlp import digestors, embedders, utils
from pymongo import UpdateOne
import os

log = logging.getLogger(__name__)

QUEUE_BATCH_SIZE = 16*os.cpu_count()
END_OF_STREAM = "END_OF_STREAM"
MAX_CLUSTER_SIZE = int(os.getenv('MAX_CLUSTER_SIZE', 256))

# MIN_WORDS_THRESHOLD = 150
MIN_WORDS_THRESHOLD_FOR_DOWNLOADING = 200 # min words needed to not download the body
MIN_WORDS_THRESHOLD_FOR_INDEXING = 70 # mininum words needed to put it through indexing
MIN_WORDS_THRESHOLD_FOR_SUMMARY = 160 # min words needed to use the generated summary

is_text_above_threshold = lambda bean, threshold: bean.text and len(bean.text.split()) >= threshold
is_storable = lambda bean: bean.embedding and bean.summary # if there is no summary and embedding then no point storing
is_indexable = lambda bean: is_text_above_threshold(bean, MIN_WORDS_THRESHOLD_FOR_INDEXING) # it has to have some text and the text has to be large enough
is_downloadable = lambda bean: not (bean.kind == POST or is_text_above_threshold(bean, MIN_WORDS_THRESHOLD_FOR_DOWNLOADING)) # if it is a post dont download it or if the body is large enough
use_summary = lambda text: text and len(text.split()) >= MIN_WORDS_THRESHOLD_FOR_SUMMARY # if the body is large enough
storables = lambda beans: [bean for bean in beans if is_storable(bean)] if beans else beans 
indexables = lambda beans: [bean for bean in beans if is_indexable(bean)] if beans else beans
downloadables = lambda beans: [bean for bean in beans if is_downloadable(bean)] if beans else beans

def log_runtime(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        log.info("execution time", extra={"source": func.__name__, "num_items": int((datetime.now() - start_time).total_seconds())})
        return result
    return wrapper

def log_runtime_async(func):
    async def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = await func(*args, **kwargs)
        log.info("execution time", extra={"source": func.__name__, "num_items": int((datetime.now() - start_time).total_seconds())})
        return result
    return wrapper

def log_beans(level, msg, beans: list[Bean]):
    source_counts = {}
    for bean in beans:
        source_counts[bean.source] = source_counts.get(bean.source, 0) + 1
        if not bean.source.strip(): ic(bean.url)
    
    if level == logging.INFO: func = log.info
    elif level == logging.WARNING or logging.WARN: func = log.warning
    elif level == logging.ERROR: func = log.error
    else: func = log.debug
    [func(msg, extra={"source": source, "num_items": count}) for source, count in source_counts.items()]

def _read_sources(file: str) -> list[str]:
    with open(file, 'r') as file:
        return [line.strip() for line in file.readlines() if line.strip()]

async def _enqueue_beans(queue: Queue, source: str, beans: list[Bean]):
    if not beans: return
    await asyncio.gather(*[queue.put((source, beans[i:i+QUEUE_BATCH_SIZE])) for i in range(0, len(beans), QUEUE_BATCH_SIZE)])

class Orchestrator:
    run_id: str = None
    download_queue: Queue = None
    index_queue: Queue = None

    az_storage_conn_str: str = None
    remotesack: MongoSack = None
    localsack: DuckSack = None
    cluster_eps: float = None

    embedder = None
    digestor = None
    run_batch_time: datetime = None

    def __init__(self, 
        remote_db_conn_str: str, 
        local_db_path: str, 
        db_name: str, 
        backup_storage_conn_str: str = None, 
        embedder_path: str = os.getenv("EMBEDDER_PATH"), 
        embedder_context_len: int = int(os.getenv("EMBEDDER_CONTEXT_LEN", embedders.CONTEXT_LEN)),
        digestor_path: str = os.getenv("DIGESTOR_PATH"), 
        digestor_base_url: str = os.getenv("DIGESTOR_BASE_URL"),
        digestor_api_key: str = os.getenv("DIGESTOR_API_KEY"),
        digestor_context_len: int = int(os.getenv("DIGESTOR_CONTEXT_LEN", digestors.CONTEXT_LEN)),
        clus_eps: float = float(os.getenv("CLUSTER_EPS", 3))
    ): 
        self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        self.digestor = digestors.from_path(digestor_path, digestor_context_len, digestor_base_url, digestor_api_key, use_short_digest=lambda text: len(text.split()) < MIN_WORDS_THRESHOLD_FOR_SUMMARY)

        self.az_storage_conn_str = backup_storage_conn_str
        self.remotesack = MongoSack(remote_db_conn_str, db_name)
        self.localsack = DuckSack(local_db_path, db_name)
        self.cluster_eps = clus_eps            
        
    def new_beans(self, beans: list[Bean]) -> list[Bean]:
        if not beans: beans

        try:
            exists = self.localsack.exists(beans)
        except Exception as e:
            exists = [bean.url for bean in beans]
            ic("new_beans query failed", e) # if the batch fails then don't process any in that batch
            
        beans = list({bean.url: bean for bean in beans if bean.source and (bean.url not in exists)}.values())
        for bean in beans:
            bean.id = bean.url
            bean.created = bean.created or bean.collected
            bean.updated = self.run_batch_time or bean.updated or bean.collected
            bean.tags = None
            bean.cluster_id = bean.url
        return beans
 
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

                bean.summary = f"U:{bean.created.strftime('%Y-%m-%d')};"+digest.expr
                bean.regions = digest.regions
                bean.entities = digest.entities
                bean.categories = digest.categories
                bean.sentiments = digest.sentiments
                bean.tags = utils.merge_tags(digest.regions, digest.entities, digest.categories)
                # bean.gist = digest.gist
                # bean.categories = digest.domains
                # bean.entities = digest.entities
                # bean.locations = digest.regions
                # bean.topic = digest.topic
                # bean.summary = digest.summary or bean.text
                # bean.highlights = digest.takeways
                # bean.insight = digest.insight
                # bean.tags = bean.entities
                
        except Exception as e:
            # log.error("failed digesting", extra={"source": beans[0].source, "num_items": len(beans)})
            log_beans(logging.ERROR, "failed digesting", beans)
            ic(e.__class__.__name__, e)

        return [bean for bean in beans if bean.summary]  
        
    def store_beans(self, source: str, beans: list[Bean]) -> list[Bean]|None:
        beans = storables(beans)
        num_stored = 0
        if beans:
            self.localsack.store_beans(beans)
            num_stored = self.remotesack.store_beans(beans)
            log_beans(logging.INFO, "stored", beans)
            # log.info("stored", extra={"source": source or beans[0].source, "num_items": num_stored})  
        return beans if num_stored else None
            
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
        
    def cluster_beans(self, source:str, beans: list[Bean]):
        if not beans: return
        
        # clusters = self.find_clusters([bean.url for bean in beans])
        clusters = {bean.url: self.localsack.search_bean_cluster(bean.url, self.cluster_eps, limit=MAX_CLUSTER_SIZE) for bean in beans}
        clustered_count = self.update_clusters(clusters)
        if clustered_count > len(beans): log.info("clustered", extra={"source": source or beans[0].source, "num_items": clustered_count})
                
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
                    K_UPDATED: self.run_batch_time or trend.last_collected      
                }
            }
        ) for trend in trends if trend.trend_score] 
        return self.remotesack.update_beans(updates)
        
    def trend_rank_beans(self, source: str = None, beans: list[Bean] = None):
        trends = self.find_trend_ranks([bean.url for bean in beans] if beans else None)
        if not trends: return

        ranked_count = self.update_trend_ranks(trends)
        if ranked_count: log.info("trend ranked", extra={"source": source or self.run_id, "num_items": ranked_count}) 

    def cleanup(self):
        # TODO: add delete from localsack
        num_items =  self.remotesack.delete_old(window=60)
        log.info("cleaned up", extra={"source": self.run_id, "num_items": num_items})

    def close(self):
        self.localsack.close()
        if not self.az_storage_conn_str: return 
        try:
            self.localsack.backup_azblob(self.az_storage_conn_str)
            log.info("local db backed up", extra={"source": self.run_id, "num_items": 1})
        except Exception as e:
            log.error("local db backup failed", extra={"source": self.run_id, "num_items": 1})

    @log_runtime_async
    async def run_collections_async(self):
        scraper = APICollector()        
        current_directory = os.path.dirname(os.path.abspath(__file__))
        rssfeeds = _read_sources(os.path.join(current_directory, "collectors/rssfeedsources.txt"))
        subreddits = _read_sources(os.path.join(current_directory, "collectors/redditsources.txt"))
        random.shuffle(rssfeeds) # shuffling out to avoid failure of the same things
        random.shuffle(subreddits) # shuffling out to avoid failure of the same things
        
        # awaiting on each group so that os is not overwhelmed by sockets
        log.info("collecting", extra={"source": HACKERNEWS, "num_items": 1})
        await self.triage_collection(scraper.collect_ychackernews())
        log.info("collecting", extra={"source": REDDIT, "num_items": len(subreddits)})
        await self.triage_collection(scraper.collect_subreddits(subreddits))
        log.info("collecting", extra={"source": "rssfeeds", "num_items": len(rssfeeds)})
        await self.triage_collection(scraper.collected_rssfeeds(rssfeeds))

    async def triage_collection(self, collection: list[tuple[Bean, Chatter]]|Awaitable[list[tuple[Bean, Chatter]]]):
        if isinstance(collection, Awaitable): collection = await collection
        if not collection: return

        beans, chatters = zip(*collection)
        chatters = [chatter for chatter in chatters if chatter] if chatters else None
        if chatters: self.localsack.store_chatters(chatters)
        
        beans = self.new_beans([bean for bean in beans if bean]) if beans else None
        if not beans: return

        log_beans(logging.INFO, "triaged", beans)
        needs_download = downloadables(beans)
        await _enqueue_beans(self.download_queue, None, needs_download)   
        await _enqueue_beans(self.index_queue, None, [bean for bean in beans if bean not in needs_download])

    @log_runtime_async
    async def run_downloading_async(self):
        scraper = WebScraper()
        
        while True:
            items = await self.download_queue.get()
            if items == END_OF_STREAM: break
            if not items: continue

            source, beans = items
            await self.download_async(source, scraper.scrape_beans(beans, collect_metadata=True))
            self.download_queue.task_done()

    async def download_async(self, source: str, collect: Awaitable[list[Bean]]):
        beans = await collect
        if not beans: return
        
        downloaded_beans = indexables(beans)
        if len(downloaded_beans) < len(beans): 
            log_beans(logging.WARNING, "download failed", [bean for bean in beans if bean not in downloaded_beans])
        if downloaded_beans: 
            log_beans(logging.INFO, "downloaded", downloaded_beans)
            await _enqueue_beans(self.index_queue, source, downloaded_beans)            
        
    @log_runtime_async
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
            beans = await self.store_cluster_and_rank_beans_async(source or beans[0].source, beans)
            
            if beans: total_new_beans += len(beans)     
            self.index_queue.task_done()

        log.info("run complete", extra={"source": self.run_id, "num_items": total_new_beans}) 

    def cluster_and_rank_beans(self, source: str, beans: list[Bean]):
        if not beans: return

        self.cluster_beans(source, beans)
        self.trend_rank_beans(source, beans)        

    async def store_cluster_and_rank_beans_async(self, source: str, beans: list[Bean]):
        beans = self.store_beans(source, beans) 
        if beans:
            await asyncio.gather(*[
                asyncio.to_thread(self.cluster_beans, source, beans),
                asyncio.to_thread(self.trend_rank_beans, source, beans)
            ])
        return beans
         
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
    @log_runtime_async
    async def run_async(self):    
        # instantiate the run specific work
        self._init_run()

        # 1. start all the scrapers
        # 2. once the chatter collection is done
        # 3. clean up and trend rank the database
        # 4. then kick off indexing
        # 5. wait for the downloading to finish and then put the end of stream for indexing
        # 6. wait for indexing to finish

        await self.run_collections_async()
        await self.download_queue.put(END_OF_STREAM)
        # self.cleanup()
        self.trend_rank_beans()

        indexing_task = asyncio.create_task(self.run_indexing_async()) 
        await self.run_downloading_async()
        await self.index_queue.put(END_OF_STREAM)
        await indexing_task
        
    def _init_run(self):
        self.download_queue = Queue()
        self.index_queue = Queue()
        self.run_id = datetime.now().strftime("%Y-%m-%d %H")
        self.run_batch_time = datetime.now()
  