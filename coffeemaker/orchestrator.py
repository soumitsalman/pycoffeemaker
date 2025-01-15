
import random
from icecream import ic
import logging
logger = logging.getLogger("orchestrator")

# import queue
import asyncio
from typing import Callable, Coroutine
import newspaper
import queue
from coffeemaker.pybeansack.ducksack import Beansack as DuckSack
from coffeemaker.pybeansack.mongosack import Beansack as MongoSack
from coffeemaker.pybeansack.embedding import BeansackEmbeddings
from coffeemaker.pybeansack.datamodels import *
from coffeemaker.pybeansack.utils import now
from coffeemaker.collectors import espresso, individual, rssfeed, ychackernews, redditor
from coffeemaker.digestors import *
from datetime import datetime as dt
from pymongo import UpdateOne

# if a bean.text is less than 75 words, it is not worth indexing
ALLOWED_BODY_LEN = 50   
allowed_body = lambda bean: bean.text and len(bean.text.split()) >= ALLOWED_BODY_LEN
# if a bean.text is more than 150 words, then assume it is a good candidate for indexing and does not need to be downloaded again
NEEDS_DOWNLOAD_BODY_LEN = 150 
needs_download = lambda bean: not bean.text or len(bean.text.split()) < NEEDS_DOWNLOAD_BODY_LEN
# if a bean.summary is less than 150 words, it is not worth summarizing again
NEEDS_SUMMARY_BODY_LEN = 150
needs_summary = lambda bean: len(bean.text.split()) >= NEEDS_SUMMARY_BODY_LEN

MAX_CATEGORIES = 10

run_id: str = None
run_start_time: datetime = None

collected_queue: queue.Queue = None
collected_queue_async: asyncio.Queue = None

remotesack: MongoSack = None
localsack: DuckSack = None
category_eps: float = None
cluster_eps: float = None

embedder: BeansackEmbeddings = None
digestor: NewspaperDigestor = None
models_dir: str = None

def initialize(db_conn_str: str, sb_conn_str: str, working_dir: str, emb_path: str, llm_path: str, cat_eps: float, clus_eps: float):
    global collected_queue, collected_queue_async
    collected_queue = queue.Queue()
    collected_queue_async = asyncio.Queue()    
    
    # global sb_connection_str  
    # sb_connection_str = sb_conn_str 
    
    global embedder, digestor
    embedder = BeansackEmbeddings(model_path=emb_path, context_len=4096)
    digestor = LocalDigestor(model_path=llm_path, context_len=8192) 

    global remotesack, localsack, category_eps, cluster_eps        
    remotesack = MongoSack(db_conn_str, embedder)
    localsack = DuckSack(db_dir=working_dir+"/.db")
    category_eps = cat_eps
    cluster_eps = clus_eps    
    
def new_beans(beans: list[Bean]) -> list[Bean]:
    if beans:
        exists = localsack.exists(beans)
        new_items = {}
        for bean in beans:
            if bean.url not in exists:
                bean.id = bean.url
                bean.created = bean.created or bean.collected
                bean.tags = None
                bean.cluster_id = bean.url
                new_items[bean.url] = bean
        return list(new_items.values())
    
def extract_new(items: list[Bean]|list[tuple[Bean, Chatter]]) -> tuple[list[Bean]|None, list[Chatter]|None]:
    beans, chatters = None, None
    if items:
        if isinstance(items[0], Bean):
            beans = items
        else:
            beans = [chunk[0] for chunk in items]
            chatters = [chunk[1] for chunk in items]                               
    return new_beans(beans), chatters

def queue_for_indexing(items: list[Bean]|list[tuple[Bean, Chatter]]):
    beans, chatters = extract_new(items)      
    if chatters:
        localsack.store_chatters(chatters)
    if beans := deep_collect(beans):        
        collected_queue.put_nowait(beans)   
        logger.info("collected", extra={"source": beans[0].source, "num_items": len(beans)})  
        
async def queue_for_indexing_async(items: Coroutine):
    beans, chatters = extract_new(await items)  
    if chatters:
        localsack.store_chatters(chatters)
    if beans := await asyncio.to_thread(deep_collect, beans):
        await collected_queue_async.put_nowait(beans)   
        logger.info("collected", extra={"source": beans[0].source, "num_items": len(beans)})      
    
def deep_collect(beans: list[Bean]) -> list[Bean]:
    new_beans, beans = [], beans or []
    for bean in beans:
        if (bean.kind in [NEWS, BLOG]) and (not bean.image_url or needs_download(bean)):
            res = individual.collect_url(bean.url)
            if res:
                bean.image_url = bean.image_url or res.top_image
                bean.source = individual.site_name(res) or bean.source
                bean.title = bean.title or res.title
                bean.created = bean.created or res.publish_date
                bean.text = (res.text if len(res.text) > len(bean.text or "") else bean.text).strip()            
        if allowed_body(bean):
            new_beans.append(bean)          
    return new_beans

merge_tags = lambda bean, new_tags: list({tag.lower(): tag for tag in ((bean.tags + new_tags) if (bean.tags and new_tags) else (bean.tags or new_tags))}.values())

def embed_beans(beans: list[Bean]) -> list[Bean]|None:   
    beans = beans or []
    for bean in beans:
        try:
            bean.embedding = embedder.embed(bean.digest())  
        except:
            logger.error("failed embedding", extra={"source": bean.source, "num_items": 1})
    return [bean for bean in beans if bean.embedding]

def find_categories(bean: Bean):    
    return localsack.search_categories(bean.embedding, category_eps, MAX_CATEGORIES)

def augment_beans(beans: list[Bean]) -> list[Bean]|None:
    beans = beans or []
    for bean in beans:             
        try:
            digest = digestor.run(bean.text)
            if digest:
                bean.summary = digest.summary or bean.summary or bean.text if needs_summary(bean) else bean.text
                bean.title = digest.title or bean.title
                bean.tags = merge_tags(bean, digest.tags)
        except:
            logger.error("failed augmenting", extra={"source": bean.source, "num_items": 1})
    return [bean for bean in beans if bean.summary]  
    
def store_beans(beans: list[Bean]) -> list[Bean]|None:
    if beans:
        localsack.store_beans(beans)
        remotesack.store_beans(beans)
        logger.info("stored", extra={"source": beans[0].source, "num_items": len(beans)})  
    return beans
        
def store_chatters(chatters: list[Chatter]) -> list[Chatter]|None:
    if chatters:
        localsack.store_chatters(chatters)
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
def find_clusters(urls: list[str]) -> dict[str, list[str]]:
    return {url: localsack.search_similar_beans(url, cluster_eps) for url in urls}

def update_clusters(clusters: dict[str, list[str]]):    
    updates = []
    for key, val in clusters.items():
        updates.extend([UpdateOne({ K_ID: url }, { "$set": { K_CLUSTER_ID: key } }) for url in val])
    return remotesack.update_beans(updates)
    
def cluster_beans(beans: list[Bean]):
    if beans:
        clusters = find_clusters([bean.url for bean in beans])
        num_items = update_clusters(clusters)
        logger.info("clustered", extra={"source": beans[0].source, "num_items": num_items})    
            
def find_trend_ranks(urls: list[str] = None) -> list[ChatterAnalysis]|None:
    calculate_trend_score = lambda bean: 100*(bean.latest_comments or 0) + 10*(bean.latest_shares or 0) + (bean.latest_likes or 0)
    trends = localsack.get_latest_chatters(1, urls)
    for trend in trends:
        trend.trend_score = calculate_trend_score(trend)
    return trends

def update_trend_ranks(trends: list[ChatterAnalysis], update_time: datetime = now()):
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
                K_UPDATED: update_time      
            }
        }
    ) for trend in trends] 
    return remotesack.update_beans(updates)
    
def trend_rank_beans(beans: list[Bean] = None):
    trends = find_trend_ranks([bean.url for bean in beans] if beans else None)
    if trends:
        num_items = update_trend_ranks(trends, run_start_time or now())
        logger.info("trend ranked", extra={"source": beans[0].source if beans else run_id, "num_items": num_items})

def cleanup():
    # TODO: add delete from localsack
    num_items = remotesack.delete_old(window=30)
    logger.info("cleaned up", extra={"source": run_id, "num_items": num_items})

def close():
    localsack.close()
    
def run_collection():      
    logger.info("collecting", extra={"source": "rssfeed", "num_items": 0})
    rssfeed.collect(queue_for_indexing)
    logger.info("collecting", extra={"source": "ychackernews", "num_items": 0})
    ychackernews.collect(queue_for_indexing)
    logger.info("collecting", extra={"source": "redditor", "num_items": 0})
    redditor.collect(queue_for_indexing)
    # logger().info("collecting|%s", "espresso")
    # espresso.collect(sb_conn_str=sb_connection_str, store_func=_collect)
    # TODO: add collection from nextdoor
    # TODO: add collection from linkedin

async def run_collection_async():    
    tasks = []    
    logger.info("collecting", extra={"source": "rssfeed", "num_items": 0})
    tasks.append(asyncio.create_task(rssfeed.collect_async(queue_for_indexing_async)))
    logger.info("collecting", extra={"source": "ychackernews", "num_items": 0})
    tasks.append(asyncio.create_task(ychackernews.collect_async(queue_for_indexing_async)))
    logger.info("collecting", extra={"source": "redditor", "num_items": 0})
    tasks.append(asyncio.create_task(redditor.collect_async(queue_for_indexing_async)))
    await asyncio.gather(*tasks)
  
# 1. schedule a clean up
# 2. start collection
# 3. await collection to finish
# 4. schedule a trend ranking for existing beans
# 5. run through the collection queue
# 6. for each set of beans run through the following:
# 7. index beans
# 8. augment beans
# 9. store beans
# 10. schedule clustering and then update clusters
# 11. schedule trend ranking and then update trend ranking new beans
    
def run():
    global run_id, run_start_time
    run_start_time = now()
    run_id = run_start_time.strftime("%Y-%m-%d %H")
    
    run_collection() 
    cleanup()        
    trend_rank_beans()
    
    total_new_beans = 0
    while not collected_queue.empty():
        beans = store_beans(
            augment_beans(
                embed_beans(
                    new_beans(collected_queue.get_nowait()))))
        collected_queue.task_done()
        if beans:
            total_new_beans += len(beans)
            cluster_beans(beans)                
            trend_rank_beans(beans)
    logger.info("finished", extra={"source": run_id, "num_items": total_new_beans})


async def run_async() -> int:
    global run_id, run_start_time
    run_start_time = now()
    run_id = run_start_time.strftime("%Y-%m-%d %H")  

    await run_collection_async()   
    cleanup()        
    trend_rank_beans()
    
    total_new_beans = 0
    while not collected_queue_async.empty():
        beans = store_beans(
            augment_beans(
                embed_beans(
                    new_beans(await collected_queue_async.get_nowait()))))
        collected_queue_async.task_done()
        if beans:
            total_new_beans += len(beans)
            cluster_beans(beans)                
            trend_rank_beans(beans)
    logger.info("finished", extra={"source": run_id, "num_items": total_new_beans})   
    
    
    
