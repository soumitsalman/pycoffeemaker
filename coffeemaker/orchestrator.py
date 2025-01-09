from icecream import ic
import logging
logger = logging.getLogger("orchestrator")

# import queue
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
import random

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

# user_req_queue_conn_str: str = None
collected_queue: queue.Queue = None

remotesack: MongoSack = None
localsack: DuckSack = None
category_eps: float = None
cluster_eps: float = None

embedder: BeansackEmbeddings = None
digestor: NewspaperDigestor = None
models_dir: str = None

def initialize(db_conn_str: str, sb_conn_str: str, working_dir: str, emb_path: str, llm_path: str, cat_eps: float, clus_eps: float):
    global collected_queue
    collected_queue = queue.Queue()
    
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
        return list({bean.url: bean for bean in beans if (bean.url not in exists)}.values())

def collect_beans(beans: list):
    if beans := download_beans(new_beans(beans)):        
        collected_queue.put_nowait(beans)        
        logger.info("collected", extra={"source": beans[0].source, "num_items": len(beans)})
    
def collect_chatters(chatters: list):
    if chatters:
        store_chatters(chatters)
        logger.info("collected chatters", extra={"source": chatters[0].channel or chatters[0].source, "num_items": len(chatters)})

def download_beans(beans: list[Bean]) -> list[Bean]:
    if beans:
        for bean in beans:   
            # reset some of the fields 
            if (bean.kind in [NEWS, BLOG]) and (not bean.image_url or needs_download(bean)):
                res = individual.load_from_url(bean.url)
                if res:
                    bean.image_url = bean.image_url or res.top_image
                    bean.source = individual.site_name(res) or bean.source
                    bean.title = bean.title or res.title
                    
                    if not bean.created and res.publish_date:
                        bean.created = dt.fromtimestamp(res.publish_date.timestamp())
                    if res.text:  # this is a decent indicator if loading from the url even worked 
                        bean.text = (res.text if len(res.text) > len(bean.text or "") else bean.text).strip()

            bean.id = bean.url
            bean.created = bean.created or bean.collected
            bean.tags = None

        return [bean for bean in beans if allowed_body(bean)]                

_merge_tags = lambda bean, new_tags: list({tag.lower(): tag for tag in ((bean.tags + new_tags) if (bean.tags and new_tags) else (bean.tags or new_tags))}.values())

def index_beans(beans: list[Bean]) -> list[Bean]|None:   
    if beans:
        for bean in beans:
            try:
                bean.embedding = remotesack.embedder.embed(bean.digest())   
                # NOTE: disabling categories for now
                # bean.tags = _merge_tags(bean, _find_categories(bean))
                bean.cluster_id = bean.url # this is a temporary cluster id for beans that are not clustered yet
            except:
                logger.error("failed indexing", extra={"source": bean.source, "num_items": 1})
        return [bean for bean in beans if bean.embedding]

# def _find_categories(bean: Bean):    
#     return localsack.search_categories(bean.embedding, category_eps, MAX_CATEGORIES)

def augment_beans(beans: list[Bean]) -> list[Bean]|None:
    if beans:
        for bean in beans:             
            try:
                digest = digestor.run(bean.text)
                if digest:
                    bean.summary = digest.summary or bean.summary or bean.text
                    bean.title = digest.title or bean.title
                    bean.tags = _merge_tags(bean, digest.tags)
            except:
                logger.error("failed augmenting", extra={"source": bean.source, "num_items": 1})
        return [bean for bean in beans if bean.summary]  
    
def store_beans(beans: list[Bean]) -> list[Bean]|None:
    if beans:
        localsack.store_beans(beans)
        remotesack.store_beans(beans)
    return beans
        
def store_chatters(chatters: list[Chatter]) -> list[Chatter]|None:
    if chatters:
        localsack.store_chatters(chatters)
    return chatters

# current clustering approach
# new beans (essentially beans without cluster gets priority for defining cluster)
# for each bean without a cluster_id (essentially a new bean) fine the related beans within cluster_eps threshold
# override their current cluster_id (if any) with the new bean's url
# a bean is already in the update list then skip that one to include as a related item
# if a new bean is found related to the query new bean, then it should be skipped for finding related beans for itself
# keep running this loop through the whole collection until there is no bean without cluster id left
# every time we find a cluster (a set of related beans) we add it to the update collection and return
def cluster_beans(urls: list[str]) -> dict[str, list[str]]:
    return {url: localsack.search_similar_beans(url, cluster_eps) for url in urls}

def update_clusters(clusters: dict[str, list[str]]):    
    if clusters:
        updates = []
        for key, val in clusters.items():
            updates.extend([UpdateOne({ K_ID: url }, { "$set": { K_CLUSTER_ID: key } }) for url in val])
        return remotesack.update_beans(updates)
            
def trend_rank_beans(urls: list[str] = None) -> list[ChatterAnalysis]|None:
    calculate_trend_score = lambda bean: 100*bean.latest_comments + 10*bean.latest_shares + bean.latest_likes
    trends = localsack.get_latest_chatters(1, urls)
    for trend in trends:
        trend.trend_score = calculate_trend_score(trend)
    return trends

def update_trend_rank(trends: list[ChatterAnalysis], update_time: datetime = now()):
    if trends:
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

def cleanup() -> int:
    # logger().info("cleaning up")
    # TODO: add delete from localsack
    return remotesack.delete_old(window=30)
    # logger().info("cleaned up|beans|%d", bcount)
    # logger().info("cleaned up|chatters|%d", ccount)

def close():
    localsack.close()
    
def run_collection():    
    logger.info("collecting", extra={"source": "rssfeed", "num_items": 0})
    rssfeed.collect(store_beans=collect_beans)
    logger.info("collecting", extra={"source": "ychackernews", "num_items": 0})
    ychackernews.collect(store_beans=collect_beans, store_chatters=collect_chatters)
    logger.info("collecting", extra={"source": "redditor", "num_items": 0})
    redditor.collect(store_beans=collect_beans, store_chatters=collect_chatters)
    # logger().info("collecting|%s", "espresso")
    # espresso.collect(sb_conn_str=sb_connection_str, store_func=_collect)
    # TODO: add collection from nextdoor
    # TODO: add collection from linkedin
    
def run(batch_id: str) -> int:
    start_time = dt.now()
    total_new_beans = 0
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
    try:
        num_items = cleanup()
        logger.info("cleaned up", extra={"source": batch_id, "num_items": num_items})
        
        run_collection()
        
        num_items = update_trend_rank(trend_rank_beans(None), start_time)
        logger.info("trend ranked", extra={"source": batch_id, "num_items": num_items})
        
        while not collected_queue.empty():
            beans = store_beans(
                augment_beans(
                    index_beans(
                        new_beans(collected_queue.get()))))
            if beans:
                total_new_beans += len(beans)
                urls = [bean.url for bean in beans]
                logger.info("stored", extra={"source": beans[0].source, "num_items": len(beans)})            
                
                num_items = update_clusters(cluster_beans(urls)) or 0
                logger.info("clustered", extra={"source": beans[0].source, "num_items": num_items})
                
                num_items = update_trend_rank(trend_rank_beans(urls), start_time) or 0
                logger.info("trend ranked", extra={"source": beans[0].source, "num_items": num_items})
    except Exception as e:
        logger.error("failed", extra={"source": batch_id, "num_items": 0})
        print(e.with_traceback())
    finally:
        close()
    return total_new_beans