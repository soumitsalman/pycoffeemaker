from itertools import chain
import logging
from icecream import ic
from pybeansack.ducksack import Beansack as DuckSack
from pybeansack.mongosack import Beansack as MongoSack
from pybeansack.embedding import BeansackEmbeddings
from pybeansack.datamodels import *
from pybeansack.utils import now
from pymongo import MongoClient, UpdateMany, UpdateOne
from pymongo.collection import Collection
from collectors import espresso, individual, rssfeed, ychackernews, redditor
from coffeemaker.digestors import *
import persistqueue
import queue
from sklearn.metrics.pairwise import euclidean_distances
import numpy as np
from datetime import datetime as dt

logger = lambda: logging.getLogger("orchestrator")

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

# index queue is a non persistence queue because at this point nothing is stored. things need to be stored before they can be processed further
# if storing fails in this round, it can succeed in the next
index_queue: queue.Queue = None 
# trend_queue: persistqueue.Queue = None
cluster_queue: persistqueue.Queue = None

remotesack: MongoSack = None
sb_connection_str: str = None
localsack: DuckSack = None
category_eps: float = None
cluster_eps: float = None

digestor: NewspaperDigestor = None
models_dir: str = None

def initialize(db_conn_str: str, sb_conn_str: str, working_dir: str, emb_path: str, llm_base_url: str, llm_api_key: str, llm_path: str, cat_eps: float, clus_eps: float):
    queue_dir=working_dir+"/.processingqueue"    

    global index_queue, trend_queue, cluster_queue

    index_queue = queue.Queue() # local queue
    cluster_queue = persistqueue.Queue(queue_dir+"/cluster", tempdir=queue_dir) 

    global sb_connection_str  
    sb_connection_str = sb_conn_str 

    global remotesack, categorystore, baristastore, localsack, category_eps, cluster_eps        
    remotesack = MongoSack(db_conn_str, BeansackEmbeddings(model_path=emb_path, context_len=4096))
    localsack = DuckSack(db_dir=working_dir+"/.db")
    category_eps = cat_eps
    cluster_eps = clus_eps

    global digestor
    digestor = RemoteDigestor(base_url=llm_base_url, api_key=llm_api_key, model_name=llm_path, context_len=8192) \
        if llm_base_url else \
            LocalDigestor(model_path=llm_path, context_len=8192) 

def run_collection():
    logger().info("collecting|%s", "rssfeed")
    rssfeed.collect(store_beans=_collect_beans)
    logger().info("collecting|%s", "ychackernews")
    ychackernews.collect(store_beans=_collect_beans, store_chatters=_collect_chatters)
    logger().info("collecting|%s", "redditor")
    redditor.collect(store_beans=_collect_beans, store_chatters=_collect_chatters)
    # logger().info("collecting|%s", "espresso")
    # espresso.collect(sb_conn_str=sb_connection_str, store_func=_collect)
    # TODO: add collection from nextdoor
    # TODO: add collection from linkedin

def _collect_beans(beans: list[Bean]):
    beans = _download(localsack.not_exists(beans))
    if beans:  
        logger().info("collected|%s|%d", beans[0].source, len(beans))
        index_queue.put_nowait(beans)            

def _collect_chatters(chatters: list[Chatter]):
    if chatters:
        localsack.store_chatters(chatters)
        # trend_queue.put_nowait([chatter.url for chatter in chatters])

def _download(beans: list[Bean]) -> list[Bean]:
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

def run_indexing_and_augmenting():
    logger().info("indexing and augmenting")
    # local index queue
    while not index_queue.empty():  
        beans = _augment(_index(localsack.not_exists(index_queue.get_nowait())))
        index_queue.task_done()
        if beans:
            localsack.store_beans(beans)
            res = remotesack.store_beans(beans)
            logger().info("stored|%s|%d", beans[0].source, res)

            # now send these urls to trend ranking and clustering
            urls = [bean.url for bean in beans]
            cluster_queue.put_nowait(urls)                    

_merge_tags = lambda bean, new_tags: list({tag.lower(): tag for tag in ((bean.tags + new_tags) if (bean.tags and new_tags) else (bean.tags or new_tags))}.values())

def _index(beans: list[Bean]):   
    if beans:
        for bean in beans:
            try:
                bean.embedding = remotesack.embedder.embed(bean.digest())   
                # NOTE: disabling categories for now
                # bean.tags = _merge_tags(bean, _find_categories(bean))
                bean.cluster_id = bean.url # this is a temporary cluster id for beans that are not clustered yet
            except:
                logger().error("failed indexing|%s", bean.url)
        return [bean for bean in beans if bean.embedding]

def _find_categories(bean: Bean):    
    return localsack.search_categories(bean.embedding, category_eps, MAX_CATEGORIES)

def _augment(beans: list[Bean]):
    if beans:
        for bean in beans:             
            try:
                digest = digestor.run(bean.text)
                if digest:
                    bean.summary = digest.summary or bean.summary or bean.text
                    bean.title = digest.title or bean.title
                    bean.tags = _merge_tags(bean, digest.tags)
            except:
                logger().error("failed augmenting|%s", bean.url)
        return [bean for bean in beans if bean.summary]  

def run_clustering():
    urls = set()    
    # finish trend ranking first since we need the similar count from clustering
    while not cluster_queue.empty():
        urls.update(cluster_queue.get_nowait())
        cluster_queue.task_done()
        # NOTE: not sending this to trend ranking because we are changing trend ranking to only count for social media scores

    logger().info("clustering|__batch__|%d", len(urls))
    clusters = _cluster(list(urls))
    # this will create some overlaps and overrides in cluster_id but that's fine since this is continuouly changing
    updates = []
    for key, val in clusters.items():
        updates.extend([UpdateOne({ K_ID: url }, { "$set": { K_CLUSTER_ID: key } }) for url in val])

    logger().info("clustered|__batch__|%d", _bulk_update(updates))

# current clustering approach
# new beans (essentially beans without cluster gets priority for defining cluster)
# for each bean without a cluster_id (essentially a new bean) fine the related beans within cluster_eps threshold
# override their current cluster_id (if any) with the new bean's url
# a bean is already in the update list then skip that one to include as a related item
# if a new bean is found related to the query new bean, then it should be skipped for finding related beans for itself
# keep running this loop through the whole collection until there is no bean without cluster id left
# every time we find a cluster (a set of related beans) we add it to the update collection and return
def _cluster(urls: list[str]) -> dict[str, list[str]]:
    return {url: localsack.search_similar_beans(url, cluster_eps) for url in urls}

def run_trend_ranking():
    logger().info("trend ranking")
    trends = _trend_rank()
    update_time = now()
    updates = [UpdateOne(
        filter={K_ID: trend.url}, 
        update={
            "$set": {
                K_UPDATED: trend.updated,
                K_LIKES: trend.likes,
                K_COMMENTS: trend.comments,
                K_SHARES: trend.shares,
                "shared_id": trend.shared_id,
                K_LATEST_LIKES: trend.latest_likes,
                K_LATEST_COMMENTS: trend.latest_comments,
                K_LATEST_SHARES: trend.latest_shares,
                K_TRENDSCORE: trend.trend_score,
                K_UPDATED: update_time      
            }
        }
    ) for trend in trends] 
    logger().info("trend ranked|__batch__|%d", _bulk_update(updates))

def _trend_rank():  
    
    calculate_trend_score = lambda bean: 100*bean.latest_comments + 10*bean.latest_shares + bean.latest_likes

    trends = localsack.get_latest_chatters(1)
    for trend in trends:
        trend.trend_score = calculate_trend_score(trend)
        # trend.updated = update_time
       
    # NOTE: disabling trend ranking for items that are not in social media yet
    # latest_chatters_urls = {bean.url for bean in beans}
    # updates.extend([UpdateOne(
    #     filter={K_ID: url}, 
    #     update={
    #         "$set": {
    #             K_UPDATED: update_time,        
    #             K_SIMILARS: cluster_sizes.get(url, 1),
    #             K_TRENDSCORE: 1000*cluster_sizes.get(url, 1)
    #         }
    #     }
    # ) for url in urls if url not in latest_chatters_urls])
    
    return trends

def run_cleanup():
    logger().info("cleaning up")
    bcount, ccount = remotesack.delete_old(window=30)
    logger().info("cleaned up|beans|%d", bcount)
    logger().info("cleaned up|chatters|%d", ccount)

def close():
    localsack.close()

BULK_CHUNK_SIZE = 10000
FIVE_MINUTES = 300
TEN_MINUTES = 600
def _bulk_update(updates):
    update_count = 0   
    for i in range(0, len(updates), BULK_CHUNK_SIZE):
        update_count += _write_batch(updates, i)
    return update_count

@retry(tries=3, delay=FIVE_MINUTES, max_delay=TEN_MINUTES, logger=logger)
def _write_batch(updates, start_index):
    return remotesack.beanstore.bulk_write(updates[start_index: start_index+BULK_CHUNK_SIZE], ordered=False, bypass_document_validation=True).modified_count

