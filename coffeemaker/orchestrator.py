from itertools import chain
import logging
from icecream import ic
from pybeansack.ducksack import Beansack as DuckSack
from pybeansack.mongosack import Beansack as MongoSack
from pybeansack.embedding import BeansackEmbeddings
from pybeansack.datamodels import *
from pybeansack.utils import now
from pymongo import MongoClient, UpdateOne
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
trend_queue: persistqueue.Queue = None
aug_queue: persistqueue.Queue = None

remotesack: MongoSack = None
categorystore: Collection = None 
baristastore: Collection = None
sb_connection_str: str = None
localsack: DuckSack = None
category_eps: float = None
cluster_eps: float = None

digestor: LocalDigestor = None
models_dir: str = None

def initialize(db_conn_str: str, sb_conn_str: str, working_dir: str, emb_path: str, llm_base_url: str, llm_api_key: str, llm_model: str, cat_eps: float, clus_eps: float):
    queue_dir=working_dir+"/.processingqueue"    

    global index_queue, trend_queue, aug_queue

    index_queue = queue.Queue() # local queue
    trend_queue = persistqueue.Queue(queue_dir+"/trend", tempdir=queue_dir)
    aug_queue = persistqueue.Queue(queue_dir+"/augment", tempdir=queue_dir) 

    global sb_connection_str  
    sb_connection_str = sb_conn_str 

    global remotesack, categorystore, baristastore, localsack, category_eps, cluster_eps        
    remotesack = MongoSack(db_conn_str, BeansackEmbeddings(model_path=emb_path, context_len=4096))
    espressodb = MongoClient(db_conn_str)['espresso']
    categorystore = espressodb['categories']
    baristastore = espressodb['baristas']
    localsack = DuckSack(db_dir=working_dir+"/.db")
    category_eps = cat_eps
    cluster_eps = clus_eps

    global digestor
    digestor = RemoteDigestor(base_url=llm_base_url, api_key=llm_api_key, model_name=llm_model, context_len=8192) \
        if llm_base_url else \
            LocalDigestor(model_path=llm_model, context_len=8192) 

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
    if beans:            
        new_beans = _download(remotesack.new_beans(beans))
        if new_beans:
            logger().info("collected|%s|%d", new_beans[0].source, len(new_beans))
            index_queue.put_nowait(new_beans)            

def _collect_chatters(chatters: list[Chatter]):
    if chatters:
        localsack.store_chatters(chatters)
        trend_queue.put_nowait([chatter.url for chatter in chatters])

def _download(beans: list[Bean]) -> list[Bean]:
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
        # beans = _augment(_index(remotesack.new_beans(index_queue.get_nowait())))
        beans = _index(remotesack.new_beans(index_queue.get_nowait()))
        index_queue.task_done()
        if beans:
            localsack.store_beans(beans)
            res = remotesack.store_beans(beans)
            trend_queue.put_nowait([bean.url for bean in beans])
            logger().info("stored|%s|%d", beans[0].source, res)        

_merge_tags = lambda bean, new_tags: list({tag.lower(): tag for tag in (bean.tags + new_tags)}.values()) if (bean.tags and new_tags) else (bean.tags or new_tags)

def _index(beans: list[Bean]):   
    if beans:
        for bean in beans:
            try:
                bean.embedding = remotesack.embedder.embed(bean.digest())   
                bean.tags = _merge_tags(bean, _find_categories(bean))
            except:
                logger().error("failed indexing|%s", bean.url)
        return [bean for bean in beans if bean.embedding]

def _find_categories(bean: Bean):    
    pipeline = [
        {
            "$search": {
                "cosmosSearch": {
                    "vector": bean.embedding,
                    "path":   K_EMBEDDING,
                    "k":      MAX_CATEGORIES,
                    "filter": {K_SOURCE: "__SYSTEM__"}
                },
                "returnStoredSource": True
            }
        },
        {"$addFields": { "search_score": {"$meta": "searchScore"} }},
        {"$match": { "search_score": {"$gte": 1-category_eps} }},
        {"$project": {K_EMBEDDING: 0}}
    ] 
    res = categorystore.aggregate(pipeline)
    return [cat[K_TEXT] for cat in res]

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
    logger().info("clustering")
    # TODO: in future optimize the beans that need to be clustered.
    # right now we are just clustering the whole database
    beans = _cluster(remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL: 1, K_CLUSTER_ID: 1, K_EMBEDDING: 1, K_ID: 0}))   
    updates = [UpdateOne({K_URL: bean.url},{"$set": {K_CLUSTER_ID: bean.cluster_id}}) for bean in beans]
    update_count = _bulk_update(updates)
    logger().info("clustered|__batch__|%d", update_count)

# current clustering approach
# new beans (essentially beans without cluster gets priority for defining cluster)
# for each bean without a cluster_id (essentially a new bean) fine the related beans within cluster_eps threshold
# override their current cluster_id (if any) with the new bean's url
# a bean is already in the update list then skip that one to include as a related item
# if a new bean is found related to the query new bean, then it should be skipped for finding related beans for itself
# keep running this loop through the whole collection until there is no bean without cluster id left
# every time we find a cluster (a set of related beans) we add it to the update collection and return
def _cluster(beans: list[Bean]):
    to_update = {}
    in_update_list = lambda b: b.url in to_update
     
    def assign_cluster_id(related_beans, cluster_id):
        # assign cluster_id to all the related ones
        for bean in related_beans:
            bean.cluster_id = cluster_id
        # prepare update package
        to_update.update({bean.url:bean for bean in related_beans})

    embeddings = np.array([bean.embedding for bean in beans])
    for bean in beans:
        if not bean.cluster_id: # this is newly added
            distances = euclidean_distances([bean.embedding], embeddings)[0]
            related_beans = [b for j, b in enumerate(beans) if distances[j] <= cluster_eps and not in_update_list(b)]
            assign_cluster_id(related_beans, bean.url)
    return list(to_update.values())

def run_trend_ranking():
    logger().info("trendranking")
    urls = set()    
    while not trend_queue.empty():
        # remove the duplicates in the batch
        # put them in database and queue the beans for trend_ranking
        urls.update(trend_queue.get_nowait())
        trend_queue.task_done()

    res = _bulk_update(_trend_rank(list(urls))) if urls else 0
    logger().info("trendranked|__batch__|%d", res)

def _trend_rank(urls: list[str]) -> int:    
    latest_chatters = localsack.get_latest_chatters(1)
    latest_chatters_urls = {bean.url for bean in latest_chatters}
    cluster_sizes = {item[K_URL]: item["cluster_size"] for item in remotesack.count_related_beans(urls)}
    update_time = now()

    calculate_trend_score = lambda bean: 1000*cluster_sizes.get(bean.url, 1) + 100*bean.latest_comments + 10*bean.latest_shares + bean.latest_likes
    
    updates = [UpdateOne(
        filter={K_ID: bean.url}, 
        update={
            "$set": {
                K_UPDATED: update_time,
                K_LIKES: bean.likes,
                K_COMMENTS: bean.comments,
                K_SHARES: bean.shares,
                K_SIMILARS: cluster_sizes.get(bean.url, 1),
                K_LATEST_LIKES: bean.latest_likes,
                K_LATEST_COMMENTS: bean.latest_comments,
                K_LATEST_SHARES: bean.latest_shares,
                K_TRENDSCORE: calculate_trend_score(bean)        
            }
        }
    ) for bean in latest_chatters]    
       
    updates.extend([UpdateOne(
        filter={K_ID: url}, 
        update={
            "$set": {
                K_UPDATED: update_time,        
                K_SIMILARS: cluster_sizes.get(url, 1),
                K_TRENDSCORE: 1000*cluster_sizes.get(url, 1)
            }
        }
    ) for url in urls if url not in latest_chatters_urls])
    
    return updates

def run_cleanup():
    logger().info("cleaning up")
    bcount, ccount = remotesack.delete_old(window=30)
    logger().info("cleaned up|beans|%d", bcount)
    logger().info("cleaned up|chatters|%d", ccount)

BULK_CHUNK_SIZE = 10000
FIVE_MINUTES = 300
TEN_MINUTES = 600
def _bulk_update(updates):
    update_count = 0   
    for i in range(0, len(updates), BULK_CHUNK_SIZE):
        update_count += _write_batch(updates, i)
    return update_count

@retry(tries=3, delay=FIVE_MINUTES, max_delay=TEN_MINUTES, logger=logger())
def _write_batch(updates, start_index):
    return remotesack.beanstore.bulk_write(updates[start_index: start_index+BULK_CHUNK_SIZE], ordered=False, bypass_document_validation=True).modified_count

