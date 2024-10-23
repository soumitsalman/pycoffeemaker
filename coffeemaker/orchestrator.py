from itertools import chain
import logging
import os
import time
from icecream import ic
from pybeansack import utils
from pybeansack.beansack import  Beansack
from pybeansack.embedding import BeansackEmbeddings
from pybeansack.datamodels import *
from pymongo import DeleteOne, MongoClient, UpdateMany, UpdateOne
from pymongo.collection import Collection
from collectors import espresso, individual, rssfeed, ychackernews, redditor
from coffeemaker.digestors import *
import persistqueue
import queue
from sklearn.metrics.pairwise import euclidean_distances
import numpy as np

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

remotesack: Beansack = None
categorystore: Collection = None 
sb_connection_str: str = None
category_eps: float = None
cluster_eps: float = None

digestor: LocalDigestor = None
models_dir: str = None

def initialize(db_conn_str: str, sb_conn_str: str, working_dir: str, emb_path: str, llm_path: str, llm_base_url: str, llm_api_key: str, llm_model: str, cat_eps: float, clus_eps: float):
    queue_dir=working_dir+"/.processingqueue"    

    global index_queue, trend_queue, aug_queue

    index_queue = queue.Queue() # local queue
    trend_queue = persistqueue.Queue(queue_dir+"/trend", tempdir=queue_dir)
    aug_queue = persistqueue.Queue(queue_dir+"/augment", tempdir=queue_dir) 

    global sb_connection_str  
    sb_connection_str = sb_conn_str 

    global remotesack, categorystore,  category_eps, cluster_eps        
    remotesack = Beansack(db_conn_str, BeansackEmbeddings(model_path=emb_path, context_len=4096))
    categorystore = MongoClient(db_conn_str)['espresso']['categories']
    category_eps = cat_eps
    cluster_eps = clus_eps

    global digestor
    digestor = LocalDigestor(model_path=llm_path, context_len=8192) \
        if llm_path else \
            RemoteDigestor(base_url=llm_base_url, api_key=llm_api_key, model_name=llm_model, context_len=8192)

def run_collection():
    logger().info("collecting|%s", "rssfeed")
    rssfeed.collect(store_func=_collect)
    logger().info("collecting|%s", "ychackernews")
    ychackernews.collect(store_func=_collect)
    logger().info("collecting|%s", "redditor")
    redditor.collect(store_func=_collect)
    logger().info("collecting|%s", "espresso")
    espresso.collect(sb_conn_str=sb_connection_str, store_func=_collect)
    # TODO: add collection from nextdoor
    # TODO: add collection from linkedin

def _collect(items: list[Bean]|list[tuple[Bean, Chatter]]|list[Chatter]):
    if not items:
        return    
    # download the articles for new beans
    beans, chatters = None, None
    if isinstance(items[0], Bean):
        beans = items
    if isinstance(items[0], Chatter):
        chatters = items
    if isinstance(items[0], tuple):
        beans = [item[0] for item in items]
        chatters = [item[1] for item in items]

    if beans:            
        downloaded = _download(remotesack.filter_unstored_beans(beans))
        if downloaded:
            logger().info("collected|%s|%d", downloaded[0].source, len(downloaded))
            index_queue.put_nowait(downloaded)
    if chatters:            
        trend_queue.put_nowait(chatters)

def _download(beans: list[Bean]) -> list[Bean]:
    for bean in beans:           
        if (bean.kind in [NEWS, BLOG]) and (not bean.image_url or needs_download(bean)):
            res = individual.load_from_url(bean.url)
            if res and res.text:  # this is a decent indicator if loading from the url even worked 
                bean.text = (res.text if len(res.text) > len(bean.text or "") else bean.text).strip()
                bean.image_url = bean.image_url or res.top_image
                bean.source = individual.site_name(res) or bean.source
                bean.created = bean.created or int(min(res.publish_date.timestamp() if res.publish_date else time.time(), time.time()))
                bean.title = bean.title or res.title
    return [bean for bean in beans if allowed_body(bean)]

def run_indexing_and_augmenting():
    logger().info("indexing and augmenting")
    # local index queue
    while not index_queue.empty():   
        beans = index_queue.get_nowait()
        beans = remotesack.filter_unstored_beans(beans) if beans else None
        beans = _augment(_index(beans)) if beans else None
        if not beans:   
            continue                        
        res = remotesack.store_beans(beans)
        logger().info("stored|%s|%d", beans[0].source, res)
        # send it out for trend analysis and augementation. these can happen in parallel
        # trimming down the embedding fields because otherwise writing embeddings back and for becomes a lot for persistence queue
        for bean in beans:
            bean.embedding = None        
        trend_queue.put_nowait(beans) 
        # aug_queue.put_nowait(beans)

def _index(beans: list[Bean]):   
    # extract digest and prepare for pushing to beansack
    embedder = remotesack.embedder
    for bean in beans:
        try:
            bean.embedding = embedder.embed(bean.digest())            
            bean.tags, bean.categories = _find_categories(bean)
            bean.created = min(bean.created, bean.updated) # sometimes due to time zone issue the created dates look wierd. this is a patch
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
    texts = [cat[K_TEXT] for cat in res]
    ids = [cat[K_ID] for cat in res]    
    return ((texts if ic(len(texts)) > 0 else None), (ids if len(ids) > 0 else None))

def _augment(beans: list[Bean]):
    for bean in beans: 
        try:
            digest = digestor.run(bean.text)
            if digest:
                bean.summary = digest.summary if needs_summary(bean) else (bean.summary or bean.text)
                bean.title = digest.title or bean.title
                bean.tags = list(set((bean.tags + digest.tags))) if (bean.tags and digest.tags) else (bean.tags or digest.tags)
        except:
            logger().error("failed augmenting|%s", bean.url)
    return beans  

def run_clustering():
    logger().info("clustering")
    # TODO: in future optimize the beans that need to be clustered.
    # right now we are just clustering the whole database
    beans = _cluster(remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL: 1, K_CLUSTER_ID: 1, K_EMBEDDING: 1}))   
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
    batch = {}
    current_time = int(time.time())
    while not trend_queue.empty():
        # remove the duplicates in the batch
        # put them in database and queue the beans for trend_ranking
        items = trend_queue.get_nowait()
        if items:
            if isinstance(items[0], Chatter):
                remotesack.store_chatters(items) 
            batch.update({item.url: item for item in items})
    
    items = list(batch.values())
    urls = [item.url for item in items]
    chatters=remotesack.get_latest_chatter_stats(urls)
    cluster_sizes=remotesack.count_related_beans(urls)
    res = _bulk_update([_make_trend_update(item, chatters, cluster_sizes, current_time) for item in items])
    logger().info("trendranked|__batch__|%d", res)

def _make_trend_update(item, chatters, cluster_sizes, updated):
    update = {
        K_UPDATED: updated,
        K_TRENDSCORE: (next((cluster for cluster in cluster_sizes if cluster[K_URL] == item.url), {'cluster_size': 1})['cluster_size'] if cluster_sizes else 1)*10
    }
    ch = next((ch for ch in chatters if ch.url==item.url), None) if chatters else None
    if ch:
        if ch.likes:
            update[K_LIKES] = ch.likes
            update[K_TRENDSCORE] += ch.likes
        if ch.comments:
            update[K_COMMENTS] = ch.comments
            update[K_TRENDSCORE] += (ch.comments*3)
    return UpdateOne({K_URL: item.url}, {"$set": update})

# def run_augmentation():
#     logger().info("augmentating")
#     _make_update = lambda bean: UpdateOne(
#         filter = {K_URL: bean.url}, 
#         update={
#             "$set": {
#                 K_TITLE: bean.title,
#                 K_SUMMARY: bean.summary,
#                 K_TAGS: bean.tags                
#             }
#         }
#     ) if bean.summary else DeleteOne(filter = {K_URL: bean.url})

#     while not aug_queue.empty():
#         beans = _augment(aug_queue.get_nowait())
#         res = remotesack.beanstore.bulk_write([_make_update(bean) for bean in beans], ordered=False, bypass_document_validation=True).modified_count
#         logger().info("augmented|%s|%d", beans[0].source, res)
#         aug_queue.task_done()

def run_cleanup():
    logger().info("cleaning up")
    bcount, ccount = remotesack.delete_old(window=30)
    logger().info("cleaned up|beans|%d", bcount)
    logger().info("cleaned up|chatters|%d", ccount)

BULK_CHUNK_SIZE = 20000
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

