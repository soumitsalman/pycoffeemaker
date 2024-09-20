from itertools import chain
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
from sklearn.cluster import AffinityPropagation, KMeans, MiniBatchKMeans
from langchain_groq import ChatGroq
from coffeemaker.chains import *
import persistqueue
import queue
from sklearn.metrics.pairwise import euclidean_distances
import numpy as np

MIN_ALLOWED_BODY_LEN = 75
MIN_DOWNLOAD_BODY_LEN = 150 
MIN_SUMMARIZER_BODY_LEN = 150
MAX_CATEGORIES = 5
CLEANUP_WINDOW = 30
N_THREADS = os.cpu_count()

logger = utils.create_logger("coffeemaker")

# index queue is a non persistence queue because at this point nothing is stored. things need to be stored before they can be processed further
# if storing fails in this round, it can succeed in the next
index_queue: queue.Queue = None 
trend_queue: persistqueue.Queue = None
aug_queue: persistqueue.Queue = None

embedder: BeansackEmbeddings = None
remotesack: Beansack = None
categorystore: Collection = None 
sb_connection_str: str = None
category_eps: float = None
cluster_eps: float = None

digestor: DigestExtractor = None
summarizer: Summarizer = None
keyphraser: KeyphraseExtractor = None

def initialize(db_conn_str: str, sb_conn_str: str, working_dir: str, emb_file: str, api_key: str, cat_eps: float, clus_eps: float):
    queue_dir=working_dir+"/.processingqueue"
    models_dir=working_dir+"/.models/"

    global index_queue, trend_queue, aug_queue

    index_queue = queue.Queue() # local queue
    trend_queue = persistqueue.Queue(queue_dir+"/trend", tempdir=queue_dir)
    aug_queue = persistqueue.Queue(queue_dir+"/augment", tempdir=queue_dir)    

    global embedder, remotesack, categorystore, sb_connection_str, category_eps, cluster_eps    
    
    embedder = BeansackEmbeddings(model_path=models_dir+emb_file, context_len=4095)
    remotesack = Beansack(db_conn_str, embedder)
    categorystore = MongoClient(db_conn_str)['espresso']['categories']
    sb_connection_str = sb_conn_str
    category_eps = cat_eps
    cluster_eps = clus_eps

    global  digestor, summarizer  

    llm = ChatGroq(api_key=api_key, model="llama3-8b-8192", temperature=0.1, verbose=False, streaming=False)
    digestor = DigestExtractor(llm, 3072) 
    summarizer = Summarizer(llm, 3072)

def run_collection():
    logger.info("Starting collection from rss feeds.")
    rssfeed.collect(store_func=_collect)
    logger.info("Starting collection from YC hackernews.")
    ychackernews.collect(store_func=_collect)
    logger.info("Starting collection from Reddit.")
    redditor.collect(store_func=_collect)
    logger.info("Starting collection from Espresso Queue.")
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
        downloaded = _download_beans(remotesack.filter_unstored_beans(beans))
        if downloaded:
            logger.info("%d (out of %d) beans downloaded from %s", len(downloaded), len(beans), downloaded[0].source)
            index_queue.put_nowait(downloaded)
    if chatters:            
        trend_queue.put_nowait(chatters)

def _download_beans(beans: list[Bean]) -> list[Bean]:
    for bean in beans:
        if (bean.kind in [NEWS, BLOG]) and (not bean.image_url or not bean.text or len(bean.text.split())<MIN_DOWNLOAD_BODY_LEN):
            res = individual.load_from_url(bean.url)
            if res and res.text:  # this is a decent indicator if loading from the url even worked 
                bean.text = (res.text if len(res.text) > len(bean.text or "") else bean.text).strip()
                bean.image_url = bean.image_url or res.top_image
                bean.source = individual.site_name(res) or bean.source
                bean.created = int(res.publish_date.timestamp()) if res.publish_date else bean.created
                bean.title = bean.title or res.title
    return [bean for bean in beans if bean.text and len(bean.text.split())>=MIN_ALLOWED_BODY_LEN]

def run_indexing():
    logger.info("Starting Indexing")
    # local index queue
    while not index_queue.empty():   
        beans = index_queue.get_nowait()
        beans = remotesack.filter_unstored_beans(beans) if beans else None
        beans = _index(beans) if beans else None
        if not beans:   
            continue                        
        res = remotesack.store_beans(beans)
        logger.info("%d beans added from %s", res, beans[0].source)
        # send it out for trend analysis and augementation. these can happen in parallel
        # trimming down some of fields because otherwise writing embeddings back and for becomes a lot for persistence queue
        beans = [Bean(url=bean.url, text=bean.text, summary=bean.summary, title=bean.title, tags=bean.tags, updated=bean.updated) for bean in beans]
        trend_queue.put_nowait(beans) 
        aug_queue.put_nowait(beans)

def _index(beans: list[Bean]):   
    # extract digest and prepare for pushing to beansack
    for bean in beans:
        try:
            bean.embedding = embedder.embed(bean.digest())            
            bean.categories = _find_categories(bean)
            bean.created = min(bean.created, bean.updated) # sometimes due to time zone issue the created dates look wierd. this is a patch
        except:
            logger.warning("Indexing failed for %s", bean.url)   
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
    ids = []
    for cat in categorystore.aggregate(pipeline):
        ids.append(cat[K_ID])
        ids.extend(cat['related'])

    return list(set(ids)) if ids else None

def run_clustering():
    logger.info("Starting Clustering")
    # TODO: in future optimize the beans that need to be clustered.
    # right now we are just clustering the whole database
    beans = _cluster(remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL: 1, K_CLUSTER_ID: 1, K_EMBEDDING: 1}))   
    updates = [UpdateOne({K_URL: bean.url},{"$set": {K_CLUSTER_ID: bean.cluster_id}}) for bean in beans]
    update_count = _bulk_update(updates)
    logger.info("%d beans updated with cluster_id", update_count)

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
    logger.info("Starting Trend Ranking")
    batch = {}
    current_time = int(time.time())
    while not trend_queue.empty():
        # remove the duplicates in the batch
        # put them in database and queue the beans for trend_ranking
        try:
            items = trend_queue.get_nowait()
            if items:
                if isinstance(items[0], Chatter):
                    remotesack.store_chatters(items) 
                batch.update({item.url: item for item in items})
        except:
            logger.warning("Dequeueing from trend-queue failed for a batch")
        finally:
            trend_queue.task_done()
    
    items = list(batch.values())
    urls = [item.url for item in items]
    chatters=remotesack.get_latest_chatter_stats(urls)
    cluster_sizes=remotesack.count_related_beans(urls)
    res = _bulk_update([_make_trend_update(item, chatters, cluster_sizes, current_time) for item in items])
    logger.info("%d beans updated with trendscore", res)

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

def run_augmentation():
    logger.info("Starting Augmentation")
    _make_update = lambda bean: UpdateOne(
        filter = {K_URL: bean.url}, 
        update={
            "$set": {
                K_SUMMARY: bean.summary,
                K_TAGS: bean.tags,
                K_TITLE: bean.title
            },
            "$unset": { K_TEXT: ""}
        }
    ) if bean.summary else DeleteOne(filter = {K_URL: bean.url})
    res = 0
    while not aug_queue.empty():
        res += _bulk_update([_make_update(bean) for bean in _augment(aug_queue.get_nowait())])
        aug_queue.task_done()
    logger.info("%d beans updated with summary, tldr & tags", res)

def _augment(beans: list[Bean]):
    for bean in beans:
        # putting this in try catch because these have remote call and are not always reliable
        try:
            # NOTE: ideally summary, highlight and keyphrase extraction can be called through the same api call
            # but the summary generation in JSON format and named entity extraction with the current model looks like shit
            bean.summary = (summarizer.run(bean.text) or bean.summary or bean.text) if len(bean.text.split()) > MIN_SUMMARIZER_BODY_LEN else bean.text
            digest = digestor.run(kind=bean.kind, text=bean.text)
            bean.title = digest.highlight or bean.title            
            bean.tags = digest.keyphrases[:5]  # take the first 5 tags and call it good          
        except:
            logger.warning("Augmenting failed for %s", bean.url)   
    return beans

BULK_CHUNK_SIZE = 20000
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
