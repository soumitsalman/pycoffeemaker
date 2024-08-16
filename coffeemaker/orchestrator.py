from itertools import chain
import os
from icecream import ic
from datetime import datetime as dt
from pybeansack import utils
from pybeansack.beansack import timewindow_filter, Beansack
from pybeansack.embedding import BeansackEmbeddings
from pybeansack.datamodels import *
from pymongo import MongoClient, UpdateMany, UpdateOne
from pymongo.collection import Collection
from collectors import individual, rssfeed, ychackernews, redditor
from sklearn.cluster import DBSCAN
from langchain_groq import ChatGroq
from coffeemaker.chains import DigestExtractor, Summarizer
from persistqueue import Queue

MIN_ALLOWED_BODY_LEN = 75
MIN_DOWNLOAD_BODY_LEN = 150 
MIN_SUMMARY_BODY_LEN = 200
MAX_CATEGORIES = 10
CLEANUP_WINDOW = 30

logger = utils.create_logger("coffeemaker")

remotesack: Beansack = None
categorystore: Collection = None 
collect_queue: Queue = None
index_queue: Queue = None
cluster_queue: Queue = None
trend_queue: Queue = None
embedder_path: str = None
llm_api_key: str = None
cluster_eps: int = None
category_eps: int = None

def initialize(db_conn_str: str, working_dir: str, emb_file: str, api_key: str, cl_eps: float, cat_eps: float):
    queue_dir=working_dir+"/.processingqueue"
    global embedder_path, remotesack, categorystore, collect_queue, index_queue, cluster_queue, trend_queue, llm_api_key, cluster_eps, category_eps

    embedder_path=working_dir+"/.models/"+emb_file

    remotesack = Beansack(db_conn_str)
    categorystore = MongoClient(db_conn_str)['espresso']['categories']
    
    collect_queue = Queue(queue_dir+"/collect", tempdir=queue_dir)     
    index_queue = Queue(queue_dir+"/index", tempdir=queue_dir)
    cluster_queue = Queue(queue_dir+"/cluster", tempdir=queue_dir)
    trend_queue = Queue(queue_dir+"/trend", tempdir=queue_dir)

    llm_api_key = api_key

    cluster_eps = cl_eps
    category_eps = cat_eps

def run_cleanup():
    logger.info("Starting clean up")
    remotesack.delete_old(CLEANUP_WINDOW)
    # local_beanstore.delete(where=timewindow_filter(CLEANUP_WINDOW))

def run_collector():
    logger.info("Starting collection from rss feeds.")
    rssfeed.collect(store_func=_process_collection)
    logger.info("Starting collection from YC hackernews.")
    ychackernews.collect(store_func=_process_collection)
    logger.info("Starting collection from Reddit.")
    redditor.collect(store_func=_process_collection)
    # TODO: add collection from nextdoor
    # TODO: add collection from linkedin

def _process_collection(items: list[Bean]|list[tuple[Bean, Chatter]]|list[Chatter]):
    _is_valid_to_index = lambda bean: bean.text and len(bean.text.split())>=MIN_ALLOWED_BODY_LEN

    if items:
        # download the articles for new beans
        if isinstance(items[0], Bean):
            beans, chatters = items, None
        elif isinstance(items[0], tuple):
            beans = [item[0] for item in items]
            chatters = [item[1] for item in items]
        else:
            beans, chatters = None,  items

        if beans:            
            downloaded = [bean for bean in _download_beans(remotesack.filter_unstored_beans(beans)) if _is_valid_to_index(bean)]
            if downloaded:
                logger.info("%d (out of %d) beans downloaded from %s", len(downloaded), len(beans), downloaded[0].source)
                index_queue.put(downloaded)
        if chatters:            
            trend_queue.put(chatters)

def _download_beans(beans: list[Bean]) -> list[Bean]:
    for bean in beans:
        body = bean.text if (bean.kind in [NEWS, BLOG] or (bean.text and len(bean.text.split())>MIN_DOWNLOAD_BODY_LEN)) else individual.load_from_url(bean.url)
        bean.text = body if (len(body or "") > len(bean.text or "")) else bean.text
    return beans

def run_indexing():
    logger.info("Starting Indexing")
    while not index_queue.empty():
        beans = remotesack.filter_unstored_beans(index_queue.get()) 
        if beans:            
            # this is the data augmentation part: embeddings, summary, highlights
            _augment(beans)            
            res = remotesack.store_beans(beans)
            logger.info("%d beans added", res)
            cluster_queue.put(beans) # set it out for clustering
        index_queue.task_done()

def _augment(beans: list[Bean]):
    embedder = BeansackEmbeddings(embedder_path, 4095)
    llm = ChatGroq(api_key=llm_api_key, model="llama3-8b-8192", temperature=0.1, verbose=False, streaming=False)
    digestor = DigestExtractor(llm, 3072) 
    summarizer = Summarizer(llm, 3072)

    # extract digest and prepare for pushing to beansack
    for bean in beans:
        # extract digest
        try:
            # embedding
            bean.embedding = embedder.embed(bean.digest())
            # summary because in the digestor the summary content looks like shit
            bean.summary = summarizer.run(bean.text) if len(bean.text.split()) > MIN_SUMMARY_BODY_LEN else bean.text
            # match categories
            bean.categories = _find_categories(bean)
            # highlights and tags
            digest = digestor.run(kind=bean.kind, text=bean.text)
            bean.tags = digest.keyphrases[:5] or bean.tags
            bean.highlights = digest.highlights[:5]
            if bean.kind in [NEWS, BLOG]: # normalize creation time of news and blogs
                bean.created = int(dt.fromtimestamp(bean.created).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        except Exception:
            logger.warning("Augmenting failed for %s", bean.url)        
        bean.text = None # text field has no use any more and will just cause extra load 

    return [bean for bean in beans if bean.embedding or bean.summary]

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
    matches = categorystore.aggregate(pipeline)
    return list(set(chain(*(cat[K_CATEGORIES] for cat in matches)))) or None

N_DEPTH = 4
MAX_CLUSTER_SIZE = 128
def run_clustering():
    logger.info("Starting Clustering")
    # TODO: in future optimize the beans that need to be clustered.
    # right now we are just clustering the whole database
    res = _run_clustering(
        remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL: 1, K_CLUSTER_ID: 1, K_EMBEDDING: 1}), 
        N_DEPTH)   

    for group in res:
        if not isinstance(group[0], str):
            ic(group)  

    updates = [UpdateMany({K_URL: {"$in": group}},{"$set": {K_CLUSTER_ID: group[0]}}) for group in res]
    update_count = remotesack.beanstore.bulk_write(updates, ordered=False).modified_count
    logger.info("%d beans updated with cluster_id", update_count)

    # for sequential posterities sake
    while not cluster_queue.empty():
        # ONLY PUT URL POINTERS IN THIS
        trend_queue.put(cluster_queue.get())
        cluster_queue.task_done()
    
def _run_clustering(beans, n_depth):
    if n_depth <= 0:
        return [[bean.url for bean in beans]]
    
    dbscan = DBSCAN(eps=cluster_eps+n_depth, min_samples=2*n_depth-1, algorithm='brute', n_jobs=(os.cpu_count()-1) or 1)
    labels = dbscan.fit([bean.embedding for bean in beans]).labels_
    
    groups = {}
    for index, label in enumerate(labels):
        label = int(label)
        if label not in groups:
            groups[label] = []
        groups[label].append(index)

    res = []
    for indexes in groups.values():
        if len(indexes) <= MAX_CLUSTER_SIZE:
            res.append([beans[index].url for index in indexes])         
        else:
            res.extend(_run_clustering([beans[index] for index in indexes], n_depth-1))
    return res

def run_trend_ranking():
    logger.info("Starting Trend Ranking")
    while not trend_queue.empty():
        # remove the duplicates in the batch
        # put them in database and queue the beans for trend_ranking
        items = trend_queue.get()
        if isinstance(items[0], Chatter):
            remotesack.store_chatters(items)
        # else item is Bean

        urls = [item.url for item in items]
        chatters=remotesack.get_latest_chatter_stats(urls)
        cluster_sizes=remotesack.count_related_beans(urls) # technically i can get this from remotesack as well

        def make_trend_update(item):
            update = {K_UPDATED: item.updated}
            ch = next((ch for ch in chatters if ch.url==item.url), None) if chatters else None
            if ch:
                if ch.likes:
                    update[K_LIKES] = ch.likes
                if ch.comments:
                    update[K_COMMENTS] = ch.comments
                update[K_TRENDSCORE] = (ch.likes or 0) + (ch.comments or 0)*3
            update[K_TRENDSCORE] = update.get(K_TRENDSCORE, 0)+cluster_sizes.get(item.url, 0)*10
            return UpdateOne({K_URL: item.url}, {"$set": update})

        res = remotesack.beanstore.bulk_write([make_trend_update(item) for item in items], ordered=False).modified_count
        logger.info("%d beans updated with trendscore", res)
        trend_queue.task_done()

