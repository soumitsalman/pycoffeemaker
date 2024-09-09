from itertools import chain
import os
from icecream import ic
from pybeansack import utils
from pybeansack.beansack import  Beansack
from pybeansack.embedding import BeansackEmbeddings
from pybeansack.datamodels import *
from pymongo import MongoClient, UpdateMany, UpdateOne
from pymongo.collection import Collection
from collectors import individual, rssfeed, ychackernews, redditor
from sklearn.cluster import AffinityPropagation, KMeans, MiniBatchKMeans
from langchain_groq import ChatGroq
from coffeemaker.chains import *
from persistqueue import Queue

MIN_ALLOWED_BODY_LEN = 75
MIN_DOWNLOAD_BODY_LEN = 150 
MIN_SUMMARIZER_BODY_LEN = 150
MAX_CATEGORIES = 5
CLEANUP_WINDOW = 30
N_THREADS = os.cpu_count()

logger = utils.create_logger("coffeemaker")

keyphraser: KeyphraseExtractor = None
embedder: BeansackEmbeddings = None
remotesack: Beansack = None
categorystore: Collection = None 
index_queue: Queue = None
trend_queue: Queue = None
llm_api_key: str = None
cluster_eps: int = None
category_eps: int = None

def initialize(db_conn_str: str, working_dir: str, emb_file: str, api_key: str, cl_eps: float, cat_eps: float):
    queue_dir=working_dir+"/.processingqueue"
    global keyphraser, embedder, remotesack, categorystore, index_queue, trend_queue, llm_api_key, cluster_eps, category_eps

    models_dir=working_dir+"/.models/"
    keyphraser = KeyphraseExtractor(cache_dir=models_dir)
    embedder = BeansackEmbeddings(model_path=models_dir+emb_file, context_len=4095)

    remotesack = Beansack(db_conn_str)
    categorystore = MongoClient(db_conn_str)['espresso']['categories']
       
    index_queue = Queue(queue_dir+"/index", tempdir=queue_dir)
    trend_queue = Queue(queue_dir+"/trend", tempdir=queue_dir)

    llm_api_key = api_key

    cluster_eps = cl_eps
    category_eps = cat_eps

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
            _queue(index_queue, downloaded)
    if chatters:            
        _queue(trend_queue, chatters)

def _download_beans(beans: list[Bean]) -> list[Bean]:
    for bean in beans:
        if (bean.kind in [NEWS, BLOG]) and (not bean.image_url or not bean.text or len(bean.text.split())<MIN_DOWNLOAD_BODY_LEN):
            res = individual.load_from_url(bean.url)
            if res:            
                bean.text = (res.text if len(res.text) > len(bean.text or "") else bean.text).strip()
                bean.image_url = bean.image_url or res.top_image
                bean.source = individual.site_name(res) or bean.source
                bean.created = int(res.publish_date.timestamp()) if res.publish_date else bean.created
    return [bean for bean in beans if bean.text and len(bean.text.split())>=MIN_ALLOWED_BODY_LEN]

def run_indexing():
    logger.info("Starting Indexing")
    while not index_queue.empty():   
        beans = _dequeue(index_queue)     
        beans = remotesack.filter_unstored_beans(beans) if beans else None
        if not beans:   
            continue   
        try:      
            beans = _augment(beans) # this is the data augmentation part: embeddings, summary, highlights           
            res = remotesack.store_beans(beans)
            logger.info("%d beans added from %s", res, beans[0].source)
            _queue(trend_queue, beans) # set it out for clustering
        except Exception as err:
            logger.warning("Indexing/Storing failed for a batch from %s. %s", beans[0].source, str(err))
        
def _augment(beans: list[Bean]):
    # embedder = BeansackEmbeddings(embedder_path, 4095)
    llm = ChatGroq(api_key=llm_api_key, model="llama3-8b-8192", temperature=0.1, verbose=False, streaming=False)
    digestor = DigestExtractor(llm, 3072) 
    summarizer = Summarizer(llm, 3072)
    
    # extract digest and prepare for pushing to beansack
    for bean in beans:
        # putting this in try catch because these have remote call and are not always reliable
        try:
            # embedding
            bean.embedding = embedder.embed(bean.digest())            
            bean.categories = _find_categories(bean)

            # summary because in the digestor the summary content looks like shit
            bean.summary = summarizer.run(bean.text) if len(bean.text.split()) > MIN_SUMMARIZER_BODY_LEN else bean.text
            
            # extract named entities.
            # TODO: merge categories and tags?
            bean.tags = keyphraser.run(bean.text)
            if bean.categories and bean.tags:
                bean.categories.extend(bean.tags)

            # highlight: not merging this summary because otherwise the summary content looks like shit
            digest = digestor.run(kind=bean.kind, text=bean.text)            
            bean.title = digest.highlight
        except:
            logger.warning("Augmenting failed for %s", bean.url)   
        finally:
            # normalize fields and values that are immaterial   
            bean.text = None # text field has no use any more and will just cause extra load 
    return [bean for bean in beans if (bean.embedding and bean.summary)]

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

def run_clustering():
    logger.info("Starting Clustering")
    # TODO: in future optimize the beans that need to be clustered.
    # right now we are just clustering the whole database
    res = _run_clustering(remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL: 1, K_CLUSTER_ID: 1, K_EMBEDDING: 1}))   
    updates = [UpdateMany({K_URL: {"$in": group}},{"$set": {K_CLUSTER_ID: group[0]}}) for group in res]
    update_count = _bulk_update(updates)
    logger.info("%d beans updated with cluster_id", update_count)

# TODO: change the whole clustering concept later
# explanation: the current approach is heuristic because 
# neither AffinityPropagation or MeanShift can handle large data set
# K-means is reasonably decent but cannot function without a defined cluster-size
# option 1: (currently implemented) use a fixed size for k-means based on the population size.
# but this does not translate well in scenarios where the contents may be similar. option 2 may be better
# option 2: find out an n-cluster size based on random samples and use that as a seed for K-Means
# option 3: process each incoming batch iteratively. we cluster the collected batch using affinityPropagation or meanshift
# and then union the cluster size with exiting cluters in the DB
def _run_clustering(beans: list[Bean]):     
    POPULATION_SIZE_THRESHOLD = 5000  
    clusterer =  AffinityPropagation(damping=0.6, convergence_iter=50, random_state=23)\
        if len(beans) < POPULATION_SIZE_THRESHOLD else \
        KMeans(n_clusters=len(beans)//10, max_iter=300, random_state=23)
    
    labels = clusterer.fit([bean.embedding for bean in beans]).labels_
    groups = {}
    for index, label in enumerate(labels):
        label = int(label)
        if label not in groups:
            groups[label] = []
        groups[label].append(index)

    res = []
    for indexes in groups.values():
        res.append([beans[index].url for index in indexes])    
    return res

def run_trend_ranking():
    logger.info("Starting Trend Ranking")
    batch = {}
    while not trend_queue.empty():
        # remove the duplicates in the batch
        # put them in database and queue the beans for trend_ranking
        items = _dequeue(trend_queue)
        if items:
            if isinstance(items[0], Chatter):
                remotesack.store_chatters(items) 
            batch.update({item.url: item for item in items})
    
    items = list(batch.values())
    urls = [item.url for item in items]
    try:    
        chatters=remotesack.get_latest_chatter_stats(urls)
        cluster_sizes=remotesack.count_related_beans(urls)
        res = _bulk_update([_make_trend_update(item, chatters, cluster_sizes) for item in items])
        logger.info("%d beans updated with trendscore", res)
    except Exception as err:
        logger.warning("Trend calculation failed for a batch of %d beans. %s", len(items), str(err))

def _make_trend_update(item, chatters, cluster_sizes):
    update = {
        K_UPDATED: item.updated,
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

BULK_CHUNK_SIZE = 7500
FIVE_MINUTES = 300
TEN_MINUTES = 600
def _bulk_update(updates):
    update_count = 0   
    for i in range(0, len(updates), BULK_CHUNK_SIZE):
        update_count += _write_batch(updates, ic(i))
    return update_count

@retry(tries=3, delay=FIVE_MINUTES, max_delay=TEN_MINUTES, logger=logger)
def _write_batch(updates, start_index):
    return remotesack.beanstore.bulk_write(updates[start_index: start_index+BULK_CHUNK_SIZE], ordered=False, bypass_document_validation=True).modified_count

def _dequeue(q: Queue):
    try:
        val = q.get()        
        return val
    finally:
        q.task_done()

def _queue(q: Queue, val) -> bool:
    try:
        q.put_nowait(val)
        return True
    except:
        return False