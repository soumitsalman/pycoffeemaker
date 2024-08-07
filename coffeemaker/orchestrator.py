from icecream import ic
from pybeansack import utils
from pybeansack.beansack import timewindow_filter, Beansack
from pybeansack.embedding import BeansackEmbeddings
from pybeansack.datamodels import *
from pymongo import MongoClient
from pymongo.collection import Collection
from collectors import individual, rssfeed, ychackernews
from sklearn.cluster import DBSCAN
from langchain_groq import ChatGroq
from coffeemaker.chains import DigestExtractor, Summarizer
from persistqueue import Queue

MIN_BODY_LEN = 512 # ~100 words
MIN_DOWNLOAD_LEN = 768 # ~ 150 words 
MAX_CATEGORIES = 3
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

    remotesack = Beansack(db_conn_str, None)
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
    # TODO: add collection from reddit
    # TODO: add collection from nextdoor
    # TODO: add collection from linkedin

def _process_collection(items: list[Bean]|list[tuple[Bean, Chatter]]|list[Chatter]):
    _is_valid_to_index = lambda bean: bean.text and len(bean.text)>=MIN_BODY_LEN

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
        body = individual.load_from_url(bean.url) if(not bean.text or (len(bean.text)) <= MIN_DOWNLOAD_LEN) else bean.text
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
            bean.summary = summarizer.run(bean.text)  
            # match categories
            bean.categories = _find_categories(bean)
            # highlights and tags
            digest = digestor.run(kind=bean.kind, text=bean.text)
            bean.tags = digest.keyphrases[:5] or bean.tags
            bean.highlights = digest.highlights[:5]
            # disabling this temporarily
            # bean.categories = bean.categories or [digest.topic]
        except Exception:
            logger.warning("Augmenting failed for %s", bean.url)        
        bean.text = None # text field has no use any more and will just cause extra load 

    return beans

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
        {"$project": {K_TEXT: 1, 'search_score': 1}}
    ] 
    matches = categorystore.aggregate(pipeline)
    return [cat[K_TEXT] for cat in matches] or None

def run_clustering():
    logger.info("Starting Clustering")
    # TODO: in future optimize the beans that need to be clustered.
    # right now we are just clustering the whole database
    beans = remotesack.get_beans(filter={K_EMBEDDING: {"$exists": True}}, projection={K_URL: 1, K_CLUSTER_ID: 1, K_EMBEDDING: 1})
    dbscan = DBSCAN(eps = cluster_eps, min_samples=1)
    labels = dbscan.fit([bean.embedding for bean in beans]).labels_
    
    groups = {}
    for index, label in enumerate(labels):
        label = int(label)
        if label not in groups:
            groups[label] = []
        groups[label].append(index)

    update_counter = 0
    for indices in groups.values():
        update_counter += remotesack.update_beans([beans[index].url for index in indices], {K_CLUSTER_ID: beans[indices[0]].url})
    logger.info("%d beans updated with cluster_id", update_counter)

    # for sequential posterities sake
    while not cluster_queue.empty():
        # ONLY PUT URL POINTERS IN THIS
        trend_queue.put(cluster_queue.get())
        cluster_queue.task_done()

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
            return update

        trends = [make_trend_update(item) for item in items]
        res = remotesack.update_beans(urls, trends)
        logger.info("%d beans updated with trendscore", res)
        trend_queue.task_done()

def run_rectification():
    logger.info("Starting Rectification")    
    existing_categories = {cat[K_TEXT]: cat for cat in categorystore.find(projection={K_ID: 0})}
    bean_categories = ic([bean_cat for bean_cat in remotesack.beanstore.distinct("categories") if bean_cat not in existing_categories])

    # filter out the ones that do not exist

    # not needed anymore
    # while(True):
    #     beans = _augment(remotesack.get_beans(
    #         filter={
    #             "text": {"$exists": True},
    #             "embedding": {"$exists": False}
    #         },
    #         limit=100))
    #     if beans:
    #         res = remotesack.update_beans(
    #             [bean.url for bean in beans],
    #             [bean.model_dump(exclude_unset=True, exclude_none=True, by_alias=True) for bean in beans])
    #         logger.info("%d beans rectified", res)
    #     else:
    #         break 