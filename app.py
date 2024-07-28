import __init__
from icecream import ic
import os
from pybeansack import utils
from pybeansack.beansack import Beansack, Localsack
from pybeansack.embedding import BeansackEmbeddings
from pybeansack.datamodels import *
from persistqueue import Queue
from collectors import individual, rssfeed, ychackernews
from sklearn.cluster import DBSCAN
from langchain_groq import ChatGroq
from chains import DigestExtractor

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
WORKING_DIR = os.getenv("WORKING_DIR", CURR_DIR)
QUEUE_DIR=WORKING_DIR+"/.processingqueue"
CHROMA_DIR=WORKING_DIR+"/.chromadb"
INDEXING_EMBEDDER = WORKING_DIR+"/.models/gte-large-Q4.gguf"
QUERY_EMBEDDER = WORKING_DIR+"/.models/snowflake-arctic-Q4.GGUF"
# FEED_SOURCES = CURR_DIR+"/feedsources.txt"
FEED_SOURCES = [
    "https://crypto.news/feed/",
    "https://www.gearpatrol.com/feed/",
    "https://betanews.com/feed/",
    "https://www.engadget.com/rss.xml",
    "https://spacenews.com/feed/"
]
MIN_BODY_LEN = 1280 # ~200 words
MAX_CATEGORIES = 3
CATEGORY_EPS = 0.22
CLUSTER_EPS = 10.25
NO_CATEGORY = "Yo Momma"

logger = utils.create_logger("coffeemaker")
collect_queue = Queue(QUEUE_DIR+"/collect", tempdir=QUEUE_DIR)     
index_queue = Queue(QUEUE_DIR+"/index", tempdir=QUEUE_DIR)
cluster_queue = Queue(QUEUE_DIR+"/cluster", tempdir=QUEUE_DIR)
trend_queue = Queue(QUEUE_DIR+"/trend", tempdir=QUEUE_DIR)
localsack = Localsack(CHROMA_DIR, BeansackEmbeddings(INDEXING_EMBEDDER, 4095))
remotesack = Beansack(os.getenv('DB_CONNECTION_STRING'))
llm = ChatGroq(
    api_key=os.getenv('GROQ_API_KEY'), 
    model="llama3-8b-8192", 
    temperature=0.1, 
    verbose=False, 
    streaming=False)

def OLD_COLLECTOR():    
    # collect news articles and then rectify
    logger.info("Starting collection from rss feeds.")
    rssfeed.collect(sources=FEED_SOURCES, store_func=remotesack.store)
    logger.info("Starting collection from YC hackernews.")
    ychackernews.collect(store_func=remotesack.store)
    # TODO: add collection from reddit
    # TODO: add collection from nextdoor
    # TODO: add collection from linkedin
    logger.info("Starting large rectification.")
    remotesack.rectify_beansack(7, True, True)

chatter_trend_score = lambda chatter: (chatter.comments or 0)*3 + (chatter.likes or 0)
bean_trend_score = lambda chatter, cluster_size : chatter_trend_score(chatter)+10*cluster_size

def _is_valid_to_index(bean: Bean):
    # TODO: this would be different for posts and subreddits and group
    return bean.text and len(bean.text)>=MIN_BODY_LEN

def _download_beans(beans: list[Bean]) -> list[Bean]:
    for bean in beans:
        body = individual.load_from_url(bean.url)
        bean.text = body if (len(body or "") > len(bean.text or "")) else bean.text
    return beans

def _process_collection(items: list[Bean]|list[tuple[Bean, Chatter]]|list[Chatter]):
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
            downloaded = [bean for bean in _download_beans(localsack.filter_unstored_beans(beans)) if _is_valid_to_index(bean)]
            if downloaded:
                logger.info("%d new beans downloaded from %s", len(downloaded), downloaded[0].source)
                index_queue.put(downloaded)
        if chatters:
            # put them in database and queue the beans for trend_ranking
            for chatter in chatters:
                chatter.trend_score = chatter_trend_score(chatter)
            remotesack.store_chatters(chatters)
            trend_queue.put([chatter.url for chatter in chatters])
 
def _run_category_match(beans: list[Bean]):    
    embs = localsack.beanstore.get(ids=[bean.url for bean in beans], include=['embeddings'])
    matches = localsack.categorystore.query(query_embeddings=embs['embeddings'], n_results=3, include=['distances'])
    for index in range(len(beans)):
        dist = matches['distances'][index]
        beans[index].categories = [cat for i, cat in enumerate(matches['ids'][index]) if dist[i]<= CATEGORY_EPS] or [NO_CATEGORY]  
    return beans

def run_collector():
    logger.info("Starting collection from rss feeds.")
    rssfeed.collect(sources=FEED_SOURCES, store_func=_process_collection)
    logger.info("Starting collection from YC hackernews.")
    ychackernews.collect(store_func=_process_collection)
    # TODO: add collection from reddit
    # TODO: add collection from nextdoor
    # TODO: add collection from linkedin

def run_indexing():
    q_embdr = BeansackEmbeddings(QUERY_EMBEDDER, 2000)
    digestor = DigestExtractor(llm) 

    while not index_queue.empty():
        beans = localsack.filter_unstored_beans(index_queue.get()) 
        if beans:
            # add to localsack for clustering
            res = localsack.store_beans(beans)
            logger.info("%d new beans added to chromasack", res)   
            cluster_queue.put(beans) # set it out for clustering

            # this is the augmentation part
            _run_category_match(beans) 
            # extract digest and prepare for pushing to beansack
            for bean in beans:
                # extract digest
                try:
                    bean.embedding = q_embdr(bean.digest())[0]

                    digest = digestor.run(kind=bean.kind, text=bean.text)
                    bean.title = digest.title
                    bean.summary = digest.summary
                    bean.tags = digest.keyphrases or bean.tags
                    bean.highlights = digest.highlights                    
                except Exception:
                    logger.warning("Error extracting digest for %s", bean.url)
                
                bean.text = None # text field has no use any more and will just cause extra load 
            res = remotesack.store_beans(beans)
            logger.info("%d beans inserted to mongosack", res)

        index_queue.task_done()

def run_clustering():
    # TODO: in future optimize the beans that need to be clustered.
    # right now we are just clustering the whole database
    beans = localsack.beanstore.get(include=['embeddings'])
    dbscan = DBSCAN(eps = CLUSTER_EPS, min_samples=1)
    labels = dbscan.fit(beans['embeddings']).labels_
    
    groups = {}
    for index, label in enumerate(labels):
        label = int(label)
        if label not in groups:
            groups[label] = []
        groups[label].append(index)

    update_counter = 0
    for indices in groups.values():
        urls=[beans['ids'][index] for index in indices]
        # just take the first one of this cluster as a cluster_id. 
        # The exact value of the cluster_id does not matter
        update = {K_CLUSTER_ID: beans['ids'][indices[0]]} 
        update_counter += remotesack.update_beans(urls, update)
        localsack.beanstore.update(ids = urls, metadatas=[update]*len(indices)) # updating the localsack is not exactly necessary
    logger.info("%d beans updated with cluster_id", update_counter)

    # for sequential posterities sake
    while not cluster_queue.empty():
        # ONLY PUT URL POINTERS IN THIS
        trend_queue.put([bean.url for bean in cluster_queue.get()])
        cluster_queue.task_done()

def run_trend_ranking():
    while not trend_queue.empty():
        # remove the duplicates in the batch
        urls = list({url for url in trend_queue.get()})    
        chatters=remotesack.get_latest_chatter_stats(urls)
        cluster_sizes=localsack.get_cluster_size(urls)
        
        updates = list(map(
            lambda chatter, cluster_size: {K_LIKES: chatter.likes, K_COMMENTS: chatter.comments, K_TRENDSCORE: bean_trend_score(chatter, cluster_size)},
            chatters, cluster_sizes))
        res = remotesack.update_beans(urls, updates)
        logger.info("%d beans updated with trendscore", res)
        trend_queue.task_done()

run_collector()
run_indexing()
run_clustering()
run_trend_ranking()