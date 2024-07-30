from __init__ import *
from icecream import ic
from pybeansack import utils
from pybeansack.embedding import BeansackEmbeddings
from pybeansack.datamodels import *
from collectors import individual, rssfeed, ychackernews
from sklearn.cluster import DBSCAN
from chains import DigestExtractor, Summarizer

FEED_SOURCES = CURR_DIR+"/feedsources.txt"
QUEUE_DIR=WORKING_DIR+"/.processingqueue"
CLEANUP_WINDOW = 30
MIN_BODY_LEN = 1280 # ~200 words
MAX_CATEGORIES = 3
CATEGORY_EPS = 0.25
CLUSTER_EPS = 10.25

logger = utils.create_logger("coffeemaker")
collect_queue = Queue(QUEUE_DIR+"/collect", tempdir=QUEUE_DIR)     
index_queue = Queue(QUEUE_DIR+"/index", tempdir=QUEUE_DIR)
cluster_queue = Queue(QUEUE_DIR+"/cluster", tempdir=QUEUE_DIR)
trend_queue = Queue(QUEUE_DIR+"/trend", tempdir=QUEUE_DIR)
llm = ChatGroq(api_key=os.getenv('GROQ_API_KEY'), model="llama3-8b-8192", temperature=0.1, verbose=False, streaming=False)

def run_cleanup():
    logger.info("Starting clean up")
    remotesack.delete_old(CLEANUP_WINDOW)
    localsack.delete_old(CLEANUP_WINDOW)

def run_collector():
    logger.info("Starting collection from rss feeds.")
    rssfeed.collect(sources=FEED_SOURCES, store_func=_process_collection)
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
            downloaded = [bean for bean in _download_beans(localsack.filter_unstored_beans(beans)) if _is_valid_to_index(bean)]
            if downloaded:
                logger.info("%d new beans downloaded from %s", len(downloaded), downloaded[0].source)
                index_queue.put(downloaded)
        if chatters:            
            trend_queue.put(chatters)

def _download_beans(beans: list[Bean]) -> list[Bean]:
    for bean in beans:
        body = individual.load_from_url(bean.url)
        bean.text = body if (len(body or "") > len(bean.text or "")) else bean.text
    return beans

def run_indexing():
    q_embdr = BeansackEmbeddings(QUERY_EMBEDDER, 2000)
    digestor = DigestExtractor(llm, 3072) 
    summarizer = Summarizer(llm, 3072)

    logger.info("Starting Indexing")
    while not index_queue.empty():
        beans = localsack.filter_unstored_beans(index_queue.get()) 
        if beans:
            # add to localsack for clustering
            res = localsack.store_beans(beans)
            logger.info("%d beans added to chromasack", res)   
            cluster_queue.put(beans) # set it out for clustering

            # this is the augmentation part
            _run_category_match(beans) 
            # extract digest and prepare for pushing to beansack
            for bean in beans:
                # extract digest
                try:
                    bean.embedding = q_embdr(bean.digest())[0]

                    digest = digestor.run(kind=bean.kind, text=bean.text)
                    # in the current model the generated title looks more of a click bait than the original title
                    # the summary content is too concise
                    # bean.title = digest.title
                    # bean.summary = digest.summary
                    bean.tags = digest.keyphrases or bean.tags
                    bean.highlights = digest.highlights    

                    bean.summary = summarizer.run(bean.text)       
                except Exception:
                    logger.warning("Error extracting digest for %s", bean.url)
                
                bean.text = None # text field has no use any more and will just cause extra load 
            res = remotesack.store_beans(beans)
            logger.info("%d beans inserted to mongosack", res)

        index_queue.task_done()

def _run_category_match(beans: list[Bean]):    
    embs = localsack.beanstore.get(ids=[bean.url for bean in beans], include=['embeddings'])
    matches = local_categorystore.query(query_embeddings=embs['embeddings'], n_results=3, include=['distances'])
    for index in range(len(beans)):
        dist = matches['distances'][index]
        cats = [cat for i, cat in enumerate(matches['ids'][index]) if dist[i]<= CATEGORY_EPS]  
        beans[index].categories = cats or None
    return beans

def run_clustering():
    logger.info("Starting Clustering")
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
        urls=ic([beans['ids'][index] for index in indices])
        # just take the first one of this cluster as a cluster_id. 
        # The exact value of the cluster_id does not matter
        update = {K_CLUSTER_ID: beans['ids'][indices[0]]} 
        update_counter += remotesack.update_beans(urls, update)
        localsack.beanstore.update(ids = urls, metadatas=[update]*len(urls)) # updating the localsack is not exactly necessary
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
  
        items = {item.url: item for item in items}
        urls, items = list(items.keys()), list(items.values())

        chatters=remotesack.get_latest_chatter_stats(urls)
        cluster_sizes=localsack.count_related_beans(urls) # technically i can get this from remotesack as well

        # trend_score = likes + 3*comments + 10*count_related_beans
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
        localsack.update_beans(urls, trends) # updating the localsack is not exactly necessary
        logger.info("%d beans updated with trendscore", res)
        trend_queue.task_done()

# run_cleanup()
# run_collector()
run_indexing()
run_clustering()
run_trend_ranking()

