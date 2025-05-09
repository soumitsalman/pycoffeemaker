from concurrent.futures import ThreadPoolExecutor
from itertools import chain
import json
from operator import add
import os
import logging
import asyncio
from icecream import ic

from pymongo import UpdateOne
from azure.storage.queue import QueueClient
from coffeemaker.pybeansack.ducksack import Beansack as DuckSack
from coffeemaker.pybeansack.mongosack import Beansack as MongoSack
from coffeemaker.pybeansack.models import *
from coffeemaker.nlp import embedders, utils
from coffeemaker.orchestrators.utils import *

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv('INDEXER_BATCH_SIZE', 16*os.cpu_count()))
MAX_CLUSTER_SIZE = int(os.getenv('MAX_CLUSTER_SIZE', 128))

is_indexable = lambda bean: above_threshold(bean.content, WORDS_THRESHOLD_FOR_INDEXING)
is_storable = lambda bean: bool(bean.embedding) # if there is no embedding then no point storing
storables = lambda beans: list(map(is_storable, beans)) if beans else beans 

class Orchestrator:
    db: MongoSack = None
    queue: QueueClient = None
    embedder: embedders.Embeddings = None
    cluster_eps: float = None

    def __init__(self, 
        db_path: str, 
        db_name: str, 
        queue_path: str,
        queue_name: str,
        embedder_path: str = os.getenv("EMBEDDER_PATH"), 
        embedder_context_len: int = int(os.getenv("EMBEDDER_CONTEXT_LEN", embedders.CONTEXT_LEN)),
        cluster_eps: float = float(os.getenv("CLUSTER_EPS", 3))
    ): 
        self.db = MongoSack(db_path, db_name)
        self.queue = QueueClient.from_connection_string(queue_path, queue_name)
        self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        self.cluster_eps = cluster_eps            
 
    def embed_beans(self, beans: list[Bean]) -> list[Bean]|None:   
        if not beans: return beans

        # TODO: do some splitting here and then average it out
        embeddings = self.embedder.embed([bean.content for bean in beans])
        for bean, embedding in zip(beans, embeddings):
            bean.embedding = embedding

        beans = storables(beans)
        make_update = lambda bean: UpdateOne({K_ID: bean.url}, {"$set": {K_EMBEDDING: bean.embedding}})
        count = self.db.update_beans(list(map(make_update, beans)))
        log.info("embedded", extra={"source": beans[0].source, "num_items": count})
        return beans

    def classify_beans(self, beans: list[Bean]) -> list[Bean]:
        if not beans: return beans
        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="classification") as executor:
            pass
            # TODO: search sentiments
            # TODO: search categories

        make_update = lambda bean: UpdateOne(
            {K_ID: bean.url}, 
            {
                "$set": {
                    K_CATEGORIES: bean.categories,
                    "sentiments": bean.sentiments
                }
            }
        )
        count = self.db.update_beans(list(map(make_update, beans)))
        log.info("classified", extra={"source": beans[0].source, "num_items": count})
        return beans
    
    # current clustering approach
    # new beans (essentially beans without cluster gets priority for defining cluster)
    # for each bean without a cluster_id (essentially a new bean) fine the related beans within cluster_eps threshold
    # override their current cluster_id (if any) with the new bean's url
    # a bean is already in the update list then skip that one to include as a related item
    # if a new bean is found related to the query new bean, then it should be skipped for finding related beans for itself
    # keep running this loop through the whole collection until there is no bean without cluster id left
    # every time we find a cluster (a set of related beans) we add it to the update collection and return
    def cluster_beans(self, beans: list[Bean]):
        if not beans: return beans

        find_cluster = lambda bean: self.db.vector_search_beans(bean.embedding, 1-self.cluster_eps, filter={K_URL: {"$ne": bean.url} }, limit=MAX_CLUSTER_SIZE, projection={K_URL: 1})
        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="cluster") as executor:
            clusters = list(executor.map(find_cluster, beans))

        make_update = lambda bean, cluster: [UpdateOne({K_ID: related.url }, { "$set": { K_CLUSTER_ID: bean.url } }) for related in cluster]+[UpdateOne({K_ID: bean.url}, {"$set": { K_CLUSTER_ID: bean.url}})]
        updates = list(chain(*map(make_update, beans, clusters)))
        count = self.db.update_beans(updates)
        log.info("clustered", extra={"source": beans[0].source, "num_items": count})
        return beans  
    
    def _process_msg(self, msg):
        self.queue.delete_message(msg)
        return msg.content

    @log_runtime(logger=log)
    def run(self):
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting indexer", extra={"source": run_id, "num_items": 1})

        for batch in self.queue.receive_messages(messages_per_page=min(MAX_QUEUE_PAGE, BATCH_SIZE)).by_page():
            urls = list(map(self._process_msg, batch))
            try:
                beans = self.db.query_beans({K_URL: {"$in": urls}}, projection={K_URL: 1, K_CONTENT: 1, K_SOURCE: 1})
                beans = self.embed_beans(beans)
                with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="indexer") as executor:
                    executor.submit(self.classify_beans, beans)
                    executor.submit(self.cluster_beans, beans)
            except Exception as e:
                log.error("failed indexing", extra={"source": run_id, "num_items": len(urls)})
                ic(e)

        log.info("completed indexer", extra={"source": run_id, "num_items": 1})

