from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from itertools import chain
from operator import add
import os
import logging
from icecream import ic

from persistqueue import Queue
from pymongo import UpdateOne
from azure.storage.queue import QueueClient
from coffeemaker.pybeansack.mongosack import VALUE_EXISTS, Beansack as MongoSack
from coffeemaker.pybeansack.models import *
from coffeemaker.nlp import digestors, embedders, utils
from coffeemaker.orchestrators.utils import *

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv('BATCH_SIZE', 16*os.cpu_count()))
MAX_CLUSTER_SIZE = int(os.getenv('MAX_CLUSTER_SIZE', 128))

is_indexable = lambda bean: above_threshold(bean.content, WORDS_THRESHOLD_FOR_INDEXING)
is_digestible = lambda bean: above_threshold(bean.content, WORDS_THRESHOLD_FOR_DIGESTING)
indexables = lambda beans: list(filter(is_indexable, beans)) if beans else beans
digestibles = lambda beans: list(filter(is_digestible, beans)) if beans else beans
index_storables = lambda beans: [bean for bean in beans if bean.embedding]
digest_storables = lambda beans: [bean for bean in beans if bean.gist]

def _make_cluster_updates(bean: Bean, cluster: list[Bean]): 
    updated_fields = { 
        "$set": { 
            K_CLUSTER_ID: bean.url,
            K_RELATED: len(cluster)
        } 
    } 
    return [UpdateOne({K_ID: related_bean.url}, updated_fields) for related_bean in cluster]+[UpdateOne({K_ID: bean.url},  updated_fields)]

def _make_classification_update(bean: Bean):
    updated_fields = {
        K_CATEGORIES: bean.categories,
        K_SENTIMENTS: bean.sentiments
    }
    updated_fields = {k:v for k,v in updated_fields.items() if v}
    return UpdateOne({K_ID: bean.url}, { "$set": updated_fields })

def _make_digest_update(bean: Bean):
    updated_fields = {
        K_GIST: bean.gist,
        K_REGIONS: bean.regions,
        K_ENTITIES: bean.entities,
        K_CATEGORIES: bean.categories,
        K_SENTIMENTS: bean.sentiments,
        K_TAGS: bean.tags
    }
    updated_fields = {k:v for k,v in updated_fields.items() if v}
    return UpdateOne({K_ID: bean.url}, { "$set": updated_fields })

class Orchestrator:
    db: MongoSack = None
    input_queue: QueueClient = None
    output_queues: list[QueueClient] = None
    processing_queue: Queue = None

    def __init__(self, 
        mongodb_conn_str: str, 
        db_name: str, 
        azqueue_conn_str: str = None,
        input_queue_name: str = None,
        output_queue_names: list[str] = None,
        embedder_path: str = None, 
        embedder_context_len: int = None,
        cluster_eps: float = 0,
        digestor_path: str = None, 
        digestor_base_url: str = None,
        digestor_api_key: str = None,
        digestor_context_len: int = None
    ): 
        self.db = MongoSack(mongodb_conn_str, db_name)
        self.processing_queue = Queue(".processingqueue", tempdir=".")

        if input_queue_name: self.input_queue = QueueClient.from_connection_string(azqueue_conn_str, input_queue_name)
        if output_queue_names: self.output_queues = initialize_azqueues(azqueue_conn_str, output_queue_names)
        if embedder_path: self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        if cluster_eps: self.cluster_eps = cluster_eps
        if digestor_path: self.digestor = digestors.from_path(digestor_path, digestor_context_len, base_url=digestor_base_url, api_key=digestor_api_key)

    def embed_beans(self, beans: list[Bean]) -> list[Bean]|None:   
        if not beans: return beans

        embeddings = self.embedder.embed([bean.content for bean in beans])
        for bean, embedding in zip(beans, embeddings):
            bean.embedding = embedding

        beans = index_storables(beans)
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
        count = self.db.update_beans(list(map(_make_classification_update, beans)))
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

        find_cluster = lambda bean: self.db.vector_search_beans(embedding=bean.embedding, similarity_score=1-self.cluster_eps, filter={K_URL: {"$ne": bean.url} }, limit=MAX_CLUSTER_SIZE, project={K_URL: 1, K_RELATED: 1})
        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="cluster") as executor:
            clusters = list(executor.map(find_cluster, beans))

        count = self.db.update_beans(list(chain(*map(_make_cluster_updates, beans, clusters))))
        log.info("clustered", extra={"source": beans[0].source, "num_items": count})
        return beans  
    
    # def _push_updates(self, task, source, updates):
    #     self.db.custom_update_beans(updates)
    #     log.info(task, extra={"source": source, "num_items": len(updates)})
        
    def digest_beans(self, beans: list[Bean]) -> list[Bean]|None:
        if not beans: return beans

        try:
            digests = self.digestor.run_batch([bean.content for bean in beans])
            for bean, digest in zip(beans, digests):
                if not digest: 
                    log.error("failed digesting", extra={"source": bean.url, "num_items": 1})
                    continue

                bean.gist = f"U:{bean.created.strftime('%Y-%m-%d')};"+digest.expr
                bean.regions = digest.regions
                bean.entities = digest.entities
                bean.categories = digest.categories
                bean.sentiments = digest.sentiments
                bean.tags = utils.merge_tags(digest.regions, digest.entities, digest.categories)
                
        except Exception as e:
            log.error("failed digesting", extra={"source": beans[0].source, "num_items": len(beans)})
            log.exception(e, extra={"source": beans[0].source, "num_items": len(beans)})

        beans = digest_storables(beans)
        count = self.db.update_beans(list(map(_make_digest_update, beans)))
        log.info("digested", extra={"source": beans[0].source, "num_items": count})
        return beans
    
    @log_runtime(logger=log)
    def run_indexer(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting indexer", extra={"source": run_id, "num_items": 1})

        for urls in dequeue_batch(self.input_queue, BATCH_SIZE):
            beans = self.db.query_beans(
                {
                    K_URL: {"$in": urls}, 
                    K_CONTENT: VALUE_EXISTS
                }, 
                project={K_URL: 1, K_CONTENT: 1, K_SOURCE: 1}
            )
            try:
                beans = self.embed_beans(beans)
                with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="indexer") as executor:
                    # executor.submit(self.classify_beans, beans)
                    executor.submit(self.cluster_beans, beans)
                total += len(beans)
            except Exception as e:
                log.error("failed indexing", extra={"source": run_id, "num_items": len(urls)})
                log.exception(e, extra={"source": run_id, "num_items": len(urls)})

        log.info("total indexed", extra={"source": run_id, "num_items": total})

    @log_runtime(logger=log)
    def run_digestor(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting digestor", extra={"source": run_id, "num_items": 1})

        for urls in dequeue_batch(self.input_queue, BATCH_SIZE):
            beans = self.db.query_beans(
                {
                    K_URL: {"$in": urls}, 
                    K_CREATED: {"$gte": datetime.now() - timedelta(days=2)},
                    K_CONTENT: VALUE_EXISTS
                }, 
                project={K_URL: 1, K_CONTENT: 1, K_SOURCE: 1, K_CREATED: 1, K_CATEGORIES: 1}
            )
            try:
                beans = self.digest_beans(digestibles(beans))
                total += len(beans)
            except Exception as e:
                log.error("failed digesting", extra={"source": run_id, "num_items": len(urls)})
                log.exception(e, extra={"source": run_id, "num_items": len(urls)})

        log.info("total digested", extra={"source": run_id, "num_items": total})

    
