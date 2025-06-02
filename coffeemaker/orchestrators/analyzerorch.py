import os
import logging
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
from persistqueue import Queue
from pymongo import UpdateOne
from azure.storage.queue import QueueClient
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.staticdb import *
from coffeemaker.nlp import embedders, agents
from coffeemaker.nlp.models import Digest
from coffeemaker.nlp.prompts import DIGESTOR_SYSTEM_PROMPT
from coffeemaker.orchestrators.utils import *
from icecream import ic

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv('BATCH_SIZE', 16*os.cpu_count()))
MAX_CLUSTER_SIZE = int(os.getenv('MAX_CLUSTER_SIZE', 128))
LAST_NDAYS = 2

is_indexable = lambda bean: above_threshold(bean.content, WORDS_THRESHOLD_FOR_INDEXING)
is_digestible = lambda bean: above_threshold(bean.content, WORDS_THRESHOLD_FOR_DIGESTING)
indexables = lambda beans: list(filter(is_indexable, beans)) if beans else beans
digestibles = lambda beans: list(filter(is_digestible, beans)) if beans else beans
index_storables = lambda beans: [bean for bean in beans if bean.embedding]
digest_storables = lambda beans: [bean for bean in beans if bean.gist]

_make_update_one = lambda url, update_fields: UpdateOne({K_ID: url}, { "$set": {k:v for k,v in update_fields.items() if v} }) 

def _make_cluster_update(bean: Bean, cluster: list[Bean]): 
    bean.cluster_id = bean.url
    bean.related = len(cluster)
    return UpdateMany(
        {
            K_ID: field_value([bean.url]+[related.url for related in cluster])
        },
        { 
            "$set": { 
                K_CLUSTER_ID: bean.url,
                K_RELATED: bean.related
            } 
        } 
    )

def _make_classification_update(bean: Bean, cat: list[str], sent: list[str]): 
    bean.categories = cat
    bean.sentiments = sent
    bean.tags = merge_tags(bean.categories, bean.tags)

    # if bean already exists append to it
    gist = f"C:{'|'.join(cat)};S:{'|'.join(sent)};"
    if bean.gist: bean.gist += ";"+gist
    else: bean.gist = gist

    return _make_update_one(
        bean.url, 
        {
            K_CATEGORIES: bean.categories,
            K_SENTIMENTS: bean.sentiments,
            K_TAGS: bean.tags,
            K_GIST: bean.gist
        }
    )    

def _make_digest_update(bean: Bean, digest: Digest):
    if not digest: return

    # if gist already exists then prepend to it
    gist = f"U:{bean.created.strftime('%Y-%m-%d')};"+digest.raw
    if bean.gist: bean.gist = gist+";"+bean.gist
    else: bean.gist = gist

    bean.regions = digest.regions
    bean.entities = digest.entities
    bean.tags = merge_tags(digest.regions, digest.entities, bean.tags)
    return _make_update_one(
        bean.url,
        {
            K_GIST: bean.gist,
            K_REGIONS: bean.regions,
            K_ENTITIES: bean.entities,
            K_TAGS: bean.tags
        }
    )

class Orchestrator:
    db: Beansack = None

    def __init__(self, 
        mongodb_conn_str: str, 
        db_name: str, 
        embedder_path: str = None, 
        embedder_context_len: int = 0,
        cluster_distance: float = 0,
        category_defs: str = None,
        sentiment_defs: str = None,
        digestor_path: str = None, 
        digestor_base_url: str = None,
        digestor_api_key: str = None,
        digestor_context_len: int = 0
    ): 
        self.db = Beansack(mongodb_conn_str, db_name)
        if embedder_path: self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        if cluster_distance: self.cluster_distance = cluster_distance
        if category_defs: self.categories = StaticDB(category_defs)
        if sentiment_defs: self.sentiments = StaticDB(sentiment_defs)
        if digestor_path: self.digestor = agents.from_path(
            model_path=digestor_path, base_url=digestor_base_url, api_key=digestor_api_key, 
            max_input_tokens=digestor_context_len, max_output_tokens=512, 
            system_prompt=DIGESTOR_SYSTEM_PROMPT, output_parser=Digest.parse_compressed, temperature=0.2
        )

    def embed_beans(self, beans: list[Bean]) -> list[Bean]|None:   
        if not beans: return beans

        embeddings = self.embedder.embed([bean.content for bean in beans])
        for bean, embedding in zip(beans, embeddings):
            bean.embedding = embedding

        beans = index_storables(beans)
        count = self.db.update_bean_fields(beans, [K_EMBEDDING])
        log.info("embedded", extra={"source": beans[0].source, "num_items": count})
        return beans

    def classify_beans(self, beans: list[Bean]) -> list[Bean]:
        if not beans: return beans

        cats = list(self.indexingexec.map(lambda bean: self.categories.vector_search(bean.embedding, limit=3), beans))
        sents = list(self.indexingexec.map(lambda bean: self.sentiments.vector_search(bean.embedding, limit=3), beans))
        updates = list(self.indexingexec.map(_make_classification_update, beans, cats, sents))
        self._queue_update(updates, "classified", beans[0].source)
        return beans
    
    find_cluster = lambda self, bean: self.db.vector_search_beans(embedding=bean.embedding, similarity_score=1-self.cluster_distance, filter={K_URL: {"$ne": bean.url} }, limit=MAX_CLUSTER_SIZE, project={K_URL: 1, K_RELATED: 1})

    # current clustering approach
    # new beans (essentially beans without cluster gets priority for defining cluster)
    # for each bean without a cluster_id (essentially a new bean) find the related beans within cluster_eps threshold
    # override their current cluster_id (if any) with the new bean's url   
    def cluster_beans(self, beans: list[Bean]):
        if not beans: return beans
        
        clusters = list(self.indexingexec.map(self.find_cluster, beans))
        updates = list(self.indexingexec.map(_make_cluster_update, beans, clusters))
        self._queue_update(updates, "clustered", beans[0].source)
        return beans  
        
    def digest_beans(self, beans: list[Bean]) -> list[Bean]|None:
        if not beans: return beans

        digests = self.digestor.run_batch([bean.content for bean in beans])
        updates = list(upd for upd in map(_make_digest_update, beans, digests) if upd)   
        count = self.db.update_beans(updates)
        log.info("digested", extra={"source": beans[0].source, "num_items": count}) 
        return digest_storables(beans)
    
    def _dequeue_beans(self, filter):
        while True:
            beans = self.db.query_beans(
                filter, 
                sort_by=NEWEST,
                project={K_URL: 1, K_CONTENT: 1, K_SOURCE: 1, K_TAGS: 1, K_CREATED: 1, K_TITLE: 1, K_GIST: 1},
                limit=BATCH_SIZE
            )
            if beans: yield beans
            else: break

    def _queue_update(self, updates, task, source):
        def _push_update(updates, task, source):
            count = self.db.update_beans(updates)
            log.info(task, extra={"source": source, "num_items": count}) 
        self.indexingexec.submit(_push_update, updates, task, source)
            
    @log_runtime(logger=log)
    def run_indexer(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting indexer", extra={"source": run_id, "num_items": 1})
        filter = {
            K_CREATED: {"$gte": ndays_ago(LAST_NDAYS)},
            K_EMBEDDING: {"$exists": False},
            K_NUM_WORDS_CONTENT: {"$gte": WORDS_THRESHOLD_FOR_INDEXING}
        }

        self.indexingexec = ThreadPoolExecutor(BATCH_SIZE, thread_name_prefix="indexer")
        with self.indexingexec: 
            for beans in self._dequeue_beans(filter):
                try:
                    beans = self.embed_beans(indexables(beans))
                    # self.classify_beans(beans)
                    # self.cluster_beans(beans)
                    self.indexingexec.submit(self.classify_beans, beans)
                    self.indexingexec.submit(self.cluster_beans, beans)
                    total += len(beans)
                except Exception as e:
                    log.error("failed indexing", extra={"source": run_id, "num_items": len(beans)})
                    log.exception(e, extra={"source": run_id, "num_items": len(beans)})

        log.info("total indexed", extra={"source": run_id, "num_items": total})

    @log_runtime(logger=log)
    def run_digestor(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting digestor", extra={"source": run_id, "num_items": 1})

        filter = {
            K_CREATED: {"$gte": ndays_ago(LAST_NDAYS)},
            K_GIST: {"$exists": False},
            K_NUM_WORDS_CONTENT: {"$gte": WORDS_THRESHOLD_FOR_DIGESTING},
            K_KIND: {"$ne": GENERATED}
        }
        for beans in self._dequeue_beans(filter):
            try:
                beans = self.digest_beans(digestibles(beans))
                total += len(beans)
            except Exception as e:
                log.error("failed digesting", extra={"source": run_id, "num_items": len(beans)})
                log.exception(e, extra={"source": run_id, "num_items": len(beans)})

        log.info("total digested", extra={"source": run_id, "num_items": total})

    
