import os
import logging
from concurrent.futures import ThreadPoolExecutor
from pymongo import UpdateOne
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.staticdb import *
from coffeemaker.nlp import embedders, agents, Digest, DIGEST_SYSTEM_PROMPT
from coffeemaker.orchestrators.utils import *
from icecream import ic

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv('BATCH_SIZE', os.cpu_count()))
MAX_RELATED = int(os.getenv('MAX_RELATED', 128))
MAX_RELATED_NDAYS = int(os.getenv('MAX_RELATED_NDAYS', 7))
MAX_RELATED_EPS=float(os.getenv("MAX_RELATED_EPS", 0.2))
MAX_ANALYZE_NDAYS =  int(os.getenv('MAX_ANALYZE_NDAYS', 2))

index_storables = lambda beans: [bean for bean in beans if bean.embedding]
digest_storables = lambda beans: [bean for bean in beans if bean.gist]
_make_update_one = lambda url, update_fields: UpdateOne({K_ID: url}, { "$set": {k:v for k,v in update_fields.items() if v} }) 

def _make_cluster_update(bean: Bean, cluster: list[Bean]): 
    bean.cluster_id = bean.url
    bean.related = len(cluster)
    return UpdateMany(
        {
            K_ID: field_value([bean.url]+cluster)
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
    if bean.gist: bean.gist += f";C:{'|'.join(bean.categories)};S:{'|'.join(bean.sentiments)};"
    
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
    
    bean.regions = digest.regions
    bean.entities = digest.entities
    bean.tags = merge_tags(bean.regions, bean.entities, bean.tags)
    # if gist already exists then prepend to it
    bean.gist = f"U:{bean.created.strftime('%Y-%m-%d')};{digest.raw};"
    if bean.categories or bean.sentiments: bean.gist += f"C:{'|'.join(bean.categories)};S:{'|'.join(bean.sentiments)};"
    
    return _make_update_one(
        bean.url,
        {
            K_REGIONS: bean.regions,
            K_ENTITIES: bean.entities,
            K_TAGS: bean.tags,
            K_GIST: bean.gist
        }
    )

class Orchestrator:
    db: Beansack = None

    def __init__(self, 
        mongodb_conn_str: str, 
        db_name: str, 
        embedder_path: str = None, 
        embedder_context_len: int = 0,       
        category_defs: str = None,
        sentiment_defs: str = None,
        digestor_path: str = None, 
        digestor_base_url: str = None,
        digestor_api_key: str = None,
        digestor_context_len: int = 0
    ): 
        self.db = Beansack(mongodb_conn_str, db_name)
        if embedder_path: self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        if category_defs: self.categories = StaticDB(filepath=category_defs)
        if sentiment_defs: self.sentiments = StaticDB(filepath=sentiment_defs)
        if digestor_path: self.digestor = agents.from_path(
            model_path=digestor_path, base_url=digestor_base_url, api_key=digestor_api_key, 
            max_input_tokens=digestor_context_len, max_output_tokens=512, 
            system_prompt=DIGEST_SYSTEM_PROMPT, output_parser=Digest.parse_compressed, temperature=0.2
        )
    
    def embed_beans(self, beans: list[Bean]) -> list[Bean]|None:   
        if not beans: return beans

        embeddings = self.embedder.embed([bean.content for bean in beans])
        for bean, embedding in zip(beans, embeddings):
            bean.embedding = embedding

        beans = index_storables(beans)
        self.cluster_db.store_items([bean.model_dump(include=["id", K_EMBEDDING], by_alias=True) for bean in beans])
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
    
    find_cluster = lambda self, bean: self.cluster_db.vector_search(embedding=bean.embedding, max_distance=MAX_RELATED_EPS, limit=MAX_RELATED, metric="l2")

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
    
    def stream_beans(self, filter):
        while True:
            beans = self.db.query_beans(
                filter, 
                sort_by=NEWEST,
                project={
                    K_ID: 1, K_URL: 1, 
                    K_CONTENT: 1,  K_CREATED: 1, 
                    K_SOURCE: 1, K_TITLE: 1,
                    K_GIST: 1, K_TAGS: 1, K_CATEGORIES: 1, K_SENTIMENTS: 1
                },
                limit=BATCH_SIZE
            )            
            if beans: yield beans
            else: break

    def _queue_update(self, updates, task, source):
        def _push_update(updates, task, source):
            count = self.db.update_beans(updates)
            log.info(task, extra={"source": source, "num_items": count}) 
        self.indexingexec.submit(_push_update, updates, task, source)

    def _hydrate_cluster_db(self):
        beans = list(self.db.beanstore.find(
            filter = {
                K_CREATED: {"$gte": ndays_ago(MAX_RELATED_NDAYS)},
                K_EMBEDDING: {"$exists": True}
            }, 
            projection = {K_ID: 1, K_EMBEDDING: 1}
        ))
        self.cluster_db = StaticDB(json_data=beans)
        return len(beans)
            
    @log_runtime(logger=log)
    def run_indexer(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filter = {
            K_CREATED: {"$gte": ndays_ago(MAX_ANALYZE_NDAYS)},
            K_EMBEDDING: {"$exists": False},
            K_NUM_WORDS_CONTENT: {"$gte": WORDS_THRESHOLD_FOR_INDEXING}
        }

        log.info("starting indexer", extra={"source": run_id, "num_items": 1})
        count = self._hydrate_cluster_db()
        log.info("cluster db loaded", extra={'source': run_id, 'num_items': count})

        self.indexingexec = ThreadPoolExecutor(BATCH_SIZE, thread_name_prefix="indexer")
        with self.indexingexec: 
            for beans in self.stream_beans(filter):
                try:
                    beans = self.embed_beans(beans)
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
        filter = {
            K_CREATED: {"$gte": ndays_ago(MAX_ANALYZE_NDAYS)},
            K_GIST: {"$exists": False},
            K_NUM_WORDS_CONTENT: {"$gte": WORDS_THRESHOLD_FOR_DIGESTING},
            K_KIND: {"$ne": GENERATED}
        }
        log.info("starting digestor", extra={"source": run_id, "num_items": 1})

        for beans in self.stream_beans(filter):
            try:
                beans = self.digest_beans(beans)
                total += len(beans)
            except Exception as e:
                log.error("failed digesting", extra={"source": run_id, "num_items": len(beans)})
                log.exception(e, extra={"source": run_id, "num_items": len(beans)})

        log.info("total digested", extra={"source": run_id, "num_items": total})

    
