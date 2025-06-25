from itertools import chain
import json
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from pymongo import UpdateOne
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.staticdb import *
from coffeemaker.nlp import embedders, agents, Digest, DIGEST_SYSTEM_PROMPT
from coffeemaker.orchestrators.utils import *
from azure.storage.blob import BlobType
from icecream import ic

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv('BATCH_SIZE', os.cpu_count()))
MAX_RELATED = int(os.getenv('MAX_RELATED', 128))
MAX_RELATED_NDAYS = int(os.getenv('MAX_RELATED_NDAYS', 7))
MAX_RELATED_EPS=float(os.getenv("MAX_RELATED_EPS", 0.15))
MAX_ANALYZE_NDAYS =  int(os.getenv('MAX_ANALYZE_NDAYS', 2))

index_storables = lambda beans: [bean for bean in beans if bean.embedding]
digest_storables = lambda beans: [bean for bean in beans if bean.gist]

def _make_update_one(url, update_fields):
    update_fields = {k:v for k,v in update_fields.items() if v}
    tags = update_fields.pop(K_TAGS, None)
    update = {"$set": update_fields}
    if tags: update["$addToSet"] = {
        K_TAGS: {
            "$each": tags if isinstance(tags, list) else [tags]
        }
    }
    return UpdateOne({K_ID: url}, update)

def _make_cluster_update(bean: Bean, cluster: list[Bean]): 
    bean.cluster_id = bean.url
    bean.related = len(cluster)
    update_fields = { 
        K_CLUSTER_ID: bean.cluster_id,
        K_RELATED: bean.related
    } 
    return list(map(_make_update_one, cluster, [update_fields]*len(cluster)))

def _make_classification_update(bean: Bean, cat: list[str], sent: list[str]): 
    bean.categories = cat
    bean.sentiments = sent
    bean.tags = merge_tags(bean.categories)
    return _make_update_one(
        bean.url, 
        {
            K_CATEGORIES: bean.categories,
            K_SENTIMENTS: bean.sentiments,
            K_TAGS: bean.tags
        }
    )    

def _make_digest_update(bean: Bean, digest: Digest):
    if not digest: return    
    bean.regions = digest.regions
    bean.entities = digest.entities
    bean.tags = merge_tags(bean.regions, bean.entities)
    bean.gist = digest.raw
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
    dbworker = None
    backup_container = None    

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
        digestor_context_len: int = 0,
        backup_azstorage_conn_str: str = None,
        batch_size: int = BATCH_SIZE
    ): 
        self.db = Beansack(mongodb_conn_str, db_name)
        self.dbworker = ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="dbupdater")
        self.batch_size = batch_size

        if embedder_path: self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        if category_defs: self.categories = StaticDB(category_defs)
        if sentiment_defs: self.sentiments = StaticDB(sentiment_defs)
        if digestor_path: self.digestor = agents.from_path(
            model_path=digestor_path, base_url=digestor_base_url, api_key=digestor_api_key, 
            max_input_tokens=digestor_context_len, max_output_tokens=400, 
            output_parser=Digest.parse_compressed
        )
        if backup_azstorage_conn_str: self.backup_container = initialize_azblobstore(backup_azstorage_conn_str, "analyzer")
    
    def embed_beans(self, beans: list[Bean]) -> list[Bean]|None:   
        if not beans: return beans

        # this is a cpu heavy calculation. run it on the main thread and let the nlp take care of it
        embeddings = self.embedder.embed_documents([bean.content for bean in beans])

        # these are simple calculations. threading causes more time loss
        for bean, embedding in zip(beans, embeddings):
            bean.embedding = embedding
        beans = index_storables(beans)

        self.dbworker.submit(self.cluster_db.store_items, [bean.model_dump(include=["id", K_EMBEDDING], by_alias=True) for bean in beans])
        self.dbworker.submit(self.db.update_bean_fields, beans, [K_EMBEDDING])
        log.info("embedded", extra={"source": beans[0].source, "num_items": len(beans)})
        return beans

    def classify_beans(self, beans: list[Bean]) -> list[Bean]:
        if not beans: return beans

        # these are IO heavy so create thread pools
        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="classify") as exec:
            cats = list(exec.map(lambda bean: self.categories.vector_search(bean.embedding, limit=3), beans))
            sents = list(exec.map(lambda bean: self.sentiments.vector_search(bean.embedding, limit=3), beans))

        # these are simple calculations. threading causes more time loss
        updates = list(map(_make_classification_update, beans, cats, sents)) 
        self._push_update(updates, "classified", beans[0].source)
        return beans
    
    find_cluster = lambda self, bean: self.cluster_db.vector_search(embedding=bean.embedding, max_distance=MAX_RELATED_EPS, limit=MAX_RELATED, metric="l2")

    # current clustering approach
    # new beans (essentially beans without cluster gets priority for defining cluster)
    # for each bean without a cluster_id (essentially a new bean) find the related beans within cluster_eps threshold
    # override their current cluster_id (if any) with the new bean's url   
    def cluster_beans(self, beans: list[Bean]):
        if not beans: return beans

        # these are IO heavy so create thread pools
        with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="cluster") as exec:
            clusters = list(exec.map(self.find_cluster, beans))

        # these are simple calculations. threading causes more time loss
        updates = list(chain(*map(_make_cluster_update, beans, clusters)))
        self._push_update(updates, "clustered", beans[0].source)
        return beans  
        
    def digest_beans(self, beans: list[Bean]) -> list[Bean]|None:
        if not beans: return beans

        # this is a cpu heavy calculation. run it on the main thread and let the nlp take care of it
        digests = self.digestor.run_batch([bean.content for bean in beans])
        # these are simple calculations. threading causes more time loss
        updates = list(upd for upd in map(_make_digest_update, beans, digests) if upd)   
        self._push_update(updates, "digested", beans[0].source)
        self._backup_digests(beans)
        return digest_storables(beans)
    
    def stream_beans(self, filter):
        while True:
            beans = self.db.query_beans(
                filter, 
                sort_by=NEWEST,
                project={
                    K_ID: 1, K_URL: 1, 
                    K_CONTENT: 1, 
                    K_SOURCE: 1
                },
                limit=self.batch_size
            )            
            if beans: yield beans
            else: break

    def _backup_digests(self, beans: list[Bean]):
        if not self.backup_container or not beans: return 
        trfile = "digests-"+now().strftime("%Y-%m-%d")+".jsonl"
        def backup():
            items = map(lambda b: json.dumps({'article': b.content, "summary": b.gist})+"\n", beans)
            try: self.backup_container.upload_blob(trfile, "".join(items), BlobType.APPENDBLOB)           
            except Exception as e: log.warning(f"backup failed - {e}", extra={'source': trfile, 'num_items': len(beans)})
        self.dbworker.submit(backup)

    def _push_update(self, updates, task, source):
        def push():
            count = self.db.update_beans(updates)
            log.info(task, extra={"source": source, "num_items": count}) 
        self.dbworker.submit(push)

    def _hydrate_cluster_db(self):
        beans = list(self.db.beanstore.find(
            filter = {
                K_CREATED: {"$gte": ndays_ago(MAX_RELATED_NDAYS)},
                K_EMBEDDING: {"$exists": True}
            }, 
            projection = {K_ID: 1, K_EMBEDDING: 1}
        ))
        self.cluster_db.store_items(beans)
        log.info("cluster db loaded", extra={'source': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'num_items': len(beans)})
            
    @log_runtime(logger=log)
    def run_indexer(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filter = {
            K_COLLECTED: {"$gte": ndays_ago(MAX_ANALYZE_NDAYS)},
            K_EMBEDDING: {"$exists": False},
            K_NUM_WORDS_CONTENT: {"$gte": WORDS_THRESHOLD_FOR_INDEXING}
        }
        self.cluster_db = StaticDB()

        log.info("starting indexer", extra={"source": run_id, "num_items": os.cpu_count()})        
        with self.dbworker:
            self.dbworker.submit(self._hydrate_cluster_db)
            for beans in self.stream_beans(filter):
                try:
                    beans = self.embed_beans(beans)
                    self.classify_beans(beans)
                    self.cluster_beans(beans)
                    total += len(beans)
                except Exception as e:
                    log.error(f"failed indexing - {e}", extra={"source": run_id, "num_items": len(beans)})
        log.info("total indexed", extra={"source": run_id, "num_items": total})

    @log_runtime(logger=log)
    def run_digestor(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filter = {
            K_COLLECTED: {"$gte": ndays_ago(MAX_ANALYZE_NDAYS)},
            K_GIST: {"$exists": False},
            K_NUM_WORDS_CONTENT: {"$gte": WORDS_THRESHOLD_FOR_DIGESTING},
            K_KIND: {"$ne": GENERATED}
        }

        log.info("starting digestor", extra={"source": run_id, "num_items": os.cpu_count()})
        with self.dbworker:
            for beans in self.stream_beans(filter):
                try:
                    beans = self.digest_beans(beans)
                    total += len(beans)
                except Exception as e:
                    log.error(f"failed digesting - {e}", extra={"source": run_id, "num_items": len(beans)})
        log.info("total digested", extra={"source": run_id, "num_items": total})

    
