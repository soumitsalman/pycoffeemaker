from itertools import chain
import json
import os
import logging
from concurrent.futures import ThreadPoolExecutor
# from pymongo import UpdateOne
from coffeemaker.pybeansack import mongosack, warehouse
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.utils import *
from coffeemaker.nlp import embedders, agents, Digest, DIGEST_SYSTEM_PROMPT
from coffeemaker.orchestrators.utils import *
from icecream import ic

log = logging.getLogger(__name__)

CACHE_DIR = ".cache"
BATCH_SIZE = int(os.getenv('BATCH_SIZE', os.cpu_count()))
MAX_RELATED = int(os.getenv('MAX_RELATED', 128))
MAX_RELATED_NDAYS = int(os.getenv('MAX_RELATED_NDAYS', 7))
MAX_RELATED_EPS=float(os.getenv("MAX_RELATED_EPS", 0.48))
MAX_ANALYZE_NDAYS =  int(os.getenv('MAX_ANALYZE_NDAYS', 2))

index_storables = lambda beans: [bean for bean in beans if bean.embedding]
digest_storables = lambda beans: [bean for bean in beans if bean.gist]

class Orchestrator:
    db: warehouse.Beansack|mongosack.Beansack = None
    dbworker = None
    backup_container = None    

    def __init__(self, 
        ducklake_conn: tuple[str, str] = None,
        mongodb_conn: tuple[str, str] = None,
        embedder_path: str = None, 
        embedder_context_len: int = 0,               
        digestor_path: str = None, 
        digestor_base_url: str = None,
        digestor_api_key: str = None,
        digestor_context_len: int = 0,
        batch_size: int = BATCH_SIZE
    ): 
        if ducklake_conn: self.db = warehouse.Beansack(catalogdb=ducklake_conn[0], storagedb=ducklake_conn[1], factory_dir=os.getenv('FACTORY_DIR', '../factory'))
        elif mongodb_conn: self.db = mongosack.Beansack(mongodb_conn[0], mongodb_conn[1])
        else: raise ValueError("Either mongodb_connection or ducklake_connection must be provided")

        if embedder_path: self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        if digestor_path: self.digestor = agents.text2text_agent_from_path(
            model_path=digestor_path, base_url=digestor_base_url, api_key=digestor_api_key, 
            max_input_tokens=digestor_context_len, max_output_tokens=400, 
            output_parser=Digest.parse_compressed
        )
        self.batch_size = batch_size
    
    def embed_beans(self, beans: list[BeanCore]) -> list[BeanEmbedding]|None:   
        if not beans: return beans

        # this is a cpu heavy calculation. run it on the main thread and let the nlp take care of it
        embeddings = self.embedder.embed_documents([bean.content or bean.summary for bean in beans])
        embs = [BeanEmbedding(url=b.url, embedding=e) for b, e in zip(beans, embeddings) if e]
        self.db.store_embeddings(embs)
        log.info("embedded", extra={"source": beans[0].source, "num_items": len(beans)})
        return beans  
        
    def digest_beans(self, beans: list[BeanCore]) -> list[BeanGist]|None:
        if not beans: return beans

        # this is a cpu heavy calculation. run it on the main thread and let the nlp take care of it
        digests = self.digestor.run_batch([bean.content for bean in beans])
        gists = [BeanGist(url=b.url, gist=d.raw, regions=d.regions, entities=d.entities) for b, d in zip(beans, digests) if d and d.raw]
        self.db.store_gists(gists)
        log.info("digested", extra={"source": beans[0].source, "num_items": len(gists)})

        return gists
    
    def stream_beans(self, filter):
        while True:
            beans = self.db.query_unprocessed_beans(
                filter,
                limit=self.batch_size
            )            
            if beans: yield beans
            else: break
           
    @log_runtime(logger=log)
    def run_indexer(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filter = [
            f"{K_EMBEDDING} IS NULL",
            f"{K_CREATED} >= '{ndays_ago_str(MAX_ANALYZE_NDAYS)}'",
            f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_INDEXING}"
        ]

        log.info("starting indexer", extra={"source": run_id, "num_items": os.cpu_count()})        
        # for beans in self.stream_beans(filter):
        #     try:
        #         beans = self.embed_beans(beans)
        #         total += len(beans)
        #     except Exception as e:
        #         log.error(f"failed indexing - {e}", extra={"source": run_id, "num_items": len(beans)})        
        # log.info("total indexed", extra={"source": run_id, "num_items": total})
        self.db.recompute()
        log.info("recomputed warehouse", extra={"source": run_id, "num_items": 1})

    @log_runtime(logger=log)
    def run_digestor(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filter = [
            f"{K_GIST} IS NULL",
            f"{K_CREATED} >= '{ndays_ago_str(MAX_ANALYZE_NDAYS)}'",
            f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_INDEXING}"
        ]

        log.info("starting digestor", extra={"source": run_id, "num_items": os.cpu_count()})
        for beans in self.stream_beans(filter):
            try:
                beans = self.digest_beans(beans)
                total += len(beans)
            except Exception as e:
                log.error(f"failed digesting - {e}", extra={"source": run_id, "num_items": len(beans)})
        log.info("total digested", extra={"source": run_id, "num_items": total})


    
