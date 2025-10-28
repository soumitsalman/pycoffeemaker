from itertools import chain
import json
import os
import logging
from concurrent.futures import ThreadPoolExecutor
# from pymongo import UpdateOne
from coffeemaker.pybeansack import warehouse
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
    db: warehouse.Beansack = None
    dbworker = None
    backup_container = None    

    def __init__(self, 
        db_conn_str: tuple[str, str],        
        embedder_path: str = None, 
        embedder_context_len: int = 0,               
        digestor_path: str = None, 
        digestor_base_url: str = None,
        digestor_api_key: str = None,
        digestor_context_len: int = 0,
        batch_size: int = BATCH_SIZE
    ): 
        self.db = initialize_db(db_conn_str)
        if embedder_path: self.embedder = embedders.from_path(embedder_path, embedder_context_len)
        if digestor_path: self.digestor = agents.text2text_agent_from_path(
            model_path=digestor_path, base_url=digestor_base_url, api_key=digestor_api_key, 
            max_input_tokens=digestor_context_len, max_output_tokens=400, 
            output_parser=Digest.parse_compressed
        )
        self.batch_size = batch_size
    
    def embed_beans(self, beans: list[Bean]) -> list[Bean]|None:   
        if not beans: return beans

        # this is a cpu heavy calculation. run it on the main thread and let the nlp take care of it
        embeddings = self.embedder.embed_documents([bean.content for bean in beans])
        embs = [Bean(url=b.url, embedding=e) for b, e in zip(beans, embeddings) if e]
        # self.db.update_beans(embs, [K_EMBEDDING])
        log.info("embedded", extra={"source": beans[0].source, "num_items": len(embs)})
        return embs

    def digest_beans(self, beans: list[Bean]) -> list[Bean]|None:
        if not beans: return beans

        # this is a cpu heavy calculation. run it on the main thread and let the nlp take care of it
        digests = self.digestor.run_batch([bean.content for bean in beans])
        gists = [Bean(url=b.url, gist=d.raw, regions=d.regions, entities=d.entities) for b, d in zip(beans, digests) if d and d.raw]
        # self.db.update_beans(gists, [K_GIST, K_REGIONS, K_ENTITIES])
        log.info("digested", extra={"source": beans[0].source, "num_items": len(gists)})
        return gists
    
    def stream_beans(self, filter):
        while True:
            beans = self.db.query_latest_beans(
                exprs=filter,
                limit=self.batch_size,
                columns=[K_URL, K_CREATED, K_CONTENT, K_SOURCE]
            )            
            if beans: yield beans
            else: break

    def get_beans(self, filter):
        return self.db.query_latest_beans(
            created=ndays_ago(MAX_ANALYZE_NDAYS),
            exprs=filter,
            columns=[K_URL, K_CREATED, K_CONTENT, K_SOURCE]
        )
           
    @log_runtime(logger=log)
    def run_indexer(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filter = [
            f"{K_EMBEDDING} IS NULL",
            f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_INDEXING}"
        ]

        all_beans = self.get_beans(filter)    
        log.info("starting indexer", extra={"source": run_id, "num_items":len(all_beans)}) 
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as exec:       
            for i in range(0, len(all_beans), self.batch_size):
                try:
                    beans = self.embed_beans(all_beans[i:i+self.batch_size])
                    exec.submit(self.db.update_beans, beans, [K_EMBEDDING])
                    total += len(beans)
                except Exception as e:
                    log.error(f"failed indexing - {e}", extra={"source": run_id, "num_items": len(beans)})    
                    
        self.db.close()
        log.info("total indexed", extra={"source": run_id, "num_items": total})

    @log_runtime(logger=log)
    def run_digestor(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filter = [
            f"{K_GIST} IS NULL",
            f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_INDEXING}",
            f"{K_KIND} <> '{POST}'"
        ]

        all_beans = self.get_beans(filter)  
        log.info("starting digestor", extra={"source": run_id, "num_items": len(all_beans)})  
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as exec:    
            for i in range(0, len(all_beans), self.batch_size):
                try:
                    beans = self.digest_beans(all_beans[i:i+self.batch_size])
                    exec.submit(self.db.update_beans, beans, [K_GIST, K_REGIONS, K_ENTITIES])
                    total += len(beans)
                except Exception as e:
                    log.error(f"failed digesting - {e}", extra={"source": run_id, "num_items": len(beans)})

        self.db.close()
        log.info("total digested", extra={"source": run_id, "num_items": total})


    
