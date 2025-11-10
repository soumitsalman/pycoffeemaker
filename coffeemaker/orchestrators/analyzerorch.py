from itertools import chain, batched
import json
import os
import logging
import threading
import queue
import gc
from concurrent.futures import ThreadPoolExecutor
# from pymongo import UpdateOne
from coffeemaker.pybeansack import warehouse
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.utils import *
from coffeemaker.nlp import embedders, agents, Digest, utils
from coffeemaker.orchestrators.utils import *
from icecream import ic

log = logging.getLogger(__name__)

CACHE_DIR = ".cache"
BATCH_SIZE = int(os.getenv('BATCH_SIZE', os.cpu_count()))
MAX_RELATED = int(os.getenv('MAX_RELATED', 128))
MAX_RELATED_NDAYS = int(os.getenv('MAX_RELATED_NDAYS', 7))
MAX_RELATED_EPS=float(os.getenv("MAX_RELATED_EPS", 0.48))
MAX_ANALYZE_NDAYS =  int(os.getenv('MAX_ANALYZE_NDAYS', 2))
END_OF_QUEUE = "__END_OF_QUEUE__" # Sentinel value to signal end of queue

index_storables = lambda beans: [bean for bean in beans if bean.embedding]
digest_storables = lambda beans: [bean for bean in beans if bean.gist]

def _embed(embedder: embedders.Embeddings, beans: list[Bean]) -> list[Bean]|None:
    if not beans: return None

    vecs = embedder.embed_documents([bean.content for bean in beans])
    embs = [Bean(url=b.url, source=b.source, embedding=e) for b, e in zip(beans, vecs) if len(e) == VECTOR_LEN]
    log.info("embedded", extra={"source": beans[0].source, "num_items": len(embs)})
    return embs

def _digest(digestor: agents.Text2TextAgent, beans: list[Bean]) -> list[Bean]|None:
    if not beans: return beans

    # this is a cpu heavy calculation. run it on the main thread and let the nlp take care of it
    gists = digestor.run_batch([bean.content for bean in beans])
    digests = [Bean(url=b.url, source=b.source, gist=d.raw, regions=d.regions, entities=d.entities) for b, d in zip(beans, gists) if d and d.raw]
    log.info("digested", extra={"source": beans[0].source, "num_items": len(digests)})
    return digests

class Orchestrator:
    db: warehouse.Beansack = None
    dbworker = None
    backup_container = None    

    def __init__(self, db_conn_str: tuple[str, str]):     
        self.db = initialize_db(db_conn_str)
        self.update_queue = queue.Queue()
        self.worker_thread = threading.Thread(target=self._run_updates, daemon=True)
        self.worker_thread.start()    

    def embed_beans(self, beans: list[Bean], embedder_path: str, context_len: int, batch_size: int) -> list[Bean]|None:
        if not beans: return beans

        total = 0
        embedder = embedders.from_path(embedder_path, context_len)
        for chunk in batched(beans, batch_size):
            try:
                chunk = _embed(embedder, list(chunk))
                self._queue_update(chunk, None, self.db.update_bean_embeddings)
                total += len(chunk)
            except Exception as e:
                log.error(f"failed indexing - {e}", extra={"source": chunk[0].source, "num_items": len(chunk)})
        
        # Clear embedder from GPU memory
        del embedder
        return total
    
    def digest_beans(self, beans: list[Bean], digestor_path: str, digestor_base_url: str, digestor_api_key: str, digestor_context_len: int, batch_size: int) -> list[Bean]|None:
        if not beans: return beans

        total = 0
        digestor = agents.text2text_agent_from_path(
            model_path=digestor_path, base_url=digestor_base_url, api_key=digestor_api_key, 
            max_input_tokens=digestor_context_len, max_output_tokens=384, 
            output_parser=Digest.parse_compressed
        )        
        for chunk in batched(beans, batch_size):
            try:
                chunk = _digest(digestor, list(chunk))
                self._queue_update(chunk, [K_GIST, K_REGIONS, K_ENTITIES])
                total += len(chunk)
            except Exception as e:
                log.error(f"failed indexing - {e}", extra={"source": chunk[0].source, "num_items": len(chunk)})
        
        # Clear digestor from GPU memory
        del digestor
        return total

    def stream_beans(self, filter, batch_size: int):
        while beans := self.get_beans(filter, batch_size):
            yield beans

    def get_beans(self, filter, batch_size: int = None):
        return self.db.query_latest_beans(
            collected=ndays_ago(MAX_ANALYZE_NDAYS),
            conditions=filter,
            limit=batch_size,
            columns=[K_URL, K_CREATED, K_CONTENT, K_SOURCE]
        )
    
    def _run_updates(self):
        """Worker thread that processes updates from the queue"""
        while True:
            try:
                # Get update task from queue with timeout
                update_task = self.update_queue.get(timeout=30)
                
                if update_task is END_OF_QUEUE:  # Sentinel value to stop worker
                    self.update_queue.task_done()
                    break
                
                beans, columns, update_func = update_task
                if columns: self.db.update_beans(beans, columns)
                elif update_func: update_func(beans)
                log.info("updated", extra={"source": beans[0].source, "num_items": len(beans)})
                self.update_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                log.error(f"update worker error - {e}")
                self.update_queue.task_done()
    
    def _queue_update(self, beans: list[Bean], columns: list[str] = None, update_func=None):
        if beans: self.update_queue.put((beans, columns, update_func))
    
    def _wait_for_updates(self):        
        """Wait for all pending database updates to complete"""
        utils.clear_gpu_cache()
        self.update_queue.join()
           
    @log_runtime(logger=log)
    def run_indexer(self, embedder_path: str = None, embedder_context_len: int = 0, batch_size: int = BATCH_SIZE):
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filter = [
            f"{K_EMBEDDING} IS NULL",
            f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_INDEXING}"
        ]

        beans = self.get_beans(filter)    
        log.info("starting indexer", extra={"source": run_id, "num_items":len(beans)}) 
        total = self.embed_beans(beans, embedder_path, embedder_context_len, batch_size)
        
        # Wait for all pending updates to complete
        self.update_queue.put(END_OF_QUEUE)        
        self._wait_for_updates()
        # self.db.refresh_classifications()
        # log.info("refreshed classifications", extra={"source": run_id, "num_items": len(beans)})
        self.db.refresh_clusters()
        log.info("refreshed clusters", extra={"source": run_id, "num_items": len(beans)})
        log.info("total indexed", extra={"source": run_id, "num_items": total})

    @log_runtime(logger=log)
    def run_digestor(self, digestor_path: str = None, digestor_base_url: str = None, digestor_api_key: str = None, digestor_context_len: int = 0, batch_size: int = BATCH_SIZE):
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filter = [
            f"{K_GIST} IS NULL",
            f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_INDEXING}",
            f"{K_KIND} <> '{POST}'"
        ]

        beans = self.get_beans(filter)  
        log.info("starting digestor", extra={"source": run_id, "num_items": len(beans)})  
        total = self.digest_beans(beans, digestor_path, digestor_base_url, digestor_api_key, digestor_context_len, batch_size)

        # Wait for all pending updates to complete
        self.update_queue.put(END_OF_QUEUE)
        self._wait_for_updates()
        log.info("total digested", extra={"source": run_id, "num_items": total})

    def close(self):        
        # Close database connection
        self._wait_for_updates()
        self.db.close()


    
