from itertools import chain, batched
import json
import os
import logging
import threading as th
import queue
from coffeemaker.pybeansack import BeansackBase
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack.utils import *
from coffeemaker.nlp import embedders, digestors, Digest, utils
from coffeemaker.orchestrators.utils import *
from icecream import ic

log = logging.getLogger(__name__)

# CACHE_DIR = ".cache"
BATCH_SIZE = int(os.getenv('BATCH_SIZE', os.cpu_count()))
# MAX_RELATED = int(os.getenv('MAX_RELATED', 128))
# MAX_RELATED_NDAYS = int(os.getenv('MAX_RELATED_NDAYS', 7))
# MAX_RELATED_EPS=float(os.getenv("MAX_RELATED_EPS", 0.48))
MAX_ANALYZE_NDAYS =  int(os.getenv('MAX_ANALYZE_NDAYS', 2))
END_OF_QUEUE = "__END_OF_QUEUE__" # Sentinel value to signal end of queue

EMBED_FILTER = [
    f"{K_EMBEDDING} IS NULL",
    f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_INDEXING}"
]
DIGEST_FILTER = [
    f"{K_GIST} IS NULL",
    f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_INDEXING}",
    f"{K_KIND} <> '{POST}'"
]

index_storables = lambda beans: [bean for bean in beans if bean.embedding]
digest_storables = lambda beans: [bean for bean in beans if bean.gist]

# def _embed(embedder: embedders.Embeddings, beans: list[Bean]) -> list[Bean]|None:
#     if not beans: return None

#     vecs = embedder.embed_documents([bean.content for bean in beans])
#     embs = [Bean(url=b.url, source=b.source, embedding=e) for b, e in zip(beans, vecs) if len(e) == VECTOR_LEN]
#     log.info("embedded", extra={"source": beans[0].source, "num_items": len(embs)})
#     return embs

# def _digest(digestor: agents.Text2TextAgent, beans: list[Bean]) -> list[Bean]|None:
#     if not beans: return beans

#     # this is a cpu heavy calculation. run it on the main thread and let the nlp take care of it
#     gists = digestor.run_batch([bean.content for bean in beans])
#     digests = [Bean(url=b.url, source=b.source, gist=d.raw, regions=d.regions, entities=d.entities) for b, d in zip(beans, gists) if d and d.raw]
#     log.info("digested", extra={"source": beans[0].source, "num_items": len(digests)})
#     return digests

class Orchestrator:
    # db_conn_str: tuple[str, str] = None
    db: BeansackBase = None
    embedder_model: str = None
    embedder_context_len: int = 0
    digestor_model: str = None
    digestor_context_len: int = 0
    dbworker = None
    backup_container = None    

    def __init__(self, 
        db_kwargs: dict[str, str] = None, 
        embedder_path: str = None, 
        embedder_context_len: int = 0,             
        digestor_path: str = None, 
        digestor_context_len: int = 0
    ):     
        # self.db_conn_str = db_conn_str
        self.db = initialize_db(**db_kwargs)
        self.embedder_model = embedder_path
        self.embedder_context_len = embedder_context_len
        self.digestor_model = digestor_path
        self.digestor_context_len = digestor_context_len
        self.update_queue = queue.Queue()
        self.updater = th.Thread(target=self._run_updates, daemon=True)
        self.updater.start()  
    
    def get_beans(self, filter, batch_size: int = None):
        return self.db.query_latest_beans(
            collected=ndays_ago(MAX_ANALYZE_NDAYS),
            conditions=filter,
            limit=batch_size,
            columns=[K_URL, K_CREATED, K_CONTENT, K_SOURCE]
        )

    def stream_beans(self, filter, batch_size: int):
        while beans := self.get_beans(filter, batch_size):
            yield beans

    def embed_beans(self, beans: list[Bean], batch_size: int) -> list[Bean]|None:
        if not beans: return beans
        if not self.embedder_model: raise ValueError("embedder_model is not set")
        with embedders.from_path(self.embedder_model, self.embedder_context_len) as embedder:
            total = 0
            for chunk in batched(beans, batch_size):
                try:
                    vectors = embedder.embed_documents([bean.content for bean in chunk])
                    updates = [Bean(url=b.url, source=b.source, embedding=v) for b, v in zip(chunk, vectors) if len(v) == VECTOR_LEN]                
                    self._queue_update(updates, None, self.db.update_embeddings)
                    log.info("embedded", extra={"source": chunk[0].source, "num_items": len(updates)})
                    total += len(chunk)
                except Exception as e:
                    log.error(f"failed indexing - {e}", extra={"source": chunk[0].source, "num_items": len(chunk)})
        return total
    
    def digest_beans(self, beans: list[Bean], batch_size: int) -> list[Bean]|None:
        if not beans: return beans
        if not self.digestor_model: raise ValueError("digestor_model is not set")
        with digestors.from_path(
            model_path=self.digestor_model, 
            max_input_tokens=self.digestor_context_len, 
            max_output_tokens=384, 
            output_parser=Digest.parse_compressed
        ) as digestor:
            total = 0      
            for chunk in batched(beans, batch_size):
                try:
                    gists = digestor.run_batch([bean.content for bean in chunk])
                    updates = [Bean(url=b.url, source=b.source, gist=d.raw, regions=d.regions, entities=d.entities) for b, d in zip(chunk, gists) if d and d.raw]                
                    self._queue_update(updates, [K_GIST, K_REGIONS, K_ENTITIES])
                    log.info("digested", extra={"source": chunk[0].source, "num_items": len(updates)})
                    total += len(chunk)
                except Exception as e:
                    log.error(f"failed indexing - {e}", extra={"source": chunk[0].source, "num_items": len(chunk)})        
        return total
         
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
    
    def _finish_updates(self):        
        """Wait for all pending database updates to complete"""   
        self.update_queue.put(END_OF_QUEUE)     
        self.updater.join()
           
    @log_runtime(logger=log)
    def run_indexer(self, batch_size: int = BATCH_SIZE):
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        beans = self.get_beans(EMBED_FILTER)    
        log.info("starting indexer", extra={"source": run_id, "num_items":len(beans)}) 
        total = self.embed_beans(beans, batch_size)
        log.info("total indexed", extra={"source": run_id, "num_items": total})
        
        # Wait for all pending updates to complete
        self._finish_updates()

    @log_runtime(logger=log)
    def run_digestor(self, batch_size: int = BATCH_SIZE):
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        beans = self.get_beans(DIGEST_FILTER)  
        log.info("starting digestor", extra={"source": run_id, "num_items": len(beans)})  
        total = self.digest_beans(beans, batch_size)
        log.info("total digested", extra={"source": run_id, "num_items": total}) 

        # Wait for all pending updates to complete
        self._finish_updates()

    def run(self, embedder_batch_size: int = BATCH_SIZE, digestor_batch_size: int = BATCH_SIZE):
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        beans = self.get_beans(EMBED_FILTER)    
        log.info("starting indexer", extra={"source": run_id, "num_items":len(beans)}) 
        total = self.embed_beans(beans, embedder_batch_size)
        log.info("total indexed", extra={"source": run_id, "num_items": total})

        beans = self.get_beans(DIGEST_FILTER)  
        log.info("starting digestor", extra={"source": run_id, "num_items": len(beans)})  
        total = self.digest_beans(beans, digestor_batch_size)
        log.info("total digested", extra={"source": run_id, "num_items": total}) 

        self._finish_updates()

    def close(self):        
        # Close database connection
        self._finish_updates()
        self.db.refresh()
        self.db.close()


    
