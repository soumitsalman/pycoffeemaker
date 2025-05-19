from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from itertools import chain
from operator import add
import os
import logging
from icecream import ic

from pymongo import UpdateOne
from azure.storage.queue import QueueClient
from coffeemaker.pybeansack.mongosack import Beansack as MongoSack
from coffeemaker.pybeansack.models import *
from coffeemaker.nlp import digestors, utils
from coffeemaker.orchestrators.utils import *

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv('DIGESTOR_BATCH_SIZE', 16*os.cpu_count()))

is_digestible = lambda bean: above_threshold(bean.content, WORDS_THRESHOLD_FOR_DIGESTING)
is_storable = lambda bean: bool(bean.gist) # if there is no embedding then no point storing
digestibles = lambda beans: list(filter(is_digestible, beans)) if beans else beans
storables = lambda beans: list(filter(is_storable, beans)) if beans else beans 

class Orchestrator:
    db: MongoSack = None
    queue: QueueClient = None
    digestor: digestors.Digestor = None

    def __init__(self, 
        db_path: str, 
        db_name: str, 
        queue_path: str,
        queue_name: str,
        digestor_path: str = os.getenv("DIGESTOR_PATH"), 
        digestor_base_url: str = os.getenv("DIGESTOR_BASE_URL"),
        digestor_api_key: str = os.getenv("DIGESTOR_API_KEY"),
        digestor_context_len: int = int(os.getenv("DIGESTOR_CONTEXT_LEN", digestors.CONTEXT_LEN))
    ): 
        self.db = MongoSack(db_path, db_name)
        self.queue = QueueClient.from_connection_string(queue_path, queue_name)
        self.digestor = digestors.from_path(digestor_path, digestor_context_len, base_url=digestor_base_url, api_key=digestor_api_key)
 
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
            ic(e.__class__.__name__, e)

        beans = storables(beans)
        make_update = lambda bean: UpdateOne(
            {K_ID: bean.url}, 
            {
                "$set": {
                    K_GIST: bean.gist,
                    K_REGIONS: bean.regions,
                    K_ENTITIES: bean.entities,
                    K_CATEGORIES: bean.categories,
                    K_SENTIMENTS: bean.sentiments,
                    K_TAGS: bean.tags
                }
            }
        )
        count = self.db.update_beans(list(map(make_update, beans)))
        log.info("digested", extra={"source": beans[0].source, "num_items": count})
        return beans

    @log_runtime(logger=log)
    def run(self):
        total = 0
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting digestor", extra={"source": run_id, "num_items": 1})

        for urls in dequeue_batch(self.queue, BATCH_SIZE):
            try:
                beans = self.db.query_beans({K_URL: {"$in": urls}, K_CREATED: {"$gte": datetime.now() - timedelta(days=2)}}, project={K_URL: 1, K_CONTENT: 1, K_SOURCE: 1, K_CREATED: 1, K_CATEGORIES: 1})
                beans = self.digest_beans(digestibles(beans))
                total += len(beans)
            except Exception as e:
                log.error("failed digesting", extra={"source": run_id, "num_items": len(urls)})
                ic(e)

        log.info("completed digestor", extra={"source": run_id, "num_items": total})
