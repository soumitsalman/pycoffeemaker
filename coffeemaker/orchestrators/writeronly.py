from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from operator import add
import os
import logging
from icecream import ic

from pymongo import UpdateOne
from azure.storage.queue import QueueClient
from coffeemaker.pybeansack.mongosack import Beansack
from coffeemaker.pybeansack.models import *
from coffeemaker.nlp import writers, utils
from coffeemaker.orchestrators.utils import *

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv('DIGESTOR_BATCH_SIZE', 16*os.cpu_count()))

is_digestible = lambda bean: above_threshold(bean.content, WORDS_THRESHOLD_FOR_DIGESTING)
is_storable = lambda bean: bool(bean.gist) # if there is no embedding then no point storing
digestibles = lambda beans: list(filter(is_digestible, beans)) if beans else beans
storables = lambda beans: list(filter(is_storable, beans)) if beans else beans 

class Orchestrator:
    db: Beansack = None
    queue: QueueClient = None
    writer: writers.ArticleWriter = None

    def __init__(self, 
        db_path: str, 
        db_name: str, 
        writer_path: str = os.getenv("WRITER_PATH"), 
        writer_base_url: str = os.getenv("WRITER_BASE_URL"),
        writer_api_key: str = os.getenv("WRITER_API_KEY"),
        writer_context_len: int = int(os.getenv("WRITER_CONTEXT_LEN", writers.CONTEXT_LEN))
    ): 
        self.db = Beansack(db_path, db_name)
        self.writer = digestors.from_path(writer_path, writer_context_len, base_url=writer_base_url, api_key=writer_api_key)
 
    def digest_beans(self, beans: list[Bean]) -> list[Bean]|None:
        if not beans: return beans

        try:
            digests = self.writer.run_batch([bean.content for bean in beans])
            for bean, digest in zip(beans, digests):
                if not digest: 
                    log.error("failed digesting", extra={"source": bean.url, "num_items": 1})
                    continue

                bean.gist = f"U:{bean.created.strftime('%Y-%m-%d')};"+digest.expr
                bean.regions = digest.regions
                bean.entities = digest.entities
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
                    K_TAGS: bean.tags
                }
            }
        )
        count = self.db.update_beans(list(map(make_update, beans)))
        log.info("digested", extra={"source": beans[0].source, "num_items": count})
        return beans
    

    @log_runtime(logger=log)
    def run(self):
        run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting digestor", extra={"source": run_id, "num_items": 1})

        for batch in self.queue.receive_messages(messages_per_page=min(MAX_QUEUE_PAGE, BATCH_SIZE)).by_page():
            urls = list(map(self._process_msg, batch))
            try:
                beans = self.db.query_beans({K_URL: {"$in": urls}}, project={K_URL: 1, K_CONTENT: 1, K_SOURCE: 1, K_CREATED: 1, K_CATEGORIES: 1})
                beans = self.digest_beans(digestibles(beans))
            except Exception as e:
                log.error("failed digesting", extra={"source": run_id, "num_items": len(urls)})
                ic(e)

        log.info("completed digestor", extra={"source": run_id, "num_items": 1})
