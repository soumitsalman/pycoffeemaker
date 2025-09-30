from concurrent.futures import ThreadPoolExecutor
import logging
# from pymongo import UpdateOne
# from pymongo.errors import BulkWriteError
from coffeemaker.pybeansack import mongosack, warehouse, utils, models
from .utils import *
# from coffeemaker.pybeansack.models import *

log = logging.getLogger(__name__)

# _ESPRESSO_DB = "espresso"
# CONTENT_WINDOW = 7
PORT_WINDOW = 2

calculate_trend_score = lambda bean_chatter: 100*bean_chatter.comments + 10*bean_chatter.shares + bean_chatter.likes    

class Orchestrator:
    master_db: warehouse.Beansack = None
    espresso_db: mongosack.Beansack = None
    run_total: int = 0

    def __init__(self, master_conn_str: tuple[str, str], replica_conn_str: tuple[str, str]):
        self.master_db = initialize_db(master_conn_str)
        self.espresso_db = initialize_db(replica_conn_str)

    def run_refresh(self): 
        self.master_db.recompute()
        log.info("recomputed warehouse", extra={"source": self.run_id, "num_items": 1})
        self.espresso_db.cleanup()  
        log.info("cleaned up espresso", extra={"source": self.run_id, "num_items": 1})
        # TODO: enable in future
        # self.master_db.cleanup()                 
        # TODO: submit this as parallel
        # with ThreadPoolExecutor(max_workers=4) as exec:
        #     exec.submit(self.espresso_db.cleanup)
        #     exec.submit(self.master_db.cleanup)            

        # make this based on a count
        max_offset = 10000
        batch_size = 1000
        total = 0
        for offset in range(0, max_offset, batch_size):
            # TODO: in future add a fixed list of sources
            beans = self.master_db.query_processed_beans(
                created=utils.ndays_ago(PORT_WINDOW),
                offset=offset,
                limit=batch_size
            )
            if not beans: break
            total += self.espresso_db.store_beans(beans)
        log.info("refreshed beans", extra={'source': self.run_id, 'num_items': total})

        chatter_stats = self.master_db.query_bean_chatters(
            collected=utils.ndays_ago(PORT_WINDOW),
            limit=batch_size
        )
        chatter_stats = [
            models.Bean(
                url = bc.url, 
                updated=bc.collected, 
                trend_score=calculate_trend_score(bc),
                chatter=bc
            ) for bc in chatter_stats
        ]
        total = self.espresso_db.update_beans(chatter_stats)
        log.info("refreshed chatters", extra={'source': self.run_id, 'num_items': total})
   
    def run(self):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.run_refresh()

    # def _hydrate_cluster_db(self):
    #     beans = list(self.db.beanstore.find(
    #         filter = {
    #             K_CREATED: {"$gte": ndays_ago(MAX_RELATED_NDAYS)},
    #             K_EMBEDDING: {"$exists": True}
    #         }, 
    #         projection = {K_ID: 1, K_EMBEDDING: 1}
    #     ))
    #     self.cluster_db.store_items(beans)
    #     log.info("cluster db loaded", extra={'source': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'num_items': len(beans)})

        # def _backup_digests(self, beans: list[Bean]):
    #     if not self.backup_container or not beans: return 
    #     trfile = "digests-"+now().strftime("%Y-%m-%d")+".jsonl"
    #     def backup():
    #         items = map(lambda b: json.dumps({'article': b.content, "summary": b.gist})+"\n", beans)
    #         try: self.backup_container.upload_blob(trfile, "".join(items), BlobType.APPENDBLOB)           
    #         except Exception as e: log.warning(f"backup failed - {e}", extra={'source': trfile, 'num_items': len(beans)})
    #     self.dbworker.submit(backup)
