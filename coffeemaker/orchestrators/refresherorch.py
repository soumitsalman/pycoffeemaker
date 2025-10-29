from concurrent.futures import ThreadPoolExecutor
import logging
import subprocess
from coffeemaker.pybeansack import mongosack, warehouse, utils, models
from pymongo import UpdateMany
from dbcache.api import kvstore
from .utils import *

log = logging.getLogger(__name__)

PORT_WINDOW = 2

calculate_trend_score = lambda bean_chatter: 100*bean_chatter.comments + 10*bean_chatter.shares + bean_chatter.likes    

class Orchestrator:
    master_db: warehouse.Beansack = None
    espresso_db: mongosack.Beansack = None
    cache: kvstore = None
    run_total: int = 0

    def __init__(self, master_conn_str: tuple[str, str], replica_conn_str: tuple[str, str]):
        self.master_db = initialize_db(master_conn_str)
        self.espresso_db = initialize_db(replica_conn_str)
        self.cache = kvstore(master_conn_str[0])

    def port_contents(self): 
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

        # NOTE: temporarily disabling publishers
        # publishers = self.master_db.query_publishers()
        # total = self.espresso_db.update_beans_adhoc(list(map(
        #     lambda p: UpdateMany(
        #         {
        #             models.K_SOURCE: p.source,
        #             "publisher": {"$exists": False}
        #         }, 
        #         {
        #             "$set": {"publisher": p.model_dump(exclude_unset=True, exclude_none=True)}
        #         }
        #     ), 
        #     publishers
        # )))
        # log.info("refreshed publishers", extra={'source': self.run_id, 'num_items': total})

        self.espresso_db.cleanup()  
        log.info("cleaned up espresso", extra={"source": self.run_id, "num_items": 1})

    def sync_storage(self):
        # get the snapshot
        current_snapshot = self.master_db.snapshot()
        # then upload what is in the directory
        # this way if some
        s3sync_cmd = os.getenv("S3SYNC_CMD")
        if s3sync_cmd: 
            subprocess.run(s3sync_cmd.split(), check=True)
            log.info("synced storage", extra={"source": self.run_id, "num_items": 1})
        # then update cache
        self.cache.set("current_snapshot", current_snapshot)
        log.info("saved snapshot", extra={"source": self.run_id, "num_items": current_snapshot})
   
    def run(self):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting refresher", extra={"source": self.run_id, "num_items": os.cpu_count()})

        self.master_db.recompute()
        log.info("recomputed warehouse", extra={"source": self.run_id, "num_items": 1})

        # NOTE: skipping cleanup for now as it is too aggressive
        self.master_db.cleanup()
        log.info("cleaned up warehouse", extra={"source": self.run_id, "num_items": 1})

        self.port_contents()
        # self.syncdf_storage()

        self.master_db.close()
