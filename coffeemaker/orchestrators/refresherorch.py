from concurrent.futures import ThreadPoolExecutor
import logging
import subprocess
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack import mongosack, warehouse, utils
from pymongo import UpdateMany
from dbcache.api import kvstore
from .utils import *
from icecream import ic

log = logging.getLogger(__name__)

PORT_WINDOW = 2
PORT_FIELDS = [
    K_URL,
    K_KIND,
    K_TITLE,
    K_SUMMARY,
    K_AUTHOR,
    K_SOURCE,
    K_IMAGEURL,
    K_CREATED,
    K_COLLECTED,
    K_CATEGORIES,
    K_REGIONS,
    K_ENTITIES,
    K_CLUSTER_ID,
    K_CLUSTER_SIZE,
    K_EMBEDDING,
    K_UPDATED,
    K_COMMENTS,
    K_LIKES,
    K_SHARES,
    K_SITE_NAME,
    K_BASE_URL,
    K_FAVICON
]

calculate_trend_score = lambda bean_chatter: 100*(bean_chatter.comments or 0) + 10*(bean_chatter.shares or 0) + (bean_chatter.likes or 0)
def _preprocess_bean(bean: AggregatedBean) -> AggregatedBean:
    bean.tags = merge_tags(bean.categories, bean.regions, bean.entities)
    bean.trend_score = calculate_trend_score(bean)
    return bean

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
        batch_size = 5000
        total = 0
        for offset in range(0, max_offset, batch_size):
            # TODO: in future add a fixed list of sources
            beans = self.master_db.query_aggregated_beans(
                created=utils.ndays_ago(PORT_WINDOW),
                conditions=[
                    "gist IS NOT NULL",
                    "categories IS NOT NULL",
                    "cluster_id IS NOT NULL"
                ],
                offset=offset,
                limit=batch_size,
                columns=PORT_FIELDS
            )
            if not beans: break
            total += self.espresso_db.store_beans([_preprocess_bean(bean) for bean in beans])
            ic(beans[0:2])
        log.info("refreshed beans", extra={'source': self.run_id, 'num_items': total})

        chatter_stats = self.master_db.query_aggregated_chatters(
            updated=utils.ndays_ago(PORT_WINDOW),
            limit=batch_size
        )
        chatter_stats = [
            Bean(
                url = bc.url, 
                updated=bc.collected, 
                trend_score=calculate_trend_score(bc),
                chatter=bc
            ) for bc in chatter_stats
        ]
        total = self.espresso_db.update_beans(chatter_stats)
        log.info("refreshed chatters", extra={'source': self.run_id, 'num_items': total})

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

        # self.master_db.recompute()
        # log.info("recomputed warehouse", extra={"source": self.run_id, "num_items": 1})

        # # NOTE: skipping cleanup for now as it is too aggressive
        # self.master_db.cleanup()
        # log.info("cleaned up warehouse", extra={"source": self.run_id, "num_items": 1})

        self.port_contents()
        # self.sync_storage()

        self.master_db.close()
