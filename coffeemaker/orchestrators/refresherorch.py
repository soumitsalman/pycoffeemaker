from concurrent.futures import ThreadPoolExecutor
import logging
import subprocess
from coffeemaker.pybeansack.models import *
from coffeemaker.pybeansack import BeansackBase, lancesack
from pymongo import UpdateMany
from dbcache.api import kvstore
from .utils import *
from icecream import ic

log = logging.getLogger(__name__)

PORT_WINDOW = 2
BEAN_PORT_FIELDS = [
    K_URL,
    K_KIND,
    K_TITLE,
    K_SUMMARY,
    K_AUTHOR,
    K_SOURCE,
    K_IMAGEURL,
    K_CREATED,
    K_COLLECTED,
    K_EMBEDDING,
    K_SENTIMENTS,
    K_CATEGORIES,
    K_GIST,
    K_REGIONS,
    K_ENTITIES
]

calculate_trend_score = lambda bean_chatter: 100*(bean_chatter.comments or 0) + 10*(bean_chatter.shares or 0) + (bean_chatter.likes or 0)
def _preprocess_bean(bean: AggregatedBean) -> AggregatedBean:
    bean.tags = merge_tags(bean.categories, bean.regions, bean.entities)
    bean.trend_score = calculate_trend_score(bean)
    return bean

class Orchestrator:
    db: BeansackBase = None
    backup_db: lancesack.Beansack = None
    run_total: int = 0

    def __init__(self, db_kwargs: dict[str, str], backup_db_kwargs: dict[str, str]):
        self.db = initialize_db(**db_kwargs)
        self.backup_db = initialize_db(**backup_db_kwargs)

    def port_contents(self):         
        beans = self.db.query_latest_beans(
            created=ndays_ago(PORT_WINDOW),
            conditions=[
                "gist IS NOT NULL",
                "embedding IS NOT NULL"
            ]
            # columns=BEAN_PORT_FIELDS
        )
        total = self.backup_db.store_beans(beans)
        log.info("ported beans", extra={'source': self.run_id, 'num_items': total})

        publishers = self.db.query_publishers(     
            collected=ndays_ago(PORT_WINDOW),       
            conditions=["""(
                rss_feed IS NOT NULL 
                OR favicon IS NOT NULL 
                OR site_name IS NOT NULL
            )"""]
        )
        total = self.backup_db.store_publishers(publishers)
        log.info("ported publishers", extra={'source': self.run_id, 'num_items': total})

        chatters = self.db.query_chatters(collected=ndays_ago(PORT_WINDOW))
        total = self.backup_db.store_chatters(chatters)
        log.info("ported chatters", extra={'source': self.run_id, 'num_items': total})
   
    def run(self):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.info("starting refresher", extra={"source": self.run_id, "num_items": os.cpu_count()})

        self.db.refresh()
        log.info("optimized", extra={"source": self.run_id, "num_items": 1})

        # # NOTE: skipping cleanup for now as it is too aggressive
        # self.master_db.cleanup()
        # log.info("cleaned up warehouse", extra={"source": self.run_id, "num_items": 1})

        self.port_contents()

    def close(self):
        self.db.close()
        self.backup_db.close()
