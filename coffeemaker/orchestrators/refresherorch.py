from concurrent.futures import ThreadPoolExecutor
import logging
import subprocess
from pybeansack.models import *
from pybeansack import Beansack, create_client
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

class Orchestrator:
    db: Beansack = None
    backup_db: Beansack = None
    run_total: int = 0

    def __init__(self, db_kwargs: dict[str, str], backup_db_kwargs: dict[str, str]):
        self.db = create_client(**db_kwargs)
        self.backup_db = create_client(**backup_db_kwargs)

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

        self.db.optimize()
        log.info("optimized", extra={"source": self.run_id, "num_items": 1})

        # # NOTE: skipping cleanup for now as it is too aggressive
        # self.master_db.cleanup()
        # log.info("cleaned up warehouse", extra={"source": self.run_id, "num_items": 1})

        self.port_contents()

    def close(self):
        self.db.close()
        self.backup_db.close()
