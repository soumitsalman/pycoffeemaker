import logging
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from coffeemaker.pybeansack.mongosack import *
from coffeemaker.pybeansack.models import *

log = logging.getLogger(__name__)

_ESPRESSO_DB = "espresso"
CONTENT_WINDOW = 7
PORT_WINDOW = 1

class Orchestrator:
    master_db: Beansack = None
    run_total: int = 0

    def __init__(self, mongodb_conn_str: str, db_name: str):
        self.master_db = Beansack(mongodb_conn_str, db_name) 
        self.espresso_db = Beansack(mongodb_conn_str, _ESPRESSO_DB) 

    def refresh_espresso(self):    
        cleanup_filter = {            
            K_KIND: {"$ne": GENERATED},
            K_UPDATED: {"$lt": ndays_ago(CONTENT_WINDOW)},
        }
        porting_filter = {
            K_ENTITIES: VALUE_EXISTS,
            K_CATEGORIES: VALUE_EXISTS,
            K_UPDATED: {"$gte": ndays_ago(PORT_WINDOW)} # take everything that has been created or updated in the last 1 day
        }
        update_projection = {
            K_URL: 1,
            K_LIKES: 1,
            K_COMMENTS: 1,
            K_SHARES: 1,
            K_SHARED_IN: 1,
            K_LATEST_LIKES: 1,
            K_LATEST_COMMENTS: 1,
            K_LATEST_SHARES: 1,
            K_TRENDSCORE: 1,
            K_UPDATED: 1  
        }
        # with ThreadPoolExecutor(max_workers=os.cpu_count()) as exec:                
        deleted = self.espresso_db.beanstore.delete_many(cleanup_filter)
        log.info("cleaned up", extra={'source': self.run_id, 'num_items': deleted.deleted_count})     

        stats = self.master_db.beanstore.find(updated_in(CONTENT_WINDOW), projection=update_projection)
        updated = self.espresso_db.update_beans([UpdateOne(filter={K_URL: up[K_URL]}, update={"$set": up}) for up in stats])
        log.info("trend ranked", extra={'source': self.run_id, 'num_items': updated})
        try:
            inserted = self.espresso_db.beanstore.insert_many(self.master_db.beanstore.find(porting_filter), ordered=False)
            log.info("ported", extra={'source': self.run_id, 'num_items': len(inserted.inserted_ids)})
        except BulkWriteError as e: log.warning(f"partially ported", extra={"source": self.run_id, "num_items": e.details['nInserted']})

    def run(self):
        self.run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.refresh_espresso()

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
