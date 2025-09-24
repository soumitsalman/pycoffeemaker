
import logging
import os
from logging import Logger
from datetime import datetime
from functools import wraps

MAX_QUEUE_PAGE = 32
WORDS_THRESHOLD_FOR_SCRAPING = int(os.getenv('WORDS_THRESHOLD_FOR_SCRAPING', 200)) # min words needed to not download the body
WORDS_THRESHOLD_FOR_INDEXING = int(os.getenv('WORDS_THRESHOLD_FOR_INDEXING', 70)) # mininum words needed to put it through indexing
WORDS_THRESHOLD_FOR_DIGESTING = int(os.getenv('WORDS_THRESHOLD_FOR_DIGESTING', 160)) # min words needed to use the generated summary

num_words = lambda text: min(len(text.split()) if text else 0, 1<<32)  # SMALLINT max value
above_threshold = lambda text, threshold: num_words(text) >= threshold

log = logging.getLogger(__name__)

def log_runtime(logger: Logger):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = func(*args, **kwargs)
            logger.info("execution time", extra={"source": func.__name__, "num_items": int((datetime.now() - start_time).total_seconds())})
            return result
        return wrapper
    return decorator

def log_runtime_async(logger: Logger):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = await func(*args, **kwargs)
            logger.info("execution time", extra={"source": func.__name__, "num_items": int((datetime.now() - start_time).total_seconds())})
            return result
        return wrapper
    return decorator

def dequeue_batch(queue, max_batch_size: int):
    def delete_and_extract(msg):
        queue.delete_message(msg)
        return msg.content
        
    batch = []    
    for page in queue.receive_messages(messages_per_page=MAX_QUEUE_PAGE).by_page():
        items = list(map(delete_and_extract, page))
        while items:
            remaining = max_batch_size - len(batch)
            batch.extend(items[:remaining])
            if len(batch) >= max_batch_size:
                yield batch
                batch = []
            items = items[remaining:]
            
    if batch: yield batch

def initialize_azqueues(azqueue_conn_str, queue_names: list[str]):    
    from azure.storage.queue import QueueClient, QueueMessage
    queues = [QueueClient.from_connection_string(azqueue_conn_str, name) for name in queue_names]
    for q in queues:
        try: q.create_queue()
        except: log.debug("queue already exists %s", q.queue_name)
    return queues

def initialize_azblobstore(azstorage_conn_str, container_name):
    from azure.storage.blob import ContainerClient, BlobType

    container = ContainerClient.from_connection_string(azstorage_conn_str, container_name)
    try: container.create_container()
    except: log.debug("blob container already exists %s", container_name)
    return container

merge_tags = lambda *args: list(set(item.lower() for arg in args if arg for item in arg))

def initialize_db(conn: tuple[str, str]):
    from coffeemaker.pybeansack import mongosack, warehouse
    if conn[0].startswith("mongodb"): return mongosack.Beansack(conn[0], conn[1])
    else: return warehouse.Beansack(catalogdb=conn[0], storagedb=conn[1], factory_dir=os.getenv('FACTORY_DIR', '../factory'))