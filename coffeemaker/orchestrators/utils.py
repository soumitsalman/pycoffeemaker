
import logging
import os
from logging import Logger
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from functools import wraps

MAX_QUEUE_PAGE = 32
WORDS_THRESHOLD_FOR_SCRAPING = int(os.getenv('WORDS_THRESHOLD_FOR_SCRAPING', 200)) # min words needed to not download the body
WORDS_THRESHOLD_FOR_INDEXING = int(os.getenv('WORDS_THRESHOLD_FOR_INDEXING', 70)) # mininum words needed to put it through indexing
WORDS_THRESHOLD_FOR_DIGESTING = int(os.getenv('WORDS_THRESHOLD_FOR_DIGESTING', 160)) # min words needed to use the generated summary

num_words = lambda text: len(text.split()) if text else 0
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

def initialize_s3bucket(s3_endpoint, s3_access_key, s3_secret_key, bucket_name):
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key
    )
    # try: client.head_bucket(Bucket=bucket_name)
    # except: client.create_bucket(Bucket=bucket_name, ACL='public-read')

    return client

calculate_trend_score = lambda chatter_delta: 100*chatter_delta.comments_change + 10*chatter_delta.shares_change + chatter_delta.likes_change    
merge_tags = lambda *args: list(set(item.lower() for arg in args if arg for item in arg))

# def batch_upload(container: ContainerClient, beans: list):
#     upload = lambda bean: container.upload_blob(bean.url, bean.content, BlobType.BLOCKBLOB)
#     with ThreadPoolExecutor(max_workers=len(beans)) as executor:
#         list(executor.map(upload, beans))
 
