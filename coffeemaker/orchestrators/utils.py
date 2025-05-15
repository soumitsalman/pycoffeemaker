from functools import wraps
import os
from logging import Logger
from datetime import datetime
from azure.storage.queue import QueueClient, QueueMessage

MAX_QUEUE_PAGE = 32
WORDS_THRESHOLD_FOR_SCRAPING = int(os.getenv('WORDS_THRESHOLD_FOR_SCRAPING', 200)) # min words needed to not download the body
WORDS_THRESHOLD_FOR_INDEXING = int(os.getenv('WORDS_THRESHOLD_FOR_INDEXING', 70)) # mininum words needed to put it through indexing
WORDS_THRESHOLD_FOR_DIGESTING = int(os.getenv('WORDS_THRESHOLD_FOR_DIGESTING', 160)) # min words needed to use the generated summary

above_threshold = lambda text, threshold: text and len(text.split()) >= threshold

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

def dequeue_batch(queue: QueueClient, max_batch_size: int):
    def delete_and_extract(msg: QueueMessage):
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

    

