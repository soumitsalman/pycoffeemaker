from functools import wraps
import os
from logging import Logger
from datetime import datetime
from coffeemaker.pybeansack.models import POST

END_OF_STREAM = "END_OF_STREAM"

WORDS_THRESHOLD_FOR_SCRAPING = int(os.getenv('WORDS_THRESHOLD_FOR_SCRAPING', 200)) # min words needed to not download the body
WORDS_THRESHOLD_FOR_INDEXING = int(os.getenv('WORDS_THRESHOLD_FOR_INDEXING', 70)) # mininum words needed to put it through indexing
WORDS_THRESHOLD_FOR_DIGESTING = int(os.getenv('WORDS_THRESHOLD_FOR_DIGESTING', 160)) # min words needed to use the generated summary

above_threshold = lambda bean, threshold: bean.text and len(bean.text.split()) >= threshold
is_indexable = lambda bean: above_threshold(bean, WORDS_THRESHOLD_FOR_INDEXING) # it has to have some text and the text has to be large enough
is_scrapable = lambda bean: not (bean.kind == POST or above_threshold(bean, WORDS_THRESHOLD_FOR_SCRAPING)) # if it is a post dont download it or if the body is large enough
use_summary = lambda text: text and len(text.split()) >= WORDS_THRESHOLD_FOR_DIGESTING # if the body is large enough
indexables = lambda beans: [bean for bean in beans if is_indexable(bean)] if beans else beans
scrapables = lambda beans: [bean for bean in beans if is_scrapable(bean)] if beans else beans

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

