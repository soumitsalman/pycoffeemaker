import logging
import os
from datetime import datetime, timezone
from functools import wraps
from logging import Logger
import msgpack

# log = logging.getLogger(__name__)

merge_lists = lambda *lists: [item for sublist in lists if sublist for item in sublist]

def log_runtime(logger: Logger):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = func(*args, **kwargs)
            logger.info(
                "execution time",
                extra={
                    "source": func.__name__,
                    "num_items": int((datetime.now() - start_time).total_seconds()),
                },
            )
            return result

        return wrapper

    return decorator


def log_runtime_async(logger: Logger):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = await func(*args, **kwargs)
            logger.info(
                "execution time",
                extra={
                    "source": func.__name__,
                    "num_items": int((datetime.now() - start_time).total_seconds()),
                },
            )
            return result

        return wrapper

    return decorator
