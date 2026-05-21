from datetime import datetime, timezone, timedelta
from functools import wraps
from logging import Logger
from nlp import valid_tags

#--------------------------------#
# Object Types
#--------------------------------#
BEANS = "beans"
SIGNALS = "signals"
PUBLISHERS = "publishers"
CHATTERS = "chatters"

#--------------------------------#
# Processing States
#--------------------------------#
COLLECTED = "collected"
EMBEDDED = "embedded"
CLUSTERED = "clustered"
DIGESTED = "digested"
CLASSIFIED = "classified"
CONSOLIDATED = "consolidated"
EXTRACTED = "extracted"
BEANSACKED = "beansacked"
CUPBOARDED = "cupboarded"

#--------------------------------#
# Bean, Publisher, Chatter, Event, Signal Fields
#--------------------------------#
ID = "id"
DIGEST = "digest"
BRIEFING = "briefing"
EMBEDDING = "embedding"
ENTITIES = "entities"
CATEGORIES = "categories"
SENTIMENTS = "sentiments"
RELATED = "related"

#--------------------------------#
# Utility Functions
#--------------------------------#
now = datetime.now
ndays_ago = lambda days: (now() - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
run_id = lambda: now().strftime("%A, %b-%d-%Y")
merge_lists = lambda *lists: [item for sublist in lists if sublist for item in sublist]
merge_tags = lambda *tag_lists: list(set(valid_tags(item for tag_list in tag_lists if tag_list for item in tag_list)))

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
