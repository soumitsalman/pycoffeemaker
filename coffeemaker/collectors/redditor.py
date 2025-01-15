from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Callable
import praw
import os
from datetime import datetime as dt
from coffeemaker.pybeansack.datamodels import *
from coffeemaker.pybeansack.utils import now
from coffeemaker.collectors.individual import *
from . import USER_AGENT, TIMEOUT

REDDIT = "Reddit"
STORY_URL_TEMPLATE = "https://www.reddit.com%s"
SUBREDDITS_FILE = os.path.dirname(os.path.abspath(__file__))+"/redditsources.txt"
MAX_LIMIT = 20

log = logging.getLogger(__name__)

def create_client():
    return praw.Reddit(
        client_id = os.getenv('REDDITOR_APP_ID'), 
        client_secret = os.getenv('REDDITOR_APP_SECRET'),
        user_agent = USER_AGENT,
        ratelimit_seconds = TIMEOUT,
        timeout = TIMEOUT
    )

def collect(process_collection: Callable, subreddits: str|list[str] = SUBREDDITS_FILE):    
    if isinstance(subreddits, str):
        with open(subreddits, 'r') as file:
            subreddits = [line.strip() for line in file.readlines() if line.strip()]  
    client = create_client()
    [process_collection(collect_subreddit(client, source)) for source in subreddits]

def register_collectors(register: Callable, subreddits: str|list[str] = SUBREDDITS_FILE):
    if isinstance(subreddits, str):
        with open(subreddits, 'r') as file:
            subreddits = [line.strip() for line in file.readlines() if line.strip()]  
    client = create_client()
    [register(lambda source=source: collect_subreddit(client, source)) for source in subreddits]

async def collect_async(process_collection: Callable, subreddits: str|list[str] = SUBREDDITS_FILE):    
    if isinstance(subreddits, str):
        with open(subreddits, 'r') as file:
            subreddits = [line.strip() for line in file.readlines() if line.strip()]  
    client = create_client()
    with ThreadPoolExecutor() as executor:
        collections = executor.map(lambda source: collect_subreddit(client, source), subreddits)
        executor.map(process_collection, collections)

def collect_subreddit(client, name) -> list[tuple[Bean, Chatter]]:    
    try:
        collection_time = now()
        return [extract(post, collection_time) for post in client.subreddit(name).hot(limit=MAX_LIMIT) if not is_non_text(post.url)]
    except Exception as e:
        print(e)
        log.warning("collection failed", extra={"source": name, "num_items": 1})

def collect_user(client, name): 
    try:
        collection_time = now()
        return [extract(post, collection_time) for post in client.redditor(name).submissions.new(limit=MAX_LIMIT) if not is_non_text(post.url)]
    except:
        pass
    
def extract(post, collection_time) -> tuple[Bean, Chatter]: 
    subreddit = f"r/{post.subreddit.display_name}"
    return (
        Bean(
            url=post.url,
            created=dt.fromtimestamp(post.created_utc),
            collected=collection_time,
            updated=collection_time,
            # this is done because sometimes is_self value is wrong
            source=subreddit if post.is_self else (extract_source(post.url)[0] or subreddit),
            title=post.title,
            kind=POST if post.is_self else NEWS,
            text=post.selftext,
            author=post.author.name if post.author else None
        ),
        Chatter(
            url=post.url,
            chatter_url=STORY_URL_TEMPLATE % post.permalink,            
            source=REDDIT,                        
            channel=subreddit,
            collected=collection_time,
            likes=post.score,
            comments=post.num_comments,
            subscribers=post.subreddit.subscribers
        )
    )


    






