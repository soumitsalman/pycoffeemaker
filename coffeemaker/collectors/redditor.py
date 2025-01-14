import asyncio
from typing import Callable
import praw
import os
from datetime import datetime as dt
from coffeemaker.pybeansack.datamodels import *
from coffeemaker.pybeansack.utils import now
from coffeemaker.collectors.individual import *

REDDIT = "Reddit"
STORY_URL_TEMPLATE = "https://www.reddit.com%s"
SUBREDDITS_FILE = os.path.dirname(os.path.abspath(__file__))+"/redditsources.txt"
MAX_LIMIT = 20

def collect(process_collection: Callable, subreddits: str|list[str] = SUBREDDITS_FILE):    
    if isinstance(subreddits, str):
        with open(subreddits, 'r') as file:
            subreddits = [line.strip() for line in file.readlines() if line.strip()]  
             
    reddit = praw.Reddit(
        client_id = os.getenv('REDDITOR_APP_ID'), 
        client_secret = os.getenv('REDDITOR_APP_SECRET'),
        user_agent = USER_AGENT
    )    
    [process_collection(collect_subreddit(reddit, source)) for source in subreddits]

async def collect_async(process_collection: Callable, subreddits: str|list[str] = SUBREDDITS_FILE):    
    if isinstance(subreddits, str):
        with open(subreddits, 'r') as file:
            subreddits = [line.strip() for line in file.readlines() if line.strip()]  
    client = praw.Reddit(
        client_id = os.getenv('REDDITOR_APP_ID'), 
        client_secret = os.getenv('REDDITOR_APP_SECRET'),
        user_agent = USER_AGENT
    )   
    tasks = (asyncio.create_task(
        process_collection(asyncio.to_thread(collect_subreddit, client, source)),
        name=source
    ) for source in subreddits)
    await asyncio.gather(*tasks)

def collect_subreddit(client, name) -> list[tuple[Bean, Chatter]]:    
    try:
        collection_time = now()
        return [extract(post, collection_time) for post in client.subreddit(name).hot(limit=MAX_LIMIT) if not is_non_text(post.url)]
    except:
        pass

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


    






