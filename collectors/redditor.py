from itertools import chain
import praw
import os
from pybeansack.datamodels import *
from .individual import *
from icecream import ic
import time

SOURCE = "Reddit"
STORY_URL_TEMPLATE = "https://www.reddit.com%s"
SUBREDDITS_FILE = os.path.dirname(os.path.abspath(__file__))+"/subreddits.txt"
MAX_LIMIT = 20

def collect(store_func):
    reddit = praw.Reddit(
        client_id = os.getenv('REDDITOR_APP_ID'), 
        client_secret = os.getenv('REDDITOR_APP_SECRET'),
        user_agent = USER_AGENT
    )
    collection_time = int(time.time())
    with open(SUBREDDITS_FILE, 'r') as file:
        subreddits = [line.strip() for line in file.readlines()]   
    store_func(list(chain(*(collect_subreddit(reddit, source, collection_time) for source in subreddits))))


def collect_subreddit(client, name, collection_time) -> list:    
    try:
        return [makedatamodel(post, collection_time) for post in client.subreddit(name).hot(limit=MAX_LIMIT) if not is_non_text(post.url)]
    except:
        return []

def collect_user(client, name, collection_time):    
    user = client.redditor(name)
    return [makedatamodel(post, collection_time) for post in user.submissions.new(limit=MAX_LIMIT) if not is_non_text(post.url)]

def makedatamodel(post, collection_time): 
    return (
        Bean(
            url=post.url,
            updated=collection_time,
            source=SOURCE,
            title=post.title,
            kind=POST if post.is_self else NEWS,
            text = post.selftext,
            author=post.author.name if post.author else None,
            created=int(post.created_utc)
        ),
        Chatter(
            url=post.url,
            updated=collection_time,
            source=SOURCE,
            container_url=STORY_URL_TEMPLATE%post.permalink,            
            channel=post.subreddit.display_name,
            likes=post.score,
            comments=post.num_comments
        )
    )


    






