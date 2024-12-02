from itertools import chain
import praw
import os
from datetime import datetime as dt
from pybeansack.datamodels import *
from pybeansack.utils import now
from .individual import *

REDDIT = "Reddit"
STORY_URL_TEMPLATE = "https://www.reddit.com%s"
SUBREDDITS_FILE = os.path.dirname(os.path.abspath(__file__))+"/redditsources.txt"
MAX_LIMIT = 20

def collect(store_beans, store_chatters):
    reddit = praw.Reddit(
        client_id = os.getenv('REDDITOR_APP_ID'), 
        client_secret = os.getenv('REDDITOR_APP_SECRET'),
        user_agent = USER_AGENT
    )
    collection_time = now()
    with open(SUBREDDITS_FILE, 'r') as file:
        subreddits = [line.strip() for line in file.readlines()]   

    for source in subreddits:
        items = collect_subreddit(reddit, source, collection_time)
        if items:
            store_beans([item[0] for item in items])
            store_chatters([item[1] for item in items])

def collect_subreddit(client, name, collection_time) -> list[tuple[Bean, Chatter]]:    
    try:
        return [extract(post, collection_time) for post in client.subreddit(name).hot(limit=MAX_LIMIT) if not is_non_text(post.url)]
    except:
        return None

def collect_user(client, name, collection_time):    
    user = client.redditor(name)
    return [extract(post, collection_time) for post in user.submissions.new(limit=MAX_LIMIT) if not is_non_text(post.url)]

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
            text=post.selftext.strip(),
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


    






