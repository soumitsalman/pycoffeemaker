import asyncio
from datetime import datetime as dt
from typing import Callable
import requests
from coffeemaker.collectors.individual import *
from coffeemaker.pybeansack.datamodels import *
from coffeemaker.pybeansack.utils import now

TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
COLLECTION_URL_TEMPLATE = "https://hacker-news.firebaseio.com/v0/item/%d.json"
STORY_URL_TEMPLATE = "https://news.ycombinator.com/item?id=%d"
YC = "ycombinator"

def collect(store_beans, store_chatters):
    entries = requests.get(TOP_STORIES_URL, headers={"User-Agent": USER_AGENT}).json()
    collection_time = now()
    items = [extract(entry, collection_time) for entry in entries]
    store_beans([item[0] for item in items if item])
    store_chatters([item[1] for item in items if item])
    
def collect_functions() -> list[Callable]   :
    entries = requests.get(TOP_STORIES_URL, headers={"User-Agent": USER_AGENT}).json()
    collection_time = now()    
    return [lambda: [extract(entry, collection_time) for entry in entries]]

def extract(id: int, collection_time: int):
    try:
        entry = requests.get(COLLECTION_URL_TEMPLATE % id, timeout=2).json()  
        url = entry.get('url', STORY_URL_TEMPLATE % id)
        created = dt.fromtimestamp(entry['time']) if 'time' in entry else collection_time
        return (
            Bean(            
                url=url, # this is either a linked url or a direct post
                # initially the bean's updated time will be the same as the created time
                # if there is a chatter that links to this, then the updated time will be changed to collection time of the chatter
                created=created,                
                collected=collection_time,
                updated=collection_time,
                source=extract_source(url)[0],
                title=entry.get('title'),
                kind=BLOG if 'url' in entry else POST,
                text=load_from_html(entry['text']) if 'text' in entry else "", # load if it has a text which usually applies to posts
                author=entry.get('by')                
            ), 
            Chatter(
                url=url,
                chatter_url=STORY_URL_TEMPLATE % id,
                collected=collection_time,
                source=YC,
                likes=entry.get('score'),
                comments=len(entry.get('kids', []))
            )
        )
    except:
        return None
        # logger.warning("Failed loading from %s. Error: %s", COLLECTION_URL_TEMPLATE%id, str(err))

