import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime as dt
from typing import Callable, Coroutine
import requests
from coffeemaker.collectors.individual import *
from coffeemaker.pybeansack.datamodels import *
from coffeemaker.pybeansack.utils import now
from . import USER_AGENT, TIMEOUT


TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
COLLECTION_URL_TEMPLATE = "https://hacker-news.firebaseio.com/v0/item/%d.json"
STORY_URL_TEMPLATE = "https://news.ycombinator.com/item?id=%d"
YC = "ycombinator"
    
def collect(process_collection: Callable):
    process_collection(_collect())

def register_collectors(register: Callable):
    register(_collect)

async def collect_async(process_collection: Callable): 
    collection_time = now() 
    entries = requests.get(TOP_STORIES_URL, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT).json()
    with ThreadPoolExecutor() as executor:
        items = executor.map(lambda entry: extract(entry, collection_time), entries)
        items = [item for item in items if item]
        # since there is only one kind of items we are collecting
        executor.map(process_collection, [items])
    
def _collect():
    entries = requests.get(TOP_STORIES_URL, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT).json()
    collection_time = now()    
    items =  [extract(entry, collection_time) for entry in entries]
    return [item for item in items if item]

def extract(id: int, collection_time: int) -> tuple[Bean, Chatter]:
    try:
        entry = requests.get(COLLECTION_URL_TEMPLATE % id, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT).json()  
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
                text=collect_html(entry['text']) if 'text' in entry else "", # load if it has a text which usually applies to posts
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
        pass

