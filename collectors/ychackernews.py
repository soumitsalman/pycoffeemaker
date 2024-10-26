import time
import requests
from .individual import *
from pybeansack.datamodels import *
from pybeansack.utils import now

TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
COLLECTION_URL_TEMPLATE = "https://hacker-news.firebaseio.com/v0/item/%d.json"
STORY_URL_TEMPLATE = "https://news.ycombinator.com/item?id=%d"
YC = "ycombinator"

def collect(store_func):
    entries = requests.get(TOP_STORIES_URL, headers={"User-Agent": USER_AGENT}).json()
    collection_time = now()
    beans = [_extract(entry, collection_time) for entry in entries]
    beans = [bean for bean in beans if bean]
    store_func(beans)

def _extract(id: int, collection_time: int):
    try:
        entry = requests.get(COLLECTION_URL_TEMPLATE % id, timeout=2).json()  
        url = entry.get('url', STORY_URL_TEMPLATE % id)
        return \
            Bean(            
                url=url, # this is either a linked url or a direct post
                updated=collection_time,
                collected=collection_time,
                source=extract_source(url)[0],
                title=entry.get('title'),
                kind=BLOG if 'url' in entry else POST,
                text=load_from_html(entry['text']) if 'text' in entry else "", # load if it has a text which usually applies to posts
                author=entry.get('by'),
                created=int(entry.get('time'))), \
            Chatter(
                url=url,
                updated=collection_time,
                source=YC,
                container_url=STORY_URL_TEMPLATE % id,
                likes=entry.get('score'),
                comments=len(entry.get('kids', [])))
    except:
        return None
        # logger.warning("Failed loading from %s. Error: %s", COLLECTION_URL_TEMPLATE%id, str(err))

