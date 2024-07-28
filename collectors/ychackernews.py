import time
from bs4 import BeautifulSoup
import requests
from .individual import *
from pybeansack.utils import create_logger
from pybeansack.datamodels import *
from icecream import ic

TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
COLLECTION_URL_TEMPLATE = "https://hacker-news.firebaseio.com/v0/item/%d.json"
STORY_URL_TEMPLATE = "https://news.ycombinator.com/item?id=%d"
SOURCE = "YC hackernews"
logger = create_logger("ychackernews")

def collect(store_func):
    entries = requests.get(TOP_STORIES_URL, headers={"User-Agent": USER_AGENT}).json()
    collection_time = int(time.time())
    beans = [_extract(entry, collection_time) for entry in entries[:30]]
    logger.info("%d items collected from %s", len(beans), SOURCE)
    store_func(beans)

def _extract(id: int, collection_time: int):
    try:
        entry = requests.get(COLLECTION_URL_TEMPLATE % id, timeout=2).json()  
        url = entry.get('url', STORY_URL_TEMPLATE % id)
        return \
            Bean(            
                url=url, # this is either a linked url or a direct post
                updated=collection_time,
                source=SOURCE,
                title=entry.get('title'),
                kind=NEWS if 'url' in entry else POST,
                text=load_from_html(entry.get('text')), # load if it has a text which usually applies to posts
                author=entry.get('by'),
                created=int(entry.get('time'))), \
            Chatter(
                url=url,
                source=SOURCE,
                container_url=STORY_URL_TEMPLATE % id,
                likes=entry.get('score'),
                comments=len(entry.get('kids') or []))
    except Exception as err:
        logger.warning("Failed loading from %s. Error: %s", COLLECTION_URL_TEMPLATE%id, str(err))

