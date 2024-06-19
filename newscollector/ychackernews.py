from bs4 import BeautifulSoup
import requests
from shared.utils import create_logger, load_text_from_url
from beanops.datamodels import *
from icecream import ic

TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
COLLECTION_URL_TEMPLATE = "https://hacker-news.firebaseio.com/v0/item/%d.json"
STORY_URL_TEMPLATE = "https://news.ycombinator.com/item?id=%d"
SOURCE = "YC hackernews"

logger = create_logger("ychackernews collector")

def collect(store_func):
    items = requests.get(TOP_STORIES_URL).json()
    beans = [collect_from(item) for item in items]
    beans = [bean for bean in beans if bean and bean.text]
    logger.info("%d items collected from %s", len(beans), SOURCE)
    store_func(beans)

def collect_from(id: int):
    try:
        body = requests.get(COLLECTION_URL_TEMPLATE % id, timeout=2).json()
        url = body.get('url')   
        bean = Bean(url=url, text=load_text_from_url(url), kind=ARTICLE) if url else Bean(url=STORY_URL_TEMPLATE % id, text=BeautifulSoup(body.get('text'), "html.parser").get_text(strip=True), kind=POST)        
        bean.source=SOURCE
        bean.title=body.get('title')
        bean.author=body.get('by')
        bean.created=body.get('time')
        bean.noise=Noise(
            container_url=STORY_URL_TEMPLATE % id,
            likes=body.get('score'),
            comments=len(body.get('kids', [])),
            source=SOURCE)   
        return bean     
    except Exception as err:
        logger.warning("Failed loading from %s. Error: %s", COLLECTION_URL_TEMPLATE%id, str(err))