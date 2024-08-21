import os
import feedparser
import tldextract
from pybeansack.utils import create_logger
from pybeansack.datamodels import Bean, NEWS
from .individual import *
import time
from icecream import ic

DEFAULT_FEEDS = os.path.dirname(os.path.abspath(__file__))+"/feedsources.txt"

logger = create_logger("rssfeed")

# reads the list of feeds from a file path and collects
# if sources is a string then it will be treated as a file path or else it will be a an array
def collect(sources: str|list[str] = DEFAULT_FEEDS, store_func = None):
    if isinstance(sources, str):
        # if sources are not provided, assume that there is sources_file provided
        with open(sources, 'r') as file:
            sources = file.readlines()
    # santize the urls and start collecting
    sources = [url.strip() for url in sources if url.strip()]
    for url in sources:
        beans = collect_from(url)   
        store_func(beans)       
        
def collect_from(feed_url: str, content_kind = NEWS):
    feed = feedparser.parse(feed_url, agent=USER_AGENT)
    # if we know that this feed is NOT in english, just skip for now. if language is not specified, assume it is english
    if not feed.get("language", "en-US").startswith("en-"):
        return []    
    collection_time = int(time.time())
    # collect only the ones that is english. if language is not specified, assume it is english
    return [_to_bean(entry, content_kind, collection_time) for entry in feed.entries if entry.get("language", "en-US").startswith("en-")]

def _to_bean(entry, kind, collection_time):
    body, summary = _extract_body_and_summary(entry)    
    created_time = collection_time
    if 'published_parsed' in entry:
        created_time = int(time.mktime(entry.published_parsed))
    elif 'updated_parsed' in entry:
        created_time = int(time.mktime(entry.updated_parsed))
    is_valid_tag = lambda tag: len(tag)>= MIN_TAG_LEN and len(tag) <= MAX_TAG_LEN and ("/" not in tag)

    return Bean(
        url=entry.link,
        updated=collection_time,
        source=tldextract.extract(entry.link).domain,
        title=entry.title,
        kind=kind,
        text=body,
        summary=summary,
        author=entry.get('author'),
        created=created_time,
        tags=[tag.term for tag in entry.get('tags', []) if is_valid_tag(tag.term)],
        image_url=_extract_image_link(entry)
    )    

# 'links': 
# [{'href': 'https://www.iflscience.com/iflscience-the-big-questions-can-we-make-dogs-live-longer-75450',
# 'rel': 'alternate',
# 'type': 'text/html'},
# {'href': 'https://assets.iflscience.com/assets/articleNo/75450/aImg/78048/tbqs4e4-l.jpg',
# 'length': '165573',
# 'rel': 'enclosure',
# 'type': 'image/jpeg'}],
# 
# 'media_content': 
# [{}],
# 
# 'media_thumbnail': 
# [{'height': '4603',
# 'url': 'https://media.wired.com/photos/66a257bc453579321b858dd1/master/pass/GettyImages-2162541554.jpg',
# 'width': '6904'}],
def _extract_image_link(entry):
    if ('links' in entry) and any(item for item in entry.links if "image" in item.get('type', "")):
        return next(item for item in entry.links if "image" in item.get('type', "")).get('href')
    if ('media_content' in entry) and entry.media_content:
        return entry.media_content[0].get('url')
    elif ('media_thumbnail' in entry) and entry.media_thumbnail:
        return entry.media_thumbnail[0].get('url')

MIN_TAG_LEN = 3
MAX_TAG_LEN = 20

def _extract_body_and_summary(entry) -> tuple[str, str]:
    body_html  = ""
    # the body usually lives in <dc:content>, <content:encoded> or <description>
    if 'dc_content' in entry:
        body_html = entry.dc_content
    elif 'content' in entry:        
        body_html = entry.content[0]['value'] if isinstance(entry.content, list) else entry.content    
    body = load_from_html(body_html)
    summary = load_from_html(entry.get('summary'))    
    
    # now time for some heuristics when there is no <dc:content> or <content>
    if len(body or "") < len(summary or ""):
        body = summary
    return body, summary
