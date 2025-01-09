import os
import time
from typing import Callable
import feedparser
from coffeemaker.pybeansack.datamodels import Bean, NEWS
from coffeemaker.pybeansack.utils import now
from coffeemaker.collectors.individual import *
from datetime import datetime as dt

DEFAULT_FEEDS = os.path.dirname(os.path.abspath(__file__))+"/rssfeedsources.txt"

# reads the list of feeds from a file path and collects
# if sources is a string then it will be treated as a file path or else it will be a an array
def collect(store_beans, sources: str|list[str] = DEFAULT_FEEDS):
    if isinstance(sources, str):
        # if sources are not provided, assume that there is sources_file provided
        with open(sources, 'r') as file:
            sources = file.readlines()
    # santize the urls and start collecting
    sources = [url.strip() for url in sources if url.strip()]
    [store_beans(collect_from(url)) for url in sources]  
    
def collect_functions(sources: str|list[str] = DEFAULT_FEEDS) -> list[Callable]:
    if isinstance(sources, str):
        # if sources are not provided, assume that there is sources_file provided
        with open(sources, 'r') as file:
            sources = file.readlines()
    # santize the urls and start collecting
    sources = [url.strip() for url in sources if url.strip()]
    return [lambda: (collect_from, url) for url in sources]
        
def collect_from(feed_url: str, kind = NEWS) -> list[Bean]:
    try:
        feed = feedparser.parse(feed_url, agent=USER_AGENT)
        # if we know that this feed is NOT in english, just skip for now. if language is not specified, assume it is english
        if not feed.get("language", "en-US").startswith("en-"):
            return []    
        collection_time = now()
        # collect only the ones that is english. if language is not specified, assume it is english
        source = extract_source_name(feed)
        return [extract(entry, source, kind, collection_time) for entry in feed.entries if entry.get("language", "en-US").startswith("en-")]
    except: # Exception as e:
        # ic(e)
        return None

def extract(entry, source, kind, collection_time) -> Bean:
    body, summary = _extract_body_and_summary(entry)  
    parsed_time = entry.get("published_parsed") or entry.get("updated_parsed")
    created_time = dt.fromtimestamp(time.mktime(parsed_time)) if parsed_time else now()
    # is_valid_tag = lambda tag: len(tag)>= MIN_TAG_LEN and len(tag) <= MAX_TAG_LEN and ("/" not in tag)    
    return Bean(
        url=entry.link,
        # in case of rss feed, the created time is the same as the updated time during collection. if it is mentioned in a social media feed then the updated time will get change
        created=created_time,         
        collected=collection_time,
        updated=created_time,
        source=source,
        title=entry.title,
        kind=kind,
        text=body,
        summary=summary,
        author=entry.get('author'),        
        image_url=_extract_image_link(entry)
    )    

def extract_source_name(feed):
    if feed.entries:
        res = load_from_url(feed.entries[0].link)
        return site_name(res) or extract_source(feed.entries[0].link)[0]

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

