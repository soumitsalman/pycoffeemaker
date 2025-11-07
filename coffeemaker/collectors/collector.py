import asyncio
import json
import logging
import os
from urllib.parse import urljoin
import aiohttp
import asyncpraw
import praw
import prawcore
import feedparser
import requests
import yaml
import time
from datetime import datetime, timezone
from typing import Callable
from retry import retry
from io import BytesIO
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
from .utils import *
from coffeemaker.pybeansack.models import *
from icecream import ic

log = logging.getLogger(__name__)

# assigning 16 io threads per cpu
BATCH_SIZE = int(os.getenv('BATCH_SIZE', os.cpu_count()))

_RSS_REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    'Accept-encoding': 'gzip, deflate',
    'A-IM': 'feed',
    'Accept': "application/atom+xml,application/rdf+xml,application/rss+xml,application/x-netcdf,application/xml;q=0.9,text/xml;q=0.2,*/*;q=0.1"
}
_JSON_REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    'Accept-encoding': 'gzip, deflate',
    'Accept': "application/json,text/json"
}

REDDIT = "Reddit"
HACKERNEWS = "ycombinator"
HACKERNEWS_TOP_STORIES = "https://hacker-news.firebaseio.com/v0/topstories.json"
HACKERNEWS_NEW_STORIES = "https://hacker-news.firebaseio.com/v0/newstories.json"
# HACKERNEWS_JOB_STORIES = "https://hacker-news.firebaseio.com/v0/jobstories.json"
HACKERNEWS_ASK_STORIES = "https://hacker-news.firebaseio.com/v0/askstories.json"
HACKERNEWS_SHOW_STORIES = "https://hacker-news.firebaseio.com/v0/showstories.json"
HACKERNEWS_STORIES_URLS = [HACKERNEWS_TOP_STORIES, HACKERNEWS_NEW_STORIES, HACKERNEWS_ASK_STORIES, HACKERNEWS_SHOW_STORIES]

from_timestamp = lambda timestamp: min(now(), datetime.fromtimestamp(timestamp, timezone.utc)) if timestamp else now()
reddit_submission_permalink = lambda permalink: f"https://www.reddit.com{permalink}"
hackernews_story_metadata = lambda id: f"https://hacker-news.firebaseio.com/v0/item/{id}.json"
hackernews_story_permalink = lambda id: f"https://news.ycombinator.com/item?id={id}"

def _batch_run(func: Callable, sources: list):
    results = None
    with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="collector") as executor:
        results = list(executor.map(func, sources))
    return results

### rss feed related utilities ###
def _extract_body(entry: feedparser.FeedParserDict) -> tuple[str, str]:
    # the body usually lives in <dc:content>, <content:encoded> or <description>
    summary, content = None, None
    if 'dc_content' in entry: content = entry.dc_content
    elif 'content' in entry: content = entry.content[0]['value'] if isinstance(entry.content, list) else entry.content    
    if 'summary' in entry: summary = entry.summary
    return strip_html_tags(summary), strip_html_tags(content or summary)

def _extract_main_image(entry: feedparser.FeedParserDict) -> str:
    if ('links' in entry) and any(item for item in entry.links if "image" in item.get('type', "")):
        return next(item for item in entry.links if "image" in item.get('type', "")).get('href')
    if ('media_content' in entry) and entry.media_content:
        return entry.media_content[0].get('url')
    elif ('media_thumbnail' in entry) and entry.media_thumbnail:
        return entry.media_thumbnail[0].get('url')
    
def _get_source_url(*urls):
    for url in urls:
        if url and isinstance(url, str) and url.startswith('http'): return url

def _fetch_json(url: str):
    try: 
        resp = requests.get(url, headers=_JSON_REQUEST_HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e: 
        log.warning(f"collection failed - {e.__class__.__name__}: {e}", extra={"source": url, "num_items": 1})

def _return_collected(source, collected: list|None):
    if collected: log.debug("collected", extra={"source": source, "num_items": len(collected)})
    else: log.debug("collection failed", extra={"source": source, "num_items": 1})
    return collected

merge_lists = lambda results: list(chain(*(r for r in results if r))) 

def parse_sources(sources: str) -> dict:
    if os.path.exists(sources):
        with open(sources, 'r') as file:
            data = yaml.safe_load(file)
    else: data = yaml.safe_load(sources)
    return data['sources']

class APICollector:
    _reddit_client = None
    session = None
    throttle = None

    def __init__(self, collect_callback: Callable = None, batch_size: int = BATCH_SIZE):  
        self.throttle = asyncio.Semaphore(batch_size)
        self.collect_callback = collect_callback
    
    @property
    def reddit_client(self):
        if not self._reddit_client:
            self._reddit_client = praw.Reddit(
                check_for_updates=True,
                client_id=os.getenv("REDDIT_CLIENT_ID"),
                client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
                user_agent=USER_AGENT+" (by u/IntelligentLeave680)",
                timeout=TIMEOUT,
                rate_limit_seconds=RATELIMIT_WAIT,
            )
        return self._reddit_client
    
    async def __aenter__(self):
        """Async context manager enter"""
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=BATCH_SIZE, limit_per_host=os.cpu_count()),
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
            raise_for_status=True
        )
        self._reddit_client = asyncpraw.Reddit(
            check_for_updates=True,
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=USER_AGENT+" (by u/IntelligentLeave680)",
            timeout=TIMEOUT,
            rate_limit_seconds=RATELIMIT_WAIT,
            requestor_kwargs={"session": self.session}
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self._reddit_client: 
            await self.session.close()
            await self._reddit_client.close()
            self.session = None
            self._reddit_client = None

    ### rss feed related utilities  ###
    def collected_rssfeeds(self, feed_urls: list[str]) -> list[Bean]:
        return merge_lists(_batch_run(self.collect_rssfeed, feed_urls))

    def collect_rssfeed(self, url: str) -> list[Bean]:        
        resp = requests.get(url, headers=_RSS_REQUEST_HEADERS, timeout=TIMEOUT) 
        resp.raise_for_status()  # Raise exception for bad status codes
        feed = feedparser.parse(BytesIO(resp.content))
                
        if feed.entries and (feed.feed.get('language') or 'en').startswith('en'): 
            source_url = _get_source_url(feed.feed.get('link'), url, feed.entries[0].link)
            return _return_collected(
                extract_source(source_url), 
                [self._from_rssfeed(entry, NEWS, source_url) for entry in feed.entries]
            ) 
   
    async def collect_rssfeed_async(self, url: str, default_kind: str = NEWS) -> list[Bean]:
        async with self.throttle, self.session.get(url, headers=_RSS_REQUEST_HEADERS) as resp:
            feed = feedparser.parse(await resp.text())
                
        # assume that if language is not mentioned, it is english
        if feed.entries and (feed.feed.get('language') or 'en').startswith('en'): 
            source_url = _get_source_url(feed.feed.get('link'), url, feed.entries[0].link)
            return _return_collected(
                extract_source(source_url), 
                [self._from_rssfeed(entry, default_kind, source_url) for entry in feed.entries]
            )

    def _from_rssfeed(self, entry: feedparser.FeedParserDict, default_kind: str, source_url: str):
        current_time = now()
        published_time = entry.get("published_parsed") or entry.get("updated_parsed")
        created_time = from_timestamp(time.mktime(published_time)) if published_time else current_time
        summary, content = _extract_body(entry)
        # if not entry.link.startswith("http"): 
        #     ic(source_url, entry.link, urljoin(source_url, entry.link))
        entry_link = urljoin(source_url, entry.link)
        source = extract_source(entry_link)

        if entry.get('wfw_commentrss') and entry.get('slash_comments'): chatter = Chatter(
            chatter_url=entry.get('wfw_commentrss'),
            url=entry_link,
            source=source,
            collected=current_time,
            comments=parse_int(entry.slash_comments)
        ) 
        else: chatter = None
        
        return {
            "bean": Bean(
                url=entry_link,
                kind=guess_article_type(entry_link, source) or default_kind,
                source=source,
                title=entry.get('title'),
                summary=summary,
                content=content,
                author=entry.get('author'),        
                image_url=_extract_main_image(entry),
                created=created_time,         
                collected=current_time
            ),
            "chatter": chatter,
            "publisher": Publisher(
                source=source,
                base_url=extract_base_url(entry_link)
            )
        }

    def _cleanup_tags(self, html: str) -> str:
        """Converts the given html into a markdown"""
        # if html: return self.md_generator.generate_markdown(input_html=html).raw_markdown.strip()
        if html: return strip_html_tags(html)
    
    ### reddit related utilities ###
    def collect_subreddits(self, subreddit_names: list[str]):
        return merge_lists(_batch_run(self.collect_subreddit, subreddit_names))
    
    def collect_subreddit(self, subreddit_name, default_kind: str = NEWS):
        @retry(exceptions=(prawcore.exceptions.ResponseException), tries=2, delay=10, jitter=(5, 10))
        def _collect():
            sr = self.reddit_client.subreddit(subreddit_name)
            return [self._from_reddit_post(post, subreddit_name, default_kind) for post in sr.hot(limit=25) if not excluded_url(post.url)]
        
        return _return_collected(subreddit_name, _collect())      
    
    async def collect_subreddit_async(self, subreddit_name: str, default_kind: str = NEWS) -> list[Bean]:
        @retry(tries=2, delay=10, jitter=(5, 10))
        async def _collect():
            async with self.throttle:
                sr = await self.reddit_client.subreddit(subreddit_name)
                return [self._from_reddit_post(post, subreddit_name, default_kind) async for post in sr.hot(limit=25) if not excluded_url(post.url)]
        
        return _return_collected(subreddit_name, await _collect()) 

    def _from_reddit_post(self, post, sr_name, default_kind): 
        subreddit = f"r/{sr_name}"
        current_time = now()
        created_time = from_timestamp(post.created_utc)
        chatter_link = reddit_submission_permalink(post.permalink)

        if post.is_self:    
            url = chatter_link
            source = subreddit
            kind = POST
        else:
            source = extract_source(post.url)
            if source:
                url = post.url
                kind = guess_article_type(url, source) or default_kind
            else: # sometimes the links are itself a reddit post
                url = reddit_submission_permalink(post.url)
                kind = POST
                source = subreddit
        return {
            "bean": Bean(
                url=url,
                kind=kind,
                title=post.title,
                content=post.selftext,
                author=post.author.name if post.author else None,
                source=source,
                created=created_time,
                collected=current_time
            ),
            "chatter": Chatter(
                chatter_url=chatter_link,
                url=url,
                source=REDDIT,                        
                forum=subreddit,
                collected=current_time,
                likes=post.score,
                comments=post.num_comments
            ),
            "publisher": Publisher(
                source=source,
                base_url=extract_base_url(url)
            )
        }
    
    ### hackernews related utilities ###
    async def collect_ychackernews_async(self, stories_urls = HACKERNEWS_STORIES_URLS) -> list[Bean]:
        if isinstance(stories_urls, str): stories_urls = [stories_urls]
        ids = await asyncio.gather(*[self._fetch_json_async(ids_url) for ids_url in stories_urls])
        ids = set(chain(*ids))
        stories = await asyncio.gather(*[self._fetch_json_async(hackernews_story_metadata(id)) for id in ids])
        stories = [self._from_hackernews_story(story, BLOG) for story in stories if story and not excluded_url(story.get('url'))]
        return _return_collected(HACKERNEWS, stories)
    
    async def _fetch_json_async(self, url: str):
        try:
            async with self.throttle, self.session.get(url, headers=_JSON_REQUEST_HEADERS) as resp:
                body = await resp.json()
            return body
        except Exception as e: 
            log.warning(f"collection failed - {e.__class__.__name__}: {e}", extra={"source": url, "num_items": 1}) 

    def collect_ychackernews(self, stories_urls = HACKERNEWS_STORIES_URLS) -> list[Bean]:
        if isinstance(stories_urls, str): stories_urls = [stories_urls]
        ids = _batch_run(_fetch_json, stories_urls)
        ids = set(chain(*ids))
        stories = _batch_run(_fetch_json, [hackernews_story_metadata(id) for id in ids])
        stories = [self._from_hackernews_story(story, BLOG) for story in stories if story and not excluded_url(story.get('url'))]
        return _return_collected(HACKERNEWS, stories)
    
    def _from_hackernews_story(self, story: dict, default_kind: str):
        # either its a shared url or it is a text
        current_time = now()
        created_time = from_timestamp(story['time'])

        id = story['id']
        if story.get('url'):
            url = story['url']
            source = extract_source(url)
            kind = guess_article_type(url, source) or default_kind
        else:
            url = hackernews_story_permalink(id)
            source = HACKERNEWS           
            kind = POST
                    
        return {
            "bean": Bean(            
                url=url, # this is either a linked url or a direct post
                kind=kind, # blog, post or job
                title=story.get('title'),
                content=self._cleanup_tags(story['text']) if 'text' in story else None, # load if it has a text which usually applies to posts
                author=story.get('by'),
                source=source,
                created=created_time,                
                collected=current_time
            ),
            "chatter": Chatter(
                chatter_url=hackernews_story_permalink(id),
                url=url,
                source=HACKERNEWS,
                forum=str(id),
                collected=current_time,
                likes=story.get('score'),
                comments=len(story.get('kids', []))
            ),
            "publisher": Publisher(
                source=source,
                base_url=extract_base_url(url)
            )
        }
    