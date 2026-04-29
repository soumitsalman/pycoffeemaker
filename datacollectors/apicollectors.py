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

def _extract_link(entry, feed, feed_url, site_url):
    if 'link' in entry: return full_url(site_url, entry.link)
    if 'links' in entry and entry.links: full_url(site_url, ic(entry.links[0]['href']))

### rss feed related utilities ###
def _extract_body(entry: feedparser.FeedParserDict) -> tuple[str, str]:
    # the body usually lives in <dc:content>, <content:encoded> or <description>
    summary, content = None, None
    if 'dc_content' in entry: content = entry.dc_content
    elif 'content' in entry: content = entry.content[0]['value'] if isinstance(entry.content, list) else entry.content    
    if 'summary' in entry: summary = entry.summary
    return strip_html_tags(summary), strip_html_tags(content or summary)

def _extract_tags(entry: feedparser.FeedParserDict) -> list[str]:
    if 'tags' in entry:
        return [tag.get('term') for tag in entry.tags if tag.get('term')]
    return None

def _extract_author_email(entry) -> str:
    return entry.get('author_detail', {}).get('email')

def _extract_language(entry, feed) -> str:
    return entry.get('language') or feed.get('language')

def _extract_feed_metadata(feed, feed_url) -> dict:
    return {
        # SITE_NAME: feed.get('title'),
        # DESCRIPTION: feed.get('subtitle') or feed.get('description'),
        # FAVICON: full_url(feed.get('link'), feed.get('icon')) if feed.get('icon') else full_url(feed.get('link'), feed.get('image', {}).get('href')) if feed.get('image') else None,
        RSS_FEED: feed_url,
        SITE_LANGUAGE: feed.get('language')
    }

def _extract_main_image(entry: feedparser.FeedParserDict) -> str:
    if ('links' in entry) and any(item for item in entry.links if "image" in item.get('type', "")):
        return next(item for item in entry.links if "image" in item.get('type', "")).get('href')
    if ('media_content' in entry) and entry.media_content:
        return entry.media_content[0].get('url')
    if ('media_thumbnail' in entry) and entry.media_thumbnail:
        return entry.media_thumbnail[0].get('url')
    if 'image' in entry:
        return entry.image.get('href')
    
def _get_site_url(*urls):
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

def _build_rss_item(feed, feed_url: str, site_url: str, entry: feedparser.FeedParserDict, default_kind: str):
    current_time = now()
    published_time = entry.get("published_parsed") or entry.get("updated_parsed")
    created_time = from_timestamp(time.mktime(published_time)) if published_time else current_time
    summary, content = _extract_body(entry)
    tags = _extract_tags(entry)
    author_email = _extract_author_email(entry)
    language = _extract_language(entry, feed)
    entry_link = _extract_link(entry, feed, feed_url, site_url)
    source = extract_source(entry_link)
    base_url = extract_base_url(entry_link)

    item = {
        URL: entry_link,
        BASE_URL: base_url,
        SOURCE: source,
        TITLE: entry.get('title'),
        SUMMARY: summary,
        CONTENT: content,
        AUTHOR: entry.get('author'),
        ARTICLE_LANGUAGE: language,
        SITE_LANGUAGE: feed.get('language'),
        TAGS: [tag.lower() for tag in (tags or []) if isinstance(tag, str) and tag.strip()],
        AUTHOR_EMAIL: author_email,
        IMAGEURL: None,
        CREATED: created_time,
        COLLECTED: current_time,
    }

    image_url = _extract_main_image(entry)
    if image_url:
        item[IMAGEURL] = full_url(site_url, image_url)

    item[KIND] = guess_article_type(item) or default_kind

    comments_url = entry.get('wfw_commentrss')
    comments_count = parse_int(entry.get('slash_comments') or entry.get('comments') or 0)
    if comments_url or comments_count > 0:
        item.update({
            CHATTER_URL: comments_url,
            URL: entry_link,
            PLATFORM: source,
            COLLECTED: current_time,
            COMMENTS: comments_count,
        })

    item.update({
        SOURCE: source,
        BASE_URL: base_url,
        COLLECTED: current_time,
        **_extract_feed_metadata(feed, feed_url),
    })

    return cleanup_item(item)

def _build_reddit_item(post, subreddit_name, default_kind: str):
    subreddit = f"r/{subreddit_name}"
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
            kind = guess_article_type({
                URL: url,
                BASE_URL: extract_base_url(url),
                SOURCE: source,
                TITLE: post.title,
                CONTENT: post.selftext,
                AUTHOR: post.author.name if post.author else None,
                CREATED: created_time,
                COLLECTED: current_time,
                TAGS: [],
            }) or default_kind
        else:
            url = reddit_submission_permalink(post.url)
            kind = POST
            source = subreddit

    base_url = extract_base_url(url)

    item = {
        URL: url,
        KIND: kind,
        TITLE: post.title,
        CONTENT: post.selftext,
        AUTHOR: post.author.name if post.author else None,
        SOURCE: source,
        BASE_URL: base_url,
        CREATED: created_time,
        COLLECTED: current_time,
        TAGS: [],
        PLATFORM: REDDIT,
        CHATTER_URL: chatter_link,
        FORUM: subreddit,
        LIKES: post.score,
        COMMENTS: post.num_comments,
    }

    return cleanup_item(item)

def _build_hackernews_item(story: dict, default_kind: str):
    current_time = now()
    created_time = from_timestamp(story['time'])
    story_id = story['id']

    if story.get('url'):
        url = story['url']
        source = extract_source(url)
        tags = []
        kind = guess_article_type({'URL': url, 'SOURCE': source}) or (SITE if "show hn" in story.get('title', '').lower() else default_kind)
    else:
        url = hackernews_story_permalink(story_id)
        source = HACKERNEWS
        tags = []
        kind = POST

    base_url = extract_base_url(url)

    item = {
        URL: url,
        KIND: kind,
        TITLE: story.get('title'),
        CONTENT: strip_html_tags(story['text']) if 'text' in story else None,
        AUTHOR: story.get('by'),
        SOURCE: source,
        BASE_URL: base_url,
        CREATED: created_time,
        COLLECTED: current_time,
        TAGS: tags,
        PLATFORM: HACKERNEWS,
        CHATTER_URL: hackernews_story_permalink(story_id),
        FORUM: str(story_id),
        LIKES: story.get('score'),
        COMMENTS: len(story.get('kids', [])),
    }

    return cleanup_item(item)


class APICollector:
    reddit_client = None

    def __init__(self, collect_callback: Callable = None, batch_size: int = BATCH_SIZE):
        self.collect_callback = collect_callback
        self.reddit_client = praw.Reddit(
            check_for_updates=True,
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=USER_AGENT + " (by u/IntelligentLeave680)",
            timeout=TIMEOUT,
            rate_limit_seconds=RATELIMIT_WAIT,
        )

    def collect_rssfeeds(self, feed_urls: list[str]) -> list[dict]:
        return merge_lists(_batch_run(self.collect_rssfeed, feed_urls))

    def collect_rssfeed(self, url: str) -> list[dict]:
        resp = requests.get(url, headers=_RSS_REQUEST_HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(BytesIO(resp.content))

        if feed.entries:
            source_url = _get_site_url(feed.feed.get('link'), url, feed.entries[0].link)
            return _return_collected(
                extract_source(source_url),
                [_build_rss_item(feed=feed, feed_url=url, site_url=source_url, entry=entry, default_kind=NEWS) for entry in feed.entries]
            )

    def collect_subreddit(self, subreddit_name, default_kind: str = NEWS):
        @retry(exceptions=(prawcore.exceptions.ResponseException), tries=2, delay=10, jitter=(5, 10))
        def _collect():
            sr = self.reddit_client.subreddit(subreddit_name)
            return [_build_reddit_item(post, subreddit_name, default_kind) for post in sr.hot(limit=25) if not excluded_url(post.url)]

        return _return_collected(subreddit_name, _collect())

    def collect_ychackernews(self, stories_urls = HACKERNEWS_STORIES_URLS) -> list[dict]:
        if isinstance(stories_urls, str):
            stories_urls = [stories_urls]
        ids = _batch_run(_fetch_json, stories_urls)
        ids = set(chain(*ids))
        stories = _batch_run(_fetch_json, [hackernews_story_metadata(id) for id in ids])
        stories = [_build_hackernews_item(story, BLOG) for story in stories if story and not excluded_url(story.get('url'))]
        return _return_collected(HACKERNEWS, stories)


class APICollectorAsync:
    reddit_client = None
    session = None

    def __init__(self, batch_size: int = BATCH_SIZE, collect_callback: Callable = None,):
        self.collect_callback = collect_callback
        self.throttle = asyncio.Semaphore(batch_size)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=BATCH_SIZE, limit_per_host=os.cpu_count()),
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
            raise_for_status=True,
        )
        self.reddit_client = asyncpraw.Reddit(
            check_for_updates=True,
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=USER_AGENT + " (by u/IntelligentLeave680)",
            timeout=TIMEOUT,
            rate_limit_seconds=RATELIMIT_WAIT,
            requestor_kwargs={"session": self.session},
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.reddit_client:
            await self.reddit_client.close()
            self.reddit_client = None
        if self.session:
            await self.session.close()
            self.session = None

    async def _fetch_json(self, url: str):
        try:
            async with self.throttle, self.session.get(url, headers=_JSON_REQUEST_HEADERS) as resp:
                return await resp.json()
        except Exception as e:
            log.warning(f"collection failed - {e.__class__.__name__}: {e}", extra={"source": url, "num_items": 1})

    async def collect_rssfeed(self, url: str, default_kind: str = NEWS) -> list[dict]:
        async with self.throttle, self.session.get(url, headers=_RSS_REQUEST_HEADERS) as resp:
            feed = feedparser.parse(await resp.text())

        if not feed.entries: return

        source_url = _get_site_url(feed.feed.get('link'), url, feed.entries[0].get('link'))
        return _return_collected(
            extract_source(source_url),
            [_build_rss_item(feed=feed, feed_url=url, site_url=source_url, entry=entry, default_kind=default_kind) for entry in feed.entries]
        )

    async def collect_subreddit(self, subreddit_name: str, default_kind: str = NEWS) -> list[dict]:
        @retry(tries=2, delay=10, jitter=(5, 10))
        async def _collect():
            async with self.throttle:
                sr = await self.reddit_client.subreddit(subreddit_name)
                return [_build_reddit_item(post, subreddit_name, default_kind) async for post in sr.hot(limit=25) if not excluded_url(post.url)]

        return _return_collected(subreddit_name, await _collect())    

    async def collect_ychackernews(self, stories_urls = HACKERNEWS_STORIES_URLS) -> list[dict]:
        if isinstance(stories_urls, str):
            stories_urls = [stories_urls]
        ids = await asyncio.gather(*[self._fetch_json(ids_url) for ids_url in stories_urls])
        ids = set(chain(*ids))
        stories = await asyncio.gather(*[self._fetch_json(hackernews_story_metadata(id)) for id in ids])
        stories = [_build_hackernews_item(story, BLOG) for story in stories if story and not excluded_url(story.get('url'))]
        return _return_collected(HACKERNEWS, stories)
    