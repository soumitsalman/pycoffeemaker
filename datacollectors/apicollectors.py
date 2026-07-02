import asyncio
import json
import zipfile
import re
import time
from utils.logs import get_logger
import os
from urllib.parse import urljoin
import aiohttp
import asyncpraw
import feedparser
import html2text
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_fixed, wait_random
from io import BytesIO
from itertools import chain
from utils.fields import *
from .settings import *
from .normalize import *

log = get_logger(__name__)

_RSS_REQUEST_HEADERS = {
    "User-Agent": BROWSER_USER_AGENT,
    'Accept-encoding': 'gzip, deflate',
    'Accept-Language': 'en-US,en;q=0.9',
    'A-IM': 'feed',
    'Accept': "application/atom+xml,application/rdf+xml,application/rss+xml,application/x-netcdf,application/xml;q=0.9,text/xml;q=0.2,*/*;q=0.1"
}
_JSON_HEADERS = {
    "User-Agent": BROWSER_USER_AGENT,
    'Accept-encoding': 'gzip, deflate',
    'Accept-Language': 'en-US,en;q=0.9',
}

REDDIT = "Reddit"
HACKERNEWS = "ycombinator"
HACKERNEWS_TOP_STORIES = "https://hacker-news.firebaseio.com/v0/topstories.json"
HACKERNEWS_NEW_STORIES = "https://hacker-news.firebaseio.com/v0/newstories.json"
HACKERNEWS_ASK_STORIES = "https://hacker-news.firebaseio.com/v0/askstories.json"
HACKERNEWS_SHOW_STORIES = "https://hacker-news.firebaseio.com/v0/showstories.json"
HACKERNEWS_STORIES_URLS = [HACKERNEWS_TOP_STORIES, HACKERNEWS_NEW_STORIES, HACKERNEWS_ASK_STORIES, HACKERNEWS_SHOW_STORIES]

from_timestamp = lambda timestamp: min(now(), datetime.fromtimestamp(timestamp, timezone.utc)) if timestamp else now()
reddit_submission_permalink = lambda permalink: f"https://www.reddit.com{permalink}"
REDDIT_JSON_URL = "https://old.reddit.com/r/{subreddit}.json"
hackernews_story_metadata = lambda id: f"https://hacker-news.firebaseio.com/v0/item/{id}.json"
hackernews_story_permalink = lambda id: f"https://news.ycombinator.com/item?id={id}"

SEC_PRESS = "sec_press"
SEC_STATEMENTS = "sec_statements"
SEC_ENFORCEMENT = "sec_enforcement"


def _return_collected(source, collected: list | None):
    if collected:
        log.debug(event="collected", source=source, num_items=len(collected))
    else:
        log.debug(event="collection failed", source=source, num_items=1)
    return collected


def _transient_fetch_error(e: BaseException) -> bool:
    if isinstance(e, aiohttp.ClientResponseError):
        return e.status >= 500
    return isinstance(e, (aiohttp.ClientPayloadError, aiohttp.ConnectionTimeoutError, TimeoutError))


async def _fetch_json(session: aiohttp.ClientSession, url: str, headers: dict = None) -> dict | None:
    try:
        async with session.get(url, headers=headers or _JSON_HEADERS) as resp:
            return await resp.json()
    except Exception as e:
        log.warning(
            event=f"fetch failed - {e.__class__.__name__}: {e}",
            source=url,
            num_items=1,
            error_type=e.__class__.__name__,
            error_details=str(e),
        )


async def _fetch_feed(session: aiohttp.ClientSession, url: str) -> feedparser.FeedParserDict | None:
    try:
        async with session.get(url, headers=_RSS_REQUEST_HEADERS) as resp:
            text = await resp.text()
    except aiohttp.ClientConnectorCertificateError:
        if not (www_url := with_www(url)):
            raise
        async with session.get(www_url, headers=_RSS_REQUEST_HEADERS) as resp:
            text = await resp.text()
    feed = feedparser.parse(text)
    return feed if feed.entries else None


def _get_site_url(*urls):
    for url in urls:
        if url and isinstance(url, str) and url.startswith('http'):
            return url


def _extract_link(entry, feed, feed_url, site_url):
    if 'link' in entry:
        return full_url(site_url, entry.link)
    if 'links' in entry and entry.links:
        return full_url(site_url, entry.links[0]['href'])
    raise ValueError(f"Invalid rss feed entry without link {entry}")


def _extract_rss_entries(feed, feed_url: str, site_url: str) -> list[tuple[feedparser.FeedParserDict, str]]:
    """Extract and filter RSS entries; return list of (entry, resolved_link)."""
    results = []
    for entry in feed.entries:
        try:
            entry_link = _extract_link(entry, feed, feed_url, site_url)
        except ValueError:
            continue
        if excluded_url(entry_link):
            continue
        results.append((entry, entry_link))
    return results


def _extract_body(entry: feedparser.FeedParserDict) -> tuple[str, str]:
    summary, content = None, None
    if 'dc_content' in entry:
        content = entry.dc_content
    elif 'content' in entry:
        content = entry.content[0]['value'] if isinstance(entry.content, list) else entry.content
    if 'summary' in entry:
        summary = entry.summary
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


def _build_rss_item(feed, feed_url: str, site_url: str, entry: feedparser.FeedParserDict, default_kind: str, entry_link: str | None = None):
    current_time = now()
    published_time = entry.get("published_parsed") or entry.get("updated_parsed")
    created_time = from_timestamp(time.mktime(published_time)) if published_time else current_time
    summary, content = _extract_body(entry)
    tags = _extract_tags(entry)
    author_email = _extract_author_email(entry)
    language = _extract_language(entry, feed)
    if entry_link is None:
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
        AUTHOR: strip_html_tags(entry.get('author')),
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

    item[KIND] = guess_content_type(item, feed_url) or default_kind

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

def _parse_reddit_rss_entry(entry) -> tuple[str | None, str | None, str | None]:
    raw = entry.content[0]['value'] if isinstance(entry.content, list) else entry.get('content', '')
    selftext = None
    external_url, comments_url = None, None
    try:
        doc = lxml.html.fromstring(raw)
        md = doc.xpath('//div[@class="md"]')
        if md:
            selftext = strip_html_tags(lxml.html.tostring(md[0], encoding='unicode'))
        for a in doc.xpath('//a'):
            href, text = a.get('href', ''), (a.text_content() or '').strip()
            if text == '[link]' and not external_url and 'reddit.com' not in href:
                external_url = href
            elif text == '[comments]' and not comments_url:
                comments_url = href
    except Exception:
        pass
    return external_url, comments_url, selftext


def _build_reddit_rss_item(entry, subreddit_name, default_kind: str, entry_link: str | None = None):
    subreddit = f"r/{subreddit_name}"
    current_time = now()
    published = entry.get("published_parsed") or entry.get("updated_parsed")
    created = from_timestamp(time.mktime(published)) if published else current_time

    external_url, comments_url, selftext = _parse_reddit_rss_entry(entry)
    if entry_link is None:
        entry_link = getattr(entry, 'link', '') or entry.get('id', '')
    author = (entry.get('author', '') or '').lstrip('/u/')

    if external_url:
        url = remove_query_params(external_url)
        source = extract_source(url)
        kind = guess_content_type({URL: url, SOURCE: source, TITLE: entry.get('title')}) or default_kind
    else:
        url = entry_link
        source = subreddit
        kind = POST

    return cleanup_item({
        URL: url, KIND: kind, TITLE: entry.get('title'), CONTENT: selftext,
        AUTHOR: author, SOURCE: source, BASE_URL: extract_base_url(url),
        CREATED: created, COLLECTED: current_time,
    })


class _RedditPost:
    __slots__ = ('created_utc', 'permalink', 'is_self', 'url', 'title',
                 'selftext', 'author', 'score', 'num_comments')

    def __init__(self, d: dict):
        self.created_utc = d.get('created_utc')
        self.permalink = d.get('permalink')
        self.is_self = d.get('is_self', False)
        self.url = d.get('url', '')
        self.title = d.get('title', '')
        self.selftext = d.get('selftext', '') or ''
        self.score = d.get('score', 0)
        self.num_comments = d.get('num_comments', 0)
        author = d.get('author') or d.get('author_fullname')
        self.author = type('A', (), {'name': author})()


def _build_reddit_json_item(post_data: dict, subreddit_name: str, default_kind: str):
    return _build_reddit_item(_RedditPost(post_data), subreddit_name, default_kind)


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
            url = remove_query_params(post.url)
            kind = guess_content_type({
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
        url = remove_query_params(story['url'])
        source = extract_source(url)
        tags = []
        kind = guess_content_type({'URL': url, 'SOURCE': source}) or (SITE if "show hn" in story.get('title', '').lower() else default_kind)
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


class APICollectorBase:
    session: aiohttp.ClientSession = None

    def __init__(self, batch_size: int):
        self.batch_size = batch_size

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.batch_size, limit_per_host=self.batch_size or 1),
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            self.session = None

class RSSFeedCollector(APICollectorBase):
    _STATEMENT_URLS = {
        "https://www.sec.gov/news/statements.rss",
        "https://www.sec.gov/news/speeches-statements.rss",
    }

    def __init__(self, batch_size: int):
        super().__init__(batch_size)

    async def collect(self, url: str) -> list[dict]:
        if excluded_url(url):
            return None
        feed = await _fetch_feed(self.session, url)
        if not feed:
            return None
        source_url = _get_site_url(feed.feed.get('link'), url, feed.entries[0].get('link'))
        if url in self._STATEMENT_URLS: items = self._extract_sec_statements_rss_entries(feed, url, source_url)
        else: items = self._extract_default_rss_entries(feed, url, source_url, NEWS)
            
        return _return_collected(extract_source(source_url), items)

    @staticmethod
    def _extract_sec_statements_rss_entries(feed, feed_url: str, site_url: str) -> list[dict]:
        items = []
        for entry, entry_link in _extract_rss_entries(feed, feed_url, site_url):
            item = _build_rss_item(feed, feed_url, site_url, entry, BLOG, entry_link=entry_link)
            item[AUTHOR] = strip_html_tags(entry.get('description', ''))
            items.append(cleanup_item(item))
        return items

    @staticmethod
    def _extract_default_rss_entries(feed, feed_url: str, site_url: str, default_kind: str) -> list[dict]:
        items = []
        for entry, entry_link in _extract_rss_entries(feed, feed_url, site_url):
            items.append(_build_rss_item(feed, feed_url, site_url, entry, default_kind, entry_link=entry_link))
        return items


class RedditCollector(APICollectorBase):
    def __init__(self, batch_size: int):
        super().__init__(batch_size)
        self._reddit_client = None

    def _reddit_client_instance(self):
        if not self._reddit_client:
            self._reddit_client = asyncpraw.Reddit(
                check_for_updates=True,
                client_id=os.getenv("REDDIT_CLIENT_ID"),
                client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
                user_agent=USER_AGENT + " (by u/IntelligentLeave680)",
                timeout=TIMEOUT,
                rate_limit_seconds=RATELIMIT_WAIT,
                requestor_kwargs={"session": self.session},
            )
        return self._reddit_client

    async def collect(self, subreddit_name: str, mode: str = "json", default_kind: str = NEWS, limit: int = 25) -> list[dict]:
        if mode == "json":
            return await self._collect_json(subreddit_name, default_kind, limit)
        elif mode == "rss":
            return await self._collect_rss(subreddit_name, default_kind)
        else:
            return await self._collect_api(subreddit_name, default_kind)

    async def _collect_api(self, subreddit_name: str, default_kind: str = NEWS) -> list[dict]:
        @retry(
            stop=stop_after_attempt(RETRY_COUNT),
            wait=wait_fixed(RATELIMIT_WAIT),
            reraise=True,
        )
        async def _collect():
            sr = await self._reddit_client_instance().subreddit(subreddit_name)
            return [_build_reddit_item(post, subreddit_name, default_kind) async for post in sr.hot(limit=25) if not excluded_url(post.url)]

        return _return_collected(subreddit_name, await _collect())

    async def _collect_json(self, subreddit_name: str, default_kind: str = NEWS, limit: int = 25) -> list[dict]:
        url = REDDIT_JSON_URL.format(subreddit=subreddit_name)
        data = await _fetch_json(self.session, f"{url}?limit={limit}", headers=_JSON_HEADERS | {'Cookie': os.getenv("REDDIT_SESSION_COOKIE", "")})
        if not data:
            return _return_collected(subreddit_name, None)
        children = data.get('data', {}).get('children', [])
        items = [_build_reddit_json_item(c['data'], subreddit_name, default_kind)
                 for c in children
                 if c.get('kind') == 't3' and not excluded_url(c.get('data', {}).get('url'))]
        return _return_collected(subreddit_name, items)

    async def _collect_rss(self, subreddit_name: str, default_kind: str = NEWS) -> list[dict]:
        url = f"https://old.reddit.com/r/{subreddit_name}/.rss"
        feed = await _fetch_feed(self.session, url)
        if not feed:
            return _return_collected(subreddit_name, None)
        site_url = _get_site_url(feed.feed.get('link'), url)
        return _return_collected(subreddit_name,
            self._extract_reddit_rss_entries(feed, url, subreddit_name, default_kind, site_url))

    @staticmethod
    def _extract_reddit_rss_entries(feed, feed_url: str, subreddit_name: str, default_kind: str, site_url: str) -> list[dict]:
        items = []
        for entry, entry_link in _extract_rss_entries(feed, feed_url, site_url):
            items.append(_build_reddit_rss_item(entry, subreddit_name, default_kind, entry_link=entry_link))
        return items



class HackerNewsCollector(APICollectorBase):
    def __init__(self, batch_size: int):
        super().__init__(batch_size)

    async def collect(self, stories_urls: list[str] = HACKERNEWS_STORIES_URLS) -> list[dict]:
        if isinstance(stories_urls, str):
            stories_urls = [stories_urls]
        ids = await asyncio.gather(*[_fetch_json(self.session, ids_url) for ids_url in stories_urls])
        ids = set(chain(*ids))
        stories = await asyncio.gather(*[_fetch_json(self.session, hackernews_story_metadata(id)) for id in ids])
        stories = [_build_hackernews_item(story, BLOG) for story in stories if story and not excluded_url(story.get('url'))]
        return _return_collected(HACKERNEWS, stories)


class SECFilingCollector(APICollectorBase):
    def __init__(self, batch_size: int):
        super().__init__(batch_size)
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = False
        self.html_converter.body_width = 0

    async def _download_zip(self, url: str) -> bytes:
        async with self.session.get(url) as resp:
            return await resp.read()

    def _extract_html_from_zip(self, zip_data: bytes) -> list[tuple[str, str]]:
        html_files = []
        with zipfile.ZipFile(BytesIO(zip_data)) as zf:
            for file_info in zf.infolist():
                if file_info.is_dir():
                    continue
                filename = Path(file_info.filename).name.lower()
                if filename.endswith(('.html', '.htm')):
                    content = zf.read(file_info.filename).decode('utf-8', errors='ignore')
                    html_files.append((file_info.filename, content))
        return html_files

    def _html_to_markdown(self, html_content: str) -> str:
        return self.html_converter.handle(html_content)

    @staticmethod
    def _extract_filing_type(title: str) -> str:
        match = re.search(r'(10-[KQ]|8-[KA]|DEF\s*14A|S\s*\d+)', title, re.IGNORECASE)
        return match.group(0) if match else 'unknown'

    @staticmethod
    def _extract_accession_number(url: str) -> str | None:
        match = re.search(r'(\d{10}-\d{2}-\d{6})', url)
        return match.group(1) if match else None

    def _build_filing_item(self, entry, feed, feed_url: str, site_url: str, content: str, filing_type: str, accession_number: str) -> dict:
        entry_link = getattr(entry, 'link', None) or entry.get('link')
        if not entry_link:
            return None

        item = _build_rss_item(feed, feed_url, site_url, entry, SEC_FILING)
        item[CONTENT] = content
        item[KIND] = SEC_FILING

        tags = ['sec', 'edgar']
        if filing_type:
            tags.append(filing_type.lower())
        if accession_number:
            tags.append(accession_number)
        item[TAGS] = tags

        return cleanup_item(item)

    async def collect(self, url: str) -> list[dict]:
        if excluded_url(url):
            return None

        feed = await _fetch_feed(self.session, url)
        if not feed:
            return None

        source_url = _get_site_url(feed.feed.get('link'), url, feed.entries[0].get('link'))

        async def _process_entry(entry):
            zip_data = None
            try:
                guid = getattr(entry, 'guid', None) or entry.get('guid')
                if not guid:
                    return None

                zip_data = await self._download_zip(guid)
                if not zip_data:
                    return None

                html_files = self._extract_html_from_zip(zip_data)
                if not html_files:
                    return None

                primary_filename, primary_html = html_files[0]
                markdown_content = self._html_to_markdown(primary_html)

                return self._build_filing_item(
                    entry=entry,
                    feed=feed,
                    feed_url=url,
                    site_url=source_url,
                    content=markdown_content,
                    filing_type=self._extract_filing_type(entry.get('title', '')),
                    accession_number=self._extract_accession_number(guid)
                )
            except Exception as e:
                log.warning(
                    event="edgar_processing_failed",
                    error=str(e),
                    guid=getattr(entry, 'guid', None),
                )
                return None
            finally:
                del zip_data

        tasks = [_process_entry(entry) for entry in feed.entries]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        items = [r for r in results if r and not isinstance(r, Exception)]

        return _return_collected('sec_edgar', items)
