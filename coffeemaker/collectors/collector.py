import asyncio
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from itertools import chain
import json
import logging
import os
from typing import Callable
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone
import time
import aiohttp
import asyncpraw
from bs4 import BeautifulSoup
import feedparser
from crawl4ai import AsyncWebCrawler, Crawl4aiDockerClient, BrowserConfig, CrawlResult, CrawlerRunConfig, CacheMode, JsonCssExtractionStrategy, DefaultMarkdownGenerator
import praw
import prawcore
import requests
from retry import retry
import tldextract
from dateutil.parser import parse as date_parser
import re
import yaml
from coffeemaker.collectors import USER_AGENT, TIMEOUT, RATELIMIT_WAIT
from coffeemaker.pybeansack.models import *
from icecream import ic

log = logging.getLogger(__name__)

REDDIT = "Reddit"
HACKERNEWS = "ycombinator"
HACKERNEWS_TOP_STORIES = "https://hacker-news.firebaseio.com/v0/topstories.json"
HACKERNEWS_NEW_STORIES = "https://hacker-news.firebaseio.com/v0/newstories.json"
# HACKERNEWS_JOB_STORIES = "https://hacker-news.firebaseio.com/v0/jobstories.json"
HACKERNEWS_ASK_STORIES = "https://hacker-news.firebaseio.com/v0/askstories.json"
HACKERNEWS_SHOW_STORIES = "https://hacker-news.firebaseio.com/v0/showstories.json"
HACKERNEWS_STORIES_URLS = [HACKERNEWS_TOP_STORIES, HACKERNEWS_NEW_STORIES, HACKERNEWS_ASK_STORIES, HACKERNEWS_SHOW_STORIES]

RSS_REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    'Accept-encoding': 'gzip, deflate',
    'A-IM': 'feed',
    'Accept': "application/atom+xml,application/rdf+xml,application/rss+xml,application/x-netcdf,application/xml;q=0.9,text/xml;q=0.2,*/*;q=0.1"
}
JSON_REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    'Accept-encoding': 'gzip, deflate',
    'Accept': "application/json,text/json"
}
HTML_REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    'Accept-encoding': 'gzip, deflate',
    'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.5,application/signed-exchange;v=b3;q=0.9"
}
MD_OPTIONS = {
    "ignore_images": True,
    "escape_html": False,
    # "skip_external_links": True,
    # "skip_internal_links": True, 
}
EXCLUDED_URL_PATTERNS = [
    r'\.(png|jpeg|jpg|gif|webp|mp4|avi|mkv|mp3|wav|pdf)$',
    r'(v\.redd\.it|i\.redd\.it|www\.reddit\.com\/gallery|youtube\.com|youtu\.be)',
    r'\/video(s)?\/',
    r'\/image(s)?\/',
] 
POST_DOMAINS = ["reddit", "redd", "linkedin", "x", "twitter", "facebook", "ycombinator"]
BLOG_URLS = ["medium.com",  "substack.", "wordpress.", "blogspot.", "newsletter.", "developers.", "blogs.", "blog.", ".so/", ".dev/", ".io/",  ".to/", ".rs/", ".tech/", ".ai/", ".blog/", "/blog/" ]
BLOG_SITENAMES = ["blog", "magazine", "newsletter", "weekly"]
NEWS_SITENAMES = ["daily", "wire", "times", "today",  "news", "the "]

# assigning 16 io threads per cpu
BATCH_SIZE = 16*os.cpu_count()

# general utilities
now = lambda: datetime.now(timezone.utc)
from_timestamp = lambda timestamp: min(now(), datetime.fromtimestamp(timestamp, timezone.utc)) if timestamp else now()
reddit_submission_permalink = lambda permalink: f"https://www.reddit.com{permalink}"
hackernews_story_metadata = lambda id: f"https://hacker-news.firebaseio.com/v0/item/{id}.json"
hackernews_story_permalink = lambda id: f"https://news.ycombinator.com/item?id={id}"

extract_source = lambda url: (extract_domain(url) or extract_base_url(url)).strip().lower()

def extract_base_url(url: str) -> str:
    try: return urlparse(url).netloc
    except: return None

def extract_domain(url: str) -> str:
    try: return tldextract.extract(url).domain
    except: return None

def parse_date(date: str) -> datetime:
    try: return date_parser(date, timezones=["UTC"])
    except: return None

# general utilities
def _excluded_url(url: str):
    return (not url) or any(re.search(pattern, url) for pattern in EXCLUDED_URL_PATTERNS)

def _strip_html_tags(html):
    if html: return BeautifulSoup(html, "lxml").get_text(separator="\n", strip=True)

### rss feed related utilities ###
def _extract_body(entry: feedparser.FeedParserDict) -> tuple[str, str]:
    # the body usually lives in <dc:content>, <content:encoded> or <description>
    summary, content = None, None
    if 'dc_content' in entry:
        content = entry.dc_content
    elif 'content' in entry:        
        content = entry.content[0]['value'] if isinstance(entry.content, list) else entry.content    
    
    if 'summary' in entry:
        summary = entry.summary
        
    return summary, (content or summary)

def _extract_main_image(entry: feedparser.FeedParserDict) -> str:
    if ('links' in entry) and any(item for item in entry.links if "image" in item.get('type', "")):
        return next(item for item in entry.links if "image" in item.get('type', "")).get('href')
    if ('media_content' in entry) and entry.media_content:
        return entry.media_content[0].get('url')
    elif ('media_thumbnail' in entry) and entry.media_thumbnail:
        return entry.media_thumbnail[0].get('url')

def _clean_markdown(markdown: str) -> str:
    """Remove any content before the first line starting with '# '."""
    if not markdown: return
    markdown = markdown.strip()
    lines = markdown.splitlines()
    # TODO: add a check to remove "advertisement"
    for i, line in enumerate(lines):
        if line.startswith("# "):
            return "\n".join(lines[i+1:])
    return markdown

async def _fetch_json_async(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, headers=JSON_REQUEST_HEADERS, timeout=TIMEOUT) as response:
            response.raise_for_status()
            body = await response.json()
        return body
    except Exception as e: 
        log.warning("collection failed", extra={"source": url, "num_items": 1})    
        log.error(e, extra={'source': url, "num_items": 1})

def _fetch_json(url: str):
    # @retry(tries=2, delay=10, jitter=(5, 10))
    # def _fetch():
    #     resp = requests.get(content_url, headers=JSON_REQUEST_HEADERS, timeout=TIMEOUT)
    #     resp.raise_for_status()
    #     return resp.json()
    try: 
        resp = requests.get(url, headers=JSON_REQUEST_HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e: 
        log.warning("collection failed", extra={"source": url, "num_items": 1})
        log.error(e, extra={'source': url, "num_items": 1})

merge_lists = lambda results: list(chain(*(r for r in results if r))) 

def _batch_run(func: Callable, sources: list):
    results = None
    with ThreadPoolExecutor(max_workers=BATCH_SIZE, thread_name_prefix="collector") as executor:
        results = list(executor.map(func, sources))
    return results

def _guess_type(url: str, source: str) -> str:
    """This is entirely heuristic to figure out if the url contains a news or a blog.
    This is the dumbest shit I ever wrote but it gets the job done for now."""

    domain_name = extract_domain(url).lower()
    if any(True for post_domain in POST_DOMAINS if domain_name == post_domain): return POST

    stripped_url = url.lower().split("?")[0]
    if any(True for blog_url in BLOG_URLS if blog_url in stripped_url): return BLOG

    source = source.lower()
    if any(sitename for sitename in BLOG_SITENAMES if sitename in source): return BLOG
    if any(sitename for sitename in NEWS_SITENAMES if sitename in source): return NEWS

    if "/news/" in url: return NEWS

def parse_sources(sources: str) -> dict:
    if os.path.exists(sources):
        with open(sources, 'r') as file:
            data = yaml.safe_load(file)
    else: data = yaml.safe_load(sources)
    return data['sources']

class APICollector:
    md_generator: DefaultMarkdownGenerator = None
    _reddit_client = None
    # _reddit_client_async = None
    collect_callback: Callable = None

    def __init__(self, collect_callback: Callable = None):  
        self.md_generator = DefaultMarkdownGenerator(options=MD_OPTIONS)
        self.collect_callback = collect_callback

    @property
    def reddit_client(self):
        if not self._reddit_client:
            self._reddit_client = praw.Reddit(
                check_for_updates=True,
                client_id=os.getenv("REDDIT_APP_ID"),
                client_secret=os.getenv("REDDIT_APP_SECRET"),
                user_agent=USER_AGENT+" (by u/randomizer_000)",
                username=os.getenv("REDDIT_COLLECTOR_USERNAME"),
                password=os.getenv("REDDIT_COLLECTOR_PASSWORD"),
                timeout=TIMEOUT,
                rate_limit_seconds=RATELIMIT_WAIT,
            )
        return self._reddit_client
    
    async def start_session(self):
        self._reddit_client = asyncpraw.Reddit(
            check_for_updates=True,
            client_id=os.getenv("REDDIT_APP_ID"),
            client_secret=os.getenv("REDDIT_APP_SECRET"),
            user_agent=USER_AGENT+" (by u/randomizer_000)",
            username=os.getenv("REDDIT_COLLECTOR_USERNAME"),
            password=os.getenv("REDDIT_COLLECTOR_PASSWORD"),
            timeout=TIMEOUT,
            rate_limit_seconds=RATELIMIT_WAIT,
        )

    async def close_session(self):
        if self._reddit_client: await self._reddit_client.close()

    def _return_collected(self, source, collected: list|None):
        if collected: log.info("collected", extra={"source": source, "num_items": len(collected)})
        else: log.warning("collection failed", extra={"source": source, "num_items": 1})
        
        if self.collect_callback: self.collect_callback(source, collected)
        else: return collected

    def collected_rssfeeds(self, feed_urls: list[str]) -> list[Bean]|list[tuple[Bean, Chatter]]:
        return merge_lists(_batch_run(self.collect_rssfeed, feed_urls))

    def collect_rssfeed(self, url: str) -> list[Bean]|list[tuple[Bean, Chatter]]:        
        resp = requests.get(url, headers=RSS_REQUEST_HEADERS, timeout=TIMEOUT) 
        resp.raise_for_status()  # Raise exception for bad status codes
        feed = feedparser.parse(BytesIO(resp.content))
                
        if feed.entries: 
            return self._return_collected(
                extract_source(feed.feed.get('link') or feed.entries[0].link) or url, 
                [self._from_rssfeed(entry, NEWS) for entry in feed.entries]
            )
        # try:
        #     resp = requests.get(url, headers=RSS_REQUEST_HEADERS, timeout=TIMEOUT)  # Set timeout to 10 seconds
        #     resp.raise_for_status()  # Raise exception for bad status codes
        #     feed = feedparser.parse(BytesIO(resp.content))
                    
        #     if feed.entries: 
        #         source = extract_source(feed.feed.get('link') or feed.entries[0].link)
        #         collected = [self._from_rssfeed(entry, NEWS) for entry in feed.entries]
        # except Exception as e: log.error(e, extra={'source': url, "num_items": 1})
        # return self._return_collected(source, collected)
 
    ### rss feed related utilities  ###
    async def collect_rssfeed_async(self, url: str, default_kind: str = NEWS) -> list[Bean]|list[tuple[Bean, Chatter]]:
        async with aiohttp.ClientSession(headers=RSS_REQUEST_HEADERS, timeout=aiohttp.ClientTimeout(total=TIMEOUT), raise_for_status=True) as session:
            resp = await session.get(url)
            resp.raise_for_status()
            feed = feedparser.parse(await resp.text())
                
        if feed.entries: 
            return self._return_collected(
                extract_source(feed.feed.get('link') or feed.entries[0].link) or url, 
                [self._from_rssfeed(entry, NEWS) for entry in feed.entries]
            )
        # try:
        #     async with aiohttp.ClientSession(headers=RSS_REQUEST_HEADERS, timeout=aiohttp.ClientTimeout(total=TIMEOUT), raise_for_status=True) as session:
        #         resp = await session.get(url)
        #         # resp.raise_for_status()
        #         feed = feedparser.parse(await resp.text())

        #         if feed.entries:
        #             source = extract_source(feed.feed.get('link') or feed.entries[0].link)
        #             collected = [self._from_rssfeed(entry, default_kind) for entry in feed.entries]
        # except Exception as e: log.error(e, extra={'source': url, "num_items": 1})
        # return self._return_collected(source, collected)

    def _from_rssfeed(self, entry: feedparser.FeedParserDict, default_kind: str) -> tuple[Bean, Chatter]:
        current_time = now()
        published_time = entry.get("published_parsed") or entry.get("updated_parsed")
        created_time = from_timestamp(time.mktime(published_time)) if published_time else current_time
        summary, content = _extract_body(entry)
        source = extract_source(entry.link)

        if 'slash_comments' in entry: chatter = Chatter(
            url=entry.link,
            source=source,
            chatter_url=entry.get('wfw_commentrss', entry.link+"#comments"),
            collected=current_time,
            comments=entry.slash_comments
        ) 
        else: chatter = None
        
        return (
            Bean(
                url=entry.link,
                # in case of rss feed, the created time is the same as the updated time during collection. if it is mentioned in a social media feed then the updated time will get change
                created=created_time,         
                collected=current_time,
                updated=created_time,
                source=source,
                site_base_url=extract_base_url(entry.link),
                title=entry.title,
                kind=_guess_type(entry.link, source) or default_kind,
                summary=self._generate_markdown(summary),
                content=self._generate_markdown(content),
                author=entry.get('author'),        
                image_url=_extract_main_image(entry)
            ),
            chatter
        )

    def _generate_markdown(self, html: str) -> str:
        """Converts the given html into a markdown"""
        if html: return self.md_generator.generate_markdown(input_html=html).raw_markdown.strip()
    
    def collect_subreddits(self, subreddit_names: list[str]):
        return merge_lists(_batch_run(self.collect_subreddit, subreddit_names))
    
    def collect_subreddit(self, subreddit_name, default_kind: str = NEWS):
        @retry(exceptions=(prawcore.exceptions.ResponseException), tries=2, delay=10, jitter=(5, 10))
        def _collect():
            sr = self.reddit_client.subreddit(subreddit_name)
            return [self._from_reddit_post(post, subreddit_name, default_kind) for post in sr.hot(limit=25) if not _excluded_url(post.url)]
        
        return self._return_collected(subreddit_name, _collect()) 
        # try: collected = _collect()
        # except Exception as e: log.error(e, extra={'source': subreddit_name, "num_items": 1})
        # return self._return_collected(subreddit_name, collected)       
    
    ### reddit related utilities ###
    async def collect_subreddit_async(self, subreddit_name: str, default_kind: str = NEWS) -> list[tuple[Bean, Chatter]]:
        @retry(tries=2, delay=10, jitter=(5, 10))
        async def _collect():
            sr = await self.reddit_client.subreddit(subreddit_name)
            return [self._from_reddit_post(post, subreddit_name, default_kind) async for post in sr.hot(limit=25) if not _excluded_url(post.url)]
        
        return self._return_collected(subreddit_name, await _collect()) 

    def _from_reddit_post(self, post, sr_name, default_kind) -> tuple[Bean, Chatter]: 
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
                kind = _guess_type(url, source) or default_kind
            else: # sometimes the links are itself a reddit post
                url = reddit_submission_permalink(post.url)
                kind = POST
                source = subreddit
        
        return (
            Bean(
                url=url,
                created=created_time,
                collected=current_time,
                updated=created_time,
                # this is done because sometimes is_self value is wrong
                source=source,
                site_base_url=extract_base_url(url),
                title=post.title,
                kind=kind,
                content=post.selftext,
                author=post.author.name if post.author else None,
                # fill in the defaults
                shared_in=[chatter_link],
                likes=post.score,
                comments=post.num_comments
            ),
            Chatter(
                url=url,
                chatter_url=chatter_link,            
                source=REDDIT,                        
                channel=subreddit,
                collected=current_time,
                likes=post.score,
                comments=post.num_comments
            )
        )
    
    ### hackernews related utilities ###
    async def collect_ychackernews_async(self, stories_urls = HACKERNEWS_STORIES_URLS) -> list[tuple[Bean, Chatter]]:
        if isinstance(stories_urls, str): stories_urls = [stories_urls]
        async with aiohttp.ClientSession() as session:
            ids = await asyncio.gather(*[_fetch_json_async(session, ids_url) for ids_url in stories_urls])
            ids = set(chain(*ids))
            stories = await asyncio.gather(*[_fetch_json_async(session, hackernews_story_metadata(id)) for id in ids])
        
        return self._return_collected(
            HACKERNEWS,
            _batch_run(
                lambda story: self._from_hackernews_story(story, BLOG), 
                [story for story in stories if story and not _excluded_url(story.get('url'))]
            )
        )

    def collect_ychackernews(self, stories_urls = HACKERNEWS_STORIES_URLS) -> list[tuple[Bean, Chatter]]:
        if isinstance(stories_urls, str): stories_urls = [stories_urls]
        ids = _batch_run(_fetch_json, stories_urls)
        ids = set(chain(*ids))
        stories = _batch_run(_fetch_json, [hackernews_story_metadata(id) for id in ids])

        return self._return_collected(
            HACKERNEWS,
            _batch_run(
                lambda story: self._from_hackernews_story(story, BLOG), 
                [story for story in stories if story and not _excluded_url(story.get('url'))]
            )
        )
    
    def _from_hackernews_story(self, story: dict, default_kind: str) -> tuple[Bean, Chatter]:
        # either its a shared url or it is a text
        current_time = now()
        created_time = from_timestamp(story['time'])

        id = story['id']
        if story.get('url'):
            url = story['url']
            source = extract_source(url)
            kind = _guess_type(url, source) or default_kind
        else:
            url = hackernews_story_permalink(id)
            source = HACKERNEWS           
            kind = POST
                    
        return (
            Bean(            
                url=url, # this is either a linked url or a direct post
                # initially the bean's updated time will be the same as the created time
                # if there is a chatter that links to this, then the updated time will be changed to collection time of the chatter
                created=created_time,                
                collected=current_time,
                updated=created_time,
                source=source,
                site_base_url=extract_base_url(url),
                title=story.get('title'),
                kind=kind, # blog, post or job
                content=self._generate_markdown(story['text']) if 'text' in story else None, # load if it has a text which usually applies to posts
                author=story.get('by'),
                # fill in the defaults
                shared_in=[hackernews_story_permalink(id)],
                likes=story.get('score'),
                comments=len(story.get('kids', []))
            ), 
            Chatter(
                url=url,
                chatter_url=hackernews_story_permalink(id),
                collected=current_time,
                source=HACKERNEWS,
                channel=str(id),
                likes=story.get('score'),
                comments=len(story.get('kids', []))
            )
        )
    
# GENERIC URL COLLECTOR CONFIG
BASE_EXCLUDED_TAGS = ["script", "style", "nav", "footer", "navbar", "comment", "contact",
"img", "audio", "video", "source", "track", "iframe", "object", "embed", "param", "picture", "figure",
"svg", "canvas", "aside", "form", "input", "button", "textarea", "select", "option", "optgroup", "ins"]
MD_SELECTOR = ", ".join([
    "article", 
    "main", 
    ".article-body", 
    ".article", 
    ".article-content", 
    ".main-article", 
    ".content"
    "[class~='main']"
])
MD_EXCLUDED_SELECTOR = ", ".join([
    ".ads-banner", 
    ".adsbygoogle", 
    ".advertisement", 
    ".advertisement-holder", 
    ".post-bottom-ad",
    "[class~='sponsor']", 
    "[id~='sponsor']", 
    "[class~='advertorial']", 
    "[id~='advertorial']", 
    "[class~='marketing-page']",
    "[id~='marketing-page']",
    ".article-sharing", 
    ".link-embed", 
    ".article-footer", 
    ".related-stories", 
    ".related-posts", 
    "[id~='related-article']", 
    "#related-articles", 
    ".comments", 
    "#comments", 
    ".comments-section", 
    ".md-sidebar", 
    ".sidebar", 
    ".image-holder", 
    ".category-label", 
    ".InlineImage-imageEmbedCredit", 
    ".metadata", 
])
MD_EXCLUDED_TAGS = BASE_EXCLUDED_TAGS + ["link", "meta"] 
METADATA_EXTRACTION_SCHEMA = {
    "name": "Site Metadata",
    "baseSelector": "html",
    "fields": [
        # all body selectors
        {"name": "title", "type": "text", "selector": "h1, title"},
        # all meta selectors
        {"name": "description", "type": "attribute", "selector": "meta[name='description']", "attribute": "content"},
        {"name": "meta_title", "type": "attribute", "selector": "meta[property='og:title'], meta[name='og:title']", "attribute": "content"},
        {"name": "published_time", "type": "attribute", "selector": "meta[property='rnews:datePublished'], meta[property='article:published_time'], meta[name='OriginalPublicationDate'], meta[itemprop='datePublished'], meta[property='og:published_time'], meta[name='article_date_original'], meta[name='publication_date'], meta[name='sailthru.date'], meta[name='PublishDate'], meta[property='pubdate']", "attribute": "content"},
        {"name": "top_image", "type": "attribute", "selector": "meta[property='og:image'], meta[property='og:image:url'], meta[name='og:image:url'], meta[name='og:image']", "attribute": "content"},
        {"name": "kind", "type": "attribute", "selector": "meta[property='og:type']", "attribute": "content"},
        {"name": "author", "type": "attribute", "selector": "meta[name='author'], meta[name='dc.creator'], meta[name='byl'], meta[name='byline']", "attribute": "content"},
        {"name": "site_name", "type": "attribute", "selector": "meta[name='og:site_name'], meta[property='og:site_name']", "attribute": "content"},
        # all link selectors
        {"name": "favicon", "type": "attribute", "selector": "link[rel='shortcut icon'][type='image/png'], link[rel='icon']", "attribute": "href"},
        {"name": "rss_feed", "type": "attribute", "selector": "link[type='application/rss+xml']", "attribute": "href"},
    ]
}

class WebScraper:    
    browser_config = None
    batch_size = None
    remote_crawler = None
    crawling_semaphore = None

    def __init__(self, remote_crawler: str = None, batch_size: int = BATCH_SIZE):
        self.browser_config = BrowserConfig(
            headless=True,
            ignore_https_errors=False,
            java_script_enabled=False,
            user_agent=USER_AGENT,
            light_mode=True,
            text_mode=True,
            verbose=False
        )
        self.batch_size = batch_size
        self.remote_crawler = remote_crawler
        self.crawling_semaphore = asyncio.Semaphore(batch_size)

    def _config(self, collect_metadata: bool):
        if collect_metadata: return CrawlerRunConfig(   
            # content processing
            word_count_threshold=100,
            markdown_generator=DefaultMarkdownGenerator(options=MD_OPTIONS),
            extraction_strategy=JsonCssExtractionStrategy(schema=METADATA_EXTRACTION_SCHEMA),
            excluded_tags=BASE_EXCLUDED_TAGS,
            excluded_selector=MD_EXCLUDED_SELECTOR,
            keep_data_attributes=False,
            remove_forms=True,

            # caching and session
            cache_mode=CacheMode.BYPASS,

            # navigation & timing
            semaphore_count=self.batch_size,
            page_timeout=TIMEOUT*1000,
            wait_for_images=False, 

            # page interaction
            scan_full_page=False,
            process_iframes=False,
            remove_overlay_elements=True,
            simulate_user=False,
            override_navigator=False,

            # media handling
            screenshot=False,
            pdf=False,
            exclude_external_images=True,
            # exclude_external_links=True,
            exclude_social_media_links=True, 

            verbose=False,
            stream=False
        )
        else: return CrawlerRunConfig(   
            # content processing
            word_count_threshold=100,
            markdown_generator=DefaultMarkdownGenerator(options=MD_OPTIONS),
            css_selector=MD_SELECTOR, # this is only for markdown generation
            excluded_tags=MD_EXCLUDED_TAGS,
            excluded_selector=MD_EXCLUDED_SELECTOR,
            keep_data_attributes=False,
            remove_forms=True,

            # caching and session
            cache_mode=CacheMode.BYPASS,

            # navigation & timing
            semaphore_count=self.batch_size,
            page_timeout=TIMEOUT*1000,
            wait_for_images=False,

            # page interaction
            scan_full_page=False,
            process_iframes=False,
            remove_overlay_elements=True,
            simulate_user=False,
            override_navigator=False,

            # media handling
            screenshot=False,
            pdf=False,
            exclude_external_images=True,
            # exclude_external_links=True,
            exclude_social_media_links=True, 

            verbose=False,
            stream=False
        )

    async def _scrape_with_remote_crawler(self, urls, run_config):
        async with Crawl4aiDockerClient(self.remote_crawler, timeout=TIMEOUT*100, verbose=False) as crawler:
            crawler._token = "AND THIS IS HOW YOU BYPASS AUTHENTICATION. BRO WTF!"
            async def process_batch(batch):
                async with self.crawling_semaphore:
                    return await crawler.crawl(
                        urls=batch, 
                        browser_config=self.browser_config, 
                        crawler_config=run_config
                    )
            batches = [urls[i:i+self.batch_size] for i in range(0, len(urls), self.batch_size)]
            batch_responses = await asyncio.gather(*(process_batch(batch) for batch in batches))
        return list(chain(*([resp] if isinstance(resp, CrawlResult) else resp for resp in batch_responses)))

    async def _scrape_with_local_crawler(self, urls, run_config):
        async with self.crawling_semaphore:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                results = await crawler.arun_many(urls=urls, config=run_config)
        return results

    async def _scrape(self, urls: list[str], collect_metadata: bool):
        run_config = self._config(collect_metadata)
        if self.remote_crawler: task = self._scrape_with_remote_crawler(urls, run_config)
        else: task = self._scrape_with_local_crawler(urls, run_config)
        return {result.url: result for result in (await task)}

    async def scrape_url(self, url: str, collect_metadata: bool) -> dict:
        """Collects the body of the url as a markdown"""
        if _excluded_url(url): return
        results = await self._scrape([url], collect_metadata)
        return WebScraper._package_result(results[url])

    async def scrape_urls(self, urls: list[str], collect_metadata: bool) -> list[dict]:
        """Collects the bodies of the urls as markdowns"""
        results = await self._scrape(urls, collect_metadata)
        return [WebScraper._package_result(results[url]) for url in urls]    

    async def scrape_beans(self, beans: list[Bean], collect_metadata: bool = False) -> list[Bean]:
        """Collects the bodies of the beans as markdowns"""
        results = await self.scrape_urls([bean.url for bean in beans], collect_metadata)
        current_time = now()
        for bean, result in zip(beans, results):
            if not result: continue
            bean.content = result.get("markdown")
            bean.title = result.get("meta_title") or bean.title or result.get("title") # this sequence is important because result['title'] is often crap
            bean.image_url = result.get("top_image") or bean.image_url
            bean.author = result.get("author") or bean.author
            bean.created = min(result.get("published_time") or bean.created or bean.collected, current_time)
            bean.summary = result.get("description") or bean.summary
            bean.site_rss_feed = result.get("rss_feed")
            bean.site_name = result.get('site_name')
            bean.site_favicon = result.get('favicon')
            bean.is_scraped = True
        return beans

    def _package_result(result) -> dict:   
        if not(result and result.success): return

        ret = {
            "url": result.url,
            "markdown": _clean_markdown(result.markdown)
        }
        if content := (json.loads(result.extracted_content) if result.extracted_content else None):
            metadata = content[0]
            if 'published_time' in metadata:
                metadata['published_time'] = parse_date(metadata['published_time'])
            if 'top_image' in metadata and not extract_base_url(metadata['top_image']):
                metadata['top_image'] = urljoin(extract_base_url(result.url), metadata['top_image'])
            if 'favicon' in metadata and not extract_base_url(metadata['favicon']):
                metadata['favicon'] = urljoin(extract_base_url(result.url), metadata['favicon'])
            ret.update(metadata)
        return ret

    # async def _collect_url(self, url: str, config: CrawlerRunConfig) -> dict:
    #     if _excluded_url(url): return
    #     result = await self.web_crawler.arun(url=url, config=config)
    #     return AsyncCollector._package_result(result)
    
    # async def collect_urls(self, urls: list[str], collect_metadata: bool = False) -> list[dict]:
    #     """Collects the bodies of the urls as markdowns"""
    #     # NOTE: serializing this cause otherwise shit dies
    #     config = AsyncCollector._run_config(collect_metadata)
    #     async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session, AsyncWebCrawler(config=AsyncCollector._browser_config) as crawler:
    #         async def _collect(url: str):
    #             try:
    #                 if _excluded_url(url): return

    #                 body = await session.get(url, headers=HTML_REQUEST_HEADERS)
    #                 body.raise_for_status()
    #                 result = await crawler.arun(url="raw:"+(await body.text()), config=config)
    #                 return AsyncCollector._package_result(result)
    #             except Exception as e:
    #                 ic(e.__class__.__name__, e)            
    #         results = await asyncio.gather(*[_collect(url) for url in urls])

    #     return results             

    # async def collect_urls(self, urls: list[str], collect_metadata: bool = False) -> list[dict]:
    #     """Collects the bodies of the urls as markdowns"""        
    #     async with AsyncWebCrawler(config=BROWSER_CONFIG) as crawler:
    #         config = AsyncCollector._run_config(collect_metadata)
    #         async def _collect(url: str):
    #             if _excluded_url(url): return
    #             result = await crawler.arun(url=url, config=config)
    #             return AsyncCollector._package_result(result)
    #         results = await asyncio.gather(*[_collect(url) for url in urls])
    #     return results

    # async def _collect_url(self, url: str, session: aiohttp.ClientSession, config: CrawlerRunConfig) -> dict:
    #     if _excluded_url(url): return
    #     try:
    #         resp = await session.get(url, headers=HTML_REQUEST_HEADERS, timeout=TIMEOUT)            
    #         resp.raise_for_status()
    #         result = await self.web_crawler.arun(url="raw:"+await resp.text(), config=config)
    #         if isinstance(result, CrawlResult): return AsyncCollector._package_result(result)
    #     except Exception as e:
    #         ic(e.__class__.__name__, e)

    # async def collect_urls(self, urls: list[str], collect_metadata: bool = False) -> list[dict]:
    #     config = AsyncCollector._run_config(collect_metadata)
    #     try:
    #         return await asyncio.gather(*[self._collect_url(url, config) for url in urls])
    #     except Exception as e:
    #         ic(e.__class__.__name__, e)

    # async def collect_urls(self, urls: list[str], collect_metadata: bool = False) -> list[dict]:
    #     """Collects the bodies of the urls as markdowns"""
    #     config = AsyncCollector._run_config(collect_metadata)
    #     results = await self.web_crawler.arun_many(urls=urls, config=config)
    #     return [AsyncCollector._package_result(result) for result in results]

    # async def collect_urls(self, urls: list[str], collect_metadata: bool = False) -> list[dict]:
    #     """Collects the bodies of the urls as markdowns"""
    #     # NOTE: serializing this cause otherwise shit dies
    #     config = AsyncCollector._run_config(collect_metadata)
    #     bodies = []
    #     async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
    #         for url in urls:
    #             body = None
    #             try:
    #                 response = await session.get(url, headers=HTML_REQUEST_HEADERS)
    #                 response.raise_for_status()
    #                 body = AsyncCollector._package_result(await self.web_crawler.arun(url="raw:"+(await response.text()), config=config))
    #             except Exception as e:
    #                 ic(e.__class__.__name__, e)
    #             bodies.append(body)
    #     return bodies

    # async def collect_urls(self, urls: list[str], collect_metadata: bool = False) -> list[dict]:
    #     """Collects the bodies of the urls as markdowns"""
    #     config = AsyncCollector._run_config(collect_metadata)
    #     results = await self.web_crawler.arun_many(urls=urls, config=config)
    #     return [(AsyncCollector._package_result(result) if (isinstance(result, CrawlResult) and result.status_code == 200) else None) for result in results]
  
    # async def _collect_html(self, session: aiohttp.ClientSession, url: str, config: CrawlerRunConfig):
    #     try:
    #         if _excluded_url(url): return
    #         response = await session.get(url, headers=HTML_REQUEST_HEADERS, timeout=TIMEOUT)
    #         if response.status == 200:
    #             html_body = await response.text()
    #             result = await self.web_crawler.arun(url="raw:"+html_body, config=config)
    #             return AsyncCollector._package_result(result)
    #     except Exception as e:
    #         ic(e.__class__.__name__, e)

    # async def _fetch_urls(self, urls: list[str]) -> list[aiohttp.ClientResponse]:
    #     responses = []
    #     async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as session:
    #         for i in range(0, len(urls), COLLECTION_THROTTLE):
    #             batch = urls[i:i+COLLECTION_THROTTLE]
    #             responses.extend(await asyncio.gather(*[session.get(url, headers=HTML_REQUEST_HEADERS) for url in batch]))
    #     return responses
    
    # async def _package_http_responses(self, responses: list[aiohttp.ClientResponse], collect_metadata: bool = False) -> list[dict]:
    #     config = AsyncCollector._run_config(collect_metadata)
    #     results = []
    #     for i in range(0, len(responses), COLLECTION_THROTTLE):
    #         batch = responses[i:i+COLLECTION_THROTTLE]
    #         results.extend(await asyncio.gather(*[self._package_http_response(resp, config) for resp in batch]))
    #     return results

    # async def _package_http_response(self, response: aiohttp.ClientResponse, config: CrawlerRunConfig) -> dict:
    #     try:
    #         response.raise_for_status()
    #         result = await self.web_crawler.arun(url="raw:"+(await response.text()), config=config)
    #         if isinstance(result, CrawlResult): return AsyncCollector._package_result(result)
    #     except Exception as e:
    #         ic(e.__class__.__name__, e)


