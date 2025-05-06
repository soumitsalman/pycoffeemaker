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
import feedparser
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlResult, CrawlerRunConfig, CacheMode, JsonCssExtractionStrategy, DefaultMarkdownGenerator
import praw
import prawcore
import requests
from retry import retry
import tldextract
from dateutil.parser import parse as date_parser
import re
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
SCRAPER_BATCH_SIZE = 16*os.cpu_count()

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

### rss feed related utilities ###
def _extract_body(entry: feedparser.FeedParserDict) -> str:
    # the body usually lives in <dc:content>, <content:encoded> or <description>
    body_html: str = ""
    if 'dc_content' in entry:
        body_html = entry.dc_content
    elif 'content' in entry:        
        body_html = entry.content[0]['value'] if isinstance(entry.content, list) else entry.content    
    elif 'summary' in entry:
        body_html = entry.summary
    return body_html.strip()

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

async def _fetch_json_async(session: aiohttp.ClientSession, url: str) -> dict:
    body = None
    async with session.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT) as response:
        response.raise_for_status()
        body = await response.json()
    return body

def _fetch_json(content_url: str):
    # @retry(tries=2, delay=10, jitter=(5, 10))
    def _fetch():
        resp = requests.get(content_url, headers=JSON_REQUEST_HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    try: return _fetch()
    except Exception as e: 
        log.warning("collection failed", extra={"source": content_url, "num_items": 1})
        ic(content_url, e, e.__class__.__name__)

merge_lists = lambda results: list(chain(*(r for r in results if r))) 

def _batch_collect(collect: Callable, sources: list):
    results = None
    with ThreadPoolExecutor(max_workers=SCRAPER_BATCH_SIZE, thread_name_prefix="collector") as executor:
        results = list(executor.map(collect, sources))
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

class APICollector:
    md_generator: DefaultMarkdownGenerator = None
    reddit_client = None

    def __init__(self):  
        self.md_generator = DefaultMarkdownGenerator(options=MD_OPTIONS)
        self.reddit_client = praw.Reddit(
            check_for_updates=True,
            client_id=os.getenv("REDDIT_APP_ID"),
            client_secret=os.getenv("REDDIT_APP_SECRET"),
            user_agent=USER_AGENT+" (by u/randomizer_000)",
            username=os.getenv("REDDIT_COLLECTOR_USERNAME"),
            password=os.getenv("REDDIT_COLLECTOR_PASSWORD"),
            timeout=TIMEOUT,
            rate_limit_seconds=RATELIMIT_WAIT,
        )

    def collected_rssfeeds(self, feed_urls: list[str]) -> list[Bean]|list[tuple[Bean, Chatter]]:
        return merge_lists(_batch_collect(self.collect_rssfeed, feed_urls))

    def collect_rssfeed(self, url: str) -> list[Bean]|list[tuple[Bean, Chatter]]:
        collected = None
        try:
            resp = requests.get(url, headers=RSS_REQUEST_HEADERS, timeout=TIMEOUT)  # Set timeout to 10 seconds
            resp.raise_for_status()  # Raise exception for bad status codes
            feed = feedparser.parse(BytesIO(resp.content))
                    
            if feed.entries: 
                source = extract_source(feed.entries[0].link)
                collected = [self._from_rssfeed(entry, source, NEWS) for entry in feed.entries]
        except Exception as e:
            ic(url, e)

        if collected: log.info("collected", extra={"source": url, "num_items": len(feed.entries)})
        else: log.warning("collection failed", extra={"source": url, "num_items": 1})
        return collected
 
    ### rss feed related utilities  ###
    async def collect_rssfeed_async(self, url: str, default_kind: str = NEWS) -> list[Bean]|list[tuple[Bean, Chatter]]:
        try:
            collected = None
            async with aiohttp.ClientSession() as session:
                resp = await session.get(url, headers=RSS_REQUEST_HEADERS, timeout=TIMEOUT)
                resp.raise_for_status()
                feed = feedparser.parse(await resp.text())
                if feed.entries:
                    source = extract_source(feed.feed.get('link') or feed.entries[0].link)
                    collected = [self._from_rssfeed(entry, source, default_kind) for entry in feed.entries]
                else:
                    log.warning("collection failed", extra={"source": url, "num_items": 1})
            return collected
        except Exception as e:
            log.warning("collection failed", extra={"source": url, "num_items": 1})
            ic(url, e) # NOTE: this is for local debugging

    def _from_rssfeed(self, entry: feedparser.FeedParserDict, source: str, default_kind: str) -> tuple[Bean, Chatter]:
        current_time = now()
        published_time = entry.get("published_parsed") or entry.get("updated_parsed")
        created_time = from_timestamp(time.mktime(published_time)) if published_time else current_time
        body_html = _extract_body(entry)
        body = f"# {entry.title}\n\n{self._generate_markdown(body_html)}" if body_html else None
        return (
            Bean(
                url=entry.link,
                # in case of rss feed, the created time is the same as the updated time during collection. if it is mentioned in a social media feed then the updated time will get change
                created=created_time,         
                collected=current_time,
                updated=created_time,
                source=source,
                title=entry.title,
                kind=_guess_type(entry.link, source) or default_kind,
                text=body,
                author=entry.get('author'),        
                image_url=_extract_main_image(entry)
            ),
            Chatter(
                url=entry.link,
                source=source,
                chatter_url=entry.get('wfw_commentrss'),
                collected=current_time,
                comments=entry.slash_comments
            ) if 'slash_comments' in entry else None
        )
    
    def _generate_markdown(self, html: str) -> str:
        """Converts the given html into a markdown"""
        # TODO: ideally this should be done with crawler.arun("raw:"+html)
        return self.md_generator.generate_markdown(input_html=html).raw_markdown.strip()
    
    def collect_subreddits(self, subreddit_names: list[str]):
        return merge_lists(_batch_collect(self.collect_subreddit, subreddit_names))
    
    def collect_subreddit(self, subreddit_name, default_kind: str = NEWS):
        @retry(exceptions=(prawcore.exceptions.ResponseException), tries=2, delay=10, jitter=(5, 10))
        def _collect():
            sr = self.reddit_client.subreddit(subreddit_name)
            return [self._from_reddit(post, subreddit_name, default_kind) for post in sr.hot(limit=25) if not _excluded_url(post.url)]
        try: return _collect()
        except Exception as e:
            log.warning("collection failed", extra={"source": subreddit_name, "num_items": 1})
            ic(subreddit_name, e, e.__class__.__name__)            
    
    ### reddit related utilities ###
    async def collect_subreddit_async(self, subreddit_name: str, default_kind: str = NEWS) -> list[tuple[Bean, Chatter]]:
        # @retry(tries=2, delay=5, jitter=(0, 10))
        async def _collect():
            subreddit = await self.reddit_client.subreddit(subreddit_name)
            return [self._from_reddit(post, subreddit_name, default_kind) async for post in subreddit.hot(limit=25) if not _excluded_url(post.url)]
        try: return await _collect()
        except Exception as e:
            log.warning("collection failed", extra={"source": subreddit_name, "num_items": 1})
            ic(subreddit_name, e, e.__class__.__name__)

    def _from_reddit(self, post, sr_name, default_kind) -> tuple[Bean, Chatter]: 
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
                title=post.title,
                kind=kind,
                text=post.selftext,
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
    async def collect_ychackernews_async(self) -> list[tuple[Bean, Chatter]]:
        try:
            async with aiohttp.ClientSession() as session:
                entry_ids = await asyncio.gather(*[_fetch_json(session, ids_url) for ids_url in HACKERNEWS_STORIES_URLS])
                entry_ids = set(chain(*entry_ids))
                stories = await asyncio.gather(*[_fetch_json(session, hackernews_story_metadata(id)) for id in entry_ids])
                collected = [self._from_hackernews_story(story, BLOG) for story in stories if story and not _excluded_url(story.get('url'))]
            return collected
        except Exception as e:
            log.warning("collection failed", extra={"source": HACKERNEWS, "num_items": 1})
            ic(HACKERNEWS, e)

    def collect_ychackernews(self) -> list[tuple[Bean, Chatter]]:
        ids = merge_lists(_batch_collect(_fetch_json, HACKERNEWS_STORIES_URLS))
        stories = _batch_collect(_fetch_json, [hackernews_story_metadata(id) for id in ids])
        return _batch_collect(
            lambda story: self._from_hackernews_story(story, BLOG), 
            [story for story in stories if story and not _excluded_url(story.get('url'))]
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
                title=story.get('title'),
                kind=kind, # blog, post or job
                text=self._generate_markdown(story['text']) if 'text' in story else None, # load if it has a text which usually applies to posts
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
        # {"name": "description", "type": "attribute", "selector": "meta[name='description']", "attribute": "content"},
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

    def __init__(self):
        self.browser_config = BrowserConfig(
            headless=True,
            ignore_https_errors=False,
            java_script_enabled=False,
            user_agent=USER_AGENT,
            light_mode=True,
            text_mode=True,
            verbose=False
        )

    def _run_config(collect_metadata: bool):
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
            semaphore_count=SCRAPER_BATCH_SIZE,
            wait_for_images=False,  
            page_timeout=30000,

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

            verbose=False
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
            cache_mode=CacheMode.ENABLED,

            # navigation & timing
            semaphore_count=SCRAPER_BATCH_SIZE,
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

            verbose=False
        )

    async def scrape_url(self, url: str, collect_metadata: bool = False) -> dict:
        """Collects the body of the url as a markdown"""
        if _excluded_url(url): return
        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            parsed_result = await crawler.arun(url=url, config=WebScraper._run_config(collect_metadata))
            result = APICollector._package_result(parsed_result)
        return result

    async def scrape_urls(self, urls: list[str], collect_metadata: bool = False) -> list[dict]:
        """Collects the bodies of the urls as markdowns"""
        try:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                parsed_results = await crawler.arun_many(urls=urls, config=WebScraper._run_config(collect_metadata))
                parsed_results = {result.url: result for result in parsed_results}
                results = [WebScraper._package_result(parsed_results[url]) for url in urls]            
            return results
        except:
            return [None]*len(urls)

    async def scrape_beans(self, beans: list[Bean], collect_metadata: bool = False) -> list[Bean]:
        """Collects the bodies of the beans as markdowns"""
        results = await self.scrape_urls([bean.url for bean in beans], collect_metadata)
        current_time = now()
        for bean, result in zip(beans, results):
            if not result: continue
            bean.text = result.get("markdown")
            bean.title = result.get("meta_title") or bean.title or result.get("title") # this sequence is important because result['title'] is often crap
            bean.image_url = result.get("top_image") or bean.image_url
            bean.author = result.get("author") or bean.author
            bean.created = min(result.get("published_time") or bean.created or bean.collected, current_time)
            bean.source = result.get("site_name") or bean.source # override the source
        return beans

    def _package_result(result) -> dict:   
        if not (result and result.status_code == 200): return        

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


