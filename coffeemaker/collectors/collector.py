
import asyncio
from itertools import chain
import json
import logging
import os
from urllib.parse import urljoin, urlparse
from datetime import datetime
import time
import aiohttp
import feedparser
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlResult, CrawlerRunConfig, CacheMode, JsonCssExtractionStrategy, DefaultMarkdownGenerator, PruningContentFilter
import asyncpraw
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
HACKERNEWS_STORIES = [HACKERNEWS_TOP_STORIES, HACKERNEWS_NEW_STORIES, HACKERNEWS_ASK_STORIES, HACKERNEWS_SHOW_STORIES]

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

EXCLUDED_URL_PATTERNS = [
    r'\.(png|jpeg|jpg|gif|webp|mp4|avi|mkv|mp3|wav|pdf)$',
    r'(v\.redd\.it|i\.redd\.it|www\.reddit\.com\/gallery|youtube\.com|youtu\.be)',
    r'\/video(s)?\/',
    r'\/image(s)?\/',
]

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
MD_OPTIONS = {
    "ignore_links": True,
    "ignore_images": True,
    "escape_html": False,
    "skip_external_links": True,
    "skip_internal_links": True, 
}  
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
        {"name": "type", "type": "attribute", "selector": "meta[property='og:type']", "attribute": "content"},
        {"name": "author", "type": "attribute", "selector": "meta[name='author'], meta[name='dc.creator'], meta[name='byl'], meta[name='byline']", "attribute": "content"},
        {"name": "site_name", "type": "attribute", "selector": "meta[name='og:site_name'], meta[property='og:site_name']", "attribute": "content"},
        # all link selectors
        {"name": "favicon", "type": "attribute", "selector": "link[rel='shortcut icon'][type='image/png'], link[rel='icon']", "attribute": "href"},
        {"name": "rss_feed", "type": "attribute", "selector": "link[type='application/rss+xml']", "attribute": "href"},
    ]
}

MD_COLLECTION_CONFIG = CrawlerRunConfig(   
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
    wait_for_images=False,
    semaphore_count=os.cpu_count(),

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
    exclude_external_links=True,
    exclude_social_media_links=True, 

    verbose=False
)
MD_AND_METADATA_COLLECTION_CONFIG = CrawlerRunConfig(   
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
    wait_for_images=False,    
    semaphore_count=os.cpu_count(),

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
    exclude_external_links=True,
    exclude_social_media_links=True, 

    verbose=False
)

# general utilities
now = lambda: datetime.now()
reddit_submission_permalink = lambda permalink: f"https://www.reddit.com{permalink}"
hackernews_story_metadata = lambda id: f"https://hacker-news.firebaseio.com/v0/item/{id}.json"
hackernews_story_permalink = lambda id: f"https://news.ycombinator.com/item?id={id}"

def extract_base_url(url: str) -> str:
    try: return urlparse(url).netloc
    except: return None

def extract_domain(url: str) -> str:
    try: return tldextract.extract(url).domain
    except: return None

def parse_date(date: str) -> datetime:
    try: return date_parser(date)
    except: return None

# general utilities
def _excluded_url(url: str):
    return (not url) or any(re.search(pattern, url) for pattern in EXCLUDED_URL_PATTERNS)

def NOT_READY_guess_type(url: str, default_kind: str = None, **kwargs) -> str:
    """This is entirely heuristic to figure out if the url contains a news or a blog"""
    look_into = [url, extract_base_url(url), extract_domain(url)] + (kwargs.values() if kwargs else [])
    look_into = [item for item in look_into if item]

    BLOG_DOMAINS = ["medium.com",  "substack", "wordpress", "blogspot", ".dev", ".io", ".blog", ".to", ]
    
    DEFINITELY_BLOG = ["medium.com", "dev.to", "x" "substack.", "wordpress.", ".dev", "dev.to", "developers.", "github.io"]
    if "medium.com" in look_into: return BLOG
    

    if "reddit.com" in url: return POST
    if "hacker-news.firebaseio.com" in url: return POST
    if "ycombinator.com" in url: return POST
    return NEWS

async def _fetch_json(session: aiohttp.ClientSession, url: str) -> dict:
    async with session.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT) as response:
        if response.status == 200: return await response.json()

class AsyncCollector:
    md_generator: DefaultMarkdownGenerator = None

    def __init__(self):  
        self.md_generator = DefaultMarkdownGenerator(options=MD_OPTIONS)

    @property
    def web_crawler(self):
        if not hasattr(self, '_web_crawler'):
            self._web_crawler = AsyncWebCrawler(
                config=BrowserConfig(
                    headless=True,
                    ignore_https_errors=False,
                    java_script_enabled=False,
                    user_agent=USER_AGENT,
                    light_mode=True,
                    text_mode=True,
                    verbose=False
                )
            )
        return self._web_crawler

    @property
    def reddit_client(self):
        if not hasattr(self, '_reddit_client'):
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
        return self._reddit_client
    
    async def close(self):
        await self.web_crawler.close()
        await self.reddit_client.close()

    ### generic url collection utilities ###
    async def collect_url(self, url: str, collect_metadata: bool = False) -> dict:
        """Collects the body of the url as a markdown"""
        config = AsyncCollector._run_config(collect_metadata)
        result = await self.web_crawler.arun(url=url, config=config)
        return AsyncCollector._package_result(result)
    
    async def _collect_html(self, session: aiohttp.ClientSession, url: str, config: CrawlerRunConfig):
        try:
            if _excluded_url(url): return

            response = await session.get(url, headers=HTML_REQUEST_HEADERS, timeout=TIMEOUT)
            if response.status == 200:
                html_body = await response.text()
                result = await self.web_crawler.arun(url="raw:"+html_body, config=config)
                return AsyncCollector._package_result(result)
        except Exception as e:
            ic(e.__class__.__name__, e)

    async def collect_urls(self, urls: list[str], collect_metadata: bool = False) -> list[dict]:
        """Collects the bodies of the urls as markdowns"""
        # config = AsyncCollector._run_config(collect_metadata)
        # async with aiohttp.ClientSession() as session:
        #     results = await asyncio.gather(*[self._collect_html(session, url, config) for url in urls])
        # return results
        
        config = AsyncCollector._run_config(collect_metadata)
        # disable collection of excluded urls
        results = await self.web_crawler.arun_many(urls=urls, config=config)
        return [(AsyncCollector._package_result(result) if (isinstance(result, CrawlResult) and result.status_code == 200) else None) for result in results]

    async def collect_beans(self, beans: list[Bean], collect_metadata: bool = False) -> list[Bean]:
        """Collects the bodies of the beans as markdowns"""
        results = await self.collect_urls([bean.url for bean in beans], collect_metadata)
        for bean, result in zip(beans, results):
            if not result: continue
            bean.text = result.get("markdown")
            bean.title = result.get("meta_title") or bean.title or result.get("title") # this sequence is important because result['title'] is often crap
            bean.image_url = result.get("top_image") or bean.image_url
            bean.author = result.get("author") or bean.author
            bean.created = result.get("published_time") or bean.created
            bean.source = result.get("site_name") or bean.source # override the source
        return beans
    
    def _package_result(result: CrawlResult) -> dict:
        ret = {
            "url": result.url,
            "markdown": AsyncCollector._clean_markdown(result.markdown)
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
    
    _run_config = lambda collect_metadata: MD_AND_METADATA_COLLECTION_CONFIG if collect_metadata else MD_COLLECTION_CONFIG

    ### rss feed related utilities  ###
    async def collect_rssfeed(self, url: str, default_kind: str = NEWS) -> list[Bean]:
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(url, headers=RSS_REQUEST_HEADERS, timeout=TIMEOUT)
                resp.raise_for_status()
                feed = feedparser.parse(await resp.text())
                if not feed.entries: return

                source = AsyncCollector.extract_source(feed.feed.get('link') or feed.entries[0].link)
                collected = [self._from_rssfeed(entry, source, default_kind) for entry in feed.entries]
            return collected
        except Exception as e:
            log.warning("collection failed", extra={"source": url, "num_items": 1})
            ic(e.__class__.__name__, e) # NOTE: this is for local debugging

    def _from_rssfeed(self, entry: feedparser.FeedParserDict, source: str, default_kind: str) -> tuple[Bean, Chatter]:
        current_time = now()
        published_time = entry.get("published_parsed") or entry.get("updated_parsed")
        created_time = datetime.fromtimestamp(time.mktime(published_time)) if published_time else current_time
        body_html = AsyncCollector._extract_body(entry)
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
                kind=default_kind,
                text=body,
                author=entry.get('author'),        
                image_url=AsyncCollector._extract_main_image(entry)
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
        return self.md_generator.generate_markdown(cleaned_html=html).raw_markdown.strip()

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
                return "\n".join(lines[i:])
        return markdown
    
    extract_source = lambda url: extract_domain(url) or extract_base_url(url)
    
    ### reddit related utilities ###
    async def collect_subreddit(self, subreddit_name: str, default_kind: str = NEWS) -> list[tuple[Bean, Chatter]]:
        try:
            subreddit = await self.reddit_client.subreddit(subreddit_name)
            return [self._from_reddit(post, default_kind) 
                    async for post in subreddit.hot(limit=20) 
                    if not _excluded_url(post.url)]
        except Exception as e:
            log.warning("collection failed", extra={"source": subreddit_name, "num_items": 1})
            print(f"collection failed",subreddit_name, e)

    def _from_reddit(self, post, default_kind) -> tuple[Bean, Chatter]: 
        subreddit = f"r/{post.subreddit.display_name}"
        url = AsyncCollector._extract_submission_url(post)
        current_time = now()
        created_time = datetime.fromtimestamp(post.created_utc)
        return (
            Bean(
                url=url,
                created=created_time,
                collected=current_time,
                updated=created_time,
                # this is done because sometimes is_self value is wrong
                source=subreddit if post.is_self else (extract_domain(post.url) or REDDIT),
                title=post.title,
                kind=POST if post.is_self else default_kind,
                text=post.selftext,
                author=post.author.name if post.author else None,
                # fill in the defaults
                shared_in=[subreddit, REDDIT],
                likes=post.score,
                comments=post.num_comments
            ),
            Chatter(
                url=url,
                chatter_url=reddit_submission_permalink(post.permalink),            
                source=REDDIT,                        
                channel=subreddit,
                collected=current_time,
                likes=post.score,
                comments=post.num_comments
            )
        )
    
    def _extract_submission_url(post: asyncpraw.models.Submission) -> str:
        if post.is_self:
            return reddit_submission_permalink(post.permalink)
        # if the shared link itself it a reddit submission then url will not have a base url
        if extract_base_url(post.url):
            return post.url
        return reddit_submission_permalink(post.url)
    
    ### hackernews related utilities ###
    async def collect_ychackernews(self, default_kind: str = BLOG) -> list[tuple[Bean, Chatter]]:
        try:
            async with aiohttp.ClientSession() as session:
                entry_ids = await asyncio.gather(*[_fetch_json(session, ids_url) for ids_url in HACKERNEWS_STORIES])
                entry_ids = set(chain(*entry_ids))
                stories = await asyncio.gather(*[_fetch_json(session, hackernews_story_metadata(id)) for id in entry_ids])
                collected = [self._from_hackernews_story(story, default_kind) for story in stories if not _excluded_url(story.get('url'))]
            return collected
        except Exception as e:
            log.warning("collection failed", extra={"source": HACKERNEWS, "num_items": 1})
            ic(e.__class__.__name__, e)
        
    def _from_hackernews_story(self, story: dict, default_kind: str) -> tuple[Bean, Chatter]:
        # either its a shared url or it is a text
        current_time = now()
        id = story['id']
        url = story.get('url') or hackernews_story_permalink(id)
        created_time = datetime.fromtimestamp(story['time']) if 'time' in story else current_time
        return (
            Bean(            
                url=url, # this is either a linked url or a direct post
                # initially the bean's updated time will be the same as the created time
                # if there is a chatter that links to this, then the updated time will be changed to collection time of the chatter
                created=created_time,                
                collected=current_time,
                updated=created_time,
                source=extract_base_url(url),
                title=story.get('title'),
                kind=POST if not 'url' in story else default_kind, # blog, post or job
                text=self._generate_markdown(story['text']) if 'text' in story else None, # load if it has a text which usually applies to posts
                author=story.get('by'),
                # fill in the defaults
                shared_in=[HACKERNEWS],
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
    
    
