
import asyncio
from itertools import chain
import logging
import os
from urllib.parse import urlparse
from datetime import datetime
import time
import aiohttp
import requests
import feedparser
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlResult, CrawlerRunConfig, CacheMode, JsonCssExtractionStrategy, DefaultMarkdownGenerator, PruningContentFilter
import asyncpraw
import tldextract

from coffeemaker.collectors import USER_AGENT, TIMEOUT, RATELIMIT_WAIT
from coffeemaker.pybeansack.models import *
from icecream import ic

REDDIT = "Reddit"

HACKERNEWS = "ycombinator"
HACKERNEWS_TOP_STORIES = "https://hacker-news.firebaseio.com/v0/topstories.json"
HACKERNEWS_NEW_STORIES = "https://hacker-news.firebaseio.com/v0/newstories.json"
# HACKERNEWS_JOB_STORIES = "https://hacker-news.firebaseio.com/v0/jobstories.json"
HACKERNEWS_ASK_STORIES = "https://hacker-news.firebaseio.com/v0/askstories.json"
HACKERNEWS_SHOW_STORIES = "https://hacker-news.firebaseio.com/v0/showstories.json"
HACKERNEWS_STORIES = [HACKERNEWS_TOP_STORIES, HACKERNEWS_NEW_STORIES, HACKERNEWS_ASK_STORIES, HACKERNEWS_SHOW_STORIES]

EXCLUDED_URL_PATTERNS = [
    r'\.(png|jpeg|jpg|gif|webp|mp4|avi|mkv|mp3|wav|pdf)$',
    r'(v\.redd\.it|i\.redd\.it|www\.reddit\.com\/gallery|youtube\.com|youtu\.be)',
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
    ".sponsored-block",
    "[class~='sponsored-block']", 
    ".sponsored-text", 
    "[id~='advertorial']", 
    "[class~='advertorial-content']", 
    "[class~='marketing-page']",
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
    page_timeout=TIMEOUT*1000, # since timeout is in milliseconds
    max_range=TIMEOUT,
    wait_for_images=False,
    semaphore_count=os.cpu_count()*4,

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
    page_timeout=TIMEOUT*1000, # since timeout is in milliseconds
    # max_range=TIMEOUT,
    # mean_delay=0.05,
    wait_for_images=False,
    
    semaphore_count=os.cpu_count()*4,

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

log = logging.getLogger(__name__)

class Collector:
    md_generator: DefaultMarkdownGenerator
    web_crawler: AsyncWebCrawler
    reddit_crawler: asyncpraw.Reddit

    def __init__(self):        
        self.md_generator = DefaultMarkdownGenerator(options=MD_OPTIONS)
        self.web_crawler = AsyncWebCrawler(
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
        self.reddit_crawler = asyncpraw.Reddit(
            check_for_updates=True,
            client_id=os.getenv("REDDIT_APP_ID"),
            client_secret=os.getenv("REDDIT_APP_SECRET"),
            user_agent=USER_AGENT+" (by u/randomizer_000)",
            username=os.getenv("REDDIT_COLLECTOR_USERNAME"),
            password=os.getenv("REDDIT_COLLECTOR_PASSWORD"),
            timeout=TIMEOUT,
            rate_limit_seconds=RATELIMIT_WAIT,
        )

    async def start(self):
        await self.web_crawler.start()

    async def close(self):
        await self.web_crawler.close()
        await self.reddit_crawler.close()

    async def collect_url(self, url: str, collect_metadata: bool = False) -> dict:
        """Collects the body of the url as a markdown"""
        result = await self.web_crawler.arun(url=url, config=Collector._run_config(collect_metadata))
        return Collector._package_result(result)

    async def collect_urls(self, urls: list[str], collect_metadata: bool = False) -> list[dict]:
        """Collects the bodies of the urls as markdowns"""        
        results = await self.web_crawler.arun_many(urls=urls, config=Collector._run_config(collect_metadata))
        ic({res.status_code for res in results})
        return [Collector._package_result(res) for res in results]
    
    async def collect_beans(self, beans: list[Bean], collect_metadata: bool = False) -> list[Bean]:
        """Collects the bodies of the beans as markdowns"""
        results = await self.collect_urls([bean.url for bean in beans], collect_metadata)
        for bean, result in zip(beans, results):
            if not result: continue
            bean.text = result.get("markdown")
            bean.title = bean.title or result.get("meta_title") or result.get("title")
            bean.image_url = bean.image_url or result.get("top_image")
            bean.author = bean.author or result.get("author")
            bean.created = bean.created or result.get("published_time")
        return beans
    
    def _package_result(result: CrawlResult) -> dict:
        if result.status_code != 200: return
        ret = {
            "url": result.url,
            "markdown": Collector._clean_markdown(result.markdown)
        }
        if result.extracted_content:
            ret.update(json.loads(result.extracted_content))
        return ret
    
    _run_config = lambda collect_metadata: MD_AND_METADATA_COLLECTION_CONFIG if collect_metadata else MD_COLLECTION_CONFIG

    async def collect_rssfeed(self, url: str, default_kind: str = NEWS) -> list[Bean]:
        try:
            feed = feedparser.parse(url)
            if not feed.entries: return

            collection_time = now()
            source = extract_base_url(feed.feed.get('link') or extract_base_url(feed.entries[0].link))
            titles_and_bodies = [(entry.title, Collector._extract_body(entry)) for entry in feed.entries]
            make_body_markdown = lambda title, body_html: ("# " + title + "\n\n" + self._generate_markdown(body_html)) if body_html else None
            bodies = [make_body_markdown(title, body_html) for title, body_html in titles_and_bodies]
            return [Collector._from_rssfeed(entry, body, source, default_kind, collection_time) for entry, body in zip(feed.entries,  bodies)]
        except Exception as e:
            print(f"ERROR COLLECTING RSS FEED: {url}: {e}")            

    def _from_rssfeed(entry: feedparser.FeedParserDict, body: str, source: str, default_kind: str, collection_time: datetime) -> tuple[Bean, Chatter]:
        # TODO: get the string
        # ic(entry)
        published_time = entry.get("published_parsed") or entry.get("updated_parsed")
        created_time = datetime.fromtimestamp(time.mktime(published_time)) if published_time else now()
        return (
            Bean(
                url=entry.link,
                # in case of rss feed, the created time is the same as the updated time during collection. if it is mentioned in a social media feed then the updated time will get change
                created=created_time,         
                collected=collection_time,
                updated=created_time,
                source=source,
                title=entry.title,
                kind=default_kind,
                text=body,
                author=entry.get('author'),        
                image_url=Collector._extract_main_image(entry)
            ),
            None
        )
    
    def _generate_markdown(self, html: str) -> str:
        """Converts the given html into a markdown"""
        # TODO: ideally this should be done with crawler.arun("raw:"+html)
        return self.md_generator.generate_markdown(cleaned_html=html).raw_markdown.strip()

    # rss feed related utilities 
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

    # reddit related utilities
    async def collect_subreddit(self, subreddit_name: str, default_kind: str = NEWS) -> list[tuple[Bean, Chatter]]:
        try:
            collection_time = now()
            subreddit = await self.reddit_crawler.subreddit(subreddit_name)
            return [Collector._from_reddit(post, default_kind, collection_time) async for post in subreddit.hot(limit=20) if not Collector._exclude_url(post.url)]
        except Exception as e:
            print(f"ERROR COLLECTING SUBREDDIT: {subreddit_name}: {e}")

    def _from_reddit(post, default_kind, collection_time) -> tuple[Bean, Chatter]: 
        subreddit = f"r/{post.subreddit.display_name}"
        url = Collector._extract_submission_url(post)
        return (
            Bean(
                url=url,
                created=datetime.fromtimestamp(post.created_utc),
                collected=collection_time,
                updated=collection_time,
                # this is done because sometimes is_self value is wrong
                source=extract_base_url(post.url) or subreddit or REDDIT,
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
                chatter_url=Collector._reddit_submission_permalink(post.permalink),            
                source=REDDIT,                        
                channel=subreddit,
                collected=collection_time,
                likes=post.score,
                comments=post.num_comments
            )
        )
    
    _reddit_submission_permalink = lambda permalink: f"https://www.reddit.com{permalink}"
    
    def _extract_submission_url(post: asyncpraw.models.Submission) -> str:
        if post.is_self:
            return Collector._reddit_submission_permalink(post.permalink)
        # if the shared link itself it a reddit submission then url will not have a base url
        if extract_base_url(post.url):
            return post.url
        return Collector._reddit_submission_permalink(post.url)
    
    async def collect_ychackernews(self, default_kind: str = BLOG) -> list[tuple[Bean, Chatter]]:
        collection_time = now()
        async with aiohttp.ClientSession() as session:
            entry_ids = await asyncio.gather(*[Collector._fetch_json(session, ids_url) for ids_url in HACKERNEWS_STORIES])
            entry_ids = set(chain(*entry_ids))
            stories = await asyncio.gather(*[Collector._fetch_json(session, Collector._hackernews_story_metadata(id)) for id in entry_ids])
        items = [Collector._from_hackernews_story(story, default_kind, collection_time) for story in stories]
        return [item for item in items if item]
    
    _hackernews_story_metadata = lambda id: f"https://hacker-news.firebaseio.com/v0/item/{id}.json"
    _hackernews_story_permalink = lambda id: f"https://news.ycombinator.com/item?id={id}"
        
    def _from_hackernews_story(story: dict, default_kind: str, collection_time: datetime) -> tuple[Bean, Chatter]:
        # either its a shared url or it is a text
        id = story['id']
        url = story.get('url') or Collector._hackernews_story_permalink(id)
        if Collector._exclude_url(url): return (None, None)

        created = datetime.fromtimestamp(story['time']) if 'time' in story else collection_time
        return (
            Bean(            
                url=url, # this is either a linked url or a direct post
                # initially the bean's updated time will be the same as the created time
                # if there is a chatter that links to this, then the updated time will be changed to collection time of the chatter
                created=created,                
                collected=collection_time,
                updated=collection_time,
                source=extract_base_url(url),
                title=story.get('title'),
                kind=POST if not 'url' in story else default_kind, # blog, post or job
                text=story.get('text'), # load if it has a text which usually applies to posts
                author=story.get('by'),
                # fill in the defaults
                shared_in=[HACKERNEWS],
                likes=story.get('score'),
                comments=len(story.get('kids', []))
            ), 
            Chatter(
                url=url,
                chatter_url=Collector._hackernews_story_permalink(id),
                collected=collection_time,
                source=HACKERNEWS,
                channel=str(id),
                likes=story.get('score'),
                comments=len(story.get('kids', []))
            )
        )
    
    # general utilities
    async def _fetch_json(session: aiohttp.ClientSession, url: str) -> dict:
        async with session.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT) as response:
            response.raise_for_status()
            return await response.json()
    
    def _exclude_url(url: str):
        return any(re.search(pattern, url) for pattern in EXCLUDED_URL_PATTERNS)

# general utilities
extract_base_url = lambda url: urlparse(url).netloc
extract_domain = lambda url: tldextract.extract(url).domain
now = lambda: datetime.now()

import re, json, random

def url_to_filename(url: str) -> str:
    return "./.test/" + re.sub(r'[^a-zA-Z0-9]', '-', url)

def save_markdown(url, markdown):
    filename = url_to_filename(url)+".md"
    with open(filename, 'w') as file:
        file.write(markdown)

def save_json(url, items):
    filename = url_to_filename(url)+".json"
    with open(filename, 'w') as file:
        json.dump(items, file)

MIN_WORDS_THRESHOLD = 100
is_above_threshold = lambda bean: bean.text and len(bean.text.split()) >= MIN_WORDS_THRESHOLD
is_storable = lambda bean: bean.kind == POST or is_above_threshold(bean)
is_downloadable = lambda bean: bean.kind != POST and not is_above_threshold(bean)
storable = lambda beans: [bean for bean in beans if is_storable(bean)]
downloadable = lambda beans: [bean for bean in beans if is_downloadable(bean)]

async def run_collections_async():
    # feed_urls = [ 
    #     "https://www.androidpolice.com/feed/",   
    #     "# https://www.ft.com/rss/home" # cant access without subscription
    #     "https://newatlas.com/index.rss",
    #     "https://www.channele2e.com/feed/topic/latest",
    #     "https://www.ghacks.net/feed/",
    #     "https://thenewstack.io/feed",
    #     "https://scitechdaily.com/feed/",
    #     "https://www.techradar.com/feeds/articletype/news",
    #     "https://www.geekwire.com/feed/",
    #     "https://investorplace.com/content-feed/",
    #     "https://dev.to/feed",
    #     "https://techxplore.com/rss-feed/",
    #     "https://spacenews.com/feed/",
    #     "https://crypto.news/feed/",
    #     "https://api.quantamagazine.org/feed/",
    #     "https://securityintelligence.com/feed/",
    #     "https://lifehacker.com/feed/rss",
    #     "https://Extremetech.com/feed" 
    # ]
    # subreddits = [
    #     "StartupAccelerators",
    #     "StartupConfessions",
    #     "startup_funding",
    #     "StartupFeedback",
    #     "StartupWeekend",
    #     "LocalLLaMA",
    #     "InternationalNews",
    #     "InfoSecNews",
    #     "golang",
    #     "GlobalMarketNews",
    #     "FinanceNews",
    #     "cybersecurity",
    #     "CryptoNews",
    #     "Crypto_Currency_News",
    #     "worldnews",
    #     "UpliftingNews",
    #     "NewsHub",
    #     "Conservative",
    # ]
    with open("./coffeemaker/collectors/rssfeedsources.txt", 'r') as file:
        feed_urls = [line.strip() for line in file.readlines() if line.strip()]
    with open("./coffeemaker/collectors/redditsources.txt", 'r') as file:
        subreddits = [line.strip() for line in file.readlines() if line.strip()]  

    collector = Collector()
    await collector.start()

    start = datetime.now()
    collection_tasks = \
        [collector.collect_rssfeed(source) for source in feed_urls] + \
        [collector.collect_subreddit(source) for source in subreddits] + \
        [collector.collect_ychackernews()]
    collection_results = await asyncio.gather(*collection_tasks)
    print(f"COLLECTION TIME: {datetime.now() - start}")
 
    start = datetime.now()
    storing_tasks, downloading_tasks = [], []
    for collected_batch in collection_results:        
        if not collected_batch:
            print("NO ITEMS")
            continue

        collected_beans, chatters = zip(*collected_batch)
        collected_beans = [bean for bean in collected_beans if bean]

        if not collected_beans:
            print("NO BEANS")
            continue

        print(f"===== COLLECTION: {collected_beans[0].source} =====")
        print("\t", len(collected_beans), "COLLECTED")
        needs_download = downloadable(collected_beans)
        if needs_download:
            print("\t", len(needs_download), "NEEDS DOWNLOAD")
            downloading_tasks.append(collector.collect_beans(needs_download))
            collected_beans = [bean for bean in collected_beans if bean not in needs_download]

        storing_tasks.append(store_beans(collected_beans))        
    download_results = await asyncio.gather(*downloading_tasks)
    print(f"DOWNLOAD TIME: {datetime.now() - start}")
    await collector.close()

    start = datetime.now()
    for downloaded_beans in download_results:
        print(f"===== DOWNLOAD: {downloaded_beans[0].source} =====")
        print("\t", len(downloaded_beans), "EXPECTED")
        beans = storable(downloaded_beans)
        print("\t", len(beans), "DOWNLOADED")
        storing_tasks.append(store_beans(beans))
    await asyncio.gather(*storing_tasks)
    print(f"STORING TIME: {datetime.now() - start}")


async def store_beans(beans: list[Bean]):
    if not beans: 
        print("NOTHING TO STORE")
        return

    print(f"===== STORE: {beans[0].source} =====")
    print("\t", len(beans), "GIVEN")
    beans = storable(beans)
    print("\t", len(beans), "STORABLE")
    save_json(beans[0].url, [bean.model_dump_json(exclude_none=True, exclude_unset=True) for bean in beans])
    random_item = random.choice(beans)
    save_markdown(random_item.url, random_item.text)

if __name__ == "__main__":
    asyncio.run(run_collections_async())
