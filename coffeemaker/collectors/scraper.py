import os
import json
import asyncio
import aiohttp
import logging
from itertools import chain
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from retry import retry
from .utils import *
from coffeemaker.pybeansack.models import *
from icecream import ic

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv('BATCH_SIZE', os.cpu_count()*os.cpu_count()))
RETRY_COUNT = 3
RETRY_JITTER = (1, 10)

_METADATA_SELECTORS = {
    'site_name': "meta[property='og:site_name'], meta[property='sitename'], meta[itemprop='name']",
    'description': "meta[name='description'], meta[itemprop='description'], meta[property='og:description']",
    'meta_title': "meta[property='og:title'], meta[name='og:title']",
    'published_time': "meta[property='article:published_time'], meta[name='OriginalPublicationDate'], meta[itemprop='datePublished']",
    'top_image': "meta[property='og:image'], meta[property='og:image:url']",
    'kind': "meta[property='og:type']",
    'author': "meta[name='author'], meta[name='dc.creator']",    
    'favicon': "link[rel='shortcut icon'], link[rel='icon']",
    'rss_feed': "link[type='application/rss+xml']"
}
_HTML_REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    # 'Accept-encoding': 'gzip, deflate',
    'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.5,application/signed-exchange;v=b3;q=0.9"
}

class WebScraperLite:
    session: aiohttp.ClientSession = None
    throttle: asyncio.Semaphore = None

    def __init__(self, batch_size: int = BATCH_SIZE):
        self.throttle = asyncio.Semaphore(batch_size)
        
    async def __aenter__(self):
        """Async context manager enter"""
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=BATCH_SIZE, limit_per_host=os.cpu_count()),
            headers=_HTML_REQUEST_HEADERS, 
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
            raise_for_status=True
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
            self.session = None

    def _get_metadata(self, url: str, html: str):
        soup = BeautifulSoup(html, 'lxml')
        metadata = {}
        for key, selector in _METADATA_SELECTORS.items():
            for sel in selector.split(", "):
                if tag := soup.select_one(sel):
                    metadata[key] = tag.get('content') or tag.get('href')
                    break

        if 'published_time' in metadata: metadata['published_time'] = parse_date(metadata['published_time'])
        if 'favicon' in metadata: metadata['favicon'] = urljoin(url, metadata['favicon'])
        if 'rss_feed' in metadata: metadata['rss_feed'] = urljoin(url, metadata['rss_feed'])
        return metadata

    @retry(exceptions=[TimeoutError, aiohttp.ConnectionTimeoutError], tries=RETRY_COUNT, jitter=RETRY_JITTER)
    async def _scrape(self, url: str) -> str:
        async with self.throttle, self.session.get(url) as response:
            html = await response.text()
        return html

    async def scrape_url(self, url: str, collect_metadata=True): 
        from readability import Document
        if excluded_url(url): return 
        try:    
            html = await self._scrape(url)
            doc = Document(html)
            body = {
                'title': doc.short_title() or doc.title(),
                'author': doc.author(), 
                'content': strip_html_tags(doc.summary(html_partial=True))
            }
            if collect_metadata: body.update(self._get_metadata(url, html))
            return body
        except Exception as e: 
            log.debug(f"scraping failed - {e.__class__.__name__} {e}", extra={"source": url, "num_items": 1})

    async def scrape_urls(self, urls: list[str], collect_metadata=True):
        results = await asyncio.gather(*[self.scrape_url(url, collect_metadata) for url in urls])
        return results
    
    async def scrape_beans(self, beans: list[Bean], collect_metadata=True):
        results = await self.scrape_urls([bean.url for bean in beans], collect_metadata)
        for bean, result in zip(beans, results):
            if not result: continue
            bean.title = bean.title or result.get("meta_title") or result.get("title") # this sequence is important because result['title'] is often crap
            bean.summary = bean.summary or result.get("description")
            bean.content = result.get("content")
            bean.restricted_content = True
            bean.image_url = bean.image_url or result.get("top_image") 
            bean.author = result.get("author") or bean.author
            bean.created = min(result.get("published_time") or bean.created, bean.collected)            
            bean.publisher.title = result.get('site_name')
            bean.publisher.favicon = result.get('favicon')
            bean.publisher.rss_feed = result.get("rss_feed")
           
        return beans

# GENERIC URL COLLECTOR CONFIG
_BASE_EXCLUDED_TAGS = ["script", "style", "nav", "footer", "navbar", "comment", "contact",
"img", "audio", "video", "source", "track", "iframe", "object", "embed", "param", "picture", "figure",
"svg", "canvas", "aside", "form", "input", "button", "textarea", "select", "option", "optgroup", "ins"]
_METADATA_EXCLUDED_TAGS = _BASE_EXCLUDED_TAGS + ["link", "meta"] 
_METADATA_SELECTORS_SCHEMA = {
    "name": "Site Metadata",
    "baseSelector": "html",
    "fields": [
        # all body selectors
        {"name": "title", "type": "text", "selector": "title, h1"},
        # all meta selectors
        {"name": "description", "type": "attribute", "selector": "meta[name='description']", "attribute": "content"},
        {"name": "meta_title", "type": "attribute", "selector": "meta[property='og:title'], meta[name='og:title']", "attribute": "content"},
        {"name": "published_time", "type": "attribute", "selector": "meta[property='rnews:datePublished'], meta[property='article:published_time'], meta[name='OriginalPublicationDate'], meta[itemprop='datePublished'], meta[property='og:published_time'], meta[name='article_date_original'], meta[name='publication_date'], meta[name='sailthru.date'], meta[name='PublishDate'], meta[property='pubdate']", "attribute": "content"},
        {"name": "top_image", "type": "attribute", "selector": "meta[property='og:image'], meta[property='og:image:url'], meta[name='og:image:url'], meta[name='og:image']", "attribute": "content"},
        {"name": "kind", "type": "attribute", "selector": "meta[property='og:type']", "attribute": "content"},
        {"name": "author", "type": "attribute", "selector": "meta[name='author'], meta[name='dc.creator'], meta[name='byl'], meta[name='byline'], meta[property='article:author_name']", "attribute": "content"},
        {"name": "site_name", "type": "attribute", "selector": "meta[name='og:site_name'], meta[property='og:site_name'], meta[property='sitename']", "attribute": "content"},
        # all link selectors
        {"name": "favicon", "type": "attribute", "selector": "link[rel='shortcut icon'][type='image/png'], link[rel='icon']", "attribute": "href"},
        {"name": "rss_feed", "type": "attribute", "selector": "link[type='application/rss+xml']", "attribute": "href"},
    ]
}
_MARKDOWN_SELECTORS = ", ".join([
    "article", 
    "main", 
    ".article-body", 
    ".article", 
    ".article-content", 
    ".main-article", 
    ".content"
    "[class~='main']"
])
_MARKDOWN_EXCLUDED_SELECTORS = ", ".join([
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
_MARKDOWN_OPTIONS = {
    "ignore_images": True,
    "escape_html": False,
    # "skip_external_links": True,
    # "skip_internal_links": True, 
}

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

class WebScraper:        
    browser_config = None
    batch_size = None
    remote_crawler = None
    crawling_semaphore = None

    def __init__(self, remote_crawler: str = None, batch_size: int = BATCH_SIZE):
        from crawl4ai import BrowserConfig

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
        from crawl4ai import CrawlerRunConfig, CacheMode, JsonCssExtractionStrategy, DefaultMarkdownGenerator

        if collect_metadata: return CrawlerRunConfig(   
            # content processing
            word_count_threshold=100,
            markdown_generator=DefaultMarkdownGenerator(options=_MARKDOWN_OPTIONS),
            extraction_strategy=JsonCssExtractionStrategy(schema=_METADATA_SELECTORS_SCHEMA),
            excluded_tags=_BASE_EXCLUDED_TAGS,
            excluded_selector=_MARKDOWN_EXCLUDED_SELECTORS,
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
            markdown_generator=DefaultMarkdownGenerator(options=_MARKDOWN_OPTIONS),
            css_selector=_MARKDOWN_SELECTORS, # this is only for markdown generation
            excluded_tags=_METADATA_EXCLUDED_TAGS,
            excluded_selector=_MARKDOWN_EXCLUDED_SELECTORS,
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
        from crawl4ai import Crawl4aiDockerClient, CrawlResult

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
        from crawl4ai import AsyncWebCrawler

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
        if excluded_url(url): return
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
            bean.title = bean.title or result.get("title") or result.get("meta_title") # this sequence is important because result['title'] is often crap
            bean.image_url = result.get("top_image") or bean.image_url
            bean.author = result.get("author") or bean.author
            bean.created = min(result.get("published_time") or bean.created or bean.collected, current_time)
            bean.summary = bean.summary or result.get("description")
            bean.site_rss_feed = result.get("rss_feed")
            bean.site_name = result.get('site_name')
            bean.site_favicon = result.get('favicon')
            bean.restricted_content = True
        return beans

    def _package_result(result) -> dict:   
        if not(result and result.success): return

        ret = {
            "url": result.url,
            "markdown": strip_html_tags(result.cleaned_html) #_clean_markdown(result.markdown)
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
    
# <meta property="og:site_name" content="Digital Seams"/>
# <meta property="og:title" content="Digital Seams"/>
# <meta property="og:url" content="https://digitalseams.com"/>
# <meta property="og:type" content="website"/>
# <meta property="og:description" content="A blog about the connections in our often-online existence, written by Bobbie Chen."/>
# <meta itemprop="name" content="Digital Seams"/>
# <meta itemprop="url" content="https://digitalseams.com"/>
# <meta itemprop="description" content="A blog about the connections in our often-online existence, written by Bobbie Chen."/>
# <link rel="icon" type="image/x-icon" href="https://images.squarespace-cdn.com/content/v1/598a2436f7e0ab837d08f4c6/5ba8778d-1fd1-45e3-a8cb-346b861097fe/favicon.ico?format=100w"/>
# <link rel="alternate" type="application/rss+xml" title="RSS Feed" href="https://digitalseams.com/blog?format=rss" />

class PublisherScraper:
    session: aiohttp.ClientSession = None
    throttle: asyncio.Semaphore = None

    def __init__(self, batch_size: int = BATCH_SIZE):
        self.throttle = asyncio.Semaphore(batch_size)
        
    async def __aenter__(self):
        """Async context manager enter"""
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=BATCH_SIZE, limit_per_host=os.cpu_count()),
            headers=_HTML_REQUEST_HEADERS, 
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
            raise_for_status=True
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
            self.session = None

    def _get_metadata(self, url: str, html: str):
        soup = BeautifulSoup(html, 'lxml')
        metadata = {}
        for key, selector in _METADATA_SELECTORS.items():
            for sel in selector.split(", "):                
                if tag := soup.select_one(sel):
                    metadata[key] = tag.get('content') or tag.get('href')
                    break

        if 'favicon' in metadata: metadata['favicon'] = urljoin(url, metadata['favicon'])
        if 'rss_feed' in metadata: metadata['rss_feed'] = urljoin(url, metadata['rss_feed'])
        return metadata
    
    @retry(exceptions=[TimeoutError, aiohttp.ConnectionTimeoutError], tries=RETRY_COUNT, jitter=RETRY_JITTER)
    async def _scrape_favicon(self, url: str):
        async with self.throttle, self.session.get("https://www.google.com/s2/favicons?domain="+extract_base_url(url)) as response:
            if response.status == 200: return response.headers.get('Content-Location')

    @retry(exceptions=[TimeoutError, aiohttp.ConnectionTimeoutError], tries=RETRY_COUNT, jitter=RETRY_JITTER)
    async def _scrape_html(self, url: str) -> str:
        async with self.throttle, self.session.get(url) as response:
            html = await response.text()
        return html

    async def scrape_url(self, url: str):
        meta = {} 
        base_url = extract_base_url(url)
        meta[K_BASE_URL] = base_url
        url = "https://"+extract_base_url(url)
        meta[K_SOURCE] = extract_domain(url)
        try:                  
            html = await self._scrape_html(url)
            meta.update(self._get_metadata(url, html))
            meta[K_TITLE] = meta.get('site_name') or meta.get('meta_title')
            meta[K_SUMMARY] = meta.get(K_DESCRIPTION)
            if K_FAVICON not in meta:
                meta[K_FAVICON] = await self._scrape_favicon(url)
            return meta
        except Exception as e: 
            log.debug(f"scraping failed - {e.__class__.__name__} {e}", extra={"source": url, "num_items": 1})

    async def scrape_urls(self, urls: list[str]):
        results = await asyncio.gather(*[self.scrape_url(url) for url in urls])
        return results
    
    async def scrape_beans(self, beans: list[Bean]):
        results = await self.scrape_urls([bean.url for bean in beans])
        for bean, result in zip(beans, results):
            if not result: continue
            bean.publisher = Publisher(**result)           
        return beans