import os
import json
import asyncio
import aiohttp
import lxml.html
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from lxml.cssselect import CSSSelector
from utils.logs import get_logger
from itertools import chain
from urllib.parse import unquote, urljoin, urlparse
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random
from .utils import *
from icecream import ic

log = get_logger(__name__)

PARSE_CONCURRENCY = int(os.getenv("PARSE_CONCURRENCY", min(4, os.cpu_count())))  # max DOM trees in memory at once
PDF_CONCURRENCY = int(os.getenv("PDF_CONCURRENCY", 2))  # PDF conversion is CPU/native-heavy; keep it isolated from HTML parsing

_METADATA_SELECTORS = {
    'site_name': "meta[property='og:site_name'], meta[property='sitename'], meta[itemprop='name']",
    'description': "meta[name='description'], meta[itemprop='description'], meta[property='og:description']",
    'meta_title': "meta[property='og:title'], meta[name='og:title']",
    'published_time': "meta[property='article:published_time'], meta[name='OriginalPublicationDate'], meta[itemprop='datePublished']",
    'top_image': "meta[property='og:image'], meta[property='og:image:url']",
    'kind': "meta[property='og:type']",
    'author': "meta[name='author'], meta[name='dc.creator']",    
    'favicon': "link[rel='shortcut icon'], link[rel='icon']",
    'rss_feed': "link[type='application/rss+xml']",
    'language': "meta[http-equiv='content-language'], meta[name='language'], html[lang]",
    'keywords': "meta[name='keywords']"
}
_METADATA_CSS = {key: [CSSSelector(sel) for sel in selector.split(", ")] for key, selector in _METADATA_SELECTORS.items()}
_HTML_REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    # 'Accept-encoding': 'gzip, deflate',
    'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.5,application/signed-exchange;v=b3;q=0.9"
}

def _get_metadata(url: str, tree: lxml.html.HtmlElement):
    metadata = {}
    for key, selectors in _METADATA_CSS.items():
        for select in selectors:
            if tags := select(tree):
                metadata[key] = tags[0].get('content') or tags[0].get('href') or tags[0].get('lang')
                break

    if 'published_time' in metadata: metadata[CREATED] = parse_date(metadata['published_time'])
    if 'favicon' in metadata: metadata[FAVICON] = full_url(url, metadata['favicon'])
    if 'rss_feed' in metadata: metadata[RSS_FEED] = full_url(url, metadata['rss_feed'])
    return metadata

def _parse_metadata(url: str, html: str) -> dict:
    """Process worker: single lxml parse for metadata only."""
    return _get_metadata(url, lxml.html.fromstring(html))

def _parse_page(url: str, html: str) -> dict:
    """Process worker: parse html into metadata + readable content."""
    from readability import Document

    tree = lxml.html.fromstring(html)
    metadata = _get_metadata(url, tree)
    title = metadata.get("meta_title")
    if not title:
        title_tags = tree.xpath("//title")
        if title_tags:
            title = " ".join(title_tags[0].text_content().split())
    doc = Document(tree, url=url)
    summary_html = doc.summary(html_partial=True)
    body = {
        TITLE: title,
        AUTHOR: metadata.get(AUTHOR),
        CONTENT: strip_html_tags(summary_html)
    }
    body.update(metadata)
    return body

def _convert_pdf_to_markdown(path: str) -> str:
    """Process worker: convert a PDF file to markdown."""
    import pymupdf4llm

    return pymupdf4llm.to_markdown(path, use_ocr=False)

def _title_from_url(url: str) -> str | None:
    try:
        filename = os.path.basename(unquote(urlparse(url).path))
        if filename.lower().endswith(".pdf"):
            filename = filename[:-4]
        return filename.replace("-", " ").replace("_", " ").strip() or None
    except Exception:
        return None

class AsyncWebScraper:
    session: aiohttp.ClientSession = None
    throttle: asyncio.Semaphore = None

    def __init__(self, batch_size: int = BATCH_SIZE):
        self.batch_size = batch_size
        self.throttle = asyncio.Semaphore(batch_size)
        self.parse_throttle = asyncio.Semaphore(PARSE_CONCURRENCY)
        self.parse_pool = ProcessPoolExecutor(max_workers=PARSE_CONCURRENCY)
        self.pdf_pool = ProcessPoolExecutor(max_workers=PDF_CONCURRENCY)
        
    async def __aenter__(self):
        """Async context manager enter"""
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.batch_size, limit_per_host=(self.batch_size>>1) or 1),
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
        self.parse_pool.shutdown(cancel_futures=True)
        self.pdf_pool.shutdown(cancel_futures=True)

    async def _run_in_process(self, pool_name: str, func, *args):
        loop = asyncio.get_running_loop()
        pool = getattr(self, pool_name)
        try:
            return await loop.run_in_executor(pool, func, *args)
        except BrokenProcessPool:
            pool.shutdown(cancel_futures=True)
            max_workers = PDF_CONCURRENCY if pool_name == "pdf_pool" else PARSE_CONCURRENCY
            setattr(self, pool_name, ProcessPoolExecutor(max_workers=max_workers))
            raise

    async def _parse_in_process(self, parser, url: str, html: str) -> dict:
        return await self._run_in_process("parse_pool", parser, url, html)

    async def _convert_pdf_in_process(self, path: str) -> str:
        return await self._run_in_process("pdf_pool", _convert_pdf_to_markdown, path)

    @retry(
        retry=retry_if_exception_type((TimeoutError, aiohttp.ConnectionTimeoutError)),
        stop=stop_after_attempt(RETRY_COUNT),
        wait=wait_random(*RETRY_JITTER),
        reraise=True,
    )
    async def _scrape_html(self, url: str) -> str | None:
        async with self.throttle, self.session.get(url) as response:
            gate = exclude_content(response, html_only=True)
            if gate.excluded:
                return None
            body = await response.content.read(gate.max_size)
            return body.decode(gate.charset, errors="replace")

    @retry(
        retry=retry_if_exception_type((TimeoutError, aiohttp.ConnectionTimeoutError)),
        stop=stop_after_attempt(RETRY_COUNT),
        wait=wait_random(*RETRY_JITTER),
        reraise=True,
    )
    async def _scrape_content_body(self, url: str) -> tuple[str, str, str] | None:
        import tempfile

        async with self.throttle, self.session.get(url) as response:
            gate = exclude_content(response)
            if gate.excluded:
                return None

            if gate.is_pdf:
                body = await response.content.read(gate.max_size + 1)
                if len(body) > gate.max_size:
                    return None
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as pdf:
                    path = pdf.name
                    pdf.write(body)
                return "pdf", gate.url, path

            body = await response.content.read(gate.max_size)
            return "html", gate.url, body.decode(gate.charset, errors="replace")

    @retry(
        retry=retry_if_exception_type((TimeoutError, aiohttp.ConnectionTimeoutError)),
        stop=stop_after_attempt(RETRY_COUNT),
        wait=wait_random(*RETRY_JITTER),
        reraise=True,
    )
    async def _scrape_favicon(self, base_url: str):
        async with self.throttle, self.session.get("https://www.google.com/s2/favicons?domain="+base_url) as response:
            if response.status == 200: return response.headers.get('Content-Location')

    async def _scrape_content(self, url: str):
        if excluded_url(url): return None
        try:
            scraped = await self._scrape_content_body(url)
            if not scraped: return None

            kind, final_url, body = scraped
            if kind == "pdf":
                try:
                    markdown = await self._convert_pdf_in_process(body)
                    if not markdown: return None
                    return {
                        KIND: FINANCIAL_REPORT,
                        TITLE: _title_from_url(final_url) or _title_from_url(url),
                        CONTENT: markdown,
                    }
                finally:
                    try: os.unlink(body)
                    except OSError: pass

            result = await self._parse_in_process(_parse_page, final_url, body)
            del body
            return result
        except Exception as e:
            log.debug(event=f"content scraping failed - {e.__class__.__name__} {e}",
                source=url,
                num_items=1,
                error_type=e.__class__.__name__,
                error_details=str(e),
            )
            return None

    async def _scrape_site(self, base_url: str):
        """Scrape a single site for publisher data."""
        if not base_url:
            return None
        if not base_url.startswith("http"): url = "https://"+base_url
        else: url = base_url

        meta = {
            BASE_URL: base_url,
            SOURCE: extract_domain(url)
        } 
        
        try:                  
            html = await self._scrape_html(url)
            if not html: return None
            meta.update(await self._parse_in_process(_parse_metadata, url, html))
            meta[SITE_NAME] = meta.get(SITE_NAME) or meta.get('meta_title')
            meta[DESCRIPTION] = meta.get(DESCRIPTION)
            meta[SITE_LANGUAGE] = meta.get(LANGUAGE)
            meta.pop(LANGUAGE, None)
            if meta.get(FAVICON): meta[FAVICON] = full_url(base_url, meta[FAVICON])
            else: meta[FAVICON] = await self._scrape_favicon(base_url)            
            if meta.get(RSS_FEED): meta[RSS_FEED] = full_url(base_url, meta[RSS_FEED])
            meta[COLLECTED] = now()
            return meta
        except Exception as e: 
            log.debug(event=f"scraping failed - {e.__class__.__name__} {e}",
                source=base_url,
                num_items=1,
                error_type=e.__class__.__name__,
                error_details=str(e),
            )
            return None

    def _prep_page_result(self, bean: dict, result) -> dict:
        """Prepare result for page scraping (bean and publisher)."""
        if not result:
            return None

        item = {
            **bean,
            KIND: bean.get(KIND) or result.get(KIND),
            TITLE: bean.get(TITLE) or result.get("meta_title") or result.get(TITLE),
            SUMMARY: bean.get(SUMMARY) or result.get("description"),
            CONTENT: result.get(CONTENT),
            AUTHOR: result.get(AUTHOR) or bean.get(AUTHOR),
            ARTICLE_LANGUAGE: result.get(LANGUAGE),
            SITE_LANGUAGE: result.get(LANGUAGE),
            TAGS: [tag.strip() for tag in result.get('keywords', '').split(',')] if result.get('keywords') else None,
            AUTHOR_EMAIL: None,
            CREATED: min(result.get(CREATED) or bean.get(CREATED) or now(), bean.get(COLLECTED)),
            RESTRICTED_CONTENT: True,
            SITE_NAME: result.get('site_name'),
            DESCRIPTION: result.get('description'),
            FAVICON: full_url(extract_base_url(bean.get(URL)), result.get('favicon')) if result.get('favicon') else None,
            RSS_FEED: full_url(extract_base_url(bean.get(URL)), result.get("rss_feed")) if result.get("rss_feed") else None,
            IMAGEURL: full_url(bean.get(URL), result.get("top_image")) if result.get("top_image") else bean.get(IMAGEURL),
        }

        created = result.get(CREATED) or bean.get(CREATED) or bean.get(COLLECTED)
        item[CREATED] = min(created, bean.get(COLLECTED)) if created and bean.get(COLLECTED) else created

        if not item.get(KIND):
            item[KIND] = guess_article_type(item)

        return cleanup_item(item)

    async def scrape_page(self, url: str):
        """Scrape a single URL for both bean and publisher data."""
        result = await self._scrape_content(url)
        if not result:
            return None

        return self._prep_page_result({
            URL: url,
            SOURCE: extract_source(url),
            COLLECTED: now()
        }, result)

    async def scrape_pages(self, urls: list[str]):
        """Scrape multiple URLs in parallel for bean and publisher data."""
        results = await asyncio.gather(*[self._scrape_content(url) for url in urls])
        return [
            self._prep_page_result({
                URL: url,
                SOURCE: extract_source(url),
                COLLECTED: now()
            }, result) for url, result in zip(urls, results)
        ]
    
    async def scrape_beans(self, beans: list[dict]):
        """Augment existing beans with scraped data."""
        return await asyncio.gather(*[self.scrape_bean(bean) for bean in beans])
        
    async def scrape_bean(self, bean: dict):
        """Scrape a single bean for page data."""
        result = await self._scrape_content(bean.get(URL))
        return self._prep_page_result(bean, result)

    async def scrape_site(self, url: str):
        """Scrape a site for publisher data."""
        base_url = extract_base_url(url)
        publisher = await self._scrape_site(base_url)
        return cleanup_item(publisher) if publisher else None

    async def scrape_sites(self, urls: list[str]):
        """Scrape multiple sites for publisher data, deduplicating by base_url."""
        base_urls = {url: extract_base_url(url) for url in urls}
        publishers = await asyncio.gather(*[self._scrape_site(base_url) for base_url in list(set(base_urls.values()))])
        publishers = {publisher.get(BASE_URL): publisher for publisher in publishers if publisher}
        return [cleanup_item(publishers.get(base_urls[url])) if publishers.get(base_urls[url]) else None for url in urls]
  

    async def scrape_publisher(self, publisher: dict):
        """Scrape a single publisher for publisher data."""
        base_url = extract_base_url(publisher.get(BASE_URL))
        result = await self._scrape_site(base_url)
        return cleanup_item(result) if result else None

    async def scrape_publishers(self, publishers: list[dict]):
        """Augment existing publishers with scraped data."""
        base_urls = [publisher.get(BASE_URL) for publisher in publishers]
        scraped = await asyncio.gather(*[self._scrape_site(base_url) for base_url in base_urls])
        return [cleanup_item(scraped[i]) if scraped[i] else None for i in range(len(publishers))]

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
        {"name": "language", "type": "attribute", "selector": "meta[http-equiv='content-language'], meta[name='language'], html", "attribute": "lang"},
        {"name": "keywords", "type": "attribute", "selector": "meta[name='keywords']", "attribute": "content"},
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

class WebCrawler:        
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
        return WebCrawler._package_result(results[url])

    async def scrape_urls(self, urls: list[str], collect_metadata: bool) -> list[dict]:
        """Collects the bodies of the urls as markdowns"""
        results = await self._scrape(urls, collect_metadata)
        return [WebCrawler._package_result(results[url]) for url in urls]    

    async def scrape_beans(self, beans: list[dict], collect_metadata: bool = False) -> list[dict]:
        """Collects the bodies of the beans as markdowns"""
        results = await self.scrape_urls([bean.get(URL) for bean in beans], collect_metadata)
        current_time = now()
        for bean, result in zip(beans, results):
            if not result: continue
            bean[CONTENT] = result.get("markdown")
            bean[TITLE] = bean.get(TITLE) or result.get("title") or result.get("meta_title") # this sequence is important because result['title'] is often crap            
            bean[AUTHOR] = result.get("author") or bean.get(AUTHOR)
            bean[CREATED] = min(result.get("published_time") or bean.get(CREATED) or bean.get(COLLECTED), current_time)
            bean[SUMMARY] = bean.get(SUMMARY) or result.get("description")
            bean[LANGUAGE] = result.get("language")
            bean[TAGS] = [tag.strip() for tag in result.get('keywords', '').split(',')] if result.get('keywords') else None
            bean[AUTHOR_EMAIL] = None
            bean[RESTRICTED_CONTENT] = True
            image_url = result.get("top_image")
            if image_url: bean[IMAGEURL] = full_url(bean.get(URL), image_url)
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
    