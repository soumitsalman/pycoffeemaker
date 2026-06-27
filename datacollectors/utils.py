import re
import os
import tldextract
import lxml.html
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlparse, urlunparse
from dateutil.parser import parse as date_parser
from aiohttp import ClientResponse

USER_AGENT = "Cafecito-Coffeemaker/v0.9.8+https://github.com/soumitsalman/pycoffeemaker"
BROWSER_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
TIMEOUT =  int(os.getenv('COLLECTOR_TIMEOUT', 120)) # 2 minutes
RATELIMIT_WAIT = 300 # 300 seconds / 5 minutes
BATCH_SIZE = int(os.getenv('BATCH_SIZE', os.cpu_count()*os.cpu_count()))
RETRY_COUNT = 3
RETRY_JITTER = (1, 30)
MAX_HTML_SIZE = int(os.getenv("MAX_HTML_SIZE", 4 << 20))
MAX_PDF_SIZE = int(os.getenv("MAX_PDF_SIZE", 16 << 20))

# content types
POST = "post"
BLOG = "blog"
NEWS = "news"
SITE = "site"
PODCAST = "podcast"
CONTRACT = "contract"
FINANCIAL_REPORT = "financial_report"
EARNINGS_REPORT = "earnings_report"
SEC_FILING = "sec_filing"

# fields
URL = "url"
KIND = "kind"
SOURCE = "source"
PLATFORM = "platform"
TITLE = "title"
SUMMARY = "summary"
CONTENT = "content"
AUTHOR = "author"
CREATED = "created"
COLLECTED = "collected"
IMAGEURL = "image_url"
CHATTER_URL = "chatter_url"
BASE_URL = "base_url"
SITE_NAME = "site_name"
DESCRIPTION = "description"
FAVICON = "favicon"
RSS_FEED = "rss_feed"
LIKES = "likes"
COMMENTS = "comments"
FORUM = "forum"
RESTRICTED_CONTENT = "restricted_content"
LANGUAGE = "language"
ARTICLE_LANGUAGE = "article_language"
SITE_LANGUAGE = "site_language"
TAGS = "tags"
AUTHOR_EMAIL = "author_email"

# content type determination heuristics
POST_DOMAINS = {"reddit", "redd", "linkedin", "x", "twitter", "facebook", "ycombinator"}
BLOG_URLS = {"medium.com",  "substack.", "wordpress.", "blogspot.", "newsletter.", "developers.", "blogs.", "blog.", ".so/", ".dev/", ".io/",  ".to/", ".rs/", ".tech/", ".ai/", ".blog/", "/blog/", "/reviews/" }
BLOG_SITENAMES = {"blog", "magazine", "newsletter", "weekly"}
NEWS_SITENAMES = {"daily", "wire", "times", "today",  "news", "the "}
NEWS_TAGS = {"news", "headline", "press release", "announcement"}
BLOG_TAGS = {"blog", "newsletter", "analysis", "opinion", "review"}
PODCAST_SITENAMES = {"podcast", "show", "episode"}
PODCAST_TAGS = {"podcast", "episode", "show"}

# heuristic url patterns to exclude from collection
EXCLUDED_URL_PATTERNS = [
    r'\.(png|jpeg|jpg|gif|webp|mp4|avi|mkv|mp3|wav)$',
    r'(v\.redd\.it|i\.redd\.it|www\.reddit\.com\/gallery|youtube\.com|youtu\.be)',
    r'\/video(s)?\/',
    r'\/image(s)?\/',
    r'://[^/?#]+\.ru(?:[:/?#]|$)',
    r'://[^/?#]+\.su(?:[:/?#]|$)',
    r'(?:^|//|\.)(?:tass\.com|rt\.com|newsru\.com|russia-insider\.com|pravdareport\.com|sputniknews\.com|sputnikglobe\.com)(?:[:/]|$)',
]

HTML_CONTENT_TYPES = (
    "text/html",
    "application/xhtml+xml",
    "text/xml",
    "application/xml",
    "application/rss+xml",
    "application/atom+xml",
    "text/plain",
)
SCRAPABLE_CONTENT_TYPES = HTML_CONTENT_TYPES + ("application/pdf",)

# heuristic invalid author names to exclude
EXCLUDED_AUTHORS = ["[no-author]", "noreply", "hidden", "admin", "isbpostadmin", "unknown", "anonymous"]

def guess_article_type(bean: dict) -> str | None:
    """Heuristically infer bean kind from url, base_url, tags, and source.

    The precedence is:
    1. explicit post domains
    2. blog URL markers
    3. tags
    4. source/site-name hints
    5. /news/ path fallback
    """

    if not bean:
        return None

    url = (bean.get(URL) or "").lower()
    base_url = (bean.get(BASE_URL) or "").lower()
    domain_name = (bean.get(SOURCE) or "").lower()
    site_name = (bean.get(SITE_NAME) or "").lower()
    tags = bean.get(TAGS)

    # 1. explicit post domains (constant-time lookup)
    if domain_name:
        if any(post_domain in domain_name for post_domain in POST_DOMAINS):
            return POST

    # 2. blog URL markers
    if url or base_url:
        if any((blog_url in url) or (blog_url in base_url) for blog_url in BLOG_URLS):
            return BLOG

    # 3. tags
    if tags:
        tags_str = (' '.join(tags)).lower()
        if any(news_tag in tags_str for news_tag in NEWS_TAGS):
            return NEWS
        if any(blog_tag in tags_str for blog_tag in BLOG_TAGS):
            return BLOG

    # 4. source/site-name hints
    if site_name:
        if any(site in site_name for site in BLOG_SITENAMES):
            return BLOG
        if any(site in site_name for site in NEWS_SITENAMES):
            return NEWS

    # 5. /news/ path fallback
    if "/news/" in url:
        return NEWS

    return None

# general utilities
@dataclass(frozen=True)
class ContentGate:
    excluded: bool
    is_pdf: bool
    max_size: int
    url: str
    charset: str

def is_pdf_content(content_type: str | None) -> bool:
    return bool(content_type and content_type.split(";")[0].strip().lower() == "application/pdf")

def is_pdf_url(url: str) -> bool:
    try:
        return urlparse(url).path.lower().endswith(".pdf")
    except Exception:
        return False

def is_pdf(url: str | None = None, content_type: str | None = None) -> bool:
    return is_pdf_content(content_type) or bool(url and is_pdf_url(url))

def exclude_content(response: ClientResponse, *, html_only: bool = False) -> ContentGate:
    """Gate a response before buffering body bytes."""
    url = str(response.url)
    content_type = response.content_type
    content_length = response.content_length
    charset = response.charset or "utf-8"
    is_pdf_doc = is_pdf(url, content_type)
    max_size = MAX_PDF_SIZE if is_pdf_doc else MAX_HTML_SIZE
    allowed = HTML_CONTENT_TYPES if html_only else SCRAPABLE_CONTENT_TYPES

    excluded = excluded_url(url)
    if not excluded and content_type:
        mime = content_type.split(";")[0].strip().lower()
        if mime and mime not in allowed:
            excluded = True
    if not excluded and (content_length or 0) > max_size:
        excluded = True

    return ContentGate(
        excluded=excluded,
        is_pdf=is_pdf_doc,
        max_size=max_size,
        url=url,
        charset=charset,
    )

def excluded_url(url: str) -> bool:
    return (not url) or any(re.search(pattern, url) for pattern in EXCLUDED_URL_PATTERNS)

def extract_base_url(url: str) -> str:
    try: return urlparse(url).netloc
    except: return None

def extract_domain(url: str) -> str:
    try: return tldextract.extract(url).domain
    except: return None

def parse_date(date: str) -> datetime:
    try: return date_parser(date, timezones=["UTC"])
    except: return None

def parse_int(val: str) -> int:
    try: return int(val)
    except: return 0

# XML 1.0 allows tab, LF, CR only among C0 controls; lxml rejects the rest.
_INVALID_XML_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

def sanitize_html_for_xml(html: str) -> str:
    """Strip NULL bytes and C0 control chars that break lxml/readability."""
    if not html:
        return html
    return _INVALID_XML_CHAR_RE.sub("", html)

def strip_html_tags(html):
    if not html: return None
    try: text = lxml.html.fromstring(html).text_content()
    except Exception: text = re.sub(r"<[^>]+>", " ", html)
    return " ".join(text.split())

def full_url(base_url: str, target_url: str) -> str:
    return urljoin(base_url, target_url)

def remove_query_params(url: str) -> str:
    try: return urlunparse(urlparse(url)._replace(query="", fragment=""))
    except: return url

def with_www(url: str) -> str | None:
    """Return the url with a 'www.' host prefix, or None if it already has one / can't be parsed."""
    try:
        parts = urlparse(url)
        if parts.netloc and not parts.netloc.startswith("www."):
            return urlunparse(parts._replace(netloc="www." + parts.netloc))
    except Exception: pass
    return None

now = lambda: datetime.now(timezone.utc)
extract_source = lambda url: (extract_domain(url) or extract_base_url(url)).strip().lower()
count_words = lambda text: min(len(text.split()) if text else 0, (1 << 15) - 1)
cleanup_text = lambda text: text.strip() if text and text.strip() else None
cleanup_author = lambda author: cleanup_text(author) if author and author.lower() not in EXCLUDED_AUTHORS else None

def _distinct_by_key(items, key):
    return list({item.get(key): item for item in items if item and item.get(key)}.values())


def cleanup_item(item: dict) -> dict:
    """Clean up a merged collection item in-place."""
    if not item:
        return item

    for field in (
        URL,
        KIND,
        SOURCE,
        PLATFORM,
        TITLE,
        SUMMARY,
        CONTENT,
        AUTHOR,
        IMAGEURL,
        CHATTER_URL,
        BASE_URL,
        SITE_NAME,
        DESCRIPTION,
        FAVICON,
        RSS_FEED,
        LANGUAGE,
        ARTICLE_LANGUAGE,
        SITE_LANGUAGE,
        AUTHOR_EMAIL,
        FORUM,
    ):
        if field in item:
            item[field] = cleanup_text(item.get(field))

    item[AUTHOR] = cleanup_author(item.get(AUTHOR))
    item[CREATED] = item.get(CREATED) or now()
    item[COLLECTED] = item.get(COLLECTED) or now()
    item[TAGS] = list(set(item.get(TAGS) or []))
    item["title_length"] = count_words(item.get(TITLE))
    item["summary_length"] = count_words(item.get(SUMMARY))
    item["content_length"] = count_words(item.get(CONTENT))

    if not item.get(BASE_URL) and item.get(URL):
        item[BASE_URL] = extract_base_url(item[URL])
    item[BASE_URL] = cleanup_text(item.get(BASE_URL))

    created = item.get(CREATED)
    if created and not getattr(created, "tzinfo", None):
        item[CREATED] = created.replace(tzinfo=timezone.utc)

    return item
