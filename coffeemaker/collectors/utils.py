import re
import os
import warnings
import tldextract
from datetime import datetime, timezone
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from urllib.parse import urljoin, urlparse
from dateutil.parser import parse as date_parser
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

USER_AGENT = "Cafecito-Coffeemaker/v0.4.0+https://github.com/soumitsalman/pycoffeemaker"
TIMEOUT =  int(os.getenv('TIMEOUT', 60)) # 1 minute
RATELIMIT_WAIT = 600 # 600 seconds / 10 minutes

# content types
POST = "post"
BLOG = "blog"
NEWS = "news"
CONTRACT = "contract"
FINANCIAL_REPORT = "financial_report"
EARNINGS_REPORT = "earnings_report"
SEC_FILING = "sec_filing"

# fields
URL = "url"
KIND = "kind"
SOURCE = "source"
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
TAGS = "tags"
AUTHOR_EMAIL = "author_email"

# content type determination heuristics
POST_DOMAINS = {"reddit", "redd", "linkedin", "x", "twitter", "facebook", "ycombinator"}
BLOG_URLS = {"medium.com",  "substack.", "wordpress.", "blogspot.", "newsletter.", "developers.", "blogs.", "blog.", ".so/", ".dev/", ".io/",  ".to/", ".rs/", ".tech/", ".ai/", ".blog/", "/blog/", "/reviews/" }
BLOG_SITENAMES = {"blog", "magazine", "newsletter", "weekly"}
NEWS_SITENAMES = {"daily", "wire", "times", "today",  "news", "the "}
NEWS_TAGS = {"news", "headline", "press release", "announcement"}
BLOG_TAGS = {"blog", "newsletter", "analysis", "opinion", "review"}

# heuristic url patterns to exclude from collection
EXCLUDED_URL_PATTERNS = [
    r'\.(png|jpeg|jpg|gif|webp|mp4|avi|mkv|mp3|wav|pdf)$',
    r'(v\.redd\.it|i\.redd\.it|www\.reddit\.com\/gallery|youtube\.com|youtu\.be)',
    r'\/video(s)?\/',
    r'\/image(s)?\/',
]

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

    url = bean.get(URL, "").lower()
    base_url = bean.get(BASE_URL, "").lower()
    domain_name = bean.get(SOURCE, "").lower()
    site_name = bean.get(SITE_NAME, "").lower()
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
def excluded_url(url: str):
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

def strip_html_tags(html):
    if html: return BeautifulSoup(html, "lxml").get_text(separator=" ", strip=True)

def full_url(base_url: str, target_url: str) -> str:
    return urljoin(base_url, target_url)

now = lambda: datetime.now(timezone.utc)
extract_source = lambda url: (extract_domain(url) or extract_base_url(url)).strip().lower()
count_words = lambda text: min(len(text.split()) if text else 0, (1 << 15) - 1)
cleanup_text = lambda text: text.strip() if text and text.strip() else None
cleanup_author = lambda author: cleanup_text(author) if author and author.lower() not in EXCLUDED_AUTHORS else None

def _distinct_by_key(items, key):
    return list({item.get(key): item for item in items if item and item.get(key)}.values())

def validate_bean_item(item: dict) -> bool:
    if not item: return False
    return bool(item.get(TITLE) and item.get(COLLECTED) and item.get(CREATED) and item.get(SOURCE) and item.get(KIND))

def validate_chatter_item(item: dict) -> bool:
    if not item: return False
    return bool(item.get(CHATTER_URL) and item.get(URL) and (item.get(LIKES) or item.get(COMMENTS) or item.get("subscribers")))

def validate_source_item(item: dict) -> bool:
    if not item: return False
    return bool(item.get(SOURCE) and item.get(BASE_URL))

# def cleanup_beans(items: list[dict]) -> list[dict]:
#     if not items: return items

#     for item in items:
#         cleanup_bean_item(item)

#     items = _distinct_by_key(items, URL)
#     return list(filter(validate_bean_item, items))

# def cleanup_sources(items: list[dict]) -> list[dict]:
#     if not items: return items

#     for item in items:
#         cleanup_source_item(item)

#     items = _distinct_by_key(items, SOURCE)
#     return list(filter(validate_source_item, items))

# def cleanup_chatters(items: list[dict]) -> list[dict]:
#     if not items: return items

#     for item in items:
#         cleanup_chatter_item(item)

#     return list(filter(validate_chatter_item, items))


def cleanup_bean_item(item: dict) -> dict:
    """Clean up a single bean item in-place."""
    if not item:
        return item

    item[URL] = cleanup_text(item.get(URL))
    item[KIND] = cleanup_text(item.get(KIND))
    item[SOURCE] = cleanup_text(item.get(SOURCE))
    item[TITLE] = cleanup_text(item.get(TITLE))
    item["title_length"] = count_words(item.get(TITLE))
    item[SUMMARY] = cleanup_text(item.get(SUMMARY))
    item["summary_length"] = count_words(item.get(SUMMARY))
    item[CONTENT] = cleanup_text(item.get(CONTENT))
    item["content_length"] = count_words(item.get(CONTENT))
    item[AUTHOR] = cleanup_text(item.get(AUTHOR))
    item[IMAGEURL] = cleanup_text(item.get(IMAGEURL))
    item[CREATED] = item.get(CREATED) or now()
    item[COLLECTED] = item.get(COLLECTED) or now()
    item[AUTHOR] = cleanup_author(item.get(AUTHOR))
    item[LANGUAGE] = cleanup_text(item.get(LANGUAGE))
    item[TAGS] = list(set(item.get(TAGS) or []))
    item[AUTHOR_EMAIL] = cleanup_text(item.get(AUTHOR_EMAIL))
    # enrich BASE_URL from URL if missing
    if not item.get(BASE_URL) and item.get(URL):
        item[BASE_URL] = extract_base_url(item[URL])
    item[BASE_URL] = cleanup_text(item.get(BASE_URL))
    created = item.get(CREATED)
    if created and not getattr(created, "tzinfo", None):
        item[CREATED] = created.replace(tzinfo=timezone.utc)

    return item


def cleanup_chatter_item(item: dict) -> dict:
    """Clean up a single chatter item in-place."""
    if not item:
        return item

    item[CHATTER_URL] = cleanup_text(item.get(CHATTER_URL))
    item[URL] = cleanup_text(item.get(URL))
    item[FORUM] = cleanup_text(item.get(FORUM))
    item[SOURCE] = cleanup_text(item.get(SOURCE))

    return item


def cleanup_source_item(item: dict) -> dict:
    """Clean up a single source item in-place."""
    if not item:
        return item

    item[SOURCE] = cleanup_text(item.get(SOURCE))
    item[BASE_URL] = cleanup_text(item.get(BASE_URL))
    item[FAVICON] = cleanup_text(item.get(FAVICON))
    item[RSS_FEED] = cleanup_text(item.get(RSS_FEED))
    item[DESCRIPTION] = cleanup_text(item.get(DESCRIPTION))
    item[SITE_NAME] = cleanup_text(item.get(SITE_NAME))
    item[COLLECTED] = item.get(COLLECTED) or now()

    return item
