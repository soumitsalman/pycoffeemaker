import re
import os
import warnings
import tldextract
from datetime import datetime, timezone
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from urllib.parse import urlparse
from dateutil.parser import parse as date_parser
from coffeemaker.pybeansack.models import NEWS, POST, BLOG

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

USER_AGENT = "Cafecito-Coffeemaker/v0.4.0+https://github.com/soumitsalman/pycoffeemaker"
TIMEOUT =  int(os.getenv('TIMEOUT', 60)) # 1 minute
RATELIMIT_WAIT = 600 # 600 seconds / 10 minutes

POST_DOMAINS = ["reddit", "redd", "linkedin", "x", "twitter", "facebook", "ycombinator"]
BLOG_URLS = ["medium.com",  "substack.", "wordpress.", "blogspot.", "newsletter.", "developers.", "blogs.", "blog.", ".so/", ".dev/", ".io/",  ".to/", ".rs/", ".tech/", ".ai/", ".blog/", "/blog/" ]
BLOG_SITENAMES = ["blog", "magazine", "newsletter", "weekly"]
NEWS_SITENAMES = ["daily", "wire", "times", "today",  "news", "the "]

def guess_article_type(url: str, source: str) -> str:
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

EXCLUDED_URL_PATTERNS = [
    r'\.(png|jpeg|jpg|gif|webp|mp4|avi|mkv|mp3|wav|pdf)$',
    r'(v\.redd\.it|i\.redd\.it|www\.reddit\.com\/gallery|youtube\.com|youtu\.be)',
    r'\/video(s)?\/',
    r'\/image(s)?\/',
] 
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

extract_source = lambda url: (extract_domain(url) or extract_base_url(url)).strip().lower()
now = lambda: datetime.now(timezone.utc)