__author__ = "Soumit Salman Rahman"
__license__ = "MIT"
__version__ = "1.0.1"

from .apicollectors import APICollector, APICollectorAsync
from .scrapers import WebCrawler, AsyncWebScraper
from .settings import MAX_HTML_SIZE, MAX_PDF_SIZE, TIMEOUT
from .normalize import (
    POST,
    BLOG,
    NEWS,
    SITE,
    PODCAST,
    CONTRACT,
    FINANCIAL_REPORT,
    EARNINGS_REPORT,
    SEC_FILING,
    ContentGate,
    exclude_content,
    excluded_url,
    cleanup_item,
    guess_article_type,
)

__all__ = [
    "APICollector",
    "APICollectorAsync",
    "WebCrawler",
    "AsyncWebScraper",
    "POST",
    "BLOG",
    "NEWS",
    "SITE",
    "PODCAST",
    "CONTRACT",
    "FINANCIAL_REPORT",
    "EARNINGS_REPORT",
    "SEC_FILING",
    "ContentGate",
    "exclude_content",
    "excluded_url",
    "cleanup_item",
    "guess_article_type",
    "MAX_HTML_SIZE",
    "MAX_PDF_SIZE",
    "TIMEOUT",
]
