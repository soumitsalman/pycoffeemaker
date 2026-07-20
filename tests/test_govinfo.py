import asyncio
import aiohttp
import random
from collections import Counter
from pathlib import Path

import feedparser
import pytest
import yaml

from datacollectors import AsyncWebScraper, GovInfoRSSCollector
from utils.fields import BASE_URL, RSS_FEED, SOURCE, TAGS, TITLE, URL


PACKAGE_ID = "PKG-123"
HTML_URL = f"https://www.govinfo.gov/content/pkg/{PACKAGE_ID}/html/{PACKAGE_ID}.htm"
PDF_URL = f"https://www.govinfo.gov/content/pkg/{PACKAGE_ID}/pdf/{PACKAGE_ID}.pdf"


def _entry(description: str) -> feedparser.FeedParserDict:
    return feedparser.FeedParserDict({
        "guid": PACKAGE_ID,
        "title": "Example package",
        "description": description,
        "tags": [{"term": "Bills and Statutes"}],
    })


class _ProbeResponse:
    def __init__(self, status):
        self.status = status
        self.read_called = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class _ProbeSession:
    def __init__(self, head_results, get_results=None):
        self.head_results = iter(head_results)
        self.get_results = iter(get_results or [])
        self.head_calls = []
        self.get_calls = []

    def head(self, url, **kwargs):
        self.head_calls.append((url, kwargs))
        result = next(self.head_results)
        if isinstance(result, BaseException):
            raise result
        return _ProbeResponse(result)

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        result = next(self.get_results)
        if isinstance(result, BaseException):
            raise result
        return _ProbeResponse(result)


def _run_probe(collector, package_id=PACKAGE_ID):
    return asyncio.run(collector._probe_guid_content_url(package_id))


def test_govinfo_selects_html_before_pdf():
    entry = _entry(f'<a href="{PDF_URL}">PDF</a><a href="{HTML_URL}">TEXT</a>')

    assert GovInfoRSSCollector._select_content_url(entry) == HTML_URL


def test_govinfo_selects_pdf_when_text_content_is_unavailable():
    entry = _entry(f'<a href="{PDF_URL}">PDF</a>')

    assert GovInfoRSSCollector._select_content_url(entry) == PDF_URL


def test_govinfo_guid_candidates_are_ordered_by_content_preference():
    assert GovInfoRSSCollector._guid_content_urls(PACKAGE_ID) == [
        f"https://www.govinfo.gov/content/pkg/{PACKAGE_ID}/html/{PACKAGE_ID}.htm",
        f"https://www.govinfo.gov/content/pkg/{PACKAGE_ID}/html/{PACKAGE_ID}.html",
        f"https://www.govinfo.gov/content/pkg/{PACKAGE_ID}/text/{PACKAGE_ID}.txt",
        PDF_URL,
    ]


def test_govinfo_probe_uses_html_before_pdf():
    collector = GovInfoRSSCollector(batch_size=1)
    session = _ProbeSession([404, 200])
    collector.session = session

    assert _run_probe(collector) == GovInfoRSSCollector._guid_content_urls(PACKAGE_ID)[1]
    assert [url for url, _ in session.head_calls] == GovInfoRSSCollector._guid_content_urls(PACKAGE_ID)[:2]


def test_govinfo_probe_falls_back_to_pdf():
    collector = GovInfoRSSCollector(batch_size=1)
    session = _ProbeSession([404, 404, 404, 200])
    collector.session = session

    assert _run_probe(collector) == PDF_URL
    assert len(session.head_calls) == 4


@pytest.mark.parametrize("head_status", [405, 501])
def test_govinfo_probe_uses_get_when_head_is_not_supported(head_status):
    collector = GovInfoRSSCollector(batch_size=1)
    session = _ProbeSession([head_status], [200])
    collector.session = session

    assert _run_probe(collector) == GovInfoRSSCollector._guid_content_urls(PACKAGE_ID)[0]
    assert len(session.get_calls) == 1


def test_govinfo_probe_continues_after_request_errors():
    collector = GovInfoRSSCollector(batch_size=1)
    session = _ProbeSession([aiohttp.ClientError(), 404, 404, 200])
    collector.session = session

    assert _run_probe(collector) == PDF_URL


def test_govinfo_fallback_does_not_probe_when_description_has_a_link():
    feed = feedparser.FeedParserDict({
        "feed": {"link": "https://www.govinfo.gov/app/collection/test"},
        "entries": [_entry(f'<a href="{PDF_URL}">PDF</a>')],
    })
    collector = GovInfoRSSCollector(batch_size=1)
    session = _ProbeSession([])
    collector.session = session

    items = asyncio.run(collector._extract_entries_with_fallback(
        feed, "https://www.govinfo.gov/rss/test.xml", feed["feed"]["link"]
    ))

    assert items[0][URL] == PDF_URL
    assert session.head_calls == []


def test_govinfo_fallback_builds_item_from_guid_url():
    feed = feedparser.FeedParserDict({
        "feed": {"link": "https://www.govinfo.gov/app/collection/test"},
        "entries": [_entry("")],
    })
    collector = GovInfoRSSCollector(batch_size=1)
    collector.session = _ProbeSession([404, 404, 404, 200])

    items = asyncio.run(collector._extract_entries_with_fallback(
        feed, "https://www.govinfo.gov/rss/test.xml", feed["feed"]["link"]
    ))

    assert items[0][URL] == PDF_URL
    assert f"/content/pkg/{PACKAGE_ID}/pdf/{PACKAGE_ID}.pdf" in items[0][URL]


def test_govinfo_fallback_skips_entries_without_available_content():
    feed = feedparser.FeedParserDict({
        "feed": {"link": "https://www.govinfo.gov/app/collection/test"},
        "entries": [_entry("")],
    })
    collector = GovInfoRSSCollector(batch_size=1)
    collector.session = _ProbeSession([404, 404, 404, 404])

    items = asyncio.run(collector._extract_entries_with_fallback(
        feed, "https://www.govinfo.gov/rss/test.xml", feed["feed"]["link"]
    ))

    assert items == []


@pytest.mark.parametrize("href", [
    f"https://www.govinfo.gov/content/pkg/OTHER/html/OTHER.htm",
    f"https://www.govinfo.gov/content/pkg/{PACKAGE_ID}/xml/{PACKAGE_ID}.xml",
    f"https://www.govinfo.gov/content/pkg/{PACKAGE_ID}.zip",
    f"https://example.com/content/pkg/{PACKAGE_ID}/html/{PACKAGE_ID}.htm",
])
def test_govinfo_rejects_unsupported_or_mismatched_links(href):
    assert GovInfoRSSCollector._select_content_url(_entry(f'<a href="{href}">file</a>')) is None


def test_govinfo_items_use_the_package_content_url():
    feed_url = "https://www.govinfo.gov/rss/bills.xml"
    feed = feedparser.FeedParserDict({
        "feed": {"link": "https://www.govinfo.gov/app/collection/bills"},
        "entries": [_entry(f'<a href="{PDF_URL}">PDF</a><a href="{HTML_URL}">TEXT</a>')],
    })

    items = GovInfoRSSCollector._extract_entries(feed, feed_url, feed["feed"]["link"])

    assert len(items) == 1
    assert items[0][URL] == HTML_URL
    assert items[0][TITLE] == "Example package"
    assert items[0][SOURCE] == "govinfo"
    assert items[0][BASE_URL] == "www.govinfo.gov"
    assert items[0][RSS_FEED] == feed_url
    assert items[0][TAGS] == ["bills and statutes"]


def test_govinfo_feed_config_contains_all_document_feeds():
    sources = yaml.safe_load(Path("factory/feeds.yaml").read_text())["sources"]
    urls = sources["govinfo"]

    assert len(urls) == 199
    assert len(set(urls)) == 199
    assert all(url.startswith("https://www.govinfo.gov/rss/") for url in urls)
    assert not any("bulkdata" in url or "batch" in url for url in urls)


@pytest.mark.integration
@pytest.mark.collect
@pytest.mark.parametrize('feed', [
    "https://www.govinfo.gov/rss/fr.xml",
    "https://www.govinfo.gov/rss/bills.xml",
    "https://www.govinfo.gov/rss/uscode.xml"
])
def test_govinfo_collector_and_scraper(feed):
    async def run():
        async with GovInfoRSSCollector(batch_size=4) as collector:
            items = await collector.collect(feed)

        assert items
        assert all("https://www.govinfo.gov/content/pkg/" in item[URL] for item in items)
        item = items[0]

        async with AsyncWebScraper(batch_size=4) as scraper:
            scraped = await scraper.scrape_bean(item)
        assert scraped and scraped.get("content")
        print(f"\nGovInfo scraped URL: {item[URL]}")
        print(f"GovInfo content preview (first 300 chars):\n{scraped['content'][:300]}")

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.collect
def test_govinfo_random_feed_links():
    feeds = yaml.safe_load(Path("factory/feeds.yaml").read_text())["sources"]["govinfo"]
    selected_feeds = random.sample(feeds, 3)

    async def run():
        async with GovInfoRSSCollector(batch_size=4) as collector:
            for feed in selected_feeds:
                items = await collector.collect(feed) or []
                links = [item[URL] for item in items]
                counts = Counter(
                    link.rsplit("/", 1)[-1].rsplit(".", 1)[-1].lower()
                    for link in links
                )
                extension_counts = {
                    extension: counts.get(extension, 0)
                    for extension in ("htm", "html", "txt", "pdf")
                }
                print(f"\nGovInfo feed: {feed}")
                print(f"GovInfo extension counts: {extension_counts}")
                print("GovInfo links:\n", "\n".join(links))
                assert all(link.startswith("https://www.govinfo.gov/content/pkg/") for link in links)
                assert sum(extension_counts.values()) == len(links)

    asyncio.run(run())
