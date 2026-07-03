import os
import json
import asyncio
import pytest
from icecream import ic

from pybeansack.models import Bean, Chatter


def url_to_filename(url: str) -> str:
    import re
    return "./.test/" + re.sub(r"[^a-zA-Z0-9]", "-", url)


def save_json(url, items):
    filename = url_to_filename(url) + ".json"
    with open(filename, "w") as file:
        json.dump(items, file)


def save_models(items: list[Bean | Chatter], file_name: str = None):
    if items:
        with open(f".test/{file_name or items[0].source}.json", "w") as file:
            json.dump(
                [
                    item.model_dump(
                        by_alias=True,
                        exclude_none=True,
                        exclude_unset=True,
                        exclude_defaults=True,
                    )
                    for item in items
                ],
                file,
            )
        return ic(items)


# ──────────────────────────────────────────────────────────────────────
# RSS Feed Collector Tests
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.collect
def test_rss_feed_collector():
    from datacollectors import RSSFeedCollector

    rss_urls = [
        "https://www.ghacks.net/feed/",
        "https://thenewstack.io/feed",
        "https://scitechdaily.com/feed/",
        "https://www.techradar.com/feeds/articletype/news",
        "https://www.geekwire.com/feed/",
        "https://investorplace.com/content-feed/",
        "https://newatlas.com/index.rss",
        "https://www.sec.gov/enforcement-litigation/administrative-proceedings/rss",
    ]

    async def run():
        collector = RSSFeedCollector(batch_size=32)
        async with collector:
            for url in rss_urls:
                items = await collector.collect(url)
                ic(items)
                ic(url, len(items) if items else 0)
                if items:
                    assert all(item.get("url") for item in items), f"Missing URL in items from {url}"
                    assert all(item.get("title") for item in items), f"Missing title in items from {url}"

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.collect
def test_rss_feed_collector_sec():
    from datacollectors import RSSFeedCollector

    sec_urls = [
        "https://www.sec.gov/news/statements.rss",
        "https://www.sec.gov/news/speeches-statements.rss",
    ]

    async def run():
        async with RSSFeedCollector(batch_size=16) as collector:
            for url in sec_urls:
                items = await collector.collect(url)
                ic(items)
                ic(url, len(items) if items else 0)
                if items:
                    assert all(item.get("url") for item in items), f"Missing URL in items from {url}"
                    assert all(item.get("title") for item in items), f"Missing title in items from {url}"
                    assert all((item.get("author") == item.get("summary") or not (item.get("author") or item.get("summary"))) for item in items), f"Author mismatch in items from {url}"

    asyncio.run(run())


# ──────────────────────────────────────────────────────────────────────
# Reddit Collector Tests
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.collect
def test_reddit_collector_json():
    from datacollectors import RedditCollector

    subreddits = ["news", "worldnews", "technology", "programming"]

    async def run():
        async with RedditCollector(batch_size=32) as collector:
            for sub in subreddits:
                items = await collector.collect(sub, mode="json", limit=10)
                ic(items)
                ic(sub, len(items) if items else 0)
                if items:
                    assert all(item.get("url") for item in items), f"Missing URL in items from r/{sub}"
                    assert all(item.get("platform") == "Reddit" for item in items), f"Non-Reddit platform tag in r/{sub}"

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.collect
def test_reddit_collector_rss():
    from datacollectors import RedditCollector

    subreddits = ["news", "worldnews"]

    async def run():
        async with RedditCollector(batch_size=16) as collector:
            for sub in subreddits:
                items = await collector.collect(sub, mode="rss", limit=10)
                ic(items)
                ic(sub, len(items) if items else 0)
                if items:
                    assert all(item.get("url") for item in items), f"Missing URL in items from r/{sub}"

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.collect
def test_reddit_collector_api():
    from datacollectors import RedditCollector

    if not os.getenv("REDDIT_CLIENT_ID"):
        pytest.skip("REDDIT_CLIENT_ID not set; skipping Reddit API test")

    async def run():
        async with RedditCollector(batch_size=16) as collector:
            items = await collector.collect("news", mode="api")
            ic(items)
            ic(len(items) if items else 0)
            if items:
                assert all(item.get("url") for item in items), "Missing URL in API items"

    asyncio.run(run())


# ──────────────────────────────────────────────────────────────────────
# Hacker News Collector Tests
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.collect
def test_hackernews_collector_top():
    from datacollectors import HackerNewsCollector, HACKERNEWS_TOP_STORIES

    async def run():
        async with HackerNewsCollector(batch_size=64) as collector:
            items = await collector.collect([HACKERNEWS_TOP_STORIES])
            ic("top stories", len(items) if items else 0)
            if items:
                assert all(item.get("url") for item in items), "Missing URL in HN items"
                assert all(item.get("platform") == "ycombinator" for item in items), "Non-HN platform tag"

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.collect
def test_hackernews_collector_all():
    from datacollectors import HackerNewsCollector, HACKERNEWS_STORIES_URLS

    async def run():
        async with HackerNewsCollector(batch_size=64) as collector:
            items = await collector.collect(HACKERNEWS_STORIES_URLS)
            ic("all stories", len(items) if items else 0)
            if items:
                assert len(items) > 0, "No HN items collected"

    asyncio.run(run())


# ──────────────────────────────────────────────────────────────────────
# SEC Filing Collector Tests
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.collect
def test_sec_filing_collector():
    from datacollectors import SECFilingCollector

    sec_rss_url = "https://www.sec.gov/Archives/edgar/usgaap.rss.xml"

    async def run():
        async with SECFilingCollector(batch_size=4) as collector:
            items = await collector.collect(sec_rss_url)
            ic(items)
            ic("SEC filings", len(items) if items else 0)
            if items:
                assert all(item.get("kind") == "sec_filing" for item in items), "Non-SEC filing kind"
                assert all(item.get("content") for item in items), "Missing content in SEC filings"

    asyncio.run(run())


# ──────────────────────────────────────────────────────────────────────
# AsyncWebScraper Tests
# ──────────────────────────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.scrape
def test_async_web_scraper_page():
    from datacollectors import AsyncWebScraper

    urls = [
        "https://www.sec.gov/newsroom/speeches-statements/atkins-statement-minimum-pricing-increments-access-fee-caps-061126",
        "https://www.sec.gov/files/litigation/admin/2026/34-105807.pdf"
        "https://newatlas.com/bicycles/amflow-suv-ebike-terrain-dji-motor-avinox-tl-carbon/",
        "https://financebuzz.com/professional-skills-more-valuable-after-60",
        "https://financebuzz.com/avoid-buying-rv-in-retirement",
        "https://financebuzz.com/trader-joes-pantry-items-june-2025",
        "https://www.startupdaily.net/topic/politics-news-analysis/launchvic-marries-into-innovation-victoria/",
        "https://startupwhale.com/how-much-do-onlyfans-models-make/",
    ]

    async def run():
        async with AsyncWebScraper() as scraper:
            for url in urls:
                result = await scraper.scrape_page(url)
                ic(url, result)
                if result:
                    assert result.get("content"), f"Missing content from {url}"
                    assert result.get("url") == url, f"URL mismatch from {url}"

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.scrape
def test_async_web_scraper_pages():
    from datacollectors import AsyncWebScraper

    urls = [
        "https://www.sec.gov/newsroom/speeches-statements/atkins-statement-minimum-pricing-increments-access-fee-caps-061126",
        "https://www.sec.gov/files/litigation/admin/2026/34-105807.pdf",
        "https://newatlas.com/bicycles/amflow-suv-ebike-terrain-dji-motor-avinox-tl-carbon/",
        "https://financebuzz.com/professional-skills-more-valuable-after-60",
    ]

    async def run():
        async with AsyncWebScraper() as scraper:
            results = await scraper.scrape_pages(urls)
            ic(results)
            assert len(results) == len(urls), "Result count mismatch"
            for url, result in zip(urls, results):
                if result:
                    assert result.get("content"), f"Missing content from {url}"

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.scrape
def test_async_web_scraper_sites():
    from datacollectors import AsyncWebScraper

    urls = [
        "https://financebuzz.com/retirees-should-buy-at-bjs-4",
        "https://financebuzz.com/southern-lake-towns-afford-social-security",
        "https://www.cyberark.com/blog/distinguishing-authn-and-authz/",
        "https://aws.amazon.com/blogs/aws/introducing-aws-capabilities-by-region-for-easier-regional-planning-and-faster-global-deployments/",
    ]

    async def run():
        async with AsyncWebScraper() as scraper:
            results = await scraper.scrape_sites(urls)
            ic(len(results))
            assert len(results) == len(urls), "Result count mismatch"
            for url, result in zip(urls, results):
                if result:
                    assert result.get("base_url") or result.get("source"), f"Missing publisher info from {url}"

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.scrapepubs
def test_async_web_scraper_publishers():
    from datacollectors import AsyncWebScraper

    publishers = [
        {"base_url": "financebuzz.com"},
        {"base_url": "cyberark.com"},
        {"base_url": "aws.amazon.com"},
    ]

    async def run():
        async with AsyncWebScraper() as scraper:
            results = await scraper.scrape_publishers(publishers)
            ic(len(results))
            assert len(results) == len(publishers), "Result count mismatch"
            for pub, result in zip(publishers, results):
                ic(pub["base_url"], bool(result))
                if result:
                    assert result.get("base_url"), f"Missing base_url in publisher {pub['base_url']}"

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.scrape
def test_async_web_scraper_bean():
    from datacollectors import AsyncWebScraper
    from utils.fields import URL, CONTENT, COLLECTED
    from pybeansack.models import now

    bean = {
        URL: "https://financebuzz.com/retirees-should-buy-at-bjs-4",
        COLLECTED: now(),
    }

    async def run():
        async with AsyncWebScraper() as scraper:
            result = await scraper.scrape_bean(bean)
            ic(bool(result))
            if result:
                assert result.get(URL), "Missing URL in scraped bean"
                assert result.get(CONTENT), "Missing content in scraped bean"

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.scrape
def test_async_web_scraper_beans():
    from datacollectors import AsyncWebScraper
    from utils.fields import URL, COLLECTED
    from pybeansack.models import now

    beans = [
        {URL: "https://financebuzz.com/retirees-should-buy-at-bjs-4", COLLECTED: now()},
        {URL: "https://financebuzz.com/southern-lake-towns-afford-social-security", COLLECTED: now()},
        {URL: "https://financebuzz.com/professional-skills-more-valuable-after-60", COLLECTED: now()},
    ]

    async def run():
        async with AsyncWebScraper() as scraper:
            results = await scraper.scrape_beans(beans)
            ic(len(results))
            assert len(results) == len(beans), "Result count mismatch"

    asyncio.run(run())


@pytest.mark.integration
@pytest.mark.scrape
def test_async_web_scraper_site():
    from datacollectors import AsyncWebScraper

    base_urls = ["financebuzz.com", "cyberark.com"]

    async def run():
        async with AsyncWebScraper() as scraper:
            for base_url in base_urls:
                result = await scraper.scrape_site(base_url)
                ic(base_url, bool(result))
                if result:
                    assert result.get("base_url"), f"Missing base_url from {base_url}"

    asyncio.run(run())

