# datacollectors

Ingest web content from RSS feeds, Reddit, Hacker News, and HTML pages. All collectors return **normalized dicts** sharing field names from `utils.py` (re-exported in `datacollectors/__init__.py`). Items are passed through `cleanup_item()` before return.

Production use: `workers/collectororch.py` runs `APICollectorAsync` for feeds/APIs and `AsyncWebScraper` for pages that need a full body. `WebCrawler` (crawl4ai) is available for heavier JS/markdown extraction (tests, optional deployments).

RSS field mapping notes: [DATAFIELDS.md](DATAFIELDS.md).

## Field constants (`utils.py`)

Import from `datacollectors` or `datacollectors.utils`.

| Constant | Key | Role |
|----------|-----|------|
| `URL` | `url` | Article or post link (bean id) |
| `KIND` | `kind` | Content type (see below) |
| `SOURCE` | `source` | Domain label (e.g. `nytimes`) |
| `PLATFORM` | `platform` | Social platform when chatter is present |
| `TITLE`, `SUMMARY`, `CONTENT` | … | Text fields |
| `AUTHOR`, `AUTHOR_EMAIL` | … | Byline |
| `CREATED`, `COLLECTED` | … | UTC datetimes |
| `IMAGEURL` | `image_url` | Lead image |
| `BASE_URL`, `SITE_NAME`, `DESCRIPTION`, `FAVICON`, `RSS_FEED` | … | Publisher/site metadata |
| `CHATTER_URL`, `LIKES`, `COMMENTS`, `FORUM` | … | Engagement (often on same dict as bean) |
| `TAGS` | `tags` | List of strings |
| `LANGUAGE`, `ARTICLE_LANGUAGE`, `SITE_LANGUAGE` | … | Locale hints |
| `RESTRICTED_CONTENT` | `restricted_content` | Set when body came from scrape (paywall heuristic) |

**`kind` values:** `post`, `blog`, `news`, `site`, `podcast`, `contract`, `financial_report`, `earnings_report`, `sec_filing`. RSS/HN/Reddit items get `guess_article_type()` when not explicit.

**Derived fields** (added by `cleanup_item`): `title_length`, `summary_length`, `content_length`; `base_url` inferred from `url` if missing.

### Utilities

| Function | Purpose |
|----------|---------|
| `cleanup_item(item)` | Strip text, normalize tags/dates, set lengths; in-place |
| `guess_article_type(bean)` | Infer `kind` from URL, source, tags, site name |
| `excluded_url(url)` | Skip media/gallery/video URLs |
| `extract_base_url`, `extract_source`, `parse_date`, `strip_html_tags` | Shared parsing helpers |

## Output format

Each collector returns `list[dict]` (or `None` on empty/failure). One dict is one **collection item**; it may combine bean fields and chatter fields (e.g. RSS entry with comment count).

### Core bean fields (typical)

```python
{
    "url": "https://example.com/article",
    "base_url": "example.com",
    "source": "example",
    "kind": "news",
    "title": "...",
    "summary": "...",           # plain text
    "content": "...",           # plain text; may be short from RSS
    "author": "...",
    "author_email": None,
    "created": datetime(..., tzinfo=UTC),
    "collected": datetime(..., tzinfo=UTC),
    "tags": ["ai", "policy"],
    "image_url": "https://...",
    "article_language": "en",
    "site_language": "en",
    "title_length": 12,
    "summary_length": 40,
    "content_length": 800,
}
```

### Chatter fields (when present)

Same dict may also include:

```python
{
    "chatter_url": "https://...",  # discussion thread
    "platform": "reddit",          # or source domain for RSS comments
    "likes": 42,
    "comments": 7,
    "forum": "r/python",           # subreddit or HN item id
}
```

### Publisher-only scrape (`scrape_site` / `scrape_publishers`)

```python
{
    "base_url": "example.com",
    "source": "example",
    "site_name": "...",
    "description": "...",
    "favicon": "https://...",
    "rss_feed": "https://.../feed",
    "site_language": "en",
    "collected": datetime(...),
}
```

### `WebCrawler` scrape result (`_package_result`)

Internal shape before merging into beans:

```python
{
    "url": "...",
    "markdown": "...",       # cleaned article text
    "title", "meta_title", "description", "author",
    "published_time", "top_image", "keywords", "language", ...
}
```

`scrape_beans()` writes `content` from `markdown` and copies metadata onto the input bean dicts.

---

## `APICollector` / `APICollectorAsync`

Fetch structured feeds and APIs. **Async** variant is what the collector worker uses (`async with APICollectorAsync(batch_size) as col:`).

| Method | Returns | Description |
|--------|---------|-------------|
| `collect_rssfeed(url, default_kind=NEWS)` | `list[dict]` | Parse RSS/Atom via feedparser; build items with `_build_rss_item` |
| `collect_rssfeeds(urls)` | `list[dict]` | Sync only: thread-pool over `collect_rssfeed` |
| `collect_subreddit(name, default_kind=NEWS)` | `list[dict]` | Hot posts (limit 25); self-posts vs link posts |
| `collect_ychackernews(stories_urls=...)` | `list[dict]` | Top/new/ask/show story lists → item metadata per story |

**Env:** `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET` for Reddit.

**Behavior:**

- URLs matching `excluded_url()` are dropped.
- Link posts: external URL is the bean; Reddit permalink is `chatter_url` with scores/comments.
- HN: external URL when present; else discussion URL as `post`.
- RSS: optional comment RSS / slash_comments → chatter fields on the same item.

### Example (async)

```python
import asyncio
from datacollectors import APICollectorAsync, NEWS

async def main():
    async with APICollectorAsync(batch_size=32) as col:
        items = await col.collect_rssfeed("https://hnrss.org/frontpage", default_kind=NEWS)
        hn = await col.collect_ychackernews()
        reddit = await col.collect_subreddit("python")
    print(len(items), len(hn), len(reddit))

asyncio.run(main())
```

### Example (sync)

```python
from datacollectors import APICollector

collector = APICollector()
feeds = collector.collect_rssfeeds(["https://example.com/feed.xml"])
```

### Feed config (collector)

`factory/feeds.yaml` lists sources by type:

```yaml
sources:
  rss: [ "https://...", ... ]
  reddit: [ "python", "machinelearning", ... ]
  ychackernews: [ ]   # optional URL list; default story endpoints used if empty
```

---

## `AsyncWebScraper`

Lightweight HTML fetch + **readability** article extraction. Used when RSS/API body is too short or publisher metadata is missing.

**Context manager required** (`async with AsyncWebScraper(batch_size) as scraper:`).

| Method | Returns | Description |
|--------|---------|-------------|
| `scrape_page(url)` | `dict` | New bean dict for one URL |
| `scrape_pages(urls)` | `list[dict]` | Parallel pages |
| `scrape_bean(bean)` | `dict` | Merge scrape into existing bean (`CONTENT`, title, etc.) |
| `scrape_beans(beans)` | `list[dict]` | Parallel `scrape_bean` |
| `scrape_site(url)` | `dict` | Publisher metadata for `base_url` |
| `scrape_sites(urls)` | `list[dict]` | Dedupe by base URL |
| `scrape_publisher(pub)` / `scrape_publishers(pubs)` | `dict` / `list` | Fill publisher fields |

Sets `restricted_content=True` on scraped articles. Uses og/meta tags for title, description, image, RSS link, favicon (Google favicon API fallback).

### Example

```python
import asyncio
from datacollectors import AsyncWebScraper

async def main():
    async with AsyncWebScraper(batch_size=8) as scraper:
        bean = await scraper.scrape_page("https://example.com/post")
        beans = await scraper.scrape_beans([
            {"url": "https://example.com/a", "source": "example", "collected": ...},
        ])

asyncio.run(main())
```

---

## `WebCrawler`

**crawl4ai**-based crawler: markdown body + CSS-extracted metadata. Supports local `AsyncWebCrawler` or remote `Crawl4aiDockerClient` (`remote_crawler` URL, e.g. docker-compose `localcrawler`).

| Method | Returns | Description |
|--------|---------|-------------|
| `scrape_url(url, collect_metadata=True)` | `dict` | Single page; metadata via JSON-CSS strategy when `True` |
| `scrape_urls(urls, collect_metadata)` | `list[dict]` | Batch |
| `scrape_beans(beans, collect_metadata=False)` | `list[dict]` | Updates beans in place: `content` ← markdown, enriches title/author/tags |

`collect_metadata=True` uses a separate crawl config (metadata extraction, lighter markdown). `False` targets article selectors (`article`, `main`, `.article-body`, …).

### Example

```python
import asyncio
from datacollectors import WebCrawler

async def main():
    crawler = WebCrawler(remote_crawler="http://localhost:11235", batch_size=4)
    result = await crawler.scrape_url("https://example.com/article", collect_metadata=True)
    beans = [{"url": "https://example.com/article", "title": "..."}]
    await crawler.scrape_beans(beans, collect_metadata=False)

asyncio.run(main())
```

Requires `crawl4ai` (and optional Docker service for remote mode).

---

## End-to-end flow (collector worker)

1. **Collect** — `APICollectorAsync` pulls RSS / Reddit / HN → list of dicts.
2. **Triage** — Split each item into bean, chatter, publisher (`collectororch._split_item`).
3. **Cache** — Beans/publishers with enough inline content go to state cache as `collected`; thin beans and incomplete publishers are queued.
4. **Scrape** — `AsyncWebScraper.scrape_beans` / `scrape_publishers` fills bodies and site metadata, then cache again.

Threshold: `WORDS_THRESHOLD_FOR_STORING` (default 160) — shorter bodies are scraped unless `kind` is `post`.

## Choosing a scraper

| Need | Use |
|------|-----|
| RSS, Reddit, HN | `APICollectorAsync` |
| Fast HTML, readability, collector default | `AsyncWebScraper` |
| JS-heavy sites, markdown pipeline, batch crawl4ai | `WebCrawler` |

## See also

- [DATAFIELDS.md](DATAFIELDS.md) — feedparser field fallbacks for RSS
- `factory/feeds.yaml` — source lists
- `tests/test.py` — `test_collect_*`, `test_scrape_*` examples
