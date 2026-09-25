# Feedly-parity collector design (P0)

**Project:** pycoffeemaker  
**Date:** 2026-09-18  
**Goal:** Collect and parse a Feedly-like multi-source dataset, reusing existing `Collector` / `processingcache` / NLP stages.

Evidence: Feedly public docs + local inventory of `factory/feeds.yaml`, `workers/collectororch.py`, `datacollectors/`, `nlp/`.

---

## Current baseline (keep)

| Group | ~Count | Path |
|---|---|---|
| `rss` / `rss_news` / `rss_blogs` / `rss_press_releases` | ~13.8k | `RSSFeedCollector` + `KindPolicy` |
| `govinfo` | 199 | `GovInfoRSSCollector` |
| `sec_edgar` | 3 | `SECFilingCollector` |
| `reddit` | 350 | `RedditCollector` |
| `ychackernews` | 5 | `HackerNewsCollector` |
| HTML body | — | `AsyncWebScraper` → scrape queue |

`parse_sources()` already folds RSS groups into `parsed["rss"]` as `(url, KindPolicy)` jobs; non-RSS groups stay as lists. New source kinds should follow that pattern.

Kinds already include `podcast`, `blog`, `news`, `research_paper`, etc. No `newsletter` kind yet — recommend adding `NEWSLETTER = "newsletter"` to `utils/kinds.py` (or map newsletters → `blog` with `platform=email` short-term).

---

## P0 deliverables (build first)

### 1. Newsletter ingest

**Feedly pattern:** dedicated inbox address; subscribe or forward; MIME → article.

**Design:**

```
datacollectors/newsletter.py
  NewsletterCollector(APICollectorBase)
    collect(mailbox_config) -> list[dict]  # normalized beans

workers/collectororch.py
  sources group: newsletters: [{imap|maildir|s3_prefix, ...}]
```

**Bean mapping:**

| Bean field | From email |
|---|---|
| `url` | `Message-ID` URL-safe id, or `List-Unsubscribe`/canonical link if present |
| `title` | Subject |
| `content` / `summary` | text/html → markdown (reuse `html_to_markdown`) |
| `author` | From display name |
| `created` | Date header |
| `source` / `base_url` | From domain or `List-Id` |
| `kind` | `newsletter` (new) or `blog` |
| `platform` | `email` |
| `tags` | List-Id / campaign headers when present |

**Config (`feeds.yaml`):**

```yaml
sources:
  newsletters:
    - id: cafecito-inbox
      transport: imap   # or maildir | s3
      host: ${NEWSLETTER_IMAP_HOST}
      folder: INBOX
      mark_seen: true
```

**Orchestration:** In `Collector.run`, for group `newsletters` call `NewsletterCollector` → `_triage` (same as RSS). Short bodies still hit scrape queue if a canonical HTTPS link exists in the body.

**Done when:** A test mailbox with 3 newsletter messages yields 3 cached beans with non-empty markdown content and stable ids across re-runs (idempotent on Message-ID).

---

### 2. RSS discovery + RSS Builder

**Feedly pattern:** paste site URL → detect feeds; if none, build feed from list pages.

**Design:**

```
datacollectors/rss_discovery.py
  discover_feeds(site_url) -> list[FeedCandidate]
    # <link rel="alternate" type="application/rss+xml|atom">
    # well-known /feed /rss /atom.xml /index.xml
    # YouTube channel_id → videos.xml

  build_feed_from_list_page(list_url, *, max_items=50) -> list[dict]
    # reuse AsyncWebScraper / readability
    # extract article links + titles + dates from index/archive pages
    # return same normalized dicts as RSSFeedCollector
```

**New `feeds.yaml` groups:**

```yaml
sources:
  sites:                    # URL paste / discover
    - https://example.com/
  site_lists:               # explicit RSS Builder targets
    - https://example.com/blog/
  youtube:
    - UCxxxxxxxx            # channel id
    # or full channel URL
  podcasts:
    - https://feeds.example.com/show.xml
```

**Orchestration:**

1. `sites` → `discover_feeds` → enqueue discovered URLs into the RSS job list with policy from optional overlay (default `rss` / unknown).
2. `site_lists` → `build_feed_from_list_page` → `_triage` directly (no persistent RSS file required; optional: write generated Atom to object storage for debugging).
3. `youtube` → normalize to `https://www.youtube.com/feeds/videos.xml?channel_id=…`, policy `non_news` + `kind_hint=podcast` (or keep `post` if you prefer video-as-post).
4. `podcasts` → same RSS path with `kind_hint=podcast`; persist enclosure URL in a new optional field `media_url` (add to fields if missing; otherwise stash in `tags`/`image_url` only as last resort — prefer real field).

**Done when:**

- Given `https://www.theverge.com`, discovery returns ≥1 feed and collector ingests items.
- Given a blog index with no RSS, Builder returns ≥5 article beans with urls/titles.

---

### 3. Keyword alerts (thin P0.5)

**Feedly pattern:** boolean keyword monitoring.

**Minimal path (no web-scale index):** generate Google News / Bing News RSS URLs from queries:

```yaml
sources:
  keyword_alerts:
    - query: '"product launch" AND (OpenAI OR Anthropic)'
      engine: google_news  # → https://news.google.com/rss/search?q=...
```

Reuse `RSSFeedCollector` + `rss_news` policy. True web-scale concept search is NLP/AI-Feed work (P1).

---

## P1 (next, not in this doc’s implementation scope)

- Source **bundles/folders** metadata (Feedly-style packs) wrapping existing feed URLs
- PDF continuous ingest (direct PDF URL feeds → text extract → bean)
- X/Twitter API connector (costly; defer)
- OPML import/export for feed packs
- Patents as **event_type** model, not a crawler (Feedly’s “New Patents” is an AI Model)

---

## NLP additions (paired with collector)

| Need | Approach in coffeemaker |
|---|---|
| Newsletter/PDF text quality | MIME/PDF → markdown; language detect; `restricted_content` |
| Concept / event models | Extend `factory/classifications.yaml` + parquet (or LLM tag) with `event_types`: product_launch, m_and_a, partnership, funding, leadership_change, earnings, regulation, … |
| AI Feed | Query layer over `processingcache` / Beansack: filters on entities + event_types + source bundle + NOT; not a new collector |
| Top stories | Already have clustering — add “≥N publishers” rollup |
| Ask AI | Consolidator + citation list of bean URLs |
| Dedup | Strengthen story-level cluster keys (title+entity fingerprint) |

Collector P0 does **not** block on full AI Feed; ship ingest first, tag later.

---

## Suggested implementation order

1. `utils/kinds.py`: add `NEWSLETTER` (and wire `NON_NEWS_KINDS` / guess rules lightly)
2. `NewsletterCollector` + `newsletters` group in `parse_sources` / `Collector`
3. `rss_discovery.discover_feeds` + `sites` / `youtube` groups (YouTube is mostly URL sugar on RSS)
4. `build_feed_from_list_page` + `site_lists`
5. `podcasts` group + `media_url` field
6. `keyword_alerts` → Google News RSS
7. Design follow-up: `event_types` + AI Feed query API

---

## Non-goals (for this phase)

- Cloning Feedly’s 140M-source index
- Building 1k–30k pretrained “AI Models” library
- STIX / Threat Graph (unless Cafecito explicitly needs CTI)
- Paying for X API unless product requires it

---

## Open decisions (defaults chosen)

| Decision | Default |
|---|---|
| Newsletter kind | New `newsletter` kind |
| YouTube kind | `podcast` (media) vs `post` — default **`podcast`** with `media_url` |
| RSS Builder persistence | Ephemeral beans only; no generated feed file required |
| Keyword engine | Google News RSS first |

