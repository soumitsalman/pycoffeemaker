# Collector Data Fields

This document describes the fields currently produced by the collectors. Code is authoritative: RSS extraction lives in `datacollectors/apicollectors.py`, normalization in `datacollectors/normalize.py`, and worker triage in `workers/collectororch.py`.

Field-name constants come from `utils/fields.py`. A collected item may contain Bean, Chatter, and Publisher fields together; the collector worker splits that item before persistence.

## Standard RSS extraction

`RSSFeedCollector` fetches and parses RSS/Atom feeds, rejects excluded entry URLs, builds one item per entry, calls `cleanup_item()`, and then assigns `kind` through the algorithm documented in [CONTENT_KIND_CLASSIFICATION.md](CONTENT_KIND_CLASSIFICATION.md).

### Entry fields

| Output field | Current source and fallback order |
|---|---|
| `url` | `entry.link`; otherwise `entry.links[0].href`. Relative links are resolved against the feed site URL. Entries without a usable link are skipped. |
| `domain_name` | Derived from the resolved item URL. |
| `base_url` | Derived from the resolved item URL. |
| `title` | `entry.title`. |
| `summary` | `entry.summary`, converted from HTML to Markdown. |
| `content` | `entry.dc_content`; otherwise the first `entry.content[].value` (or scalar `entry.content`); otherwise `entry.summary`. Converted from HTML to Markdown. |
| `author` | `entry.author`, with HTML removed. |
| `author_email` | `entry.author_detail.email`. |
| `created` | `entry.published_parsed`; otherwise `entry.updated_parsed`; otherwise collection time. Future or unusable dates are replaced by collection time. |
| `collected` | Current UTC collection time. |
| `tags` | Each `entry.tags[].term`, lowercased; empty values are removed. |
| `article_language` | `entry.language`; otherwise `feed.language`. |
| `image_url` | First image link in `entry.links`; otherwise first `media_content.url`; first `media_thumbnail.url`; then `entry.image.href`. Relative URLs are resolved against the feed site URL. |
| `rss_feed` | The configured feed URL. |
| `site_language` | `feed.language`. |
| `kind` | Assigned by `guess_content_type()` using collection provenance, authoritative document evidence, explicit format evidence, and the configured source policy. |

The site URL used for relative-link resolution is the first HTTP URL among `feed.link`, the configured feed URL, and the first entry link.

### Chatter fields from RSS

When an entry supplies a comment feed/link or a positive comment count, the same item also contains:

| Output field | Current source |
|---|---|
| `chatter_url` | `entry.wfw_commentrss`. |
| `comments` | Integer parsed from `entry.slash_comments`; otherwise `entry.comments`; otherwise `0`. |
| `platform` | Domain derived from the resolved item URL. |
| `url` | The resolved item URL, used to associate chatter with its Bean. |

## Specialized sources

### GovInfo RSS

`GovInfoRSSCollector` does not use the entry page as the Bean URL. It extracts an official `https://www.govinfo.gov/content/pkg/...` HTML, text, or PDF download from `entry.description`. If none is present, it probes URLs derived from `entry.guid` or `entry.id`, preferring HTML/text before PDF. Entries without an available package document are skipped. The standard RSS field builder then runs with `origin="govinfo"` so document kind is assigned from authoritative GovInfo evidence.

### SEC EDGAR

`SECFilingCollector` downloads each ZIP archive referenced by an EDGAR entry GUID, extracts the first HTML filing, converts it to Markdown, and adds `sec`, `edgar`, filing-type, and accession-number tags. The kind classifier assigns `sec_filing` from SEC provenance.

### Reddit

`RedditCollector` emits:

- external link posts with the destination as `url` and the Reddit permalink as `chatter_url`;
- self-posts with the Reddit permalink as `url` and `kind="post"`;
- `platform="reddit"`, `forum="r/<subreddit>"`, score as `likes`, comment count, author, timestamps, and self-text when available.

The production worker requests Reddit JSON mode. RSS parsing remains an implementation fallback.

### Hacker News

`HackerNewsCollector` emits external story URLs when present; otherwise it uses the Hacker News discussion URL. The discussion URL is also recorded as `chatter_url`, with score mapped to `likes` and the number of immediate child IDs mapped to `comments`. Native job, self-post, and `Show HN:` evidence is passed to the kind classifier.

## Normalization

`cleanup_item()` mutates each item before it leaves a collector:

- cleans text, titles, authors, URLs, and language values;
- derives `base_url` from `url` when missing;
- replaces a missing or unusable `created` value with `collected` and ensures UTC;
- detects language from content, summary, or title when no language field exists;
- adds `title_length`, `summary_length`, and `content_length`.

`content_length` is the normalized word count used by worker triage, not bytes or characters.

## Worker triage and persistence

`Collector._split_item()` separates each combined item into:

- a Bean requiring `title`, `collected`, `created`, `base_url`, and `kind`;
- Chatter requiring `chatter_url`, `url`, and at least one engagement value;
- a Publisher requiring `domain_name` and `base_url`.

Beans with at least `WORDS_THRESHOLD_FOR_STORING` words are cached directly. The current default is `200`. Short non-`post` Beans are deduplicated, scraped with `AsyncWebScraper`, re-normalized and reclassified using page evidence, and cached only if they then meet the threshold. Word-game titles listed in `IGNORE_WORD_GAMES` are neither stored nor scraped.

Publishers with any of `site_name`, `favicon`, or `description` are cached directly. Incomplete Publishers are deduplicated and scraped first. Transient `_kind_context` and `_kind_decision` fields support scrape-time reclassification and diagnostics but are removed before Bean persistence.

## Authorities

- Field constants: `utils/fields.py`
- RSS/API extraction: `datacollectors/apicollectors.py`
- Cleanup and kind classification: `datacollectors/normalize.py`
- Scrape enrichment: `datacollectors/scrapers.py`
- Source configuration: `factory/feeds.yaml`
- Source parsing and persistence rules: `workers/collectororch.py`
- Kind algorithm: [CONTENT_KIND_CLASSIFICATION.md](CONTENT_KIND_CLASSIFICATION.md)
