# Bean Kind / Content-Type Classification

This file describes the classification algorithm currently implemented during collection. The code authority is `guess_content_type()` in `normalize.py`. `bean.kind` and `content_type` refer to the same classification concept; the stored Bean field is `kind`.

## Core rule

Classification is deterministic and first-match-wins. A non-empty item is evaluated in this order:

```python
decision = (
    native_item_decision(bean, context)
    or primary_document_decision(bean, context)
    or explicit_format_decision(bean, context)
    or source_policy_decision(bean, context, default_kind)
)
```

Every result is represented internally as:

```python
KindDecision(kind="news", rule_id="policy_reporting", evidence_fields=("policy", "url"))
```

The first matching decision supplies `bean.kind`. The decision metadata is used for diagnostics during collection and is removed before the Bean is cached.

## Inputs

The classifier uses the Bean plus a `KindContext` containing collection provenance and evidence:

- origin, such as RSS, Hacker News, Reddit, GovInfo, or SEC EDGAR;
- feed URL and the publisher URL declared by the feed;
- source `KindPolicy`;
- native item type and whether the item is a self-post;
- RSS tags;
- page JSON-LD schema types and article sections discovered during scraping.

Site names, descriptions, generic occurrences of the word `news`, and `default_kind="news"` are not sufficient authority for classifying an item as `news`.

## Ordered algorithm

### 1. Native platform identity

Use native provenance when it gives an unambiguous type:

- Hacker News job item -> `job`;
- Reddit or Hacker News self-post -> `post`;
- external Hacker News item whose title begins with `Show HN:` -> `site`.

If none match, continue.

### 2. Primary-document evidence

Classify authoritative document types before editorial formats:

1. SEC EDGAR origin -> `sec_filing`.
2. Exact trusted host plus path patterns identify documents such as SEC filings, procurement notices, contracts, bills, laws, regulations, rulemaking notices, court opinions, lawsuits, and research papers.
3. Approved SEC or GovInfo feed families identify their corresponding document types only when the destination is also on the compatible official host.
4. Strong item evidence may identify:
   - earnings and financial reports when supported by the appropriate source policy;
   - press releases when a leading release label is corroborated by a release policy or release tag;
   - contracts when both the title and opening text match contract structure.

Ambiguous conflicting authoritative URL matches fall back conservatively to `blog` with rule `conflicting_evidence`.

### 3. Explicit format evidence

Use tags, URL path segments, and page JSON-LD to identify non-news formats:

- podcast evidence -> `podcast`;
- blog, opinion, editorial, analysis, review, tutorial, guide, newsletter, changelog, or `BlogPosting` evidence -> `blog`;
- a GitHub repository root -> `site`;
- a GitHub release URL -> `blog`.

These decisions run before source reporting policy. Therefore an opinion or podcast from a reviewed news publisher does not become `news` merely because of the publisher.

### 4. Source policy

RSS groups in `factory/feeds.yaml` establish the default policy:

| Feed group | Policy | Result |
|---|---|---|
| `rss` | `unknown` | no authority to produce `news` |
| `rss_news` | `reporting` | eligible ordinary reporting becomes `news` |
| `rss_blogs` | `non_news`, hint `blog` | eligible unresolved item becomes `blog` |
| `rss_press_releases` | `non_news`, hint `press_release` | eligible unresolved item becomes `press_release` |

An optional `content_kind_sources` entry may refine a configured RSS feed with:

- `mode`: `unknown`, `reporting`, `mixed`, or `non_news`;
- `kind_hint`: a canonical non-news kind for `non_news` mode;
- `hosts`: exact allowed destination hosts;
- `news_paths`: segment-boundary reporting paths for `mixed` mode.

A source policy applies only when the article destination is trusted. Trust is an exact match against an explicit `hosts` allow-list when present; otherwise it is an exact match against either the configured feed host or the publisher host declared by that feed.

Policy behavior:

- eligible `non_news` policy -> its non-news `kind_hint`;
- eligible `reporting` policy -> `news`;
- eligible `mixed` policy -> `news` only for an approved `news_paths` prefix or matching `NewsArticle` schema;
- ineligible destinations receive no policy-derived kind.

## News invariant

An item becomes `news` only through an eligible `reporting` policy or through an eligible `mixed` policy with item-level reporting evidence.

The following alone do not produce `news`:

- `news` in the title, tags, site name, description, or URL;
- `NewsArticle` schema on an unknown source;
- `default_kind="news"`;
- an article linked from a reporting feed when its destination host is not trusted.

This restriction keeps the classifier conservative while allowing reviewed publishers whose RSS host differs from their declared publisher host.

## Fallback

If no earlier rule matches:

1. preserve a valid legacy non-news `default_kind`;
2. otherwise return `blog` with rule `fallback_unresolved`.

For an empty Bean, `guess_content_type()` returns `None`.

## Collection and scraping flow

1. `workers/collectororch.py` parses the feed group and optional per-feed policy.
2. The API collector builds `KindContext` and calls `apply_kind_decision()`.
3. If the page must be scraped, the context travels with the transient Bean.
4. The scraper adds JSON-LD schema types and article sections, then calls `apply_kind_decision()` again. Stronger page evidence may therefore refine the initial kind.
5. `_kind_context` and `_kind_decision` are removed before persistence; only `bean.kind` is stored.

## Authorities

- Decision algorithm and kinds: `datacollectors/normalize.py`
- RSS context construction: `datacollectors/apicollectors.py`
- Page-evidence reclassification: `datacollectors/scrapers.py`
- Source-policy parsing: `workers/collectororch.py`
- Feed policy data: `factory/feeds.yaml`
- Canonical values: `utils/kinds.py`
- Behavioral tests: `tests/test_content_kind.py`, `tests/test_normalize.py`, and `tests/test_collectors.py`
