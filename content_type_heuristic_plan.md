# Conservative `bean.kind` Classification

## Summary

Current flow:

`feeds.yaml group → collector default_kind → RSS/Reddit/HN builder → guess_content_type() → external fallback → optional scrape/reclassification`

Primary error paths:

- Generic `rss` assigns `news` to 10,798 feeds; full-text feed items bypass scraping, so that default becomes final.
- Live data has 12,621 RSS publishers. Candidate `news` misclassification paths include 11,173 beans from blog-described sources, 18,294 from software/code sources, 7,534 from podcast sources, and 2,517 from newsletter sources. These overlapping lexical counts are audit signals, not confirmed errors.
- The offender artifact currently has 23 sources and 31 alternative-kind matches: 10 press releases, 4 earnings reports, 4 lawsuits, 4 research papers, and smaller groups.
- Generic news checks run before precise title/body rules. Notably, “press release” currently resolves to `news`.
- Feed title/description are added after classification, and fallback logic exists outside `guess_content_type()`.

## Implementation Changes

1. `datacollectors/normalize.py`:
   - Make `guess_content_type()` the sole final-kind authority and rename `default_kind` to `kind_hint`.
   - Apply precedence: structural/exact feed and URL rules → explicit non-news source evidence → precise document rules → conservative news gate → non-news hint → `blog`.
   - Match URL segments and normalized tokens, not arbitrary substrings.
   - Remove `announcement`, `press release`, and broad `report`/`press` source terms as standalone news evidence.
   - Classify `news` only for an explicit verified-news hint or when both source-level and item-level news evidence exist.
   - Pros: one deterministic model; ambiguous content cannot become news.
   - Cons: news recall will fall; `blog` remains the compatibility catch-all.

```python
def guess_content_type(bean, *, feed_url=None, kind_hint=None):
    if kind := authoritative_kind(bean, feed_url):
        return kind
    if kind := explicit_non_news_kind(bean, kind_hint):
        return kind
    if kind := precise_document_kind(bean):
        return kind
    if kind_hint == NEWS or has_source_news_signal(bean) and has_item_news_signal(bean):
        return NEWS
    return kind_hint if kind_hint in NON_NEWS_KINDS else BLOG
```

2. `datacollectors/apicollectors.py`, `datacollectors/scrapers.py`:
   - Add RSS feed title and description to `SITE_NAME`/`DESCRIPTION` before classification.
   - Remove every `guess_content_type(...) or default_kind` and direct fallback assignment; builders only assign the classifier result.
   - Pass `POST`, `BLOG`, `PRESS_RELEASE`, or other known source facts as hints rather than final values.
   - Remove Reddit’s outbound-link `news` default; unknown outbound Reddit/HN links resolve through the shared model.
   - During scraping, call the same classifier with the existing generic kind as a hint, allowing stronger URL/body evidence to replace it.
   - Pros: feed-only and scraped items follow identical semantics.
   - Cons: noisy feed metadata must be handled by exact-token rules.

3. `workers/collectororch.py`, `factory/feeds.yaml`:
   - Add `rss_news`; reserve it for operator-verified news feeds.
   - Make generic `rss` untyped, while `rss_blogs` and `rss_press_releases` retain explicit non-news hints.
   - Initially leave `rss_news: []`; do not bulk-promote the existing generic feeds.
   - Rename the flow consistently to `kind_hint`: YAML group → parsed source tuple → collector → classifier.
   - Preserve every existing feed URL and assert no URL is lost or duplicated during the config change.
   - Pros: source configuration describes evidence rather than deciding `bean.kind`.
   - Cons: verified-news curation becomes an explicit maintenance task.

## Tests and Acceptance

4. `tests/test_normalize.py`, `tests/test_collectors.py`:
   - Verify press/news/media releases resolve to `press_release`, even with a news hint.
   - Verify personal blogs, Substack/newsletters, tutorials, code announcements, research archives, podcasts, and generic RSS ambiguity do not resolve to news.
   - Verify `rss_news` is sufficient for news, while automatic news requires both source and item evidence; either signal alone returns blog.
   - Preserve SEC, GovInfo, HN job/show, social self-post, and authoritative URL behavior.
   - Verify full-content RSS and scraped RSS produce the same kind.
   - Verify feed-group parsing preserves the complete URL set.
   - Run `.venv/bin/pytest tests/test_normalize.py tests/test_collectors.py -q`.
   - Re-run the latest-five-per-source Neon audit read-only and manually inspect the highest-volume changed sources.
   - Pros: tests cover precedence and observed failure paths.
   - Cons: production precision still requires periodic editorial sampling.

## Assumptions

- Ambiguous collected content becomes `blog`; no new taxonomy value is introduced.
- Existing database rows are not backfilled; the change affects new or recollected beans.
- `bean.kind` and the database schema remain unchanged.
- Title-rule matches are audit candidates, not automatically confirmed editorial corrections.
