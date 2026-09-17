import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from dataclasses import replace

from datacollectors.normalize import (
    KIND_CONTEXT_KEY,
    KIND_DECISION_KEY,
    KindContext,
    KindDecision,
    KindPolicy,
    guess_content_type,
)
from datacollectors.scrapers import AsyncWebScraper, _extract_jsonld_kind_evidence
from utils.dates import now
from workers.collectororch import Collector, parse_sources


def _decision(bean, **kwargs):
    return guess_content_type(bean, explain=True, **kwargs)


def _reporting(feed="https://news.example/feed", **kwargs):
    return KindContext(origin="rss", feed_url=feed, policy=KindPolicy(mode="reporting"), **kwargs)


def _mixed(**kwargs):
    policy = KindPolicy(
        mode="mixed",
        hosts=("mixed.example",),
        news_paths=("/reporting/",),
    )
    return KindContext(origin="rss", feed_url="https://mixed.example/feed", policy=policy, **kwargs)


def _press_policy(feed="https://ir.example/press.rss"):
    return KindContext(
        origin="rss",
        feed_url=feed,
        policy=KindPolicy(mode="non_news", kind_hint="press_release"),
    )


@pytest.mark.parametrize(("bean", "kwargs", "kind", "rule_id"), [
    ({"title": "Product update"}, {"default_kind": "news"}, "blog", "fallback_unresolved"),
    (
        {"url": "https://example.com/news/item", "site_name": "Daily News", "tags": ["news"]},
        {"default_kind": "news"},
        "blog",
        "fallback_unresolved",
    ),
    (
        {"url": "https://example.com/item", "title": "Event"},
        {"context": KindContext(schema_types=("NewsArticle",))},
        "blog",
        "fallback_unresolved",
    ),
    (
        {"url": "https://news.example/ordinary-report", "title": "City council votes tonight"},
        {"context": _reporting()},
        "news",
        "policy_reporting",
    ),
    (
        {"url": "https://news.example/op-ed", "title": "Why the vote matters", "tags": ["opinion"]},
        {"context": _reporting(rss_tags=("opinion",))},
        "blog",
        "format_blog",
    ),
    (
        {"url": "https://mixed.example/reporting/item", "title": "A report"},
        {"context": _mixed()},
        "news",
        "mixed_reporting_path",
    ),
    (
        {"url": "https://mixed.example/reporting-fake/item", "title": "Not a report"},
        {"context": _mixed()},
        "blog",
        "fallback_unresolved",
    ),
    (
        {"url": "https://mixed.example/other", "title": "A review", "tags": ["review"]},
        {"context": replace(_mixed(), schema_types=("NewsArticle",), rss_tags=("review",))},
        "blog",
        "format_blog",
    ),
    (
        {"url": "https://other.example/story", "title": "External syndicate"},
        {"context": _reporting()},
        "blog",
        "fallback_unresolved",
    ),
    (
        {"url": "https://www.publisher.example/story", "title": "Syndicated reporting feed item"},
        {"context": KindContext(
            origin="rss",
            feed_url="https://feeds.publisher.example/rss.xml",
            publisher_url="https://www.publisher.example/news",
            policy=KindPolicy(mode="reporting"),
        )},
        "news",
        "policy_reporting",
    ),
    (
        {"url": "https://external.example/story", "title": "Untrusted external item"},
        {"context": KindContext(
            origin="rss",
            feed_url="https://feeds.publisher.example/rss.xml",
            publisher_url="https://www.publisher.example/news",
            policy=KindPolicy(mode="reporting"),
        )},
        "blog",
        "fallback_unresolved",
    ),
    (
        {"url": "https://news.example/story", "title": "Acme issued a press release about layoffs"},
        {"context": _reporting()},
        "news",
        "policy_reporting",
    ),
    (
        {"url": "https://ir.example/q2", "title": "Acme Reports Second Quarter 2026 Earnings Results"},
        {"context": _press_policy()},
        "earnings_report",
        "item_earnings",
    ),
    (
        {"url": "https://news.example/markets", "title": "Acme Reports Second Quarter 2026 Earnings Results"},
        {"context": _reporting()},
        "news",
        "policy_reporting",
    ),
    (
        {"url": "https://news.example/law", "title": "Shareholders file class-action lawsuit against Acme"},
        {"context": _reporting()},
        "news",
        "policy_reporting",
    ),
    (
        {"url": "https://www.courtlistener.com/docket/123/acme-v-example/"},
        {},
        "lawsuit",
        "document_url:lawsuit",
    ),
    (
        {"url": "https://arxiv.org/abs/2501.00001"},
        {},
        "research_paper",
        "document_url:research_paper",
    ),
    (
        {"url": "https://news.example/science", "title": "A review of several climate papers"},
        {"context": _reporting()},
        "news",
        "policy_reporting",
    ),
    (
        {"url": "https://news.example/science", "title": "A review of several climate papers", "tags": ["analysis"]},
        {"context": _reporting(rss_tags=("analysis",))},
        "blog",
        "format_blog",
    ),
    (
        {"url": "https://sec.gov.evil.example/Archives/edgar/data/1/filing.htm", "title": "Form 10-K"},
        {},
        "blog",
        "fallback_unresolved",
    ),
    (
        {"url": "https://example.com/item?redirect=https://www.sec.gov/Archives/edgar/data/1"},
        {},
        "blog",
        "fallback_unresolved",
    ),
    (
        {"title": "Show HN: Acme notes", "url": "https://example.com/app"},
        {},
        "blog",
        "fallback_unresolved",
    ),
    (
        {"url": "https://github.com/acme/careers", "title": "Hiring"},
        {"context": KindContext(origin="hackernews", native_type="job")},
        "job",
        "native_hn_job",
    ),
    (
        {"url": "https://www.reddit.com/r/python/comments/1/self", "title": "A self post"},
        {"context": KindContext(origin="reddit", is_self_post=True)},
        "post",
        "native_self_post",
    ),
    (
        {"url": "https://news.example/story", "title": "Storms close schools"},
        {"context": replace(_reporting(), feed_description="Home of our award-winning podcasts")},
        "news",
        "policy_reporting",
    ),
    (
        {"url": "https://example.com/ep", "title": "Weekly news", "tags": ["podcast", "news"]},
        {"context": KindContext(rss_tags=("podcast", "news"))},
        "podcast",
        "format_podcast",
    ),
])
def test_mandatory_kind_catalog(bean, kwargs, kind, rule_id):
    decision = _decision(bean, **kwargs)
    assert decision.kind == kind
    assert decision.rule_id == rule_id


def test_empty_input_returns_none_in_both_modes():
    assert guess_content_type(None) is None
    assert guess_content_type({}) is None
    assert guess_content_type(None, explain=True) is None
    assert guess_content_type({}, explain=True) is None


def test_legacy_non_news_fallback_and_ignored_news_default():
    bean = {"title": "Product update", "url": "https://example.com/update"}
    assert _decision(bean, default_kind="blog").rule_id == "legacy_non_news_fallback"
    assert _decision(bean, default_kind="news").rule_id == "fallback_unresolved"
    assert _decision(bean, default_kind="not_a_kind").rule_id == "fallback_unresolved"


def test_explain_matches_kind_and_does_not_mutate():
    bean = {"url": "https://arxiv.org/abs/2501.00001", "kind": "news"}
    snapshot = dict(bean)
    kind = guess_content_type(bean)
    decision = guess_content_type(bean, explain=True)
    assert kind == decision.kind == "research_paper"
    assert bean == snapshot
    assert guess_content_type(bean, explain=True) == decision


def test_conflicting_feed_url_raises():
    ctx = KindContext(feed_url="https://news.example/feed")
    with pytest.raises(ValueError, match="conflicting feed_url"):
        guess_content_type({"title": "x"}, feed_url="https://other.example/feed", context=ctx)


def test_document_hosts_and_iacr_and_github():
    assert _decision({"url": "https://eprint.iacr.org/2024/123"}).rule_id == "document_url:research_paper"
    assert _decision({"url": "https://github.com/acme/notes"}).rule_id == "repository_site"
    assert _decision({"url": "https://github.com/acme/notes/releases/tag/v1"}).rule_id == "repository_release"
    assert _decision({"url": "https://www.sec.gov/Archives/edgar/data/1/file.htm"}).kind == "sec_filing"
    assert _decision({"url": "https://pacer.uscourts.gov/"}).rule_id == "fallback_unresolved"


def test_item_release_requires_leading_label_and_corroboration():
    ctx = _press_policy()
    tagged = KindContext(rss_tags=("press release",))
    assert _decision(
        {"url": "https://ir.example/p", "title": "Press release: Acme ships"},
        context=ctx,
    ).rule_id == "item_release"
    assert _decision(
        {"url": "https://example.com/p", "title": "Press release: Acme ships"},
        context=tagged,
    ).rule_id == "item_release"
    assert _decision(
        {"url": "https://ir.example/p", "title": "Acme issued a press release"},
        context=ctx,
    ).rule_id == "policy_non_news"
    assert _decision(
        {"url": "https://example.com/p", "title": "Press release: Acme ships"},
    ).rule_id == "fallback_unresolved"


def test_item_contract_needs_opening_and_title_label():
    bean = {
        "url": "https://example.com/contracts/acme",
        "title": "Acme purchase agreement",
        "content": "This Agreement is entered into by and between the parties.",
    }
    assert _decision(bean).rule_id == "item_contract"
    assert _decision({"title": "Acme purchase agreement", "url": "https://example.com/x"}).rule_id != "item_contract"


def test_sec_and_govinfo_feeds_need_compatible_hosts():
    statements = "https://www.sec.gov/news/statements.rss"
    bills = "https://www.govinfo.gov/rss/bills.xml"
    assert _decision(
        {"url": "https://www.sec.gov/news/statement/example.htm", "title": "Press release"},
        feed_url=statements,
    ).kind == "official_statement"
    assert _decision(
        {"url": "https://www.govinfo.gov/content/pkg/BILLS-119hr1/html/BILLS-119hr1.htm"},
        feed_url=bills,
    ).kind == "legislative_bill"
    assert _decision({"title": "Press release"}, feed_url=statements, default_kind="blog").kind == "blog"


def test_authoritative_urls_still_map():
    assert guess_content_type({"url": "https://www.congress.gov/public-law/119th-congress/house-bill/1/text"}) == "enacted_law"
    assert guess_content_type({"url": "https://www.ecfr.gov/current/title-17/chapter-II"}) == "regulation"
    assert guess_content_type({"url": "https://www.federalregister.gov/documents/2026/08/17/example-rule"}) == "rulemaking_notice"
    assert guess_content_type({"url": "https://www.supremecourt.gov/opinions/25pdf/24-1_abc1.pdf"}) == "court_opinion"
    assert guess_content_type({"url": "https://sam.gov/opp/abc123/view"}) == "procurement_notice"
    assert guess_content_type({"url": "https://www.usaspending.gov/award/CONT_AWD_123"}) == "contract"


def test_jsonld_graph_list_and_unrelated_nodes():
    article = "https://mixed.example/reporting/item"
    html = '''
    <script type="application/ld+json">
    {"@graph": [
      {"@type": "NewsArticle", "url": "https://mixed.example/reporting/item", "articleSection": "reporting"},
      {"@type": "NewsArticle", "url": "https://mixed.example/other", "articleSection": "sports"}
    ]}
    </script>
    '''
    evidence = _extract_jsonld_kind_evidence(html, article)
    assert evidence["schema_types"] == ("NewsArticle",)
    assert evidence["article_sections"] == ("reporting",)

    html_list = '''
    <script type="application/ld+json">
    [{"@type": ["BlogPosting"], "headline": "Solo"}]
    </script>
    '''
    lone = _extract_jsonld_kind_evidence(html_list, "https://example.com/solo")
    assert lone["schema_types"] == ("BlogPosting",)

    html_multi = '''
    <script type="application/ld+json">
    [{"@type": "NewsArticle", "url": "https://a.example/1"},
     {"@type": "NewsArticle", "url": "https://a.example/2"}]
    </script>
    '''
    assert _extract_jsonld_kind_evidence(html_multi, "https://a.example/3")["schema_types"] == ()


def test_scrape_keeps_feed_context_and_can_change_kind():
    collected = now()
    context = _reporting()
    bean = {
        "kind": "news",
        "url": "https://news.example/contracts/acme",
        "title": "Acme purchase agreement",
        "collected": collected,
        KIND_CONTEXT_KEY: context,
    }
    result = {
        "content": "This Agreement is entered into by and between the parties.",
        "keywords": None,
        "schema_types": (),
        "article_sections": (),
    }
    classified = AsyncWebScraper._prep_page_result(None, bean, result)
    assert classified["kind"] == "contract"
    assert classified[KIND_CONTEXT_KEY].feed_url == context.feed_url
    assert classified[KIND_DECISION_KEY].rule_id == "item_contract"


def test_page_nulls_do_not_clear_rss_tags_or_feed():
    collected = now()
    context = KindContext(origin="rss", feed_url="https://news.example/feed", rss_tags=("live",))
    bean = {
        "url": "https://news.example/story",
        "title": "Hello",
        "tags": ["live"],
        "rss_feed": "https://news.example/feed",
        "collected": collected,
        KIND_CONTEXT_KEY: context,
        KIND_DECISION_KEY: KindDecision("news", "policy_reporting", ("policy",)),
    }
    result = {"content": "word " * 50, "keywords": None, "rss_feed": "https://cdn.example/other.xml"}
    classified = AsyncWebScraper._prep_page_result(None, bean, result)
    assert classified["rss_feed"] == "https://news.example/feed"
    assert "live" in classified["tags"]
    assert classified[KIND_CONTEXT_KEY].rss_tags == ("live",)


def test_split_and_cache_drop_reserved_keys_from_payloads():
    collector = object.__new__(Collector)
    captured = {}

    async def fake_set(kind, state, payload):
        captured["payload"] = payload
        return len(payload)

    collector.cache = SimpleNamespace(set=fake_set)
    collector.beans_collected = 0
    collected = now()
    item = {
        "url": "https://news.example/story",
        "title": "Hello",
        "collected": collected,
        "created": collected,
        "base_url": "news.example",
        "domain_name": "news",
        "kind": "news",
        "content_length": 400,
        KIND_CONTEXT_KEY: _reporting(),
        KIND_DECISION_KEY: KindDecision("news", "policy_reporting", ("policy",)),
        "site_name": "News Example",
        "description": "Publisher blurb",
        "rss_feed": "https://news.example/feed",
    }
    bean, chatter, publisher = collector._split_item(item)
    assert KIND_CONTEXT_KEY in bean
    assert KIND_DECISION_KEY in bean
    assert KIND_CONTEXT_KEY not in (publisher or {})
    asyncio.run(collector._cache_beans([bean]))
    assert captured["payload"]
    assert KIND_CONTEXT_KEY not in captured["payload"][0]
    assert KIND_DECISION_KEY not in captured["payload"][0]


def test_parse_sources_policies_and_multiset():
    unknown = KindPolicy(mode="unknown")
    blog = KindPolicy(mode="non_news", kind_hint="blog")
    press = KindPolicy(mode="non_news", kind_hint="press_release")
    sources = parse_sources("""
sources:
  rss:
    - https://example.com/feed
    - https://example.com/feed
  rss_blogs:
    - https://example.com/blog/feed.xml
  rss_press_releases:
    - https://example.com/press/rss
content_kind_sources:
  https://example.com/feed:
    mode: mixed
    hosts: [example.com, www.example.com]
    news_paths: [/reporting/]
""")
    assert sources["rss"][0][1] == KindPolicy(
        mode="mixed", hosts=("example.com", "www.example.com"), news_paths=("/reporting/",)
    )
    assert sources["rss"][1][0] == "https://example.com/feed"
    assert sources["rss"][1][1].mode == "mixed"
    assert sources["rss"][2] == ("https://example.com/blog/feed.xml", blog)
    assert sources["rss"][3] == ("https://example.com/press/rss", press)
    assert sources["rss"][0][1] != unknown


@pytest.mark.parametrize("config, match", [
    ("""
sources:
  rss:
    - https://example.com/feed
content_kind_sources:
  https://example.com/feed:
    mode: nope
""", "invalid mode"),
    ("""
sources:
  rss:
    - https://example.com/feed
content_kind_sources:
  https://missing.example/feed:
    mode: mixed
    news_paths: [/reporting/]
""", "not a configured RSS feed"),
    ("""
sources:
  rss:
    - https://example.com/feed
  rss_blogs:
    - https://example.com/feed
""", "same feed appears in groups"),
    ("""
sources:
  rss_blogs:
    - https://example.com/blog/feed
content_kind_sources:
  https://example.com/blog/feed:
    mode: reporting
""", "must agree with group"),
    ("""
sources:
  rss:
    - https://example.com/feed
content_kind_sources:
  https://user:pass@example.com/feed:
    mode: mixed
    news_paths: [/reporting/]
""", "credentials"),
])
def test_parse_sources_rejects_invalid_config(config, match):
    with pytest.raises(ValueError, match=match):
        parse_sources(config)


def test_feeds_yaml_url_multiset_preserved():
    from pathlib import Path
    import yaml
    from collections import Counter

    raw = yaml.safe_load(Path("factory/feeds.yaml").read_text())["sources"]
    parsed = parse_sources("factory/feeds.yaml")
    scheduled = [url for url, _policy in parsed["rss"]]
    expected = (raw["rss"] or []) + (raw.get("rss_news") or []) + raw["rss_blogs"] + raw["rss_press_releases"]
    assert Counter(scheduled) == Counter(expected)
    assert {policy.mode for _, policy in parsed["rss"]} <= {"unknown", "reporting", "mixed", "non_news"}


def test_collector_forwards_rss_policy():
    collector = object.__new__(Collector)
    collector.rss_collector = SimpleNamespace(collect=AsyncMock(return_value=[]))
    collector._triage = AsyncMock()
    policy = KindPolicy(mode="non_news", kind_hint="blog")
    asyncio.run(collector._collect("rss", "https://example.com/feed.xml", policy))
    collector.rss_collector.collect.assert_awaited_once_with(
        "https://example.com/feed.xml", policy=policy
    )
