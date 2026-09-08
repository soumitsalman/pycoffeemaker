import pytest

from datacollectors.apicollectors import _build_hackernews_item, _build_reddit_json_item
from datacollectors.normalize import cleanup_item, cleanup_language, cleanup_title, guess_content_type, html_to_markdown
from datacollectors.scrapers import AsyncWebScraper, _extract_jsonld_content
from utils.dates import now
from utils.fields import ARTICLE_LANGUAGE, CONTENT, LANGUAGE, SUMMARY, TITLE

_NO_H1 = "<p>Hello <strong>world</strong>. <a href='https://x.com'>link</a></p>"
_WITH_H1 = "<h1>Article Title</h1><p>First paragraph.</p><ul><li>one</li></ul>"
_REDDIT_MD = '<div class="md"><p>Self post <em>text</em>.</p></div>'
_HN_TEXT = "<p>Ask HN: question here?</p><pre><code>code block</code></pre>"


@pytest.mark.parametrize("html,expected_in", [
    (_NO_H1, "Hello **world**"),
    (_WITH_H1, "# Article Title"),
    (_WITH_H1, "First paragraph"),
    (_REDDIT_MD, "Self post"),
    (_HN_TEXT, "Ask HN"),
    ("Plain text, no tags", "Plain text"),
])
def test_html_to_markdown_partial_html(html, expected_in):
    result = html_to_markdown(html)
    assert result
    assert expected_in in result


@pytest.mark.parametrize("html", [None, "", "   "])
def test_html_to_markdown_empty(html):
    assert html_to_markdown(html) is None


def test_html_to_markdown_with_h1_has_heading():
    result = html_to_markdown(_WITH_H1)
    assert result.startswith("#")


def test_html_to_markdown_without_h1_no_atx_heading():
    result = html_to_markdown(_NO_H1)
    assert not result.startswith("#")


def test_html_to_markdown_malformed_fallback():
    result = html_to_markdown("<p>unclosed")
    assert result
    assert "unclosed" in result


@pytest.mark.parametrize("html,expected_in", [
    ("&lt;p&gt;Hello &lt;strong&gt;world&lt;/strong&gt;&lt;/p&gt;", "Hello **world**"),
    ("&amp;lt;p&amp;gt;Hello&amp;lt;/p&amp;gt;", "Hello"),
    ("<![CDATA[<p>Hello <em>there</em></p>]]>", "Hello *there*"),
    ("Intro text &lt;p&gt;Body para&lt;/p&gt; more", "Body para"),
])
def test_html_to_markdown_decodes_escaped_and_cdata(html, expected_in):
    result = html_to_markdown(html)
    assert result
    assert expected_in in result
    assert "<p>" not in result.lower()
    assert "</p>" not in result.lower()
    assert "<![CDATA[" not in result


def test_html_to_markdown_keeps_tags_inside_code_fences():
    result = html_to_markdown("<pre><code>&lt;div&gt;code&lt;/div&gt;</code></pre>")
    assert result
    assert "```" in result
    assert "<div>" in result


def test_cleanup_item_converts_leftover_html_in_body_fields():
    item = cleanup_item({
        TITLE: "<b>Breaking</b> news",
        SUMMARY: "<p>Short <em>blurb</em>.</p>",
        CONTENT: "<div><p>Hello <strong>world</strong>.</p></div>",
        "url": "https://example.com/a",
    })
    assert "<" not in item[TITLE]
    assert "Breaking" in item[TITLE]
    assert "<p>" not in item[SUMMARY]
    assert "*blurb*" in item[SUMMARY] or "blurb" in item[SUMMARY]
    assert "<p>" not in item[CONTENT]
    assert "**world**" in item[CONTENT]


@pytest.mark.parametrize(("title", "site_name", "expected"), [
    ("CNN | Blah blah", "CNN", "Blah blah"),
    ("Blah blah | the WIRE", "The Wire", "Blah blah"),
    ("CNN: Blah blah", "cnn.com", "Blah blah"),
    ("Blah blah - CNN", "CNN", "Blah blah"),
    ("CNN — Blah blah", "CNN", "Blah blah"),
    ("Blah blah / CNN.com", "CNN", "Blah blah"),
    ("[CNN] Blah blah", "CNN", "Blah blah"),
    ("Blah blah (The Wire)", "The Wire", "Blah blah"),
    ("CNN | Blah blah | CNN", "CNN", "Blah blah"),
    ("CNN reports on Blah blah", "CNN", "CNN reports on Blah blah"),
    ("Blah blah | Not CNN", "CNN", "Blah blah | Not CNN"),
])
def test_cleanup_title_removes_delimited_site_name(title, site_name, expected):
    assert cleanup_title(title, site_name) == expected


def test_cleanup_item_uses_site_name_to_clean_title():
    item = cleanup_item({TITLE: "CNN | Blah blah", "site_name": "CNN", "url": "https://cnn.com/a"})
    assert item[TITLE] == "Blah blah"


@pytest.mark.parametrize("raw,expected", [
    ('"en-US"', "en-us"),
    ("'en_GB'", "en-gb"),
    ("en, US", "en"),
    ("  EN  ", "en"),
    ("zh-Hans", "zh-hans"),
    ("pt BR", "pt-br"),
    ('"English, British"', "english"),
])
def test_cleanup_item_normalizes_language_fields(raw, expected):
    assert cleanup_language(raw) == expected
    item = cleanup_item({LANGUAGE: raw, ARTICLE_LANGUAGE: raw, "url": "https://example.com/a"})
    assert item[LANGUAGE] == expected
    assert item[ARTICLE_LANGUAGE] == expected


def test_jsonld_html_body_is_converted_to_markdown():
    html = (
        '<script type="application/ld+json">'
        '{"@graph": [{"@type": "NewsArticle", "headline": "H",'
        ' "articleBody": "<p>Hello <strong>world</strong></p>",'
        ' "description": "<p>ignored summary</p>"}]}'
        "</script>"
    )
    result = _extract_jsonld_content(html)
    assert result
    assert "<p>" not in result[CONTENT]
    assert "Hello" in result[CONTENT]
    assert "**world**" in result[CONTENT]


@pytest.mark.parametrize(("bean", "feed_url", "expected"), [
    ({"url": "https://www.sec.gov/Archives/edgar/data/123/filing.htm"}, None, "sec_filing"),
    ({"url": "https://www.govinfo.gov/content/pkg/BILLS-119hr1/html/BILLS-119hr1.htm"}, "https://www.govinfo.gov/rss/bills.xml", "legislative_bill"),
    ({"url": "https://www.govinfo.gov/content/pkg/USCOURTS-ca2-24-1/html/opinion.htm"}, "https://www.govinfo.gov/rss/uscourts-ca2.xml", "court_opinion"),
    ({"title": "Acme Reports Second Quarter 2026 Earnings Results"}, None, "earnings_report"),
    ({"title": "Draft legislation for clean energy"}, None, "legislative_proposal"),
    ({"title": "Acme v. Example Corp. complaint filed"}, None, "lawsuit"),
    ({"content": "This Agreement is entered into by and between the parties."}, None, "contract"),
    ({"title": "Annual report and consolidated financial statements"}, None, "financial_report"),
    ({"title": "Product press release"}, None, "press_release"),
    ({"url": "https://www.congress.gov/public-law/119th-congress/house-bill/1/text"}, None, "enacted_law"),
    ({"url": "https://www.ecfr.gov/current/title-17/chapter-II"}, None, "regulation"),
    ({"url": "https://www.federalregister.gov/documents/2026/08/17/example-rule"}, None, "rulemaking_notice"),
    ({"url": "https://www.supremecourt.gov/opinions/25pdf/24-1_abc1.pdf"}, None, "court_opinion"),
    ({"url": "https://sam.gov/opp/abc123/view"}, None, "procurement_notice"),
    ({"url": "https://www.usaspending.gov/award/CONT_AWD_123"}, None, "contract"),
    ({"url": "https://investor.example.com/financials/quarterly-results"}, None, "earnings_report"),
    ({"title": "Acme files Form 10-K annual report"}, None, "sec_filing"),
    ({"title": "Acme announces full-year results"}, None, "earnings_report"),
    ({"title": "Acme signs definitive agreement to acquire Example"}, None, "contract"),
    ({"title": "What a 10-K tells shareholders"}, None, None),
])
def test_guess_content_type_uses_authoritative_url_feed_and_text_signals(bean, feed_url, expected):
    assert guess_content_type(bean, feed_url) == expected


def test_guess_content_type_detects_research_papers():
    bean = {"url": "https://arxiv.org/abs/2501.00001"}
    assert guess_content_type(bean) == "research_paper"


def test_scraped_content_reclassifies_a_generic_kind():
    collected = now()
    bean = {
        "kind": "news",
        "url": "https://example.com/contracts/acme",
        "source": "example",
        "title": "Acme agreement",
        "collected": collected,
    }
    result = {"content": "This Agreement is entered into by and between the parties."}

    classified = AsyncWebScraper._prep_page_result(None, bean, result)

    assert classified["kind"] == "contract"


def test_outbound_hacker_news_uses_its_inline_body_for_kind():
    item = _build_hackernews_item({
        "id": 1,
        "time": 0,
        "url": "https://example.com/contracts/acme",
        "title": "Acme agreement",
        "text": "<p>This Agreement is entered into by and between the parties.</p>",
    }, "blog")

    assert item["kind"] == "contract"


def test_hacker_news_and_reddit_without_outbound_urls_are_posts():
    hacker_news = _build_hackernews_item({"id": 1, "time": 0, "title": "Ask HN"}, "blog")
    reddit = _build_reddit_json_item({
        "created_utc": 0,
        "permalink": "/r/python/comments/1/self_post",
        "is_self": True,
        "url": "https://www.reddit.com/r/python/comments/1/self_post",
        "title": "A self post",
        "selftext": "body",
        "author": "author",
    }, "python", "news")

    assert hacker_news["kind"] == reddit["kind"] == "post"


def test_outbound_reddit_url_uses_guess_content_type():
    item = _build_reddit_json_item({
        "created_utc": 0,
        "permalink": "/r/procurement/comments/1/opportunity",
        "is_self": False,
        "url": "https://sam.gov/opp/abc123/view",
        "title": "Federal opportunity",
        "author": "author",
    }, "procurement", "news")

    assert item["kind"] == "procurement_notice"


def test_strip_tracking_keeps_page_and_drops_share_params():
    from datacollectors.normalize import strip_tracking_params
    assert strip_tracking_params("https://x.com/a?utm_source=reddit&page=2") == "https://x.com/a?page=2"
    assert strip_tracking_params("https://x.com/a?source=linkedin") == "https://x.com/a"
    assert strip_tracking_params("https://x.com/a?utm_source=reddit") == "https://x.com/a"


def test_error_canonical_is_rejected_and_page_query_is_kept():
    from datacollectors.normalize import is_compatible_content_url, resolve_content_url
    retrieval = "https://www.govinfo.gov/content/pkg/uscourts-x/html/x.htm"
    assert not is_compatible_content_url(retrieval, "https://www.govinfo.gov/error")
    assert resolve_content_url(retrieval, meta_url="https://www.govinfo.gov/error", final_url="https://www.govinfo.gov/error") == retrieval
    chosen = resolve_content_url(
        "https://www.bbc.co.uk/news/foo?at_medium=rss&page=1",
        meta_url="https://www.bbc.co.uk/news/foo",
    )
    assert chosen == "https://www.bbc.co.uk/news/foo?page=1"


def test_prep_rejects_incompatible_redirect():
    collected = now()
    bean = {
        "kind": "news",
        "url": "https://www.govinfo.gov/content/pkg/uscourts-x/html/x.htm",
        "title": "Opinion",
        "collected": collected,
        "created": collected,
        "base_url": "govinfo.gov",
        "domain_name": "govinfo",
    }
    result = {
        "url": "https://www.govinfo.gov/error",
        "content": "error page " + ("word " * 400),
    }
    assert AsyncWebScraper._prep_page_result(None, bean, result) is None
    assert bean["url"] == "https://www.govinfo.gov/content/pkg/uscourts-x/html/x.htm"
