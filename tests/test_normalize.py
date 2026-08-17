import pytest

from datacollectors.apicollectors import _build_hackernews_item, _build_reddit_json_item
from datacollectors.scrapers import AsyncWebScraper
from utils.dates import now
from datacollectors.normalize import guess_content_type, html_to_markdown

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
