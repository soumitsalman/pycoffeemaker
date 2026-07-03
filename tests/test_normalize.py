import pytest

from datacollectors.normalize import html_to_markdown

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
