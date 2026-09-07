import re

import lxml.html
import tldextract
from aiohttp import ClientResponse
from dataclasses import dataclass
from dateutil.parser import parse as date_parser
from html_to_markdown import ConversionOptions, convert
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
from utils.dates import ensure_utc, now, usable_created
from utils.fields import (
    ARTICLE_LANGUAGE,
    AUTHOR,
    AUTHOR_EMAIL,
    BASE_URL,
    CHATTER_URL,
    COLLECTED,
    CONTENT,
    CONTENT_LENGTH,
    SUMMARY_LENGTH,
    TITLE_LENGTH,
    CREATED,
    DESCRIPTION,
    FAVICON,
    FORUM,
    IMAGE_URL,
    LIKES,
    COMMENTS,
    KIND,
    LANGUAGE,
    PLATFORM,
    RESTRICTED_CONTENT,
    RSS_FEED,
    SITE_LANGUAGE,
    SITE_NAME,
    DOMAIN_NAME,
    SUMMARY,
    TAGS,
    TITLE,
    URL,
)

from .settings import MAX_HTML_SIZE, MAX_PDF_SIZE

POST = "post"
BLOG = "blog"
NEWS = "news"
SITE = "site"
PODCAST = "podcast"
CONTRACT = "contract"
PROCUREMENT_NOTICE = "procurement_notice"
FINANCIAL_REPORT = "financial_report"
EARNINGS_REPORT = "earnings_report"
SEC_FILING = "sec_filing"
PRESS_RELEASE = "press_release"
OFFICIAL_STATEMENT = "official_statement"
ENFORCEMENT_ACTION = "enforcement_action"
LEGISLATIVE_BILL = "legislative_bill"
LEGISLATIVE_PROPOSAL = "legislative_proposal"
ENACTED_LAW = "enacted_law"
REGULATION = "regulation"
RULEMAKING_NOTICE = "rulemaking_notice"
COURT_OPINION = "court_opinion"
LAWSUIT = "lawsuit"
GOVERNMENT_REPORT = "government_report"
BUDGET_DOCUMENT = "budget_document"
LEGISLATIVE_RECORD = "legislative_record"
HEARING = "hearing"
RESEARCH_PAPER = "research_paper"
WHITEPAPER = "whitepaper"
TECHNICAL_DOCUMENTATION = "technical_documentation"

POST_DOMAINS = {"reddit", "redd", "linkedin", "x", "twitter", "facebook", "ycombinator"}
BLOG_URLS = {
    "medium.com", "substack.", "wordpress.", "blogspot.", "newsletter.", "developers.",
    "blogs.", "blog.", ".so/", ".dev/", ".io/", ".to/", ".rs/", ".tech/", ".ai/", ".blog/",
    "/blog/", "/reviews/",
}
BLOG_SITENAMES = {"blog", "magazine", "newsletter", "weekly"}
NEWS_SITENAMES = {"daily", "wire", "times", "today", "news", "the "}
NEWS_TAGS = {"news", "headline", "press release", "announcement"}
BLOG_TAGS = {"blog", "newsletter", "analysis", "opinion", "review"}
PODCAST_SITENAMES = {"podcast", "show", "episode"}
PODCAST_TAGS = {"podcast", "episode", "show"}
SEC_FEED_KIND = {
    "https://www.sec.gov/news/pressreleases.rss": PRESS_RELEASE,
    "https://www.sec.gov/news/statements.rss": OFFICIAL_STATEMENT,
    "https://www.sec.gov/news/speeches-statements.rss": OFFICIAL_STATEMENT,
    "https://www.sec.gov/enforcement-litigation/administrative-proceedings/rss": ENFORCEMENT_ACTION,
    "https://www.sec.gov/enforcement-litigation/litigation-releases/rss": ENFORCEMENT_ACTION,
}

# factory/feeds.yaml has 199 GovInfo collection feeds. The feed slug is a
# stable, authoritative indicator of document family.
GOVINFO_FEED_KIND = (
    (re.compile(r"/rss/bills(?:-enr)?\.xml$"), LEGISLATIVE_BILL),
    (re.compile(r"/rss/plaw\.xml$"), ENACTED_LAW),
    (re.compile(r"/rss/(?:statute|uscode)\.xml$"), ENACTED_LAW),
    (re.compile(r"/rss/cfr\.xml$"), REGULATION),
    (re.compile(r"/rss/fr\.xml$"), RULEMAKING_NOTICE),
    (re.compile(r"/rss/uscourts-[a-z0-9]+\.xml$"), COURT_OPINION),
    (re.compile(r"/rss/usreports\.xml$"), COURT_OPINION),
    (re.compile(r"/rss/chrg\.xml$"), HEARING),
    (re.compile(r"/rss/(?:crec|crecb|hjournal|sjournal)\.xml$"), LEGISLATIVE_RECORD),
    (re.compile(r"/rss/(?:budget|erp)\.xml$"), BUDGET_DOCUMENT),
    (re.compile(r"/rss/(?:gaoreports|crpt|cprt)\.xml$"), GOVERNMENT_REPORT),
)

# Ordered from the most authoritative signal to the least.
URL_KIND_RULES = (
    (SEC_FILING, re.compile(r"(?:sec\.gov/(?:archives/edgar|ixviewer)|/edgar/data/)")),
    (PROCUREMENT_NOTICE, re.compile(r"sam\.gov/opp/")),
    (CONTRACT, re.compile(r"(?:sam\.gov/award/|usaspending\.gov/award/)")),
    (LEGISLATIVE_BILL, re.compile(r"(?:congress\.gov/(?:bill|legislation)/|govtrack\.us/congress/bills/|legiscan\.com/.*/bill/|govinfo\.gov/content/pkg/bills-)")),
    (ENACTED_LAW, re.compile(r"(?:congress\.gov/public-law/|govinfo\.gov/content/pkg/(?:plaw|statute|uscode)-)")),
    (REGULATION, re.compile(r"(?:ecfr\.gov/|govinfo\.gov/content/pkg/cfr-)")),
    (RULEMAKING_NOTICE, re.compile(r"(?:federalregister\.gov/documents/|regulations\.gov/(?:document|docket)/|govinfo\.gov/content/pkg/fr-)")),
    (COURT_OPINION, re.compile(r"(?:supremecourt\.gov/opinions/|courtlistener\.com/opinion/|law\.justia\.com/cases/|govinfo\.gov/content/pkg/(?:uscourts|usreports)-)")),
    (LAWSUIT, re.compile(r"(?:courtlistener\.com/docket/|pacer\.uscourts\.gov/)")),
    (EARNINGS_REPORT, re.compile(r"(?:/earnings(?:[-_/]|\?|\b)|/quarterly[-_/]?(?:results|earnings)|/financials/quarterly-results)")),
    (FINANCIAL_REPORT, re.compile(r"(?:/annual-reports?/|/financials/(?:annual|reports?))")),
    (RESEARCH_PAPER, re.compile(r"(?:arxiv\.org/(?:abs|pdf)/|doi\.org/10\.)")),
    (TECHNICAL_DOCUMENTATION, re.compile(r"(?:/docs?/(?:[^/]+/)?|readthedocs\.io/)")),
)

TITLE_KIND_RULES = (
    (SEC_FILING, re.compile(r"\b(?:form\s+(?:10-[kq]|8-k(?:/a)?|20-f|40-f|def\s*14a|s-[134])|(?:10-[kq]|8-k(?:/a)?|20-f|40-f|def\s*14a)\s+(?:filing|annual report|quarterly report))\b")),
    (EARNINGS_REPORT, re.compile(r"\b(?:q[1-4]|first|second|third|fourth)\s+(?:quarter\s+)?(?:\d{4}\s+)?(?:earnings|financial results)|earnings\s+(?:results|release|report)|quarterly\s+results|full[- ]year\s+results\b")),
    (FINANCIAL_REPORT, re.compile(r"\b(?:annual|quarterly|financial)\s+report\b|\bform\s+(?:10-k|10-q|20-f|40-f)\b")),
    (CONTRACT, re.compile(r"\b(?:master\s+(?:service|purchase)\s+agreement|(?:asset|purchase|employment|license|lease|credit|share)\s+agreement|definitive\s+agreement|contract\s+(?:award|agreement)|indenture)\b")),
    (LEGISLATIVE_PROPOSAL, re.compile(r"\b(?:draft\s+(?:bill|legislation)|proposed\s+(?:bill|legislation|act)|legislative\s+proposal)\b")),
    (LAWSUIT, re.compile(r"\b(?:class action|civil|antitrust)\s+(?:lawsuit|complaint)|\bcomplaint\s+(?:filed|against|for)\b|\b[a-z][\w.& -]+\s+v\.\s+[a-z]")),
    (COURT_OPINION, re.compile(r"\b(?:opinion of the court|court opinion|memorandum opinion|per curiam)\b")),
    (PRESS_RELEASE, re.compile(r"\b(?:press release|news release|media release)\b")),
    (OFFICIAL_STATEMENT, re.compile(r"\b(?:official\s+)?(?:statement|remarks|speech)\s+(?:by|from)\b")),
    (ENFORCEMENT_ACTION, re.compile(r"\b(?:enforcement action|administrative proceeding|litigation release|cease-and-desist order)\b")),
    (GOVERNMENT_REPORT, re.compile(r"\b(?:gao|inspector general|government accountability office)\s+report\b")),
    (WHITEPAPER, re.compile(r"\bwhite\s*paper\b")),
    (RESEARCH_PAPER, re.compile(r"\b(?:research|working)\s+paper\b")),
)

BODY_KIND_RULES = (
    (SEC_FILING, re.compile(r"\b(?:united states securities and exchange commission|form 10-[kq])\b")),
    (CONTRACT, re.compile(r"\bthis (?:agreement|contract) is (?:made|entered into)\b")),
    (FINANCIAL_REPORT, re.compile(r"\bconsolidated financial statements\b")),
    (COURT_OPINION, re.compile(r"\b(?:opinion of the court|memorandum opinion|per curiam)\b")),
    (WHITEPAPER, re.compile(r"\bwhite\s*paper\b")),
)

EXCLUDED_URL_PATTERNS = [
    r"\.(png|jpeg|jpg|gif|webp|mp4|avi|mkv|mp3|wav)$",
    r"(v\.redd\.it|i\.redd\.it|www\.reddit\.com\/gallery|youtube\.com|youtu\.be)",
    r"\/video(s)?\/",
    r"\/image(s)?\/",
    r"://[^/?#]+\.ru(?:[:/?#]|$)",
    r"://[^/?#]+\.su(?:[:/?#]|$)",
    r"(?:^|//|\.)(?:tass\.com|rt\.com|newsru\.com|russia-insider\.com|pravdareport\.com|sputniknews\.com|sputnikglobe\.com)(?:[:/]|$)",
]

HTML_CONTENT_TYPES = (
    "text/html",
    "application/xhtml+xml",
    "text/xml",
    "application/xml",
    "application/rss+xml",
    "application/atom+xml",
    "text/plain",
)
SCRAPABLE_CONTENT_TYPES = HTML_CONTENT_TYPES + ("application/pdf",)

EXCLUDED_AUTHORS = [
    "[no-author]", "noreply", "hidden", "admin", "isbpostadmin", "unknown", "anonymous",
]

def _text_value(value) -> str:
    if isinstance(value, str):
        return value.lower()
    if isinstance(value, (list, tuple, set)):
        return " ".join(_text_value(item) for item in value)
    return ""


def _matching_kind(rules, evidence: str) -> str | None:
    return next((kind for kind, pattern in rules if pattern.search(evidence)), None)


def _matching_govinfo_kind(evidence: str) -> str | None:
    return next((kind for pattern, kind in GOVINFO_FEED_KIND if pattern.search(evidence)), None)


def guess_content_type(bean: dict, feed_url: str = None) -> str | None:
    """Classify an item from authoritative feed/URL signals before text hints."""
    if not bean:
        return None

    feeds = (feed_url, bean.get(RSS_FEED))
    for feed in (_text_value(value) for value in feeds if value):
        if kind := SEC_FEED_KIND.get(feed):
            return kind
        if kind := _matching_govinfo_kind(feed):
            return kind

    url = _text_value(bean.get(URL))
    base_url = _text_value(bean.get(BASE_URL))
    if kind := _matching_kind(URL_KIND_RULES, f"{url} {base_url}"):
        return kind

    descriptor = " ".join(
        _text_value(bean.get(field))
        for field in (TITLE, SUMMARY, DESCRIPTION, TAGS)
    )
    if kind := _matching_kind(TITLE_KIND_RULES, descriptor):
        return kind

    content = _text_value(bean.get(CONTENT))
    if kind := _matching_kind(BODY_KIND_RULES, content):
        return kind

    domain_name = _text_value(bean.get(DOMAIN_NAME))
    site_name = _text_value(bean.get(SITE_NAME))
    if any(post_domain in domain_name for post_domain in POST_DOMAINS):
        return POST
    if any((blog_url in url) or (blog_url in base_url) for blog_url in BLOG_URLS):
        return BLOG
    if any(podcast_tag in descriptor for podcast_tag in PODCAST_TAGS) or any(
        podcast_name in site_name for podcast_name in PODCAST_SITENAMES
    ):
        return PODCAST
    if any(news_tag in descriptor for news_tag in NEWS_TAGS):
        return NEWS
    if any(blog_tag in descriptor for blog_tag in BLOG_TAGS):
        return BLOG
    if any(site in site_name for site in BLOG_SITENAMES):
        return BLOG
    if any(site in site_name for site in NEWS_SITENAMES) or "/news/" in url:
        return NEWS
    return None

@dataclass(frozen=True)
class ContentGate:
    excluded: bool
    is_pdf: bool
    max_size: int
    url: str
    charset: str


def is_pdf_content(content_type: str | None) -> bool:
    return bool(content_type and content_type.split(";")[0].strip().lower() == "application/pdf")


def is_pdf_url(url: str) -> bool:
    try:
        return urlparse(url).path.lower().endswith(".pdf")
    except Exception:
        return False


def is_pdf(url: str | None = None, content_type: str | None = None) -> bool:
    return is_pdf_content(content_type) or bool(url and is_pdf_url(url))


def is_excluded_content(response: ClientResponse, *, html_only: bool = False) -> ContentGate:
    url = str(response.url)
    content_type = response.content_type
    content_length = response.content_length
    charset = response.charset or "utf-8"
    is_pdf_doc = is_pdf(url, content_type)
    max_size = MAX_PDF_SIZE if is_pdf_doc else MAX_HTML_SIZE
    allowed = HTML_CONTENT_TYPES if html_only else SCRAPABLE_CONTENT_TYPES

    excluded = excluded_url(url)
    if not excluded and content_type:
        mime = content_type.split(";")[0].strip().lower()
        if mime and mime not in allowed:
            excluded = True
    if not excluded and (content_length or 0) > max_size:
        excluded = True

    return ContentGate(
        excluded=excluded,
        is_pdf=is_pdf_doc,
        max_size=max_size,
        url=url,
        charset=charset,
    )


def excluded_url(url: str) -> bool:
    return (not url) or any(re.search(pattern, url) for pattern in EXCLUDED_URL_PATTERNS)


def extract_base_url(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return None


def extract_domain(url: str) -> str:
    try:
        return tldextract.extract(url).domain
    except Exception:
        return None


def parse_date(date: str):
    try:
        parsed = ensure_utc(date_parser(date))
        return parsed if usable_created(parsed) else None
    except Exception:
        return None


def parse_int(val: str) -> int:
    try:
        return int(val)
    except Exception:
        return 0


_INVALID_XML_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def sanitize_html_for_xml(html: str) -> str:
    if not html:
        return html
    return _INVALID_XML_CHAR_RE.sub("", html)


def strip_html_tags(html):
    if not html:
        return None
    try:
        text = lxml.html.fromstring(html).text_content()
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html)
    return " ".join(text.split())


_HTML_TO_MD_OPTIONS = ConversionOptions(extract_metadata=False)
_HTML_TO_MD_PASSES = 4
_CDATA_RE = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL | re.IGNORECASE)
_FENCED_CODE_RE = re.compile(r"```.*?```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`[^`]+`")
_HTML_TAG_RE = re.compile(
    r"</?\s*(?:p|div|span|br|hr|ul|ol|li|h[1-6]|table|thead|tbody|tfoot|tr|td|th|"
    r"a|img|em|strong|b|i|u|s|font|center|blockquote|pre|code|section|article|"
    r"header|footer|nav|main|figure|figcaption|iframe|script|style|html|body|"
    r"head|meta|link|form|input|button|label|textarea|select|option|svg|video|"
    r"audio|source|picture|object|embed|aside|noscript|dl|dt|dd|small|sup|sub|"
    r"mark|del|ins|cite|q|abbr|time|address|details|summary|fieldset|legend|"
    r"optgroup|canvas|map|area|col|colgroup|caption|nobr|tt|kbd|samp|var)\b"
    r"(?:\s[^>]*)?/?>",
    re.IGNORECASE,
)
_MARKDOWN_BODY_FIELDS = (SUMMARY, CONTENT, DESCRIPTION)


def _unwrap_cdata(text: str) -> str:
    while True:
        unwrapped = _CDATA_RE.sub(r"\1", text)
        if unwrapped == text:
            return text
        text = unwrapped


def _text_outside_code(text: str) -> str:
    stripped = _FENCED_CODE_RE.sub("", text)
    return _INLINE_CODE_RE.sub("", stripped)


def _needs_html_conversion(text: str | None) -> bool:
    if not text:
        return False
    if _CDATA_RE.search(text):
        return True
    return bool(_HTML_TAG_RE.search(_text_outside_code(text)))


def _strip_html_tags_outside_code(text: str) -> str:
    parts = re.split(r"(```.*?```)", text, flags=re.DOTALL)
    cleaned = []
    for i, part in enumerate(parts):
        if i % 2:
            cleaned.append(part)
            continue
        subparts = re.split(r"(`[^`]+`)", part)
        cleaned.extend(
            sp if j % 2 else _HTML_TAG_RE.sub("", sp)
            for j, sp in enumerate(subparts)
        )
    return "".join(cleaned).strip()


def _converted_markdown(html: str) -> str:
    result = convert(html, _HTML_TO_MD_OPTIONS)
    md = getattr(result, "content", None)
    if md is None and isinstance(result, dict):
        md = result.get("content")
    elif md is None and isinstance(result, str):
        md = result
    return (md or "").strip()


def html_to_markdown(html: str | None) -> str | None:
    if html is None or not str(html).strip():
        return None
    text = _unwrap_cdata(str(html))
    md = None
    try:
        for _ in range(_HTML_TO_MD_PASSES):
            md = _converted_markdown(text)
            if not md:
                return None
            if not _needs_html_conversion(md):
                return md
            text = _unwrap_cdata(md)
    except Exception:
        return strip_html_tags(html)
    return _strip_html_tags_outside_code(md) or None


def full_url(base_url: str, target_url: str) -> str:
    return urljoin(base_url, target_url)


def remove_query_params(url: str) -> str:
    try:
        return urlunparse(urlparse(url)._replace(query="", fragment=""))
    except Exception:
        return url


_TRACKING_QUERY_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "utm_id",
    "fbclid", "gclid", "gclsrc", "dclid", "msclkid", "twclid", "igshid",
    "mc_cid", "mc_eid", "_hsenc", "_hsmi", "mkt_tok",
    "at_medium", "at_campaign", "at_source",
    "source",
}


def _is_tracking_param(key: str) -> bool:
    k = (key or "").lower()
    return k.startswith("utm_") or k in _TRACKING_QUERY_KEYS


def _host_key(netloc: str) -> str:
    n = (netloc or "").lower()
    return n[4:] if n.startswith("www.") else n


def _path_segments(path: str) -> list[str]:
    return [p for p in (path or "").split("/") if p]


def strip_tracking_params(url: str) -> str:
    if not url:
        return url
    try:
        parts = urlparse(url)
        kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if not _is_tracking_param(k)]
        return urlunparse(parts._replace(query=urlencode(kept, doseq=True), fragment=""))
    except Exception:
        return url


def is_compatible_content_url(retrieval: str, candidate: str) -> bool:
    """True when candidate is the same article: same host, same or deeper path."""
    if not retrieval or not candidate:
        return False
    try:
        r, c = urlparse(retrieval), urlparse(candidate)
    except Exception:
        return False
    if _host_key(r.netloc) != _host_key(c.netloc):
        return False
    rpath = (r.path or "/").rstrip("/") or "/"
    cpath = (c.path or "/").rstrip("/") or "/"
    rsegs, csegs = _path_segments(rpath), _path_segments(cpath)
    if len(csegs) < len(rsegs):
        return False
    if rpath == cpath:
        return True
    if rpath != "/" and cpath.startswith(rpath + "/"):
        return True
    return len(csegs) == len(rsegs) and csegs[:-1] == rsegs[:-1]


def _merge_material_query(retrieval: str, canonical: str) -> str:
    r, c = urlparse(retrieval), urlparse(canonical)
    r_mat = {k: v for k, v in parse_qsl(r.query, keep_blank_values=True) if not _is_tracking_param(k)}
    c_mat = {k: v for k, v in parse_qsl(c.query, keep_blank_values=True) if not _is_tracking_param(k)}
    merged = {**r_mat, **c_mat}
    query = urlencode(list(merged.items()), doseq=True)
    return urlunparse(c._replace(query=query, fragment=""))


def resolve_content_url(retrieval: str, meta_url: str | None = None, final_url: str | None = None) -> str:
    """Prefer canonical/final URL only when it is the same article; keep material query params."""
    for candidate in (meta_url, final_url):
        if candidate and is_compatible_content_url(retrieval, candidate):
            return _merge_material_query(retrieval, candidate)
    return strip_tracking_params(retrieval) if retrieval else retrieval


def with_www(url: str) -> str | None:
    try:
        parts = urlparse(url)
        if parts.netloc and not parts.netloc.startswith("www."):
            return urlunparse(parts._replace(netloc="www." + parts.netloc))
    except Exception:
        pass
    return None


extract_source = lambda url: (extract_domain(url) or extract_base_url(url)).strip().lower()
count_words = lambda text: min(len(text.split()) if text else 0, (1 << 15) - 1)
cleanup_url = lambda url: url.strip().lower() if url and url.strip() else None
cleanup_text = lambda text: text.strip() if text and text.strip() else None
cleanup_author = lambda author: cleanup_text(author) if author and author.lower() not in EXCLUDED_AUTHORS else None

_LANGUAGE_QUOTE_RE = re.compile(r"[\"'`“”‘’]")
_LANGUAGE_KEBAB_RE = re.compile(r"[^a-z0-9]+")


def cleanup_language(value: str | None) -> str | None:
    if not value:
        return None
    text = _LANGUAGE_QUOTE_RE.sub("", str(value).strip().lower())
    text = text.split(",", 1)[0].strip()
    text = _LANGUAGE_KEBAB_RE.sub("-", text).strip("-")
    return text or None

def cleanup_item(item: dict) -> dict:
    if not item: return item

    if not item.get(BASE_URL) and item.get(URL):
        item[BASE_URL] = extract_base_url(item[URL])

    for text_field in (
        KIND, DOMAIN_NAME, PLATFORM, TITLE, SUMMARY, CONTENT, AUTHOR,
        CHATTER_URL, BASE_URL, SITE_NAME, DESCRIPTION, LANGUAGE,
        ARTICLE_LANGUAGE, SITE_LANGUAGE, AUTHOR_EMAIL, FORUM,
    ):
        if not (value := item.get(text_field)):
            continue
        if text_field in (LANGUAGE, ARTICLE_LANGUAGE):
            item[text_field] = cleanup_language(value)
        elif text_field in _MARKDOWN_BODY_FIELDS and _needs_html_conversion(value):
            item[text_field] = html_to_markdown(value)
        elif text_field == TITLE and _HTML_TAG_RE.search(value):
            item[text_field] = strip_html_tags(value)
        else:
            item[text_field] = cleanup_text(value)

    for url_field in (URL, BASE_URL, FAVICON, RSS_FEED, IMAGE_URL, DOMAIN_NAME, CHATTER_URL):
        if value := item.get(url_field):
            item[url_field] = cleanup_url(value)

    item[AUTHOR] = cleanup_author(item.get(AUTHOR))
    item[COLLECTED] = item.get(COLLECTED) or now()
    item[CREATED] = ensure_utc(item.get(CREATED) if usable_created(item.get(CREATED)) else item[COLLECTED])
    item[TAGS] = item.get(TAGS)
    item[TITLE_LENGTH] = count_words(item.get(TITLE))
    item[SUMMARY_LENGTH] = count_words(item.get(SUMMARY))
    item[CONTENT_LENGTH] = count_words(item.get(CONTENT))

    return item
