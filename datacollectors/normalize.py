import re
import unicodedata

import lxml.html
import tldextract
from aiohttp import ClientResponse
from dataclasses import dataclass, replace
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
    TYPE,
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
from utils.kinds import (
    POST,
    BLOG,
    NEWS,
    SITE,
    JOB,
    PODCAST,
    CONTRACT,
    PROCUREMENT_NOTICE,
    FINANCIAL_REPORT,
    EARNINGS_REPORT,
    SEC_FILING,
    PRESS_RELEASE,
    OFFICIAL_STATEMENT,
    ENFORCEMENT_ACTION,
    LEGISLATIVE_BILL,
    LEGISLATIVE_PROPOSAL,
    ENACTED_LAW,
    REGULATION,
    RULEMAKING_NOTICE,
    COURT_OPINION,
    LAWSUIT,
    GOVERNMENT_REPORT,
    BUDGET_DOCUMENT,
    LEGISLATIVE_RECORD,
    HEARING,
    RESEARCH_PAPER,
    WHITEPAPER,
    TECHNICAL_DOCUMENTATION,
)
import utils.kinds as _kinds_mod

CANONICAL_KINDS = frozenset(
    value for name, value in vars(_kinds_mod).items()
    if name.isupper() and isinstance(value, str)
)
NON_NEWS_KINDS = CANONICAL_KINDS - {NEWS}

KIND_CONTEXT_KEY = "_kind_context"
KIND_DECISION_KEY = "_kind_decision"
_POLICY_MODES = frozenset({"unknown", "reporting", "mixed", "non_news"})
_HTTP_SCHEMES = frozenset({"http", "https"})
_SEC_HOSTS = frozenset({"sec.gov", "www.sec.gov"})
_GOVINFO_HOSTS = frozenset({"govinfo.gov", "www.govinfo.gov"})
_GITHUB_HOSTS = frozenset({"github.com", "www.github.com"})
_ARXIV_HOSTS = frozenset({"arxiv.org", "www.arxiv.org"})
_IACR_HOSTS = frozenset({"eprint.iacr.org"})
_NEWSARTICLE_TYPES = frozenset({"newsarticle"})
_BLOG_SCHEMA_TYPES = frozenset({"blogposting"})
_PODCAST_SCHEMA_TYPES = frozenset({"podcastepisode"})
_BLOG_LABELS = frozenset({
    "blog", "opinion", "editorial", "analysis", "review", "reviews",
    "tutorial", "tutorials", "how-to", "guide", "guides", "newsletter",
    "changelog", "release notes",
})
_PODCAST_LABELS = frozenset({"podcast", "podcasts", "podcast episode"})
_BLOG_PATH_SEGMENTS = frozenset({
    "opinion", "editorials", "reviews", "tutorials", "how-to", "newsletters", "changelog",
})
_PODCAST_PATH_SEGMENTS = frozenset({"podcasts"})
_RELEASE_LABELS = ("press release:", "news release:", "media release:")
_RELEASE_TAGS = frozenset({"press release", "news release", "media release"})
_OPENING_MAX_CHARS = 240

SEC_DOCUMENT_FEEDS = {
    "https://www.sec.gov/news/statements.rss": OFFICIAL_STATEMENT,
    "https://www.sec.gov/news/speeches-statements.rss": OFFICIAL_STATEMENT,
    "https://www.sec.gov/enforcement-litigation/administrative-proceedings/rss": ENFORCEMENT_ACTION,
    "https://www.sec.gov/enforcement-litigation/litigation-releases/rss": ENFORCEMENT_ACTION,
}

# factory/feeds.yaml has 199 GovInfo collection feeds. The feed slug is a
# stable, authoritative indicator of document family.
GOVINFO_FEED_KIND = (
    (re.compile(r"/rss/bills(?:-enr)?\.xml$", re.IGNORECASE), LEGISLATIVE_BILL),
    (re.compile(r"/rss/plaw\.xml$", re.IGNORECASE), ENACTED_LAW),
    (re.compile(r"/rss/(?:statute|uscode)\.xml$", re.IGNORECASE), ENACTED_LAW),
    (re.compile(r"/rss/cfr\.xml$", re.IGNORECASE), REGULATION),
    (re.compile(r"/rss/fr\.xml$", re.IGNORECASE), RULEMAKING_NOTICE),
    (re.compile(r"/rss/uscourts-[a-z0-9]+\.xml$", re.IGNORECASE), COURT_OPINION),
    (re.compile(r"/rss/usreports\.xml$", re.IGNORECASE), COURT_OPINION),
    (re.compile(r"/rss/chrg\.xml$", re.IGNORECASE), HEARING),
    (re.compile(r"/rss/(?:crec|crecb|hjournal|sjournal)\.xml$", re.IGNORECASE), LEGISLATIVE_RECORD),
    (re.compile(r"/rss/(?:budget|erp)\.xml$", re.IGNORECASE), BUDGET_DOCUMENT),
    (re.compile(r"/rss/(?:gaoreports|crpt|cprt)\.xml$", re.IGNORECASE), GOVERNMENT_REPORT),
)

_GOVINFO_PKG_PREFIXES = (
    (LEGISLATIVE_BILL, ("bills-",)),
    (ENACTED_LAW, ("plaw-", "statute-", "uscode-")),
    (REGULATION, ("cfr-",)),
    (RULEMAKING_NOTICE, ("fr-",)),
    (COURT_OPINION, ("uscourts-", "usreports-")),
)

# Host-plus-path document rules. Prefixes use segment-boundary matching unless
# noted. Paths listed here are matched case-insensitively (SEC/GovInfo/court hosts).
_DOCUMENT_PREFIX_RULES = (
    (SEC_FILING, _SEC_HOSTS, ("/archives/edgar", "/ixviewer"), True),
    (PROCUREMENT_NOTICE, frozenset({"sam.gov", "www.sam.gov"}), ("/opp",), True),
    (CONTRACT, frozenset({"sam.gov", "www.sam.gov"}), ("/award",), True),
    (CONTRACT, frozenset({"usaspending.gov", "www.usaspending.gov"}), ("/award",), True),
    (LEGISLATIVE_BILL, frozenset({"congress.gov", "www.congress.gov"}), ("/bill", "/legislation"), True),
    (LEGISLATIVE_BILL, frozenset({"govtrack.us", "www.govtrack.us"}), ("/congress/bills",), True),
    (ENACTED_LAW, frozenset({"congress.gov", "www.congress.gov"}), ("/public-law",), True),
    (REGULATION, frozenset({"ecfr.gov", "www.ecfr.gov"}), ("/",), True),
    (RULEMAKING_NOTICE, frozenset({"federalregister.gov", "www.federalregister.gov"}), ("/documents",), True),
    (RULEMAKING_NOTICE, frozenset({"regulations.gov", "www.regulations.gov"}), ("/document", "/docket"), True),
    (COURT_OPINION, frozenset({"supremecourt.gov", "www.supremecourt.gov"}), ("/opinions",), True),
    (COURT_OPINION, frozenset({"www.courtlistener.com", "courtlistener.com"}), ("/opinion",), True),
    (COURT_OPINION, frozenset({"law.justia.com", "www.law.justia.com"}), ("/cases",), True),
    (LAWSUIT, frozenset({"www.courtlistener.com", "courtlistener.com"}), ("/docket",), True),
)

_ARXIV_ABS_RE = re.compile(r"^/abs/[^/]+$", re.IGNORECASE)
_ARXIV_PDF_RE = re.compile(r"^/pdf/[^/]+?(?:\.pdf)?$", re.IGNORECASE)
_IACR_RE = re.compile(r"^/\d{4}/\d+(?:\.pdf)?$", re.IGNORECASE)
_LEGISCAN_HOSTS = frozenset({"legiscan.com", "www.legiscan.com"})
_EARNINGS_TITLE_RE = re.compile(
    r"\b(?:q[1-4]|first|second|third|fourth)\s+(?:quarter\s+)?(?:\d{4}\s+)?"
    r"(?:earnings|financial results)|earnings\s+(?:results|release|report)|"
    r"quarterly\s+results|full[- ]year\s+results\b",
    re.IGNORECASE,
)
_CONTRACT_TITLE_RE = re.compile(
    r"\b(?:master\s+(?:service|purchase)\s+agreement|"
    r"(?:asset|purchase|employment|license|lease|credit|share)\s+agreement|"
    r"definitive\s+agreement|contract\s+(?:award|agreement)|indenture)\b",
    re.IGNORECASE,
)
_CONTRACT_OPENING_RE = re.compile(
    r"^this (?:agreement|contract) is (?:made|entered into)\b",
    re.IGNORECASE,
)
_FINANCIAL_REPORT_TITLE_PREFIXES = ("annual report", "financial report")

POST_DOMAINS = {"reddit", "redd", "linkedin", "x", "twitter", "facebook", "ycombinator"}

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

def _idna_host(host: str | None) -> str | None:
    if not host:
        return None
    text = host.strip().rstrip(".").casefold()
    if not text:
        return None
    try:
        return text.encode("idna").decode("ascii")
    except (UnicodeError, ValueError):
        return text


def normalize_label(value: str | None) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    return " ".join(text.casefold().split())


def normalize_feed_key(url: str) -> str:
    if not url or not isinstance(url, str) or not url.strip():
        raise ValueError(f"malformed feed URL: {url!r}")
    parsed = urlparse(url.strip())
    scheme = (parsed.scheme or "").lower()
    if scheme not in _HTTP_SCHEMES:
        raise ValueError(f"non-HTTP(S) feed URL: {url}")
    if parsed.username or parsed.password:
        raise ValueError(f"credentials are not allowed in feed URL: {url}")
    host = _idna_host(parsed.hostname)
    if not host:
        raise ValueError(f"malformed feed URL: {url}")
    netloc = f"{host}:{parsed.port}" if parsed.port else host
    return urlunparse((scheme, netloc, parsed.path, parsed.params, parsed.query, ""))


def try_normalize_feed_key(url: str | None) -> str | None:
    try:
        return normalize_feed_key(url) if url else None
    except (TypeError, ValueError):
        return None


def feed_identity(url: str) -> str:
    parsed = urlparse((url or "").strip())
    scheme = (parsed.scheme or "").lower()
    host = _idna_host(parsed.hostname) or (parsed.hostname or "").casefold()
    netloc = f"{host}:{parsed.port}" if parsed.port else host
    return urlunparse((scheme, netloc, parsed.path, parsed.params, parsed.query, ""))


def normalize_policy_host(host: str) -> str:
    if not host or not isinstance(host, str):
        raise ValueError(f"host must be an exact DNS hostname: {host!r}")
    text = host.strip()
    if any(token in text for token in ("://", "/", "?", "#", "*", "\\", "@")):
        raise ValueError(f"host must be an exact DNS hostname: {host}")
    normalized = _idna_host(text)
    if not normalized:
        raise ValueError(f"host must be an exact DNS hostname: {host}")
    return normalized


def validate_news_path(path: str) -> str:
    if not isinstance(path, str) or not path.startswith("/") or path == "/":
        raise ValueError(f"news_paths must start with '/' and cannot be '/': {path!r}")
    if "?" in path or "#" in path:
        raise ValueError(f"news_paths cannot contain query or fragment: {path}")
    return path


def _path_matches_prefix(path: str, prefix: str, *, ignore_case: bool = False) -> bool:
    candidate = path.casefold() if ignore_case else path
    needle = prefix.casefold() if ignore_case else prefix
    trimmed = needle.rstrip("/")
    if not trimmed:
        return True
    return candidate == trimmed or candidate.startswith(trimmed + "/")


def _path_segments(path: str) -> tuple[str, ...]:
    return tuple(segment for segment in (path or "").split("/") if segment)


@dataclass(frozen=True, order=True)
class KindPolicy:
    mode: str = "unknown"
    kind_hint: str | None = None
    hosts: tuple[str, ...] = ()
    news_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class KindContext:
    origin: str = "unknown"
    feed_url: str | None = None
    publisher_url: str | None = None
    feed_title: str | None = None
    feed_description: str | None = None
    policy: KindPolicy = KindPolicy()
    native_type: str | None = None
    is_self_post: bool = False
    rss_tags: tuple[str, ...] = ()
    schema_types: tuple[str, ...] = ()
    article_sections: tuple[str, ...] = ()


@dataclass(frozen=True)
class KindDecision:
    kind: str
    rule_id: str
    evidence_fields: tuple[str, ...]


def _conflict_decision(fields: tuple[str, ...]) -> KindDecision:
    return KindDecision(BLOG, "conflicting_evidence", fields)


def _article_url(bean: dict):
    raw = bean.get(URL)
    if not raw or not isinstance(raw, str):
        return None
    try:
        parsed = urlparse(raw)
    except Exception:
        return None
    if (parsed.scheme or "").lower() not in _HTTP_SCHEMES:
        return None
    if not parsed.hostname:
        return None
    return parsed


def _article_host(bean: dict) -> str | None:
    parsed = _article_url(bean)
    return _idna_host(parsed.hostname) if parsed else None


def _article_path(bean: dict) -> str:
    parsed = _article_url(bean)
    if not parsed:
        return ""
    return parsed.path or ""


def _feed_host(feed_url: str | None) -> str | None:
    if not feed_url:
        return None
    try:
        parsed = urlparse(feed_url)
    except Exception:
        return None
    return _idna_host(parsed.hostname)


def _title_text(bean: dict) -> str:
    value = bean.get(TITLE)
    return value if isinstance(value, str) else ""


def _normalized_title(bean: dict) -> str:
    return normalize_label(_title_text(bean))


def _opening_line(bean: dict) -> str:
    content = bean.get(CONTENT)
    if not isinstance(content, str) or not content.strip():
        return ""
    for line in content.splitlines():
        text = line.strip()
        if not text or text.startswith(">"):
            continue
        return text[:_OPENING_MAX_CHARS]
    return ""


def _normalized_opening(bean: dict) -> str:
    return normalize_label(_opening_line(bean))


def _label_set(*groups) -> frozenset[str]:
    labels = []
    for group in groups:
        if not group:
            continue
        if isinstance(group, str):
            group = (group,)
        for item in group:
            if item is None:
                continue
            normalized = normalize_label(str(item))
            if normalized:
                labels.append(normalized)
    return frozenset(labels)


def _schema_names(types: tuple[str, ...]) -> frozenset[str]:
    names = []
    for raw in types or ():
        text = str(raw).strip()
        if "/" in text:
            text = text.rsplit("/", 1)[-1]
        normalized = normalize_label(text).replace(" ", "")
        if normalized:
            names.append(normalized)
    return frozenset(names)


def _legacy_fallback_kind(default_kind: str | None) -> str | None:
    if not default_kind or default_kind == NEWS:
        return None
    if default_kind in NON_NEWS_KINDS:
        return default_kind
    return None


def _policy_host_eligible(bean: dict, ctx: KindContext) -> bool:
    host = _article_host(bean)
    if not host:
        return False
    if ctx.policy.hosts:
        return host in ctx.policy.hosts
    trusted_hosts = {
        candidate
        for candidate in (_feed_host(ctx.feed_url), _feed_host(ctx.publisher_url))
        if candidate
    }
    return host in trusted_hosts


def _compatible_destination(bean: dict, official_hosts: frozenset[str]) -> bool:
    host = _article_host(bean)
    return bool(host) and host in official_hosts


def _normalize_context(context, feed_url) -> KindContext:
    ctx = KindContext() if context is None else context
    if not isinstance(ctx, KindContext):
        raise TypeError("context must be KindContext")
    if feed_url and ctx.feed_url:
        left = try_normalize_feed_key(feed_url) or feed_url
        right = try_normalize_feed_key(ctx.feed_url) or ctx.feed_url
        if left != right:
            raise ValueError(f"conflicting feed_url arguments: {feed_url!r} vs {ctx.feed_url!r}")
    if feed_url and not ctx.feed_url:
        ctx = replace(ctx, feed_url=feed_url)
    return ctx


def _native_item_decision(bean: dict, ctx: KindContext) -> KindDecision | None:
    native_type = normalize_label(ctx.native_type)
    title = _normalized_title(bean)
    if ctx.origin == "hackernews" and native_type == JOB:
        return KindDecision(JOB, "native_hn_job", ("native_type",))
    if ctx.origin in {"reddit", "hackernews"} and ctx.is_self_post:
        return KindDecision(POST, "native_self_post", ("is_self_post",))
    if ctx.origin == "hackernews" and not ctx.is_self_post and title.startswith("show hn:"):
        return KindDecision(SITE, "native_hn_show", ("title",))
    return None


def _document_url_kinds(bean: dict) -> list[tuple[str, str]]:
    parsed = _article_url(bean)
    if not parsed:
        return []
    host = _idna_host(parsed.hostname)
    path = parsed.path or ""
    matches = []
    for kind, hosts, prefixes, ignore_case in _DOCUMENT_PREFIX_RULES:
        if host not in hosts:
            continue
        if any(_path_matches_prefix(path, prefix, ignore_case=ignore_case) for prefix in prefixes):
            matches.append((kind, f"document_url:{kind}"))
    if host in _ARXIV_HOSTS and (_ARXIV_ABS_RE.match(path) or _ARXIV_PDF_RE.match(path)):
        matches.append((RESEARCH_PAPER, f"document_url:{RESEARCH_PAPER}"))
    if host in _IACR_HOSTS and _IACR_RE.match(path):
        matches.append((RESEARCH_PAPER, f"document_url:{RESEARCH_PAPER}"))
    if host in _GOVINFO_HOSTS:
        lowered = path.casefold()
        pkg = "/content/pkg/"
        if lowered.startswith(pkg):
            remainder = lowered[len(pkg):]
            for kind, prefixes in _GOVINFO_PKG_PREFIXES:
                if remainder.startswith(prefixes):
                    matches.append((kind, f"document_url:{kind}"))
                    break
    if host in _LEGISCAN_HOSTS:
        segments = {segment.casefold() for segment in _path_segments(path)}
        if "bill" in segments:
            matches.append((LEGISLATIVE_BILL, f"document_url:{LEGISLATIVE_BILL}"))
    return matches


def _document_feed_decision(bean: dict, ctx: KindContext) -> KindDecision | None:
    feed_url = ctx.feed_url
    normalized = try_normalize_feed_key(feed_url)
    feed_host = _feed_host(feed_url)
    if not feed_host or not normalized:
        return None
    if feed_host in _SEC_HOSTS:
        kind = SEC_DOCUMENT_FEEDS.get(normalized)
        if kind and _compatible_destination(bean, _SEC_HOSTS):
            return KindDecision(kind, f"document_feed:{kind}", ("feed_url", "url"))
        return None
    if feed_host in _GOVINFO_HOSTS and _compatible_destination(bean, _GOVINFO_HOSTS):
        path = urlparse(normalized).path or ""
        for pattern, kind in GOVINFO_FEED_KIND:
            if pattern.search(path) or pattern.search(normalized):
                return KindDecision(kind, f"document_feed:{kind}", ("feed_url", "url"))
    return None


def _explicit_document_decision(bean: dict, ctx: KindContext) -> KindDecision | None:
    title = _title_text(bean)
    normalized_title = _normalized_title(bean)
    opening = _normalized_opening(bean)
    tags = _label_set(ctx.rss_tags, ctx.article_sections, bean.get(TAGS))
    eligible = _policy_host_eligible(bean, ctx)
    press_policy = (
        eligible
        and ctx.policy.mode == "non_news"
        and ctx.policy.kind_hint == PRESS_RELEASE
    )
    earnings_policy = (
        eligible
        and ctx.policy.mode == "non_news"
        and ctx.policy.kind_hint in {PRESS_RELEASE, EARNINGS_REPORT}
    )
    financial_policy = (
        eligible
        and ctx.policy.mode == "non_news"
        and ctx.policy.kind_hint == FINANCIAL_REPORT
    )
    if earnings_policy and title and _EARNINGS_TITLE_RE.search(title):
        return KindDecision(EARNINGS_REPORT, "item_earnings", ("title", "policy"))
    financial_from_press = press_policy and any(
        normalized_title.startswith(prefix) for prefix in _FINANCIAL_REPORT_TITLE_PREFIXES
    )
    if financial_policy or financial_from_press:
        return KindDecision(FINANCIAL_REPORT, "item_financial_report", ("title", "policy"))
    title_release = any(normalized_title.startswith(label) for label in _RELEASE_LABELS)
    opening_release = any(opening.startswith(label) for label in _RELEASE_LABELS)
    if (title_release or opening_release) and (press_policy or bool(tags & _RELEASE_TAGS)):
        fields = []
        if title_release:
            fields.append("title")
        if opening_release:
            fields.append("content")
        if press_policy:
            fields.append("policy")
        if tags & _RELEASE_TAGS:
            fields.append("tags")
        return KindDecision(PRESS_RELEASE, "item_release", tuple(fields))
    if opening and _CONTRACT_OPENING_RE.search(opening) and title and _CONTRACT_TITLE_RE.search(title):
        return KindDecision(CONTRACT, "item_contract", ("title", "content"))
    return None


def _primary_document_decision(bean: dict, ctx: KindContext) -> KindDecision | None:
    if ctx.origin == "sec_edgar":
        return KindDecision(SEC_FILING, "native_sec_filing", ("origin",))
    url_matches = _document_url_kinds(bean)
    if url_matches:
        kinds = {kind for kind, _rule in url_matches}
        if len(kinds) > 1:
            return _conflict_decision(("url",))
        kind, rule_id = url_matches[0]
        return KindDecision(kind, rule_id, ("url",))
    if feed_decision := _document_feed_decision(bean, ctx):
        return feed_decision
    return _explicit_document_decision(bean, ctx)


def _explicit_format_decision(bean: dict, ctx: KindContext) -> KindDecision | None:
    labels = _label_set(ctx.rss_tags, ctx.article_sections, bean.get(TAGS))
    schema = _schema_names(ctx.schema_types)
    path = _article_path(bean)
    segments = {segment.casefold() for segment in _path_segments(path)}
    host = _article_host(bean)
    podcast = bool(labels & _PODCAST_LABELS) or bool(schema & _PODCAST_SCHEMA_TYPES) or bool(segments & _PODCAST_PATH_SEGMENTS)
    blog = (
        bool(labels & _BLOG_LABELS)
        or bool(schema & _BLOG_SCHEMA_TYPES)
        or bool(segments & _BLOG_PATH_SEGMENTS)
    )
    if podcast:
        return KindDecision(PODCAST, "format_podcast", ("tags", "schema_types", "url"))
    if blog:
        return KindDecision(BLOG, "format_blog", ("tags", "schema_types", "url"))
    if host in _GITHUB_HOSTS:
        segs = _path_segments(path)
        if len(segs) >= 3 and segs[2].casefold() == "releases":
            return KindDecision(BLOG, "repository_release", ("url",))
        if len(segs) == 2:
            return KindDecision(SITE, "repository_site", ("url",))
    return None


def _source_policy_decision(bean: dict, ctx: KindContext, default_kind: str | None) -> KindDecision:
    eligible = _policy_host_eligible(bean, ctx)
    policy = ctx.policy
    if eligible and policy.mode == "non_news" and policy.kind_hint in NON_NEWS_KINDS:
        return KindDecision(policy.kind_hint, "policy_non_news", ("policy", "url"))
    if eligible and policy.mode == "reporting":
        return KindDecision(NEWS, "policy_reporting", ("policy", "url"))
    if eligible and policy.mode == "mixed":
        path = _article_path(bean)
        if any(_path_matches_prefix(path, prefix, ignore_case=False) for prefix in policy.news_paths):
            return KindDecision(NEWS, "mixed_reporting_path", ("url", "policy"))
        if _schema_names(ctx.schema_types) & _NEWSARTICLE_TYPES:
            return KindDecision(NEWS, "mixed_news_schema", ("schema_types", "policy"))
    if kind := _legacy_fallback_kind(default_kind):
        return KindDecision(kind, "legacy_non_news_fallback", ("default_kind",))
    return KindDecision(BLOG, "fallback_unresolved", ())


def guess_content_type(bean, feed_url=None, default_kind=None, *, context=None, explain=False):
    if not bean:
        return None
    ctx = _normalize_context(context, feed_url)
    decision = (
        _native_item_decision(bean, ctx)
        or _primary_document_decision(bean, ctx)
        or _explicit_format_decision(bean, ctx)
        or _source_policy_decision(bean, ctx, default_kind)
    )
    return decision if explain else decision.kind


def apply_kind_decision(item: dict, *, context: KindContext, feed_url=None, default_kind=None) -> dict:
    decision = guess_content_type(
        item, feed_url=feed_url, default_kind=default_kind, context=context, explain=True
    )
    if decision is None:
        return item
    item[KIND] = decision.kind
    item[KIND_CONTEXT_KEY] = context
    item[KIND_DECISION_KEY] = decision
    return item

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


_TITLE_SITE_SEPARATOR_RE = re.compile(r"\s*(?:\||:|/|»|[–—]|(?<=\s)-(?=\s))\s*")
_TITLE_SITE_DOMAIN_SUFFIX_RE = re.compile(
    r"\.(?:com|org|net|io|ai|co|tv|fm|news|us|uk|ca|au|in)(?:\.[a-z]{2})?$",
    re.IGNORECASE,
)
_TITLE_SITE_WRAPPERS = (("[", "]"), ("(", ")"), ("{", "}"), ("\"", "\""), ("“", "”"))


def _title_site_key(value: str | None) -> str:
    text = str(value or "").strip().casefold()
    text = re.sub(r"^[a-z]+://", "", text)
    text = text.split("/", 1)[0]
    text = re.sub(r"^www\.", "", text)
    text = _TITLE_SITE_DOMAIN_SUFFIX_RE.sub("", text)
    return re.sub(r"[\W_]+", " ", text, flags=re.UNICODE).strip()


def _is_title_site_label(value: str, site_key: str) -> bool:
    candidate = str(value or "").strip().strip("[](){}<>\"\x27“”‘’")
    return bool(candidate) and _title_site_key(candidate) == site_key


def _strip_delimited_title_site(text: str, site_key: str) -> str:
    while True:
        separator = _TITLE_SITE_SEPARATOR_RE.search(text)
        if not separator or not _is_title_site_label(text[:separator.start()], site_key):
            break
        text = text[separator.end():].lstrip()

    while True:
        separators = list(_TITLE_SITE_SEPARATOR_RE.finditer(text))
        if not separators:
            break
        separator = separators[-1]
        if not _is_title_site_label(text[separator.end():], site_key):
            break
        text = text[:separator.start()].rstrip()
    return text


def _strip_wrapped_title_site(text: str, site_key: str) -> str:
    leading = text.lstrip()
    for opener, closer in _TITLE_SITE_WRAPPERS:
        if not leading.startswith(opener):
            continue
        end = leading.find(closer, len(opener))
        if end < 0 or not _is_title_site_label(leading[len(opener):end], site_key):
            continue
        text = leading[end + len(closer):].lstrip()
        return re.sub(r"^(?:\||:|/|»|[–—]|-)\s*", "", text)

    trailing = text.rstrip()
    for opener, closer in _TITLE_SITE_WRAPPERS:
        if not trailing.endswith(closer):
            continue
        start = trailing.rfind(opener, 0, len(trailing) - len(closer))
        if start < 0 or not _is_title_site_label(trailing[start + len(opener):-len(closer)], site_key):
            continue
        return trailing[:start].rstrip()
    return text


def cleanup_title(title: str | None, site_name: str | None = None) -> str | None:
    """Trim a publisher label from a delimited title prefix or suffix."""
    text = cleanup_text(title)
    site_key = _title_site_key(site_name)
    if not text or not site_key:
        return text

    for _ in range(2):
        cleaned = _strip_wrapped_title_site(_strip_delimited_title_site(text, site_key), site_key)
        if cleaned == text:
            break
        text = cleaned
    return cleanup_text(text)


def cleanup_language(value: str | None, content: str | None = None) -> str | None:
    if not value:
        if not content or not content.strip():
            return None
        from ftlangdetect import detect

        value = detect(text=content, low_memory=True)["lang"]
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

    if TITLE in item:
        item[TITLE] = cleanup_title(item.get(TITLE), item.get(SITE_NAME))

    if not any(item.get(key) for key in (LANGUAGE, ARTICLE_LANGUAGE, SITE_LANGUAGE)):
        content = item.get(CONTENT) or item.get(SUMMARY) or item.get(TITLE)
        if language := cleanup_language(None, content):
            item[LANGUAGE] = language

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
