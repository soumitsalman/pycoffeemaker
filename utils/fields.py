# Canonical bean / article / publisher field names (JSON keys)

_ID = "_id"
ID = "id"
URL = "url"
KIND = "kind"
CATEGORIES = "categories"
SENTIMENTS = "sentiments"
TAGS = "tags"
TITLE = "title"
CONTENT = "content"
SOURCE = "source"
CHATTER_GROUP = "group"
EMBEDDING = "embedding"
GIST = "gist"
SUMMARY = "summary"
REGIONS = "regions"
ENTITIES = "entities"
PEOPLE = "people"
PRODUCTS = "products"
COMPANIES = "companies"
STOCK_TICKERS = "stock_tickers"
UPDATED = "updated"
COLLECTED = "collected"
CLUSTER_ID = "cluster_id"
CLUSTER_SIZE = "cluster_size"
HIGHLIGHTS = "highlights"
IMAGEURL = "image_url"
CREATED = "created"
AUTHOR = "author"
SEARCH_SCORE = "search_score"
RELATED = "related"
SHARED_IN = "shared_in"
TRENDSCORE = "trend_score"
CHATTER_URL = "chatter_url"
LIKES = "likes"
COMMENTS = "comments"
SHARES = "shares"
DESCRIPTION = "description"
RESTRICTED_CONTENT = "restricted_content"
CONTENT_LENGTH = "content_length"
SUMMARY_LENGTH = "summary_length"
TITLE_LENGTH = "title_length"
BASE_URL = "base_url"
RSS_FEED = "rss_feed"
FAVICON = "favicon"
SITE_NAME = "site_name"

# Collector-only fields (same JSON keys, not always stored in Beansack)
PLATFORM = "platform"
LANGUAGE = "language"
ARTICLE_LANGUAGE = "article_language"
SITE_LANGUAGE = "site_language"
AUTHOR_EMAIL = "author_email"
FORUM = "forum"

# Cache / digest payload keys (same JSON keys as above where applicable)
DIGEST = "digest"
BRIEFING = "briefing"

# Cupboard / related-article keys
RELATED_URL = "related_url"
DOMAIN_NAME = "domain_name"

def non_null_fields(items: list[dict]) -> list[str]:
    return list({k for item in items for k, v in item.items() if v is not None})

def clear_null_bytes(obj):
    """Recursively remove null bytes (\\u0000) from all strings in the object."""
    if isinstance(obj, dict):
        return {k: clear_null_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clear_null_bytes(v) for v in obj]
    elif isinstance(obj, str):
        return obj.replace("\u0000", "NULL_BYTE")
    return obj
