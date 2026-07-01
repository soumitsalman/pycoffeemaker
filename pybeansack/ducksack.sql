CREATE TABLE IF NOT EXISTS beans (
    -- CORE FIELDS
    url VARCHAR NOT NULL,
    kind VARCHAR,
    title VARCHAR,
    author VARCHAR,
    source VARCHAR,
    image_url VARCHAR,
    created TIMESTAMP NOT NULL,
    collected TIMESTAMP NOT NULL,

    -- TEXT HEAVY FIELDS
    summary TEXT,
    content TEXT,
    restricted_content BOOLEAN,

    -- CLASSIFICATION FIELDS
    embedding FLOAT[],
    categories VARCHAR[],
    sentiments VARCHAR[],

    -- COMPRESSED EXTRACTION FIELDS
    regions VARCHAR[],
    entities VARCHAR[]
);

CREATE TABLE IF NOT EXISTS publishers (
    source VARCHAR NOT NULL,
    base_url VARCHAR NOT NULL,
    site_name VARCHAR,
    description TEXT,
    favicon VARCHAR,
    rss_feed VARCHAR,
    collected TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chatters (
    chatter_url VARCHAR NOT NULL,
    -- this is a foreign key to beans.url but not enforced due to insertion sequence
    url VARCHAR NOT NULL,
    source VARCHAR,
    forum VARCHAR,
    collected TIMESTAMP,
    likes UINT32 DEFAULT 0,
    comments UINT32 DEFAULT 0,
    subscribers UINT32 DEFAULT 0,
    shares UINT32 DEFAULT 0
);

CREATE TABLE IF NOT EXISTS related_beans (
    url VARCHAR NOT NULL,
    related_url VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS trend_aggregates (
    url VARCHAR NOT NULL,
    likes UINT32 DEFAULT 0,
    comments UINT32 DEFAULT 0,
    subscribers UINT32 DEFAULT 0,
    shares UINT32 DEFAULT 0,
    related UINT32 DEFAULT 0,
    updated DATE,
    trend_score FLOAT DEFAULT 0.0
);

CREATE VIEW IF NOT EXISTS trending_beans_view AS
SELECT
    b.*,
    tr.updated, tr.comments, tr.shares, tr.likes, tr.subscribers, tr.related, tr.trend_score
FROM beans b
INNER JOIN trend_aggregates tr ON b.url = tr.url;

CREATE VIEW IF NOT EXISTS aggregated_beans_view AS
WITH related_groups AS (
    SELECT url, ARRAY_AGG(related_url) AS related_urls
    FROM related_beans
    GROUP BY url
)
SELECT
    b.*,
    tr.updated, tr.comments, tr.shares, tr.likes, tr.subscribers, tr.related, tr.trend_score,
    rel.related_urls,
    p.base_url, p.site_name, p.description, p.favicon, p.rss_feed
FROM beans b
LEFT JOIN trend_aggregates tr ON b.url = tr.url
LEFT JOIN related_groups rel ON b.url = rel.url
LEFT JOIN publishers p ON b.source = p.source;