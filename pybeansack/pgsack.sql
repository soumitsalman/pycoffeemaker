CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE OR REPLACE FUNCTION immutable_tags_to_text(
    a varchar[],
    b varchar[],
    c varchar[]
)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT array_to_string(
        (
            SELECT array_agg(elem)
            FROM unnest(
                COALESCE(a, '{}') ||
                COALESCE(b, '{}') ||
                COALESCE(c, '{}')
            ) AS elem
            WHERE elem IS NOT NULL
        ),
        ' '
    );
$$;

-- CONTENT TABLES
CREATE TABLE IF NOT EXISTS beans (
    -- CORE FIELDS
    id UUID PRIMARY KEY,
    url VARCHAR NOT NULL,
    kind VARCHAR,
    source_id UUID,
    base_url VARCHAR,    
    author VARCHAR,
    image_url VARCHAR,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    collected TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    language VARCHAR,

    -- TEXT HEAVY FIELDS
    title VARCHAR,
    summary TEXT,
    content TEXT,
    restricted_content BOOLEAN,

    -- CLASSIFICATION FIELDS
    embedding vector(320), -- vector length is not easily mutable once set, so hardcoding it for now
    categories VARCHAR[],
    sentiments VARCHAR[],
    ideology VARCHAR,

    -- COMPRESSED EXTRACTION FIELDS
    regions VARCHAR[],
    entities VARCHAR[],

    -- TEXT SEARCH FIELD
    tags TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('simple', immutable_tags_to_text(regions, entities, categories))
    ) STORED
);

CREATE TABLE IF NOT EXISTS publishers (
    id UUID PRIMARY KEY,
    domain_name VARCHAR NOT NULL,
    base_url VARCHAR NOT NULL,
    site_name VARCHAR,
    description TEXT,
    favicon VARCHAR,
    rss_feed VARCHAR,
    collected TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chatters (
    chatter_url VARCHAR NOT NULL,
    -- this is a foreign key to beans.url but not enforced due to insertion sequence
    url VARCHAR NOT NULL,
    bean_id UUID,
    platform VARCHAR,
    forum VARCHAR,
    collected TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    likes INTEGER DEFAULT 0,
    comments INTEGER DEFAULT 0,
    subscribers INTEGER DEFAULT 0,
    shares INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS related_beans (
    bean_id UUID NOT NULL,
    related_bean_id UUID NOT NULL,
    collected TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (bean_id, related_bean_id)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS trend_aggregates AS
WITH RECURSIVE
  best_chatters AS (
    SELECT DISTINCT
      ON (chatters.chatter_url) chatters.chatter_url,
      chatters.bean_id,
      chatters.likes,
      chatters.comments,
      chatters.subscribers,
      chatters.collected
    FROM
      chatters
    ORDER BY
      chatters.chatter_url,
      chatters.comments DESC,
      chatters.likes DESC,
      chatters.collected
  ),
  chatter_stats AS (
    SELECT
      best_chatters.bean_id,
      date (max(best_chatters.collected)) AS first_collected,
      sum(best_chatters.likes) AS likes,
      sum(best_chatters.comments) AS comments,
      sum(best_chatters.subscribers) AS subscribers,
      count(best_chatters.chatter_url) AS mentions
    FROM
      best_chatters
    GROUP BY
      best_chatters.bean_id
  ),
  related_stats AS (
    SELECT
      edges.bean_id,
      count(DISTINCT edges.rel) AS related,
      date (min(edges.collected)) AS first_collected
    FROM
      (
        SELECT
          related_beans.bean_id,
          related_beans.related_bean_id AS rel,
          related_beans.collected
        FROM
          related_beans
        UNION ALL
        SELECT
          related_beans.related_bean_id,
          related_beans.bean_id,
          related_beans.collected
        FROM
          related_beans
      ) edges
    WHERE
      edges.bean_id <> edges.rel
    GROUP BY
      edges.bean_id
  ),
  cluster_candidates AS (
    SELECT
      related_beans.bean_id,
      related_beans.bean_id AS cand,
      related_beans.collected
    FROM
      related_beans
    UNION ALL
    SELECT
      related_beans.bean_id,
      related_beans.related_bean_id,
      related_beans.collected
    FROM
      related_beans
    UNION ALL
    SELECT
      related_beans.related_bean_id,
      related_beans.bean_id,
      related_beans.collected
    FROM
      related_beans
    UNION ALL
    SELECT
      related_beans.related_bean_id,
      related_beans.related_bean_id,
      related_beans.collected
    FROM
      related_beans
  ),
  first_seen_related AS (
    SELECT
      cluster_candidates.cand,
      min(cluster_candidates.collected) AS first_seen
    FROM
      cluster_candidates
    GROUP BY
      cluster_candidates.cand
  ),
  cluster_ids AS (
    SELECT DISTINCT
      ON (cc.bean_id) cc.bean_id,
      cc.cand AS cluster_id
    FROM
      cluster_candidates cc
      JOIN first_seen_related fs ON fs.cand = cc.cand
    ORDER BY
      cc.bean_id,
      cc.collected,
      fs.first_seen,
      cc.cand
  ),
  cluster_walk AS (
    SELECT
      cluster_ids.bean_id,
      cluster_ids.cluster_id,
      1 AS depth
    FROM
      cluster_ids
    UNION ALL
    SELECT
      w.bean_id,
      c.cluster_id,
      w.depth + 1
    FROM
      cluster_walk w
      JOIN cluster_ids c ON c.bean_id = w.cluster_id
    WHERE
      c.cluster_id <> w.cluster_id
      AND w.depth < 32
  ),
  cluster_roots AS (
    SELECT DISTINCT
      ON (cluster_walk.bean_id) cluster_walk.bean_id,
      cluster_walk.cluster_id
    FROM
      cluster_walk
    ORDER BY
      cluster_walk.bean_id,
      cluster_walk.depth DESC
  ),
  active AS (
    SELECT
      chatter_stats.bean_id
    FROM
      chatter_stats
    UNION
    SELECT
      related_stats.bean_id
    FROM
      related_stats
  ),
  trend_stats AS (
    SELECT
      a.bean_id AS id,
      COALESCE(cs.likes, 0::bigint) AS likes,
      COALESCE(cs.comments, 0::bigint) AS comments,
      COALESCE(cs.subscribers, 0::bigint) AS subscribers,
      COALESCE(cs.mentions, 0::bigint) AS mentions,
      COALESCE(rs.related, 0::bigint) AS related,
      GREATEST(rs.first_collected, cs.first_collected) AS observed,
      cr.cluster_id
    FROM
      active a
      LEFT JOIN chatter_stats cs ON a.bean_id = cs.bean_id
      LEFT JOIN related_stats rs ON a.bean_id = rs.bean_id
      LEFT JOIN cluster_roots cr ON a.bean_id = cr.bean_id
  )
SELECT
  id,
  likes,
  comments,
  subscribers,
  mentions,
  related,
  observed,
  cluster_id,
  (
    (
      100 * related + 50 * comments + 10 * mentions + likes
    ) / (CURRENT_DATE + 2 - observed)
  )::double precision AS trend_score
FROM
  trend_stats
WHERE
  GREATEST(likes, comments, mentions, related) > 0;
-- PRIMARY DIFF: between latest vs trending
-- trending requires some chatter or related items. Hence INNER JOIN trend_aggregates
-- latest does not require chatter or related items. Hence LEFT JOIN trend_aggregates

CREATE OR REPLACE VIEW beans_sources_view AS
SELECT
    b.*,
    p.domain_name, p.site_name, p.description, p.favicon, p.rss_feed
FROM beans b
LEFT JOIN publishers p ON b.source_id = p.id;

CREATE OR REPLACE VIEW latest_beans_view AS
SELECT
    b.*,
    tr.likes, tr.comments, tr.subscribers, tr.mentions, tr.related, tr.observed, tr.cluster_id, tr.trend_score
FROM beans_sources_view b
LEFT JOIN trend_aggregates tr ON b.id = tr.id;

CREATE OR REPLACE VIEW trending_beans_view AS
SELECT
    b.*,
    tr.likes, tr.comments, tr.subscribers, tr.mentions, tr.related, tr.observed, tr.cluster_id, tr.trend_score
FROM beans_sources_view b
INNER JOIN trend_aggregates tr ON b.id = tr.id;



-- INDEXES --
-- beans
CREATE INDEX IF NOT EXISTS idx_beans_url ON beans(url);
CREATE INDEX IF NOT EXISTS idx_beans_kind ON beans(kind);
CREATE INDEX IF NOT EXISTS idx_beans_created ON beans(created DESC);
CREATE INDEX IF NOT EXISTS idx_beans_source ON beans(source_id);
CREATE INDEX IF NOT EXISTS idx_beans_lang ON beans(language);
CREATE INDEX IF NOT EXISTS idx_beans_categories ON beans USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_beans_entities ON beans USING gin(entities);
CREATE INDEX IF NOT EXISTS idx_beans_regions ON beans USING gin(regions);
-- tags search
CREATE INDEX IF NOT EXISTS idx_beans_tags ON beans USING gin(tags);
-- vector search
CREATE INDEX IF NOT EXISTS idx_beans_embedding_hnsw_cosine ON beans USING hnsw (embedding vector_cosine_ops)
    WITH (m = 24, ef_construction = 128);

-- publishers
CREATE INDEX IF NOT EXISTS idx_publishers_base_url ON publishers(base_url);
CREATE INDEX IF NOT EXISTS idx_publishers_source ON publishers(domain_name);

-- chatters
CREATE INDEX IF NOT EXISTS idx_chatters_url ON chatters(url);
CREATE INDEX IF NOT EXISTS idx_chatters_collected ON chatters(collected DESC);

-- related_beans
CREATE INDEX IF NOT EXISTS idx_related_beans_related_url ON related_beans(related_bean_id);
CREATE INDEX IF NOT EXISTS idx_related_beans_collected ON related_beans(collected DESC);
CREATE INDEX IF NOT EXISTS idx_chatters_chatter_url ON chatters(chatter_url);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trend_agg_url ON trend_aggregates(id);