-- [DONE] SQL: UPDATE BEANS TABLE
-- save duplicate bean ids
SELECT id, count(*) AS n
FROM beans
GROUP BY id
HAVING count(*) > 1
ORDER BY n DESC, id;

DELETE FROM beans
WHERE id IN (
  SELECT id
  FROM beans
  GROUP BY id
  HAVING count(*) > 1
);

-- [DONE] replace beans primary key: url -> id
ALTER TABLE beans DROP CONSTRAINT IF EXISTS beans_pkey;
ALTER TABLE beans ADD CONSTRAINT beans_pkey PRIMARY KEY (id);
CREATE INDEX IF NOT EXISTS idx_beans_url ON beans(url);

-- [DONE] add source columns
ALTER TABLE beans
  ADD COLUMN IF NOT EXISTS source_id uuid,
  ADD COLUMN IF NOT EXISTS base_url varchar;

CREATE INDEX IF NOT EXISTS idx_beans_source_id ON beans(source_id);


-- [DONE] SQL: create new relationship table
-- create related_beans_v2 table
CREATE TABLE IF NOT EXISTS related_beans_v2 (
  bean_id uuid NOT NULL,
  related_bean_id uuid NOT NULL,
  collected timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (bean_id, related_bean_id)
);


-- [DONE] SQL: update chatters table
ALTER TABLE chatters
    ADD COLUMN IF NOT EXISTS bean_id uuid,
    ADD COLUMN IF NOT EXISTS platform varchar;

CREATE INDEX IF NOT EXISTS idx_chatters_bean_id ON chatters(bean_id);

-- [DONE] SQL: update chatters.platform with existing source
UPDATE chatters
SET platform = LOWER(source)
WHERE source IS NOT NULL AND platform IS NULL;


-- [DONE] PYTHON: update chatters.bean_id with existing beans.id

-- [DONE] PYTHON: update beans.source_id with existing publishers.id

-- [DONE] SQL: update publishers table
SELECT id, count(*) AS n
FROM publishers
GROUP BY id
HAVING count(*) > 1
ORDER BY n DESC, id;

DELETE FROM publishers
WHERE id IN (
  SELECT id
  FROM publishers
  GROUP BY id
  HAVING count(*) > 1
);

-- [DONE] SQL: replace publishers primary key: source -> id
ALTER TABLE publishers DROP CONSTRAINT IF EXISTS publishers_pkey;
ALTER TABLE publishers ADD CONSTRAINT publishers_pkey PRIMARY KEY (id);
CREATE INDEX IF NOT EXISTS idx_publishers_base_url ON publishers(base_url);


-- [DANGER ZONE]
-- [DONE] SQL: remove old views
DROP VIEW IF EXISTS aggregated_beans_view;
DROP VIEW IF EXISTS trending_beans_view;
DROP VIEW IF EXISTS latest_beans_view;

-- [DONE] SQL: fix beans.source column
DROP VIEW IF EXISTS beans_sources_view;
ALTER TABLE beans 
    DROP COLUMN source;
ALTER TABLE publishers
    RENAME COLUMN source TO domain_name;

-- [DONE] SQL: create dependent views bean source
CREATE OR REPLACE VIEW beans_sources_view AS
SELECT
    b.*,
    p.domain_name, p.site_name, p.description, p.favicon, p.rss_feed
FROM beans b
LEFT JOIN publishers p ON b.source_id = p.id;


-- [DONE] SQL: remove old views for trend info
DROP MATERIALIZED VIEW IF EXISTS trend_aggregates;
DROP TABLE IF EXISTS related_beans;
ALTER TABLE related_beans_v2 RENAME TO related_beans;

-- [DONE] SQL: create new trend_aggregates table
CREATE MATERIALIZED VIEW IF NOT EXISTS trend_aggregates AS
WITH RECURSIVE
    -- per chatter_url, the peak-engagement row at the earliest time it hit that peak;
    -- lexicographic ranking (comments, then likes) keeps the chatter even when
    -- the likes and comments maxima occur on different rows
    best_chatters AS (
        SELECT DISTINCT ON (chatter_url)
            chatter_url, bean_id, likes, comments, subscribers, collected
        FROM chatters
        ORDER BY chatter_url, comments DESC, likes DESC, collected ASC
    ),
    chatter_stats AS (
        SELECT
            bean_id,
            DATE(MAX(collected)) AS first_collected,
            SUM(likes) AS likes,
            SUM(comments) AS comments,
            SUM(subscribers) AS subscribers,
            COUNT(chatter_url) AS mentions
        FROM best_chatters
        GROUP BY bean_id
    ),
    related_stats AS (
        SELECT
            bean_id,
            COUNT(DISTINCT rel) AS related,
            DATE(MIN(collected)) AS first_collected
        FROM (
            SELECT bean_id, related_bean_id AS rel, collected FROM related_beans
            UNION ALL
            SELECT related_bean_id, bean_id, collected FROM related_beans
        ) edges
        WHERE bean_id <> rel
        GROUP BY bean_id
    ),
    -- relations are logically bidirectional but stored unidirectionally;
    -- include both directions (plus self) so every bean gets a cluster_id
    cluster_candidates AS (
        SELECT bean_id, bean_id AS cand, collected FROM related_beans
        UNION ALL
        SELECT bean_id, related_bean_id, collected FROM related_beans
        UNION ALL
        SELECT related_bean_id, bean_id, collected FROM related_beans
        UNION ALL
        SELECT related_bean_id, related_bean_id, collected FROM related_beans
    ),
    -- earliest appearance of each candidate anywhere in related_beans;
    -- frozen once set (new rows always carry a later collected)
    first_seen_related AS (
        SELECT cand, MIN(collected) AS first_seen
        FROM cluster_candidates
        GROUP BY cand
    ),
    -- winner comes from the bean's earliest (immutable) relation batch,
    -- preferring the earliest-seen candidate (the cluster seed), so the
    -- pointer is stable across refreshes and late joiners inherit the seed
    cluster_ids AS (
        SELECT DISTINCT ON (cc.bean_id)
            cc.bean_id,
            cc.cand AS cluster_id
        FROM cluster_candidates cc
        JOIN first_seen_related fs ON fs.cand = cc.cand
        ORDER BY cc.bean_id, cc.collected ASC, fs.first_seen ASC, cc.cand ASC
    ),
    -- chase each bean's pointer to its root (union-find): pointers strictly
    -- decrease by (first_seen, uuid) so chains are acyclic and end at a
    -- self-pointing seed; frozen pointers make the root equally stable
    cluster_walk AS (
        SELECT bean_id, cluster_id, 1 AS depth
        FROM cluster_ids
        UNION ALL
        SELECT w.bean_id, c.cluster_id, w.depth + 1
        FROM cluster_walk w
        JOIN cluster_ids c ON c.bean_id = w.cluster_id
        WHERE c.cluster_id <> w.cluster_id
          AND w.depth < 32    -- safety cap; chains are provably finite
    ),
    cluster_roots AS (
        SELECT DISTINCT ON (bean_id)
            bean_id,
            cluster_id
        FROM cluster_walk
        ORDER BY bean_id, depth DESC    -- deepest hop = root
    ),
    active AS (
        SELECT bean_id FROM chatter_stats
        UNION
        SELECT bean_id FROM related_stats
    ),
    trend_stats AS (
        SELECT
            a.bean_id as id,
            COALESCE(cs.likes, 0) AS likes,
            COALESCE(cs.comments, 0) AS comments,
            COALESCE(cs.subscribers, 0) AS subscribers,
            COALESCE(cs.mentions, 0) AS mentions,
            COALESCE(rs.related, 0) AS related,
            GREATEST(rs.first_collected, cs.first_collected) AS observed,
            cr.cluster_id
        FROM active a
        LEFT JOIN chatter_stats cs ON a.bean_id = cs.bean_id
        LEFT JOIN related_stats rs ON a.bean_id = rs.bean_id
        LEFT JOIN cluster_roots cr ON a.bean_id = cr.bean_id
    )
SELECT
    *,
    ((100*related + 50*comments + 10*mentions + likes) / (CURRENT_DATE + 2 - observed))::float AS trend_score
FROM trend_stats
WHERE GREATEST(likes, comments, mentions, related) > 0;

CREATE UNIQUE INDEX IF NOT EXISTS idx_trend_aggregates_id
    ON trend_aggregates (id);


-- [DONE] SQL: create other views
CREATE OR REPLACE VIEW latest_beans_view AS
SELECT
    b.*,
    tr.observed, tr.comments, tr.mentions, tr.likes, tr.subscribers, tr.related, tr.trend_score, tr.cluster_id
FROM beans_sources_view b
LEFT JOIN trend_aggregates tr ON b.id = tr.id;

CREATE OR REPLACE VIEW trending_beans_view AS
SELECT
    b.*,
    tr.observed, tr.comments, tr.mentions, tr.likes, tr.subscribers, tr.related, tr.trend_score, tr.cluster_id
FROM beans_sources_view b
INNER JOIN trend_aggregates tr ON b.id = tr.id;


-- CREATE OR REPLACE VIEW aggregated_beans_view AS
-- WITH related_groups AS (
--     SELECT bean_id, ARRAY_AGG(related_bean_id) AS related_bean_ids
--     FROM related_beans
--     GROUP BY bean_id
-- )
-- SELECT
--     b.*,
--     tr.observed, tr.comments, tr.mentions, tr.likes, tr.subscribers, tr.related, tr.trend_score, tr.cluster_id,
--     rel.related_urls
-- FROM beans_sources_view b
-- LEFT JOIN trend_aggregates tr ON b.id = tr.bean_id
-- LEFT JOIN related_groups rel ON b.id = rel.bean_id;
