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

-- [VERIFIED] SQL: update chatters.platform with existing source
UPDATE chatters
SET platform = LOWER(source)
WHERE source IS NOT NULL AND platform IS NULL;


-- [DONE] PYTHON: update chatters.bean_id with existing beans.id


-- [DONE] SQL: update views
-- trend_aggregate2_v2: like trend_aggregates but keyed by bean_id (not url)
-- and chatter stats grouped by (bean_id, platform); uses related_beans_v2
CREATE MATERIALIZED VIEW IF NOT EXISTS trend_aggregates_v2 AS
WITH
    max_chatters AS (
        SELECT
            chatter_url,
            MAX(likes) AS likes,
            MAX(comments) AS comments
        FROM chatters
        GROUP BY chatter_url
    ),
    first_seen_max_chatters AS (
        SELECT
            fs.chatter_url,
            MIN(fs.collected) AS collected
        FROM chatters fs
        LEFT JOIN max_chatters mx ON fs.chatter_url = mx.chatter_url
        WHERE fs.likes = mx.likes AND fs.comments = mx.comments
        GROUP BY fs.chatter_url
    ),
    chatter_stats AS (
        SELECT
            bean_id,
            DATE(MAX(collected)) AS updated,
            SUM(likes) AS likes,
            SUM(comments) AS comments,
            SUM(subscribers) AS subscribers,
            COUNT(chatter_url) AS mentions
        FROM (
            SELECT ch.* FROM chatters ch
            LEFT JOIN first_seen_max_chatters fs ON fs.chatter_url = ch.chatter_url
            WHERE fs.collected = ch.collected
        )
        GROUP BY bean_id
    ),
    related_stats AS (
        SELECT bean_id, COUNT(*) AS related
        FROM related_beans_v2
        GROUP BY bean_id
    ),
    related_freq AS (
        SELECT related_bean_id AS cand, COUNT(*)::int AS cnt
        FROM related_beans_v2
        GROUP BY related_bean_id
    ),
    cluster_candidates AS (
        SELECT bean_id, bean_id AS cand FROM related_beans_v2
        UNION
        SELECT bean_id, related_bean_id FROM related_beans_v2
    ),
    cluster_ids AS (
        SELECT DISTINCT ON (cc.bean_id)
            cc.bean_id,
            cc.cand AS cluster_id
        FROM cluster_candidates cc
        LEFT JOIN related_freq rf ON rf.cand = cc.cand
        ORDER BY cc.bean_id, COALESCE(rf.cnt, 0) DESC, cc.cand
    ),
    active AS (
        SELECT bean_id FROM chatter_stats
        UNION
        SELECT bean_id FROM related_stats
    ),
    trend_stats AS (
        SELECT
            a.bean_id,
            COALESCE(cg.likes, 0) AS likes,
            COALESCE(cg.comments, 0) AS comments,
            COALESCE(cg.subscribers, 0) AS subscribers,
            COALESCE(cg.mentions, 0) AS mentions,
            COALESCE(rg.related, 0) AS related,
            GREATEST(DATE(b.created), COALESCE(cg.updated, DATE(b.created))) AS updated,
            ci.cluster_id
        FROM active a
        INNER JOIN beans b ON b.id = a.bean_id
        LEFT JOIN chatter_stats cg ON a.bean_id = cg.bean_id
        LEFT JOIN related_stats rg ON a.bean_id = rg.bean_id
        LEFT JOIN cluster_ids ci ON ci.bean_id = a.bean_id
    )
SELECT
    *,
    ((100*related + 50*comments + 10*mentions + likes) / (CURRENT_DATE + 2 - updated))::float AS trend_score
FROM trend_stats
WHERE GREATEST(likes, comments, mentions, related) > 0;


CREATE UNIQUE INDEX IF NOT EXISTS idx_trend_agg2_v2_bean
    ON trend_aggregate2_v2 (bean_id);

-- [DONE] PYTHON: update beans.source_id with existing publishers.id

-- [DONE] SQL: update publishers table
-- save duplicate publisher ids
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
-- SQL: remove old views
DROP VIEW IF EXISTS aggregated_beans_view;
DROP VIEW IF EXISTS trending_beans_view;
DROP VIEW IF EXISTS latest_beans_view;
DROP VIEW IF EXISTS beans_sources_view;

DROP MATERIALIZED VIEW IF EXISTS trend_aggregates;
ALTER MATERIALIZED VIEW trend_aggregates_v2 RENAME TO trend_aggregates;

-- SQL: remove old columns
ALTER TABLE beans 
    DROP COLUMN source;
ALTER TABLE publishers
    RENAME COLUMN source TO domain_name;

CREATE OR REPLACE VIEW beans_sources_view AS
SELECT
    b.*,
    p.domain_name, p.base_url, p.site_name, p.description, p.favicon, p.rss_feed
FROM beans b
LEFT JOIN publishers p ON b.source_id = p.id;


CREATE VIEW IF NOT EXISTS latest_beans_view AS
SELECT
    b.*,
    tr.updated, tr.comments, tr.shares, tr.likes, tr.subscribers, tr.related, tr.trend_score, tr.cluster_id
FROM beans_sources_view b
LEFT JOIN trend_aggregates tr ON b.id = tr.bean_id;


CREATE VIEW IF NOT EXISTS trending_beans_view AS
SELECT
    b.*,
    tr.updated, tr.comments, tr.shares, tr.likes, tr.subscribers, tr.related, tr.trend_score, tr.cluster_id
FROM beans_sources_view b
INNER JOIN trend_aggregates tr ON b.id = tr.bean_id;


CREATE VIEW IF NOT EXISTS aggregated_beans_view AS
WITH related_groups AS (
    SELECT bean_id, ARRAY_AGG(related_bean_id) AS related_bean_ids
    FROM related_beans
    GROUP BY bean_id
)
SELECT
    b.*,
    tr.updated, tr.comments, tr.shares, tr.likes, tr.subscribers, tr.related, tr.trend_score, tr.cluster_id,
    rel.related_urls
FROM beans_sources_view b
LEFT JOIN trend_aggregates tr ON b.id = tr.bean_id
LEFT JOIN related_groups rel ON b.id = rel.bean_id;
