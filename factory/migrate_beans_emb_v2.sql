-- Promote beans.emb_v2 -> embedding (in-transaction).
-- Prerequisite: emb_v2 fully backfilled (see factory/rectify.py).
-- Matches pybeansack/pgsack.sql views + HNSW index (m=24, ef_construction=128).
-- NOTE: CREATE INDEX inside the txn holds an exclusive lock until COMMIT — plan downtime.

BEGIN;

-- CASCADE drops dependent views (trending_beans_view, aggregated_beans_view) and the HNSW index.
ALTER TABLE beans DROP COLUMN IF EXISTS embedding CASCADE;
ALTER TABLE beans RENAME COLUMN emb_v2 TO embedding;

CREATE VIEW trending_beans_view AS
SELECT
    b.*,
    tr.updated, tr.comments, tr.shares, tr.likes, tr.subscribers, tr.related, tr.trend_score
FROM beans b
INNER JOIN trend_aggregates tr ON b.url = tr.url;

CREATE VIEW aggregated_beans_view AS
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

COMMIT;


SET maintenance_work_mem = '4GB';
CREATE INDEX CONCURRENTLY idx_beans_embedding_hnsw_cosine
    ON beans USING hnsw (embedding vector_cosine_ops)
    WITH (m = 24, ef_construction = 128);