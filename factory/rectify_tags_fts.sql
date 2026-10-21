-- Run with psql autocommit (not a single wrapping transaction).
-- CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
--
-- 1. Rewrite beans under ACCESS EXCLUSIVE (old tags still serves FTS once the lock drops).
-- 2. Build the new GIN index without blocking reads/writes.
-- 3. Short exclusive cutover: drop old tags, rename tags_v2.

-- Truncated to 2048 characters so to_tsvector never hits the 1 MiB tsvector cap.
SET maintenance_work_mem = '8GB';
SET max_parallel_maintenance_workers = 16;

CREATE OR REPLACE FUNCTION immutable_fts_vector_string(
    regions varchar[],
    entities varchar[],
    categories varchar[],
    title varchar
)
RETURNS tsvector
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT to_tsvector('simple', left(concat_ws(' ',
        array_to_string(regions[1:10], ' '),
        array_to_string(entities[1:10], ' '),
        array_to_string(categories, ' '),
        title), 2048))
$$;

ALTER TABLE beans ADD COLUMN IF NOT EXISTS fts_vector tsvector
    GENERATED ALWAYS AS (
        immutable_fts_vector_string(regions, entities, categories, title)
    ) STORED;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_beans_fts_vector ON beans USING gin (fts_vector);

BEGIN;

DROP VIEW IF EXISTS trending_beans_view;
DROP VIEW IF EXISTS latest_beans_view;
DROP VIEW IF EXISTS beans_sources_view;

ALTER TABLE beans DROP COLUMN tags;
ALTER TABLE beans DROP COLUMN IF EXISTS tags_V2;
DROP FUNCTION IF EXISTS immutable_tags_to_text(varchar[], varchar[], varchar[]);

CREATE VIEW beans_sources_view AS
SELECT b.*, p.domain_name, p.site_name, p.description, p.favicon, p.rss_feed
FROM beans b LEFT JOIN publishers p ON b.source_id = p.id;

CREATE VIEW latest_beans_view AS
SELECT b.*, tr.likes, tr.comments, tr.subscribers, tr.mentions,
       tr.related, tr.observed, tr.cluster_id, tr.trend_score
FROM beans_sources_view b LEFT JOIN trend_aggregates tr ON b.id = tr.id;

CREATE VIEW trending_beans_view AS
SELECT b.*, tr.likes, tr.comments, tr.subscribers, tr.mentions,
       tr.related, tr.observed, tr.cluster_id, tr.trend_score
FROM beans_sources_view b INNER JOIN trend_aggregates tr ON b.id = tr.id;

COMMIT;
