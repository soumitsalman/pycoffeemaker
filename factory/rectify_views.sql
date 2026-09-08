DROP VIEW IF EXISTS latest_beans_view;
DROP VIEW IF EXISTS trending_beans_view;
DROP VIEW IF EXISTS beans_sources_view;

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

