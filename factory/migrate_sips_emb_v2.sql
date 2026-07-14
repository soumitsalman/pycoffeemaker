-- Promote sips.emb_v2 -> embedding (in-transaction).
-- Prerequisite: emb_v2 fully backfilled (see factory/rectify.py).
-- Matches pycupboard/pgcupboard.py HNSW index (m=24, ef_construction=128).
-- NOTE: CREATE INDEX inside the txn holds an exclusive lock until COMMIT — plan downtime.

BEGIN;

ALTER TABLE sips DROP COLUMN IF EXISTS embedding CASCADE;
ALTER TABLE sips RENAME COLUMN emb_v2 TO embedding;

COMMIT;

SET maintenance_work_mem = '4GB';
CREATE INDEX CONCURRENTLY idx_sips_embedding_hnsw
    ON sips USING hnsw (embedding vector_cosine_ops)
    WITH (m = 24, ef_construction = 128);
