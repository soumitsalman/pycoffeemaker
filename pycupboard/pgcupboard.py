import asyncio
import uuid
import os
import logging
from datetime import datetime
from itertools import batched, chain
from typing import Any

from psycopg_pool import AsyncConnectionPool
from pgvector.psycopg import Vector, register_vector_async
from psycopg import sql
from psycopg.types.json import Jsonb

from .models import *

log = logging.getLogger("cupboard")

BATCH_SIZE = 256
VECTOR_LEN = int(os.getenv('VECTOR_LEN', 384))

_INIT_STMTS = """
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE OR REPLACE FUNCTION immutable_tags_to_text(tags text[])
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT array_to_string(COALESCE(tags, '{}'), ' ');
$$;

CREATE TABLE IF NOT EXISTS sips (
    id UUID PRIMARY KEY,
    kind TEXT NOT NULL,
    created TIMESTAMPTZ NOT NULL,    
    source UUID,    
    embedding vector(384) NOT NULL,
    tags TEXT[], 
    tags_fts tsvector GENERATED ALWAYS AS (
        to_tsvector('simple', immutable_tags_to_text(tags))
    ) STORED,
    digest JSONB,       
    url TEXT, -- used for deriving id 
    base_url TEXT-- used for deriving source 
);

CREATE TABLE IF NOT EXISTS sources (
    id UUID PRIMARY KEY,
    base_url TEXT NOT NULL, -- used for deriving id
    domain_name TEXT,    
    site_name TEXT,
    description TEXT,
    favicon TEXT,
    rss_feed TEXT
);

CREATE TABLE IF NOT EXISTS relations (
    -- NOTE: from_id and to_id are supposed to be foreign keys to sips but ignoring it to improve performance
    from_id UUID NOT NULL,
    to_id UUID NOT NULL,
    relationship TEXT NOT NULL,
    UNIQUE(from_id, to_id, relationship)
);

CREATE INDEX IF NOT EXISTS idx_sips_url ON sips(url);
CREATE INDEX IF NOT EXISTS idx_sips_base_url ON sips(base_url);
CREATE INDEX IF NOT EXISTS idx_sips_kind ON sips(kind);
CREATE INDEX IF NOT EXISTS idx_sips_created ON sips(created);
CREATE INDEX IF NOT EXISTS idx_sips_source ON sips(source);
CREATE INDEX IF NOT EXISTS idx_sips_tags ON sips(tags);
CREATE INDEX IF NOT EXISTS idx_sips_tags_fts ON sips USING gin(tags_fts);
CREATE INDEX IF NOT EXISTS idx_sips_embedding_hnsw ON sips USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

CREATE INDEX IF NOT EXISTS idx_sources_base_url ON sources(base_url);

CREATE INDEX IF NOT EXISTS idx_relations_from_id ON relations(from_id);
CREATE INDEX IF NOT EXISTS idx_relations_to_id ON relations(to_id);
CREATE INDEX IF NOT EXISTS idx_relations_relationship ON relations(relationship);
"""
# relation fields
RELATED = 'related'
FROM_ID = 'from_id'
TO_ID = 'to_id'
RELATIONSHIP = 'relationship'

SIP_COLUMNS = [ID, CREATED, KIND, SOURCE, EMBEDDING, TAGS, DIGEST, URL, BASE_URL]
SOURCE_COLUMNS = [ID, DOMAIN_NAME, BASE_URL, SITE_NAME, DESCRIPTION, FAVICON, RSS_FEED]
RELATION_COLUMNS = [FROM_ID, TO_ID, RELATIONSHIP]

class Cupboard:
    pool: AsyncConnectionPool

    def __init__(self, conn_str: str):
        self.conn_str = conn_str
        self.pool = None

    async def __aenter__(self):
        self.pool = AsyncConnectionPool(
            self.conn_str,
            min_size=0,
            max_size=32,
            timeout=270,
            num_workers=os.cpu_count()*2,
            configure=register_vector_async
        )
        await self.pool.open()
        await self._init_schema()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def _init_schema(self):
        async with self.pool.connection() as conn:
            await conn.execute(_INIT_STMTS)

    async def store_sips(self, sips: list[Sip]) -> int:
        """Store a list of sips in the database."""
        if not sips: return 0
        
        for sip in sips:
            sip.embedding = Vector(sip.embedding)
            if sip.digest: sip.digest = Jsonb(sip.digest)

        row_placeholder = sql.SQL("(" + ",".join(["%s"] * len(SIP_COLUMNS)) + ")")
        store_batches = [
            {
                "expr": sql.SQL(
                    "INSERT INTO sips ({cols}) VALUES {values} ON CONFLICT (id) DO NOTHING"
                ).format(
                    cols=sql.SQL(", ").join(map(sql.Identifier, SIP_COLUMNS)),
                    values=sql.SQL(", ").join(row_placeholder for _ in chunk),
                ),
                "params": list(chain.from_iterable((getattr(sip, c) for c in SIP_COLUMNS) for sip in chunk)),
            }
            for chunk in batched(sips, BATCH_SIZE)
        ]
        return await self._batch_insert(store_batches)    

    async def store_sources(self, sources: list[Source]) -> int:
        if not sources:
            return 0

        row_placeholder = sql.SQL("(" + ",".join(["%s"] * len(SOURCE_COLUMNS)) + ")")
        store_batches = [
            {
                "expr": sql.SQL(
                    "INSERT INTO sources ({cols}) VALUES {values} ON CONFLICT (id) DO NOTHING"
                ).format(
                    cols=sql.SQL(", ").join(map(sql.Identifier, SOURCE_COLUMNS)),
                    values=sql.SQL(", ").join(row_placeholder for _ in chunk),
                ),
                "params": list(chain.from_iterable((getattr(src, c) for c in SOURCE_COLUMNS) for src in chunk)),
            }
            for chunk in batched(sources, BATCH_SIZE)
        ]
        return await self._batch_insert(store_batches)

    async def link_sips(
        self,
        relations: list[dict[str | UUID, list[str | UUID]]],
        relationship: str,
    ) -> int:
        """Link a list of sips to a list of other sips.

        Args:
            - `relations`: A list of dictionaries with the following keys:
                - `url`: The url or the id of the sip to link from.
                - `related`: A list of urls or ids of the sips to link to.
            - `relationship`: The relationship to link the sips with.
        
        Returns: The number of relations linked.
        """
        if not relations:
            return 0

        def _sip_id(ref: str | UUID) -> uuid.UUID | None:
            if isinstance(ref, UUID): return ref
            # no need to resolve urls to ids because the ids are deterministic based on the url
            if isinstance(ref, str): return generate_id(ref)
            
        # step 2: build relation rows
        relation_rows: list[tuple[uuid.UUID, uuid.UUID, str]] = []
        for item in relations:
            from_id = _sip_id(item.get(URL))
            related_urls = item.get(RELATED)
            if from_id and related_urls:
                relation_rows.extend((from_id, to_id, relationship) for to_ref in related_urls if (to_id := _sip_id(to_ref)))

        # step 3: create insertion statements and insert relation rows
        expr_format = """INSERT INTO relations (from_id, to_id, relationship) VALUES {values}
            ON CONFLICT (from_id, to_id, relationship) DO NOTHING"""
        store_batches = [
            {
                "expr": expr_format.format(values=", ".join("(%s, %s, %s)" for _ in chunk)),
                "params": list(chain.from_iterable(chunk)),
            }
            for chunk in batched(relation_rows, BATCH_SIZE)
        ]
        return await self._batch_insert(store_batches)

    async def _batch_insert(self, to_store: list[dict[str, Any]]):
        async with self.pool.connection() as conn:
            results = await asyncio.gather(*(
                conn.execute(item["expr"], item["params"])
                for item in to_store
            ))
            await conn.commit()
            return sum(item.rowcount for item in results)

    async def query_sips(
        self,
        created: datetime = None,
        tags: list[str] = None,        
        kind: list[str] = None,
        text: list[str] | str = None,
        embedding: list[float] = None,
        distance: float = None,
        conditions: dict[str, Any] | list[str] = None,
        limit: int = None,
        columns: list[str] | None = None,
    ) -> list[Sip]:
        field_expr = "*"
        where_parts = []
        params = {}
        order = ["created DESC"]

        if columns:
            field_expr = ", ".join(columns)

        if created:
            where_parts.append("created >= %(created)s")
            params["created"] = created

        if kind:
            where_parts.append("kind = ANY(%(kind)s)")
            params["kind"] = kind

        if tags:
            where_parts.append("tags && %(tags)s")
            params["tags"] = tags

        if text:
            where_parts.append("tags_fts @@ plainto_tsquery('simple', %(query)s)")
            params["query"] = text if isinstance(text, str) else " ".join(text)

        if embedding:
            params["embedding"] = Vector(embedding)
            if distance:
                where_parts.append("(embedding <=> %(embedding)s) <= %(distance)s")
                params["distance"] = distance
            else:
                order = ["(embedding <=> %(embedding)s) ASC"]+order

        if conditions:
            if isinstance(conditions, list):
                where_parts.extend(conditions)
            elif isinstance(conditions, dict):
                for k, v in conditions.items():
                    if isinstance(v, list):
                        where_parts.append(f"{k} = ANY(%({k})s)")
                    else:
                        where_parts.append(f"{k} = %({k})s")
                    params[k] = v

        expr = f"SELECT {field_expr} FROM sips"
        if where_parts:
            expr += " WHERE " + " AND ".join(where_parts)
        if order:
            expr += " ORDER BY " + ", ".join(order)
        if limit:
            expr += " LIMIT %(limit)s"
            params["limit"] = limit

        async with self.pool.connection() as conn:
            cur = await conn.execute(expr, params)
            cols = [desc[0] for desc in cur.description]
            results = [Sip(**dict(zip(cols, row))) async for row in cur]
        return results

    async def optimize(self):
        pass