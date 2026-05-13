import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from itertools import batched
from typing import Any, Literal
from surrealdb import AsyncSurreal, AsyncHttpSurrealConnection, AsyncEmbeddedSurrealConnection, AsyncSurrealTransaction, AsyncWsSurrealConnection

from icecream import ic

NS = "__coffeemaker__"
DB = "__cupboard__"
BATCH_SIZE = 2048

K_ID = 'id'
K_BASE_URL = 'base_url'
K_URL = 'url'
K_RELATED_URL = 'related_url'
K_EMBEDDING = 'embedding'

DATA_TYPES = Literal["events", "signals", "sources"]

VECTOR_LEN = int(os.getenv('VECTOR_LEN', 384))

_INIT_STMTS = [
    # event tables, fields and indexes
    "DEFINE TABLE IF NOT EXISTS events SCHEMALESS;",
    "DEFINE FIELD IF NOT EXISTS url ON TABLE events TYPE string;",
    "DEFINE FIELD IF NOT EXISTS base_url ON TABLE events TYPE string;",
    "DEFINE FIELD IF NOT EXISTS created ON TABLE events TYPE datetime;",
    # "DEFINE FIELD IF NOT EXISTS tags ON TABLE events TYPE option<array<string>>;",
    "DEFINE FIELD IF NOT EXISTS embedding ON TABLE events TYPE array<float>;",
    # "DEFINE FIELD IF NOT EXISTS published_by ON events TYPE option<record<sources>>;",
    # "DEFINE FIELD IF NOT EXISTS same_as ON events TYPE option<array<record<events>>>;",
    "DEFINE INDEX IF NOT EXISTS url_idx ON TABLE events COLUMNS url UNIQUE;",
    "DEFINE INDEX IF NOT EXISTS base_url_idx ON TABLE events COLUMNS base_url;",
    "DEFINE INDEX IF NOT EXISTS created_idx ON TABLE events COLUMNS created;",
    # "DEFINE INDEX IF NOT EXISTS tags_idx ON TABLE user COLUMNS tags SEARCH;",
    f"DEFINE INDEX IF NOT EXISTS embedding_idx ON events FIELDS embedding HNSW DIMENSION {VECTOR_LEN} DIST COSINE;",
    # sources tables, fields and indexes
    "DEFINE TABLE IF NOT EXISTS sources SCHEMALESS;",
    "DEFINE FIELD IF NOT EXISTS base_url ON TABLE sources TYPE string;",
    "DEFINE INDEX IF NOT EXISTS base_url_idx ON TABLE sources COLUMNS base_url UNIQUE;",
    # composite singals
    "DEFINE TABLE IF NOT EXISTS signals SCHEMALESS;",
]

_OPTIMIZE_STMTS = [
    "REBUILD INDEX IF EXISTS embedding_idx ON TABLE events;"
]

class Cupboard:
    db_path: str
    db: AsyncEmbeddedSurrealConnection|AsyncWsSurrealConnection|AsyncHttpSurrealConnection

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = None

    async def __aenter__(self):
        self.db = AsyncSurreal(self.db_path)
        await self.db.use(NS, DB)
        await self._init_schema()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.db:
            await self.db.close()
        # pass

    async def close(self):
        await self.db.close()

    async def _init_schema(self):
        await self.db.query("\n".join(_INIT_STMTS))

    @asynccontextmanager
    async def transaction(self):
        session = await self.db.new_session()
        await session.use(NS, DB)
        txn = await session.begin_transaction()
        try:
            yield txn
            await txn.commit()
        except Exception as e:
            await txn.cancel()            
            raise e
        finally:
            await session.close_session()

    async def store(self, data_type: DATA_TYPES, items: list[dict[str, Any]]):
        if not items:
            return 0

        async with self.transaction() as txn:
            inserted = await self._insert_ignore(txn, data_type, items)
            if data_type == "events":
                await self._link_sources_to_events(txn, inserted)
            if data_type == "sources":
                await self._link_events_to_sources(txn, inserted)
            return len(inserted)

    async def _insert_ignore(self, txn: AsyncSurrealTransaction, data_type: str, data_items: list[dict[str, Any]]):
        """Insert a batch while skipping records that already exist."""
        results = []
        for chunk in batched(data_items, BATCH_SIZE):
            results.extend(await txn.query(f"INSERT IGNORE INTO {data_type} $items", {"items": chunk}))
        return results

    async def _link_sources_to_events(self, txn: AsyncSurrealTransaction, events: list[dict[str, Any]]):
        """Link inserted events to matching sources via `source['base_url']->PUBLISHED-> events`."""
        existing_sources = await txn.query(
            "SELECT id, base_url FROM sources WHERE base_url IN $bu",
            {"bu": list({e[K_BASE_URL] for e in events})},
        )
        if not existing_sources: return

        source_ids = {s[K_BASE_URL]: s[K_ID] for s in existing_sources}
        relations = [
            f"RELATE {source_ids[e[K_BASE_URL]]} -> PUBLISHED -> {e[K_ID]};"
            for e in events 
            if source_ids.get(e[K_BASE_URL])
        ]
        return await self._insert_relations(txn, relations)

    async def _link_events_to_sources(self, txn: AsyncSurrealTransaction, sources: list[dict[str, Any]]):
        """Link matching events to inserted sources via `source->PUBLISHED->event['base_url']`."""
        existing_events = await txn.query(
            "SELECT id, base_url FROM events WHERE base_url IN $bu",
            {"bu": list({src[K_BASE_URL] for src in sources})},
        )
        if not existing_events: return

        relations = []
        for src in sources:
            relations.extend(f"RELATE {src[K_ID]} -> PUBLISHED -> {e[K_ID]};" for e in existing_events if e[K_BASE_URL] == src[K_BASE_URL])
        return await self._insert_relations(txn, relations)
  
    async def link_events(self, links: list[dict[str, str]]):
        urls = list(set(item['url'] for item in links).union(item['related_url'] for item in links))
        find_events_expr = "SELECT id, url FROM events WHERE url INSIDE $urls"
        events = []
        for chunk in batched(urls, BATCH_SIZE):
            events.extend(await self.db.query(find_events_expr, {"urls": chunk}))

        event_ids = {row['url']: row['id'] for row in events}
        relations = [
            f"RELATE {event_ids[link['url']]} -> SAME_AS -> {event_ids[link['related_url']]};"
            for link in links 
            if event_ids.get(link['url']) and event_ids.get(link['related_url'])
        ]        
        async with self.transaction() as txn:
            return await self._insert_relations(txn, relations)

    async def _insert_relations(cls, txn: AsyncSurrealTransaction, relations: list[str]):
        for chunk in batched(relations, BATCH_SIZE):
            await txn.query("\n".join(chunk))
        return len(relations)

    async def query_events(
        self,
        created: datetime = None,
        tags: list[str] = None,
        embedding: list[float] = None, distance: float = None,
        conditions: dict[str, Any] | list[str] = None,
        limit: int = None,
        columns: list[str] = None,
    ):
        field_expr = "*"
        where_parts = []
        params = {}
        order = None

        if columns: 
            field_expr = ", ".join(columns)

        if created:
            where_parts.append(f"created >= $created")
            params["created"] = created

        if embedding:
            field_expr += ", vector::distance::knn() AS distance"
            where_parts.append(f"embedding <|{limit}, COSINE|> $embedding")
            params["embedding"] = embedding
            
            if distance is not None:
                where_parts.append("distance <= $distance")
                params["distance"] = distance

            order = ["distance"]

        if conditions:
            op = lambda val: "=" if not isinstance(val, list) else "INSIDE"
            if isinstance(conditions, list): 
                where_parts.extend(conditions)
            elif isinstance(conditions, dict):
                where_parts.extend([f"{k} {op(v)} ${k}" for k, v in conditions.items()])
                params = {k: v for k, v in conditions.items()}

        expr = f"SELECT {field_expr} FROM events"
        if where_parts: 
            expr += " WHERE "+" AND ".join(where_parts)
        if order:
            expr += " ORDER BY "+ ", ".join(order)
        if limit:
            expr += " LIMIT $limit"
            params['limit'] = limit
        # expr += " TIMEOUT 120s"
        return await self.db.query(expr, params)

    async def optimize(self):
        await self.db.query_raw("\n".join(_OPTIMIZE_STMTS))
