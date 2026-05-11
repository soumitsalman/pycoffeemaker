import asyncio
from datetime import datetime, timezone
from typing import Any
from pybeansack.models import (K_BASE_URL)
from pybeansack import VECTOR_LEN
from icecream import ic

EVENTS = "events"
SIGNALS = "signals"
SOURCES = "sources"

NS = "__coffeemaker__"
DB = "__cupboard__"

# LIST_DIGEST_FIELDS = [
#     "regions",
#     "people",
#     "products",
#     "companies",
#     "stock_tickers",
#     "tags",
#     "key_events",
#     "cross_domain_impacts",
#     "entities",
#     "sentiments",
#     "main_drivers",
#     "mdna_key_takeaways",
#     "strategic_initiatives",
#     "top_risk_factors",
#     "legal_proceedings_status",
#     "business_overview_highlights",
#     "one_time_items",
#     "material_trends_uncertainties",
#     "qna_hot_topics",
#     "mitigations",
#     "affected_routes",
#     "lead_investors",
#     "other_companies",
#     "companies_mentioned",
#     "threat_actors",
#     "vulnerabilities",
# ]
# SCALAR_DIGEST_FIELDS = [
#     "event_type",
#     "impact_level",
#     "macro_context",
#     "briefing",
#     "future_outlook",
#     "guidance_tone",
#     "management_tone",
#     "incident_type",
#     "malware_family",
#     "transportation_mode",
#     "sector_rotation_signal",
#     "macro_signal",
#     "product_system",
#     "manufacturer",
#     "category",
#     "main_company",
#     "acquirer",
#     "round_type",
#     "strategic_rationale",
#     "use_of_funds",
#     "ticker_or_index",
#     "financial_analysis_summary",
#     "market_significance",
#     "fiscal_period",
#     "company_name",
#     "document_subtype",
#     "material_event_description",
# ]
# GRAPH_EDGE_TYPES = {"PUBLISHED", "SAME_AS"}

_INIT_STMTS = [
    # event tables, fields and indexes
    "DEFINE TABLE IF NOT EXISTS events SCHEMALESS;",
    "DEFINE FIELD IF NOT EXISTS url ON TABLE events TYPE string;",
    "DEFINE FIELD IF NOT EXISTS base_url ON TABLE events TYPE string;",
    "DEFINE FIELD IF NOT EXISTS created ON TABLE events TYPE datetime;",
    # "DEFINE FIELD IF NOT EXISTS tags ON TABLE events TYPE option<array<string>>;",
    "DEFINE FIELD IF NOT EXISTS embedding ON TABLE events TYPE array<float>;",
    "DEFINE FIELD IF NOT EXISTS published_by ON events TYPE option<record<sources>>;",
    "DEFINE FIELD IF NOT EXISTS same_as ON events TYPE option<array<record<events>>>;",
    "DEFINE INDEX IF NOT EXISTS url_idx ON TABLE events COLUMNS url UNIQUE;",
    "DEFINE INDEX IF NOT EXISTS base_url_idx ON TABLE events COLUMNS base_url;",
    "DEFINE INDEX IF NOT EXISTS created_idx ON TABLE events COLUMNS created;",
    # "DEFINE INDEX IF NOT EXISTS tags_idx ON TABLE user COLUMNS tags SEARCH;",
    f"DEFINE INDEX IF NOT EXISTS embedding_idx ON events FIELDS embedding HNSW DIMENSION {VECTOR_LEN} DIST COSINE;",
    # sources tables, fields and indexes
    "DEFINE TABLE IF NOT EXISTS sources SCHEMALESS;",
    "DEFINE FIELD IF NOT EXISTS base_url ON TABLE sources TYPE string;",
    "DEFINE INDEX IF NOT EXISTS base_url_idx ON TABLE sources COLUMNS base_url UNIQUE;"
    # composite singals
    "DEFINE TABLE IF NOT EXISTS signals SCHEMALESS;",
]

_OPTIMIZE_STMTS = [
    "REBUILD INDEX IF EXISTS embedding_idx ON TABLE events;"
]

from surrealdb import AsyncSurreal, AsyncHttpSurrealConnection, AsyncEmbeddedSurrealConnection, AsyncWsSurrealConnection


class Cupboard:
    db_path: str
    db: AsyncEmbeddedSurrealConnection|AsyncWsSurrealConnection|AsyncHttpSurrealConnection

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = None

    async def __aenter__(self):
        self.db = AsyncSurreal(self.db_path)
        # await self.db.connect(self.db_path)
        token = await self.db.signin({"username": "root", "password": "pass"})
        # await self.db.authenticate(ic(token))
        await self.db.use(NS, DB)
        await self._init_schema()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # if self.db:
        #     await self.db.close()
        pass

    async def close(self):
        await self.db.close()

    async def _init_schema(self):
        await self.db.query("\n".join(_INIT_STMTS))

    async def store(self, data_type: str, items: list[dict[str, Any]]):
        if not items:
            return 0

        if data_type == EVENTS:
            events = await self._link_sources_to_events(items)
            inserted = await self._insert_ignore(data_type, events)
        if data_type == SOURCES:
            inserted = await self._insert_ignore(data_type, items)
            await self._link_sources_to_events(inserted)

        return len(inserted)

    async def _insert_ignore(self, data_type: str, items: list[dict[str, Any]]):
        """Insert a batch while skipping records that already exist."""
        return await self.db.query(f"INSERT IGNORE INTO {data_type} $items", {"items": items})

    async def _link_sources_to_events(self, events: list[dict[str, Any]]):
        """Attaches `published_by` field in the to-be-inserted `events` to link to a corresponding `source` defined by `base_url`."""
        existing_sources = await self.db.query(
            "SELECT id, base_url FROM sources WHERE base_url IN $bu",
            {"bu": list({e[K_BASE_URL] for e in events})},
        )
        if not existing_sources:
            return events

        source_ids = {s[K_BASE_URL]: s["id"] for s in existing_sources}
        for item in events:
            if src_id := source_ids.get(item[K_BASE_URL]):
                item["published_by"] = src_id

        return events

    async def _link_events_to_sources(self, sources: list[dict[str, Any]]):
        """Bulk updates matching events with the corresponding source record."""
        expr = "UPDATE events MERGE { published_by: $source_id } WHERE base_url = $base_url"
        await asyncio.gather(
            *[
                self.db.query(
                    expr, {"base_url": src[K_BASE_URL], "source_id": src["id"]}
                )
                for src in sources
            ]
        )

    # async def _populate_related_events(self, events: list[dict[str, Any]]):
    #     urls = [e[K_URL] for e in events if e.get(K_URL)]
    #     if not urls:
    #         return
    #     linked = await self.db.query(
    #         f"SELECT id, base_url FROM events WHERE url IN $urls",
    #         {"urls": urls},
    #     )
    #     if not linked:
    #         return
    #     linked = linked[0] if isinstance(linked, list) else []
    #     linked_ids = [e["id"] for e in linked if e.get("id")]
    #     if not linked_ids:
    #         return

    #     base_url_groups = {}
    #     for e in linked:
    #         bu = e.get("base_url")
    #         if bu:
    #             if bu not in base_url_groups:
    #                 base_url_groups[bu] = []
    #             base_url_groups[bu].append(e["id"])

    #     update_stmts = []
    #     for bu, ids in base_url_groups.items():
    #         if len(ids) > 1:
    #             for eid in ids:
    #                 others = [i for i in ids if i != eid]
    #                 update_stmts.append(
    #                     f"UPDATE events:{eid} SET related_events = {others}"
    #                 )
    #     if update_stmts:
    #         await asyncio.gather(*[self.db.query(s) for s in update_stmts])

    # async def _populate_related_events_by_source(self, sources: list[dict[str, Any]]):
    #     base_urls = [s[K_BASE_URL] for s in sources if s.get(K_BASE_URL)]
    #     if not base_urls:
    #         return
    #     events = await self.db.query(
    #         "SELECT id, base_url FROM events WHERE base_url IN $bu LIMIT 10000",
    #         {"bu": base_urls},
    #     )
    #     if not events:
    #         return
    #     events = events[0] if isinstance(events, list) else []

    #     base_url_groups = {}
    #     for e in events:
    #         bu = e.get("base_url")
    #         if bu:
    #             if bu not in base_url_groups:
    #                 base_url_groups[bu] = []
    #             base_url_groups[bu].append(e["id"])

    #     update_stmts = []
    #     for bu, ids in base_url_groups.items():
    #         if len(ids) > 1:
    #             for eid in ids:
    #                 others = [i for i in ids if i != eid]
    #                 update_stmts.append(
    #                     f"UPDATE events:{eid} SET related_events = {others}"
    #                 )
    #     if update_stmts:
    #         await asyncio.gather(*[self.db.query(s) for s in update_stmts])

    # async def link_events(self, items: list[dict[str, str]]):
    #     if not items:
    #         return 0

    #     all_urls = list(
    #         set(
    #             u
    #             for item in items
    #             for u in [item.get(K_URL), item.get("related_url")]
    #             if u
    #         )
    #     )
    #     raw = await self.db.query(
    #         f"SELECT id, url FROM events WHERE url IN $urls",
    #         {"urls": all_urls},
    #     )
    #     if not raw:
    #         return 0
    #     records = raw[0] if isinstance(raw, list) else raw
    #     url_to_id = {r["url"]: r["id"] for r in records if r.get("url")}

    #     relate_stmts = []
    #     linked_event_ids = set()
    #     for item in items:
    #         src_url = item.get(K_URL)
    #         rel_url = item.get("related_url")
    #         src_id = url_to_id.get(src_url)
    #         rel_id = url_to_id.get(rel_url)
    #         if src_id and rel_id:
    #             relate_stmts.append(
    #                 f"RELATE events:{src_id}->event_links:->events:{rel_id} SET link_type = 'SAME_AS', created_at = time::now()"
    #             )
    #             linked_event_ids.add(src_id)
    #             linked_event_ids.add(rel_id)

    #     if relate_stmts:
    #         await asyncio.gather(*[self.db.query(s) for s in relate_stmts])

    #     if linked_event_ids:
    #         linked_list = list(linked_event_ids)
    #         events_grouped = await self.db.query(
    #             f"SELECT id, base_url FROM events WHERE id IN $ids LIMIT 10000",
    #             {"ids": linked_list},
    #         )
    #         if events_grouped:
    #             eg = (
    #                 events_grouped[0]
    #                 if isinstance(events_grouped[0], list)
    #                 else events_grouped
    #             )
    #             base_url_groups = {}
    #             for e in eg:
    #                 bu = e.get("base_url")
    #                 if bu:
    #                     if bu not in base_url_groups:
    #                         base_url_groups[bu] = []
    #                     base_url_groups[bu].append(e["id"])

    #             update_stmts = []
    #             for bu, ids in base_url_groups.items():
    #                 if len(ids) > 1:
    #                     for eid in ids:
    #                         others = [i for i in ids if i != eid]
    #                         update_stmts.append(
    #                             f"UPDATE events:{eid} SET related_events = {others}"
    #                         )
    #             if update_stmts:
    #                 await asyncio.gather(*[self.db.query(s) for s in update_stmts])

    #     return len(relate_stmts)

    # async def get_events_by_property(self, prop_name: str, prop_values: list):
    #     raw = await self.db.query(
    #         f"SELECT id, {prop_name} FROM events WHERE {prop_name} IN $vals LIMIT 10000",
    #         {"vals": prop_values},
    #     )
    #     if not raw:
    #         return {}
    #     records = raw[0] if isinstance(raw, list) else raw
    #     result = {}
    #     for r in records:
    #         val = r.get(prop_name)
    #         if val is not None:
    #             if val not in result:
    #                 result[val] = []
    #             result[val].append(r["id"])
    #     return result

    async def query_events(
        self,
        embedding: list[float] = None,
        conditions: dict[str, Any] | list[str] = None,
        distance: float = None,
        limit: int = None,
        columns: list[str] | None = None,
    ):

        field_expr = "*"
        where_parts = []
        params = {}
        order = None

        if columns: 
            field_expr = ", ".join(columns)

        if conditions:
            op = lambda val: "=" if not isinstance(val, list) else "INSIDE"
            if isinstance(conditions, list): 
                where_parts.extend(conditions)
            elif isinstance(conditions, dict):
                where_parts.extend([f"{k} {op(v)} ${k}" for k in conditions])
                params = {f"${k}": v for k,v in conditions.items()}
        
        if embedding:
            field_expr += ", vector::distance::knn() AS distance"
            where_parts.append(f"embedding <|{limit}, COSINE|> $embedding")
            params["embedding"] = embedding
            
            if distance:
                where_parts.append("distance <= $distance")
                params['distance']

            order = ["distance"]


        expr = f"SELECT {field_expr} FROM events"
        if where_parts: 
            expr += " WHERE "+" AND ".join(where_parts)
        if order:
            expr += " ORDER BY "+ ", ".join(order)
        if limit:
            expr += " LIMIT $limit"
            params['limit'] = limit

        return await self.db.query(expr, params)

    async def optimize(self):
        await self.db.query_raw("\n".join(_OPTIMIZE_STMTS))
