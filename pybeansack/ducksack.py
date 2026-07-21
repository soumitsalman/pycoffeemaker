import os
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Any

import duckdb
from duckdb import TransactionException
import pandas as pd
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random

from .database import *
from .models import *
from utils.config import CLEANUP_WINDOW, CLUSTER_EPS, VECTOR_LEN
from utils.dates import ndays_ago, ndays_ago_str, now

log = logging.getLogger(__name__)

RETRY_COUNT = 10
RETRY_DELAY = (1, 5)  # seconds

ORDER_BY_LATEST = "created DESC"
ORDER_BY_TRENDING = "trend_score DESC"
ORDER_BY_DISTANCE = "distance ASC"

_TYPES = {
    BEANS: Bean,
    PUBLISHERS: Publisher,
    CHATTERS: Chatter,
    "trending_beans_view": TrendingBean,
    "aggregated_beans_view": AggregatedBean,
}

_PRIMARY_KEYS: dict[str, str | list[str]] = {
    BEANS: URL,
    PUBLISHERS: SOURCE,
    RELATED_BEANS: [URL, "related_url"],
}


def _split_from_to(dt: DATETIME) -> tuple[datetime | None, datetime | None]:
    if dt is None:
        return None, None
    if isinstance(dt, datetime):
        return dt, None
    return dt[0], dt[1]


def _primary_key_fields(table: str) -> list[str]:
    pk = _PRIMARY_KEYS.get(table)
    if not pk:
        return []
    return [pk] if isinstance(pk, str) else list(pk)


_EXCLUDE_COLUMNS = ["tags", "chatter", "publisher", "trend_score", "updated", "distance"]


def _beans_to_df(beans: list[Bean], columns: list[str] | None):
    if not beans:
        return None
    if columns:
        payload = [bean.model_dump(include=set(columns)) for bean in beans]
    else:
        payload = [bean.model_dump(exclude_none=True, exclude=set(_EXCLUDE_COLUMNS)) for bean in beans]
    df = pd.DataFrame(payload)
    if df.empty:
        return df
    fields = columns or [col for col in df.columns if df[col].notnull().any()]
    dtype_specs = {field: mapping for field, mapping in Bean.model_config.get("dtype_specs", {}).items() if field in fields}
    return df.astype(dtype_specs) if dtype_specs else df


def _publishers_to_df(publishers: list[Publisher]):
    if not publishers:
        return None
    return pd.DataFrame([pub.model_dump(exclude_none=True) for pub in publishers])


def _stamp_missing(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Fill missing timestamp columns; ducklake baseline has no DEFAULT CURRENT_TIMESTAMP."""
    ts = now()
    for col in columns:
        if col not in df.columns:
            df[col] = ts
        else:
            df[col] = df[col].fillna(ts)
    return df


class DuckSack(Beansack):
    """Unified DuckDB/DuckLake beansack backend."""

    db: duckdb.DuckDBPyConnection
    db_path: str | None
    catalog_db: str | None
    storage_path: str | None
    _mode: str

    def __init__(self, db_path: str = None, catalog_db: str = None, storage_path: str = None):
        """Initialize a DuckSack connection.

        Args:
            db_path: Path to a local DuckDB database file. Selects duckdb mode.
            catalog_db: DuckLake catalog connection (postgresql://... or sqlite path).
                Must be used together with storage_path.
            storage_path: DuckLake data path for parquet files. Must be used together
                with catalog_db.

        Raises:
            ValueError: If arguments do not match one of the supported modes.
        """
        if db_path and (catalog_db or storage_path):
            raise ValueError("Provide either db_path (duckdb) OR catalog_db+storage_path (ducklake), not both.")
        if db_path:
            self._mode = "duckdb"
            self.db_path = os.path.expanduser(db_path)
            self.catalog_db = None
            self.storage_path = None
            self.db = duckdb.connect(self.db_path, read_only=False)
            return

        if catalog_db and storage_path:
            self._mode = "ducklake"
            self.db_path = None
            self.catalog_db = catalog_db
            self.storage_path = os.path.expanduser(storage_path)

            config = {
                "threads": max((os.cpu_count() or 2) >> 1, 1),
                "enable_http_metadata_cache": True,
                "ducklake_max_retry_count": 100,
                "s3_access_key_id": os.getenv("S3_ACCESS_KEY_ID"),
                "s3_secret_access_key": os.getenv("S3_SECRET_ACCESS_KEY"),
                "s3_endpoint": os.getenv("S3_ENDPOINT"),
                "s3_region": os.getenv("S3_REGION"),
            }

            catalog_db_type = "postgres" if "postgresql://" in catalog_db else "sqlite"
            sql_connect = f"""
            INSTALL ducklake;
            LOAD ducklake;
            INSTALL {catalog_db_type};
            LOAD {catalog_db_type};
            ATTACH 'ducklake:{catalog_db}' AS warehouse (DATA_PATH '{self.storage_path}');
            USE warehouse;
            """
            self.db = duckdb.connect(config=config)
            self.execute(sql_connect)
            return

        raise ValueError("Provide db_path for duckdb, or catalog_db and storage_path for ducklake.")

    def _qualify(self, table: str) -> str:
        return f"warehouse.{table}" if self._mode == "ducklake" else table

    @contextmanager
    def cursor(self):
        with self.db.cursor() as cur:
            yield cur

    def _exists(self, table: str, fields: list[str], ids: list[Any]) -> list[Any]:
        if not ids:
            return []
        qualified = self._qualify(table)

        if len(fields) == 1:
            field = fields[0]
            sql_exists = f"SELECT {field} FROM {qualified} WHERE {field} IN ({','.join('?' for _ in ids)})"
            return [row[field] for row in self.query(sql_exists, params=list(ids))]

        placeholders = ", ".join(["(" + ", ".join(["?"] * len(fields)) + ")"] * len(ids))
        flat_params: list[Any] = []
        for key in ids:
            flat_params.extend(list(key))
        fields_expr = ", ".join(fields)
        sql_exists = f"""
        SELECT {fields_expr} FROM {qualified}
        WHERE ({fields_expr}) IN ({placeholders})
        """
        return [tuple(row[f] for f in fields) for row in self.query(sql_exists, params=flat_params)]

    def deduplicate(self, table: str, items: list) -> list:
        if not items:
            return items
        pk_fields = _primary_key_fields(table)
        if not pk_fields:
            return items

        if len(pk_fields) == 1:
            field = pk_fields[0]
            ids = [getattr(item, field) for item in items]
            existing = set(self._exists(table, [field], ids))
            return [item for _id, item in zip(ids, items) if _id not in existing]

        ids = [tuple(getattr(item, f) for f in pk_fields) for item in items]
        existing = set(self._exists(table, pk_fields, ids))
        return [item for _id, item in zip(ids, items) if _id not in existing]

    @retry(
        retry=retry_if_exception_type(TransactionException),
        stop=stop_after_attempt(RETRY_COUNT),
        wait=wait_random(*RETRY_DELAY),
        reraise=True,
    )
    def _execute_df(self, sql_expr: str):
        with self.cursor() as cur:
            cur.execute(sql_expr)
        return True

    def store_beans(self, beans: list[Bean]):
        if not beans:
            return 0
        df = _beans_to_df(beans, None)
        if df is None or df.empty:
            return 0
        df = _stamp_missing(df, [CREATED, COLLECTED])
        fields = ", ".join(df.columns.to_list())

        qualified = self._qualify(BEANS)
        sql_insert = f"""
        INSERT INTO {qualified} ({fields})
        SELECT {fields} FROM df
        WHERE NOT EXISTS (
            SELECT 1 FROM {qualified} b
            WHERE b.url = df.url
        );
        """
        with self.db.cursor() as cur:
            cur.execute(sql_insert)
        return len(df)

    def store_related(self, related_beans: list[dict]):
        if not related_beans:
            return 0
        df = pd.DataFrame(related_beans)
        if df.empty:
            return 0
        fields = ", ".join(df.columns.to_list())

        qualified = self._qualify(RELATED_BEANS)
        sql_insert = f"""
        INSERT INTO {qualified} ({fields})
        SELECT {fields} FROM df
        WHERE NOT EXISTS (
            SELECT 1 FROM {qualified} rb
            WHERE rb.url = df.url AND rb.related_url = df.related_url
        );
        """
        with self.db.cursor() as cur:
            cur.execute(sql_insert)
        return len(df)

    def store_publishers(self, publishers: list[Publisher]):
        df = _publishers_to_df(publishers)
        if df is None or df.empty:
            return 0
        df = _stamp_missing(df, [COLLECTED])
        fields = ", ".join(df.columns.to_list())

        qualified = self._qualify(PUBLISHERS)
        sql_insert = f"""
        INSERT INTO {qualified} ({fields})
        SELECT {fields} FROM df
        WHERE source NOT IN (
            SELECT source FROM {qualified} p
        );
        """
        with self.db.cursor() as cur:
            cur.execute(sql_insert)
        return len(df)

    def store_chatters(self, chatters: list[Chatter]):
        if not chatters:
            return 0
        df = pd.DataFrame([chatter.model_dump(exclude=[UPDATED]) for chatter in chatters])
        if df.empty:
            return 0
        df = _stamp_missing(df, [COLLECTED])
        fields = ", ".join(df.columns.to_list())

        qualified = self._qualify(CHATTERS)
        sql_insert = f"""
        INSERT INTO {qualified} ({fields})
        SELECT {fields} FROM df;
        """
        with self.db.cursor() as cur:
            cur.execute(sql_insert)
        return len(df)

    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        if not beans:
            return 0
        df = _beans_to_df(beans, list(set((columns or []) + [URL])) if columns else None)
        if df is None or df.empty:
            return 0

        fields = columns or df.columns.to_list()
        if URL in fields:
            fields = [f for f in fields if f != URL]
        if not fields:
            return 0

        updates = ", ".join([f"{f} = pack.{f}" for f in fields])
        qualified = self._qualify(BEANS)
        sql_update = f"""
        MERGE INTO {qualified}
        USING (SELECT url, {', '.join(fields)} FROM df) AS pack
        USING (url)
        WHEN MATCHED THEN UPDATE SET {updates};
        """
        with self.db.cursor() as cur:
            cur.execute(sql_update)
        return len(df)

    def update_embeddings(self, beans: list[Bean]):
        if not beans:
            return 0
        df = _beans_to_df(beans, [URL, EMBEDDING])
        if df is None or df.empty:
            return 0

        qualified_beans = self._qualify(BEANS)
        qualified_categories = self._qualify(FIXED_CATEGORIES)
        qualified_sentiments = self._qualify(FIXED_SENTIMENTS)

        sql_update = f"""
        WITH update_pack AS (
            SELECT
                url,
                ANY_VALUE(embedding) AS embedding,
                LIST(DISTINCT fc.category) AS categories,
                LIST(DISTINCT fs.sentiment) AS sentiments
            FROM df
            LEFT JOIN LATERAL (
                SELECT category FROM {qualified_categories}
                ORDER BY array_cosine_distance(df.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
                LIMIT 2
            ) fc ON TRUE
            LEFT JOIN LATERAL (
                SELECT sentiment FROM {qualified_sentiments}
                ORDER BY array_cosine_distance(df.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
                LIMIT 2
            ) fs ON TRUE
            GROUP BY df.url
        )
        MERGE INTO {qualified_beans}
        USING (SELECT * FROM update_pack) AS pack
        USING (url)
        WHEN MATCHED THEN UPDATE SET embedding = pack.embedding, categories = pack.categories, sentiments = pack.sentiments;
        """
        with self.db.cursor() as cur:
            cur.execute(sql_update)
        return len(df)

    def update_publishers(self, publishers: list[Publisher]):
        df = _publishers_to_df(publishers)
        if df is None or df.empty:
            return 0
        fields = df.columns.to_list()
        if SOURCE in fields:
            fields.remove(SOURCE)
        if not fields:
            return 0

        updates = ", ".join([f"{f} = pack.{f}" for f in fields])
        qualified = self._qualify(PUBLISHERS)
        sql_update = f"""
        MERGE INTO {qualified}
        USING (SELECT source, {', '.join(fields)} FROM df) AS pack
        USING (source)
        WHEN MATCHED THEN UPDATE SET {updates};
        """
        with self.db.cursor() as cur:
            cur.execute(sql_update)
        return len(df)

    def _select(self, table: str, columns: list[str] = None, embedding: list[float] = None):
        fields = columns.copy() if columns else ["*"]
        if embedding:
            fields.append(
                f"array_cosine_distance(embedding::FLOAT[{VECTOR_LEN}], ?::FLOAT[{VECTOR_LEN}]) AS distance"
            )
            return f"SELECT {', '.join(fields)} FROM {self._qualify(table)}", [embedding]
        return f"SELECT {', '.join(fields)} FROM {self._qualify(table)}", []

    def _where(
        self,
        urls: list[str] = None,
        kind: str = None,
        created: DATETIME = None,
        collected: DATETIME = None,
        updated: DATETIME = None,
        categories: list[str] = None,
        regions: list[str] = None,
        entities: list[str] = None,
        tags: list[str] = None,
        sources: list[str] = None,
        distance: float = 0,
        conditions: list[str] = None,
    ):
        exprs: list[str] = []
        params: list[Any] = []

        if urls:
            exprs.append(f"url IN ({', '.join('?' for _ in urls)})")
            params.extend(urls)
        if kind:
            exprs.append("kind = ?")
            params.append(kind)

        if created:
            _from, _to = _split_from_to(created)
            if _from:
                exprs.append("created >= ?")
                params.append(_from)
            if _to:
                exprs.append("created <= ?")
                params.append(_to)

        if collected:
            _from, _to = _split_from_to(collected)
            if _from:
                exprs.append("collected >= ?")
                params.append(_from)
            if _to:
                exprs.append("collected <= ?")
                params.append(_to)

        if updated:
            _from, _to = _split_from_to(updated)
            if _from:
                exprs.append("updated >= ?")
                params.append(_from)
            if _to:
                exprs.append("updated <= ?")
                params.append(_to)

        if categories:
            exprs.append("ARRAY_HAS_ANY(categories, ?)")
            params.append(categories)
        if regions:
            exprs.append("ARRAY_HAS_ANY(regions, ?)")
            params.append(regions)
        if entities:
            exprs.append("ARRAY_HAS_ANY(entities, ?)")
            params.append(entities)

        if tags:
            exprs.append("(ARRAY_HAS_ANY(regions, ?) OR ARRAY_HAS_ANY(entities, ?) OR ARRAY_HAS_ANY(categories, ?))")
            params.extend([tags, tags, tags])

        if sources:
            exprs.append(f"source IN ({', '.join('?' for _ in sources)})")
            params.extend(sources)

        if distance:
            exprs.append("distance <= ?")
            params.append(distance)

        if conditions:
            exprs.extend(conditions)

        if not exprs:
            return "", []
        return " WHERE " + " AND ".join(exprs), params

    def _fetch_all(
        self,
        table: str,
        urls: list[str] = None,
        kind: str = None,
        created: DATETIME = None,
        collected: DATETIME = None,
        updated: DATETIME = None,
        categories: list[str] = None,
        regions: list[str] = None,
        entities: list[str] = None,
        tags: list[str] = None,
        sources: list[str] = None,
        embedding: list[float] = None,
        distance: float = 0,
        conditions: list[str] = None,
        order: str = None,
        limit: int = 0,
        offset: int = 0,
        columns: list[str] = None,
    ):
        select_expr, select_params = self._select(table, columns, embedding)
        where_expr, where_params = self._where(
            urls=urls,
            kind=kind,
            created=created,
            collected=collected,
            updated=updated,
            categories=categories,
            regions=regions,
            entities=entities,
            tags=tags,
            sources=sources,
            distance=distance,
            conditions=conditions,
        )
        expr = select_expr + where_expr
        params: list[Any] = []
        params.extend(select_params)
        params.extend(where_params)

        with self.db.cursor() as cur:
            rel = cur.query(expr, params=params)
            if order:
                rel = rel.order(order)
            if embedding:
                rel = rel.order(ORDER_BY_DISTANCE)
            if offset or limit:
                rel = rel.limit(limit, offset=offset)
            items = [dict(zip(rel.columns, row)) for row in rel.fetchall()]
            if table in _TYPES:
                items = [_TYPES[table](**item) for item in items]
        return items

    def query_latest_beans(
        self,
        kind: str = None,
        created: DATETIME = None,
        collected: DATETIME = None,
        categories: list[str] = None,
        regions: list[str] = None,
        entities: list[str] = None,
        tags: list[str] = None,
        sources: list[str] = None,
        embedding: list[float] = None,
        distance: float = 0,
        conditions: list[str] = None,
        limit: int = 0,
        offset: int = 0,
        columns: list[str] = None,
    ) -> list[Bean]:
        return self._fetch_all(
            table=BEANS,
            urls=None,
            kind=kind,
            created=created,
            collected=collected,
            categories=categories,
            regions=regions,
            entities=entities,
            tags=tags,
            sources=sources,
            embedding=embedding,
            distance=distance,
            conditions=conditions,
            order=ORDER_BY_LATEST,
            limit=limit,
            offset=offset,
            columns=columns,
        )

    def query_trending_beans(
        self,
        kind: str = None,
        updated: DATETIME = None,
        collected: DATETIME = None,
        categories: list[str] = None,
        regions: list[str] = None,
        entities: list[str] = None,
        tags: list[str] = None,
        sources: list[str] = None,
        embedding: list[float] = None,
        distance: float = 0,
        conditions: list[str] = None,
        limit: int = 0,
        offset: int = 0,
        columns: list[str] = None,
    ) -> list[TrendingBean]:
        return self._fetch_all(
            table="trending_beans_view",
            urls=None,
            kind=kind,
            updated=updated,
            collected=collected,
            categories=categories,
            regions=regions,
            entities=entities,
            tags=tags,
            sources=sources,
            embedding=embedding,
            distance=distance,
            conditions=conditions,
            order=ORDER_BY_TRENDING,
            limit=limit,
            offset=offset,
            columns=columns,
        )

    def query_aggregated_beans(
        self,
        kind: str = None,
        created: DATETIME = None,
        collected: DATETIME = None,
        updated: DATETIME = None,
        categories: list[str] = None,
        regions: list[str] = None,
        entities: list[str] = None,
        tags: list[str] = None,
        sources: list[str] = None,
        embedding: list[float] = None,
        distance: float = 0,
        conditions: list[str] = None,
        limit: int = 0,
        offset: int = 0,
        columns: list[str] = None,
    ) -> list[AggregatedBean]:
        return self._fetch_all(
            table="aggregated_beans_view",
            urls=None,
            kind=kind,
            created=created,
            collected=collected,
            updated=updated,
            categories=categories,
            regions=regions,
            entities=entities,
            tags=tags,
            sources=sources,
            embedding=embedding,
            distance=distance,
            conditions=conditions,
            order=ORDER_BY_LATEST,
            limit=limit,
            offset=offset,
            columns=columns,
        )

    def query_publishers(
        self,
        collected: DATETIME = None,
        tags: list[str] = None,
        sources: list[str] = None,
        conditions: list[str] = None,
        limit: int = 0,
        offset: int = 0,
        columns: list[str] = None,
    ) -> list[Publisher]:
        return self._fetch_all(
            table=PUBLISHERS,
            urls=None,
            collected=collected,
            tags=tags,
            sources=sources,
            conditions=conditions,
            limit=limit,
            offset=offset,
            columns=columns,
        )

    def distinct_categories(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = f"SELECT category FROM {self._qualify(FIXED_CATEGORIES)} ORDER BY category"
        if limit:
            expr += " LIMIT ?"
            return [row["category"] for row in self.query(expr, params=[limit])]
        return [row["category"] for row in self.query(expr)]

    def distinct_sentiments(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = f"SELECT sentiment FROM {self._qualify(FIXED_SENTIMENTS)} ORDER BY sentiment"
        if limit:
            expr += " LIMIT ?"
            return [row["sentiment"] for row in self.query(expr, params=[limit])]
        return [row["sentiment"] for row in self.query(expr)]

    def distinct_entities(self, limit: int = 0, offset: int = 0) -> list[str]:
        qualified = self._qualify(BEANS)
        expr = f"SELECT DISTINCT unnest(entities) AS entity FROM {qualified} WHERE entities IS NOT NULL ORDER BY entity"
        return [row["entity"] for row in self.query(expr)]

    def distinct_regions(self, limit: int = 0, offset: int = 0) -> list[str]:
        qualified = self._qualify(BEANS)
        expr = f"SELECT DISTINCT unnest(regions) AS region FROM {qualified} WHERE regions IS NOT NULL ORDER BY region"
        return [row["region"] for row in self.query(expr)]

    def distinct_publishers(self, limit: int = 0, offset: int = 0) -> list[str]:
        qualified = self._qualify(PUBLISHERS)
        expr = f"SELECT source FROM {qualified} ORDER BY source"
        return [row["source"] for row in self.query(expr)]

    def count_rows(self, table: str, conditions: list[str] = None) -> int:
        where_expr, where_params = self._where(conditions=conditions)
        expr = f"SELECT count(*) AS count FROM {self._qualify(table)}{where_expr};"
        rows = self.query(expr, params=where_params)
        return int(rows[0]["count"]) if rows else 0

    def query(self, query_expr: str, params: list[Any] = None) -> list[dict]:
        with self.db.cursor() as cur:
            rel = cur.query(query_expr, params=params or [])
            return [dict(zip(rel.columns, row)) for row in rel.fetchall()]

    def execute(self, expr: str, params: list[Any] = None):
        with self.db.cursor() as cur:
            cur.execute(expr, params or [])

    def optimize(self):
        qualified_beans = self._qualify(BEANS)
        qualified_chatters = self._qualify(CHATTERS)
        qualified_related = self._qualify(RELATED_BEANS)
        qualified_trends = self._qualify("trend_aggregates")

        sql_refresh = f"""
        DELETE FROM {qualified_trends};

        INSERT INTO {qualified_trends}
        WITH
            max_chatters AS (
                SELECT
                    chatter_url,
                    MAX(likes) AS likes,
                    MAX(comments) AS comments
                FROM {qualified_chatters}
                GROUP BY chatter_url
            ),
            first_seen_max_chatters AS (
                SELECT
                    fs.chatter_url,
                    MIN(fs.collected) AS collected
                FROM {qualified_chatters} fs
                LEFT JOIN max_chatters mx ON fs.chatter_url = mx.chatter_url
                WHERE fs.likes = mx.likes AND fs.comments = mx.comments
                GROUP BY fs.chatter_url
            ),
            chatter_stats AS (
                SELECT
                    url,
                    DATE(MAX(collected)) AS updated,
                    SUM(likes) AS likes,
                    SUM(comments) AS comments,
                    SUM(subscribers) AS subscribers,
                    SUM(shares) AS shares
                FROM (
                    SELECT ch.* FROM {qualified_chatters} ch
                    LEFT JOIN first_seen_max_chatters fs ON fs.chatter_url = ch.chatter_url
                    WHERE fs.collected = ch.collected
                )
                GROUP BY url
            ),
            related_stats AS (
                SELECT url, COUNT(*) AS related
                FROM {qualified_related}
                GROUP BY url
            ),
            trend_stats AS (
                SELECT
                    b.url,
                    COALESCE(cg.likes, 0) AS likes,
                    COALESCE(cg.comments, 0) AS comments,
                    COALESCE(cg.subscribers, 0) AS subscribers,
                    COALESCE(cg.shares, 0) AS shares,
                    COALESCE(rg.related, 0) AS related,
                    GREATEST(DATE(b.created), COALESCE(cg.updated, DATE(b.created))) AS updated
                FROM {qualified_beans} b
                LEFT JOIN related_stats rg ON b.url = rg.url
                LEFT JOIN chatter_stats cg ON b.url = cg.url
            )
        SELECT
            url,
            likes,
            comments,
            subscribers,
            shares,
            related,
            updated,
            CAST((100 * related + 50 * comments + 10 * shares + likes) AS FLOAT) / (CURRENT_DATE + 2 - updated) AS trend_score
        FROM trend_stats
        WHERE GREATEST(likes, comments, shares, related) > 0;
        """
        self.execute(sql_refresh)

    def cleanup(self):
        if self._mode != "ducklake":
            return
        sql_cleanup = """
        CALL ducklake_expire_snapshots('warehouse', older_than => now() - INTERVAL '1 day');
        CALL ducklake_merge_adjacent_files('warehouse');
        CALL ducklake_cleanup_old_files('warehouse', cleanup_all => true);
        CALL ducklake_delete_orphaned_files('warehouse', cleanup_all => true);
        """
        self.execute(sql_cleanup)

    # def snapshot(self):
    #     if self._mode != "ducklake":
    #         return None
    #     sql_current_snapshot = "SELECT * FROM warehouse.current_snapshot();"
    #     rows = self.query(sql_current_snapshot)
    #     return rows[0] if rows else None

    # def backup(self, store_func: Callable):
    #     if self._mode != "duckdb":
    #         raise ValueError("backup() is only supported in duckdb mode.")
    #     if not self.db_path:
    #         raise ValueError("DuckDB path not set.")
    #     with open(self.db_path, "rb") as data:
    #         store_func(data)

    def close(self):
        if not self.db:
            return
        self.db.close()


def create_db(*, db_path: str = None, catalog_db: str = None, storage_path: str = None) -> DuckSack:
    with open(os.path.join(os.path.dirname(__file__), "ducksack.sql"), "r") as sql_file:
        init_sql = sql_file.read()

    if db_path:
        os.makedirs(os.path.dirname(os.path.expanduser(db_path)) or ".", exist_ok=True)
        db = DuckSack(db_path=db_path)
        db.execute(init_sql)
        return db

    if catalog_db and storage_path:
        s3_endpoint = os.getenv("S3_ENDPOINT", "")
        s3_region = os.getenv("S3_REGION", "")
        s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID", "")
        s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY", "")

        catalog_db_type = "postgres" if "postgresql://" in catalog_db else "sqlite"
        attach_sql = f"""
        INSTALL ducklake;
        LOAD ducklake;
        INSTALL sqlite;
        LOAD sqlite;
        INSTALL httpfs;
        LOAD httpfs;
        INSTALL {catalog_db_type};
        LOAD {catalog_db_type};

        CREATE OR REPLACE SECRET s3secret (
            TYPE s3,
            PROVIDER config,
            ENDPOINT '{s3_endpoint}',
            REGION '{s3_region}',
            KEY_ID '{s3_access_key_id}',
            SECRET '{s3_secret_access_key}'
        );

        ATTACH 'ducklake:{catalog_db}' AS warehouse (DATA_PATH '{os.path.expanduser(storage_path)}');
        USE warehouse;
        """
        db = DuckSack(catalog_db=catalog_db, storage_path=storage_path)
        db.execute(attach_sql)        
        db.execute(init_sql)
        return db

    raise ValueError("Provide db_path for duckdb, or catalog_db and storage_path for ducklake.")

