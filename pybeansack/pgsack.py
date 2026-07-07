from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import os
import logging
from pathlib import Path
from typing import Any
from itertools import batched, chain
import pandas as pd
from psycopg import sql
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector
from pgvector import Vector
from .models import *
from utils.collections import non_null_fields
from .database import *
from tenacity import retry, stop_after_attempt, wait_fixed

PG_TIMEOUT = int(os.getenv('PG_TIMEOUT', 300))
PG_WORKERS = int(os.getenv('PG_WORKERS', 4))
BATCH_SIZE = 512
RETRY_COUNT = 3
RETRY_DELAY = 15
_store_executor = ThreadPoolExecutor(max_workers=PG_WORKERS, thread_name_prefix="pgstore")

_TYPES = {
    BEANS: Bean,
    PUBLISHERS: Publisher,
    CHATTERS: Chatter,
    "trending_beans_view": TrendingBean,
    "aggregated_beans_view": AggregatedBean,
}

_PRIMARY_KEYS = {
    BEANS: URL,
    PUBLISHERS: SOURCE,
    RELATED_BEANS: [URL, "related_url"],
}

ORDER_BY_LATEST = "created DESC"
ORDER_BY_TRENDING = "trend_score DESC"
ORDER_BY_DISTANCE = "distance ASC"

log = logging.getLogger(__name__)

_create_row = lambda item, columns: tuple(Vector(item.get(column)) if column == EMBEDDING else item.get(column) for column in columns)

def _primary_key_fields(table: str) -> list[str]:
    pk = _PRIMARY_KEYS.get(table)
    if not pk: return []
    return [pk] if isinstance(pk, str) else list(pk)

def _conflict_target(table: str):
    pk_fields = _primary_key_fields(table)
    if not pk_fields: return sql.SQL("")
    return sql.SQL("ON CONFLICT ({}) DO NOTHING").format(
        sql.SQL(', ').join(map(sql.Identifier, pk_fields))
    )

def _insert_multivalues_sql(table: str, columns: list[str], rowcount: int):
    row_ph = sql.SQL("(" + ",".join(["%s"] * len(columns)) + ")")
    return sql.SQL("INSERT INTO {table} ({fields}) VALUES {values}{on_conflict}").format(
        table=sql.Identifier(table),
        fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        values=sql.SQL(", ").join(row_ph for _ in range(rowcount)),
        on_conflict=_conflict_target(table),
    )

class PGSack(Beansack):
    pool: ConnectionPool

    def __init__(self, conn_str: str):
        """Initialize the Beansack with a PostgreSQL connection string."""
        self.pool = ConnectionPool(
            conn_str, 
            min_size=0,
            max_size=16,
            timeout=PG_TIMEOUT,
            max_idle=120,
            max_lifetime=180,
            num_workers=PG_WORKERS,
            configure=register_vector
        )
        self.pool.open()

    @contextmanager
    def cursor(self):
        """Get a new transaction context manager."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                yield cur
                conn.commit()
    
    # STORE METHODS
    def deduplicate(self, table: str, items: list) -> list:
        if not items: return items
        pk_fields = _primary_key_fields(table)
        if len(pk_fields) != 1:
            raise ValueError(f"deduplicate only supports single-column primary keys, got {pk_fields!r}")
        get_id = lambda item: getattr(item, pk_fields[0])
        ids = [get_id(item) for item in items]

        SQL_DEDUP = sql.SQL("""
        SELECT unnest(%(ids)s::varchar[]) AS id
        EXCEPT
        SELECT {pk_col} FROM {table};
        """).format(
            table=sql.Identifier(table),
            pk_col=sql.Identifier(_PRIMARY_KEYS[table])
        )
        non_existing_ids = self._query_scalars(SQL_DEDUP, {"ids": ids})
        return [item for item in items if get_id(item) in non_existing_ids]

    @retry(stop=stop_after_attempt(RETRY_COUNT), wait=wait_fixed(RETRY_DELAY), reraise=True)
    def _store(self, table: str, items: list[dict | BaseModel]) -> int:
        if not items: return 0

        if isinstance(items[0], BaseModel): data = [item.model_dump() for item in items]
        elif isinstance(items[0], dict): data = items
        else: raise ValueError("Items must be a list of dicts or BaseModel instances.")

        columns = non_null_fields(data)
        if not columns: return 0

        # rows = [_create_row(item, columns) for item in data]
        row_placeholder = sql.SQL("(" + ",".join(["%s"] * len(columns)) + ")")
        store_batches = [
            {
                "expr": sql.SQL(
                    "INSERT INTO {table} ({cols}) VALUES {values} {on_conflict}"
                ).format(
                    table=sql.Identifier(table),
                    cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
                    values=sql.SQL(", ").join(row_placeholder for _ in chunk),
                    on_conflict=_conflict_target(table),
                ),
                "params": list(chain.from_iterable(_create_row(item, columns) for item in chunk)),
            }
            for chunk in batched(data, BATCH_SIZE)
        ]

        def insert_chunk(chunk: dict):
            with self.pool.connection() as conn:
                return conn.execute(chunk["expr"], params=chunk["params"], binary=True).rowcount
        
        counts = list(_store_executor.map(insert_chunk, store_batches))
        return sum(counts)

    def store_beans(self, beans: list[Bean]):
        """Store a list of Beans in the database."""
        return self._store(BEANS, beans)
    
    def store_related(self, related_beans: list[dict]):
        return self._store(RELATED_BEANS, related_beans)
    
    def store_publishers(self, publishers: list[Publisher]):
        """Store a list of Publishers in the database."""
        return self._store(PUBLISHERS, publishers)
    
    @retry(stop=stop_after_attempt(RETRY_COUNT), wait=wait_fixed(RETRY_DELAY), reraise=True)
    def store_chatters(self, chatters: list[Chatter]):
        """Store a list of Chatters in the database."""
        if not chatters:
            return 0

        data = [chatter.model_dump() for chatter in chatters]
        columns = list(Chatter.model_fields.keys())
        if not columns:
            return 0

        copy_sql = sql.SQL("COPY chatters ({}) FROM STDIN").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
        )

        with self.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for item in data:
                    copy.write_row(tuple(item.get(column) for column in columns))

        count = len(data)
        log.debug("stored", extra={"source": CHATTERS, "num_items": count})
        return count
    
    def _update(self, table: str, items: list, columns: list[str] = None):
        if not items: return 0

        pk = _PRIMARY_KEYS[table]
        data = items # default assumption is that items are already dicts
        # convert BaseModel to dict if needed
        if isinstance(items[0], BaseModel):
            if columns: data = [bean.model_dump(include=set(columns) | ({pk} if pk else {})) for bean in items]
            else: data = [bean.model_dump() for bean in items]
        
        setters = sql.SQL(', ').join(
            sql.Composed([
                sql.Identifier(col),
                sql.SQL(" = "),
                sql.Placeholder(col)
            ]) for col in data[0].keys() if col != pk
        )
        SQL_UPDATE = sql.SQL("UPDATE {table} SET {setters} WHERE {pk_field} = {pk_placeholder};").format(
            table=sql.Identifier(table),
            setters=setters,
            pk_field=sql.Identifier(pk),
            pk_placeholder=sql.Placeholder(pk)
        )
        with self.cursor() as cur:
            cur.executemany(SQL_UPDATE, data)
            count = cur.rowcount
        return count
    
    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        """Partially update a list of Beans in the database."""
        if not beans: return 0
        for bean in beans:
            bean.embedding = Vector(bean.embedding)
        return self._update(BEANS, beans, columns)
    
    # def update_embeddings(self, beans: list[Bean]):
    #     """Update embeddings for a list of Beans and the computed categories + sentiments during the process."""
    #     if not beans: return 0
    #     # data = distinct(beans, URL)
    #     urls = [bean.url for bean in beans]
    #     embeddings = [Vector(bean.embedding) for bean in beans]
    #     SQL_UPDATE = """
    #     UPDATE beans AS b
    #     SET
    #         embedding = d.embedding::vector,
    #         categories = (
    #             SELECT ARRAY(
    #                 SELECT category
    #                 FROM fixed_categories fc
    #                 ORDER BY d.embedding <=> fc.embedding LIMIT 2
    #             )
    #         ),
    #         sentiments = (
    #             SELECT ARRAY(
    #                 SELECT sentiment
    #                 FROM fixed_sentiments fs
    #                 ORDER BY d.embedding <=> fs.embedding LIMIT 2
    #             )
    #         )
    #     FROM (
    #         SELECT 
    #             u.url,
    #             u.embedding
    #         FROM unnest(%s::varchar[], %s) AS u(url, embedding)
    #     ) AS d
    #     WHERE b.url = d.url;
    #     """
    #     count = self.execute(SQL_UPDATE, (urls, embeddings)).rowcount
    #     # NOTE: system gets overwhelmed
    #     # self._cluster_unmapped_beans(urls)  # re-cluster only the updated beans
    #     return count
    
    def update_publishers(self, publishers: list[Publisher]):
        """Store a list of Publishers in the database."""
        if not publishers: return 0
        return self._update(PUBLISHERS, publishers, columns=None)    

    @retry(stop=stop_after_attempt(RETRY_COUNT), wait=wait_fixed(RETRY_DELAY), reraise=True)
    def _query_composites(self, expr: str, params: dict = None) -> list[Any]:
        with self.pool.connection() as conn:
            with conn.execute(expr, params=params, binary=True) as cur:
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
                items = [dict(zip(cols, row)) for row in rows]
        return items    

    @retry(stop=stop_after_attempt(RETRY_COUNT), wait=wait_fixed(RETRY_DELAY), reraise=True)
    def _query_scalars(self, expr: str, params: dict = None) -> list:
        with self.pool.connection() as conn:
            with conn.execute(expr, params=params, binary=True) as cur: 
                rows = cur.fetchall()         
                items = [row[0] for row in rows]
        return items
    
    @retry(stop=stop_after_attempt(RETRY_COUNT), wait=wait_fixed(RETRY_DELAY), reraise=True)
    def _query_one(self, expr: str, params: dict = None):
        with self.pool.connection() as conn:
            with conn.execute(expr, params=params, binary=True) as cur: 
                result = cur.fetchone()         
        return result[0]

    def _fetch_all(self, 
        table: str,
        urls: list[str] = None,
        kind: str = None, 
        created: DATETIME = None, collected: DATETIME = None, updated: DATETIME = None,
        categories: list[str] = None, 
        regions: list[str] = None, 
        entities: list[str] = None, 
        tags: list[str] = None,
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        order: str = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ):        
        fields_expr = ", ".join(columns) if columns else "*"
        # Build base WHERE conditions (without embedding distance)
        where_expr, where_params = _where( 
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
            conditions=conditions
        )
        params = where_params
        
        # Use CTE when embedding is provided
        if embedding:
            expr = f"""
            WITH vector_distances AS (
                SELECT *, (embedding <=> %(embedding)s::vector) AS distance
                FROM {table}
                {where_expr}
            )
            SELECT {fields_expr}
            FROM vector_distances
            WHERE distance <= %(distance)s """
            
            params['embedding'] = embedding
            params['distance'] = distance
        else:
            # No embedding, use regular query
            expr = f"SELECT {fields_expr} FROM {table} {where_expr} "
        
        # Add ORDER BY
        if embedding: order = f"{ORDER_BY_DISTANCE}, {order}" if order else ORDER_BY_DISTANCE
        if order: expr += f" ORDER BY {order} "
        
        # Add LIMIT/OFFSET
        if limit or offset:
            limit_expr, limit_params = _limit(limit=limit, offset=offset)
            expr += limit_expr
            params.update(limit_params)
        
        items = self._query_composites(expr, params)
        if table in _TYPES: items = [_TYPES[table](**item) for item in items]
        log.debug("queried", extra={"source": table, "num_items": len(items)})
        return items
    
    def query_latest_beans(self,
        kind: str = None, 
        created: DATETIME = None, collected: DATETIME = None,
        categories: list[str] = None, 
        regions: list[str] = None, 
        entities: list[str] = None, 
        tags: list[str] = None,
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
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
            columns=columns
        )
    
    def query_trending_beans(self,
        kind: str = None, 
        updated: DATETIME = None, collected: DATETIME = None,
        categories: list[str] = None, 
        regions: list[str] = None,
        entities: list[str] = None, 
        tags: list[str] = None,
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
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
            limit=limit,
            offset=offset,
            columns=columns
        )
    
    def query_aggregated_beans(self,
        kind: str = None, 
        created: DATETIME = None, 
        collected: DATETIME = None,
        updated: DATETIME = None,
        categories: list[str] = None, 
        regions: list[str] = None, 
        entities: list[str] = None, 
        tags: list[str] = None,
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[AggregatedBean]:
        return self._fetch_all(
            table="aggregated_beans_view",
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
            columns=columns
        )

    def query_aggregated_chatters(self, urls: list[str] = None, updated: DATETIME = None, limit: int = 0, offset: int = 0, columns: list[str] = None) -> list[AggregatedBean]:        
        return self._fetch_all(
            table="_materialized_chatter_aggregates",
            urls=urls,
            updated=updated,            
            order=ORDER_BY_TRENDING,
            limit=limit,
            offset=offset,
            columns=columns
        )

    def query_publishers(self, 
        collected: DATETIME = None, 
        tags: list[str] = None,
        sources: list[str] = None, 
        conditions: list[str] = None, 
        limit: int = 0, 
        offset: int = 0, 
        columns: list[str] = None
    ) -> list[Publisher]:
        return self._fetch_all(
            table=PUBLISHERS,
            collected=collected,
            tags=tags,
            sources=sources,
            conditions=conditions,
            limit=limit,
            offset=offset,
            columns=columns
        )
    
    def query_chatters(self, collected: DATETIME = None, sources: list[str] = None, conditions: list[str] = None, limit: int = 0, offset: int = 0, columns: list[str] = None) -> list[Chatter]:
        return self._fetch_all(
            table=CHATTERS,
            collected=collected,
            sources=sources,
            conditions=conditions,
            limit=limit,
            offset=offset,
            columns=columns
        )
    
    def distinct_categories(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = "SELECT category FROM fixed_categories ORDER BY category "
        limit_expr, limit_params = _limit(limit=limit, offset=offset)
        expr += limit_expr
        return self._query_scalars(expr, limit_params)
    
    def distinct_sentiments(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = "SELECT sentiment FROM fixed_sentiments ORDER BY sentiment "
        limit_expr, limit_params = _limit(limit=limit, offset=offset)
        expr += limit_expr
        return self._query_scalars(expr, limit_params)
    
    def distinct_entities(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = "SELECT DISTINCT unnest(entities) as entity FROM beans WHERE entities IS NOT NULL ORDER BY entity "
        limit_expr, limit_params = _limit(limit=limit, offset=offset)
        expr += limit_expr
        return self._query_scalars(expr, limit_params)
    
    def distinct_regions(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = "SELECT DISTINCT unnest(regions) as region FROM beans WHERE regions IS NOT NULL ORDER BY region "
        limit_expr, limit_params = _limit(limit=limit, offset=offset)
        expr += limit_expr
        return self._query_scalars(expr, limit_params)
    
    def distinct_publishers(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = "SELECT source FROM publishers ORDER BY source "
        limit_expr, limit_params = _limit(limit=limit, offset=offset)
        expr += limit_expr
        return self._query_scalars(expr, limit_params)

    def count_rows(self, table: str, conditions: list[str] = None) -> int:
        expr = f"SELECT count(*) FROM {table} "
        where_exprs, _ = _where(conditions=conditions)
        if where_exprs: expr += where_exprs        
        return self._query_one(expr)
    
    # MAINTENANCE METHODS
    def execute(self, sql: str, params = None):
        """Execute arbitrary SQL commands."""
        with self.pool.connection() as conn:
            if params: return conn.execute(sql, params=params, binary=True)
            return conn.execute(sql)

    # def refresh_classifications(self):  
    #     SQL_UPDATE_CLASSIFICATIONS = """
    #     WITH pack AS (
    #         SELECT 
    #             b.url,
    #             ARRAY(
    #                 SELECT category FROM fixed_categories fc
    #                 ORDER BY b.embedding <=> fc.embedding LIMIT 2
    #             )  AS categories,
    #             ARRAY(
    #                 SELECT sentiment FROM fixed_sentiments fs
    #                 ORDER BY b.embedding <=> fs.embedding LIMIT 2
    #             )  AS sentiments
    #         FROM beans b
    #         WHERE b.embedding is NOT NULL AND b.categories IS NULL
    #     )
    #     UPDATE beans b
    #     SET 
    #         categories = pack.categories,
    #         sentiments = pack.sentiments
    #     FROM pack
    #     WHERE b.url = pack.url;
    #     """      
    #     self.execute(SQL_UPDATE_CLASSIFICATIONS)

    # def _cluster_unmapped_beans(self, unmapped_urls):
    #     SQL_INSERT_CLUSTERS = """
    #     INSERT INTO _internal_clusters (url, related)
    #     WITH input_embeddings AS (
    #         SELECT url, categories, embedding
    #         FROM beans
    #         WHERE url = %(input_url)s
    #     ),
    #     similar_beans AS (
    #         SELECT DISTINCT ie.url, b.url AS related
    #         FROM input_embeddings ie
    #         CROSS JOIN beans b
    #         WHERE ie.url <> b.url
    #             AND ie.categories = b.categories
    #             AND b.embedding IS NOT NULL
    #             AND (ie.embedding <-> b.embedding) <= %(cluster_eps)s
    #     )
    #     SELECT url, related FROM similar_beans
    #     UNION
    #     SELECT related, url FROM similar_beans
    #     ON CONFLICT (url, related) DO NOTHING;
    #     """
    #     with self.cursor() as cur:
    #         cur.executemany(SQL_INSERT_CLUSTERS, [{"input_url": url, "cluster_eps": CLUSTER_EPS} for url in unmapped_urls])
    #         return cur.rowcount

    # def refresh_clusters(self):        
    #     SQL_QUERY_UNMAPPED = """
    #     SELECT url FROM beans b
    #     WHERE b.embedding IS NOT NULL
    #     AND NOT EXISTS (
    #         SELECT 1 FROM _internal_clusters ic WHERE ic.url = b.url
    #     );
    #     """
    #     self._cluster_unmapped_beans(self._query_scalars(SQL_QUERY_UNMAPPED))  
    #     self.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY _materialized_cluster_aggregates;")

    # def refresh_chatters(self):
    #     self.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY _materialized_chatter_aggregates;")
        
    def optimize(self):
        self.execute(f"""
        DELETE FROM beans 
        WHERE collected < CURRENT_DATE - INTERVAL '{BEANSACK_CLEANUP_WINDOW}';        
        
        DELETE FROM chatters 
        WHERE collected < CURRENT_DATE - INTERVAL '{BEANSACK_CLEANUP_WINDOW}';
        """)        
        self.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY trend_aggregates;")
        # NOTE: ideally this should be before the refresh but the current deletion is a hit or miss
        self.execute("""
        DELETE FROM related_beans rb 
        WHERE NOT EXISTS (
            SELECT 1 FROM beans WHERE url = rb.url
        );
        """)
    
    def close(self):        
        self.pool.close()
        
def create_db(conn_str: str) -> PGSack:
    """Create the new tables, views, indexes etc."""
    with open(os.path.join(os.path.dirname(__file__), 'pgsack.sql'), 'r') as sql_file:
        init_sql = sql_file.read()      

    pool = ConnectionPool(conn_str, timeout=PG_TIMEOUT)
    pool.open()
    with pool.connection() as conn:
        conn.execute(init_sql)
    return PGSack(pool)

def _store_parquet(db, file_path: Path, table_name: str, override: bool = False):
    """Load a parquet file into a database table, converting embedding columns to lists."""
    df = pd.read_parquet(file_path)    
    # Convert embedding column to list if it exists
    if EMBEDDING in df.columns: 
        df[EMBEDDING] = df[EMBEDDING].apply(lambda x: x.tolist() if hasattr(x, 'tolist') else x)
    return db._store(table_name, df.to_dict('records'), override=override)

def _where(
    urls: list[str] = None,
    kind: str = None, 
    created: DATETIME = None, collected: DATETIME = None, updated: DATETIME = None,
    categories: list[str] = None, regions: list[str] = None, entities: list[str] = None, tags: list[str] = None,
    sources: list[str] = None, 
    conditions: list[str] = None,
):    
    exprs = []
    params = {}
    if urls: 
        exprs.append("url = ANY(%(urls)s)")
        params['urls'] = urls
    if kind: 
        exprs.append("kind = %(kind)s")
        params['kind'] = kind
    if created:
        _from, _to = split_from_to(created)
        if _from: 
            exprs.append("created >= %(created_from)s")
            params['created_from'] = _from
        if _to: 
            exprs.append("created <= %(created_to)s")
            params['created_to'] = _to
    if collected: 
        _from, _to = split_from_to(collected)
        if _from: 
            exprs.append("collected >= %(collected_from)s")
            params['collected_from'] = _from
        if _to: 
            exprs.append("collected <= %(collected_to)s")
            params['collected_to'] = _to
    if updated: 
        _from, _to = split_from_to(updated)
        if _from: 
            exprs.append("updated >= %(updated_from)s")
            params['updated_from'] = _from
        if _to: 
            exprs.append("updated <= %(updated_to)s")
            params['updated_to'] = _to
    # array overlap operator: &&
    if categories: 
        exprs.append("categories && %(categories)s::varchar[]")
        params['categories'] = categories
    if regions: 
        exprs.append("regions && %(regions)s::varchar[]")
        params['regions'] = regions
    if entities: 
        exprs.append("entities && %(entities)s::varchar[]")
        params['entities'] = entities
    if tags:
        exprs.append("tags @@ plainto_tsquery('simple', %(tags)s)")
        params['tags'] = " ".join(tags)
    if sources: 
        exprs.append("source = ANY(%(sources)s)")
        params['sources'] = sources
    # Note: embedding distance filtering is handled in _fetch_all() via CTE
    if conditions: exprs.extend(conditions)
    if exprs: return ("WHERE " + " AND ".join(exprs), {k: v for k, v in params.items() if v})
    else: return ("", {})

def _limit(limit: int = 0, offset: int = 0) -> tuple[str, dict]:
    expr, params = "", {}
    if limit: 
        expr += "LIMIT %(limit)s "
        params['limit'] = limit
    if offset: 
        expr += "OFFSET %(offset)s "
        params['offset'] = offset
    return expr, params
        
def split_from_to(dt: DATETIME) -> tuple[datetime, datetime|None]:
    if isinstance(dt, datetime): return dt, None
    return dt[0], dt[1]
