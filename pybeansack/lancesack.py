from cmath import log
import os
from pydantic import Field
import lancedb
from lancedb.rerankers import Reranker
from lancedb.pydantic import LanceModel, Vector
from datetime import timedelta
import pyarrow as pa
import pandas as pd
from .models import *
from .database import *
import logging

log = logging.getLogger(__name__)

VECTOR_TYPE = Vector(VECTOR_LEN, nullable=True)

_PRIMARY_KEYS = {
    BEANS: URL,
    PUBLISHERS: SOURCE
}

class _Bean(Bean, LanceModel):
    embedding: VECTOR_TYPE = Field()

class _Publisher(Publisher, LanceModel):
    pass

class _Chatter(Chatter, LanceModel):
    pass

# class _Sip(Sip, LanceModel):    
#     embedding: VECTOR_TYPE = Field(None, description="This is the embedding vector of title+content")

class _RelatedBean(LanceModel):
    url: str
    related_url: list[str]

class _ScalarReranker(Reranker):
    column: str
    direction: str

    def __init__(self, column: str, desc: bool = False):
        super().__init__("relevance")
        self.column = column
        self.direction = "descending" if desc else "ascending"       

    def _add_relevance_score(self, table: pa.Table):
        """Add _relevance_score column based on the scalar column value"""
        scores = table[self.column].to_pylist()
        # Normalize scores to 0-1 range (higher is better)
        min_score = min(scores) if scores else 0
        max_score = max(scores) if scores else 1
        range_score = max_score - min_score if max_score != min_score else 1
        normalized_scores = [(s - min_score) / range_score for s in scores]
        return table.append_column("_relevance_score", pa.array(normalized_scores, type=pa.float32()))

    def _rerank(self, table: pa.Table):
        table = self._add_relevance_score(table)
        return table.sort_by([("_relevance_score", "descending")])

    def rerank_hybrid(self, query: str, vector_results: pa.Table, fts_results: pa.Table):
        table = self._merge_and_keep_scores(vector_results, fts_results)
        return self._rerank(table)

    def rerank_vector(self, query: str, vector_results: pa.Table):
        return self._rerank(vector_results)

    def rerank_fts(self, query: str, fts_results: pa.Table):
        return self._rerank(fts_results)

ORDER_BY_LATEST = _ScalarReranker(column="created", desc=True)

class LanceSack(Beansack): 
    db: lancedb.DBConnection
    # tables: dict[str, lancedb.Table]
    # allbeans: lancedb.Table
    # allpublishers: lancedb.Table
    # allchatters: lancedb.Table
    # allclusters: lancedb.Table

    def __init__(self, storage_path: str):
        self.db = _connect(storage_path)
        self.db.create_table(BEANS, schema=_Bean, exist_ok=True)
        self.db.create_table(PUBLISHERS, schema=_Publisher, exist_ok=True)
        self.db.create_table(CHATTERS, schema=_Chatter, exist_ok=True)
        self.db.create_table(RELATED_BEANS, schema = _RelatedBean, exist_ok=True)
        # NOTE: no need to recreate indexes

    # INGESTION functions
    def store_beans(self, beans: list[Bean]):
        if not beans: return 0

        # to_store = prepare_beans_for_store(beans) 
        result = self.db[BEANS].merge_insert("url") \
            .when_not_matched_insert_all() \
            .execute([_Bean(**bean.model_dump(exclude_none=True)) for bean in beans])
        return result.num_inserted_rows
    
    def store_related(self, related_beans: list[dict[str, str]]):
        if not related_beans: return 0
        
        self.db[RELATED_BEANS].add(data=related_beans)
        return len(related_beans)
    
    def store_publishers(self, publishers: list[Publisher]):
        if not publishers: return 0

        # to_store = prepare_publishers_for_store(publishers)  
        result = self.db[PUBLISHERS].merge_insert(SOURCE) \
            .when_not_matched_insert_all() \
            .execute([_Publisher(**publisher.model_dump(exclude_none=True)) for publisher in publishers])
        return result.num_inserted_rows
    
    def store_chatters(self, chatters: list[Chatter]):
        if not chatters: return 0

        # to_store = prepare_chatters_for_store(chatters)
        self.db[CHATTERS].add([_Chatter(**chatter.model_dump(exclude_none=True)) for chatter in chatters])
        return len(chatters)
    
    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        if not beans: return 0

        if columns:
            fields = list(set(columns + [URL]))
            updates = [bean.model_dump(include=fields) for bean in beans]
        else:
            updates = [bean.model_dump(exclude_none=True) for bean in beans]
            fields = non_null_fields(updates)

        get_field_values = lambda field: [update.get(field) for update in updates]
        result = self.db[BEANS].merge_insert("url") \
            .when_matched_update_all() \
            .execute(
                pa.table(
                    data={ field: get_field_values(field) for field in fields },
                    schema=pa.schema(list(map(self.db[BEANS].schema.field, fields)))
                )
            )
        return result.num_updated_rows
    
    # this assuming that the embedding field is already set
    # this is a specialized update that also updates categories, sentiments and clusters
    def update_embeddings(self, beans: list[Bean]):
        if not beans: return 0

        urls = [bean.url for bean in beans]
        vecs = [bean.embedding for bean in beans]

        # inserting along with classification
        categories = self.fixed_categories.search(query=vecs, query_type="vector", vector_column_name=EMBEDDING).distance_type("cosine").limit(2).select(["category", "_distance"]).to_pandas()
        sentiments = self.fixed_sentiments.search(query=vecs, query_type="vector", vector_column_name=EMBEDDING).distance_type("cosine").limit(2).select(["sentiment", "_distance"]).to_pandas()
        updates = {
            URL: urls,
            EMBEDDING: vecs,
            CATEGORIES: categories.groupby('query_index')['category'].apply(list).sort_index().tolist(),
            SENTIMENTS: sentiments.groupby('query_index')['sentiment'].apply(list).sort_index().tolist()
        } 
        result = self.db[BEANS].merge_insert("url") \
            .when_matched_update_all() \
            .execute(
                pa.table(
                    data=updates,
                    schema=pa.schema(list(map(self.db[BEANS].schema.field, [URL, EMBEDDING, CATEGORIES, SENTIMENTS])))
                )
            )

        # compute clusters with existing items
        clusters = self.db[BEANS].search(query=vecs, query_type="vector", vector_column_name=EMBEDDING).distance_type("l2").distance_range(upper_bound=CLUSTER_EPS).select(["url", "_distance"]).to_pandas()
        self.db[RELATED_BEANS].add([
            _RelatedBean(url=url, related=related) for url, related
                in zip(urls, clusters.groupby('query_index')['url'].apply(list).sort_index().tolist())
        ])   

        return result.num_updated_rows

    def update_publishers(self, publishers: list[Publisher]):
        if not publishers: return 0

        updates = [publisher.model_dump(exclude_none=True, exclude=[BASE_URL]) for publisher in publishers]
        fields = non_null_fields(updates)

        get_field_values = lambda field: [update.get(field) for update in updates]
        result = self.db[PUBLISHERS].merge_insert(SOURCE) \
            .when_matched_update_all() \
            .execute(
                pa.table(
                    data={field: get_field_values(field) for field in fields},
                    schema=pa.schema(list(map(self.db[PUBLISHERS].schema.field, fields)))
                )
            )
        return result.num_updated_rows

    # QUERY functions
    def deduplicate(self, table: str, items: list) -> list:
        if not items: return items    
        idkey = _PRIMARY_KEYS[table]    
        ids = [getattr(item, idkey) for item in items]
        existing_ids = self.tables[table].search().where(f"{idkey} IN ({list_expr(ids)})").select([idkey]).to_list()
        existing_ids = [item[idkey] for item in existing_ids]
        return list(filter(lambda item: getattr(item, idkey) not in existing_ids, items))

    def count_rows(self, table, conditions: list[str] = None) -> int:
        where_exprs = _where(conditions=conditions)
        return self.tables[table].count_rows(where_exprs)

    def _query_beans(self,
        kind: str = None, 
        created: DATETIME = None, collected: DATETIME = None, updated: DATETIME = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        tags: list[str] = None,
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        order = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Bean]:
        query = self.db[BEANS].search() if not embedding else self.db[BEANS].search(query=embedding, query_type="vector", vector_column_name=EMBEDDING)      
        where_expr = _where(urls=None, kind=kind, created=created, collected=collected, updated=updated, categories=categories, regions=regions, entities=entities, tags=tags, sources=sources, conditions=conditions)
        if where_expr: query = query.where(where_expr)
        if embedding: query = query.distance_type("cosine")
        if distance: query = query.distance_range(upper_bound = distance)
        if order and embedding: query = query.rerank(order, query_string="default")
        if limit: query = query.limit(limit)
        if offset: query = query.offset(offset)
        if columns: query = query.select(columns+(["_distance"] if embedding else []))
        return query.to_pydantic(_Bean)
    
    def query_latest_beans(self,
        kind: str = None, 
        created: DATETIME = None, 
        collected: DATETIME = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        tags: list[str] = None,
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Bean]:
        return self._query_beans(
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
        updated: DATETIME = None, 
        collected: DATETIME = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        tags: list[str] = None,
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[TrendingBean]:
        raise NOT_SUPPORTED
    
    def query_aggregated_beans(self,
        kind: str = None, 
        created: DATETIME = None, 
        collected: DATETIME = None,
        updated: DATETIME = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        tags: list[str] = None,
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0,
        columns: list[str] = None
    ) -> list[AggregatedBean]:
        beans = self._query_beans(
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
        publishers = self.db[PUBLISHERS].search().where(_where(sources=[bean.source for bean in beans])).to_pydantic(_Publisher)
        clusters = self.db[RELATED_BEANS].search().where(_where(urls=[bean.url for bean in beans])).to_pydantic(_RelatedBean)
        get_publisher = lambda source: next((pub.model_dump(exclude_none=True, exclude=[SOURCE]) for pub in publishers if pub.source == source), {})
        get_cluster = lambda url: next(({RELATED: cluster.related, CLUSTER_SIZE: len(cluster.related)} for cluster in clusters if cluster.url == url), {})
        beans = [AggregatedBean(**bean.model_dump(exclude_none=True), **get_publisher(bean.source), **get_cluster(bean.url)) for bean in beans]
        # TODO: add cluster_id -- related with the highest cluster_size
        # TODO: add aggregated chatter stats when ready
        # Additional aggregation logic can be added here
        return beans

    def query_aggregated_chatters(self, urls: list[str] = None, updated: DATETIME = None, limit: int = 0, offset: int = 0, columns: list[str] = None) -> list[AggregatedBean]:      
        raise NOT_IMPLEMENTED
    
    def query_chatters(self, collected: DATETIME = None, sources: list[str] = None, conditions: list[str] = None, limit: int = 0, offset: int = 0, columns: list[str] = None) -> list[Chatter]:  
        query = self.db[CHATTERS].search()
        if conditions: query = query.where(_where(collected=collected, sources=sources, conditions=conditions))
        if limit: query = query.limit(limit)
        if offset: query = query.offset(offset)
        if columns: query = query.select(columns)
        return query.to_pydantic(_Chatter)
    
    def query_publishers(self, collected: DATETIME = None, tags: list[str] = None, sources: list[str] = None, conditions: list[str] = None, limit: int = 0, offset: int = 0, columns: list[str] = None) -> list[Publisher]:  
        query = self.db[PUBLISHERS].search()
        if conditions: query = query.where(_where(collected=collected, sources=sources, conditions=conditions))
        if limit: query = query.limit(limit)
        if offset: query = query.offset(offset)
        if columns: query = query.select(columns)
        return query.to_pydantic(_Publisher)
    
    def distinct_categories(self, limit: int = 0, offset: int = 0) -> list[str]:
        raise NOT_IMPLEMENTED
    
    def distinct_sentiments(self, limit: int = 0, offset: int = 0) -> list[str]:
        raise NOT_IMPLEMENTED
    
    def distinct_entities(self, limit: int = 0, offset: int = 0) -> list[str]:
        raise NOT_IMPLEMENTED
    
    def distinct_regions(self, limit: int = 0, offset: int = 0) -> list[str]:
        raise NOT_IMPLEMENTED
    
    def distinct_publishers(self, limit: int = 0, offset: int = 0) -> list[str]:
        raise NOT_IMPLEMENTED
    
    # MAINTENANCE functions
    # def refresh_classifications(self):
    #     raise NOT_SUPPORTED

    # def refresh_clusters(self):
    #     raise NOT_SUPPORTED
    
    # def refresh_chatters(self):
    #     raise NOT_SUPPORTED

    def optimize(self):
        try: self.db[BEANS].create_index(vector_column_name=EMBEDDING, index_type="IVF_RQ", metric="cosine")
        except: pass
        [self.db[table].optimize() for table in self.db.table_names()]

    def close(self):
        del self.db


def create_db(storage_path: str):
    db = _connect(storage_path)
    beans = db.create_table(BEANS, schema=_Bean, exist_ok=True)
    publishers = db.create_table(PUBLISHERS, schema=_Publisher,  exist_ok=True)
    chatters = db.create_table(CHATTERS, schema=_Chatter, exist_ok=True)
    related_beans = db.create_table(RELATED_BEANS, schema = _RelatedBean, exist_ok=True)

    beans.create_scalar_index(URL, index_type="BTREE")
    beans.create_scalar_index(KIND, index_type="BITMAP")
    beans.create_scalar_index(CREATED, index_type="BTREE")    
    beans.create_scalar_index(CATEGORIES, index_type="LABEL_LIST")
    beans.create_scalar_index(REGIONS, index_type="LABEL_LIST")
    beans.create_scalar_index(ENTITIES, index_type="LABEL_LIST")
    publishers.create_scalar_index(SOURCE, index_type="BTREE")
    chatters.create_scalar_index(URL, index_type="BTREE")
    related_beans.create_scalar_index(URL, index_type="BTREE")

    return LanceSack(storage_path)

def _connect(storage_path: str):
    storage_options = None
    if storage_path.startswith("s3://"):
        storage_options = {
            "access_key_id": os.getenv("S3_ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("S3_SECRET_ACCESS_KEY"),
            "endpoint": os.getenv("S3_ENDPOINT"),
            "region": os.getenv("S3_REGION"),
            "timeout": "60s"
        }
    return lancedb.connect(
        uri=storage_path, 
        read_consistency_interval = timedelta(hours=1),
        storage_options=storage_options
    )

list_expr = lambda items: ", ".join(f"'{item}'" for item in items)
date_expr = lambda date_val: f"date '{date_val.strftime('%Y-%m-%d')}'"

def _where(
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
    conditions: list[str] = None
):
    exprs = []
    if urls: exprs.append(f"url IN ({list_expr(urls)})")
    if kind: exprs.append(f"kind = '{kind}'")
    if created: exprs.append(f"created >= {date_expr(created)}")
    if collected: exprs.append(f"collected >= {date_expr(collected)}")
    if updated: raise NOT_SUPPORTED
    if categories: exprs.append(f"ARRAY_HAS_ANY(categories, [{list_expr(categories)}])")
    if regions: exprs.append(f"ARRAY_HAS_ANY(regions, [{list_expr(regions)}])")
    if entities: exprs.append(f"ARRAY_HAS_ANY(entities, [{list_expr(entities)}])")
    if tags: exprs.append(f"ARRAY_HAS_ANY(tags, [{list_expr(tags)}])")
    if sources: exprs.append(f"source IN ({list_expr(sources)})")
    if conditions: exprs.extend([c for c in conditions if c])

    if exprs: return " AND ".join(exprs)