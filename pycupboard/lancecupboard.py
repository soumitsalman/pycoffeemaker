# NOTE: this is deprecated. IGNORE

from datetime import datetime
from deprecation import deprecated
import lancedb
from lancedb.pydantic import LanceModel
from pydantic import BaseModel, Field
from typing import Optional
from .models import Sip
SIPS = "sips"


# sip kinds
HEADLINE = "headline"
REPORT = "report"
EDITORIAL = "editorial"
NEWSLETTER = "newsletter"
SECTION = "section"

@deprecated
class _Sip(Sip, LanceModel):
    """Generated article stored in cupboard"""
    # ID must have
    id: str = Field(description="The unique identifier of the item.")
    
    # Main body fields
    kind: Optional[str] = Field(default=None, description="Kind of sip, e.g., headline, report, editorial, newsletter, post.")
    title: Optional[str] = Field(default=None, description="Title of the sip.")    
    content: Optional[str] = Field(default=None, description="Content of the sip.")
    summary: Optional[str] = Field(default=None, description="Summary of the sip.")
    tags: Optional[list[str]] = Field(default=None, description="List of tags associated with the sip.")
    image_url: Optional[str] = Field(default=None, description="URL of the image associated with the sip.")
    
    # retrieval and linking fields
    beans: Optional[list[str]] = Field(default=None, description="List of beans that were used to generate this sip.")
    embedding: Optional[list[float]] = Field(default=None, description="Vector Embedding of the sip.")

    # timestamps
    created: Optional[datetime] = Field(default=None, description="The creation timestamp.")
    updated: Optional[datetime] = Field(default=None, description="The last updated timestamp.")

    def __str__(self):
        return f"# {self.title}\n{self.content}"

    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False
        exclude_none = True
        exclude_unset = True
        by_alias=True
        json_encoders={datetime: rfc3339}
 

@deprecated
class LanceDBCupboard: 
    db: lancedb.DBConnection
    tables: dict[str, lancedb.Table]

    def __init__(self, storage_path: str):
        self.db = _connect(storage_path)
        self.tables = {
            SIPS: self.db.create_table(SIPS, schema=_Sip, exist_ok=True)
        }

    # INGESTION functions   
    def store_sips(self, sips: list[Sip]):
        if not sips: return 0

        result = self.tables[SIPS].merge_insert("id") \
            .when_not_matched_insert_all() \
            .execute([_Sip(**sip.model_dump(exclude_none=True)) for sip in sips])
        return result.num_inserted_rows

    def count_rows(self, table=SIPS, conditions: list[str] = None) -> int:
        where_exprs = _where(conditions=conditions)
        return self.tables[table].count_rows(where_exprs)

    def _query_items(self,
        table: str,
        created: datetime = None, updated: datetime = None,
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        order = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ):
        query = self.tables[table].search() if not embedding else self.tables[table].search(query=embedding, query_type="vector", vector_column_name=K_EMBEDDING)      
        where_expr = _where(created=created, updated=updated, conditions=conditions)
        if where_expr: query = query.where(where_expr)
        if embedding: query = query.distance_type("cosine")
        if distance: query = query.distance_range(upper_bound = distance)
        if order and embedding: query = query.rerank(order, query_string="default")
        if limit: query = query.limit(limit)
        if offset: query = query.offset(offset)
        if columns: query = query.select(columns+(["_distance"] if embedding else []))
        return query.to_pydantic(_Sip)
    
    def _query_with_multiple_vectors(self,
        table: str,
        created: datetime = None, updated: datetime = None,
        vectors: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        order = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Sip]:
        query = self.tables[table].search() if not vectors else self.tables[table].search(query=vectors, query_type="vector", vector_column_name=K_EMBEDDING)      
        where_expr = _where(created=created, updated=updated, conditions=conditions)
        if where_expr: query = query.where(where_expr)
        if vectors: query = query.distance_type("cosine")
        if distance: query = query.distance_range(upper_bound = distance)
        if order and vectors: query = query.rerank(order, query_string="default")
        if limit: query = query.limit(limit)
        if offset: query = query.offset(offset)
        if columns: query = query.select(columns+(["_distance"] if vectors else []))
        
        # Get all rows and deduplicate by 'id'
        df = query.to_pandas().drop_duplicates(subset=['id'], keep='first')
        return [_Sip(**sip) for sip in df.to_dict('records')]
    
    def query_sips(self,
        created: datetime = None, updated: datetime = None,
        embedding: list[float]|list[list[float]] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Sip]:
        if embedding and isinstance(embedding[0], list):
            return self._query_with_multiple_vectors(
                table=SIPS,
                created=created,
                updated=updated,
                vectors=embedding,
                distance=distance,
                conditions=conditions,
                limit=limit,
                offset=offset,
                columns=columns
            )
        return self._query_items(
            table=SIPS,
            created=created,
            updated=updated,
            embedding=embedding,
            distance=distance,
            conditions=conditions,
            limit=limit,
            offset=offset,
            columns=columns
        )
    
    def _remove_from(self, table: str, created: datetime = None, conditions: list[str] = None) -> int:       
        where_expr = _where(created=created, conditions=conditions)
        current_total = self.tables[table].count_rows()
        self.tables[table].delete(where_expr)
        return current_total - self.tables[table].count_rows()
    
    def remove_sips(self, created: datetime = None, conditions: list[str] = None) -> int:
        return self._remove_from(SIPS, created=created, conditions=conditions)
    
    def optimize(self):
        # NOTE: something wrong with the vector index creation
        # try: [self.tables[table].create_index(vector_column_name=K_EMBEDDING, index_type="IVF_PQ", metric="cosine") for table in [MUGS, SIPS]]
        # except: pass
        [table.optimize() for table in self.tables.values()]

    def close(self):
        del self.db
        del self.tables

list_expr = lambda items: ", ".join(f"'{item}'" for item in items)
date_expr = lambda date_val: f"date '{date_val.strftime('%Y-%m-%d')}'"

def _where(
    urls: list[str] = None,
    kind: str = None,
    created: datetime = None,
    collected: datetime = None,
    updated: datetime = None,
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