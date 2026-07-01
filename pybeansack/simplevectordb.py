
from datetime import datetime, timezone

import lancedb
from lancedb.pydantic import LanceModel, Vector
import pyarrow as pa
from typing import Any, Literal, Optional
from utils.config import VECTOR_LEN

ITEMS = "__items__"
ID = "id"
TS = "ts"
INDEXING_THRESHOLD = 100000
DISTANCE_FUNC = Literal["l2", "cosine", "dot"]

class SimpleVectorDB:
    db: lancedb.DBConnection
    id_keys: dict[str, str]

    def __init__(self, db_path: str, table_id_keys: dict[str, str], **additional_tables):
        """Opens connection to existing or new simple database on `db_path`.
        Creates tables specified in `table_id_keys` if they don't exist. Each table will have an id and embedding columns.          
        Additional static tables can be created (if they don't exist) by passing them as keyword arguments.
        
        Parameters:
            db_path: path to the database
            table_id_keys: dict where the key is the table name and the value is the id key for the table. The id key is used for upsert operations.
            additional_tables: Names and data for additional static tables. Data should be dict[str, pd.DataFrame|list[dict[str, list[float]]]]. Once created, subsequent calls do not need to pass the same static tables.
        """
        self.db = lancedb.connect(db_path)
        self.id_keys = table_id_keys.copy()

        for table_name, id_key in self.id_keys.items():
            tab = self.db.create_table(
                table_name, 
                schema=pa.schema([
                    (id_key, pa.string()), 
                    ("embedding", pa.list_(pa.float32(), VECTOR_LEN)),
                    (TS, pa.timestamp("s", tz="UTC"))
                ]), 
                exist_ok=True
            )
            try: tab.create_scalar_index(id_key, replace=False)
            except: pass
        # setup additonal tables
        for table_name, table_data in additional_tables.items():
            self.db.create_table(table_name, data=table_data, exist_ok=True)
        
    @classmethod
    def create_db(cls, db_path: str, table_id_keys: dict[str, str], **additional_tables):
        """Classifications should be dict[str, pd.DataFrame|list[dict[str, list[float]]]] where the dataframe has columns "category" or "sentiment" and "embedding" """
        db = lancedb.connect(db_path)
        # setup main tables
        for table, id_key in table_id_keys.items():
            db.create_table(
                table, 
                schema=pa.schema([
                    (id_key, pa.string()), 
                    ("embedding", pa.list_(pa.float32(), VECTOR_LEN)),
                    (TS, pa.timestamp("s", tz="UTC"))
                ]), 
                mode="overwrite"
            ).create_scalar_index(id_key, replace=True)
        # setup additonal tables
        for cls_name, cls_values in additional_tables.items():
            db.create_table(cls_name, data=cls_values, mode="overwrite")
        return cls(db_path, table_id_keys=table_id_keys)

    def store(self, table: str, items: list[dict[str, Any]]):
        if not items: return 0

        id_key = self.id_keys[table]
        result = self.db[table].merge_insert(id_key) \
            .when_not_matched_insert_all() \
                .execute(_prepare_to_store(items, id_key))
        return result.num_inserted_rows
        
    def search(self, table: str, embedding: list[float], distance_func: DISTANCE_FUNC = "l2", distance: Optional[float] = None, limit: Optional[int] = None, columns: list[str] = None):
        query = self.db[table].search(embedding, vector_column_name="embedding", query_type="vector").distance_type(distance_func)  
        if distance: query = query.distance_range(upper_bound = distance)
        if limit: query = query.limit(limit)
        if columns: query = query.select(columns+["_distance"])
        return query.to_list()
    
    def optimize(self, **kwargs):
        for tbl in self.db.table_names():
            if self.db[tbl].count_rows() > INDEXING_THRESHOLD:
                # self.db[tbl].create_index(metric="l2", vector_column_name="embedding", index_type="IVF_RQ", replace=True)
                self.db[tbl].optimize(**kwargs)

    def close(self):
        self.optimize()
        del self.db

def _prepare_to_store(items: list[dict[str, Any]], id_key: str):
    """attaches timestamps"""
    ts = int(datetime.now(tz=timezone.utc).timestamp())
    for item in items:
        item[TS] = ts        
    return items
