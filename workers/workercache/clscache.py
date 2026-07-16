from datetime import datetime, timedelta, timezone
from itertools import batched, chain
import os
from typing import Any, Optional
from pathlib import Path
from utils import generate_uuid, VECTOR_LEN
from uuid import UUID
import zvec
from icecream import ic

ID = "id"
TS = "ts"
EMBEDDING = "embedding"
METRIC_MAP = {"l2": zvec.MetricType.L2, "cosine": zvec.MetricType.COSINE}
DEFAULT_TOPN = 10

class ClassificationCache:
    def __init__(self, db_path: str, table_settings: dict[str, dict[str, Any]]):
        self.db_path = db_path
        self.table_settings = table_settings

        Path(self.db_path).mkdir(parents=True, exist_ok=True)
        zvec.init(query_threads=os.cpu_count(), optimize_threads=os.cpu_count())
        self.collections: dict[str, zvec.Collection] = {}
        for tab, setting in table_settings.items():            
            path = os.path.join(self.db_path, tab)
            if os.path.exists(path): 
                coll = zvec.open(path)
            else:
                schema = zvec.CollectionSchema(
                    name=tab,
                    vectors=zvec.VectorSchema(
                        EMBEDDING, 
                        data_type=zvec.DataType.VECTOR_FP32, 
                        dimension=VECTOR_LEN,
                        index_param=zvec.HnswIndexParam(METRIC_MAP.get(setting.get("distance_func"), zvec.MetricType.L2))
                    ),
                    fields=[
                        zvec.FieldSchema(ID, zvec.DataType.STRING),
                        zvec.FieldSchema(TS, zvec.DataType.FLOAT)
                    ],
                )                
                coll = zvec.create_and_open(path=path, schema=schema)
            self.collections[tab] = coll

    def store(self, object_type: str, items: list[dict[str, Any]]) -> int:
        if not items:
            return 0
        ts = datetime.now(tz=timezone.utc).timestamp()
        docs = [
            zvec.Doc(
                id=generate_uuid(item[ID]).hex,
                vectors={EMBEDDING: item[EMBEDDING]},
                fields={ID: item[ID], TS: ts},
            )
            for item in items
        ]
        conn = self.collections[object_type]
        results = [conn.insert(chunk) for chunk in batched(docs, 1024)]
        return len([r for r in chain(*results) if r.ok()])

    def search(self, object_type: str, embedding: list[float], distance: Optional[float] = None, top_n: int = DEFAULT_TOPN) -> list[str]:        
        results = self.collections[object_type].query(
            zvec.VectorQuery(EMBEDDING, vector=embedding),
            topk=top_n,
            output_fields=[ID]
        )
        filter_distance = lambda r: not distance or (r.score <= distance)    
        return [r.fields[ID] for r in filter(filter_distance, results)]

    def batch_search(self, object_type: str, embeddings: list[list[float]], distance: Optional[float] = None, top_n: int = DEFAULT_TOPN) -> list[list[str]]:
        return [self.search(object_type, emb, distance, top_n) for emb in embeddings]        

    def close(self):
        try: [coll.optimize() for coll in self.collections.values()]
        except Exception as e: print("ERROR: CLASSIFICATION CACHE OPTIMIZE", e)
        self.collections.clear()
