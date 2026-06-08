import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from itertools import batched, chain
import os
import queue
import threading
from typing import Any, Optional
from pathlib import Path
import hashlib
import zvec
from icecream import ic

_METRIC_MAP = {"l2": zvec.MetricType.L2, "cosine": zvec.MetricType.COSINE}
DEFAULT_TOPN = 10

class ClassificationCache:
    def __init__(self, db_path: str, table_settings: dict[str, dict[str, Any]]):
        self.db_path = db_path
        self.table_settings = table_settings
        self.id_keys = {
            tab: setting["id_key"]
            for tab, setting in table_settings.items()
            if "id_key" in setting
        }

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
                        "embedding", 
                        data_type=zvec.DataType.VECTOR_FP32, 
                        dimension=setting["vector_length"],
                        index_param=zvec.HnswIndexParam(_METRIC_MAP.get(setting.get("distance_func"), zvec.MetricType.L2))
                    ),
                    fields=[
                        zvec.FieldSchema(setting["id_key"], zvec.DataType.STRING),
                        zvec.FieldSchema("ts", zvec.DataType.FLOAT)
                    ],
                )                
                coll = zvec.create_and_open(path=path, schema=schema)
            self.collections[tab] = coll

    def store(self, object_type: str, items: list[dict[str, Any]]) -> int:
        if not items:
            return 0
        id_key = self.id_keys[object_type]
        ts = datetime.now(tz=timezone.utc).timestamp()
        docs = [
            zvec.Doc(
                id=hashlib.sha256(item[id_key].encode('utf-8')).hexdigest(),
                vectors={"embedding": item["embedding"]},
                fields={id_key: item[id_key], "ts": ts},
            )
            for item in items
        ]
        conn = self.collections[object_type]
        results = chain(*(conn.insert(chunk) for chunk in batched(docs, 768)))        
        return len([r for r in results if r.code == 0])

    def search(self, object_type: str, embedding: list[float], distance: Optional[float] = None, top_n: int = DEFAULT_TOPN) -> list[str]:        
        results = self.collections[object_type].query(
            zvec.VectorQuery("embedding", vector=embedding),
            topk=top_n,
            output_fields=[self.id_keys[object_type]]
        )
        filter_distance = lambda r: not distance or (r.score <= distance)    
        return [r.fields[self.id_keys[object_type]] for r in filter(filter_distance, results)]

    def batch_search(self, object_type: str, embeddings: list[list[float]], distance: Optional[float] = None, top_n: int = DEFAULT_TOPN) -> list[list[str]]:
        return list(ThreadPoolExecutor().map(lambda emb: self.search(object_type, emb, distance, top_n), embeddings))        

    def close(self):
        try: [coll.optimize() for coll in self.collections.values()]
        except Exception as e: print("ERROR: CLASSIFICATION CACHE OPTIMIZE", e)
        self.collections.clear()
