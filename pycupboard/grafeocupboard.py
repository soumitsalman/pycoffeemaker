# NOTE: NOT READY FOR USE. IGNORE

from itertools import chain
from typing import Any
from grafeo import GrafeoDB
from pybeansack.models import (
    BASE_URL, CATEGORIES, CONTENT, CONTENT_LENGTH, CREATED, EMBEDDING, RELATED,
    RESTRICTED_CONTENT, SENTIMENTS, SOURCE, SUMMARY, SUMMARY_LENGTH, TAGS, TITLE,
    TITLE_LENGTH, URL, VECTOR_LEN,
)
import pandas as pd
from icecream import ic

EVENTS = "Events"
SIGNALS = "Signals"
REPORTS = "Reports"
SOURCES = "Sources"

distinct = lambda items: list({item.strip().lower():item for item in items}.values())

class Cupboard:
    db_path: str
    db: GrafeoDB

    def __init__(self, db_path: str):
        self.db = GrafeoDB(db_path)

    @classmethod
    def create_db(cls, db_path: str):
        db = GrafeoDB(db_path)
        db.create_property_index(URL)
        db.create_property_index(SOURCE)
        db.create_property_index(CREATED)
        db.create_property_index(CATEGORIES)
        db.create_property_index(BASE_URL)
        db.create_text_index(EVENTS, TAGS)
        db.create_text_index(SIGNALS, TAGS)
        db.create_text_index(REPORTS, TAGS)
        db.create_vector_index(EVENTS, EMBEDDING, VECTOR_LEN, "cosine")
        db.create_vector_index(SIGNALS, EMBEDDING, VECTOR_LEN, "cosine")
        db.create_vector_index(REPORTS, EMBEDDING, VECTOR_LEN, "cosine")

    def store(self, data_type: str, items: list[dict[str, Any]]):
        if not items: return 0

        node_ids = self.db.batch_create_nodes_with_props(data_type, items)
        
        if data_type == EVENTS:
            event_ids = node_ids
            source_ids = self.get_node_ids_by_unique_property(SOURCES, BASE_URL, list({item[BASE_URL] for item in items}))
            edges = [
                (source_ids.get(item[BASE_URL]), e_id)
                for item, e_id in zip(items, event_ids)
                if source_ids.get(item[BASE_URL])
            ]
            ic(self.batch_create_edges(edges, "PUBLISHED"))

        elif data_type == SOURCES:
            source_ids = node_ids
            event_ids = self.get_node_ids_by_property(EVENTS, BASE_URL, [item[BASE_URL] for item in items])
            edges = []
            for src, s_id in zip(items, source_ids):
                if e_ids := event_ids.get(src[BASE_URL]):
                    edges.extend((s_id, e) for e in e_ids)
            ic(self.batch_create_edges(edges, "PUBLISHED"))

        return len(node_ids)

    def get_node_ids_by_property(self, data_type: str, prop_name: str, prop_values: list):
        expr = f"""MATCH (d:{data_type})
        WHERE d.{prop_name} IN $prop_values
        RETURN id(d) AS id, d.{prop_name} AS {prop_name}"""
        results = self.db.execute(expr, {"prop_values": prop_values})
        items = {}
        for cur in results:
            if prop_name in items: items[prop_name].append(cur['id'])
            else: items[prop_name] = [cur['id']]
        ic(list(items.items())[:3], len(items))
        return items

    def get_node_ids_by_unique_property(self, data_type: str, prop_name: str, prop_values: list):
        expr = f"""MATCH (d:{data_type})
        WHERE d.{prop_name} IN $prop_values
        RETURN id(d) AS id, d.{prop_name} AS {prop_name}"""
        res = self.db.execute(expr, {"prop_values": prop_values})
        return {item[prop_name]: item['id'] for item in res}

    def link_events(self, items: list[dict[str, str]]):     
        node_ids = self.get_node_ids_by_unique_property(
            EVENTS, URL, 
            list(set(item[URL] for item in items).union(item["related_url"] for item in items))
        )        
        edges = [
            (node_ids[item[URL]], node_ids[item["related_url"]])
            for item in items
            if node_ids.get(item[URL]) and node_ids.get(item["related_url"])
        ]        
        return self.batch_create_edges(edges, "SAME_AS")

    def batch_create_edges(self, pairs: list[tuple[int, int]], edge_type: str):
        if not pairs:
            return 0

        edges_df = pd.DataFrame(
            [{"source": src, "target": dst} for src, dst in pairs]
        )
        return self.db.import_df(
            edges_df,
            mode="edges",
            edge_type=edge_type,
            source="source",
            target="target",
        )


    def query_events(self, embedding: list[float], distance: float = None, limit: int = 0, fields: list[str] = None):
        params = {"embedding": embedding}
        query = "MATCH (e:Events)"

        if distance:
            query += "\nWHERE cosine_distance(e.embedding, $embedding) AS distance <= $distance"
            params["distance"] = distance

        return_expr = "e" if not fields else ", ".join(f"e.{f}" for f in fields)
        query += f"\nRETURN {return_expr}, cosine_distance(e.embedding, $embedding) AS distance\nORDER BY distance"
        
        if limit:
            query += "\nLIMIT $limit"
            params["limit"] = limit

        return self.db.execute(query, params)

    def close(self):
        self.db.close()