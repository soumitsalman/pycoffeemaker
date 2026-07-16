__all__ = [
    'models', 
    'Bean', 'Chatter', 'Publisher', "TrendingBean", "AggregatedBean", 
    'Beansack', 'DuckSack', 'LanceSack', 'PGSack',
    'SimpleVectorDB', 'AsyncCDNStore',
    "create_client", "create_db",
    "BEANS", "PUBLISHERS", "CHATTERS", "RELATED_BEANS", "DATETIME"
] 

from .ducksack import DuckSack
from .lancesack import LanceSack
from .pgsack import PGSack
from .models import *
from .simplevectordb import *
from .cdnstore import *
from .database import *

DB_TYPE = Literal["duckdb", "duck", "lancedb", "lance", "ducklake", "dl", "postgres", "postgresql", "pg"]

def create_client(db_type: DB_TYPE, **connection_kwargs) -> Beansack:
    if db_type in ["postgres", "postgresql", "pg"]: return PGSack(connection_kwargs['pg_connection_string'])
    if db_type in ["lancedb", "lance"]: return LanceSack(connection_kwargs['lancedb_storage'])
    if db_type in ["duckdb", "duck"]: return DuckSack(db_path=connection_kwargs['duckdb_storage'])
    if db_type in ["ducklake", "dl"]: return DuckSack(catalog_db=connection_kwargs['ducklake_catalog'], storage_path=connection_kwargs['ducklake_storage'])
    raise ValueError("unsupported connection string")

def create_db(db_type: DB_TYPE, **connection_kwargs) -> Beansack:
    if db_type in ["pg", "postgres", "postgresql"]: return pgsack.create_db(connection_kwargs['pg_connection_string'])
    if db_type in ["lancedb", "lance"]: return lancesack.create_db(connection_kwargs['lancedb_storage'])
    if db_type in ["duckdb", "duck"]:
        return ducksack.create_db(db_path=connection_kwargs["duckdb_storage"], catalogs_dir=connection_kwargs.get("catalogs_dir"))
    if db_type in ["ducklake", "dl"]:
        return ducksack.create_db(
            catalog_db=connection_kwargs["ducklake_catalog"],
            storage_path=connection_kwargs["ducklake_storage"],
            catalogs_dir=connection_kwargs.get("catalogs_dir"),
        )
    raise ValueError("unsupported db type")
