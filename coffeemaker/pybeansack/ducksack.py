import asyncio
import duckdb
from .models import *
from .utils import *
import os
from icecream import ic
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

SQL_INSTALL_VSS = """
INSTALL vss;
LOAD vss;
SET hnsw_enable_experimental_persistence = true;
SET checkpoint_threshold='1TB';
"""

SQL_CREATE_BEANS = """
CREATE TABLE IF NOT EXISTS beans (
    url VARCHAR PRIMARY KEY,
    text TEXT,
    created TIMESTAMP,    
    collected TIMESTAMP,
    updated TIMESTAMP,
    source VARCHAR,
    author VARCHAR,
    kind VARCHAR,
    title VARCHAR,
    embedding FLOAT[1024],
    categories VARCHAR[],
    tags VARCHAR[],
    summary TEXT,
    cluster_id VARCHAR
);
"""
SQL_CREATE_BEANS_VECTOR_INDEX = """
CREATE INDEX IF NOT EXISTS beans_embedding 
ON beans 
USING HNSW (embedding)
WITH (metric = 'cosine');
"""
SQL_INSERT_BEANS = """
INSERT INTO beans (url, created, collected, updated, source, author, kind, title, embedding) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
"""

SQL_CREATE_CHATTERS = """
CREATE TABLE IF NOT EXISTS chatters (
    url VARCHAR,
    chatter_url VARCHAR,
    collected TIMESTAMP,
    source VARCHAR,
    channel VARCHAR,
    likes INTEGER DEFAULT 0,
    comments INTEGER DEFAULT 0,
    shares INTEGER DEFAULT 0,
    subscribers INTEGER DEFAULT 0,
    UNIQUE (url, chatter_url, likes, comments, shares)
)
"""
SQL_INSERT_CHATTERS = """
INSERT INTO chatters (url, chatter_url, source, channel, collected, likes, comments, shares, subscribers) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (url, chatter_url, likes, comments, shares) DO NOTHING
"""

SQL_CREATE_CATEGORIES = """
CREATE TABLE IF NOT EXISTS categories (
    id VARCHAR PRIMARY KEY,
    text VARCHAR,
    related VARCHAR[],
    description VARCHAR,
    embedding FLOAT[1024]
);
"""
SQL_CREATE_CATEGORIES_VECTOR_INDEX = """
CREATE INDEX IF NOT EXISTS categories_embedding 
ON categories 
USING HNSW (embedding)
WITH (metric = 'cosine');
"""
SQL_INSERT_CATEGORIES = """
INSERT INTO categories (id, text, related, description, embedding) VALUES (?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
"""

sql_where_urls = lambda urls: "url IN (" + ', '.join(f"'{url}'" for url in urls) + ")"

sql_search_beans = lambda embedding, min_score: f"""
SELECT url, created, collected, updated, kind, array_cosine_similarity(embedding, {embedding}::FLOAT[1024]) as search_score
FROM beans
WHERE search_score >= {min_score}
ORDER BY search_score DESC
"""
sql_search_similar_beans = lambda url, max_distance: f"""
SELECT 
    url, 
    array_distance(
        embedding, 
        (SELECT embedding FROM beans WHERE url = '{url}')::FLOAT[1024]
    ) as distance_score            
FROM beans
WHERE distance_score <= {max_distance}
ORDER BY distance_score
"""
SQL_TOTAL_CHATTERS = """
SELECT url, 
    SUM(likes) as likes, 
    SUM(comments) as comments, 
    MAX(collected) as collected,
    COUNT(chatter_url) as shares,
    ARRAY_AGG(DISTINCT source) FILTER (WHERE source IS NOT NULL) || ARRAY_AGG(DISTINCT channel) FILTER (WHERE channel IS NOT NULL) as shared_in

FROM(
    SELECT url, 
        chatter_url, 
        MAX(collected) as collected, 
        MAX(likes) as likes, 
        MAX(comments) as comments, 
        FIRST(source) as source, 
        FIRST(channel) as channel
    FROM chatters 
    GROUP BY url, chatter_url
) 
GROUP BY url
"""
sql_total_chatters_ndays_ago = lambda last_ndays: f"""
SELECT url, 
    SUM(likes) as likes, 
    SUM(comments) as comments, 
    MAX(collected) as collected,
    COUNT(chatter_url) as shares,
    ARRAY_AGG(DISTINCT source) FILTER (WHERE source IS NOT NULL) || ARRAY_AGG(DISTINCT channel) FILTER (WHERE channel IS NOT NULL) as shared_in
FROM(
    SELECT url, 
        chatter_url, 
        MAX(collected) as collected, 
        MAX(likes) as likes, 
        MAX(comments) as comments, 
        FIRST(source) as source, 
        FIRST(channel) as channel
    FROM chatters 
    WHERE collected < CURRENT_TIMESTAMP - INTERVAL '{last_ndays} days'
    GROUP BY url, chatter_url
)
GROUP BY url
"""
sql_search_categories = lambda embedding, min_score: f"""
SELECT text, array_cosine_similarity(embedding, {embedding}::FLOAT[1024]) as search_score 
FROM categories 
WHERE search_score >= {min_score}
ORDER BY search_score DESC
"""

class Beansack:
    db_filepath: str
    db: duckdb.DuckDBPyConnection

    def __init__(self, db_dir: str):
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
        self.db_filepath = f"{db_dir}/beansack.db"
        self.db = duckdb.connect(self.db_filepath, read_only=False) \
            .execute(SQL_INSTALL_VSS) \
            .execute(SQL_CREATE_BEANS) \
            .execute(SQL_CREATE_CHATTERS) \
            .execute(SQL_CREATE_CATEGORIES) \
            .commit()

    def store_beans(self, beans: list[Bean]):
        local_conn = self.db.cursor()
        beans_data = [
            (
                bean.url,
                bean.created,                
                bean.collected,
                bean.updated,
                bean.source,
                bean.author,
                bean.kind,
                bean.title,
                bean.embedding
            ) for bean in beans
        ]
        local_conn.executemany(SQL_INSERT_BEANS, beans_data).commit()

    def exists(self, beans: list[Bean]) -> list[str]:
        local_conn = self.db.cursor()
        query = local_conn.sql("SELECT url FROM beans").filter(sql_where_urls([bean.url for bean in beans]))
        return {item[0] for item in query.fetchall()}

    def search_beans(self, embedding: list[float], min_score: float = DEFAULT_VECTOR_SEARCH_SCORE, limit: int = 0) -> list[Bean]:
        local_conn = self.db.cursor()
        result = local_conn.sql(sql_search_beans(embedding, min_score))
        if limit:
            result = result.limit(limit)
        # result.show()
        return [Bean(
            url=bean[0],
            created=bean[1],
            collected=bean[2],
            updated=bean[3],
            kind=bean[4],   
            search_score=bean[5],
            slots=True
        ) for bean in result.fetchall()]
    
    def search_similar_beans(self, url: str, max_distance: float, limit: int = 0) -> list[str]:        
        local_conn = self.db.cursor()
        result = local_conn.query(sql_search_similar_beans(url, max_distance))
        if limit:
            result = result.limit(limit)
        # result.show()
        return [bean[0] for bean in result.fetchall()]

    def store_chatters(self, chatters: list[Chatter]):
        chatters_data = [
            (
                chatter.url,
                chatter.chatter_url,                
                chatter.source,
                chatter.channel,
                chatter.collected,
                chatter.likes,
                chatter.comments,
                chatter.shares,
                chatter.subscribers
            ) for chatter in chatters
        ]
        local_conn = self.db.cursor()
        local_conn.executemany(SQL_INSERT_CHATTERS, chatters_data).commit()

    def get_latest_chatters(self, last_ndays: int, urls: list[str] = None) -> list[ChatterAnalysis]:
        local_conn = self.db.cursor()
        total = local_conn.query(SQL_TOTAL_CHATTERS)
        ndays_ago = local_conn.query(sql_total_chatters_ndays_ago(last_ndays))
        if urls:
            total = total.filter(sql_where_urls(urls))
            ndays_ago = ndays_ago.filter(sql_where_urls(urls))
        result = local_conn.query("""
            SELECT 
                total.url as url, 
                total.likes as likes, 
                total.comments as comments, 
                total.collected as last_collected,
                total.shares as shares,
                total.shared_in as shared_in,                            
                total.likes - COALESCE(ndays_ago.likes, 0) as likes_change, 
                total.comments - COALESCE(ndays_ago.comments, 0) as comments_change, 
                total.shares - COALESCE(ndays_ago.shares, 0) as shares_change, 
                ndays_ago.shared_in as shared_in_change,
            FROM total
            LEFT JOIN ndays_ago ON total.url = ndays_ago.url
            WHERE likes_change <> 0 OR comments_change <> 0 OR shares_change <> 0
        """)
        return [ChatterAnalysis(
            url=chatter[0],
            likes=chatter[1],
            comments=chatter[2],
            last_collected=chatter[3],
            shares=chatter[4],
            shared_in=chatter[5],
            likes_change=chatter[6],
            comments_change=chatter[7],
            shares_change=chatter[8],
            shared_in_change=chatter[9],
            slots=True
        ) for chatter in result.fetchall()]
    
    # def get_total_chatters(self) -> list[Chatter]:
    #     result = self.db.query(SQL_TOTAL_CHATTERS)        
    #     result.show()
    #     return [Chatter(
    #         url=chatter[0],
    #         likes=chatter[1],
    #         comments=chatter[2],
    #         shares=chatter[3],
    #         slots=True
    #     ) for chatter in result.fetchall()]
    
    def store_categories(self, categories: list[dict]):
        categories_data = [(
                category.get('_id'),
                category.get('text'),
                category.get('related'),
                category.get('description'),
                category.get('embedding')
            ) for category in categories
        ]
        local_conn = self.db.cursor()
        local_conn.executemany(SQL_INSERT_CATEGORIES, categories_data).commit()

    def search_categories(self, embedding: list[float], min_score: float = DEFAULT_VECTOR_SEARCH_SCORE, limit: int = 0) -> list[str]:
        local_conn = self.db.cursor()
        result = local_conn.query(sql_search_categories(embedding, min_score))
        if limit:
            result = result.limit(limit)
        # result.show()
        return [category[0] for category in result.fetchall()]

    def close(self):
        self.db.close()

    def backup_azblob(self, conn_str: str):
        try:
            client = BlobClient.from_connection_string(conn_str, "backup", "beansack.db")
            with open(self.db_filepath, "rb") as data:
                client.upload_blob(data, overwrite=True)            
        except Exception as e:
            print("Failed backup to Azure Blob Storage", e)
  