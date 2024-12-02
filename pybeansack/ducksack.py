import duckdb
from .datamodels import *
from .utils import *
import os
from icecream import ic

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

sql_where_urls = lambda urls: "url IN (" + ','.join([f"'{url}'" for url in urls]) + ")"

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
SELECT url, SUM(likes) as likes, SUM(comments) as comments, COUNT(chatter_url) as shares
FROM(
    SELECT url, chatter_url, MAX(collected) as collected, MAX(likes) as likes, MAX(comments) as comments 
    FROM chatters 
    GROUP BY url, chatter_url
) 
GROUP BY url
"""
sql_total_chatters_ndays_ago = lambda last_ndays: f"""
SELECT url, SUM(likes) as likes, SUM(comments) as comments, COUNT(chatter_url) as shares
FROM(
    SELECT url, chatter_url, MAX(collected) as collected, MAX(likes) as likes, MAX(comments) as comments 
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
    db: duckdb.DuckDBPyConnection
    categories_db: duckdb.DuckDBPyConnection

    def __init__(self, db_dir: str):
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

        self.db = duckdb.connect(f"{db_dir}/beansack.db").cursor()  
        self.db.execute(SQL_INSTALL_VSS)      
        self.db.execute(SQL_CREATE_BEANS)        
        self.db.execute(SQL_CREATE_CHATTERS)
        self.db.execute(SQL_CREATE_CATEGORIES)
        # not adding categories vector index and beans vector index for now since vss in duckdb is unstable

    def store_beans(self, beans: list[Bean]):
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
        self.db.executemany(SQL_INSERT_BEANS, beans_data)

    def not_exists(self, beans: list[Bean]) -> list[Bean]:
        if beans:
            exists = {item[0] for item in self.db.query(f"SELECT url FROM beans WHERE {sql_where_urls([bean.url for bean in beans])}").fetchall()}
            return list({bean.url: bean for bean in beans if (bean.url not in exists)}.values())

    def search_beans(self, embedding: list[float], min_score: float = DEFAULT_VECTOR_SEARCH_SCORE, limit: int = 0) -> list[Bean]:
        result = self.db.sql(sql_search_beans(embedding, min_score))
        if limit:
            result = result.limit(limit)
        result.show()
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
        result = self.db.query(sql_search_similar_beans(url, max_distance))
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
        self.db.executemany(SQL_INSERT_CHATTERS, chatters_data)

    def get_latest_chatters(self, last_ndays: int) -> list[Bean]:
        total = self.db.query(SQL_TOTAL_CHATTERS)
        ndays_ago = self.db.query(sql_total_chatters_ndays_ago(last_ndays))
        result = self.db.query("""
            SELECT 
                total.url as url, 
                total.likes as likes, 
                total.comments as comments, 
                total.shares as shares,                            
                total.likes - COALESCE(ndays_ago.likes, 0) as latest_likes, 
                total.comments - COALESCE(ndays_ago.comments, 0) as latest_comments, 
                total.shares - COALESCE(ndays_ago.shares, 0) as latest_shares, 
            FROM total
            LEFT JOIN ndays_ago ON total.url = ndays_ago.url
            WHERE latest_likes <> 0 OR latest_comments <> 0 OR latest_shares <> 0
        """)
        result.show()
        return [Bean(
            url=chatter[0],
            likes=chatter[1],
            comments=chatter[2],
            shares=chatter[3],
            latest_likes=chatter[4],
            latest_comments=chatter[5],
            latest_shares=chatter[6],
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
        self.db.executemany(SQL_INSERT_CATEGORIES, categories_data)

    def search_categories(self, embedding: list[float], min_score: float = DEFAULT_VECTOR_SEARCH_SCORE, limit: int = 0) -> list[str]:
        result = self.db.query(sql_search_categories(embedding, min_score))
        if limit:
            result = result.limit(limit)
        # result.show()
        return [category[0] for category in result.fetchall()]

    def close(self):
        self.db.close()
  