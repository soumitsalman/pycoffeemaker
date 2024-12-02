import duckdb
from datetime import datetime as dt
from pybeansack.datamodels import *
import os

INSTALL_VSS = """
INSTALL vss;
LOAD vss;
SET hnsw_enable_experimental_persistence = true;
SET checkpoint_threshold='1TB';
"""

CREATE_BEANS = """
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

CREATE INDEX IF NOT EXISTS beans_embedding 
ON beans 
USING HNSW (embedding)
WITH (metric = 'cosine');
"""

INSERT_BEANS = """
INSERT INTO beans (url, created, collected, updated, source, author, kind, title, embedding) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
"""

CREATE_CHATTERS = """
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
INSERT_CHATTERS = """
INSERT INTO chatters (url, chatter_url, source, channel, collected, likes, comments, shares, subscribers) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (url, chatter_url, likes, comments, shares) DO NOTHING
"""

CREATE_CATEGORIES = """
CREATE TABLE IF NOT EXISTS categories (
    id VARCHAR PRIMARY KEY,
    text VARCHAR,
    related VARCHAR[],
    description VARCHAR,
    embedding FLOAT[1024]
);

CREATE INDEX IF NOT EXISTS categories_embedding 
ON categories 
USING HNSW (embedding)
WITH (metric = 'cosine');
"""
INSERT_CATEGORIES = """
INSERT INTO categories (id, text, related, description, embedding) VALUES (?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
"""

# SELECT url, SUM(likes) as likes, SUM(comments) as comments, COUNT(chatter_url) as shares
# FROM(
#     SELECT url, chatter_url, MAX(collected) as collected, MAX(likes) as likes, MAX(comments) as comments 
#     FROM chatters 
#     GROUP BY url, chatter_url
# ) 
# GROUP BY url
# """
where_urls = lambda urls: "url IN (" + ','.join([f"'{url}'" for url in urls]) + ")"
get_chatters = lambda last_ndays, before: f"""
SELECT url, SUM(likes) as likes, SUM(comments) as comments, COUNT(chatter_url) as shares
FROM(
    SELECT url, chatter_url, MAX(collected) as collected, MAX(likes) as likes, MAX(comments) as comments 
    FROM chatters 
    WHERE collected {'<' if before else '>='} CURRENT_TIMESTAMP - INTERVAL '{last_ndays} days'
    GROUP BY url, chatter_url
)
GROUP BY url
"""

class Beansack:
    db: duckdb.DuckDBPyConnection
    categories_db: duckdb.DuckDBPyConnection

    def __init__(self, db_dir: str):
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

        self.db = duckdb.connect(f"{db_dir}/beansack.db").cursor()  
        self.db.execute(INSTALL_VSS)      
        self.db.execute(CREATE_BEANS)        
        self.db.execute(CREATE_CHATTERS)
        self.db.execute(CREATE_CATEGORIES)
        
        # self.categories_db = duckdb.connect(f"{db_dir}/categories.db").cursor()
        # self.categories_db.execute(INSTALL_VSS)
        # self.categories_db.execute(CREATE_CATEGORIES)

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
        self.db.executemany(INSERT_BEANS, beans_data)

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
        self.db.executemany(INSERT_CHATTERS, chatters_data)

    def get_latest_chatters(self, last_ndays: int) -> list[Bean]:
        in_last_ndays = self.db.query(get_chatters(last_ndays, False))
        before_last_ndays = self.db.query(get_chatters(last_ndays, True))
        result = self.db.query("""
            SELECT 
                in_last_ndays.url as url, 
                in_last_ndays.likes as likes, 
                in_last_ndays.comments as comments, 
                in_last_ndays.shares as shares,                            
                in_last_ndays.likes - COALESCE(before_last_ndays.likes, 0) as latest_likes, 
                in_last_ndays.comments - COALESCE(before_last_ndays.comments, 0) as latest_comments, 
                in_last_ndays.shares - COALESCE(before_last_ndays.shares, 0) as latest_shares, 
            FROM in_last_ndays
            LEFT JOIN before_last_ndays ON in_last_ndays.url = before_last_ndays.url
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
    
    def store_categories(self, categories: list[dict]):
        categories_data = [(
                category.get('_id'),
                category.get('text'),
                category.get('related'),
                category.get('description'),
                category.get('embedding')
            ) for category in categories
        ]
        self.db.executemany(INSERT_CATEGORIES, categories_data)

    def get_categories(self, embedding: list[float]) -> list[str]:
        res = self.db.query(f"SELECT text FROM categories WHERE array_cosine_distance(embedding, {embedding}::FLOAT[1024]) BETWEEN -0.2 AND 0.2")
        res.show()
        return [category[0] for category in res.fetchall()]

    # def get_total_chatters(self) -> list[Chatter]:
    #     res = self.db.query(TOTAL_CHATTERS)        
    #     return _deserialize_chatters(res)
        
def _deserialize_chatters(result: duckdb.DuckDBPyRelation) -> list[Chatter]:
    result.show()
    return [Chatter(
        url=chatter[0],
        likes=chatter[1],
        comments=chatter[2],
        shares=chatter[3]
    ) for chatter in result.fetchall()]

