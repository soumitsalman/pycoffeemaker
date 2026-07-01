# PYBEANSACK

> **Canonical location:** this package lives in the [pycoffeemaker](https://github.com/soumitsalman/pycoffeemaker) monorepo at `pybeansack/`. The standalone GitHub repo is archived.

An intelligent SDK for storing, querying, and analyzing articles ("beans"), social media chatter, and publisher metadata. Designed for news aggregation, trend analysis, and content discovery with support for vector search and semantic queries.

## Features

- **Multi-Backend Support**: PostgreSQL (with pgvector), DuckDB, LanceDB, and DuckLake
- **Semantic Search**: Vector-based similarity search with embeddings
- **Social Signal Integration**: Correlate articles with social media likes, comments, and shares
- **Rich Filtering**: Query by categories, sentiments, entities, regions, sources, date ranges, and more
- **Data Models**: Type-safe Pydantic models for beans (articles), chatters, publishers, and aggregated content
- **Flexible Ingestion**: Support for Parquet, JSON, and CSV file formats
- **Async/Concurrent Operations**: ThreadPoolExecutor support for batch operations

## Installation

```bash
pip install pybeansack
```

## Quick Start

### Initialize a Database Client

```python
from pybeansack import create_client

# PostgreSQL
db = create_client(
    "postgres",
    pg_connection_string="postgresql://user:password@localhost/beansack"
)

# DuckDB
db = create_client("duckdb", duckdb_storage="/path/to/beansack.duckdb")

# LanceDB
db = create_client("lance", lancedb_storage="/path/to/lancedb")
```

### Store Articles (Beans)

```python
from pybeansack.models import Bean
from datetime import datetime

beans = [
    Bean(
        url="https://example.com/article1",
        kind="news",
        title="Breaking News",
        summary="Article summary...",
        content="Full article content...",
        source="example.com",
        author="John Doe",
        created=datetime.now(),
        categories=["technology", "AI"],
        regions=["US"],
        entities=["OpenAI", "GPT-4"]
    )
]

stored_count = db.store_beans(beans)
print(f"Stored {stored_count} beans")
```

### Query Beans

```python
from datetime import datetime, timedelta

# Query latest articles
beans = db.query_latest_beans(
    kind="news",
    categories=["technology"],
    limit=10
)

# Query with date range
past_week = datetime.now() - timedelta(days=7)
recent_beans = db.query_latest_beans(
    created=(past_week, datetime.now()),
    limit=20
)

# Vector search (semantic similarity)
embedding = [0.1, 0.2, ...]  # Your embedding vector
similar_beans = db.query_latest_beans(
    embedding=embedding,
    distance=0.75,
    limit=5
)
```

### Store and Query Social Chatter

```python
from pybeansack.models import Chatter

chatters = [
    Chatter(
        url="https://example.com/article1",
        chatter_url="https://twitter.com/user/status/123",
        source="twitter",
        forum="tech",
        likes=100,
        comments=25,
        subscribers=5000
    )
]

db.store_chatters(chatters)

# Query aggregated content (articles + social signals)
aggregated = db.query_aggregated_beans(
    categories=["technology"],
    limit=10
)
```

### Store and Query Publishers

```python
from pybeansack.models import Publisher

publishers = [
    Publisher(
        source="example.com",
        base_url="https://example.com",
        site_name="Example News",
        description="Latest news and updates",
        favicon="https://example.com/favicon.ico",
        rss_feed="https://example.com/feed.rss"
    )
]

db.store_publishers(publishers)

# Query publishers
sources = db.query_publishers(
    sources=["example.com"],
    limit=10
)
```

## Data Models

### Bean (Article)
An article from an RSS feed, news site, or blog post.

**Key Fields:**
- `url`: Unique article URL
- `kind`: Type of content (news, blog, oped, job, post, comments)
- `title`: Article title
- `summary`: Short summary or gist
- `content`: Full article content
- `source`: Publisher domain (e.g., "example.com")
- `author`: Article author
- `created`: Publication date
- `updated`: Last updated date
- `categories`: Content categories
- `sentiments`: Sentiment labels
- `entities`: Named entities mentioned
- `regions`: Geographic regions relevant to content
- `embedding`: Vector embedding for semantic search
- `tags`: Custom tags
- `cluster_id`: Clustering ID for related articles

### Chatter (Social Media)
Social media mentions, comments, or engagement with an article.

**Key Fields:**
- `url`: Article URL being referenced
- `chatter_url`: URL of the social media post
- `source`: Source platform (twitter, linkedin, reddit, etc.)
- `forum`: Group or community where chatter was found
- `collected`: Collection timestamp
- `likes`: Like count
- `comments`: Comment count
- `subscribers`: Audience size of the source

### Publisher
Metadata about a news source or content publisher.

**Key Fields:**
- `source`: Domain/publisher ID
- `base_url`: Publisher base URL
- `site_name`: Publisher name
- `description`: Publisher description
- `favicon`: Publisher icon URL
- `rss_feed`: RSS feed URL
- `collected`: Metadata collection timestamp

### AggregatedBean
Combines Bean, Chatter, and Publisher data for rich content representation.

## Database Backends

### PostgreSQL
Relational backend with pgvector extension for semantic search.

```python
db = create_client("postgres", pg_connection_string="postgresql://user:password@localhost/beansack")
```

`trend_aggregates` is a single materialized view over `chatters`, `related_beans`, and `beans`. Refresh via `db.optimize()` or:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY trend_aggregates;
```

### DuckDB
Embedded OLAP database, great for local development and analytics.

```python
db = create_client("duck", duckdb_storage="/path/to/beansack.duckdb")
```

### LanceDB
Vector database optimized for fast similarity search.

```python
db = create_client("lance", lancedb_storage="/path/to/lancedb")
```

### DuckLake
Combined DuckDB catalog with object storage.

```python
db = create_client(
    "ducklake",
    ducklake_catalog="/path/to/catalog",
    ducklake_storage="/path/to/storage"
)
```

## Common Patterns

### Filter by Multiple Criteria

```python
beans = db.query_latest_beans(
    kind="news",
    categories=["technology", "AI"],
    sources=["techcrunch.com", "theverge.com"],
    created=(datetime.now() - timedelta(days=7), datetime.now()),
    limit=20,
    offset=0
)
```

### Deduplicate Before Storing

```python
new_beans = [...]  # Your beans to store
deduplicated = db.deduplicate("beans", new_beans)
db.store_beans(deduplicated)
```

### Update Bean Metadata

```python
beans_to_update = [...]  # Beans with new metadata
db.update_beans(
    beans_to_update,
    columns=["categories", "sentiments", "cluster_id"]
)
```

### Update Embeddings

```python
beans_with_embeddings = [...]  # Beans with new embedding vectors
db.update_embeddings(beans_with_embeddings)
```

### Get Distinct Values

```python
categories = db.distinct_categories(limit=100)
sentiments = db.distinct_sentiments()
entities = db.distinct_entities()
regions = db.distinct_regions()
publishers = db.distinct_publishers()
```

### Count Records

```python
total_beans = db.count_rows("beans")
recent_count = db.count_rows("beans", conditions=["created > NOW() - INTERVAL 7 DAY"])
```

## Advanced Usage

### Using Custom Database Functions

Each backend has specialized features accessible via their specific classes:

```python
from pybeansack.pgsack import PGSack
from pybeansack.lancesack import LanceSack

pg_db = create_client("pg", pg_connection_string="postgresql://user:password@localhost/beansack")
lance_db = create_client("lance", lancedb_storage="/path/to/lancedb")
```

### Batch Processing

```python
from concurrent.futures import ThreadPoolExecutor

def process_batch(offset):
    beans = fetch_beans_from_source(offset, batch_size=128)
    prepared = prepare_beans_for_store(beans)
    deduplicated = db.deduplicate("beans", prepared)
    return db.store_beans(deduplicated)

with ThreadPoolExecutor(max_workers=8) as executor:
    results = list(executor.map(process_batch, range(0, total, 128)))
```

## Development

### Requirements

```
pydantic
pyarrow
pandas
duckdb
lancedb
pgvector
psycopg[binary,pool]
s3fs
rfc3339
tenacity
```

### Running Tests

```bash
# From repo root (integration tests opt-in via marker)
pytest tests/test_pybeansack_db.py -m "integration and pg"

# Or run the test module directly (forwards args to pytest)
python tests/test_pybeansack_db.py -m "integration and pg"
```

### Docker Setup

A `docker-compose.yml` is included for local development with multiple databases:

```bash
docker-compose up -d
```

This starts PostgreSQL, and other services needed for testing.

## Architecture

- **Beansack** (Abstract): Base interface for all database backends
- **Cupboard** (Abstract): Base interface for catalog operations
- **Backend Implementations**: Postgres, DuckDB, LanceDB, DuckLake (legacy MongoDB in `deprecated/`)
- **Models**: Pydantic-based data models with validation
- **Utils**: Helper functions for data processing, deduplication, and transformation

## License

See LICENSE file for details.

