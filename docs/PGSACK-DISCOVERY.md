# PGSack Primary-Key and Relationship Discovery

## Verified production state

Database: Neon project `cafecito-apps-v2`, production branch, database `beansdb`.

The live schema differs from `pybeansack/pgsack.sql`:

| Object | Live primary key | Required |
|---|---|---|
| `beans` | `url` | `id` |
| `publishers` | `source` | `id`; rename `source` to `domain_name` |
| `beans.source` | domain string | `publishers.id` UUID foreign key |

Data checks:

- `beans`: 1,996,050 rows; all IDs populated; 1,996,008 distinct IDs.
- `publishers`: 27,188 rows; all IDs populated; 27,177 distinct IDs.
- 42 duplicate bean UUID groups exist, caused by case-variant URLs after `generate_uuid()` lowercases input.
- 11 duplicate publisher UUID groups exist, including eight Reddit publishers sharing `www.reddit.com` as `base_url`.
- 1,566,319 beans do not currently match a publisher through `beans.source = publishers.source`.
- Current cluster selection has 68,081 top-degree ties; the existing tie-breaker is lexical candidate URL.
- Current storage is approximately 19 GB for `beans` and 3.7 GB for `related_beans` plus indexes.

## Required work

### 1. `pybeansack/pgsack.sql`

Final table shape:

```sql
beans.id UUID PRIMARY KEY
beans.source UUID NOT NULL REFERENCES publishers(id)
publishers.id UUID PRIMARY KEY
publishers.domain_name VARCHAR NOT NULL UNIQUE
```

Retain a unique constraint on `beans.url`; it remains the ingestion identity even though it is no longer the primary key.

The live database needs an explicit migration. Updating `CREATE TABLE IF NOT EXISTS` only affects new databases.

### 2. `pybeansack/pgsack.py`

Switch operational keys to UUID IDs for conflicts, deduplication, and updates:

```python
_PRIMARY_KEYS = {
    BEANS: ID,
    PUBLISHERS: ID,
    RELATED_BEANS: [URL, "related_url"],
}
```

Use deterministic canonical IDs:

```python
bean.id = generate_uuid(bean.url)
publisher.id = generate_uuid(publisher.domain_name)
```

Publisher IDs should derive from `domain_name`, not `base_url`, because multiple logical publishers can share one base URL.

### 3. `pybeansack/models.py`

Update the shared storage contract:

```python
class Publisher(BaseModel):
    id: UUID
    domain_name: str

class Bean(BaseModel):
    id: UUID
    source: UUID  # publishers.id
```

`AggregatedBean` should expose `source` as the publisher UUID and `domain_name` as the display/filter field.

Query APIs need separate publisher-ID and domain-name filters. Since the models are shared by DuckSack and LanceSack, their adapters/tests must be updated or a PostgreSQL-specific model boundary must be introduced.

### 4. `workers/porterorch.py`

Publishers must be ported before beans. The current concurrent hydration cannot satisfy a foreign key.

Resolve cached domain names to publisher IDs before constructing/storing beans:

```python
publisher_ids = db.ensure_publishers(domain_names)
bean["source"] = publisher_ids[bean.pop("domain_name")]
```

The 1.56 million unmatched beans require a policy. Recommended: create placeholder publishers keyed by domain name and allow `publishers.base_url` to be nullable when no canonical URL is known.

### 5. Trending and aggregated views

Rebuild `trend_aggregates`, `trending_beans_view`, and `aggregated_beans_view` so their grain is one row per `beans.id`:

```sql
JOIN trend_aggregates tr ON tr.bean_id = b.id
JOIN publishers p ON p.id = b.source
```

Views cannot have physical primary keys, so enforce uniqueness in the materialized trend relation with a unique index on `bean_id`.

For cluster selection, retain the current highest-frequency rule and add deterministic tie-breaking:

```sql
ORDER BY cc.bean_url,
         COALESCE(rf.cnt, 0) DESC,
         candidate.collected ASC NULLS LAST,
         candidate.id ASC
```

Return `cluster_id` as the representative bean UUID.

### 6. Migration rollout

Use a Neon branch for rehearsal. Because of table size, build indexes concurrently and stage constraints:

1. Identify and resolve duplicate bean/publisher identity groups.
2. Backfill canonical publisher rows, including placeholders for unmatched bean domains.
3. Backfill `beans.source` UUID values.
4. Create unique indexes concurrently on `beans.id`, `beans.url`, `publishers.id`, and `publishers.domain_name`.
5. Add the foreign key as `NOT VALID`, then validate it.
6. Swap primary-key constraints from `url`/`source` to `id`.
7. Rename `publishers.source` to `domain_name`.
8. Rebuild and refresh the trend materialized view and dependent views.
9. Run count, uniqueness, FK, and view-cardinality checks before production cutover.

For duplicate identity rows, use the lowest `collected` row as the winner and merge non-null enrichment fields from the losing rows before deletion.

## Optional P2: ID-based related beans

Replace `related_beans(url, related_url)` with `(bean_id, related_bean_id)` UUID columns, populate them through the canonical bean-ID map, add UUID foreign keys and a unique pair constraint, then update porter logic and trend CTEs to operate on IDs.

## Verification tests

Add PostgreSQL integration coverage for:

- UUID primary-key constraints on both tables.
- Duplicate/case-variant identity resolution.
- Publisher UUID foreign-key enforcement.
- Bean and publisher updates keyed by `id`.
- Cluster tie resolution by earliest `collected`, then UUID.
- One trend/aggregated row per bean UUID.
- P2 related-bean UUID foreign keys and uniqueness.
