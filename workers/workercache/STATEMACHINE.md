# State cache (processing state machine)

Workers share a **state cache**: durable storage for in-flight documents. Each orchestrator decides what to process with `get()` and records progress with `set()`. Implementations live under `workers/workercache/` (default: PostgreSQL via `PROCESSING_CACHE`; alternates in `extensions/`). All expose the same `StateCacheBase` / `AsyncStateCacheBase` API.

## Concepts

- **Object types** — One logical table per kind of data (configured at startup, not per worker):

| Type | Default id field |
|------|------------------|
| `beans` | `url` |
| `publishers` | `base_url` |
| `chatters` | `id` |
| `composites` | `id` |

- **States** — Named pipeline steps (`workers/utils.py`): `collected`, `embedded`, `classified`, `clustered`, `extracted`, `digested`, `consolidated`, `beansacked`, `cupboarded`, etc.
- **Per-object history** — Each object can have one entry per state it has reached. Payloads are usually **partial** (only fields that worker added). The full view for downstream work is built by reading multiple states together.
- **Idempotency** — Re-`set()`ting the same `(object, state)` is ignored. Workers select work with “has state A, does not have state B” (or several required states).

## How to use the API

### `set(object_type, state, items)`

Mark work done: store items under `state`. Pass dicts or Pydantic models; the cache derives `id` from the type’s id field. Typically only new fields for that step (e.g. `embedding`, `digest`).

### `get(object_type, states, exclude_states=..., ids=None, window=..., limit=0, offset=0)`

Fetch work queue or merged documents.

- **Single `states` + `exclude_states`** — Objects that reached `states` but not `exclude_states`. Used by collector follow-ups and most analyzers.  
  Example: `get(BEANS, states=collected, exclude_states=embedded)` → beans ready to embed.
- **List of `states`** — Merge payloads from every listed state for each id (porters, consolidator). All listed states must exist for that id.
- **`ids`** — Restrict to specific ids (e.g. consolidator fetching related beans).
- **`window`** — Only consider recent items (`PROCESSING_WINDOW` env).
- **`limit` / `offset`** — Batch large result sets (porters).

### `deduplicate(object_type, state, items)`

Drop items whose id already has `state` (collector: skip re-scrape).

### `optimize(cleanup_older_than=...)`

Prune old payload data; state markers remain so work is not redone.

## Bean state flow

All analyzer steps after collect read `collected` (or `embedded` for classifier) and write a new state row; branches are independent until a porter or consolidator merges multiple states.

```
                         collected
              ┌────────────┼────────────┐
              ▼            ▼            ▼
          embedded     extracted     digested
              │
         ┌────┴────┐
         ▼         ▼
    classified  clustered
              │
              │  consolidator: merge collected + embedded + classified
              │               + clustered + digested → consolidated
              │               (composites → collected on composites table)
              ▼
        consolidated

Porter merge requirements (each needs every listed state row for that bean):

  BeansackPorter — main beans:  collected + embedded + classified + extracted  → beansacked
                   related:     clustered  → beansacked:link

  CupboardPorter — events:      collected + embedded + classified + digested  → cupboarded
                   signals:     composites.collected  → cupboarded
                   related:     clustered  → cupboarded:link
```

`extracted` is required for Beansack but not for Cupboard events. `digested` is required for Cupboard events and consolidator, but not for Beansack main beans. `clustered` is only required for related-link porters and consolidator, not for the main bean/event hydrate paths.

## Who uses it

| Worker | Cache class | `get` | `set` / other |
|--------|-------------|-------|----------------|
| **Collector** | `AsyncStateCache` | — | `beans` / `publishers` / `chatters` → `collected`; `deduplicate` before scrape |
| **Embedder** | `StateCache` | `beans`: `collected` ∖ `embedded` | `embedded` (+ `embedding`) |
| **Extractor** | `StateCache` | `beans`: `collected` ∖ `extracted` | `extracted` (+ `entities`) |
| **Digestor** | `StateCache` | `beans`: `collected` ∖ `digested` | `digested` (+ `digest`) |
| **Classifier** | `StateCache` | `beans`: `embedded` ∖ `classified` / ∖ `clustered` | `classified`, `clustered` |
| **Consolidator** | `StateCache` | `beans`: multi-state; related by `ids` | `composites` → `collected`; `beans` → `consolidated` |
| **Beansack porter** | `AsyncStateCache` | `beans`: `[collected, embedded, classified, extracted]` ∖ `beansacked`; publishers, chatters, clustered links | `beansacked`, `beansacked:link`, … |
| **Cupboard porter** | `AsyncStateCache` | `beans`: `[collected, embedded, classified, digested]` ∖ `cupboarded`; composites, publishers, clustered | `cupboarded`, `cupboarded:link`, … |

**Classifier** also uses `ClassificationCache` (`CLASSIFICATION_CACHE`) for topic/sentiment vectors and clustering — separate from the processing state cache.

Wiring in `run.py`: `PROCESSING_CACHE` + `cache_settings` (id keys per object type). Collector and porter use async cache; analyzers use sync cache.

## Related docs

- `workers/utils.py` — state and field constants
- `AGENTS.md` / `README.md` — modes and env vars
- `workers/workercache/pgcache.py` — default backend implementation
