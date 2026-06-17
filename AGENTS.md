# Coffeemaker

Backend processing engine for **Project Cafecito**: collect web content, enrich with NLP/LLMs, ship to downstream DBs (Beansack, Cupboard). Workers are independent processes; `run.py` selects one via `--mode` or `MODE` env (parallel or sequential deployments).

## Repository layout

```
pycoffeemaker/
├── run.py                 # Entry: --mode, --batch_size; loads .env
├── machine_ops.py         # GPU cloud start/stop (TensorDock, Azure)
├── factory/               # feeds.yaml, classifications.yaml, migrate/rectify
├── workers/               # Orchestrators + state cache
├── datacollectors/        # RSS/API/scrapers (APICollectorAsync, AsyncWebScraper)
├── nlp/                   # Embeddings, digests, NER (submodule; see nlp/README.md)
├── pybeansack/            # Bean/Chatter/Publisher models + DB backends (submodule)
├── pycupboard/            # Sip/Source + Cupboard (Cortado)
└── tests/                 # Integration tests
```

Operational detail (install, env vars, Docker): `README.md`.

## Workers (`workers/`)

One **mode** per process. Orchestrators are unaware of each other; coordination is via `PROCESSING_CACHE` state tables.

| Mode | Module | Role | Load |
|------|--------|------|------|
| `COLLECTOR` | `collectororch.py` | Ingest RSS, APIs, Reddit, scraped pages; normalize fields; scrape publishers | IO |
| `EMBEDDER` | `analyzerorch.py` | Vector embeddings on bean content | Compute |
| `EXTRACTOR` | `analyzerorch.py` | NER (people, orgs, regions, tickers) via GLiNER | Compute |
| `DIGESTOR` | `analyzerorch.py` | Structured digests (gist, highlights) via LLM | Compute / GPU |
| `CLASSIFIER` | `analyzerorch.py` | Topics, sentiments, related-article clustering (`CLASSIFICATION_CACHE`) | Compute |
| `CONSOLIDATOR` | `analyzerorch.py` | Composite briefings from related beans | Compute / GPU or remote API |
| `PORTER` | `porterorch.py` | `BeansackPorter` + `CupboardPorter` → PG Beansack + Cupboard | IO |

Suggested cadence (see `run.py` comments): collector ~2×/day; embedder/extractor/classifier/digestor ~3×/day; porter on demand.

### State machine (`workers/workercache/`)

Fault-tolerant warehouse: per-type tables (`beans`, `publishers`, `chatters`, `composites`) with `state`, `ts`, `data`, optional `id`. Workers prefer bulk insert/delete over update. Default backend: PostgreSQL (`pgcache.py`) via `PROCESSING_CACHE`; alternates in `extensions/` (sqlite/Turso, firebird, surreal, pg+cls).

Schema and read/write patterns: `workers/workercache/STATEMACHINE.md`. State constants: `workers/utils.py`.

Bean pipeline (simplified): `collected` → `embedded` → `classified` / `clustered` → `extracted` / `digested` → `consolidated` → `beansacked` / `cupboarded`.

Idempotency: workers query include/exclude states; finished work is skipped.

## Data units

| Unit | Package | Notes |
|------|---------|-------|
| **Bean**, **Chatter**, **Publisher**, **Composite** | `pybeansack/models.py` | Article, engagement, source, consolidated briefing |
| **Sip**, **Source** | `pycupboard/` | Cupboard (Cortado) units |

Storage: `pybeansack.create_client("pg"\|"lance"\|"duck"\|"dl", ...)`. Cupboard: `pycupboard.pgcupboard.Cupboard`.

## Cafecito naming

| Name | Role |
|------|------|
| Beansack | Primary article DB (**Beans**) |
| Cupboard | Cortado vector store (**Sips**) |
| Espresso | UI that may consume Coffeemaker output |

## Other components

- **`datacollectors/`** — shared field constants (`URL`, `CONTENT`, `SOURCE`, …); `apicollectors.py`, `scrapers.py`
- **`nlp/`** — `create_embedder`, `create_micro_agent`, `Digest`, `EntityExtractor`; local HF, vLLM, ONNX, remote APIs
- **`factory/`** — `feeds.yaml` (`COLLECTOR_SOURCES`), `classifications.yaml`, DB setup/migrations (not runtime libs)

---
HARD REQUIREMENT: EXTREMELY CONCISE TOKEN EFFICIENT RESPONSES ONLY

## graphify

Knowledge graph at `graphify-out/` (god nodes, communities, cross-file edges).

When the user types `/graphify`, invoke the `skill` tool with `skill: "graphify"` before doing anything else.

Rules:
- ALWAYS read `graphify-out/GRAPH_REPORT.md` before reading source files, running grep/glob, or answering codebase questions.
- IF `graphify-out/wiki/index.md` EXISTS, navigate it instead of reading raw files
- For cross-module "how does X relate to Y", prefer `graphify query "<question>"`, `graphify path "<A>" "<B>"`, or `graphify explain "<concept>"` over grep
- After modifying code, run `graphify update .` using .venv of the current project (AST-only, no API cost)
