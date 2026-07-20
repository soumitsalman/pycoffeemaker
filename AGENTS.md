# Coffeemaker

Backend processing engine for **Project Cafecito**: collect web content, enrich with NLP/LLMs, ship to downstream DBs (Beansack, Cupboard). Workers are independent processes; `run.py` selects one via `--mode` or `MODE` env (parallel or sequential deployments).

## Repository layout

```
pycoffeemaker/
├── run.py                 # Entry: --mode, --batch_size; loads .env
├── run_pipeline.sh        # Multi-stage scheduler (GPU/CPU/IO ordering)
├── machine_ops.py         # GPU cloud start/stop (TensorDock, Azure)
├── factory/               # feeds.yaml, pipeline-defaults.env, classifications.yaml, migrate/rectify
├── workers/               # Orchestrators + state cache
├── datacollectors/        # RSS/API/scrapers (APICollectorAsync, AsyncWebScraper)
├── nlp/                   # Embeddings, digests, NER (vendored package; see nlp/README.md)
├── pybeansack/            # Bean/Chatter/Publisher models + DB backends (vendored package)
├── pycupboard/            # Sip/Source + Cupboard (Cortado)
└── tests/                 # Integration tests
```

Operational detail (install, env vars, Docker): `README.md`.

## Workers (`workers/`)

One **mode** per process. Orchestrators are unaware of each other; coordination is via `PROCESSING_CACHE` state tables.

| Mode | Module | Role | Load |
|------|--------|------|------|
| `COLLECTOR` | `collectororch.py` | Ingest RSS, APIs, Reddit, scraped pages; normalize fields; scrape publishers | IO |
| `EMBEDDER` | `analyzerorch.py` | Vector embeddings; lightweight topic/sentiment labels (CPU) | GPU + light CPU |
| `CLUSTERING` | `analyzerorch.py` | Related-article clustering (`CLASSIFICATION_CACHE`) | CPU-heavy |
| `EXTRACTOR` | `analyzerorch.py` | NER (people, orgs, regions, tickers) via GLiNER | GPU |
| `DIGESTOR` | `analyzerorch.py` | Structured digests (gist, highlights) via LLM | GPU |
| `CONSOLIDATOR` | `analyzerorch.py` | Composite briefings from related beans | GPU or remote API |
| `PORTER` | `porterorch.py` | `BeansackPorter` + `CupboardPorter` → PG Beansack + Cupboard | IO |

### Scheduling (`run_pipeline.sh`)

Single-process entry: `run.py --mode MODE`. Multi-stage runs use `run_pipeline.sh`:

```bash
./run_pipeline.sh --collector 128 --embedder 512 --clustering 128 \
  --extractor 24 --digestor 32 --consolidator 32 --porter 32
```

Each flag enables a stage and sets its `--batch_size`. Omit flags for stages you do not want.

**Resource model**

| Stage | Bound | Parallelism |
|-------|-------|-------------|
| `COLLECTOR`, `PORTER` | IO | Background; overlap with other stages |
| `EMBEDDER`, `EXTRACTOR`, `DIGESTOR`, `CONSOLIDATOR` | GPU | Serial with each other (one GPU job at a time) |
| `CLUSTERING` | CPU | Background after embedder; overlaps extractor and/or digestor |

**Order and dependencies** (enforced by `run_pipeline.sh`):

```
collector (bg) ─────────────────────────────────────────┐
embedder (sync)                                         │
  ├─ clustering (bg, CPU) ───────── wait before ────────┤
  ├─ extractor (sync, GPU)     ── serial GPU stages ────┤
  └─ digestor (sync, GPU)                               │
consolidator (sync, GPU)  ← needs digest; needs clustering if enabled
porter (bg) ────────────────────────────────────────────┘
```

- **Embedder** must finish before clustering, extractor, or digestor (clustering reads embeddings).
- **Clustering** starts immediately after embedder and may still be running while extractor/digestor run.
- **Consolidator** runs after extractor and digestor (if enabled) and waits for clustering when `--clustering` is set (needs `embedded`, `digested`, `clustered`, related links).
- **Porter** can start while collector is still running; hydrates Beansack/Cupboard from finished cache states.

Suggested cadence: collector ~2×/day; embedder/clustering/extractor/digestor ~3×/day; consolidator with digestor; porter on demand.

**Configuration** — `factory/pipeline-defaults.env` holds checked-in defaults for deployment convenience (model paths, context lengths, analyzer tuning). Python entrypoints load it first via `utils/env.load_coffeemaker_env`, then `.env` at repo root with override. Without a local `.env`, workers use those defaults as-is. Put secrets and host-specific overrides (`PROCESSING_CACHE`, `BEANSACK_CONNECTION_STRING`, API keys, alternate `EMBEDDER_PATH`, etc.) in `.env` only. `run_pipeline.sh` sources `.env` for shell-level vars (e.g. `SHUTDOWN_URL`).

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

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

When the user types `/graphify`, use the installed graphify skill or instructions before doing anything else.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- Dirty graphify-out/ files are expected after hooks or incremental updates; dirty graph files are not a reason to skip graphify. Only skip graphify if the task is about stale or incorrect graph output, or the user explicitly says not to use it.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).

## MUST FOLLOW
- Extremely concise short response for every ask and plan
- Answer every ask/plan with yes/no/may-be if the answer is binary
- Include code samples in every plan
- Include pros and cons as bullet points for every suggestion
