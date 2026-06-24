# Pycoffeemaker (Coffeemaker)

Backend processing engine for **Project Cafecito**: collect web content, enrich it with NLP/LLMs, and ship results to downstream databases (Beansack, Cupboard, etc.). Workers run independently and are scheduled by `run.py`—in parallel or sequence—as needed.

Can be deployed as standalone worker nodes or imported by other services (e.g. Espresso UI).

## Directory structure

```
pycoffeemaker/
├── run.py                 # Entry point: --mode selects worker
├── run_pipeline.sh        # Multi-stage scheduler (GPU/CPU/IO ordering)
├── machine_ops.py         # Start/stop GPU cloud instances (TensorDock, Azure)
├── requirements.txt       # Full deps (GPU/LLM workloads)
├── requirements-io.txt    # IO-only deps (collector, porter)
├── Dockerfile             # CPU image (collector + light analysis)
├── DockerfileGPU          # CUDA image (digestor, vLLM, etc.)
├── DockerfileIO           # Slim IO image
├── docker-compose.yaml    # Local stack: pgcache, mongo, postgres, workers, azurite, crawl4ai
├── factory/
│   ├── feeds.yaml         # RSS/API/social source lists for COLLECTOR
│   ├── pipeline-defaults.env  # Checked-in model paths and analyzer defaults
│   ├── classifications.yaml  # Topic/sentiment labels
│   ├── setup.py / migrate.py / rectify.py  # DB setup & maintenance
├── workers/               # Orchestrators (operators)
│   ├── collectororch.py   # COLLECTOR
│   ├── analyzerorch.py    # EMBEDDER, CLUSTERING, EXTRACTOR, DIGESTOR, CONSOLIDATOR
│   ├── porterorch.py      # PORTER → Beansack + Cupboard
│   ├── utils.py           # State names, field constants
│   └── workercache/       # Fault-tolerant state store (pg, sqlite, firebird, surreal)
│       ├── base.py        # StateCacheBase / AsyncStateCacheBase
│       ├── pgcache.py     # Default PostgreSQL state cache (PROCESSING_CACHE)
│       ├── clscache.py    # Classification vector store (CLASSIFICATION_CACHE)
│       └── extensions/    # Alternate backends (sqlite, firebird, surreal, pg+cls)
├── datacollectors/        # RSS, APIs, async web scrapers
├── nlp/                   # Embeddings, digests, NER (see nlp/README.md)
├── pybeansack/            # Bean/Chatter/Publisher models + DB backends (pg, lance, duck, mongo)
├── pycupboard/            # Sip/Source models for Cortado (Cupboard)
├── tests/                 # Integration tests & sample source YAMLs
└── .env                   # Local secrets and connection strings (not committed)
```

## Capabilities

| Stage | Mode | What it does |
|-------|------|----------------|
| Collect | `COLLECTOR` | Ingest RSS, APIs, Reddit, scraped pages; normalize title, content, metadata, chatter stats |
| Embed | `EMBEDDER` | Vector embeddings; lightweight topic/sentiment labels (CPU) |
| Cluster | `CLUSTERING` | Related-article clustering (`CLASSIFICATION_CACHE`) |
| Extract | `EXTRACTOR` | Named entities (people, companies, regions, tickers) via GLiNER |
| Digest | `DIGESTOR` | Structured digests (gist, highlights) via LLM |
| Consolidate | `CONSOLIDATOR` | Group related digests into composite briefings/signals |
| Port | `PORTER` | Push finished beans/chatters/publishers to Beansack (PG) and Cupboard |

Processing is **idempotent**: each item moves through named states in `workers/workercache`; already-done work is skipped.

## Pipeline scheduling

Single-process entry: `python run.py --mode MODE`. Multi-stage runs use `run_pipeline.sh`:

```bash
./run_pipeline.sh --collector 128 --embedder 512 --clustering 128 \
  --extractor 24 --digestor 32 --consolidator 32 --porter 32
```

Each flag enables a stage and sets its `--batch_size`. Omit flags for stages you do not want.

### Stages → cache → downstream

| Mode | Cache state | Downstream (via `PORTER`) |
|------|-------------|---------------------------|
| `COLLECTOR` | `collected` | title, content, source, dates, tags |
| `EMBEDDER` | `embedded` | `embedding`, categories, sentiments |
| `CLUSTERING` | `clustered` | `related` links (porter link tables) |
| `EXTRACTOR` | `extracted` | `entities` (Beansack main beans) |
| `DIGESTOR` | `digested` | digest-derived regions/entities (Cupboard events) |
| `CONSOLIDATOR` | `consolidated` | composite briefings → Cupboard signals |
| `PORTER` | `beansacked` / `cupboarded` | rows in Beansack / Cupboard |

State merge rules: `workers/workercache/STATEMACHINE.md`.

### Resource model and order

| Stage | Bound | Parallelism |
|-------|-------|-------------|
| `COLLECTOR`, `PORTER` | IO | Background; overlap with other stages |
| `EMBEDDER`, `EXTRACTOR`, `DIGESTOR`, `CONSOLIDATOR` | GPU | Serial with each other (one GPU job at a time) |
| `CLUSTERING` | CPU | Background after embedder; overlaps extractor and/or digestor |

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
- **Consolidator** runs after extractor and digestor (if enabled) and waits for clustering when `--clustering` is set.
- **Porter** can start while collector is still running; hydrates Beansack/Cupboard from finished cache states.

Suggested cadence: collector ~2×/day; embedder/clustering/extractor/digestor ~3×/day; consolidator with digestor; porter on demand or at end of each pipeline run.

### Configuration

`factory/pipeline-defaults.env` holds checked-in defaults for deployment convenience (model paths, context lengths, analyzer tuning). Python entrypoints load it first via `utils/env.load_coffeemaker_env`, then `.env` at repo root with override. Without a local `.env`, workers use those defaults as-is. Put secrets and host-specific overrides (`PROCESSING_CACHE`, `BEANSACK_CONNECTION_STRING`, API keys, alternate `EMBEDDER_PATH`, etc.) in `.env` only. `run_pipeline.sh` sources `.env` for shell-level vars (e.g. `SHUTDOWN_URL`).

## Project Cafecito naming

| Name | Role |
|------|------|
| **Bean** | Atomic article unit (news, blog, post, etc.) |
| **Chatter** | Social engagement for a bean URL (likes, comments, forum) |
| **Publisher** | Site/source metadata (favicon, RSS, description) |
| **Composite** | Consolidated briefing spanning multiple related beans |
| **Beansack** | Primary article store for **Cafecito Beans** |
| **Cupboard** | **Cortado** store; `Sip` records with embeddings + digests |
| **Sip** | Cupboard’s normalized content unit (UUID from URL) |
| **Espresso** | UI that may import or consume Coffeemaker output |

## Component guide

### `workers/` — orchestrators

- **Collector** (`collectororch.py`): reads `COLLECTOR_SOURCES` (default `factory/feeds.yaml`), uses `datacollectors` for RSS/API/scrape; writes `collected` state.
- **Embedder / Clustering / Extractor / Digestor / Consolidator** (`analyzerorch.py`): read from cache by state, call `nlp`, write next state (`embedded`, `clustered`, `extracted`, `digested`, `consolidated`).
- **Porter** (`porterorch.py`): `BeansackPorter` + `CupboardPorter` hydrate downstream DBs from cache.

### `datacollectors/`

`APICollectorAsync`, `AsyncWebScraper`—shared field constants (`URL`, `CONTENT`, `SOURCE`, etc.).

### `workers/workercache/`

Fault-tolerant state machine used by all orchestrators. Default backend: `pgcache.StateCache` / `pgcache.AsyncStateCache` via `PROCESSING_CACHE`. `clscache.ClassificationCache` backs `CLUSTERING` (`CLASSIFICATION_CACHE`). Alternate backends live under `extensions/` (sqlite, firebird, surreal). State flow: `workers/workercache/STATEMACHINE.md`.

Tracks per-object processing states (`beans`, `publishers`, `chatters`, `composites`).

### `nlp/`

Embeddings (`create_embedder`), structured extraction (`create_micro_agent` / `Digest`, `Briefing`), NER (`EntityExtractor`). Supports local HF, vLLM, ONNX, OpenVINO, remote OpenAI-compatible APIs. Details: `nlp/README.md`.

### `pybeansack/`

Pydantic models (`Bean`, `Chatter`, `Publisher`) and storage: `create_client("pg"|"lance"|"duck"|"dl", ...)`.

### `pycupboard/`

`Sip`, `Source` models; PostgreSQL cupboard via `Cupboard` connection string.

### `factory/`

Operational config: feed lists, `pipeline-defaults.env`, classification taxonomies, migrations—not runtime library code.

## How to use

### Prerequisites

- Python 3.10+ (Docker images use 3.10 or 3.13)
- `factory/pipeline-defaults.env` for checked-in pipeline defaults; `.env` at repo root for secrets and local overrides
- Model paths for analyzer modes (`EMBEDDER_PATH`, `EXTRACTOR_PATH`, `DIGESTOR_PATH`, `CONSOLIDATOR_PATH`)
- `PROCESSING_CACHE` — state DB connection (default: PostgreSQL via `workers/workercache/pgcache.py`; see `extensions/` for sqlite, firebird, surreal)
- For `PORTER`: `BEANSACK_CONNECTION_STRING`, `CUPBOARD_CONNECTION_STRING`

### Install (local)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r pybeansack/requirements.txt
pip install -r requirements.txt   # or requirements-io.txt for collector-only
```

### Run a worker

```bash
# Collector
python run.py --mode COLLECTOR --batch_size 32

# Single analyzer stage
python run.py --mode EMBEDDER --batch_size 64
python run.py --mode CLUSTERING --batch_size 128
python run.py --mode EXTRACTOR
python run.py --mode DIGESTOR
python run.py --mode CONSOLIDATOR

# Porter (partial backfill; needs upstream cache states)
python run.py --mode PORTER --batch_size 32

# Full pipeline (see Pipeline scheduling above)
./run_pipeline.sh --collector 128 --embedder 512 --clustering 128 \
  --extractor 24 --digestor 32 --consolidator 32 --porter 32
```

`MODE` and `BATCH_SIZE` can be set via environment instead of CLI.

### Key Environment Variables

Python entrypoints load `factory/pipeline-defaults.env` first, then `.env`; duplicate values in `.env` win. See **Configuration** under Pipeline scheduling. `run_pipeline.sh` sources `.env` for shell-level settings like `SHUTDOWN_URL`; deployment task scripts can export host-specific GPU settings before calling it.

| Variable | Used by |
|----------|---------|
| `MODE` | Worker selection (if not passed as `--mode`) |
| `BATCH_SIZE` | Batch size for all modes |
| `PROCESSING_CACHE` | State machine connection |
| `COLLECTOR_SOURCES` | Path to feeds YAML (default: `factory/feeds.yaml`) |
| `EMBEDDER_PATH`, `EMBEDDER_CONTEXT_LEN` | EMBEDDER |
| `EXTRACTOR_PATH`, `EXTRACTOR_CONTEXT_LEN` | EXTRACTOR |
| `DIGESTOR_PATH`, `DIGESTOR_CONTEXT_LEN` | DIGESTOR |
| `CONSOLIDATOR_PATH`, `CONSOLIDATOR_BASE_URL`, `CONSOLIDATOR_API_KEY` | CONSOLIDATOR (remote LLM optional) |
| `CLASSIFICATION_CACHE` | Vector store for `CLUSTERING` (`workers/workercache/clscache.py`, zvec) |
| `BEANSACK_CONNECTION_STRING` | PORTER |
| `CUPBOARD_CONNECTION_STRING` | PORTER |
| `LOG_DIR` | Optional hourly logfmt log file; otherwise logfmt to stderr |
| `WORDS_THRESHOLD_FOR_STORING` | Min words before full scrape (collector) |

#### Optional Environment Variables
For Digestor with LiquidAI/LFM2.5-1.2B-Instruct
```
DIGESTOR_TEMPERATURE=0.15
DIGESTOR_TOP_K=50
DIGESTOR_REPETITION_PENALTY=1.05
```

For Digestor with nvidia/NVIDIA-Nemotron-3-Nano-4B-BF16
```
DIGESTOR_TEMPERATURE=0.4
DIGESTOR_TOP_P=0.95
DIGESTOR_REPETITION_PENALTY=1.15
```

For Consolidator with nvidia/NVIDIA-Nemotron-3-Nano-4B-BF16
```
CONSOLIDATOR_TEMPERATURE=0.8
CONSOLIDATOR_TOP_P=0.95
CONSOLIDATOR_TOP_K=50
CONSOLIDATOR_REPETITION_PENALTY=1.15
```

### Processing states (beans)

`collected` → `embedded` → `classified` / `clustered` → `extracted` / `digested` → `consolidated` → `beansacked` / `cupboarded`

## How to deploy

### Docker images

| File | Use |
|------|-----|
| `Dockerfile` | CPU PyTorch; collector + embedder defaults |
| `DockerfileGPU` | CUDA 12.8; digestor / GPU LLM |
| `DockerfileIO` | Slim Python 3.13; IO-bound modes only |

Build example:

```bash
docker build -f DockerfileGPU -t coffeemaker:gpu .
```

Run example:

```bash
docker run --gpus all --env-file .env \
  -e MODE=DIGESTOR -e BATCH_SIZE=4 \
  coffeemaker:gpu
```

`DockerfileIO` entrypoint: `python run.py --mode $MODE`.

### Docker Compose (local dev)

```bash
docker compose up pgcache localmongo localpostgres   # infra only
# Worker services reference image soumitsr/coffeemaker:* and dockertest.env
```

Services include: `pgcache` (pgvector), `localmongo`, `localpostgres`, `localcollector`, `localdigestor`, `localcrawler` (crawl4ai), `azurite`. Compose env uses legacy mode names in places (`INDEXER`, `COMPOSER`); current `run.py` modes are listed above.

### Cloud GPU ops

`machine_ops.py` starts/stops TensorDock or Azure instances:

```bash
python machine_ops.py --provider tensordock --action stop
# Requires TD_INSTANCE_ID, TD_API_KEY or AZ_AUTH_URL, GPU_PROVIDER
```

### Production notes

- Run **one mode per container/process**; scale collectors and analyzers independently.
- GPU nodes: `DockerfileGPU` + `DIGESTOR` / `CONSOLIDATOR` with remote API (`base_url`/`api_key`) if no local GPU.
- IO nodes: `DockerfileIO` + `COLLECTOR` / `PORTER`.
- Keep `PROCESSING_CACHE` (state DB) and downstream DBs reachable from every worker tier.

## Further reading

- `AGENTS.md` — design overview for agents/tools
- `workers/workercache/STATEMACHINE.md` — state machine schema and worker read/write flow
- `nlp/README.md` — model backends and API
- `pybeansack/` — data model and DB adapters
