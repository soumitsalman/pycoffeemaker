# Pycoffeemaker (Coffeemaker)

Backend processing engine for **Project Cafecito**: collect web content, enrich it with NLP/LLMs, and ship results to downstream databases (Beansack, Cupboard, etc.). Workers run independently and are scheduled by `run.py`—in parallel or sequence—as needed.

Can be deployed as standalone worker nodes or imported by other services (e.g. Espresso UI).

## Directory structure

```
pycoffeemaker/
├── run.py                 # Entry point: --mode selects worker
├── machine_ops.py         # Start/stop GPU cloud instances (TensorDock, Azure)
├── requirements.txt       # Full deps (GPU/LLM workloads)
├── requirements-io.txt    # IO-only deps (collector, porter)
├── Dockerfile             # CPU image (collector + light analysis)
├── DockerfileGPU          # CUDA image (digestor, vLLM, etc.)
├── DockerfileIO           # Slim IO image
├── docker-compose.yaml    # Local stack: pgcache, mongo, postgres, workers, azurite, crawl4ai
├── factory/
│   ├── feeds.yaml         # RSS/API/social source lists for COLLECTOR
│   ├── classifications.yaml  # Topic/sentiment labels for CLASSIFIER
│   ├── setup.py / migrate.py / rectify.py  # DB setup & maintenance
├── workers/               # Orchestrators (operators)
│   ├── collectororch.py   # COLLECTOR
│   ├── analyzerorch.py    # EMBEDDER, EXTRACTOR, DIGESTOR, CLASSIFIER, CONSOLIDATOR
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
| Embed | `EMBEDDER` | Vector embeddings on article content |
| Extract | `EXTRACTOR` | Named entities (people, companies, regions, tickers) via GLiNER |
| Digest | `DIGESTOR` | Structured digests (gist, highlights) via LLM |
| Classify | `CLASSIFIER` | Topic categories + sentiments; related-article clustering |
| Consolidate | `CONSOLIDATOR` | Group related digests into composite briefings/signals |
| Port | `PORTER` | Push finished beans/chatters/publishers to Beansack (PG) and Cupboard |

Processing is **idempotent**: each item moves through named states in `workers/workercache`; already-done work is skipped.

Suggested schedule (from `run.py` comments): collector ~2×/day; embedder/extractor/classifier/digestor ~3×/day; porter on demand.

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
- **Embedder / Extractor / Digestor / Classifier / Consolidator** (`analyzerorch.py`): read from cache by state, call `nlp`, write next state (`embedded`, `extracted`, `digested`, `classified`, `consolidated`).
- **Porter** (`porterorch.py`): `BeansackPorter` + `CupboardPorter` hydrate downstream DBs from cache.

### `datacollectors/`

`APICollectorAsync`, `AsyncWebScraper`—shared field constants (`URL`, `CONTENT`, `SOURCE`, etc.).

### `workers/workercache/`

Fault-tolerant state machine used by all orchestrators. Default backend: `pgcache.StateCache` / `pgcache.AsyncStateCache` via `PROCESSING_CACHE`. `clscache.ClassificationCache` backs CLASSIFIER (`CLASSIFICATION_CACHE`). Alternate backends live under `extensions/` (sqlite, firebird, surreal). State flow: `workers/workercache/STATEMACHINE.md`.

Tracks per-object processing states (`beans`, `publishers`, `chatters`, `composites`).

### `nlp/`

Embeddings (`create_embedder`), structured extraction (`create_micro_agent` / `Digest`, `Briefing`), NER (`EntityExtractor`). Supports local HF, vLLM, ONNX, OpenVINO, remote OpenAI-compatible APIs. Details: `nlp/README.md`.

### `pybeansack/`

Pydantic models (`Bean`, `Chatter`, `Publisher`) and storage: `create_client("pg"|"lance"|"duck"|"dl", ...)`.

### `pycupboard/`

`Sip`, `Source` models; PostgreSQL cupboard via `Cupboard` connection string.

### `factory/`

Operational config: feed lists, classification taxonomies, migrations—not runtime library code.

## How to use

### Prerequisites

- Python 3.10+ (Docker images use 3.10 or 3.13)
- `.env` at repo root (loaded by `run.py`)
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

# Embedder (needs EMBEDDER_PATH)
python run.py --mode EMBEDDER --batch_size 64

# Extractor, digestor, classifier, consolidator — same pattern
python run.py --mode EXTRACTOR
python run.py --mode DIGESTOR
python run.py --mode CLASSIFIER
python run.py --mode CONSOLIDATOR

# Porter
python run.py --mode PORTER
```

`MODE` and `BATCH_SIZE` can be set via environment instead of CLI.

### Key environment variables

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
| `CLASSIFICATION_CACHE` | Vector store for CLASSIFIER (`workers/workercache/clscache.py`, zvec) |
| `BEANSACK_CONNECTION_STRING` | PORTER |
| `CUPBOARD_CONNECTION_STRING` | PORTER |
| `LOG_DIR` | Optional hourly logfmt log file; otherwise logfmt to stderr |
| `WORDS_THRESHOLD_FOR_STORING` | Min words before full scrape (collector) |

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
