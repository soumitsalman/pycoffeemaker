# Pycoffeemaker (Coffeemaker)

Backend processing engine for **Project Cafecito**: collect web content, enrich it with NLP/LLMs, and ship results to downstream databases (Beansack, Cupboard, etc.). Workers run independently and are scheduled by `run.py`—in parallel or sequence—as needed.

Can be deployed as standalone worker nodes or imported by other services (e.g. Espresso UI).

## Directory structure

```
pycoffeemaker/
├── run.py                 # Entry: --mode selects worker; loads .env
├── run_pipeline.sh        # Multi-stage scheduler + checked-in model defaults
├── requirements.txt       # Full deps (local GPU/LLM; includes vllm)
├── requirements-gpu.txt   # DockerfileGPU add-on deps (no vllm; base image supplies it)
├── requirements-io.txt    # DockerfileIO: collector, porter, remote OpenAI NLP
├── requirements-dev.txt   # Test/dev extras
├── DockerfileGPU          # vllm/vllm-openai:latest; ENTRYPOINT run_pipeline.sh
├── DockerfileIO           # Slim Python 3.13; ENTRYPOINT run_pipeline.sh
├── docker-compose.yaml    # Legacy local stack (mongo / INDEXER / COMPOSER)
├── fly.collector.toml     # Fly.io collector job (DockerfileIO)
├── fly.porter.toml        # Fly.io porter job (DockerfileIO)
├── factory/
│   ├── feeds.yaml         # RSS/API/social source lists for COLLECTOR
│   ├── classifications.yaml
│   ├── categories.parquet / sentiments.parquet  # Embedder label indexes
│   ├── setup.py / migrate.py / rectify*.py      # DB setup & maintenance
│   ├── install-thundercompute-s6-tasks.sh       # s6 oneshot installer (no args)
│   ├── thundercompute-s6-tasks.sh               # Boot: hardcoded GPU stages
│   ├── salad-deployment/                        # Salad Compute recipes
│   └── deprecated/                              # Old GPU ops / prod setup
├── workers/               # Orchestrators (operators)
│   ├── collectororch.py   # COLLECTOR
│   ├── analyzerorch.py    # EMBEDDER, CLUSTERING, EXTRACTOR, DIGESTOR
│   ├── consolidatororch.py # CONSOLIDATOR
│   ├── porterorch.py      # PORTER → Beansack + Cupboard
│   ├── states.py          # Cache table names + pipeline state constants
│   └── cacheops.py        # Shared bean encache/decache helpers
├── processingcache/       # Fault-tolerant state store (pg default)
│   ├── base.py            # StateCacheBase / AsyncStateCacheBase
│   ├── pgcache.py         # Default PostgreSQL state cache (PROCESSING_CACHE)
│   ├── clscache.py        # Classification vector store (CLASSIFICATION_CACHE)
│   ├── extensions/        # sqlite, surreal, pg+cls
│   └── STATEMACHINE.md    # Schema and read/write patterns
├── utils/                 # Shared logging, dates, ids, fields, env loading
├── datacollectors/        # RSS, APIs, async web scrapers (see datacollectors/README.md)
├── nlp/                   # Embeddings, digests, NER (see nlp/README.md)
├── pybeansack/            # Bean/Chatter/Publisher models + DB backends (see pybeansack/README.md)
├── pycupboard/            # Sip/Source models; PG Cupboard for PORTER
├── design/                # Design notes
├── tests/                 # Integration tests & sample source YAMLs
└── .env                   # Local secrets and connection strings (not committed)
```

## Capabilities

| Stage | Mode | What it does |
|-------|------|----------------|
| Collect | `COLLECTOR` | Ingest RSS, APIs, Reddit, scraped pages; normalize title, content, metadata, chatter stats |
| Embed | `EMBEDDER` | Vector embeddings (GPU) plus topic/sentiment labels (CPU kNN); both written as `embedded` |
| Cluster | `CLUSTERING` | Related-article clustering (`CLASSIFICATION_CACHE`) |
| Extract | `EXTRACTOR` | Named entities (people, companies, regions, tickers) via GLiNER |
| Digest | `DIGESTOR` | Structured digests (gist, highlights) via LLM |
| Consolidate | `CONSOLIDATOR` | Group related digests into composite briefings/signals |
| Port | `PORTER` | Push finished beans/chatters/publishers to Beansack (PG) and Cupboard |

Processing is **idempotent**: each item moves through named states in `processingcache`; already-done work is skipped.

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
| `EXTRACTOR` | `extracted` | `entities` (Beansack beans and Cupboard events) |
| `DIGESTOR` | `digested` | digest fields (Cupboard events) |
| `CONSOLIDATOR` | `consolidated` on beans; composites → `collected` | composite briefings → Cupboard signals |
| `PORTER` | `beansacked` / `cupboarded` | rows in Beansack / Cupboard |

`CLASSIFIED` exists in `workers/states.py` but no worker writes or reads that state. Topic/sentiment labels are stored on `embedded`.

State merge rules: `processingcache/STATEMACHINE.md`.

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

Checked-in model defaults live at the top of `run_pipeline.sh` (paths, context lengths, sampling). The script then sources `.env` so secrets and host overrides win.

`run.py` loads **only** `.env` via `dotenv` (`load_dotenv(..., override=True)`). It does not apply `run_pipeline.sh` defaults — set `EMBEDDER_PATH` and the other model vars in `.env` (or the environment) before `python run.py`. Factory scripts call `utils.env.load_coffeemaker_env`, which also only loads `.env`.

Put `PROCESSING_CACHE`, `BEANSACK_CONNECTION_STRING`, API keys, and host-specific model paths in `.env`. `run_pipeline.sh` uses `.env` for shell-level vars such as `COMPLETION_WEBHOOK_URL`.

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
- **Embedder / Clustering / Extractor / Digestor** (`analyzerorch.py`): read from cache by state, call `nlp`, write next state (`embedded`, `clustered`, `extracted`, `digested`).
- **Consolidator** (`consolidatororch.py`): composite briefings from related beans → `composites.collected` and beans `consolidated`.
- **Porter** (`porterorch.py`): `BeansackPorter` + `CupboardPorter` hydrate downstream DBs from cache.
- **States / cache helpers**: `states.py` (table + state constants), `cacheops.py` (shared encache/decache).

### [`datacollectors/`](datacollectors/README.md)

`APICollectorAsync`, `AsyncWebScraper`—shared field constants (`URL`, `CONTENT`, `SOURCE`, etc.). See [datacollectors/README.md](datacollectors/README.md) for feeds, scrape settings, and normalization.

### `processingcache/`

Fault-tolerant state machine used by all orchestrators. Default backend: `pgcache.StateCache` / `pgcache.AsyncStateCache` via `PROCESSING_CACHE`. `clscache.ClassificationCache` backs `CLUSTERING` (`CLASSIFICATION_CACHE`). Alternate backends live under `extensions/` (sqlite, surreal, pg+cls). State flow: `processingcache/STATEMACHINE.md`.

Tracks per-object processing states (`beans`, `publishers`, `chatters`, `composites`).

### `utils/`

Shared helpers used across workers and entrypoints: logging, dates/ids, field constants, env loading (`utils.env.load_coffeemaker_env` → `.env` only).

### [`nlp/`](nlp/README.md)

Embeddings (`create_embedder`), structured extraction (`create_text_analyst` / `Digest`, `Briefing`), NER (`EntityExtractor` / `Entities`). Backends: local HF, vLLM, ONNX, llama.cpp, Infinity, remote OpenAI-compatible APIs. The `openvino://` prefix still routes to `OVEmbeddings`, but that backend raises (`optimum-intel` removed). Details: [nlp/README.md](nlp/README.md).

### [`pybeansack/`](pybeansack/README.md)

Pydantic models (`Bean`, `Chatter`, `Publisher`) and storage: `create_client("pg"|"lance"|"duck"|"dl", ...)`. See [pybeansack/README.md](pybeansack/README.md) for backends, queries, and tests.

### `pycupboard/`

`Sip`, `Source` models; production porter uses `pycupboard.pgcupboard.Cupboard`.

### `factory/`

Operational config and ops scripts: feed lists, parquet label indexes, migrations, ThunderCompute boot, Salad recipes. Not runtime library code. Old TensorDock/Azure helpers live under `factory/deprecated/`.

## How to use

### Prerequisites

- Python 3.10+ locally (`DockerfileIO` is 3.13; `DockerfileGPU` is `vllm/vllm-openai:latest`)
- `.env` at repo root for secrets and connection strings
- Model paths for analyzer modes (`EMBEDDER_PATH`, `EXTRACTOR_PATH`, `DIGESTOR_PATH`, `CONSOLIDATOR_PATH`) — set in `.env`, or rely on `run_pipeline.sh` defaults when using that script
- `PROCESSING_CACHE` — state DB connection (default: PostgreSQL via `processingcache/pgcache.py`; see `extensions/` for sqlite, surreal, pg+cls)
- For `PORTER`: `BEANSACK_CONNECTION_STRING`, `CUPBOARD_CONNECTION_STRING`

### Install (local)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt   # or requirements-io.txt for collector/porter/remote NLP
# DockerfileGPU uses requirements-gpu.txt on top of the vLLM base image
# optional: pip install -r requirements-dev.txt
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

`run.py` and factory scripts load `.env` only. `run_pipeline.sh` applies its built-in defaults, then sources `.env`.

| Variable | Used by |
|----------|---------|
| `MODE` | Worker selection (if not passed as `--mode`) |
| `BATCH_SIZE` | Batch size for all modes |
| `PROCESSING_CACHE` | State machine connection |
| `COLLECTOR_SOURCES` | Path to feeds YAML (default: `factory/feeds.yaml`) |
| `EMBEDDER_PATH`, `EMBEDDER_CONTEXT_LEN` | EMBEDDER |
| `EXTRACTOR_PATH`, `EXTRACTOR_CONTEXT_LEN` | EXTRACTOR |
| `DIGESTOR_PATH`, `DIGESTOR_CONTEXT_LEN`, `DIGESTOR_BASE_URL`, `DIGESTOR_API_KEY` | DIGESTOR (remote LLM optional) |
| `CONSOLIDATOR_PATH`, `CONSOLIDATOR_BASE_URL`, `CONSOLIDATOR_API_KEY` | CONSOLIDATOR (remote LLM optional) |
| `CLASSIFICATION_CACHE` | Vector store for `CLUSTERING` (`processingcache/clscache.py`, zvec) |
| `BEANSACK_CONNECTION_STRING` | PORTER |
| `CUPBOARD_CONNECTION_STRING` | PORTER |
| `LOG_DIR` | Optional hourly logfmt log file; otherwise logfmt to stderr |
| `WORDS_THRESHOLD_FOR_STORING` | Min words before full scrape (collector) |
| `COMPLETION_WEBHOOK_URL` | `run_pipeline.sh` POST on exit (optional `COMPLETION_WEBHOOK_API_KEY`) |

`run_pipeline.sh` defaults (override in `.env`):

```
EMBEDDER_PATH=codefuse-ai/F2LLM-v2-80M
EMBEDDER_CONTEXT_LEN=8192
EXTRACTOR_PATH=knowledgator/modern-gliner-bi-base-v1.0
EXTRACTOR_CONTEXT_LEN=2048
DIGESTOR_PATH=vllm://LiquidAI/LFM2.5-2.6B
DIGESTOR_CONTEXT_LEN=9216
DIGESTOR_TEMPERATURE=0.1
DIGESTOR_TOP_K=50
DIGESTOR_REPETITION_PENALTY=1.1
CONSOLIDATOR_PATH=vllm://nvidia/NVIDIA-Nemotron-3-Nano-4B-BF16
CONSOLIDATOR_CONTEXT_LEN=16384
CONSOLIDATOR_TEMPERATURE=0.7
CONSOLIDATOR_TOP_P=0.95
CONSOLIDATOR_REPETITION_PENALTY=1.25
```

Digestor also reads `DIGESTOR_TOP_P` and `DIGESTOR_PRESENCE_PENALTY` when set. Consolidator also reads `CONSOLIDATOR_TOP_K`.

### Processing states (beans)

```
collected
   ├─ embedded  (embedding + categories + sentiments)
   │     └─ clustered  (related urls)
   ├─ extracted
   └─ digested
        └─ consolidated  (beans) + composites.collected
             └─ beansacked / cupboarded
```

Analyzer branches after `collected` are independent until porter or consolidator merge them.

## How to deploy

### Docker images

| File | Use | Entrypoint |
|------|-----|------------|
| `DockerfileGPU` | `vllm/vllm-openai:latest`; analyzer / GPU LLM (`requirements-gpu.txt`) | `run_pipeline.sh` (stage flags) |
| `DockerfileIO` | Slim Python 3.13; IO-bound stages | `run_pipeline.sh` (stage flags) |

Build example (Nemotron is typically gated; pass a Hub token):

```bash
docker build -f DockerfileGPU --build-arg HF_TOKEN="$HF_TOKEN" -t coffeemaker:gpu .
docker build -f DockerfileIO -t coffeemaker:io .
```

GPU run (pipeline flags forwarded to `run_pipeline.sh`; Hub weights are baked into `HF_HOME`):

```bash
docker run --gpus all --ipc=host --shm-size=8g --env-file .env \
  coffeemaker:gpu --embedder 64 --clustering 128 --extractor 32

docker run --gpus all --ipc=host --shm-size=8g --env-file .env \
  coffeemaker:gpu --digestor 160 --consolidator 128
```

IO run:

```bash
docker run --env-file .env coffeemaker:io --collector 64 --porter 512
```

### Docker Compose (local dev)

```bash
docker compose up pgcache localmongo localpostgres   # infra only
```

This compose file is a **legacy** stack: Mongo-backed worker images (`MODE=INDEXER`, `--mode COMPOSER`), `localcrawler` (crawl4ai), Azurite, n8n. It is not the current `PROCESSING_CACHE` + `run.py` modes. Prefer `pgcache` (pgvector) plus a local `.env` and `run.py` / `run_pipeline.sh`.

### Fly.io (collector / porter)

IO jobs: `fly.collector.toml` (`--collector 64`) and `fly.porter.toml` (`--porter 512`). Deploy with `flyctl deploy --config ./fly.collector.toml` (and the porter config). CI: `.github/workflows/fly-deploy.yml`.

### ThunderCompute boot pipeline

On [ThunderCompute](https://www.thundercompute.com/) GPU VMs, register a one-shot s6 service so `factory/thundercompute-s6-tasks.sh` starts `run_pipeline.sh` after boot.

**One-time setup** (from the repo checkout, typically `/home/ubuntu/pycoffeemaker`):

```bash
cd /home/ubuntu/pycoffeemaker
sudo ./factory/install-thundercompute-s6-tasks.sh
```

The installer takes **no arguments**. Stages and batch sizes are hardcoded in `factory/thundercompute-s6-tasks.sh`:

| Stage | Batch |
|-------|-------|
| embedder | 80 |
| extractor | 40 |
| clustering | 256 |
| digestor | 192 |
| consolidator | 128 |

Collector is commented out in that script. Change stages by editing `thundercompute-s6-tasks.sh`, then reinstall.

**What gets installed:**

- s6 oneshot `thundercompute-tasks` (depends on `sshd`, runs `factory/thundercompute-s6-tasks.sh` at boot)
- Pipeline log: `/home/ubuntu/.logs/pipeline-YYYY-MM-DD-HH-MM-SS.log`
- Boot exports `PROCESSING_WINDOW=3`

**Manual run** (without reboot):

```bash
./factory/thundercompute-s6-tasks.sh
# or
./run_pipeline.sh --embedder 80 --extractor 40 --clustering 256 --digestor 192 --consolidator 128
```

Ensure `.env` is configured (`PROCESSING_CACHE`, secrets) before boot. Model defaults come from `run_pipeline.sh` unless overridden in `.env`.

Deprecated TensorDock/Azure helpers: `factory/deprecated/machine_ops.py`.

### Production notes

- Run **one mode per container/process** (`run.py --mode`); or one `run_pipeline.sh` invocation per GPU host.
- GPU nodes: `DockerfileGPU` + `run_pipeline.sh` stage flags (`--embedder`, `--clustering`, `--extractor`, `--digestor`, `--consolidator`).
- IO nodes: `DockerfileIO` + `--collector` / `--porter` (Fly jobs use this).
- Keep `PROCESSING_CACHE` (state DB) and downstream DBs reachable from every worker tier.

## Package documentation

| Package | README | Topics |
|---------|--------|--------|
| **datacollectors** | [datacollectors/README.md](datacollectors/README.md) | RSS/API/scrape collectors, field normalization, `APICollectorAsync`, `AsyncWebScraper`, crawl4ai |
| **nlp** | [nlp/README.md](nlp/README.md) | Embeddings, text analysts, NER, model backends (`create_embedder`, `create_text_analyst`, `EntityExtractor`) |
| **pybeansack** | [pybeansack/README.md](pybeansack/README.md) | Bean/Chatter/Publisher models, PG/Lance/Duck/DuckLake backends, queries, porting |

## Further reading

- [`AGENTS.md`](AGENTS.md) — design overview for agents/tools
- [`processingcache/STATEMACHINE.md`](processingcache/STATEMACHINE.md) — state machine schema and worker read/write flow
