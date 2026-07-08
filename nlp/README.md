# NLP — Embeddings & Micro-Agents

> **Canonical location:** this package lives in the [pycoffeemaker](https://github.com/soumitsalman/pycoffeemaker) monorepo at `nlp/`. The standalone GitHub repo is archived.

Lightweight NLP utilities for [Pycoffeemaker](../README.md):

- **Embeddings** — vectorize text for retrieval and semantic search
- **Micro-agents** — structured extraction (entities, events, briefing fields) via Pydantic schemas
- **Named entities** — GLiNER-based extraction (`EntityExtractor`)

Public API (see `__init__.py`): `create_embedder`, `create_micro_agent`, `Digest`, `Briefing`, `EntityExtractor`, and selected backend classes.

## Package layout

```
nlp/
├── __init__.py          # exports: create_embedder, create_text_analyst, Digest, …
├── embedders.py         # EmbedderBase + backends; create_embedder()
├── analysts.py          # TextAnalystBase + backends; create_text_analyst(); EntityExtractor
├── models.py           # Digest, Briefing, domain-specific digest schemas
├── runtime.py           # model-path prefixes, run_batch, GPU helpers
├── requirements.txt
└── deprecated/          # legacy digestors, prompts (not used by current API)

Integration tests live in the repo-root `tests/` directory (`test_nlp.py`, fixtures in `texts-for-nlp.json`).
```

## Installation

From the repo root (so `nlp` is on `PYTHONPATH`):

```bash
pip install -r nlp/requirements.txt
# or full stack:
pip install -r requirements.txt
```

## Quickstart

### Embedding

```python
from nlp import create_embedder

texts = [
    "AI will change how developers build software.",
    "Open-source models enable local experimentation.",
]

embedder = create_embedder(
    model_path="sentence-transformers/all-MiniLM-L6-v2",
    context_len=512,
)
with embedder:
    vectors = embedder.embed_documents(texts)   # list[list[float]]
    qvec = embedder.embed_query("What will change in developer tooling?")  # list[float]
```

### Structured extraction (micro-agent)

```python
from nlp import create_micro_agent, Digest

article = "Long article text to summarize and extract intelligence from..."

agent = create_micro_agent(
    model_path="LiquidAI/LFM2.5-1.2B-Instruct",
    context_len=32768,
    instruction="Extract structured intelligence per the schema.",
    input_template="{msg}",
    output_model=Digest,
)

with agent:
    results = agent.run_batch([article])

digest = results[0]
print(digest.model_dump())
```

#### Batching

```python
with agent:
    digests = agent.run_batch([article, article])  # list[Digest | None]
```

### Named entity extraction

```python
from nlp import EntityExtractor

with EntityExtractor(
    "knowledgator/modern-gliner-bi-base-v1.0",
    context_len=4096,
    threshold=0.4,
) as extractor:
    entities = extractor.run_batch([article])  # list[Digest | None]
```

## Backend selection

`create_embedder` / `create_micro_agent` pick a backend from the model path (and optional remote credentials).

| Prefix / signal | Backend | Use case |
|-----------------|---------|----------|
| (none) | `TransformerEmbeddings` / `TransformerMicroAgent` | HuggingFace Hub or local path |
| `onnx://` | `ORTEmbeddings` | ONNX Runtime (embeddings only) |
| `openvino://` | `OVEmbeddings` | OpenVINO (embeddings only) |
| `llamacpp://` | `LlamaCppEmbeddings` | llama.cpp GGUF (embeddings only) |
| `vllm://` | `VLLMEmbedder` / `VLLMMicroAgent` | vLLM batched inference |
| `infinity://` | `InfinityEmbeddings` | `infinity_emb` in-process embeddings |
| `base_url` + `api_key` (kwargs) | `RemoteEmbeddings` / `RemoteMicroAgent` | OpenAI-compatible HTTP API |

Prefix constants live in `utils.py`.

Examples:

```python
# HuggingFace (default)
create_embedder("sentence-transformers/all-MiniLM-L6-v2", context_len=512)

# ONNX / OpenVINO / llama.cpp / vLLM / Infinity
create_embedder("onnx://./model.onnx", context_len=512)
create_embedder("openvino://./model_ir.xml", context_len=512)
create_embedder("llamacpp://./model.gguf", context_len=512)
create_embedder("vllm://BAAI/bge-small-en-v1.5", context_len=512)
create_embedder("infinity://BAAI/bge-small-en-v1.5", context_len=512)

# Remote embeddings
create_embedder(
    model_path="text-embedding-3-small",
    context_len=512,
    base_url="https://api.openai.com/v1",
    api_key="sk-...",
)

# Remote micro-agent (requires both base_url and api_key)
create_micro_agent(
    model_path="openai/gpt-oss-20b",
    context_len=32768,
    output_model=Briefing,
    base_url="https://integrate.api.nvidia.com/v1",
    api_key="nvapi-...",
)
```

## API summary

**`create_embedder(model_path, context_len=512, base_url=None, api_key=None)`** → `EmbedderBase`

- `embed_documents(text | list[str])` → `list[float]` or `list[list[float]]`
  - Long inputs are chunked; chunk embeddings are **mean**-pooled per document.
- `embed_query(query)` → `list[float]`
- Use as context manager: `with embedder:`

**`create_micro_agent(model_path, context_len=32768, instruction=None, input_template=None, output_model=Digest, **kwargs)`** → `MicroAgentBase`

- `run_batch(list[str])` → `list[BaseModel | None]` (type depends on `output_model`)
- Remote backend: pass `base_url` and `api_key` in `kwargs`
- `vllm://` prefix selects `VLLMMicroAgent`
- Use as context manager: `with agent:`

**`EntityExtractor(model_path, context_len=4096, threshold=0.5)`** — separate from micro-agents; maps GLiNER labels into `Digest` fields via `run_batch`.

## Return types

- Embeddings: `list[float]` or `list[list[float]]`
- Micro-agents: Pydantic models (`Digest`, `Briefing`, or domain subclasses in `models.py`) when `output_model` is set; `None` if parsing fails
- Legacy markdown/compressed parsers: `parse_markdown`, `parse_compressed` in `agents.py` (also used internally when `output_model` is unset)

## Coffeemaker integration

`workers/analyzerorch.py` wires this package into the pipeline:

| Worker mode | NLP API |
|-------------|---------|
| `EMBEDDER` | `create_embedder` |
| `EXTRACTOR` | `EntityExtractor` |
| `DIGESTOR`, `CONSOLIDATOR` | `create_micro_agent` + `Digest` / `Briefing` |

## Implementation notes

- Embedder backends: `embedders.py` (`RemoteEmbeddings`, `LlamaCppEmbeddings`, `TransformerEmbeddings`, `OVEmbeddings`, `ORTEmbeddings`, `VLLMEmbedder`, `InfinityEmbeddings`)
- Micro-agent backends: `agents.py` (`TransformerMicroAgent`, `VLLMMicroAgent`, `RemoteMicroAgent`)
- NER: `agents.py` (`EntityExtractor`)
- Schemas: `models.py` (`Digest`, `Briefing`, `AINewsDigest`, `FinancialMarketsNewsSummary`, …)

### Tests

```bash
# from repo root (integration tests opt-in via marker)
pytest tests/test_nlp.py -m integration

# run a single smoke test directly
python tests/test_nlp.py
```

Uncomment or invoke `test_digestor`, `test_extractor`, etc. in `tests/test_nlp.py` as needed.

## Contribution

- Keep digests concise and faithful to source tone.
- Tune embedding batch sizes for available GPU memory and backend limits.

---
## APPENDIX: Content Generation Models Evaluation


### Local Models for Summarization, Extraction, Small Scale Reasoning

| Model Category | Recommendation | Notes |
|---|---|---|
| `Qwen/Qwen3.5-4B` | ⭐⭐⭐ Great (Digestor & Consolidator) | Thinker by default. Non-thinking mode is better for Digestor. Great context window 256K |
| `Qwen/Qwen3.5-9B` | ⭐⭐⭐ Great (Digestor & Consolidator) | Thinker by default. Better for Consolidator although slower than 4B, Great context window 256K |
| `LiquidAI/LFM2.5-1.2B-Instruct` | ⭐⭐ Good (Digestor) | Good for non-thinking extraction. Issues: Won't recommend for reasoning, Occational failure in structured output, Small context window 32K |
| `nvidia/NVIDIA-Nemotron-3-Nano-4B-BF16` | ⭐⭐ Very Good (Digestor) | Good output structure adherence, Good context window 128K, Great search tag generation, Slightly worse content quality than Qwen3.5-4B |
| `soumitsr/led-base-article-digestor` (Seq2Seq) | ⭐ Decent (Digestor ONLY) | Better efficiency, control over format. DEPRECATED |
| `soumitsr/SmolLM2-360M-Instruct-article-digestor` (Decoder) | ⭐ Decent (Digestor ONLY) | Better efficiency, control over format, ideal for on-device. DEPRECATED |


### Article & Content Generation

| Model | Rating | Cost | Key Notes |
|-------|--------|------|-----------|
| `o3-mini` / `o4-mini` | ⭐ Excellent | API | Strong instruction following, occasional API edge cases |
| `deepseek-ai/DeepSeek-R1` (or `0528`) | ⭐ Good | – | Well-structured articles, good compliance |
| `microsoft/WizardLM-2-8x22B` | ⭐ Good | – | Solid article generation |
| `NovaSky-AI/Sky-T1-32B-Preview` | ⚠️ Mixed | ~$0.12/M | Underwhelming performance |
| `Sao10K/L3.1-70B-Euryale-v2.2` | ⚠️ Mixed | – | Acceptable but inconsistent instruction following |
| `nvidia/Llama-3.1-Nemotron-70B-Instruct` | ⚠️ Mixed | ~$0.12/M | Poor instruction following for articles |
| `gpt-4.1-nano` / `gpt-4-mini` | ❌ Poor | API | Not suitable for content generation |

### Image Generation

| Model | Rating | Cost | Key Notes |
|-------|--------|------|-----------|
| `black-forest-labs/FLUX-1-schnell` | ⭐ Good | ~$0.0005 | Fast, low cost |
| `black-forest-labs/FLUX-1-dev` | ⭐ Good | ~$0.009 | Better quality |
| `run-diffusion/Juggernaut-Lightning-Flux` | ⭐ Good | ~$0.009 | Strong quality/speed balance |
| `run-diffusion/Juggernaut-Flux` | ⭐ Good | ~$0.009 | Higher quality, slower |
| `stabilityai/sdxl-turbo` | ❌ Poor | ~$0.0002 | Poor quality |
| `stabilityai/sd3.5-medium` | ❌ Poor | ~$0.03 | Mediocre output, high cost |

### Named Entity Extraction
| Model | Rating |
|-------|--------|
| `knowledgator/modern-gliner-bi-base-v1.0` | ⭐ Great | 
