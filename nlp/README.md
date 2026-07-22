# NLP — Embeddings & Text Analysts

> **Canonical location:** this package lives in the [pycoffeemaker](https://github.com/soumitsalman/pycoffeemaker) monorepo at `nlp/`. The standalone GitHub repo is archived.

Lightweight NLP utilities for [Pycoffeemaker](../README.md):

- **Embeddings** — vectorize text for retrieval and semantic search
- **Text analysts** — structured extraction (digest/briefing fields) via Pydantic schemas
- **Named entities** — GLiNER-based extraction (`EntityExtractor` → `Entities`)
- **Tag helpers** — normalize/merge tag lists used by workers

Public API (see `__init__.py`): `create_embedder`, `create_text_analyst`, `Digest`, `Briefing`, `Entities`, `EntityExtractor`, `normalize_tags` / `merge_tags` / `merge_lists`, and selected backend classes.

## Package layout

```
nlp/
├── __init__.py          # public exports
├── embedders.py         # EmbedderBase + backends; create_embedder()
├── analysts.py          # TextAnalystBase + backends; create_text_analyst()
├── extractors.py        # EntityExtractor (GLiNER → Entities)
├── models.py            # Entities, Digest, Briefing, domain-specific schemas
├── normalize.py         # normalize_tags, merge_tags, merge_lists, field cleanup
├── formatters.py        # schema/text formatting helpers for analysts
└── runtime.py           # model-path prefixes, run_batch, GPU helpers

Integration tests live in the repo-root `tests/` directory (`test_nlp.py`, fixtures in `texts-for-nlp.json`).
```

## Installation

From the repo root (so `nlp` is on `PYTHONPATH`):

```bash
pip install -r requirements.txt
# IO-only hosts can use requirements-io.txt instead
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

### Structured extraction (text analyst)

```python
from nlp import create_text_analyst, Digest

article = "Long article text to summarize and extract intelligence from..."

analyst = create_text_analyst(
    model_path="LiquidAI/LFM2.5-1.2B-Instruct",
    context_len=32768,
    instruction="Extract structured intelligence per the schema.",
    input_template="{msg}",
    output_model=Digest,
)

with analyst:
    results = analyst.run_batch([article])

digest = results[0]
print(digest.model_dump())
```

#### Batching

```python
with analyst:
    digests = analyst.run_batch([article, article])  # list[Digest | None]
```

### Named entity extraction

```python
from nlp import EntityExtractor

with EntityExtractor(
    "knowledgator/modern-gliner-bi-base-v1.0",
    context_len=4096,
    threshold=0.4,
) as extractor:
    entities = extractor.run_batch([article])  # list[Entities | None]
```

## Backend selection

`create_embedder` / `create_text_analyst` pick a backend from the model path (and optional remote credentials).

| Prefix / signal | Backend | Use case |
|-----------------|---------|----------|
| (none) | `TransformerEmbeddings` / `TransformerTextAnalyst` | HuggingFace Hub or local path |
| `onnx://` | `ORTEmbeddings` | ONNX Runtime (embeddings only) |
| `openvino://` | `OVEmbeddings` | OpenVINO (embeddings only) |
| `llamacpp://` | `LlamaCppEmbeddings` | llama.cpp GGUF (embeddings only) |
| `vllm://` | `VLLMEmbedder` / `VLLMTextAnalyst` | vLLM batched inference |
| `infinity://` | `InfinityEmbeddings` | `infinity_emb` in-process embeddings |
| `base_url` + `api_key` (kwargs) | `RemoteEmbeddings` / `RemoteTextAnalyst` | OpenAI-compatible HTTP API |

Prefix constants live in `runtime.py`.

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

# Remote text analyst (requires both base_url and api_key)
create_text_analyst(
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

**`create_text_analyst(model_path, context_len=32768, instruction=None, input_template=None, output_model=Digest, enable_thinking=False, max_new_tokens=2048, **kwargs)`** → `TextAnalystBase`

- `run_batch(list[str])` → `list[BaseModel | None]` (type depends on `output_model`)
- Remote backend: pass `base_url` and `api_key` in `kwargs`
- `vllm://` prefix selects `VLLMTextAnalyst`
- Use as context manager: `with analyst:`

**`EntityExtractor(model_path, context_len=4096, threshold=0.5)`** — separate from text analysts; maps GLiNER labels into `Entities` via `run_batch`.

## Return types

- Embeddings: `list[float]` or `list[list[float]]`
- Text analysts: Pydantic models (`Digest`, `Briefing`, or domain subclasses in `models.py`) when `output_model` is set; `None` if parsing fails
- NER: `Entities` (people, companies, regions, products, stock_tickers)
- Legacy markdown/compressed parsers: `parse_markdown`, `parse_compressed` in `analysts.py` (also used internally when `output_model` is unset)

## Coffeemaker integration

Workers wire this package into the pipeline:

| Worker mode | Module | NLP API |
|-------------|--------|---------|
| `EMBEDDER` | `workers/analyzerorch.py` | `create_embedder` |
| `EXTRACTOR` | `workers/analyzerorch.py` | `EntityExtractor` |
| `DIGESTOR` | `workers/analyzerorch.py` | `create_text_analyst` + `Digest` |
| `CONSOLIDATOR` | `workers/consolidatororch.py` | `create_text_analyst` + `Briefing` |

## Implementation notes

- Embedder backends: `embedders.py` (`RemoteEmbeddings`, `LlamaCppEmbeddings`, `TransformerEmbeddings`, `OVEmbeddings`, `ORTEmbeddings`, `VLLMEmbedder`, `InfinityEmbeddings`)
- Text-analyst backends: `analysts.py` (`TransformerTextAnalyst`, `VLLMTextAnalyst`, `RemoteTextAnalyst`)
- NER: `extractors.py` (`EntityExtractor`)
- Schemas: `models.py` (`Entities`, `Digest`, `Briefing`, `AINewsDigest`, `FinancialMarketsNewsSummary`, …)
- Tag/list helpers: `normalize.py` (`normalize_tags`, `merge_tags`, `merge_lists`)

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
