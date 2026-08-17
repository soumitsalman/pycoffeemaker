# Direct `json` / `compact` output modes

## Summary

Remove the registry abstraction. Add a direct `output_format` parameter to `create_text_analyst`:

```python
create_text_analyst(
    ...,
    output_format="json",      # existing default
)
```

Supported values are `"json"` and `"compact"`. The JSON path remains unchanged. The compact path uses a model-derived grammar and inherited `Digest.parse_compact()`.

## Implementation changes

### `nlp/models.py`

Add a reusable inherited parser to `Digest`:

```python
class Digest(_NLPBaseModel):
    @classmethod
    def parse_compact(cls, response: str):
        values = _parse_compact_records(
            response,
            field_map=compact_field_map(cls),
            model_fields=cls.model_fields,
        )
        return cls.model_validate(values)
```

Add `compact_digest_grammar(output_model)` and `compact_field_map(output_model)` helpers. Generate the grammar from `output_model.model_fields`, allowing all current `Digest` subclasses to use the inherited parser.

Compact wire format:

```text
K=key point
K=another key point
D=causal driver
T=event type
I=observed impact
G=impacted domain
L=high
M=macro context
F=future outlook
B=briefing
A=additional subclass field
END
```

Rules: base fields receive stable short tags; subclass fields receive deterministic tags; list fields may repeat; scalar fields may appear once; optional fields may be omitted; required Pydantic fields must appear; values are single-line text; `END` is mandatory; malformed or unknown records fail.

### `nlp/analysts.py`

Change the factory signature:

```python
def create_text_analyst(
    ...,
    output_format: Literal["json", "compact"] = "json",
    **kwargs,
)
```

In `VLLMTextAnalyst.__enter__`:

```python
if self.output_format == "json":
    structured_outputs = StructuredOutputsParams(
        json=self.output_model.model_json_schema()
    )
elif self.output_format == "compact":
    structured_outputs = StructuredOutputsParams(
        grammar=compact_digest_grammar(self.output_model)
    )
```

Route compact responses through `self.output_model.parse_compact(response)`. Reject compact mode for non-vLLM backends. Inject compact tag instructions into the prompt while preserving the existing JSON prompt behavior.

### `workers/analyzerorch.py` and `run.py`

Add an explicit `output_format="json"` argument to `Digestor`, passing it to `create_text_analyst`.

Add the code-level swap point:

```python
DIGESTOR_OUTPUT_FORMAT = "json"
```

Pass it to `Digestor`; changing it to `"compact"` activates the grammar. Leave `run_pipeline.sh` unchanged. Keep `Consolidator` on JSON by default because `Briefing` is not a `Digest` subclass.

### Documentation

Document:

```python
create_text_analyst(
    "vllm://LiquidAI/LFM2.5-2.6B",
    output_model=Digest,
    output_format="compact",
)
```

## Tests

Add focused tests covering JSON defaults, JSON/grammar vLLM parameters, `Digest.parse_compact()` with repeated and optional fields, subclass fields, malformed records, missing required fields, unsupported annotations, non-vLLM rejection, and unchanged legacy/JSON parsing.

```bash
pytest -q tests/test_output_plugins.py
python -m compileall nlp workers run.py
```

## Tradeoffs and assumptions

- Pros: no registry, direct API, backward-compatible JSON default, subclass support, and less generated syntax than JSON.
- Cons: model-dependent grammar generation, deterministic subclass tag mappings, and single-line value handling.
- The compact grammar enforces structural rules; existing natural-language field descriptions remain prompt-level guidance unless represented as actual Pydantic constraints.
