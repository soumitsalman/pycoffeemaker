import json
import os
from pathlib import Path

import numpy as np
import pytest
from pydantic import BaseModel

from nlp.analysts import PROMPT_TOKEN_MARGIN, TextAnalystBase, create_text_analyst
from nlp.llamacppanalyst import LlamaCppTextAnalyst, parse_gguf_source, select_gguf
from nlp.onnxanalyst import (
    ONNXTextAnalyst,
    _cache_input_name,
    _empty_onnx_cache,
    _sample_token,
    onnx_precision_file,
)
from nlp.runtime import LLAMACPP_PREFIX, ONNX_PREFIX, VLLM_PREFIX

ROOT = Path(__file__).resolve().parent.parent
TEXTS_FOR_NLP = Path(__file__).resolve().parent / "texts-for-nlp.json"
_LOCAL_GGUF = ROOT / ".models" / "LFM3-1.7B-Q4_K_M.gguf"

GGUF_NAMES = [
    "LFM3-1.7B-Q4_K_M.gguf",
    "LFM3-1.7B-QAD-Q4_0.gguf",
    "LFM3-1.7B-Q8_0.gguf",
    "LFM3-1.7B-NVFP4.gguf",
]


def digestor_model_paths() -> list[str]:
    """One model when DIGESTOR_TEST_PATH is set, otherwise the four backends."""
    chosen = os.getenv("DIGESTOR_TEST_PATH")
    if chosen:
        return [chosen]
    llamacpp = f"llamacpp://{_LOCAL_GGUF}" if _LOCAL_GGUF.is_file() else "llamacpp://LiquidAI/LFM3-1.7B-GGUF"
    return [
        llamacpp,
        "onnx://LiquidAI/LFM3-1.7B-ONNX",
        "vllm://LiquidAI/LFM2.5-1.2B-Instruct",
        "LiquidAI/LFM2.5-1.2B-Instruct",
    ]


def _backend_id(model_path: str) -> str:
    if model_path.startswith(LLAMACPP_PREFIX):
        return "llamacpp"
    if model_path.startswith(ONNX_PREFIX):
        return "onnx"
    if model_path.startswith(VLLM_PREFIX):
        return "vllm"
    return "transformer"


def test_llamacpp_prefix_selects_gguf_analyst():
    analyst = create_text_analyst(
        "llamacpp://LiquidAI/LFM3-1.7B-GGUF:Q8_0",
        context_len=8192,
        output_model=None,
        temperature=0.1,
        top_k=50,
        repetition_penalty=1.1,
    )
    assert isinstance(analyst, LlamaCppTextAnalyst)
    assert analyst.model_name == "LiquidAI/LFM3-1.7B-GGUF:Q8_0"
    assert parse_gguf_source(analyst.model_name) == ("LiquidAI/LFM3-1.7B-GGUF", None, "Q8_0")
    assert select_gguf(GGUF_NAMES, "Q4_K_M", None) == "LFM3-1.7B-Q4_K_M.gguf"
    assert select_gguf(GGUF_NAMES, "Q4_0", None) == "LFM3-1.7B-QAD-Q4_0.gguf"
    assert select_gguf(GGUF_NAMES, "NVFP4", None) == "LFM3-1.7B-NVFP4.gguf"
    sampled = analyst._sampling_kwargs()
    assert sampled["temperature"] == 0.1
    assert sampled["top_k"] == 50
    assert sampled["repeat_penalty"] == 1.1

    greedy = create_text_analyst(
        "llamacpp://LiquidAI/LFM3-1.7B-GGUF",
        context_len=128,
        output_model=None,
        max_new_tokens=8,
    )
    assert greedy._sampling_kwargs()["temperature"] == 0.0
    assert greedy._sampling_kwargs()["max_tokens"] == 8


class _Llama:
    def __init__(self):
        self.calls = []
        self.resets = 0
        self.chat_handler = self._handle
        self.chat_format = "chat_template.default"
        self._chat_handlers = {"chat_template.default": self._handle}

    def reset(self):
        self.resets += 1

    def _handle(self, **kwargs):
        self.calls.append(kwargs)
        return {"choices": [{"message": {"content": "A"}}]}


class _LlamaTokenizer:
    def apply_chat_template(self, messages, tokenize=True, **kwargs):
        if tokenize:
            return [1]
        return messages[-1]["content"]

    def encode(self, text, add_special_tokens=False):
        return [ord(ch) for ch in text]

    def decode(self, ids, skip_special_tokens=True):
        return "".join(chr(int(i)) for i in ids)


def test_run_batch_uses_embedded_chat_handler():
    analyst = create_text_analyst(
        "llamacpp://LiquidAI/LFM3-1.7B-GGUF",
        context_len=128,
        output_model=None,
        max_new_tokens=4,
        enable_thinking=False,
    )
    model = _Llama()
    analyst._llm = model
    analyst._tokenizer = _LlamaTokenizer()

    assert analyst.run_batch([]) == []
    assert analyst.run_batch(["ab"]) == ["A"]
    assert model.resets == 1
    call = model.calls[0]
    assert call["enable_thinking"] is False
    assert call["temperature"] == 0.0
    assert call["messages"][-1]["content"] == "ab"
    assert call["response_format"] is None


class _Tensor:
    def __init__(self, name, shape, type):
        self.name = name
        self.shape = shape
        self.type = type


def test_onnx_prefix_selects_onnx_analyst():
    analyst = create_text_analyst(
        "onnx://LiquidAI/LFM3-1.7B-ONNX",
        context_len=8192,
        precision="q4",
        temperature=0.1,
        top_k=50,
        repetition_penalty=1.1,
    )
    assert isinstance(analyst, ONNXTextAnalyst)
    assert analyst.model_name == "LiquidAI/LFM3-1.7B-ONNX"
    assert analyst.precision == "q4"
    assert analyst.sampling_params["temperature"] == 0.1
    assert analyst.sampling_params["top_k"] == 50
    assert onnx_precision_file("q4f16") == "onnx/model_q4f16.onnx"


def test_empty_cache_matches_lfm_graph_axes():
    inputs = [
        _Tensor("input_ids", ["batch_size", "sequence_length"], "tensor(int64)"),
        _Tensor("attention_mask", ["batch_size", "total_sequence_length"], "tensor(int64)"),
        _Tensor("position_ids", ["batch_size", "sequence_length"], "tensor(int64)"),
        _Tensor("logits_to_keep", [1], "tensor(int64)"),
        _Tensor("past_conv.0", ["batch_size", 2048, 7], "tensor(float16)"),
        _Tensor("past_key_values.2.key", ["batch_size", 4, "past_sequence_length", 128], "tensor(float16)"),
        _Tensor("past_key_values.2.value", ["batch_size", 4, "past_sequence_length", 128], "tensor(float16)"),
    ]
    cache = _empty_onnx_cache(inputs)
    assert set(cache) == {"past_conv.0", "past_key_values.2.key", "past_key_values.2.value"}
    assert cache["past_conv.0"].shape == (1, 2048, 7)
    assert cache["past_conv.0"].dtype == np.float16
    assert cache["past_key_values.2.key"].shape == (1, 4, 0, 128)
    assert _cache_input_name("present_conv.0") == "past_conv.0"
    assert _cache_input_name("present.2.key") == "past_key_values.2.key"


def test_sample_token_greedy_and_repetition_penalty():
    assert _sample_token(np.array([0.1, 2.0, 0.3]), [], {}) == 1
    penalized = _sample_token(np.array([1.0, 3.0]), [1], {"repetition_penalty": 10})
    assert penalized == 0


class _OnnxTokenizer:
    eos_token_id = 99
    pad_token = "<pad>"

    def apply_chat_template(self, messages, tokenize=False, **kwargs):
        return messages[-1]["content"]

    def encode(self, text, add_special_tokens=False):
        return [ord(ch) for ch in text]

    def decode(self, ids, skip_special_tokens=True):
        return "".join(chr(int(i)) for i in ids)


class _Session:
    def __init__(self):
        self.feeds = []

    def get_inputs(self):
        return [
            _Tensor("input_ids", ["batch_size", "sequence_length"], "tensor(int64)"),
            _Tensor("attention_mask", ["batch_size", "total_sequence_length"], "tensor(int64)"),
            _Tensor("position_ids", ["batch_size", "sequence_length"], "tensor(int64)"),
            _Tensor("logits_to_keep", [1], "tensor(int64)"),
            _Tensor("past_conv.0", ["batch_size", 4, 7], "tensor(float16)"),
            _Tensor("past_key_values.2.key", ["batch_size", 2, "past_sequence_length", 8], "tensor(float16)"),
            _Tensor("past_key_values.2.value", ["batch_size", 2, "past_sequence_length", 8], "tensor(float16)"),
        ]

    def get_outputs(self):
        return [
            _Tensor("logits", None, None),
            _Tensor("present_conv.0", None, None),
            _Tensor("present.2.key", None, None),
            _Tensor("present.2.value", None, None),
        ]

    def run(self, names, feed):
        self.feeds.append({key: value.copy() for key, value in feed.items()})
        step = len(self.feeds)
        vocab = np.full((1, 1, 128), -1e9, dtype=np.float32)
        vocab[0, 0, 65 if step == 1 else 99] = 5
        past = feed["past_key_values.2.key"].shape[2]
        new_len = past + feed["input_ids"].shape[1]
        key = np.ones((1, 2, new_len, 8), dtype=np.float16)
        return [vocab, np.ones((1, 4, 7), dtype=np.float16), key, key.copy()]


def test_run_batch_decodes_one_sequence_and_updates_cache():
    analyst = create_text_analyst(
        "onnx://LiquidAI/LFM3-1.7B-ONNX",
        context_len=128,
        output_model=None,
        max_new_tokens=4,
    )
    session = _Session()
    analyst._llm = session
    analyst._tokenizer = _OnnxTokenizer()
    analyst._input_names = {inp.name for inp in session.get_inputs()}
    analyst._logits_to_keep = np.ones((1,), dtype=np.int64)

    assert analyst.run_batch([]) == []
    assert analyst.run_batch(["ab"]) == ["A"]
    first, second = session.feeds
    assert first["input_ids"].tolist() == [[97, 98]]
    assert first["position_ids"].tolist() == [[0, 1]]
    assert first["attention_mask"].shape == (1, 2)
    assert first["logits_to_keep"].tolist() == [1]
    assert first["past_key_values.2.key"].shape == (1, 2, 0, 8)
    assert second["input_ids"].tolist() == [[65]]
    assert second["position_ids"].tolist() == [[2]]
    assert second["attention_mask"].shape == (1, 3)
    assert second["past_key_values.2.key"].shape == (1, 2, 2, 8)
    assert second["past_conv.0"].shape == (1, 4, 7)


class _Schema(BaseModel):
    @classmethod
    def model_text_schema(cls):
        return "briefing=short"


class _FakeTokenizer:
    """One token per character, plus a fixed chat-template overhead."""

    def encode(self, text, add_special_tokens=False):
        return [ord(ch) for ch in text]

    def decode(self, ids, skip_special_tokens=True):
        return "".join(chr(i) for i in ids)

    def apply_chat_template(
        self,
        messages,
        tokenize=True,
        add_generation_prompt=True,
        continue_final_message=False,
        enable_thinking=False,
        **kwargs,
    ):
        ids = []
        for message in messages:
            ids.extend((1, 2, 3))
            ids.extend(self.encode(message["content"]))
        if add_generation_prompt:
            ids.extend((4, 5))
        return ids


class _Analyst(TextAnalystBase):
    def __init__(self, tokenizer=None, **kwargs):
        self._tok = tokenizer
        super().__init__(**kwargs)

    def __enter__(self):
        return self

    def run_batch(self, input_messages, output_model=None):
        return []

    def _prompt_tokenizer(self):
        return self._tok


def _analyst(tokenizer, *, enable_thinking, context_len, max_new_tokens, instruction="SYS"):
    return _Analyst(
        tokenizer,
        model_name="fake",
        context_len=context_len,
        instruction=instruction,
        input_template="CONTENT=\n{input_text}",
        output_model=_Schema,
        enable_thinking=enable_thinking,
        max_new_tokens=max_new_tokens,
    )


def test_short_article_is_kept_with_system_prompt():
    analyst = _analyst(
        _FakeTokenizer(),
        enable_thinking=False,
        context_len=400,
        max_new_tokens=32,
    )
    prompt = analyst.create_prompt("hello")
    assert prompt[0] == {"role": "system", "content": "SYS"}
    assert "hello" in prompt[1]["content"]
    assert prompt[1]["content"].endswith("hello")
    assert "<|im_start|>" not in prompt[1]["content"]


def test_long_article_fits_prompt_budget():
    tokenizer = _FakeTokenizer()
    analyst = _analyst(
        tokenizer,
        enable_thinking=False,
        context_len=400,
        max_new_tokens=32,
    )
    prompt = analyst.create_prompt("a" * 500)
    rendered = tokenizer.apply_chat_template(
        prompt,
        tokenize=True,
        **analyst._generation_kwargs(),
    )
    assert len(rendered) <= (
        analyst.context_len
        - analyst.max_new_tokens
        - analyst.max_thinking_budget
        - PROMPT_TOKEN_MARGIN
    )
    assert len(prompt[1]["content"]) < len("CONTENT=\n") + 500
    assert prompt[0]["content"] == "SYS"


def test_digest_template_omits_text_schema():
    from workers.analyzerorch import DIGEST_INST, DIGEST_SYS

    assert "{description}" not in DIGEST_INST
    assert "{input_text}" in DIGEST_INST
    assert "TARGET_INFORMATION" not in DIGEST_SYS
    assert "CONTENT_TO_ANALYZE" in DIGEST_SYS


def test_digestor_enables_thinking(monkeypatch):
    captured = {}

    def fake_create(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("workers.analyzerorch.create_text_analyst", fake_create)
    from workers.analyzerorch import Digestor

    Digestor(cache=None, model_path="fake", context_len=16384, batch_size=1)
    assert captured["enable_thinking"] is False
    assert captured["max_new_tokens"] == 2048
    assert "{description}" not in captured["input_template"]


class _Brief(BaseModel):
    briefing: str


def test_parse_output_keeps_trailing_json_object():
    analyst = _analyst(
        None,
        enable_thinking=True,
        context_len=400,
        max_new_tokens=32,
    )
    parsed = analyst.parse_output(
        '<|think|>draft {"a": 1}<|/think|>\n{"briefing": "ok"}',
        _Brief,
    )
    assert parsed.briefing == "ok"
    parsed = analyst.parse_output('```json\n{"briefing": "fenced"}\n```', _Brief)
    assert parsed.briefing == "fenced"


def test_thinking_on_has_no_prefill():
    analyst = _analyst(
        None,
        enable_thinking=True,
        context_len=8192,
        max_new_tokens=32,
    )
    prompt = analyst.create_prompt("hello")
    assert [message["role"] for message in prompt] == ["system", "user"]


def test_thinking_reserve_stays_in_the_prompt_window():
    off = _analyst(None, enable_thinking=False, context_len=16384, max_new_tokens=2048)
    on = _analyst(None, enable_thinking=True, context_len=16384, max_new_tokens=2048)
    assert off.max_thinking_budget == 2048
    assert on.max_thinking_budget == 2048
    assert off.input_token_budget == 16384 - 2048 - 2048 - PROMPT_TOKEN_MARGIN
    assert off.input_token_budget == on.input_token_budget


def _digestor_sampling() -> dict:
    sampling = {}
    if temperature := os.getenv("DIGESTOR_TEMPERATURE"):
        sampling["temperature"] = float(temperature)
    if top_p := os.getenv("DIGESTOR_TOP_P"):
        sampling["top_p"] = float(top_p)
    if top_k := os.getenv("DIGESTOR_TOP_K"):
        sampling["top_k"] = int(top_k)
    if repetition_penalty := os.getenv("DIGESTOR_REPETITION_PENALTY"):
        sampling["repetition_penalty"] = float(repetition_penalty)
    if presence_penalty := os.getenv("DIGESTOR_PRESENCE_PENALTY"):
        sampling["presence_penalty"] = float(presence_penalty)
    return sampling


@pytest.mark.integration
@pytest.mark.parametrize(
    "model_path",
    [pytest.param(path, id=_backend_id(path)) for path in digestor_model_paths()],
)
def test_digestor_texts(model_path):
    """Print one digest per texts-for-nlp.json row.

    Set DIGESTOR_TEST_PATH to run one model. Unset, this runs llamacpp, onnx,
    vllm, and transformer. Sampling and context come from DIGESTOR_* in .env.
    """
    from utils import now_str
    from utils.fields import KIND
    from workers.analyzerorch import DIGEST_INST, DIGEST_MODEL_BY_KIND, DIGEST_SYS, NewsDigest

    context_len = int(os.getenv("DIGESTOR_CONTEXT_LEN", 32768))
    articles = json.loads(TEXTS_FOR_NLP.read_text())
    total = len(articles)
    backend = _backend_id(model_path)
    print(f"\n##### {backend} {model_path}", flush=True)

    with create_text_analyst(
        model_path,
        context_len=context_len,
        instruction=DIGEST_SYS,
        input_template=f"SYSTEM_DATE={now_str()}\n" + DIGEST_INST,
        output_model=NewsDigest,
        enable_thinking=False,
        max_new_tokens=2048,
        **_digestor_sampling(),
    ) as analyst:
        for index, article in enumerate(articles, start=1):
            kind = article.get(KIND)
            output_model = DIGEST_MODEL_BY_KIND.get(kind, NewsDigest)
            digest = analyst.run_batch([article.get("content") or ""], output_model=[output_model])[0]
            payload = digest.model_dump(mode="json") if digest is not None else None
            print(
                f"\n===== [{backend} {index}/{total}] {kind} | {article.get('title')}\n"
                f"{article.get('url')}\n"
                f"{json.dumps(payload, indent=2, ensure_ascii=False)}",
                flush=True,
            )


if __name__ == "__main__":
    for path in digestor_model_paths():
        test_digestor_texts(path)
