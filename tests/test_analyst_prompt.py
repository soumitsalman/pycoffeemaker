from pydantic import BaseModel

from nlp.analysts import PROMPT_TOKEN_MARGIN, TextAnalystBase


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
        enable_thinking=False,
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
        add_generation_prompt=True,
        enable_thinking=False,
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
    assert captured["enable_thinking"] is True
    assert captured["max_new_tokens"] == 2048
    assert "{description}" not in captured["input_template"]


class _VocabTokenizer:
    def __init__(self, added, rendered=""):
        self._added = added
        self._rendered = rendered

    def get_vocab(self):
        return dict(self._added)

    def get_added_vocab(self):
        return dict(self._added)

    def apply_chat_template(self, messages, tokenize=False, add_generation_prompt=True, enable_thinking=True):
        return self._rendered


def test_reasoning_delimiters_come_from_tokenizer_vocab():
    standard = _analyst(
        _VocabTokenizer({"<think>": 1, "</think>": 2, "<|im_end|>": 3}),
        enable_thinking=True,
        context_len=400,
        max_new_tokens=32,
    )
    assert standard._reasoning_delimiters == ("<think>", "</think>")

    custom = _analyst(
        _VocabTokenizer({"<|think|>": 1, "<|/think|>": 2}),
        enable_thinking=True,
        context_len=400,
        max_new_tokens=32,
    )
    assert custom._reasoning_delimiters == ("<|think|>", "<|/think|>")


def test_thinking_reserve_is_zero_when_disabled():
    off = _analyst(None, enable_thinking=False, context_len=16384, max_new_tokens=2048)
    on = _analyst(None, enable_thinking=True, context_len=16384, max_new_tokens=2048)
    assert off.max_thinking_budget == 0
    assert on.max_thinking_budget == 2048
    assert off.input_token_budget == 16384 - 2048 - PROMPT_TOKEN_MARGIN
    assert off.input_token_budget > on.input_token_budget
