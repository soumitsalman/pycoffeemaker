from itertools import chain

from .runtime import TOKEN_MARGIN


class TextSplitter:
    """Truncate and chunk text to a token context window."""

    def __init__(self, context_len: int, tokenizer=None, margin: int = 0):
        self.context_len = context_len
        self.tokenizer = tokenizer
        self.chunk_size = max(1, context_len - margin)
        self._splitter = None

    def truncate(self, texts: str | list[str]) -> str | list[str]:
        if isinstance(texts, str):
            return self._truncate(texts)
        return self._truncate_batch(texts)

    def _truncate(self, text: str) -> str:
        """Keep the leading ``context_len`` tokens. Unchanged when no tokenizer is set."""
        if not text or not self.context_len or self.tokenizer is None:
            return text
        tokenizer = self.tokenizer
        encode = getattr(tokenizer, "encode", None)
        decode = getattr(tokenizer, "decode", None)
        if encode is None or decode is None:
            return text
        ids = encode(text, add_special_tokens=False)
        if len(ids) <= self.context_len:
            return text
        return decode(ids[:self.context_len], skip_special_tokens=True)

    def _truncate_batch(self, texts: list[str]) -> list[str]:
        if not texts or not self.context_len:
            return texts
        tokenizer = self.tokenizer
        if tokenizer is None:
            return texts
        backend = getattr(tokenizer, "backend_tokenizer", None)
        encode_batch = getattr(backend, "encode_batch_fast", None) or getattr(backend, "encode_batch", None)
        decode_batch = getattr(backend, "decode_batch", None)
        if encode_batch is not None and decode_batch is not None:
            ids_batch = encode_batch(texts, add_special_tokens=False)
            return self._clip(texts, ids_batch, decode_batch)
        encode_batch = getattr(tokenizer, "batch_encode_plus", None) or getattr(tokenizer, "batch_encode", None)
        decode_batch = getattr(tokenizer, "batch_decode", None)
        if encode_batch is None or decode_batch is None:
            return [self._truncate(text) for text in texts]
        encoded = encode_batch(texts, add_special_tokens=False, padding=False, truncation=False)
        return self._clip(texts, encoded["input_ids"], decode_batch)

    def _clip(self, texts: list[str], ids_batch, decode_batch) -> list[str]:
        limit = self.context_len
        clipped, clipped_at = [], []
        for i, ids in enumerate(ids_batch):
            token_ids = ids.ids if hasattr(ids, "ids") else ids
            if texts[i] and len(token_ids) > limit:
                clipped.append(token_ids[:limit])
                clipped_at.append(i)
        if not clipped:
            return texts
        decoded = decode_batch(clipped, skip_special_tokens=True)
        out = list(texts)
        for i, text in zip(clipped_at, decoded):
            out[i] = text
        return out

    def _split(self, text: str):
        # Imported here so llama-index is optional when only truncate() is used.
        from llama_index.core.text_splitter import TokenTextSplitter
        if not self._splitter:
            self._splitter = TokenTextSplitter(
                chunk_size=self.chunk_size,
                chunk_overlap=TOKEN_MARGIN << 1,
                tokenizer=self.tokenizer.encode if self.tokenizer is not None else None,
                include_metadata=False,
                include_prev_next_rel=False,
            )
        chunks = self._splitter.split_text(text)
        if len(chunks) > 1 and len(chunks[-1]) < (TOKEN_MARGIN << 2):
            chunks = chunks[:-1]
        return chunks

    def split(self, texts: list[str]) -> tuple[list[str], list[int], list[int]]:
        texts = texts if isinstance(texts, list) else [texts]
        chunks = list(map(self._split, texts))
        counts = list(map(len, chunks))
        start_idx = [0] * len(chunks)
        for i in range(1, len(counts)):
            start_idx[i] = start_idx[i - 1] + counts[i - 1]
        return list(chain(*chunks)), start_idx, counts
