"""NLP embedder / digestor / extractor smoke tests (GPU/network)."""

import json
import logging
import os
import random
import re
from itertools import batched
from pathlib import Path

import pytest
from icecream import ic
from tqdm import tqdm

TESTS_DIR = Path(__file__).resolve().parent
ROOT = TESTS_DIR.parent
EMBEDDER_CONTEXT_LEN = 512
TEXTS_FOR_NLP = TESTS_DIR / "texts-for-nlp.json"


def _to_filename(name: str) -> str:
    return os.path.join(ROOT, ".test", re.sub(r"[^a-zA-Z0-9]", "-", str(name)))


def _load_json(path: Path):
    with open(path) as file:
        return json.load(file)


def _save_json(name, items):
    filename = _to_filename(name) + ".json"
    with open(filename, "w") as file:
        json.dump(items, file)
    return filename


def test_merge_chunks_extends_entities_and_keeps_first_classification():
    from nlp.extractors import EntityExtractor
    from nlp.models import Classifications, Entities

    extractor = EntityExtractor("unused", context_len=512)
    merged = extractor._merge_chunks(
        [
            {
                "entities": {
                    "companies": [{"text": "Apple Inc."}],
                    "people": [{"text": "Tim Cook"}],
                },
                "domain_genre": ["Economics, Accounting and Finance", "Software and Data Engineering"],
                "expression_sentiment": "positive",
                "political_ideology": "right",
            },
            {
                "entities": {
                    "companies": [{"text": "apple inc."}, {"text": "Microsoft"}],
                    "products": [{"text": "iPhone"}],
                },
                "domain_genre": "Software and Data Engineering",
                "expression_sentiment": "negative",
                "political_ideology": "left",
            },
            {
                "entities": {
                    "people": [{"text": "Satya Nadella"}],
                },
                "domain_genre": "Politics and Global Affairs",
                "expression_sentiment": "neutral",
                "political_ideology": "center",
            },
        ],
        start_idx=[0, 2],
        counts=[2, 1],
        entity_type=Entities,
        class_type=Classifications,
    )
    first_ents, first_cl = merged[0]
    second_ents, second_cl = merged[1]
    assert first_ents.companies == ["apple_inc", "microsoft"]
    assert first_ents.people == ["tim_cook"]
    assert first_ents.products == ["i_phone"]
    assert first_cl.category == "economics_accounting_and_finance"
    assert first_cl.sentiment == "positive"
    assert first_cl.ideology == "right"
    assert second_ents.people == ["satya_nadella"]
    assert second_cl.category == "politics_and_global_affairs"
    assert second_cl.sentiment == "neutral"


@pytest.mark.integration
def test_embedder():
    from nlp import InfinityEmbeddings

    data = _load_json(TEXTS_FOR_NLP)
    input_texts = [d["content"] for d in data]

    embedder = InfinityEmbeddings("avsolatorio/GIST-small-Embedding-v0", EMBEDDER_CONTEXT_LEN)
    batch_size = 32
    with embedder, tqdm(total=len(input_texts), desc="Progress", unit="bean") as pbar:
        for i in range(0, len(input_texts), batch_size):
            vecs = embedder.embed_documents(input_texts[i : i + batch_size])
            ic([(vec[:2] + vec[-1:]) for vec in random.sample(vecs, 2)])
            pbar.update(len(input_texts[i : i + batch_size]))


@pytest.mark.integration
def test_digestor():
    from nlp import NewsDigest, create_text_analyst

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    batch_size = 16
    data_batches = list(batched(_load_json(TEXTS_FOR_NLP), batch_size))
    result = []
    with create_text_analyst(
        "vllm://LiquidAI/LFM2.5-1.2B-Instruct",
        context_len=32768,
        output_model=NewsDigest,
    ) as digestor:
        for chunk in tqdm(data_batches, desc="Progress: ", unit="bean chunk", total=len(data_batches)):
            result.extend(ic(digestor.run_batch([d["content"] for d in chunk])))
    _save_json("digestor-structured-output-results", [r.model_dump() for r in result])


@pytest.mark.integration
def test_extractor():
    from nlp import EntityExtractor

    batch_size = 5
    data_batches = list(batched(_load_json(TEXTS_FOR_NLP)[:60], batch_size))

    with EntityExtractor(
        "knowledgator/modern-gliner-bi-base-v1.0",
        context_len=4096,
        threshold=0.4,
    ) as extractor:
        for chunk in tqdm(data_batches, total=len(data_batches), desc="Progress: ", unit="bean chunk"):
            [ic(ents.model_dump(), cl.model_dump()) for ents, cl in extractor.run_batch([d["content"] for d in chunk])]


if __name__ == "__main__":
    test_embedder()
