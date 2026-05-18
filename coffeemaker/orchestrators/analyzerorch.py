import logging
import os
from datetime import datetime
from itertools import batched
from typing import Optional

from coffeemaker.processingcache.base import StateCacheBase, ClassificationCacheBase
from nlp import digestors, embedders, valid_tags
from pybeansack import BEANS
from pybeansack.models import (
    K_CATEGORIES,
    K_CONTENT,
    K_EMBEDDING,
    K_ENTITIES,
    K_RELATED,
    K_SENTIMENTS,
    K_SOURCE,
    K_URL,
)
from pycupboard.pgcupboard import DIGEST

from .utils import *

log = logging.getLogger("analyzerworker")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count()))
MAX_CLASSIFICATIONS = int(os.getenv("MAX_CLASSIFICATIONS", 2))
MAX_RELATED = int(os.getenv('MAX_RELATED', 500))
CLUSTER_EPS = float(os.getenv("CLUSTER_EPS", 0.4))
VECTOR_LEN = int(os.getenv("VECTOR_LEN", 384))

clean_updates = lambda packlist: [{k:v for k,v in pack.items() if v} for pack in packlist if pack]
run_id = lambda: datetime.now().strftime("%A, %b-%d-%Y")


class Embedder:
    cache: StateCacheBase
    embedder: embedders.EmbedderBase

    def __init__(
        self,
        cache: StateCacheBase,
        embedder_path: str,
        embedder_context_len: int,
    ):
        self.cache = cache
        self.embedder = embedders.from_path(
            model_path=embedder_path, context_len=embedder_context_len
        )

    def embed_beans(self, beans: list[dict], batch_size: int):
        with self.embedder:
            for chunk in batched(beans, batch_size):
                try:
                    vectors = self.embedder.embed_documents([bean[K_CONTENT] for bean in chunk])
                    updates = clean_updates([
                        {K_URL: b[K_URL], K_EMBEDDING: vec}
                        for b, vec in zip(chunk, vectors)
                        if vec and len(vec) == VECTOR_LEN
                    ])
                    log.info(
                        "embedded",
                        extra={"source": chunk[0][K_SOURCE], "num_items": len(updates)},
                    )
                    yield updates
                except Exception:
                    log.error(
                        "failed embedding",
                        extra={"source": chunk[0][K_SOURCE], "num_items": len(chunk)},
                        exc_info=True,
                        stack_info=True,
                    )

    @log_runtime(logger=log)
    def run(self, batch_size: int = BATCH_SIZE):
        beans = self.cache.get(BEANS, states="collected", exclude_states="embedded", limit=100)
        log.info("starting embedder", extra={"source": run_id(), "num_items": len(beans)})
        total = 0
        for updates in self.embed_beans(beans, batch_size):
            count = self.cache.set(BEANS, "embedded", updates)
            total += (count or len(updates))
        log.info("total embedded", extra={"source": run_id(), "num_items": total})
        return total


class Extractor:
    cache: StateCacheBase
    extractor: digestors.NamedEntityExtractor

    def __init__(
        self,
        cache: StateCacheBase,
        extractor_path: str,
        extractor_context_len: int,
    ):
        self.cache = cache
        self.extractor = digestors.NamedEntityExtractor(
            model_path=extractor_path,
            context_len=extractor_context_len,
            threshold=0.35,
        )

    def extract_beans(self, beans: list[dict], batch_size: int):
        with self.extractor:
            for chunk in batched(beans, batch_size):
                try:
                    extractions = self.extractor.run_batch([b[K_CONTENT] for b in chunk])
                    updates = clean_updates([
                        {
                            K_URL: b[K_URL],
                            K_ENTITIES: ents.model_dump()
                        } if ents else {K_URL:b[K_URL]}
                        for b, ents in zip(chunk, extractions)
                    ])
                    log.info("extracted", extra={"source": chunk[0][K_SOURCE], "num_items": len(updates)})
                    yield updates
                except Exception:
                    log.error(
                        "failed extracting",
                        extra={"source": chunk[0][K_SOURCE], "num_items": len(chunk)},
                        exc_info=True,
                        stack_info=True,
                    )

    @log_runtime(logger=log)
    def run(self, batch_size: int = BATCH_SIZE):
        beans = self.cache.get(BEANS, states="collected", exclude_states="extracted")
        log.info("starting extractor", extra={"source": run_id(), "num_items": len(beans)})
        total = 0
        for updates in self.extract_beans(beans, batch_size):
            count = self.cache.set(BEANS, "extracted", updates)
            total += (count or len(updates))
        log.info("total extracted", extra={"source": run_id(), "num_items": total})
        return total


class Digestor:
    cache: StateCacheBase
    digestor: digestors.DigestorBase

    def __init__(
        self,
        cache: StateCacheBase,
        digestor_path: str,
        digestor_context_len: int,
    ):
        self.cache = cache
        self.digestor = digestors.from_path(
            model_path=digestor_path,
            context_len=digestor_context_len,
            response_mode="json",
        )

    def digest_beans(self, beans: list[dict], batch_size: int):
        with self.digestor:
            for chunk in batched(beans, batch_size):
                try:
                    digests = self.digestor.run_batch([bean[K_CONTENT] for bean in chunk])
                    updates = clean_updates([
                        {
                            K_URL: b[K_URL],
                            DIGEST: d.model_dump()
                        }
                        for b, d in zip(chunk, digests)
                        if d
                    ])
                    log.info(
                        "digested",
                        extra={"source": chunk[0][K_SOURCE], "num_items": len(updates)},
                    )
                    yield updates
                except Exception:
                    log.error(
                        "failed digesting",
                        extra={"source": chunk[0][K_SOURCE], "num_items": len(chunk)},
                        exc_info=True,
                        stack_info=True,
                    )

    @log_runtime(logger=log)
    def run(self, batch_size: int = BATCH_SIZE):
        beans = self.cache.get(BEANS, states="collected", exclude_states="digested")
        log.info("starting digestor", extra={"source": run_id(), "num_items": len(beans)})
        total = 0
        for updates in self.digest_beans(beans, batch_size):
            count = self.cache.set(BEANS, "digested", updates)
            total += (count or len(updates))
        log.info("total digested", extra={"source": run_id(), "num_items": total})
        return total


class Classifier:
    cache: StateCacheBase
    cls_cache: ClassificationCacheBase

    def __init__(self, cache: StateCacheBase, cls_cache: ClassificationCacheBase):
        self.cache = cache
        self.cls_cache = cls_cache

    def classify_beans(self, beans: list[dict], batch_size: int):
        for chunk in batched(beans, batch_size):
            embeddings = [bean[K_EMBEDDING] for bean in chunk]
            categories_list = self.cls_cache.batch_search("categories", embeddings, top_n=MAX_CLASSIFICATIONS)
            sentiments_list = self.cls_cache.batch_search("sentiments", embeddings, top_n=MAX_CLASSIFICATIONS)
            updates = clean_updates([
                {
                    K_URL: bean[K_URL],
                    K_CATEGORIES: valid_tags(categories),
                    K_SENTIMENTS: valid_tags(sentiments),
                }
                for bean, categories, sentiments in zip(chunk, categories_list, sentiments_list)
            ])
            log.info("classified", extra={"source": chunk[0][K_URL], "num_items": len(updates)})
            yield updates

    def cluster_beans(self, beans: list[dict], batch_size: int):
        self.cls_cache.store(BEANS, beans)
        for chunk in batched(beans, batch_size):
            embeddings = [bean[K_EMBEDDING] for bean in chunk]
            related_list = self.cls_cache.batch_search(BEANS, embeddings, distance=CLUSTER_EPS, top_n=MAX_RELATED)
            updates = clean_updates([
                {
                    K_URL: bean[K_URL],
                    K_RELATED: list(filter(lambda x: x != bean[K_URL], related)),
                }
                for bean, related in zip(chunk, related_list)
            ])
            log.info("clustered", extra={"source": chunk[0][K_URL], "num_items": len(updates)})
            yield updates

    @log_runtime(logger=log)
    def run(self, batch_size: int = BATCH_SIZE):
        # run classification before clustering
        beans = self.cache.get(BEANS, states="embedded", exclude_states="classified")
        log.info("starting classifier", extra={"source": run_id(), "num_items": len(beans)})
        classified = 0
        embedded = [b for b in beans if K_EMBEDDING in b]
        for updates in self.classify_beans(embedded, batch_size):
            count = self.cache.set(BEANS, "classified", updates)
            classified += (count or len(updates))
        log.info("total classified", extra={"source": run_id(), "num_items": classified})

        # run clustering after classification
        beans = self.cache.get(BEANS, states="embedded", exclude_states="clustered")
        log.info("starting clusterer", extra={"source": run_id(), "num_items": len(beans)})
        clustered = 0
        embedded = [b for b in beans if K_EMBEDDING in b]
        for updates in self.cluster_beans(embedded, batch_size):
            count = self.cache.set(BEANS, "clustered", updates)
            clustered += (count or len(updates))
        log.info("total clustered", extra={"source": run_id(), "num_items": clustered})
        return classified, clustered
