import logging
import os
from concurrent.futures import ThreadPoolExecutor
from itertools import batched, chain
from typing import Optional

from icecream import ic

from coffeemaker.orchestrators.utils import *
from nlp import Digest, digestors, embedders
from pybeansack import Beansack, create_client
from pybeansack.models import (
    K_CONTENT,
    K_CONTENT_LENGTH,
    K_CREATED,
    K_EMBEDDING,
    K_ENTITIES,
    K_GIST,
    K_KIND,
    K_REGIONS,
    K_SOURCE,
    K_URL,
    POST,
    Bean,
)
from pybeansack.utils import *

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count()))
MAX_ANALYZE_NDAYS = int(os.getenv("MAX_ANALYZE_NDAYS", 2))
END_OF_QUEUE = "__END_OF_QUEUE__"  # Sentinel value to signal end of queue

EMBED_FILTER = [
    f"{K_EMBEDDING} IS NULL",
    f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_INDEXING}",
    f"{K_KIND} <> '{POST}'",
]
EXTRACT_FILTER = [
    f"{K_ENTITIES} IS NULL",
    f"{K_REGIONS} IS NULL",
    f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_EXTRACTING}",
    f"{K_KIND} <> '{POST}'",
]
DIGEST_FILTER = [
    f"{K_GIST} IS NULL",
    f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_DIGESTING}",
    f"{K_KIND} <> '{POST}'",
]

index_storables = lambda beans: [bean for bean in beans if bean.embedding]
digest_storables = lambda beans: [bean for bean in beans if bean.gist]
run_id = lambda: datetime.now().strftime("%A, %b-%d-%Y")


class Orchestrator:
    db: Beansack
    embedder_model: Optional[str] = None
    embedder_context_len: int = 0
    extractor_model: Optional[str] = None
    extractor_context_len: int = 0
    digestor_model: Optional[str] = None
    digestor_context_len: int = 0
    dbworker: ThreadPoolExecutor
    backup_container = None

    def __init__(
        self,
        db_kwargs: dict,
        embedder_path: Optional[str] = None,
        embedder_context_len: int = 0,
        extractor_path: Optional[str] = None,
        extractor_context_len: int = 0,
        digestor_path: Optional[str] = None,
        digestor_context_len: int = 0,
    ):
        self.db = create_client(**db_kwargs)
        self.embedder_model = embedder_path
        self.embedder_context_len = embedder_context_len
        self.extractor_model = extractor_path
        self.extractor_context_len = extractor_context_len
        self.digestor_model = digestor_path
        self.digestor_context_len = digestor_context_len
        self.dbworker = ThreadPoolExecutor(thread_name_prefix="analyzer-dbworker")

    def get_beans(self, filter, batch_size: int = 0):
        return self.db.query_latest_beans(
            collected=ndays_ago(MAX_ANALYZE_NDAYS),
            conditions=filter,
            limit=batch_size,
            columns=[K_URL, K_CREATED, K_CONTENT, K_SOURCE],
        )

    def stream_beans(self, filter, batch_size: int):
        while beans := self.get_beans(filter, batch_size):
            yield beans

    def embed_beans(self, beans: list[Bean], batch_size: int):
        if not beans:
            return 0
        if not self.embedder_model:
            raise ValueError("embedder_model is not set")
        with embedders.from_path(
            self.embedder_model, self.embedder_context_len
        ) as embedder:
            total = 0
            for chunk in batched(beans, batch_size):
                try:
                    vectors = embedder.embed_documents([bean.content for bean in chunk])
                    updates = [
                        Bean(url=b.url, source=b.source, embedding=v)
                        for b, v in zip(chunk, vectors)
                        if len(v) == VECTOR_LEN
                    ]
                    log.info(
                        "embedded",
                        extra={"source": chunk[0].source, "num_items": len(updates)},
                    )
                    self._queue_update(self.db.update_embeddings, updates)
                    total += len(chunk)
                except Exception as e:
                    log.error(
                        "failed indexing",
                        extra={"source": chunk[0].source, "num_items": len(chunk)},
                        exc_info=True,
                        stack_info=True,
                    )
        return total

    def extract_beans(self, beans: list[Bean], batch_size: int):
        if not beans:
            return 0
        if not self.extractor_model:
            raise ValueError("extractor_model is not set")
        with digestors.NamedEntityExtractor(
            self.extractor_model, self.extractor_context_len, confidence=0.3
        ) as extractor:
            total = 0
            for chunk in batched(beans, batch_size):
                try:
                    digests = extractor.run_batch([b.content for b in chunk])
                    updates = [
                        Bean(
                            url=b.url,
                            source=b.source,
                            entities=merge_lists(d.people, d.organizations, d.products),
                            regions=d.regions,
                        )
                        for b, d in zip(chunk, digests)
                        if d
                    ]
                    log.info(
                        "extracted",
                        extra={"source": chunk[0].source, "num_items": len(digests)},
                    )
                    self._queue_update(
                        self.db.update_beans, updates, columns=[K_REGIONS, K_ENTITIES]
                    )
                    total += len(chunk)
                except Exception:
                    log.error(
                        "failed extracting",
                        extra={"source": chunk[0].source, "num_items": len(chunk)},
                        exc_info=True,
                        stack_info=True,
                    )
        return total

    def digest_beans(self, beans: list[Bean], batch_size: int):
        if not beans:
            return 0
        if not self.digestor_model:
            raise ValueError("digestor_model is not set")
        with digestors.from_path(
            model_path=self.digestor_model,
            max_input_tokens=self.digestor_context_len,
            max_output_tokens=384,
            output_parser=Digest.parse_compressed,
        ) as digestor:
            total = 0
            for chunk in batched(beans, batch_size):
                try:
                    gists = digestor.run_batch([bean.content for bean in chunk])
                    updates = [
                        Bean(url=b.url, source=b.source, gist=d.raw)
                        for b, d in zip(chunk, gists)
                        if d and d.raw
                    ]
                    log.info(
                        "digested",
                        extra={"source": chunk[0].source, "num_items": len(updates)},
                    )
                    self._queue_update(self.db.update_beans, updates, columns=[K_GIST])
                    total += len(chunk)
                except Exception:
                    log.error(
                        "failed indexing",
                        extra={"source": chunk[0].source, "num_items": len(chunk)},
                        exc_info=True,
                        stack_info=True,
                    )
        return total

    def _queue_update(self, update_func, beans, columns=None):
        if not beans:
            return
        if columns:
            future = self.dbworker.submit(update_func, beans=beans, columns=columns)
        else:
            future = self.dbworker.submit(update_func, beans=beans)
        future.add_done_callback(
            lambda f: log.info(
                "updated", extra={"source": beans[0].source, "num_items": f.result()}
            )
        )

    def _refresh_clusters(self):
        self.db.refresh_clusters()
        log.info("refreshed clusters", extra={"source": run_id(), "num_items": 1})

    @log_runtime(logger=log)
    def run_indexer(self, batch_size: int = BATCH_SIZE):
        with self.dbworker:
            beans = self.get_beans(EMBED_FILTER)
            log.info(
                "starting indexer", extra={"source": run_id(), "num_items": len(beans)}
            )
            total = self.embed_beans(beans, batch_size)
            log.info("total indexed", extra={"source": run_id(), "num_items": total})

    @log_runtime(logger=log)
    def run_extractor(self, batch_size: int = BATCH_SIZE):
        with self.dbworker:
            beans = self.get_beans(EXTRACT_FILTER)
            log.info(
                "starting digestor", extra={"source": run_id(), "num_items": len(beans)}
            )
            total = self.extract_beans(beans, batch_size)
            log.info("total extracted", extra={"source": run_id(), "num_items": total})

    @log_runtime(logger=log)
    def run_digestor(self, batch_size: int = BATCH_SIZE):
        with self.dbworker:
            beans = self.get_beans(DIGEST_FILTER)
            log.info(
                "starting digestor", extra={"source": run_id(), "num_items": len(beans)}
            )
            total = self.digest_beans(beans, batch_size)
            log.info("total digested", extra={"source": run_id(), "num_items": total})

    @log_runtime(logger=log)
    def run(
        self,
        embedder_batch_size: int = BATCH_SIZE,
        extractor_batch_size: int = BATCH_SIZE,
        digestor_batch_size: int = BATCH_SIZE,
    ):
        with self.dbworker:
            # run embeddings
            beans = self.get_beans(EMBED_FILTER)
            log.info(
                "starting indexer", extra={"source": run_id(), "num_items": len(beans)}
            )
            total = self.embed_beans(beans, embedder_batch_size)
            log.info("total indexed", extra={"source": run_id(), "num_items": total})

            # run digests
            beans = self.get_beans(EXTRACT_FILTER)
            log.info(
                "starting extractor",
                extra={"source": run_id(), "num_items": len(beans)},
            )
            total = self.extract_beans(beans, extractor_batch_size)
            log.info("total extracted", extra={"source": run_id(), "num_items": total})

            # run digests
            beans = self.get_beans(DIGEST_FILTER)
            log.info(
                "starting digestor", extra={"source": run_id(), "num_items": len(beans)}
            )
            total = self.digest_beans(beans, digestor_batch_size)
            log.info("total digested", extra={"source": run_id(), "num_items": total})

    def close(self):
        # Close database connection
        self.db.optimize()
        self.db.close()
