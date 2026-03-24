import logging
import os
from concurrent.futures import ThreadPoolExecutor
from itertools import batched, chain
from typing import Optional

from .processingcache import ProcessingCache
from .utils import *
from nlp import Digest, digestors, embedders
from pybeansack import Beansack, create_client
from pybeansack.models import (
    K_COLLECTED,
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
from icecream import ic

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count()))
MAX_ANALYZE_NDAYS = int(os.getenv("MAX_ANALYZE_NDAYS", 2))
END_OF_QUEUE = "__END_OF_QUEUE__"  # Sentinel value to signal end of queue
WORDS_THRESHOLD_FOR_INDEXING = int(
    os.getenv("WORDS_THRESHOLD_FOR_INDEXING", 160)
)  # mininum words needed to put it through indexing
WORDS_THRESHOLD_FOR_EXTRACTING = int(
    os.getenv("WORDS_THRESHOLD_FOR_EXTRACTING", 160)
)  # min words needed to use the generated summary
WORDS_THRESHOLD_FOR_DIGESTING = int(
    os.getenv("WORDS_THRESHOLD_FOR_DIGESTING", 160)
)  # min words needed to use the generated summary
WORDS_THRESHOLD_FOR_ANALYZING = int(
    os.getenv("WORDS_THRESHOLD_FOR_ANALYZING", 160)
)  # min words needed to use the generated summary

EMBED_FILTER = [
    f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_INDEXING}",
    f"{K_KIND} != '{POST}'",
]
EXTRACT_FILTER = [
    f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_EXTRACTING}",
    f"{K_KIND} != '{POST}'",
]
DIGEST_FILTER = [
    f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_DIGESTING}",
    f"{K_KIND} != '{POST}'",
]
ANALYZER_FILTER = [
    f"{K_CONTENT_LENGTH} >= {WORDS_THRESHOLD_FOR_ANALYZING}",
    f"{K_KIND} != '{POST}'",
]

index_storables = lambda beans: [bean for bean in beans if bean.embedding]
digest_storables = lambda beans: [bean for bean in beans if bean.gist]
run_id = lambda: datetime.now().strftime("%A, %b-%d-%Y")

class Orchestrator:
    db: Beansack
    embedder: embedders.EmbedderBase
    extractor: digestors.NamedEntityExtractor
    digestor: digestors.DigestorBase

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
        if embedder_path:
            self.embedder = embedders.from_path(
                model_path=embedder_path, 
                context_len=embedder_context_len
            )
        if extractor_path:
            self.extractor = digestors.NamedEntityExtractor(
                model_path=extractor_path,
                context_len=extractor_context_len,
                confidence=0.4,
            )
        if digestor_path:
            self.digestor = digestors.from_path(
                model_path=digestor_path,
                context_len=digestor_context_len,
                max_output_tokens=384,
                output_parser=Digest.parse_compressed,
            )

    def embed_beans(self, beans: list[Bean], batch_size: int):
        with self.embedder:
            for chunk in batched(beans, batch_size):
                try:
                    vectors = self.embedder.embed_documents([bean.content for bean in chunk])
                    updates = [
                        Bean(url=b.url, embedding=vec) if len(vec) == VECTOR_LEN else Bean(url=b.url)
                        for b, vec in zip(chunk, vectors)
                    ]
                    log.info("embedded", extra={"source": chunk[0].source, "num_items": len(updates)})
                    yield updates
                except Exception as e:
                    log.error(
                        "failed embedding",
                        extra={"source": chunk[0].source, "num_items": len(chunk)},
                        exc_info=True,
                        stack_info=True,
                    )
        

    def extract_beans(self, beans: list[Bean], batch_size: int):        
        with self.extractor:
            for chunk in batched(beans, batch_size):
                try:
                    extractions = self.extractor.run_batch([b.content for b in chunk])
                    updates = [
                        Bean(
                            url=b.url,
                            entities=merge_lists(ents.people, ents.organizations, ents.products, ents.stock_tickers),
                            regions=ents.regions,
                        ) if ents else Bean(url=b.url)
                        for b, ents in zip(chunk, extractions)
                    ]
                    log.info("extracted", extra={"source": chunk[0].source, "num_items": len(extractions)})
                    yield updates
                except Exception:
                    log.error(
                        "failed extracting",
                        extra={"source": chunk[0].source, "num_items": len(chunk)},
                        exc_info=True,
                        stack_info=True,
                    )

    def digest_beans(self, beans: list[Bean], batch_size: int):
        with self.digestor:
            for chunk in batched(beans, batch_size):
                try:
                    gists = self.digestor.run_batch([bean.content for bean in chunk])
                    updates = [
                        Bean(url=b.url, source=b.source, gist=d.raw) if d and d.raw else Bean(url=b.url)
                        for b, d in zip(chunk, gists)
                    ]
                    log.info("digested", extra={"source": chunk[0].source, "num_items": len(updates)})
                    yield updates
                except Exception:
                    log.error(
                        "failed digesting",
                        extra={"source": chunk[0].source, "num_items": len(chunk)},
                        exc_info=True,
                        stack_info=True,
                    )

    @log_runtime(logger=log)
    def run_embedder(self, batch_size: int = BATCH_SIZE):
        total = 0
        with ProcessingCache(db_name="beans", id_key=K_URL, model_cls=Bean) as cache:
            beans = cache.get(
                "collected_beans", 
                "embedded_beans", 
                filters=ANALYZER_FILTER, 
                columns=[K_URL, K_SOURCE, K_CONTENT]
            )
            log.info("starting embedder", extra={"source": run_id(), "num_items": len(beans)})
            for batch in self.embed_beans(beans, batch_size):
                stored = cache.store("embedded_beans", batch, columns=[K_EMBEDDING])
                total += len(stored)
        log.info("total embedded", extra={"source": run_id(), "num_items": total})

    @log_runtime(logger=log)
    def run_extractor(self, batch_size: int = BATCH_SIZE):
        total = 0
        with ProcessingCache(db_name="beans", id_key=K_URL, model_cls=Bean) as cache:
            beans = cache.get(
                "collected_beans",
                "extracted_beans",
                filters=ANALYZER_FILTER,
                columns=[K_URL, K_SOURCE, K_CONTENT],
            )
            log.info("starting extractor", extra={"source": run_id(), "num_items": len(beans)})
            for batch in self.extract_beans(beans, batch_size):
                stored = cache.store("extracted_beans", batch, columns=[K_REGIONS, K_ENTITIES])
                total += len(stored)
        log.info("total extracted", extra={"source": run_id(), "num_items": total})

    @log_runtime(logger=log)
    def run_digestor(self, batch_size: int = BATCH_SIZE):
        total = 0
        with ProcessingCache(db_name="beans", id_key=K_URL, model_cls=Bean) as cache:
            beans = cache.get(
                "collected_beans",
                "digested_beans",
                filters=ANALYZER_FILTER,
                columns=[K_URL, K_SOURCE, K_CONTENT],
            )
            log.info("starting digestor", extra={"source": run_id(), "num_items": len(beans)})
            for batch in self.digest_beans(beans, batch_size):
                stored = cache.store("digested_beans", batch, columns=[K_GIST])
                total += len(stored)
        log.info("total digested", extra={"source": run_id(), "num_items": total})

    @log_runtime(logger=log)
    def run(
        self,
        embedder_batch_size: int = BATCH_SIZE,
        extractor_batch_size: int = BATCH_SIZE,
        digestor_batch_size: int = BATCH_SIZE,
    ):
        self.run_embedder(batch_size=embedder_batch_size)
        self.run_extractor(batch_size=extractor_batch_size)
        self.run_digestor(batch_size=digestor_batch_size)

    def close(self):
        # Close database connection
        # self.db.optimize()
        self.db.close()
