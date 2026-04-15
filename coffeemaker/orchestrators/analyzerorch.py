import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import batched
from typing import Optional

from slugify import slugify

from coffeemaker.processingcache.base import ProcessingCacheBase
from coffeemaker.processingcache.pgcache import ClassificationCache
from nlp import Digest, digestors, embedders
from pybeansack import CDNStore, BEANS
from pybeansack.models import (
    K_CATEGORIES,
    K_CONTENT,
    K_CONTENT_LENGTH,
    K_CREATED,
    K_EMBEDDING,
    K_ENTITIES,
    K_GIST,
    K_KIND,
    K_REGIONS,
    K_RELATED,
    K_SENTIMENTS,
    K_SOURCE,
    K_URL,
    # POST,
)

from .utils import *

log = logging.getLogger("analyzerworker")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count()))
MAX_CLASSIFICATIONS = int(os.getenv("MAX_CLASSIFICATIONS", 2))
CLUSTER_EPS = float(os.getenv("CLUSTER_EPS", 0.4))
VECTOR_LEN = int(os.getenv("VECTOR_LEN", 384))

index_storables = lambda beans: [bean for bean in beans if bean.embedding]
digest_storables = lambda beans: [bean for bean in beans if bean.gist]
run_id = lambda: datetime.now().strftime("%A, %b-%d-%Y")

class Indexer:
    cache: ProcessingCacheBase
    embedder: embedders.EmbedderBase
    extractor: digestors.NamedEntityExtractor
    digestor: digestors.DigestorBase
    cdn: CDNStore

    def __init__(
        self,
        cache: ProcessingCacheBase,        
        embedder_path: Optional[str] = None,
        embedder_context_len: int = 0,
        extractor_path: Optional[str] = None,
        extractor_context_len: int = 0,
        digestor_path: Optional[str] = None,
        digestor_context_len: int = 0,
        cls_cache: Optional[ClassificationCache] = None,
        cdn: Optional[CDNStore] = None,
    ):
        self.cache = cache
        if embedder_path:
            self.embedder = embedders.from_path(
                model_path=embedder_path, context_len=embedder_context_len
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
        self.cls_cache = cls_cache
        self.cdn = cdn

    def embed_beans(self, beans: list[dict], batch_size: int):
        with self.embedder:
            for chunk in batched(beans, batch_size):
                try:
                    vectors = self.embedder.embed_documents(
                        [bean[K_CONTENT] for bean in chunk]
                    )
                    updates = [
                        {K_URL: b[K_URL], K_EMBEDDING: vec}
                        if len(vec) == VECTOR_LEN
                        else {K_URL: b[K_URL]}
                        for b, vec in zip(chunk, vectors)
                    ]
                    log.info(
                        "embedded",
                        extra={"source": chunk[0][K_SOURCE], "num_items": len(updates)},
                    )
                    yield updates
                except Exception as e:
                    log.error(
                        "failed embedding",
                        extra={"source": chunk[0][K_SOURCE], "num_items": len(chunk)},
                        exc_info=True,
                        stack_info=True,
                    )

    def _search_classification(self, bean: dict):
        embedding = bean[K_EMBEDDING]
        related = self.cls_cache.search("beans", embedding=embedding, distance=CLUSTER_EPS)
        categories = self.cls_cache.search(
            "categories",
            embedding=embedding,
            distance_func="cosine",
            top_n=MAX_CLASSIFICATIONS,            
        )
        sentiments = self.cls_cache.search(
            "sentiments",
            embedding=embedding,
            distance_func="cosine",
            top_n=MAX_CLASSIFICATIONS,
        )
        
        return {
            K_URL: bean[K_URL],
            K_RELATED: list(
                filter(lambda x: x != bean[K_URL], related)
            ),
            K_CATEGORIES: categories,
            K_SENTIMENTS: sentiments,
        }

    def classify_beans(self, beans: list[dict], batch_size: int):
        # store the items first
        for chunk in batched(beans, batch_size):
            self.cls_cache.store(BEANS, chunk)
            updates = list(ThreadPoolExecutor(max_workers=batch_size).map(self._search_classification, chunk))
            # updates = list(map(self._create_classification, chunk))
            log.info(
                "classified",
                extra={"source": chunk[0][K_URL], "num_items": len(updates)},
            )
            yield updates

    def extract_beans(self, beans: list[dict], batch_size: int):
        with self.extractor:
            for chunk in batched(beans, batch_size):
                try:
                    extractions = self.extractor.run_batch(
                        [b[K_CONTENT] for b in chunk]
                    )
                    updates = [
                        {
                            K_URL: b[K_URL],
                            K_ENTITIES: merge_lists(
                                ents.people,
                                ents.organizations,
                                ents.products,
                                ents.stock_tickers,
                            ),
                            K_REGIONS: ents.regions,
                        }
                        if ents
                        else {K_URL: b[K_URL]}
                        for b, ents in zip(chunk, extractions)
                    ]
                    log.info(
                        "extracted",
                        extra={
                            "source": chunk[0][K_SOURCE],
                            "num_items": len(extractions),
                        },
                    )
                    yield updates
                except Exception:
                    log.error(
                        "failed extracting",
                        extra={"source": chunk[0][K_SOURCE], "num_items": len(chunk)},
                        exc_info=True,
                        stack_info=True,
                    )

    def digest_beans(self, beans: list[dict], batch_size: int):
        with self.digestor:
            for chunk in batched(beans, batch_size):
                try:
                    gists = self.digestor.run_batch([bean[K_CONTENT] for bean in chunk])
                    updates = [
                        {K_URL: b[K_URL], K_GIST: d.raw}
                        if d and d.raw
                        else {K_URL: b[K_URL]}
                        for b, d in zip(chunk, gists)
                    ]
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
    def run_embedder(self, batch_size: int = BATCH_SIZE):
        beans = self.cache.get(
            "beans", states="collected", exclude_states="embedded"
        )
        log.info(
            "starting embedder", extra={"source": run_id(), "num_items": len(beans)}
        )
        total = 0
        for updates in self.embed_beans(beans, batch_size):
            self.cache.set("beans", "embedded", updates)
            total += len(updates)
        log.info("total embedded", extra={"source": run_id(), "num_items": total})
        return total

    @log_runtime(logger=log)
    def run_classifier(self, batch_size: int = BATCH_SIZE):
        # NOTE: this runs both classifier and clustering
        beans = self.cache.get(
            "beans", states="embedded", exclude_states="classified"
        )
        log.info(
            "starting classifier", extra={"source": run_id(), "num_items": len(beans)}
        )
        total = 0        
        for updates in self.classify_beans([b for b in beans if K_EMBEDDING in b], batch_size):
            self.cache.set("beans", "classified", updates)
            total += len(updates)
        log.info("total classified", extra={"source": run_id(), "num_items": total})
        return total

    @log_runtime(logger=log)
    def run_extractor(self, batch_size: int = BATCH_SIZE):
        beans = self.cache.get(
            "beans", states="collected", exclude_states="extracted"
        )
        log.info(
            "starting extractor", extra={"source": run_id(), "num_items": len(beans)}
        )
        total = 0
        for updates in self.extract_beans(beans, batch_size):
            self.cache.set("beans", "extracted", updates)
            total += len(updates)
        log.info("total extracted", extra={"source": run_id(), "num_items": total})
        return total

    @log_runtime(logger=log)
    def run_digestor(self, batch_size: int = BATCH_SIZE):
        beans = self.cache.get(
            "beans", states="collected", exclude_states="digested"
        )
        log.info(
            "starting digestor", extra={"source": run_id(), "num_items": len(beans)}
        )
        total = 0
        for updates in self.digest_beans(beans, batch_size):
            self.cache.set("beans", "digested", updates)
            total += len(updates)
        log.info("total digested", extra={"source": run_id(), "num_items": total})
        return total

    # @log_runtime(logger=log)
    # def run_cdn(self, batch_size: int = BATCH_SIZE):
    #     """Put bean contents in CDN"""
    #     CDN_PATH_TEMPLATE = "beansack/contents/{date}/{slugurl}.md"
    #     create_cdn_path = lambda bean: CDN_PATH_TEMPLATE.format(
    #         date=bean[K_CREATED].strftime("%Y/%m/%d"), slugurl=slugify(bean[K_URL])
    #     )
        
    #     total = 0
    #     if beans := self.cache.get("beans", states="collected", exclude_states="cdned"):
    #         log.info("starting cdn", extra={"num_items": len(beans), "source": run_id()})
    #         with ThreadPoolExecutor(max_workers=batch_size) as exec:
    #             cdn_urls = exec.map(
    #                 lambda bean: self.cdn.upload_text(path=create_cdn_path(bean), content=bean[K_CONTENT]),
    #                 beans,
    #             )

    #         updates = [
    #             {K_URL: bean[K_URL], "content_url": cdn_url}
    #             for bean, cdn_url in zip(beans, cdn_urls)
    #         ]
    #         self.cache.set("beans", "cdned", updates)
    #         total += len(updates)

    #     log.info("total cdned", extra={"source": run_id(), "num_items": total})
    #     return total
    
    @log_runtime(logger=log)
    def run(
        self,
        embedder_batch_size: int = BATCH_SIZE,
        extractor_batch_size: int = BATCH_SIZE,
        digestor_batch_size: int = BATCH_SIZE,
    ):
        total = 0
        total += self.run_embedder(batch_size=embedder_batch_size)
        total += self.run_extractor(batch_size=extractor_batch_size)
        total += self.run_digestor(batch_size=digestor_batch_size)
        return total
