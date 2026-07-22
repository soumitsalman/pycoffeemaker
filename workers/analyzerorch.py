from pathlib import Path
from utils.dates import now
from utils.logs import get_logger, log_runtime
import os
from datetime import datetime
from itertools import batched, chain
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from processingcache import StateCacheBase, ClassificationCache
from nlp import (
    Digest, 
    Entities,
    EntityExtractor, 
    EmbedderBase,
    TextAnalystBase, 
    create_embedder, 
    create_text_analyst,
    normalize_tags,
    is_cuda_oom,
)
from utils.fields import *
from utils import VECTOR_LEN
from datacollectors import POST
from .cacheops import *
from .states import *
from icecream import ic

log = get_logger("analyzerworker")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count()))
MAX_DOCUMENT_LEN = int(os.getenv("MAX_DOCUMENT_LEN", 4096)) # 16KB

CLASSIFICATION_LIMIT = int(os.getenv("CLASSIFICATION_LIMIT", 2))
class Embedder:
    cache: StateCacheBase
    embedder: EmbedderBase
    classifications: dict

    def __init__(
        self,
        cache: StateCacheBase,
        model_path: str,
        context_len: int,        
        batch_size: int = BATCH_SIZE,
        **classification_kwargs
    ):
        self.cache = cache
        self.embedder = create_embedder(model_path=model_path, context_len=context_len)
        self.batch_size = batch_size
        self.classifications = {key: self._load_label_index(value) for key, value in classification_kwargs.items()}
        
    @classmethod
    def _load_label_index(cls, path: Path):
        df = pd.read_parquet(path)
        labels = df["id"].tolist()
        vectors = np.asarray(df[EMBEDDING].tolist(), dtype=np.float32)
        index = NearestNeighbors(
            metric="cosine",
            algorithm="brute",
            n_jobs=-1,
        )
        index.fit(vectors)
        return {"labels": labels, "index": index}

    @classmethod
    def _label_batch_search(cls, index_pack: dict, embeddings: list[list[float]], top_n: int) -> list[list[str]]:
        if not embeddings:
            return []
        labels = index_pack["labels"]
        _, indices = index_pack["index"].kneighbors(
            np.asarray(embeddings, dtype=np.float32),
            n_neighbors=min(top_n, len(labels)),
            return_distance=True,
        )
        return [[labels[i] for i in row] for row in indices]

    def classify_beans(self, beans: list[dict]):
        embeddings = [bean[EMBEDDING] for bean in beans]
        for key, index in self.classifications.items():
            labels = self._label_batch_search(index, embeddings, CLASSIFICATION_LIMIT)
            # NOTE: updating in place for future extension when I put the classifications in the queue for digestion
            [
                b.update({key: normalize_tags(lbl)}) 
                for b, lbl in zip(beans, labels) 
                if lbl
            ]   
        return beans

    def embed_beans(self, beans: list[dict]):
        try:
            vectors = self.embedder.embed_documents(
                [bean[CONTENT][:MAX_DOCUMENT_LEN << 1] for bean in beans]
            )
        except Exception as e:
            if not is_cuda_oom(e):
                raise

            if len(beans) == 1:
                log.warning(
                    event="skipped embedding after cuda oom",
                    source=beans[0].get(SOURCE),
                    url=beans[0].get(URL),
                )
                return []

            midpoint = (len(beans) + 1) // 2
            return self.embed_beans(beans[:midpoint]) + self.embed_beans(beans[midpoint:])

        # Do not advance beans with missing or invalid embeddings.
        return [
            {**bean, EMBEDDING: vector}
            for bean, vector in zip(beans, vectors)
            if vector and len(vector) == VECTOR_LEN
        ]

    @log_runtime(logger=log)
    def run(self): 
        total = 0
        with self.embedder:
            for chunk in decache_beans(self.cache, states=COLLECTED, exclude_states=EMBEDDED, batch_size=self.batch_size, log=log):
                try:
                    updates = self.embed_beans(chunk)
                    if not updates:
                        continue
                    log.info(event="embedded", source=chunk[0][SOURCE], num_items=len(updates))
                    updates = self.classify_beans(updates)
                    log.info(event="classified", source=chunk[0][SOURCE], num_items=len(updates))
                    total += encache_beans(self.cache, EMBEDDED, updates)
                    
                except Exception:
                    log.error(event="failed embedding and classifying",
                        source=chunk[0][SOURCE],
                        num_items=len(chunk),
                        exc_info=True,
                    )

        log.info(event="embedder completed", total_embedded=total)
        return total


class Extractor:
    cache: StateCacheBase
    extractor: EntityExtractor

    def __init__(
        self,
        cache: StateCacheBase,
        model_path: str,
        context_len: int,
        batch_size: int = BATCH_SIZE,
    ):
        self.cache = cache
        self.extractor = EntityExtractor(
            model_path=model_path,
            context_len=context_len,
            threshold=0.35,
            batch_size=batch_size,
        )
        self.batch_size = batch_size

    def extract_beans(self, chunk: list[dict]):
        extractions = self.extractor.run_batch([b[CONTENT][:MAX_DOCUMENT_LEN<<2] for b in chunk])
        return [
            {
                URL: b[URL],
                ENTITIES: ents.model_dump() if ents else None
            }
            for b, ents in zip(chunk, extractions)
        ]

    @log_runtime(logger=log)
    def run(self):
        total = 0
        with self.extractor:
            for chunk in decache_beans(self.cache, states=COLLECTED, exclude_states=EXTRACTED, batch_size=self.batch_size, log=log):
                try:
                    updates = self.extract_beans(chunk)
                    log.info(event="extracted", source=chunk[0][SOURCE], num_items=len(updates))
                    total += encache_beans(self.cache, EXTRACTED, updates)
                
                except Exception as e:                    
                    log.error(event="failed extracting",
                        source=chunk[0][SOURCE],
                        num_items=len(chunk),
                        exc_info=True,
                    )

        log.info(event="extractor completed", total_extracted=total)
        return total


DIGEST_SYS = """
TASK=DERIVE target_information FROM content IF specified
RULES=
exclude:unspecified data,implied assessments,assumptions,null,empty fields
avoid:markdown,prose,code_fences,null_placeholders,implied_information
RESPONSE=JSON object matching schema; compact; avoid newline char `\n`
"""
DIGEST_INST = """
TRANSLATE content TO english 
DERIVE target_information FROM english_content
TARGET_INFORMATION=
{description}
CONTENT=
{input_text}
"""

   
class Digestor:
    cache: StateCacheBase
    digestor: TextAnalystBase

    def __init__(
        self,
        cache: StateCacheBase,
        model_path: str,
        context_len: int,
        batch_size: int,
        **model_kwargs
    ):
        self.cache = cache
        if not model_kwargs: model_kwargs = {}
        if temperature := os.getenv("DIGESTOR_TEMPERATURE"): model_kwargs["temperature"] = float(temperature)
        if top_p := os.getenv("DIGESTOR_TOP_P"): model_kwargs["top_p"] = float(top_p)
        if top_k := os.getenv("DIGESTOR_TOP_K"): model_kwargs["top_k"] = int(top_k)
        if repetition_penalty := os.getenv("DIGESTOR_REPETITION_PENALTY"): model_kwargs["repetition_penalty"] = float(repetition_penalty)
        if presence_penalty := os.getenv("DIGESTOR_PRESENCE_PENALTY"): model_kwargs["presence_penalty"] = float(presence_penalty)
        self.digestor = create_text_analyst(
            model_path=model_path,
            context_len=context_len,
            instruction=DIGEST_SYS,
            input_template=DIGEST_INST,
            output_model=Digest,                       
            enable_thinking=False,
            max_new_tokens=2048,
            **model_kwargs
        )
        self.batch_size = batch_size

    @classmethod
    def _article_to_str(cls, article: dict) -> str:
        text = f"reported:{article[CREATED].strftime('%Y-%m-%d')}\n"
        if (article.get(KIND) == POST) and article.get(AUTHOR):
            text += f"author:{article[AUTHOR]}\n"
        return text + article[CONTENT][:MAX_DOCUMENT_LEN<<2]

    def digest_beans(self, beans: list[dict]):
        digests = self.digestor.run_batch(list(map(self._article_to_str, beans)))
        updates = [
            {
                URL: b[URL],
                DIGEST: dig
            }
            for b, d in zip(beans, digests)
            if d and (dig := d.model_dump())
        ]
        return updates

    @log_runtime(logger=log)
    def run(self):
        total = 0

        with self.digestor:
            for chunk in decache_beans(self.cache, states=COLLECTED, exclude_states=DIGESTED, batch_size=self.batch_size, log=log):
                try:
                    updates =self.digest_beans(chunk)
                    log.info(event="digested", source=chunk[0][SOURCE], num_items=len(updates))
                    total += encache_beans(self.cache, DIGESTED, updates)

                except Exception as e:
                    log.error(event="failed digesting",
                        source=chunk[0][SOURCE],
                        num_items=len(chunk),
                        exc_info=True,
                    )

        log.info(event="digestor completed", total_digested=total)
        return total


CLUSTER_LIMIT = int(os.getenv('CLUSTER_LIMIT', 500))
CLUSTER_EPS = float(os.getenv("CLUSTER_EPS", 0.4))

class Clusterer:
    cache: StateCacheBase
    cls_cache: ClassificationCache

    def __init__(self, cache: StateCacheBase, cls_cache: ClassificationCache, batch_size: int = BATCH_SIZE, log=log):
        self.cache = cache
        self.cls_cache = cls_cache
        self.batch_size = batch_size  

    def cluster_beans(self, beans: list[dict]):
        self.cls_cache.store(BEANS, [{ID: b[URL], EMBEDDING: b[EMBEDDING]} for b in beans])  
        related_list = self.cls_cache.batch_search(BEANS, [bean[EMBEDDING] for bean in beans], distance=CLUSTER_EPS, top_n=CLUSTER_LIMIT)
        return [            
            {
                URL: b[URL],
                RELATED: related
            }
            for b, related in zip(beans, related_list)
        ]

    @log_runtime(logger=log)
    def run(self):
        total = 0
        for chunk in decache_beans(self.cache, states=EMBEDDED, exclude_states=CLUSTERED, batch_size=self.batch_size):
            # no need for try-except since this does not have a OOM issue
            updates = self.cluster_beans(chunk)
            log.info(event="clustered", source=chunk[0].get(SOURCE, chunk[0][URL]), num_items=len(updates))
            total += encache_beans(self.cache, CLUSTERED, updates)            
            
        log.info(event="clusterer completed", total_clustered=total)
        return total
