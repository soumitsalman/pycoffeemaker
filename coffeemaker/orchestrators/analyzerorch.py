import logging
import os
from datetime import datetime
from itertools import batched, chain
from textcase import snake
from coffeemaker.processingcache.base import StateCacheBase
from coffeemaker.processingcache.clscache import ClassificationCache
from nlp import (
    Digest, Briefing, 
    NamedEntityExtractor, DigestorBase, EmbedderBase, 
    valid_tags, create_embedder, create_digestor,
    DIGEST_SYS, DIGEST_INST, BRIEFING_SYS, BRIEFING_INST,
)
from datacollectors.utils import (
    SUMMARY,
    TAGS,
    TITLE,
    URL,
    CONTENT,
    SOURCE,
    CREATED,
)

from .utils import *
from icecream import ic

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count()))

log = logging.getLogger("analyzerworker")
clean_updates = lambda packlist: [{k:v for k,v in pack.items() if v} for pack in packlist if pack]

VECTOR_LEN = int(os.getenv("VECTOR_LEN", 384))

class Embedder:
    cache: StateCacheBase
    embedder: EmbedderBase

    def __init__(
        self,
        cache: StateCacheBase,
        embedder_model_path: str,
        embedder_context_len: int,
    ):
        self.cache = cache
        self.embedder = create_embedder(model_path=embedder_model_path, context_len=embedder_context_len)

    def embed_beans(self, beans: list[dict], batch_size: int):
        for chunk in batched(beans, batch_size):
            try:
                vectors = self.embedder.embed_documents([bean[CONTENT] for bean in chunk])
                updates = clean_updates([
                    {URL: b[URL], EMBEDDING: vec}
                    for b, vec in zip(chunk, vectors)
                    if vec and len(vec) == VECTOR_LEN
                ])
                log.info(
                    "embedded",
                    extra={"source": chunk[0][SOURCE], "num_items": len(updates)},
                )
                yield updates
            except Exception:
                log.error(
                    "failed embedding",
                    extra={"source": chunk[0][SOURCE], "num_items": len(chunk)},
                    exc_info=True,
                    stack_info=True,
                )

    @log_runtime(logger=log)
    def run(self, batch_size: int = BATCH_SIZE):
        beans = self.cache.get(BEANS, states=COLLECTED, exclude_states=EMBEDDED)
        log.info("starting embedder", extra={"source": run_id(), "num_items": len(beans)})
        if not beans: return 0
        
        with self.embedder:
            total = 0
            for updates in self.embed_beans(beans, batch_size):
                count = self.cache.set(BEANS, EMBEDDED, updates)
                total += (count or len(updates))
            log.info("total embedded", extra={"source": run_id(), "num_items": total})
            return total


class Extractor:
    cache: StateCacheBase
    extractor: NamedEntityExtractor

    def __init__(
        self,
        cache: StateCacheBase,
        extractor_model_path: str,
        extractor_context_len: int
    ):
        self.cache = cache
        self.extractor = NamedEntityExtractor(
            model_path=extractor_model_path,
            context_len=extractor_context_len,
            threshold=0.35,
        )

    def extract_beans(self, beans: list[dict], batch_size: int):
        for chunk in batched(beans, batch_size):
            try:
                extractions = self.extractor.run_batch([b[CONTENT] for b in chunk])
                updates = clean_updates([
                    {
                        URL: b[URL],
                        ENTITIES: ents.model_dump()
                    } if ents else {URL:b[URL]}
                    for b, ents in zip(chunk, extractions)
                ])
                log.info("extracted", extra={"source": chunk[0][SOURCE], "num_items": len(updates)})
                yield updates
            except Exception:
                log.error(
                    "failed extracting",
                    extra={"source": chunk[0][SOURCE], "num_items": len(chunk)},
                    exc_info=True,
                    stack_info=True,
                )

    @log_runtime(logger=log)
    def run(self, batch_size: int = BATCH_SIZE):
        beans = self.cache.get(BEANS, states=COLLECTED, exclude_states=EXTRACTED)
        log.info("starting extractor", extra={"source": run_id(), "num_items": len(beans)})
        if not beans: return 0
        
        with self.extractor:
            total = 0
            for updates in self.extract_beans(beans, batch_size):
                count = self.cache.set(BEANS, EXTRACTED, updates)
                total += (count or len(updates))
            log.info("total extracted", extra={"source": run_id(), "num_items": total})
            return total


class Digestor:
    cache: StateCacheBase
    digestor: DigestorBase

    def __init__(
        self,
        cache: StateCacheBase,
        digestor_model_path: str,
        digestor_context_len: int,
    ):
        self.cache = cache
        self.digestor = create_digestor(
            model_path=digestor_model_path,
            context_len=digestor_context_len,
            system_template=DIGEST_SYS,
            input_template=DIGEST_INST,
            output_model=Digest,
        )

    def digest_beans(self, beans: list[dict], batch_size: int):
        for chunk in batched(beans, batch_size):
            try:
                digests = self.digestor.run_batch([bean[CONTENT] for bean in chunk])
                updates = clean_updates([
                    {
                        URL: b[URL],
                        DIGEST: d.model_dump()
                    }
                    for b, d in zip(chunk, digests)
                    if d
                ])
                log.info("digested", extra={"source": chunk[0][SOURCE], "num_items": len(updates)})
                yield updates
            except Exception:
                log.error(
                    "failed digesting",
                    extra={"source": chunk[0][SOURCE], "num_items": len(chunk)},
                    exc_info=True,
                    stack_info=True,
                )

    @log_runtime(logger=log)
    def run(self, batch_size: int = BATCH_SIZE):
        beans = self.cache.get(BEANS, states="collected", exclude_states="digested")
        log.info("starting digestor", extra={"source": run_id(), "num_items": len(beans)})
        if not beans: return 0
        
        with self.digestor:
            total = 0
            for updates in self.digest_beans(beans, batch_size):
                count = self.cache.set(BEANS, "digested", updates)
                total += (count or len(updates))
            log.info("total digested", extra={"source": run_id(), "num_items": total})
            return total


CLASSIFICATION_LIMIT = int(os.getenv("CLASSIFICATION_LIMIT", 2))
CLUSTER_LIMIT = int(os.getenv('CLUSTER_LIMIT', 500))
CLUSTER_EPS = float(os.getenv("CLUSTER_EPS", 0.4))

class Classifier:
    cache: StateCacheBase
    cls_cache: ClassificationCache

    def __init__(self, cache: StateCacheBase, cls_cache: ClassificationCache):
        self.cache = cache
        self.cls_cache = cls_cache

    def classify_beans(self, beans: list[dict], batch_size: int):
        for chunk in batched(beans, batch_size):
            embeddings = [bean[EMBEDDING] for bean in chunk]
            categories = self.cls_cache.batch_search("categories", embeddings, top_n=CLASSIFICATION_LIMIT)
            sentiments = self.cls_cache.batch_search("sentiments", embeddings, top_n=CLASSIFICATION_LIMIT)
            updates = clean_updates([
                {
                    URL: bean[URL],
                    CATEGORIES: valid_tags(cats),
                    SENTIMENTS: valid_tags(sents),
                }
                for bean, cats, sents in zip(chunk, categories, sentiments)
                if cats and sents
            ])
            log.info("classified", extra={"source": chunk[0][URL], "num_items": len(updates)})
            # debug
            print("# MISSING CATEGORIES AND SENTIMENTS")
            [print(bean[URL], len(embs), cats, sents) for bean, embs, cats, sents in zip(chunk, embeddings, categories, sentiments) if not cats or not sents]
            print("# END MISSING CATEGORIES AND SENTIMENTS")
            yield updates

    def cluster_beans(self, beans: list[dict], batch_size: int):
        self.cls_cache.store(BEANS, beans)
        for chunk in batched(beans, batch_size):
            embeddings = [bean[EMBEDDING] for bean in chunk]
            related_list = self.cls_cache.batch_search(BEANS, embeddings, distance=CLUSTER_EPS, top_n=CLUSTER_LIMIT)
            updates = clean_updates([
                {
                    URL: bean[URL],
                    RELATED: list(filter(lambda x: x != bean[URL], related)),
                }
                for bean, related in zip(chunk, related_list)
            ])
            log.info("clustered", extra={"source": chunk[0][URL], "num_items": len(updates)})
            yield updates

    @log_runtime(logger=log)
    def run(self, batch_size: int = BATCH_SIZE):
        # run classification before clustering
        beans = self.cache.get(BEANS, states=EMBEDDED, exclude_states=CLASSIFIED)
        log.info("starting classifier", extra={"source": run_id(), "num_items": len(beans)})
        classified = 0
        embedded = [b for b in beans if EMBEDDING in b]
        for updates in self.classify_beans(embedded, batch_size):
            count = self.cache.set(BEANS, CLASSIFIED, updates)
            classified += (count or len(updates))
        log.info("total classified", extra={"source": run_id(), "num_items": classified})

        # run clustering after classification
        beans = self.cache.get(BEANS, states=EMBEDDED, exclude_states=CLUSTERED)
        log.info("starting clusterer", extra={"source": run_id(), "num_items": len(beans)})
        clustered = 0
        embedded = [b for b in beans if EMBEDDING in b]
        for updates in self.cluster_beans(embedded, batch_size):
            count = self.cache.set(BEANS, CLUSTERED, updates)
            clustered += (count or len(updates))
        log.info("total clustered", extra={"source": run_id(), "num_items": clustered})
        return classified, clustered


CONSOLIDATION_EPS = float(os.getenv("CONSOLIDATION_EPS", 0.5))
CONSOLIDATION_WINDOW = int(os.getenv("CONSOLIDATION_WINDOW", 2))
CONSOLIDATION_RELATED_WINDOW = int(os.getenv("CONSOLIDATION_RELATED_WINDOW", 30))
CONSOLIDATION_MAX_SIZE = int(os.getenv("CONSOLIDATION_MAX_SIZE", 64))
CONSOLIDATION_MIN_SIZE = int(os.getenv("CONSOLIDATION_MIN_SIZE", 2))

class Consolidator:
    """Consolidates events and data to create consolidated briefings and signals"""
    cache: StateCacheBase
    consolidator: DigestorBase

    def __init__(
        self,
        cache: StateCacheBase,
        consolidator_model_path: str,
        consolidator_context_len: int,
        **consolidator_kwargs
    ):
        self.cache = cache
        self.consolidator = create_digestor(
            model_path=consolidator_model_path,
            context_len=consolidator_context_len,
            system_template=BRIEFING_SYS,
            input_template=BRIEFING_INST,
            output_model=Briefing,
            **consolidator_kwargs
        )

    def _get_beans(self, states: list[str], exclude_states: list[str] = None, ids: list[str] = None, window: int = None, limit: int = None) -> list[dict]:
        beans = self.cache.get(BEANS, states=states, exclude_states=exclude_states, ids=ids, window=window, limit=limit)
        # remove content, summary, and title from beans to save memory
        beans = [b for b in beans if b.get(EMBEDDING)]
        for bean in beans:
            bean.pop(CONTENT, None)
            bean.pop(SUMMARY, None)
            bean.pop(TITLE, None)
        return beans        

    def _expand_group(self, group: dict) -> dict:
        if len(group["data"]) < CONSOLIDATION_MAX_SIZE:
            related = self._get_beans(
                states=[COLLECTED, EMBEDDED, DIGESTED], 
                ids=list(chain.from_iterable(b.get(RELATED, []) for b in group["data"])), 
                window=CONSOLIDATION_RELATED_WINDOW, 
                limit=CONSOLIDATION_MAX_SIZE
            )
            group["data"].extend(related)
            group["embedding"] = np.median([b[EMBEDDING] for b in group["data"]], axis=0).tolist()
        return group

    def _create_consolidation_groups(self, beans: list[dict]) -> list[dict]:
        groups = _group_items(beans, CONSOLIDATION_EPS)[:10]
        ic(len(groups))
        groups = [self._expand_group(group) for group in groups if len(group['data']) >= CONSOLIDATION_MIN_SIZE]   
        ic(len(groups)) 
        return groups

    def run(self, batch_size: int = BATCH_SIZE):
        beans = self._get_beans(states=[COLLECTED, EMBEDDED, CLUSTERED, DIGESTED], exclude_states=CONSOLIDATED, window=CONSOLIDATION_WINDOW)
        log.info("starting consolidator", extra={"source": run_id(), "num_items": len(beans)})

        groups = self._create_consolidation_groups(beans)
        if not groups: return 0

        with self.consolidator:
            total_briefings, total_beans = 0, 0

            for chunk in batched(groups, batch_size):
                briefings = ic(self.consolidator.run_batch(list(map(_group_to_str, chunk))))

                briefing_updates = [
                    {
                        ID: now().strftime("%b_%d_%Y") + '_' + snake(br.briefing),
                        CREATED: max(b[CREATED] for b in group['data']), 
                        EMBEDDING: group[EMBEDDING],
                        # TODO: add classification
                        TAGS: br.tags,
                        DIGEST: br.model_dump(),
                        RELATED: list(chain.from_iterable(b[URL] for b in group['data']))
                    } 
                    for group, br in zip(chunk, briefings)
                    if br
                ]
                count = self.cache.set(SIGNALS, COLLECTED, briefing_updates)
                count = count or len(briefing_updates)
                log.info("consolidated briefings", extra={"source": run_id(), "num_items": count})
                total_briefings += count

                bean_updates = [
                    {URL: b[URL]} 
                    for group, br in zip(chunk, briefings) for b in group['data']
                    if br
                ]
                count = self.cache.set(BEANS, CONSOLIDATED, bean_updates)
                count = count or len(bean_updates)
                log.info("consolidated beans", extra={"source": run_id(), "num_items": count})
                total_beans += count

            log.info("total consolidated briefings", extra={"source": run_id(), "num_items": total_briefings})
            log.info("total consolidated beans", extra={"source": run_id(), "num_items": total_beans})
            return total_briefings + total_beans

def _value_to_str(value) -> str:
    if isinstance(value, list): return "|".join(_value_to_str(v) for v in value)
    if isinstance(value, dict): return "|".join(f"{k}:{_value_to_str(v)}" for k,v in value.items() if v)
    if isinstance(value, datetime): return value.strftime('%A, %b-%d-%Y')
    return str(value)

def _bean_to_str(bean: dict) -> str:
    lines = []
    if created := bean.get(CREATED): lines.append(f"reported:{_value_to_str(created)}")
    if digest := bean.get(DIGEST): lines.extend(f"{k}:{_value_to_str(v)}" for k, v in digest.items() if v)
    return "\n".join(lines)

def _group_to_str(group: dict) -> str:
    return "\n\n".join(_bean_to_str(b) for b in group["data"])

from sklearn.cluster import DBSCAN
import numpy as np

def _group_items(items: list, distance: float) -> list[dict]:
    """Groups items based on L2 distance between embeddings.
    
    Args:
        items: List of items. Must have an `embedding` field.
        distance: L2 distance between two items <= distance are considered similar
    
    Returns:
        List of groups where each item has an `embedding` field and a `data` field containing the original item.
        `embedding` is the median embedding of the group.
        `data` is the list of original items in the group.
    """
    if not items: return items

    # Cluster vectors using DBSCAN; points within `distance` and density-connected
    # end up in the same cluster. We treat noise points as singletons.
    labels = DBSCAN(eps=distance, min_samples=1, metric="euclidean").fit_predict(np.array([item[EMBEDDING] for item in items]))
    # Build groups of indices: clusters (label >= 0) and singleton noise points
    cluster_map: dict[int, list[int]] = {}
    for idx, label in enumerate(labels):
        if label == -1: cluster_map[len(labels)+idx] = [idx] # Noise point, treat as its own cluster
        else: cluster_map.setdefault(label, []).append(idx)
    
    return [
        dict(
            embedding=np.median([i[EMBEDDING] for i in group], axis=0).tolist(), 
            data=group
        ) 
        for group in map(lambda idxs: [items[i] for i in idxs], cluster_map.values())
    ]