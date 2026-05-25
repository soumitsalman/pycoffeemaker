from concurrent.futures import ThreadPoolExecutor
import logging
import os
from datetime import datetime
from itertools import batched, chain
from textcase import snake
from .workercache.base import StateCacheBase
from .workercache.clscache import ClassificationCache
from nlp import (
    Digest, 
    Briefing, 
    EntityExtractor, 
    EmbedderBase,
    MicroAgentBase, 
    valid_tags, 
    create_embedder, 
    create_micro_agent
)
from datacollectors import (
    CONTENT, CREATED, SUMMARY, TAGS, TITLE, URL, SOURCE
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
    extractor: EntityExtractor

    def __init__(
        self,
        cache: StateCacheBase,
        extractor_model_path: str,
        extractor_context_len: int
    ):
        self.cache = cache
        self.extractor = EntityExtractor(
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


DIGEST_SYS = """
TASK=EXTRACT target_information FROM content IF specified
TARGET_INFORMATION=
{description}
RULES=
exclude:unspecified data,implied assessments,assumptions,null,empty fields
avoid:markdown,prose,code_fences,null_placeholders,implied_information
RESPONSE=JSON object matching schema
"""
DIGEST_INST = """
EXTRACT target_information FROM content IF specified
=== content ===
{input_text}
"""

class Digestor:
    cache: StateCacheBase
    digestor: MicroAgentBase

    def __init__(
        self,
        cache: StateCacheBase,
        digestor_model_path: str,
        digestor_context_len: int,
    ):
        self.cache = cache
        self.digestor = create_micro_agent(
            model_path=digestor_model_path,
            context_len=digestor_context_len,
            instruction=DIGEST_SYS.format(description=Digest.model_text_schema()),
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
        beans = self.cache.get(BEANS, states=COLLECTED, exclude_states=DIGESTED)
        log.info("starting digestor", extra={"source": run_id(), "num_items": len(beans)})
        if not beans: return 0
        
        with self.digestor:
            total = 0
            for updates in self.digest_beans(beans, batch_size):
                count = self.cache.set(BEANS, DIGESTED, updates)
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
# CONSOLIDATION_WINDOW = int(os.getenv("CONSOLIDATION_WINDOW", 1))
CONSOLIDATION_RELATED_WINDOW = int(os.getenv("CONSOLIDATION_RELATED_WINDOW", 30))
CONSOLIDATION_MAX_SIZE = int(os.getenv("CONSOLIDATION_MAX_SIZE", 40))
CONSOLIDATION_MIN_SIZE = int(os.getenv("CONSOLIDATION_MIN_SIZE", 4))

BRIEFING_SYS = """
TASK=CREATE intelligence_briefing FROM event_stream
BRIEFING_ELEMENTS=
{description}
RULES=
grounding:normative,multi_events,strict_tracing
phrasing:structured,dynamic,specific,granular,direct
tone:informative,objective,concrete,analytical,data_driven
avoid:clickbait,sensationalism,ambiguity,vagueness,generic_phrasing,speculative_narrative,emotive_language,technical_inconsistencies,political_biases
RESPONSE=JSON object matching schema
"""
BRIEFING_INST = """
CREATE intelligence_briefing FROM event_stream
STEPS=
1.DETERMINE relationships between events,entities,domains,datapoints,impacts
2.DETERMINE causal_chain driving or preceding the events
3.DETERMINE impacts,implications,target_groups of the events sequence
4.DETERMINE forecast FROM impacts,implications
=== EVENT STREAM ===
{input_text}
"""

class Consolidator:
    """Consolidates events and data to create consolidated briefings and signals"""
    cache: StateCacheBase
    consolidator: MicroAgentBase

    def __init__(
        self,
        cache: StateCacheBase,
        consolidator_model_path: str,
        consolidator_context_len: int,
        **consolidator_kwargs
    ):
        self.cache = cache
        self.consolidator = create_micro_agent(
            model_path=consolidator_model_path,
            context_len=consolidator_context_len,
            instruction=BRIEFING_SYS.format(description=Briefing.model_text_schema()),
            input_template=BRIEFING_INST,
            output_model=Briefing,
            max_tokens=4096,
            **consolidator_kwargs
        )

    def _get_beans(self, states: list[str], exclude_states: list[str] = None, ids: list[str] = None, window: int = None, limit: int = None) -> list[dict]:
        beans = self.cache.get(BEANS, states=states, exclude_states=exclude_states, ids=ids, window=window, limit=limit)
        # remove content, summary, and title from beans to save memory
        beans = [b for b in beans if b.get(EMBEDDING) and b.get(DIGEST)]
        for bean in beans:
            bean.pop(CONTENT, None)
            bean.pop(SUMMARY, None)
            bean.pop(TITLE, None)
        return beans        

    def _expand_group(self, group: dict) -> dict:
        # fetch and expand only if we don't have enough beans and 
        # we can make enough beans by fetching
        ids_to_fetch = list(chain.from_iterable(b.get(RELATED, []) for b in group["data"]))
        if len(group["data"]) < CONSOLIDATION_MAX_SIZE and len(ids_to_fetch) > 0 and len(ids_to_fetch) + len(group["data"]) >= CONSOLIDATION_MIN_SIZE:
            related = self._get_beans(
                states=[COLLECTED, EMBEDDED, DIGESTED], 
                ids=ids_to_fetch, 
                limit=CONSOLIDATION_MAX_SIZE
            )
            group["data"].extend(related)
        return group

    def _create_consolidation_groups(self, beans: list[dict]) -> list[dict]:
        groups = _group_items(beans, CONSOLIDATION_EPS, CONSOLIDATION_MIN_SIZE)       
        log.info("initial groups", extra={"source": run_id(), "num_items": len(groups)})
        with ThreadPoolExecutor(max_workers=32) as exec:
            groups = list(exec.map(self._expand_group, groups))

        groups = [group for group in groups if len(group["data"]) >= CONSOLIDATION_MIN_SIZE]   
        for group in groups:
            group["data"] = group["data"][:CONSOLIDATION_MAX_SIZE]
            group["embedding"] = np.median([b[EMBEDDING] for b in group["data"]], axis=0).tolist()
        
        log.info("consolidation groups", extra={"source": run_id(), "num_items": len(groups)}) 
        return groups

    def consolidate_bean_groups(self, groups: list[dict], batch_size: int):
        for chunk in batched(groups, batch_size):
            try:
                briefings = self.consolidator.run_batch(list(map(_group_to_str, chunk)))

                composite_updates, bean_updates = [], []
                for group, br in zip(chunk, briefings):
                    if not br: continue

                    composite_updates.append({
                        ID: now().strftime("%Y_%m_%d") + '_' + snake(br.briefing),
                        CREATED: max(b[CREATED] for b in group['data']), 
                        EMBEDDING: group[EMBEDDING],
                        TAGS: merge_tags(br.tags, *[b.get(CATEGORIES, []) for b in group['data']]),
                        DIGEST: br.model_dump(),
                        RELATED: [b[URL] for b in group['data']]
                    })
                    bean_updates.extend({URL: b[URL]} for b in group['data'])

                log.info("consolidated composites", extra={"source": run_id(), "num_items": len(composite_updates)})
                log.info("consolidated beans", extra={"source": run_id(), "num_items": len(bean_updates)})
                yield composite_updates, bean_updates

            except Exception:
                log.error(
                    "failed consolidating",
                    extra={"source": chunk[0]['data'][0][SOURCE], "num_items": len(chunk)},
                    exc_info=True,
                    stack_info=True,
                )

    def run(self, batch_size: int = BATCH_SIZE):
        beans = self._get_beans(states=[COLLECTED, EMBEDDED, CLASSIFIED, CLUSTERED, DIGESTED], exclude_states=CONSOLIDATED)
        log.info("starting consolidator", extra={"source": run_id(), "num_items": len(beans)})

        groups = self._create_consolidation_groups(beans)
        if not groups: return 0

        with self.consolidator:
            total_composites, total_beans = 0, 0

            for composite_updates, bean_updates in self.consolidate_bean_groups(groups, batch_size):
                [print({k:v for k,v in update.items() if k != EMBEDDING}) for update in composite_updates]
                count = self.cache.set(COMPOSITES, COLLECTED, composite_updates)
                total_composites += (count or len(composite_updates))
                count = self.cache.set(BEANS, CONSOLIDATED, bean_updates)
                total_beans += (count or len(bean_updates))

            log.info("total consolidated composites", extra={"source": run_id(), "num_items": total_composites})
            log.info("total consolidated beans", extra={"source": run_id(), "num_items": total_beans})
            return total_composites + total_beans

def _value_to_str(value) -> str:
    if isinstance(value, list): return ",".join(_value_to_str(v) for v in value)
    if isinstance(value, dict): return "|".join(f"{k}:{_value_to_str(v)}" for k,v in value.items() if v)
    if isinstance(value, datetime): return value.strftime('%Y-%m-%d')
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

def _group_items(items: list, distance: float, min_group_size: int) -> list[dict]:
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
    labels = DBSCAN(eps=distance, min_samples=min_group_size, metric="euclidean").fit_predict(np.array([item[EMBEDDING] for item in items]))
    # Build groups of indices: clusters (label >= 0) and singleton noise points
    cluster_map: dict[int, list[int]] = {}
    for idx, label in enumerate(labels):
        if label == -1: cluster_map[len(items)+idx] = [idx] # Noise point, treat as its own cluster
        else: cluster_map.setdefault(label, []).append(idx)
    
    return [
        dict(
            embedding=np.median([i[EMBEDDING] for i in group], axis=0).tolist(), 
            data=group
        ) 
        for group in map(lambda idxs: [items[i] for i in idxs], cluster_map.values())
    ]