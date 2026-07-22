from sklearn.cluster import DBSCAN
import numpy as np
import os
from processingcache import StateCacheBase
from nlp import (
    Briefing, 
    TextAnalystBase, 
    merge_tags,
    create_text_analyst,
)
from itertools import chain, batched
from utils.fields import *
from utils import VECTOR_LEN, get_logger, now, log_runtime
from datacollectors import POST
from concurrent.futures import ThreadPoolExecutor
import hashlib
from datetime import datetime
from nlp import Entities
from .cacheops import encache_beans, _clean_updates
from .states import *
from icecream import ic


log = get_logger("consolidatorworker")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count()))

CONSOLIDATION_EPS = float(os.getenv("CONSOLIDATION_EPS", 0.5))
CONSOLIDATION_RELATED_WINDOW = int(os.getenv("CONSOLIDATION_RELATED_WINDOW", 30))
CONSOLIDATION_MAX_SIZE = int(os.getenv("CONSOLIDATION_MAX_SIZE", 64))
CONSOLIDATION_MIN_SIZE = int(os.getenv("CONSOLIDATION_MIN_SIZE", 4))

BRIEFING_SYS = """
TASK=CREATE intelligence_briefing FROM event_stream
RULES=
selection:identify_dominant_theme_from_most_common_related_items;exclude_outliers
grounding:only_items_linked_to_thesis;strict_tracing;no_synthesis_of_unrelated_items
phrasing:plain_sentences,structured,dynamic,specific,granular,direct
tone:informative,objective,concrete,analytical,data_driven
avoid:forced_unification,clickbait,sensationalism,ambiguity,vagueness,generic_phrasing,speculative_narrative,emotive_language,technical_inconsistencies,political_biases
RESPONSE=JSON object matching schema
"""
BRIEFING_INST = """
CREATE intelligence_briefing FROM event_stream
BRIEFING_ELEMENTS=
{description}
STEPS=
0.CLUSTER event_items BY shared related,actions,events,impacts FROM event_stream
1.CREATE dominant_theme_or_main_thesis FROM the largest/most_coherent cluster WHERE items CONTAIN SAME(related OR causal link)
2.FILTER event_items FROM event_stream BY relation TO dominant_theme
3.DISCARD item WHERE NOT EXIST IN (dominant_theme OR retained_items)
4.DETERMINE relationships between related,actions,events,impacts FROM retained_items
5.DETERMINE causal_chain driving or preceding the retained events FROM retained_items
6.DETERMINE impacts,implications,target_groups,forecast FROM retained_items
CONSTRAINTS=
USE retained_items ONLY
EVENT_STREAM=
{input_text}
"""
IGNORE_WORD_GAMES = ['word_game', 'daily_puzzle', 'nyt', 'wordle']

class Consolidator:
    """Consolidates events and data to create consolidated briefings and signals"""
    cache: StateCacheBase
    consolidator: TextAnalystBase

    def __init__(
        self,
        cache: StateCacheBase,
        model_path: str,
        context_len: int,
        batch_size: int = BATCH_SIZE,
        **model_kwargs
    ):
        self.cache = cache
        if not model_kwargs: model_kwargs = {}
        if temperature := os.getenv("CONSOLIDATOR_TEMPERATURE"): model_kwargs["temperature"] = float(temperature)
        if top_p := os.getenv("CONSOLIDATOR_TOP_P"): model_kwargs["top_p"] = float(top_p)
        if repetition_penalty := os.getenv("CONSOLIDATOR_REPETITION_PENALTY"): model_kwargs["repetition_penalty"] = float(repetition_penalty)
        if top_k := os.getenv("CONSOLIDATOR_TOP_K"): model_kwargs["top_k"] = int(top_k)
        self.consolidator = create_text_analyst(model_path=model_path, context_len=context_len, instruction=BRIEFING_SYS, input_template=BRIEFING_INST, output_model=Briefing, enable_thinking=True, max_new_tokens=3072, **model_kwargs)
        self.batch_size = batch_size

    def _get_beans(self, **kwargs) -> list[dict]:
        beans = self.cache.get(BEANS, **{k:v for k,v in kwargs.items() if v})
        # remove content, summary, and title from beans to save memory
        beans = [
            b for b in beans 
            if b.get(EMBEDDING) \
                and len(b.get(EMBEDDING)) == VECTOR_LEN \
                and b.get(DIGEST) \
                and not any(tag in IGNORE_WORD_GAMES for tag in b.get(TAGS, []))
        ]
        for bean in beans:
            bean.pop(CONTENT, None)
            bean.pop(SUMMARY, None)
            bean.pop(TITLE, None)
        return beans        

    def _expand_group(self, group: dict) -> dict:
        # fetch and expand only if we don't have enough beans and 
        # we can make enough beans by fetching
        existing = {b[URL] for b in group["data"]}
        ids_to_fetch = list(set(chain.from_iterable(b.get(RELATED, []) for b in group["data"])) - existing)
        if len(group["data"]) < CONSOLIDATION_MAX_SIZE and len(ids_to_fetch) > 0 and len(ids_to_fetch) + len(group["data"]) >= CONSOLIDATION_MIN_SIZE:
            related = self._get_beans(
                states=[COLLECTED, EMBEDDED, EXTRACTED, DIGESTED], 
                ids=ids_to_fetch, 
                limit=CONSOLIDATION_MAX_SIZE,
                window=CONSOLIDATION_RELATED_WINDOW
            )
            group["data"].extend(related)
        return group

    def _create_consolidation_groups(self, beans: list[dict]) -> list[dict]:
        groups = _group_items(beans, CONSOLIDATION_EPS, CONSOLIDATION_MIN_SIZE)       
        log.info(event="initial groups", num_items=len(groups))
        with ThreadPoolExecutor(max_workers=32) as exec:
            groups = list(exec.map(self._expand_group, groups))

        # split oversized groups into smaller groups
        resized_groups = []
        for group in groups:
            if len(group["data"]) >= CONSOLIDATION_MAX_SIZE:
                resized_groups.extend(
                    {"data": gr_data, "embedding": np.median([b[EMBEDDING] for b in gr_data], axis=0).tolist()} 
                    for gr_data in batched(group["data"], CONSOLIDATION_MAX_SIZE)
                    if len(gr_data) >= CONSOLIDATION_MIN_SIZE
                )
            elif len(group["data"]) >= CONSOLIDATION_MIN_SIZE:
                resized_groups.append(group)
        log.info(event="consolidation groups", num_items=len(resized_groups))
        return resized_groups

    def consolidate_bean_groups(self, groups: list[dict]):
        for chunk in batched(groups, self.batch_size):
            try:
                briefings = self.consolidator.run_batch(list(map(_group_to_str, chunk)))

                composite_updates, bean_updates = [], []
                for group, br in zip(chunk, briefings):
                    if not br: continue
                    
                    consolidated_item = {
                        ID: now().strftime("%Y_%m_%d") + "_" + hashlib.sha1(br.briefing.encode("utf-8")).hexdigest(),
                        CREATED: max(b[CREATED] for b in group['data']), 
                        EMBEDDING: group[EMBEDDING],
                        DIGEST: br.model_dump(),
                        RELATED: [b[URL] for b in group['data']]
                    } 
                    if tags := _merge_consolidated_tags(group['data']):
                        consolidated_item[TAGS] = tags
                        consolidated_item[DIGEST] |= {TAGS: tags}
                        
                    composite_updates.append(consolidated_item)
                    bean_updates.extend({URL: b[URL]} for b in group['data'])

                log.info(event="consolidated composites", composites=len(composite_updates), beans=len(bean_updates))
                yield composite_updates, bean_updates

            except Exception as e:
                log.error(event="failed consolidating",
                    source=chunk[0]["data"][0][SOURCE],
                    num_items=len(chunk),
                    exc_info=True,
                )

    def _fetch_and_group_beans(self) -> list[dict]:
        beans = self._get_beans(
            states=[COLLECTED, EMBEDDED, CLUSTERED, EXTRACTED, DIGESTED],
            exclude_states=CONSOLIDATED,
        )
        log.info(event="starting consolidator", num_items=len(beans))
        return self._create_consolidation_groups(beans)

    @log_runtime(logger=log)
    def run(self):
        total_composites, total_beans = 0, 0
        with ThreadPoolExecutor(max_workers=1) as exec:
            groups_future = exec.submit(self._fetch_and_group_beans)
            with self.consolidator:
                groups = groups_future.result()
                if not groups:
                    return 0
                for composite_updates, bean_updates in self.consolidate_bean_groups(groups):
                    total_composites += self.cache.set(COMPOSITES, COLLECTED, _clean_updates(composite_updates))
                    total_beans += encache_beans(self.cache, CONSOLIDATED, bean_updates)

        log.info(event="total consolidated", composites=total_composites, beans=total_beans)
        return total_composites + total_beans

def _value_to_str(value) -> str:
    if isinstance(value, list): return "|".join(_value_to_str(v) for v in value)
    if isinstance(value, dict): return "|".join(f"{k}:{_value_to_str(v)}" for k,v in value.items() if v)
    if isinstance(value, datetime): return value.strftime('%Y-%m-%d')
    return str(value)

_OUTLOOK_KEYS = ("forecast", "future_outlook")
_PRIORITY_DIGEST_KEYS = ("briefing", "actions")
_ENTITY_KEYS = tuple(Entities.model_fields)
_CONSOLIDATED_DIGEST_TAG_FIELDS = (
    TAGS, REGIONS, PEOPLE, PRODUCTS, COMPANIES, STOCK_TICKERS,
)

def _merge_consolidated_tags(beans: list[dict]) -> list[str]:
    """Combine classification and digest tags from a consolidation group."""
    return merge_tags(
        *(bean.get(CATEGORIES) for bean in beans),
        *(bean[ENTITIES].get(field) for bean in beans if bean.get(ENTITIES) for field in _CONSOLIDATED_DIGEST_TAG_FIELDS),
    )

def _entity_tags(entities: dict) -> list:
    if not entities: return
    tags = set()
    for key in _ENTITY_KEYS:
        if v := entities.get(key):
            tags.update(v if isinstance(v, list) else [v])
    return tags

def _bean_to_str(bean: dict) -> str:
    lines = []
    if created := bean.get(CREATED):
        lines.append(f"reported:{_value_to_str(created)}")
    if tags := _entity_tags(bean.get(ENTITIES)):
        lines.append(f"related:{_value_to_str(tags)}")
    if digest := bean.get(DIGEST):
        for key in _PRIORITY_DIGEST_KEYS:
            if v := digest.get(key):
                lines.append(f"{key}:{_value_to_str(v)}")
        for key, v in digest.items():
            if not v or key in _PRIORITY_DIGEST_KEYS or key in _OUTLOOK_KEYS or key in _ENTITY_KEYS:
                continue
            lines.append(f"{key}:{_value_to_str(v)}")
        for key in _OUTLOOK_KEYS:
            if v := digest.get(key):
                lines.append(f"{key}:{_value_to_str(v)}")
    return "\n".join(lines)

def _group_to_str(group: dict) -> str:
    return "\n\n".join(_bean_to_str(b) for b in group["data"])

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
