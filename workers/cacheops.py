from processingcache import StateCacheBase
from .states import *
from itertools import batched

def _clean_updates(updates: list[dict]) -> list[dict]:
    for update in updates:
        for k in [k for k, v in update.items() if not v]:
            update.pop(k)
    return updates

def encache_beans(cache: StateCacheBase, state: str, beans: list[dict]):
    from icecream import ic
    count = cache.set(BEANS, state, ic(_clean_updates(beans)))
    return count if count is not None else len(beans)

def decache_beans(cache: StateCacheBase, states: list[str], exclude_states: list[str], batch_size: int, *, log) -> list[dict]:
    beans = cache.get(BEANS, states=states, exclude_states=exclude_states)
    if log: log.info(event=f"starting {log.name}", target_state=exclude_states, num_items=len(beans))
    for chunk in batched(beans, batch_size):
        yield chunk