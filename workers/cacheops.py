from processingcache import StateCacheBase
from utils.fields import KIND
from utils.kinds import POST
from .states import *
from itertools import batched

_SKIP_KINDS = frozenset({POST})
filtered_beans=lambda beans: [b for b in beans if b.get(KIND) not in _SKIP_KINDS]

def _clean_updates(updates: list[dict]) -> list[dict]:
    for update in updates:
        for k in [k for k, v in update.items() if not v]:
            update.pop(k)
    return updates

def encache_beans(cache: StateCacheBase, state: str, beans: list[dict]):
    count = cache.set(BEANS, state, _clean_updates(filtered_beans(beans)))
    return count if count is not None else len(beans)

def decache_beans(cache: StateCacheBase, states: list[str], exclude_states: list[str], batch_size: int, *, log, filter_bean = lambda x: True) -> list[dict]:
    beans = cache.get(BEANS, states=states, exclude_states=exclude_states)
    beans = filtered_beans(beans)
    if log: log.info(event=f"starting {log.name}", target_state=exclude_states, num_items=len(beans))
    for chunk in batched(beans, batch_size):
        yield chunk