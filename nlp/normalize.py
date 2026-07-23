import re
import types
from typing import Any, Optional, Union, get_args, get_origin

import textcase
from pydantic import BaseModel

_TAG_MAX_LEN = 50
_TICKER_MAX_LEN = 6

_UNDETERMINED = {"n/a", "na", "none", "unmentioned", "not mentioned", "unspecified", "undetermined", "not specified", "not found", "null"}
_IMPACT_LEVELS = {"low", "medium", "high", "critical", "transformative"}


_strip = lambda txt: txt.strip().replace('\n', ' ')
_snake = lambda s: re.sub(r'_+', '_', textcase.snake(s)).strip('_')

def normalize_names(items: list[str]):
    """Remove leading/trailing non-alphanumeric characters, filter out empty and undetermined values, and deduplicate while preserving original casing of first occurrence."""
    if not items: return items

    texts = map(lambda text: re.sub(r"^[\W_]+|[\W_]+$", "", text).strip(), items)
    texts = filter(lambda tag: tag and len(tag) <= _TAG_MAX_LEN and tag.lower() not in _UNDETERMINED, texts)
    return list({item.lower(): item for item in texts}.values())

def normalize_actions_and_briefing(items: str | list[str]):    
    if isinstance(items, str): return _strip(items)
    elif isinstance(items, list): return [_strip(item) for item in items if item.strip()]
    raise ValueError("Wrong item types. Should `str` | `list[str]`")

def normalize_stock_tickers(items: list[str]):
    return list(filter(lambda x: len(x) <= _TICKER_MAX_LEN and x.isupper(), normalize_names(items)))

def normalize_impact_or_risk(val: Optional[str]):
    if val and val.lower() in _IMPACT_LEVELS: return val.lower()

def normalize_tags(items: str|list[str]):
    """Converts the tags into snake_case"""
    return list(map(_snake, normalize_names(items)))

def normalize_context_tag(tag: str):
    tag = _snake(tag)
    if len(tag) <= _TAG_MAX_LEN and tag not in _UNDETERMINED:
        return tag

def normalize_future_outlook(outlook: str):
    if outlook and outlook.lower() not in _UNDETERMINED:
        return outlook

def normalize_cross_domain_impacts(impacts: list[str]):
    if not impacts: return impacts

    unwrap = lambda s: s.strip().removeprefix("<").removesuffix(">")
    def split_and_unwrap(s: str):
        if ":" in s: 
            domain, rest = s.split(":", 1)
            return f"{_snake(unwrap(domain))}: {unwrap(rest)}"
        return unwrap(s)
    return [split_and_unwrap(impact) for impact in impacts if impact]


_NORMALIZE_FUNCTIONS = {
    "regions": normalize_tags,
    "people": normalize_tags,
    "products": normalize_tags,
    "companies": normalize_tags,    
    "entities": normalize_tags,
    "stock_tickers": normalize_stock_tickers,    
    "macro_context": normalize_context_tag,
    "cross_domain_impacts": normalize_cross_domain_impacts,
    "event_type": normalize_context_tag,
    "impact_level": normalize_impact_or_risk,
    "confidence": normalize_impact_or_risk,
    "future_outlook": normalize_future_outlook,
    "forecast": normalize_future_outlook,
    "actions": normalize_actions_and_briefing,
    "events": normalize_actions_and_briefing,
    "briefing": normalize_actions_and_briefing
}
def normalize_fields(data):
    for field, normalize_func in _NORMALIZE_FUNCTIONS.items():
        if val := getattr(data, field, None):
            setattr(data, field, normalize_func(val))

def merge_tags(*tag_values) -> list[str]:
    return list(
        set(
            normalize_tags(
                item
                for value in tag_values
                if value
                for item in ([value] if isinstance(value, str) else value)
            )
        )
    )

def merge_lists(*lists) -> list:
    return list(
        set(
            item
            for value in lists
            if value
            for item in ([value] if isinstance(value, str) else value)
        )
    )


split_parts = lambda text, sep=r"[,]+": [
    part.strip() for part in re.split(sep, text) if part.strip()
]

def remove_before(text: str, sub: str) -> str:
    index = text.find(sub)
    if index >= 0:
        return text[index + len(sub):]
    return text


def remove_after(text: str, sub: str) -> str:
    index = text.find(sub)
    if index >= 0:
        return text[:index]
    return text
