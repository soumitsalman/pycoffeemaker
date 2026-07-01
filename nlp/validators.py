import re
import types
from typing import Any, Optional, Union, get_args, get_origin

import textcase
from pydantic import BaseModel

_TAG_MAX_LEN = 50
_TICKER_MAX_LEN = 6

_UNDETERMINED = {"n/a", "na", "none", "unmentioned", "not mentioned", "unspecified", "undetermined", "not specified", "not found", "null"}

_snake = lambda s: re.sub(r'_+', '_', textcase.snake(s)).strip('_')

def cleanup_names(items: list[str]):
    """Remove leading/trailing non-alphanumeric characters, filter out empty and undetermined values, and deduplicate while preserving original casing of first occurrence."""
    if not items: return items

    texts = map(lambda text: re.sub(r"^[\W_]+|[\W_]+$", "", text).strip(), items)
    texts = filter(lambda tag: tag and len(tag) <= _TAG_MAX_LEN and tag.lower() not in _UNDETERMINED, texts)
    return list({item.lower(): item for item in texts}.values())

def valid_stock_tickers(items: list[str]):
    return list(filter(lambda x: len(x) <= _TICKER_MAX_LEN and x.isupper(), cleanup_names(items)))

_IMPACT_LEVELS = {"low", "medium", "high", "critical", "transformative"}
def valid_impact_or_risk(val: Optional[str]):
    if val and val.lower() in _IMPACT_LEVELS: return val.lower()

def valid_tags(items: str|list[str]):
    """Converts the tags into snake_case"""
    return list(map(_snake, cleanup_names(items)))

def valid_context_tag(tag: str):
    tag = _snake(tag)
    if len(tag) <= _TAG_MAX_LEN and tag not in _UNDETERMINED:
        return tag

def valid_future_outlook(outlook: str):
    if outlook and outlook.lower() not in _UNDETERMINED:
        return outlook

def valid_cross_domain_impacts(impacts: list[str]):
    if not impacts: return impacts

    unwrap = lambda s: s.strip().removeprefix("<").removesuffix(">")
    def split_and_unwrap(s: str):
        if ":" in s: 
            domain, rest = s.split(":", 1)
            return f"{_snake(unwrap(domain))}: {unwrap(rest)}"
        return unwrap(s)
    return [split_and_unwrap(impact) for impact in impacts if impact]
    

_CLEANUP_FUNCTIONS = {
    "regions": valid_tags,
    "people": valid_tags,
    "products": valid_tags,
    "companies": valid_tags,    
    "entities": valid_tags,
    "tags": valid_tags,
    "stock_tickers": valid_stock_tickers,    
    "macro_context": valid_context_tag,
    "cross_domain_impacts": valid_cross_domain_impacts,
    "event_type": valid_context_tag,
    "impact_level": valid_impact_or_risk,
    "future_outlook": valid_future_outlook,
    "forecast": valid_future_outlook,
}

def cleanup_fields(digest, __context):
    for field, cleanup_func in _CLEANUP_FUNCTIONS.items():
        if val := getattr(digest, field, None):
            setattr(digest, field, cleanup_func(val))

# ────────────────────────────────────────────────
# Schema generation utilities
# ────────────────────────────────────────────────

def model_text_schema(model: BaseModel):
    return "\n".join(
        f"{fname}={finfo.description}"
        for fname, finfo in model.model_fields.items()
    )

def typeinfo(annotation: Any) -> str:
    """Render a readable type name from a Pydantic FieldInfo annotation."""

    if annotation is None or annotation is type(None):  # noqa: E721
        return "null"
    if annotation is Any:
        return "any"

    # Primitive normalization (match requested output)
    if annotation is int:
        return "int"
    if annotation is float:
        return "float"
    if annotation is bool:
        return "bool"
    if annotation is str:
        return "str"

    origin = get_origin(annotation)

    # `Annotated[T, ...]` -> unwrap to `T`
    if origin is getattr(types, "AnnotatedAlias", object()) or str(origin) == "typing.Annotated":
        args = get_args(annotation)
        return typeinfo(args[0]) if args else "any"
    if origin is getattr(__import__("typing"), "Annotated", object()):
        args = get_args(annotation)
        return typeinfo(args[0]) if args else "any"

    # `T | U` (py3.10+) and `Union[T, U]`
    if origin is Union or isinstance(annotation, types.UnionType):
        args = list(get_args(annotation))
        non_none = [a for a in args if a is not type(None)]  # noqa: E721
        if len(non_none) != len(args):
            if len(non_none) == 1:
                return f"{typeinfo(non_none[0])}|exclude empty"
            return "|".join(typeinfo(a) for a in non_none) + "|exclude empty"
        return "|".join(typeinfo(a) for a in args)

    if origin in (list, List):
        (item_t,) = get_args(annotation) or (Any,)
        return f"list[{typeinfo(item_t)}]"

    if isinstance(annotation, type):
        # Normalize common typing-ish names while keeping unknowns readable
        return getattr(annotation, "__name__", str(annotation))

    # Fallback for uncommon typing constructs
    return str(annotation).replace("typing.", "")

def text_value(val, item_delim="|", field_delim="\n") -> str:
    lines = []
    for field_name in val.model_fields:
        if value := getattr(val, field_name):
            if isinstance(value, list): value_str = item_delim.join(str(v) for v in value)
            else: value_str = str(value)
            lines.append(f"{field_name}:{value_str}")
    return field_delim.join(lines)
