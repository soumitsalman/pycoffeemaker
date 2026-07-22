import re
import types
from typing import Any, Optional, Union, get_args, get_origin
from pydantic import BaseModel


def apply_model_json_constraints(schema: dict, list_item_max_len: dict[str, int] = {}) -> dict:
    for name, definition in schema["properties"].items():
        if "anyOf" in definition:
            definition["type"] = "string"
            del definition["anyOf"]
        if item_max := list_item_max_len.get(name):
            items = definition.setdefault("items", {"type": "string"})
            items["maxLength"] = item_max
    return schema

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
