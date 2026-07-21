def clear_null_bytes(obj):
    """Recursively remove null bytes (\\u0000) from all strings in the object."""
    if isinstance(obj, dict):
        return {k: clear_null_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clear_null_bytes(v) for v in obj]
    elif isinstance(obj, str):
        return obj.replace("\u0000", "NULL_BYTE")
    return obj
