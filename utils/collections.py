def merge_lists(*lists) -> list:
    return [item for sublist in lists if sublist for item in sublist]


def non_null_fields(items: list[dict]) -> list[str]:
    return list({k for item in items for k, v in item.items() if v is not None})
