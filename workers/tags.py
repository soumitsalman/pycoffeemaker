from nlp import valid_tags


def merge_tags(*tag_lists) -> list[str]:
    return list(
        set(valid_tags(item for tag_list in tag_lists if tag_list for item in tag_list))
    )
