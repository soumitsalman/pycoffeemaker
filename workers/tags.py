from nlp import valid_tags


def merge_tags(*tag_values) -> list[str]:
    return list(
        set(
            valid_tags(
                item
                for value in tag_values
                if value
                for item in ([value] if isinstance(value, str) else value)
            )
        )
    )
