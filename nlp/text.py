import re

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
