from uuid import NAMESPACE_URL, UUID, uuid4, uuid5


def generate_uuid(url: str | None) -> UUID:
    if url:
        return uuid5(NAMESPACE_URL, url.strip().lower())
    return uuid4()
