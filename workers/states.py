from utils.collections import merge_lists
from utils.fields import (
    BRIEFING,
    CATEGORIES,
    DIGEST,
    EMBEDDING,
    ENTITIES,
    ID,
    RELATED,
    SENTIMENTS,
)

# Cache table names
BEANS = "beans"
COMPOSITES = "composites"
PUBLISHERS = "publishers"
CHATTERS = "chatters"

# Pipeline states (processing cache state machine)
COLLECTED = "collected"
EMBEDDED = "embedded"
CLUSTERED = "clustered"
DIGESTED = "digested"
CLASSIFIED = "classified"
CONSOLIDATED = "consolidated"
EXTRACTED = "extracted"
BEANSACKED = "beansacked"
CUPBOARDED = "cupboarded"
