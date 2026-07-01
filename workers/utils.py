from nlp import valid_tags

#--------------------------------#
# Object Types
#--------------------------------#
BEANS = "beans"
COMPOSITES = "composites"
PUBLISHERS = "publishers"
CHATTERS = "chatters"

#--------------------------------#
# Processing States
#--------------------------------#
COLLECTED = "collected"
EMBEDDED = "embedded"
CLUSTERED = "clustered"
DIGESTED = "digested"
CLASSIFIED = "classified"
CONSOLIDATED = "consolidated"
EXTRACTED = "extracted"
BEANSACKED = "beansacked"
CUPBOARDED = "cupboarded"

#--------------------------------#
# Bean, Publisher, Chatter, Event, Signal Fields
#--------------------------------#
ID = "id"
DIGEST = "digest"
BRIEFING = "briefing"
EMBEDDING = "embedding"
ENTITIES = "entities"
CATEGORIES = "categories"
SENTIMENTS = "sentiments"
RELATED = "related"

#--------------------------------#
# Utility Functions
#--------------------------------#
merge_lists = lambda *lists: [item for sublist in lists if sublist for item in sublist]
merge_tags = lambda *tag_lists: list(
    set(valid_tags(item for tag_list in tag_lists if tag_list for item in tag_list))
)
