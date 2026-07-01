from functools import cached_property
from typing_extensions import deprecated
from rfc3339 import rfc3339
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime

from utils.config import BEANSACK_CLEANUP_WINDOW, CLUSTER_EPS, VECTOR_LEN
from utils.dates import ndays_ago, ndays_ago_str, now
from utils.fields import *

# CHANNEL = "social media group/forum"
POST = "post"
JOB = "job"
NEWS = "news"
BLOG = "blog"
OPED = "opinion"

SYSTEM = "__SYSTEM__"

DIGEST_COLUMNS = [URL, CREATED, GIST]
CONTENT_COLUMNS = [URL, CREATED, SOURCE, TITLE, CONTENT]

class Chatter(BaseModel):
    """Social media engagement stats of an article/bean (specified by `url`)."""
    chatter_url: Optional[str] = Field(default=None, min_length=1, description="The URL of the social medium post/comment that contains the article URL.")
    url: str = Field(min_length=1, description="The URL of the article mentioned in the social medium post/comment.")
    source: Optional[str] = Field(default=None, description="The publisher ID of the social medium from which the data was collected.")
    forum: Optional[str] = Field(default=None, description="The social medium group/forum/community/page from which the data was collected.")
    collected: Optional[datetime] = Field(default=None, description="The date and time when the data was collected.")
    likes: int = Field(default=0, description="The cumulative total number of likes (lower bound).")
    comments: int = Field(default=0, description="The cumulative total number of comments (lower bound).")
    subscribers: int = Field(default=0, description="The cumulative total number of subscribers (lower bound).")

    def to_tuple(self) -> tuple:
        return (
            self.chatter_url,
            self.url,
            self.source,
            self.forum,
            self.collected,
            self.likes,
            self.comments,
            self.subscribers
        )

    model_config = ConfigDict(
        populate_by_name = True,
        arbitrary_types_allowed=False,
        exclude_none = True,
        exclude_unset = True,
        by_alias=True,
        json_encoders={datetime: rfc3339},
        dtype_specs = {
            'chatter_url': 'string',
            'url': 'string',
            'source': 'string',
            'forum': 'string',
            'likes': 'uint32',
            'comments': 'uint32',
            'subscribers': 'uint32'
        }
    )


class Publisher(BaseModel):
    """Metadata of the website, publication or social medium from which an article or chatter is sourced."""
    source: str = Field(min_length=1, description="The publisher ID/domain name of the publisher. This matches the source field in Bean.")
    base_url: str = Field(min_length=1, description="The base URL of the publisher.")
    site_name: Optional[str] = Field(default=None, description="The name of the site.")
    description: Optional[str] = Field(default=None, description="A description/details of the publisher.")
    favicon: Optional[str] = Field(default=None, description="The URL of the publisher's favicon.")
    rss_feed: Optional[str] = Field(default=None, description="The URL of the publisher's RSS feed.")
    collected: Optional[datetime] = Field(default=None, description="The date and time when the publisher information was collected.")

    model_config = ConfigDict(
        populate_by_name = True,
        arbitrary_types_allowed = False,
        exclude_none = True,
        exclude_unset = True,
        by_alias=True,
        json_encoders={datetime: rfc3339},
        dtype_specs = {
            'source': 'string',
            'base_url': 'string',
            'site_name': 'string',
            'description': 'string',
            'favicon': 'string',
            'rss_feed': 'string'
        }
    )


class Bean(BaseModel):    
    """Metadata of an article such as a news or blog post."""
    url: str = Field(description="The URL of the article.")
    kind: Optional[str] = Field(default=None, description="The content type of the article, e.g., news, blog, oped, job, post.")
    source: Optional[str] = Field(default=None, description="The publisher ID of the article.")
    title: Optional[str] = Field(default=None, description="The title of the article.")
    summary: Optional[str] = Field(default=None, description="A summary of the article.")
    content: Optional[str] = Field(default=None, description="The full content of the article if available.")
    restricted_content: Optional[bool] = Field(default=None, description="Indicates if the content is restricted.")
    image_url: Optional[str] = Field(default=None, description="The URL of the article's featured image.")
    author: Optional[str] = Field(default=None, description="The author of the article (if available).")
    created: Optional[datetime] = Field(default=None, description="The publish date of the article.")
    collected: Optional[datetime] = Field(default=None, description="The date when the article was collected into the system.")

    # llm fields
    embedding: Optional[list[float]] = Field(default=None, description="The vector embedding for the article content.")
    entities: Optional[list[str]] = Field(default=None, description="Named entities mentioned in the article content.")
    regions: Optional[list[str]] = Field(default=None, description="Geographic regions mentioned in the article content.")
    categories: Optional[list[str]] = Field(default=None, description="Categories/topics of the article content.")
    sentiments: Optional[list[str]] = Field(default=None, description="Sentiments expressed in the article content.")
  
    model_config = ConfigDict(
        populate_by_name = True,
        arbitrary_types_allowed = False,
        exclude_none = True,
        exclude_unset = True,
        by_alias=True,
        json_encoders={datetime: rfc3339},
        dtype_specs = {            
            'kind': 'string',
            'title': 'string',
            'summary': 'string',
            'content': 'string',
            'author': 'string',
            'source': 'string',
            'image_url': 'string',
            'embedding': 'object',
            'regions': 'object', 
            'entities': 'object'  
        }
    )

class TrendingBean(Bean):
    """Bean with additional fields for tracking social media engagement and propagation across other publishers"""
    updated: Optional[datetime] = Field(default=None, description="The last updated date during chatter aggregation.")
    likes: Optional[int] = Field(default=None, description="The number of likes.")
    comments: Optional[int] = Field(default=None, description="The number of comments.")
    shares: Optional[int] = Field(default=None, description="The number of shares.")
    subscribers: Optional[int] = Field(default=None, description="The number of subscribers.")
    related: Optional[int] = Field(default=None, description="The size of the cluster.")
    related_urls: Optional[list[str]] = Field(default=None, description="Related bean URLs.")
    trend_score: Optional[int] = Field(default=None, description="The trend score of the bean.")

    model_config = ConfigDict(
        populate_by_name = True,
        arbitrary_types_allowed = False,
        exclude_none = True,
        exclude_unset = True,
        by_alias=True,
        json_encoders={datetime: rfc3339},
    )

class AggregatedBean(TrendingBean, Publisher):
    """Bean with additional trend stats and publisher fields."""
    # modifying publisher fields for rendering
    source: Optional[str] = Field(default=None, description="The domain name that matches the source field in Bean.")
    base_url: Optional[str] = Field(default=None, description="The base URL of the publisher.")
    # query support fields    
    distance: Optional[float|int] = Field(default=None, description="The distance score for queries.")

    model_config = ConfigDict(
        populate_by_name = True,
        arbitrary_types_allowed=False,
        exclude_none = True,
        exclude_unset = True,
        by_alias=True,
        json_encoders={datetime: rfc3339},
    )


# @deprecated("User will move to application-specific models outside of pybeansack")
# class User(BaseModel):
#     id: Optional[str] = Field(default=None, alias="_id", description="The unique identifier of the user.")
#     email: str = Field(description="The email address of the user.")
#     name: Optional[str] = Field(default=None, description="The name of the user.")
#     image_url: Optional[str] = Field(default=None, description="The URL of the user's profile image.")
#     linked_accounts: Optional[list[str]] = Field(default=None, description="List of linked account identifiers.")
#     following: Optional[list[str]] = Field(default=None, description="List of users or entities the user is following.")
#     created: Optional[datetime] = Field(default=None, description="The creation date of the user account.")
#     updated: Optional[datetime] = Field(default=None, description="The last updated date of the user account.")

#     class Config:
#         populate_by_name = True
#         arbitrary_types_allowed=False
#         by_alias=True

# @deprecated("Page will move to application-specific models outside of pybeansack")
# class Page(BaseModel):
#     id: str = Field(alias="_id", description="The unique identifier of the page.")
#     title: Optional[str] = Field(default=None, description="The title of the page.")
#     description: Optional[str] = Field(default=None, description="A description of the page.")
#     created: Optional[datetime] = Field(default_factory=datetime.now, description="The creation date of the page.")
#     owner: Optional[str] = Field(default=SYSTEM, description="The owner of the page.")
#     public: Optional[bool] = Field(default=False, description="Indicates if the page is public.")
#     related: Optional[list[str]] = Field(default=None, description="Related page identifiers.")
    
#     query_urls: Optional[list[str]] = Field(default=None, alias="urls", description="Query URLs for the page.")
#     query_kinds: Optional[list[str]] = Field(default=None, alias="kinds", description="Query kinds for the page.")
#     query_sources: Optional[list[str]] = Field(default=None, alias="sources", description="Query sources for the page.")
#     query_tags :Optional[list[str]] = Field(default=None, alias="tags", description="Query tags for the page.")
#     query_text: Optional[str] = Field(default=None, alias="text", description="Query text for the page.")
#     query_embedding: Optional[list[float]] = Field(default=None, alias="embedding", description="Query embedding for the page.")
#     query_distance: Optional[float] = Field(default=None, alias="distance", description="Query distance for the page.")
    
#     class Config:
#         populate_by_name = True
#         arbitrary_types_allowed=False
#         by_alias=True

# _EXCLUDE_AUTHORS = ["[no-author]", "noreply", "hidden", "admin", "isbpostadmin", "unknown", "anonymous"]

# distinct = lambda items, key: list({getattr(item, key): item for item in items}.values())  # deduplicate by url
# non_null_fields = lambda items: list(set().union(*[[k for k, v in item.items() if v] for item in items]))

# bean_filter = lambda x: bool(x.title and x.collected and x.created and x.source and x.kind)
# chatter_filter = lambda x: bool(x.chatter_url and x.url and (x.likes or x.comments or x.subscribers))
# publisher_filter = lambda x: bool(x.source and x.base_url)
# count_words = lambda text: min(len(text.split()) if text else 0, (1<<15)-1)  # SMALLINT max value

# clean_text = lambda text: text.strip() if text and text.strip() else None
# clean_author = lambda author: clean_text(author) if author and author.lower() not in _EXCLUDE_AUTHORS else None

# def prepare_beans_for_store(items: list[Bean]) -> list[Bean]:
#     if not items: return items

#     for item in items:
#         item.url = clean_text(item.url)
#         item.kind = clean_text(item.kind)
#         item.source = clean_text(item.source)
#         item.title = clean_text(item.title)
#         item.title_length = count_words(item.title)
#         item.summary = clean_text(item.summary)
#         item.summary_length = count_words(item.summary)
#         item.content = clean_text(item.content)
#         item.content_length = count_words(item.content)
#         item.author = clean_text(item.author)
#         item.image_url = clean_text(item.image_url)
#         item.created = item.created or now()
#         item.collected = item.collected or now()
#         item.author = clean_author(item.author)
#         if not item.created.tzinfo: item.created.replace(tzinfo=timezone.utc)
    
#     items = distinct(items, URL)
#     return list(filter(bean_filter, items))

# def prepare_publishers_for_store(items: list[Publisher]) -> list[Publisher]:
#     if not items: return items

#     for item in items:        
#         item.source = clean_text(item.source)
#         item.base_url = clean_text(item.base_url)
#         item.favicon = clean_text(item.favicon)
#         item.rss_feed = clean_text(item.rss_feed)
#         item.description = clean_text(item.description)
#         item.site_name = clean_text(item.site_name)
#         item.collected = item.collected or now()

#     items = distinct(items, SOURCE)
#     return list(filter(publisher_filter, items))

# def prepare_chatters_for_store(items: list[Chatter]) -> list[Chatter]:
#     if not items: return items

#     for item in items:        
#         item.chatter_url = clean_text(item.chatter_url)
#         item.url = clean_text(item.url)
#         item.forum = clean_text(item.forum)
#         item.source = clean_text(item.source)
        
#     return list(filter(chatter_filter, items))