## DATA MODELS ##
from bson import ObjectId
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

# CHANNEL = "social media group/forum"
POST = "post"
JOB = "job"
NEWS = "news"
BLOG = "blog"
COMMENTS = "comments"

# names of important fields of collections
K_ID="_id"
K_URL="url"
K_KIND = "kind"
K_CATEGORIES = "categories"
K_TAGS = "tags"
K_TITLE = "title"
K_TEXT = "text"
K_SOURCE = "source"
K_CHANNEL = "channel"
K_EMBEDDING = "embedding"
K_SUMMARY = "summary"
K_UPDATED = "updated"
K_COLLECTED = "collected"
K_CLUSTER_ID = "cluster_id"
K_HIGHLIGHTS = "highlights"
K_IMAGEURL = "image_url"
K_CREATED = "created"
K_AUTHOR = "author"
K_SEARCH_SCORE = "search_score"
K_SIMILARS = "similars"
K_LATEST_LIKES = "latest_likes"
K_LATEST_COMMENTS = "latest_comments"
K_LATEST_SHARES = "latest_shares"
K_TRENDSCORE = "trend_score"
K_CONTAINER_URL = "container_url"
K_LIKES = "likes"
K_COMMENTS = "comments"
K_SHARES = "shares"
K_OWNER = "owner"
K_FOLLOWING = "following"
K_DESCRIPTION = "description"
SYSTEM = "__SYSTEM__"

class Bean(BaseModel):
    id: str = Field(default=None, alias="_id")
    url: str    
    source: Optional[str] = None
    title: Optional[str] = None
    kind: Optional[str] = None
    text: Optional[str] = None
    image_url: Optional[str] = None
    author: Optional[str] = None    
    created: Optional[datetime] = None 
    collected: Optional[datetime] = None
    updated: Optional[datetime] = None
    categories: Optional[list[str]] = None
    tags: Optional[list[str]|str] = None
    summary: Optional[str] = None
    embedding: Optional[list[float]] = None
    
    likes: Optional[int] = Field(default=0)
    comments: Optional[int] = Field(default=0)
    shares: Optional[int] = Field(default=0)
    similars: Optional[int] = Field(default=1) # a bean is always similar to itself
    trend_score: Optional[int] = Field(default=0) # a bean is always similar to itself
    shared_in: Optional[list[str]] = None
    cluster_id: Optional[str] = None

    search_score: Optional[float|int] = None

    def digest(self) -> str:
        if not self.summary: return self.text or self.title
        tag_text = {"\n\nTags: "+(", ".join(self.tags))} if self.tags else ""
        return f"# {self.title}\n\n{self.summary}{tag_text}"
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False

class Chatter(BaseModel):
    # this is the url of bean it represents
    url: Optional[str] = None 
    # this is the url in context of the social media post that contains the bean represented 'url'
    # when the bean itself is a post (instead of a news/article url) container url is the same as 'url' 
    chatter_url: Optional[str] = None
    source: Optional[str] = None
    channel: Optional[str] = None    
    collected: Optional[datetime] = None
   
    likes: Optional[int] = Field(default=0)    
    comments: Optional[int] = Field(default=0)
    shares: Optional[int] = Field(default=0)
    subscribers: Optional[int] = Field(default=0)
    
    def digest(self):
        return f"From: {self.source}\nBody: {self.text}"
    
class Source(BaseModel):
    url: str
    kind: str
    name: str
    cid: Optional[str] = None

class ChatterAnalysis(BaseModel):
    url: str
    likes: Optional[int] = 0
    comments: Optional[int] = 0
    shares: Optional[int] = 0
    shared_in: Optional[list[str]] = None
    last_collected: Optional[datetime] = None
    likes_change: Optional[int] = 0
    comments_change: Optional[int] = 0
    shares_change: Optional[int] = 0
    shared_in_change: Optional[list[str]] = None
    trend_score: Optional[int] = 0
    
class User(BaseModel):
    id: Optional[str] = Field(default=None, serialization_alias="_id")  
    email: str = None 
    name: Optional[str] = None
    image_url: Optional[str] = None  
    linked_accounts: Optional[list[str]] = None
    following: Optional[list[str]] = None
    created: Optional[datetime] = None
    updated: Optional[datetime] = None

    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False

class Barista(BaseModel):
    id: str = Field(alias="_id")
    title: Optional[str] = None
    description: Optional[str] = None
    created: Optional[datetime] = Field(default_factory=datetime.now)
    owner: Optional[str] = Field(default=SYSTEM)
    public: Optional[bool] = Field(default=False)
    
    query_urls: Optional[list[str]] = Field(default=None, alias="urls")
    query_kinds: Optional[list[str]] = Field(default=None, alias="kinds")
    query_sources: Optional[list[str]] = Field(default=None, alias="sources")   
    query_tags :Optional[list[str]] = Field(default=None, alias="tags")
    query_text: Optional[str] = Field(default=None, alias="text")
    query_embedding: Optional[list[float]] = Field(default=None, alias="embedding")
    query_distance: Optional[float] = Field(default=None, alias="distance")
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False