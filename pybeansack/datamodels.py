## DATA MODELS ##
from bson import ObjectId
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

# CHANNEL = "social media group/forum"
POST = "post"
NEWS = "news"
BLOG = "blog"
COMMENTS = "comments"

# names of important fields of collections
K_CONTAINER_URL = "container_url"
K_LIKES = "likes"
K_COMMENTS = "comments"
K_SHARES = "shares"

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
    search_score: Optional[float|int] = None
    likes: Optional[int] = Field(default=0)
    comments: Optional[int] = Field(default=0)
    shares: Optional[int] = Field(default=0)
    similars: Optional[int] = Field(default=1) # a bean is always similar to itself
    # latest_likes: Optional[int] = Field(default=0)
    # latest_comments: Optional[int] = Field(default=0)
    # latest_shares: Optional[int] = Field(default=0)
    trend_score: Optional[int] = Field(default=1) # a bean is always similar to itself
    shared_in: Optional[list[str]] = None
    cluster_id: Optional[str] = None

    def digest(self):
        return f"{self.kind} from {self.source}\n{self.title}\n{self.text}"
    

K_ID = "_id"
K_KEYPHRASE = "keyphrase"
K_EVENT="event"
K_DESCRIPTION = "description"
K_URLS = "urls"

class Highlight(BaseModel):
    id: str = Field(default=None, alias="_id")
    url: Optional[str] = None
    description: str 
    updated: Optional[int] = None
    embedding: Optional[list[float]] = None
    cluster_id: Optional[str] = None
    trend_score: Optional[int] = None

    def digest(self) -> str:
        return self.description
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed=True

    
class Source(BaseModel):
    url: str
    kind: str
    name: str
    cid: Optional[str] = None

class ChatterAnalysis(BaseModel):
    url: str
    likes: Optional[int] = None
    comments: Optional[int] = None
    shares: Optional[int] = None
    shared_in: Optional[list[str]] = None
    latest_likes: Optional[int] = None
    latest_comments: Optional[int] = None
    latest_shares: Optional[int] = None
    latest_shared_in: Optional[list[str]] = None
    trend_score: Optional[int] = None
    