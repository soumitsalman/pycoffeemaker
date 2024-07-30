## DATA MODELS ##
from bson import ObjectId
from pydantic import BaseModel, Field
from typing import Optional

CHANNEL = "social media group/forum"
POST = "social media post"
NEWS = "news"
BLOG = "blog/journal"
COMMENT = "social media comment"

# names of important fields of collections
K_LIKES = "likes"
K_COMMENTS = "comments"
K_TRENDSCORE = "trend_score"
K_MAPPED_URL = "mapped_url"

class Chatter(BaseModel):
    # this is the url of bean it represents
    url: Optional[str] = None 
    updated: Optional[int] = None
    source: Optional[str] = None
    text: Optional[str] = None
    cid: Optional[str] = None
    channel: Optional[str] = None
    # this is the url in context of the social media post that contains the bean represented 'url'
    # when the bean itself is a post (instead of a news/article url) container url is the same as 'url'
    container_url: Optional[str] = None 
    likes: Optional[int] = Field(default=None)
    likes_ratio: Optional[float] = Field(default=None)
    comments: Optional[int] = Field(default=None)
    subscribers: Optional[int] = Field(default=None)
    trend_score: Optional[int] = Field(default=None)
    
    def digest(self):
        return f"From: {self.source}\nBody: {self.text}"

K_URL="url"
K_KIND = "kind"
K_TITLE = "title"
K_TEXT = "text"
K_SOURCE = "source"
K_EMBEDDING = "embedding"
K_SUMMARY = "summary"
K_UPDATED = "updated"
K_CLUSTER_ID = "cluster_id"

class Bean(BaseModel):
    id: str|ObjectId = Field(default=None, alias="_id")
    url: str
    updated: Optional[int] = None
    source: Optional[str] = None
    title: Optional[str] = None
    kind: Optional[str] = None
    text: Optional[str] = None
    image_url: Optional[str] = None
    author: Optional[str] = None    
    created: Optional[int] = None   
    categories: Optional[list[str]] = Field(default=None) 
    tags: Optional[list[str]] = Field(default=None)
    highlights: Optional[list[str]] = Field(default=None)
    summary: Optional[str] = Field(default=None)
    embedding: Optional[list[float]] = Field(default=None)
    search_score: Optional[float|int] = Field(default=None)
    likes: Optional[int] = Field(default=None),
    comments: Optional[int] = Field(default=None)
    trend_score: Optional[int] = Field(default=None)    
    cluster_id: Optional[str] = Field(default=None)

    def digest(self):
        return f"{self.kind} from {self.source}\nTitle: {self.title}\nBody: {self.text}"
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed=True

K_ID = "_id"
K_KEYPHRASE = "keyphrase"
K_EVENT="event"
K_DESCRIPTION = "description"
K_URLS = "urls"

class Highlight(BaseModel):
    id: str|ObjectId = Field(default=None, alias="_id")
    url: Optional[str] = None
    keyphrase: str = None
    event: str = None
    description: str 
    updated: Optional[int] = None
    embedding: Optional[list[float]] = None
    cluster_id: Optional[str] = None
    trend_score: Optional[int] = None

    def digest(self) -> str:
        return (self.keyphrase or "" )+((": "+self.description) if self.description else "")
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed=True

    
class Source(BaseModel):
    url: str
    kind: str
    name: str
    cid: Optional[str] = None
    
