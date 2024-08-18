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
    likes: Optional[int] = None
    likes_ratio: Optional[float] = None
    comments: Optional[int] = None
    subscribers: Optional[int] = None
    trend_score: Optional[int] = None
    
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
K_CLUSTER_ID = "cluster_id"
K_HIGHLIGHTS = "highlights"
K_IMAGEURL = "image_url"
K_CREATED = "created"
K_AUTHOR = "author"

class Bean(BaseModel):
    url: str
    updated: Optional[int] = None
    source: Optional[str] = None
    channel: Optional[str] = None
    title: Optional[str] = None
    kind: Optional[str] = None
    text: Optional[str] = None
    image_url: Optional[str] = None
    author: Optional[str] = None    
    created: Optional[int] = None   
    categories: Optional[list[str]] = None
    tags: Optional[str|list[str]] = None
    highlights: Optional[list[str]] = None
    summary: Optional[str] = None
    embedding: Optional[list[float]] = None
    search_score: Optional[float|int] = None
    likes: Optional[int] = None
    comments: Optional[int] = None
    trend_score: Optional[int] = None    
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
    