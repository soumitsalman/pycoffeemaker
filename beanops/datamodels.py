## DATA MODELS ##
from pydantic import BaseModel
from typing import Optional

CHANNEL = "social media group/forum"
POST = "social media post"
COMMENT = "social media comment"
ARTICLE = "news article/blog"

# names of important fields of collections
K_MAPPED_URL = "mapped_url"

class Noise(BaseModel):
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
    comments: Optional[int] = None
    subscribers: Optional[int] = None
    likes: Optional[int] = None
    likes_ratio: Optional[float] = None    
    score: Optional[int] = None
    
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

class Bean(BaseModel):
    url: str
    updated: Optional[int] = None
    source: Optional[str] = None
    title: Optional[str] = None
    kind: Optional[str] = None
    text: Optional[str] = None
    author: Optional[str] = None    
    created: Optional[int] = None    
    tags: Optional[list[str]] = None
    summary: Optional[str] = None
    topic: Optional[str] = None
    embedding: Optional[list[float]] = None
    search_score: Optional[float|int] = None
    trend_score: Optional[int] = None
    noise: Optional[Noise] = None

    def digest(self):
        return f"{self.kind} from {self.source}\nTitle: {self.title}\nBody: {self.text}"

K_KEYPHRASE = "keyphrase"
K_EVENT="event"
K_DESCRIPTION = "description"
K_TRENDSCORE = "trend_score"
K_URLS = "urls"

class Nugget(BaseModel):
    keyphrase: str 
    event: str 
    description: str 
    updated: Optional[int] = None
    embedding: Optional[list[float]] = None
    urls: Optional[list[str]] = None
    trend_score: Optional[int] = None

    def digest(self) -> str:
        return (self.keyphrase or "" )+((": "+self.description) if self.description else "")


    
class Source(BaseModel):
    url: str
    kind: str
    name: str
    cid: Optional[str] = None
    
