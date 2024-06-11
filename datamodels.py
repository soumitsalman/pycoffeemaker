## DATA MODELS ##
from pydantic import BaseModel
from typing import Optional

CHANNEL = "social media group/forum"
POST = "social media post"
COMMENT = "social media comment"
ARTICLE = "news article/blog"

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

    def digest(self):
        return f"{self.kind} from {self.source}\nTitle: {self.title}\nBody: {self.text}"

class Nugget(BaseModel):
    keyphrase: str 
    event: str 
    description: str 
    updated: Optional[int] = None
    embedding: Optional[list[float]] = None
    urls: Optional[list[str]] = None
    trend_score: Optional[int] = None

class Noise(BaseModel):
    mapped_url: Optional[str] = None
    updated: Optional[int] = None
    source: Optional[str] = None
    text: Optional[str] = None
    cid: Optional[str] = None
    channel: Optional[str] = None
    container_url: Optional[str] = None
    comments: Optional[int] = None
    subscribers: Optional[int] = None
    likes: Optional[int] = None
    likes_ratio: Optional[float] = None    
    score: Optional[int] = None
    
    def digest(self):
        return f"Title: {self.title}\nBody: {self.text}"
