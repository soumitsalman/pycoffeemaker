from uuid import UUID, uuid5, NAMESPACE_URL
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

# sip fields
ID = 'id'
CREATED = 'created'
KIND = 'kind'
SOURCE = 'source'
EMBEDDING = 'embedding'
TAGS = 'tags'
DIGEST = 'digest'
URL = 'url'

# source fields
BASE_URL = 'base_url'
DOMAIN_NAME = 'domain_name'
SITE_NAME = 'site_name'
DESCRIPTION = 'description'
FAVICON = 'favicon'
RSS_FEED = 'rss_feed'

generate_id = lambda url: uuid5(NAMESPACE_URL, url)
DEFAULT_SOURCE = generate_id("https://cafecito.tech")

class Sip(BaseModel):
    id: Optional[UUID] = Field(default=None)
    created: Optional[datetime] = Field(default=None)
    kind: Optional[str] = Field(default=None)
    source: Optional[UUID] = Field(default=None)
    embedding: Optional[list[float]] = Field(default=None)
    tags: Optional[list[str]] = Field(default=None)
    digest: Optional[dict] = Field(default=None)
    url: Optional[str] = Field(default=None)
    base_url: Optional[str] = Field(default=None)

    def model_post_init(self, __context):
        if not self.id:
            if self.url: self.id = generate_id(self.url)
            else: raise ValueError("Sip must have a `url` or `id`")
        if not self.source:
            if self.base_url: self.source = generate_id(self.base_url)
            else: self.source = DEFAULT_SOURCE

class Source(BaseModel):
    id: Optional[UUID] = Field(default=None)
    base_url: Optional[str] = Field(default=None)
    domain_name: Optional[str] = Field(default=None)
    site_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    favicon: Optional[str] = Field(default=None)
    rss_feed: Optional[str] = Field(default=None)

    def model_post_init(self, __context):
        if not self.id:
            if self.base_url: self.id = generate_id(self.base_url)
            else: self.id = DEFAULT_SOURCE