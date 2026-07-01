from uuid import UUID

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

from utils.fields import (
    BASE_URL,
    CREATED,
    DESCRIPTION,
    DIGEST,
    DOMAIN_NAME,
    EMBEDDING,
    FAVICON,
    ID,
    KIND,
    RSS_FEED,
    SITE_NAME,
    SOURCE,
    TAGS,
    URL,
)
from utils.ids import generate_uuid

generate_id = generate_uuid
DEFAULT_SOURCE = generate_uuid("https://cafecito.tech")


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
            if self.url:
                self.id = generate_id(self.url)
            else:
                raise ValueError("Sip must have a `url` or `id`")
        if not self.source:
            if self.base_url:
                self.source = generate_id(self.base_url)
            else:
                self.source = DEFAULT_SOURCE


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
            if self.base_url:
                self.id = generate_id(self.base_url)
            else:
                self.id = DEFAULT_SOURCE
