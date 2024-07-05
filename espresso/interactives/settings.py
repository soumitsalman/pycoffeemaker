# from enum import Enum
# import json
# from beanops.beansack import get_timevalue, timewindow_filter
# from beanops.datamodels import *
# from langchain.pydantic_v1 import BaseModel, Field
# from typing import Optional
# import copy

# DEFAULT_LIMIT = 10
# DEFAULT_NDAYS = 3

# class ContentType(str, Enum):    
#     POSTS = "posts"
#     COMMENTS = "comments"
#     NEWS = "news"
#     BLOGS = "blogs"
#     HIGHLIGHTS = "highlights"
#     NEWSLETTER = "newsletter"
#     ALL = "all"

# class Settings(BaseModel):
#     topics: Optional[str|list[str]] = Field(description="Users topics, domains, areas of interest/preference.", default = None)
#     content_types: Optional[str|list[ContentType]] = Field(description="The list of content types the user is interested in such as social media posts, news articles, blogs etc.", default = None)
#     last_ndays: Optional[int] = Field(description="The last N number of days of content user is interested in.", default = DEFAULT_NDAYS)
#     limit: Optional[int] = Field(description="The top N items to return in the search result", default=DEFAULT_NDAYS)
#     sources: Optional[list[str]] = Field(description="The list of news or social media sources the user is interested in such as Hackernews, Verge, Reddit.", default = None)

#     def update(self, topics: str|list[str]=None, content_types: ContentType|list[ContentType] = None, last_ndays: int = None, topn: int = None):
#         """Changes/updates application settings settings so that by default all future contents follow the change directive."""
#         if topics:
#             self.topics=topics
#         if content_types:
#             self.content_types=content_types
#         if last_ndays:
#             self.last_ndays=last_ndays    
#         if topn:
#             self.limit=topn        
#         return self.json(indent=2)
    
#     def overrides(self, topics: None, content_type: ContentType = None, last_ndays: int = None, topn: int = None):       
#         query = topics or self.topics
#         is_highlight = lambda content_type: (not content_type) or (content_type == ContentType.HIGHLIGHTS)
#         filter = self._create_time_filter(last_ndays) if is_highlight(content_type) else self._create_filters(content_type, last_ndays)
#         topn = topn or self.limit
#         return query, filter, topn
    
#     def _create_filters(self, content_types: ContentType|list[ContentType], last_n_days: int):
#         filter = {}
#         filter.update(self._create_ctype_filter(content_types) or {})        
#         filter.update(self._create_time_filter(last_n_days) or {})
#         return filter

#     def _create_ctype_filter(self, content_types):
#         content_types = content_types or self.settings.content_types
#         if content_types and content_types != ContentType.ALL:
#            return {K_KIND: { "$in": [_translate_ctype(item) for item in ([content_types] if isinstance(content_types, ContentType) else content_types)] } }

#     def _create_time_filter(self, last_ndays):
#         last_ndays = last_ndays or self.last_ndays
#         if last_ndays:
#             return { K_UPDATED: { "$gte": get_timevalue(last_ndays) } }
        
#     def __deepcopy__(self, memo):
#         return Settings(
#             topics=copy.deepcopy(self.topics, memo),
#             content_types=copy.deepcopy(self.content_types, memo),
#             last_ndays=self.last_ndays,
#             limit=self.limit, 
#             sources=copy.deepcopy(self.sources, memo))


# def update(settings: dict, topics: str|list[str]=None, content_types: ContentType|list[ContentType] = None, last_ndays: int = None, topn: int = None):
#     """Changes/updates application settings settings so that by default all future contents follow the change directive."""
#     if topics:
#         settings['topics']=topics
#     if content_types:
#         settings['content_types']=content_types
#     if last_ndays:
#         settings['last_ndays']=last_ndays    
#     if topn:
#         settings['limit']=topn        
#     return json.dumps(settings, indent=2)

# def overrides(settings: dict, topics: None, content_type: ContentType = None, last_ndays: int = None, topn: int = None):       
#     query = topics or settings.get("topics")
#     is_highlight = lambda content_type: (not content_type) or (content_type == ContentType.HIGHLIGHTS)
#     filter = _create_time_filter(settings, last_ndays) if is_highlight(content_type) else create_filter(settings, content_type, last_ndays)
#     topn = topn or settings.get("limit")
#     return query, filter, topn

# def create_filter(settings: dict, content_types: ContentType|list[ContentType], last_n_days: int):
#     filter = {}
#     filter.update(_create_ctype_filter(settings, content_types) or {})        
#     filter.update(_create_time_filter(settings, last_n_days) or {})
#     return filter

# def _create_ctype_filter(settings: dict, content_types):
#     content_types = content_types or settings.get("content_types")
#     if content_types and content_types != ContentType.ALL:
#         return {K_KIND: { "$in": [_translate_ctype(item) for item in ([content_types] if isinstance(content_types, ContentType) else content_types)] } }

# def _create_time_filter(settings: dict, last_ndays):
#     last_ndays = last_ndays or settings.get('last_ndays')
#     if last_ndays:
#         return { K_UPDATED: { "$gte": get_timevalue(last_ndays) } }
    
# def _translate_ctype(ctype: ContentType):
#     if ctype == ContentType.POSTS:
#         return POST
#     elif ctype in [ContentType.NEWS, ContentType.BLOGS, ContentType.NEWSLETTER]:
#         return ARTICLE
#     elif ctype == ContentType.COMMENTS:
#         return COMMENT  