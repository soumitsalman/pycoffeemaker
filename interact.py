import json
from icecream import ic
from enum import Enum
from typing import TypedDict
from nlp.chains import ArticleWriter
from beanops.beansack import Beansack, get_timevalue
from beanops.datamodels import *

DEFAULT_CTYPE_TO_WRITE="newsletter"
DEFAULT_LIMIT=10
DEFAULT_LAST_N_DAYS=15

class ContentType(str, Enum):    
    POSTS = "posts"
    COMMENTS = "comments"
    NEWS = "news"
    BLOGS = "blogs"
    HIGHLIGHTS = "highlights"
    NEWSLETTER = "newsletter"

def _translate_ctype(ctype: ContentType):
    if ctype == ContentType.POSTS:
        return POST
    elif ctype in [ContentType.NEWS, ContentType.BLOGS, ContentType.NEWSLETTER]:
        return ARTICLE
    elif ctype == ContentType.COMMENTS:
        return COMMENT    

class InteractSession:
    def __init__(self, bsack: Beansack, llm_api_key: str):
        self.beansack = bsack
        self.article_writer = ArticleWriter(llm_api_key)
        self.limit = DEFAULT_LIMIT
        self.last_n_days = DEFAULT_LAST_N_DAYS
        self.content_types = None
        # TODO: process the topics in the trending
        self.topics = None
        self.topic_embeddings = None
        
    # vector search beans
    # get nuggets that map to the urls
    # use the description of the nugget as a topic
    # use the body of the beans as contents
    def write(self, query: str, content_type: str = DEFAULT_CTYPE_TO_WRITE, last_n_days: int = None):
        nuggets_and_beans = self.beansack.search_nuggets_with_beans(query=query, filter=self._create_time_filter(last_n_days), limit=DEFAULT_LIMIT)    

        makecontents = lambda beans: [f"## {item.title}\n{item.text}" for item in beans]
        makesources = lambda beans: [(item.source, item.url) for item in beans]
        return self.article_writer.write_article(
            highlights = [item[0].digest() for item in nuggets_and_beans], 
            contents = [makecontents(item[1]) for item in nuggets_and_beans],
            sources = [makesources(item[1]) for item in nuggets_and_beans],
            content_type=content_type)

    def trending(self, query: str=None, content_type: ContentType = None, last_n_days: int = None):
        if (not content_type) or content_type == ContentType.HIGHLIGHTS:
            # then query from nuggets
            return self.beansack.trending_nuggets(query=query, filter=self._create_time_filter(last_n_days), limit=self.limit)               
        else:
            #else query from beans
            return self.beansack.trending_beans(query=query, filter=self._create_filters(content_type, last_n_days), limit=self.limit) 
            
    def search(self, query: str, content_type: ContentType = None, last_n_days: int = None):
        # TODO: create content type filter
        return self.beansack.search_beans(query=query, filter=self._create_filters(content_type, last_n_days), limit=self.limit)
    
    def configure(self, topics: list[str]=None, content_types: list[ContentType] = None, last_n_days: int = None, top_n: int = None) -> dict:
        resp = {}
        if topics:
            resp["topics"]=self.topics=topics
        if content_types:
            resp["content_types"]=self.content_types=content_types
        if last_n_days:
            resp["last_n_days"]=self.last_n_days=last_n_days    
        if top_n:
            resp["top_n"]=self.limit=top_n
        return {"topics": self.topics, "content_types": self.content_types, "last_n_days": self.last_n_days, "top_n": self.limit }
    
    def _create_filters(self, content_types: ContentType|list[ContentType], last_n_days: int):
        filter = {}
        filter.update(self._create_ctype_filter(content_types) or {})        
        filter.update(self._create_time_filter(last_n_days) or {})
        return filter

    def _create_ctype_filter(self, content_types):
        content_types = content_types or self.content_types
        if content_types:
           return {K_KIND: { "$in": [_translate_ctype(item) for item in ([content_types] if isinstance(content_types, ContentType) else content_types)] } }

    def _create_time_filter(self, last_n_days):
        last_n_days = last_n_days or self.last_n_days
        if last_n_days:
            return { K_UPDATED: { "$gte": get_timevalue(last_n_days) } }


# this is a test application.
import argparse
def _create_argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('task', help='The main task')
    parser.add_argument('-q', '--query', help='The search query')
    parser.add_argument('-t', '--type', help='The type of content to search or to create.')    
    parser.add_argument('-d', '--ndays', help='The last N days of data to retrieve. N should be between 1 - 30')
    parser.add_argument('-n', '--topn', help='The top N items to retrieve. Must be a positive int')
    parser.add_argument('-s', '--source', help='Data source to pull from')
    return parser

import shlex
def _parse_args(parser: argparse.ArgumentParser, cmd: str):
    try:
        args = parser.parse_args(shlex.split(cmd.lower()))
        ctype = getattr(ContentType, args.type.upper(), None) if args.type else None
        ndays = int(args.ndays) if args.ndays else None
        topn = int(args.topn) if args.topn else None
        return (args.task, args.query, ctype, ndays, topn)
    except:
        return (None, None, None, None, None)

espresso = "Espresso:"
from datetime import datetime as dt
def _render_to_console(items: list[Nugget|Bean]):
    if not items:
        print(espresso, "Nothing found")
    elif isinstance(items[0], Nugget):
        print(espresso, len(items), "highlights trending")
        for nug in items:
            print("[", dt.fromtimestamp(nug.updated).strftime('%Y-%m-%d'), "] ", nug.trend_score, " | ", nug.digest())
    elif isinstance(items[0], Bean):
        print(espresso, len(items), "items found")
        for bean in items:
            print("[", dt.fromtimestamp(bean.updated).strftime('%Y-%m-%d'), "] ", bean.source, ":", bean.title)
            print(bean.summary, "\n")

def _run_console(session: InteractSession):    
    input_parser = _create_argparser()
    try:
        # for user_input in ["generative ai", "Donald Trump"]:
        while True:
            args = _parse_args(input_parser, input("You: "))
            if args[0] == "exit":
                print("Exiting...")
                break
            elif args[0] == "/trending":
                resp = session.trending(args[1], args[2], args[3])
                _render_to_console(resp)
            elif args[0] == "/lookfor":
                resp = session.search(args[1], args[2], args[3])
                _render_to_console(resp)
            elif args[0] == "/write":
                resp = session.write(args[1], args[2], args[3])
                print(espresso, resp)
            elif args[0] == "/config":
                resp = session.configure(args[1], args[2], args[3])
                print(espresso, "current settings", json.dumps(resp, indent=2))
            else:
                print(espresso, "WTF is this?")
                
    except KeyboardInterrupt:
        print("\nExiting...")
