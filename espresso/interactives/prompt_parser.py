############################################
## USER INPUT PARSER FOR STRUCTURED INPUT ##
############################################

from enum import Enum
import argparse
import shlex
from beanops.datamodels import *

class ContentType(str, Enum):    
    POSTS = "posts"
    COMMENTS = "comments"
    NEWS = "news"
    BLOGS = "blogs"
    HIGHLIGHTS = "highlights"
    NEWSLETTER = "newsletter"

class InteractiveInputParser:
    def __init__(self, settings: dict = None):
        self.defaults = settings

        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('task', help='The main task')
        self.parser.add_argument('-q', '--query', help='The search query')
        self.parser.add_argument('-t', '--type', help='The type of content to search or to create.')    
        self.parser.add_argument('-d', '--ndays', help='The last N days of data to retrieve. N should be between 1 - 30')
        self.parser.add_argument('-n', '--topn', help='The top N items to retrieve. Must be a positive int')
        self.parser.add_argument('-s', '--source', help='Data source to pull from')
        self.parser.format_help()
        
    def parse(self, prompt: str):
        def one_or_list(items):
            if isinstance(items, list):
                return items if len(items) > 1 else items[0]
            else:
                return items            
            
        try:
            args = self.parser.parse_args(shlex.split(prompt.lower()))      
            # parse query/topics            
            query = [item.strip() for item in args.query.split(",")] if args.query else self.defaults.get('topics', [])
            # parser content_types/kind
            ctypes = [_translate_ctype(getattr(ContentType, item.strip().upper(), None)) for item in args.type.split(",")] if args.type else self.defaults.get('content_types', [])
            ndays = int(args.ndays) if args.ndays else self.defaults.get('last_ndays')
            topn = int(args.topn) if args.topn else self.defaults.get('topn')
            return (args.task, one_or_list(query), one_or_list(ctypes) , ndays, topn)
        except:
            return (None, self.defaults.get('topics', []), self.defaults.get('content_types', []), self.defaults.get('last_ndays'), self.defaults.get('topn'))
        
    def update_defaults(self, topics: str|list[str], content_types, last_ndays: int, topn: int):
        """Changes/updates application settings settings so that by default all future contents follow the change directive."""
        if topics:
            self.defaults['topics']=topics
        if content_types:
            self.defaults['content_types']=content_types
        if last_ndays:
            self.defaults['last_ndays']=last_ndays    
        if topn:
            self.defaults['topn']=topn        
        return self.defaults
        
def _translate_ctype(ctype: ContentType):
    if ctype == ContentType.POSTS:
        return POST
    elif ctype in [ContentType.NEWS, ContentType.BLOGS, ContentType.NEWSLETTER]:
        return ARTICLE
    elif ctype == ContentType.COMMENTS:
        return COMMENT  
    else:
        return ctype.value