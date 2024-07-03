from itertools import chain
from icecream import ic
from shared.utils import create_logger
from beanops.beansack import *
from beanops.datamodels import *
from .settings import *
from .chains import *

DEFAULT_CTYPE_TO_WRITE="newsletter"

beansack: Beansack = None
article_writer = None
prompt_parser = None

def initiatize_tools(db_conn, llm_api_key, embedder_path):
    global beansack, article_writer, prompt_parser
    beansack=Beansack(db_conn, llm_api_key, embedder_model_path=embedder_path)
    article_writer = ArticleWriter(llm_api_key)
    prompt_parser = InteractiveInputParser()

def get_beans_for_nugget(session_settings: Settings, nugget_id: str, content_type: ContentType):
    _, filter, topn = session_settings.overrides(None, content_type, None, None)
    nuggets_and_beans = beansack.get_beans_by_nuggets(nugget_ids=nugget_id, filter=filter, limit=topn)
    return (nuggets_and_beans[0][1] if nuggets_and_beans else None) or []

def trending(session_settings: Settings, topics: None, content_type: ContentType = None, last_ndays: int = None, topn: int = None):
    """Retrieves the trending news articles, social media posts, blog articles or news highlights that match user interest, topic or query."""
    query, filter, topn = session_settings.overrides(topics, content_type, last_ndays, topn)
    query = query if isinstance(query, list) else [query]
    is_highlight = lambda content_type: (not content_type) or (content_type == ContentType.HIGHLIGHTS)
    trend_func = beansack.trending_nuggets if is_highlight(content_type) else beansack.trending_beans
    return list(chain(*((trend_func(query=item, filter=filter, limit=topn, projection={K_EMBEDDING: 0, K_TEXT:0}) or []) for item in query)))
    
def search(session_settings: Settings, query: str, content_type: ContentType = None, last_ndays: int = None, topn: int = None):
    """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
    query, filter, topn = session_settings.overrides(query, content_type, last_ndays, topn)
    return beansack.search_beans(query=query, filter=filter, limit=topn, sort_by=LATEST, projection={K_EMBEDDING: 0, K_TEXT:0}) or []

def search_all(session_settings: Settings, query: str):
    """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
    query, filter, topn = session_settings.overrides(query, None, None, None)
    result = []
    result += (beansack.search_beans(query=query, filter=filter, limit=topn, sort_by=LATEST, projection={K_EMBEDDING: 0, K_TEXT:0}) or [])
    result += (beansack.search_nuggets(query=query, filter=filter, limit=topn, sort_by=LATEST, projection={K_EMBEDDING: 0, K_TEXT:0}) or [])
    return result

def write(session_settings: Settings, topic: str, content_type: str = DEFAULT_CTYPE_TO_WRITE, last_ndays: int = None, stream: bool = False):
        """Writes a newsletter, blogs, social media posts from trending news articles, social media posts blog articles or news highlights on user interest/topic"""
        query, filter, _ = session_settings.overrides(topic, content_type, last_ndays, None)
        nuggets_and_beans = beansack.search_nuggets_with_beans(query=query, filter=filter, limit=5)    
        if nuggets_and_beans:    
            highlights = [item[0].digest() for item in nuggets_and_beans]
            makecontents = lambda beans: [f"## {bean.title}\n{bean.text}" for bean in beans]
            initial_content = [makecontents(item[1]) for item in nuggets_and_beans]
            makesources = lambda beans: [(bean.source, bean.url) for bean in beans]
            sources = [makesources(item[1]) for item in nuggets_and_beans]
                 
            return article_writer.write_article(highlights, initial_content, sources, content_type)  
        
def parse_prompt(prompt: str):
    return prompt_parser.parse(prompt)

############################################
## USER INPUT PARSER FOR STRUCTURED INPUT ##
############################################

# this is a test application.
import argparse
import shlex
from .settings import ContentType

class InteractiveInputParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('task', help='The main task')
        self.parser.add_argument('-q', '--query', help='The search query')
        self.parser.add_argument('-t', '--type', help='The type of content to search or to create.')    
        self.parser.add_argument('-d', '--ndays', help='The last N days of data to retrieve. N should be between 1 - 30')
        self.parser.add_argument('-n', '--topn', help='The top N items to retrieve. Must be a positive int')
        self.parser.add_argument('-s', '--source', help='Data source to pull from')

        self.parser.format_help()
        
    def parse(self, prompt: str):
        try:
            args = self.parser.parse_args(shlex.split(prompt.lower()))            
            ndays = int(args.ndays) if args.ndays else None
            topn = int(args.topn) if args.topn else None
            # parser content_types/kind
            ctype = args.type
            if ctype:
                ctype = [getattr(ContentType, item.strip().upper(), None) for item in ctype.split(",")]
                ctype = ctype[0] if len(ctype) == 1 else ctype
            # parse query/topics
            query = args.query
            if query:
                query = [item.strip() for item in args.query.split(",")] if args.query else None
                query = query[0] if len(query) == 1 else query
            return (args.task, query, ctype, ndays, topn)
        except:
            return (None, None, None, None, None)