from itertools import chain
from icecream import ic
from collectors.utils import create_logger
from beanops.beansack import *
from beanops.datamodels import *
from .chains import *

DEFAULT_CTYPE_TO_WRITE="newsletter"

beansack: Beansack = None
article_writer = None
prompt_parser = None

def initiatize_tools(db_conn, embedder, llm):
    global beansack, article_writer, prompt_parser
    # this is content retrieval (not processing). This does not need an llm in the beansack
    beansack=Beansack(db_conn, embedder, None)
    if llm:
        article_writer = ArticleWriter(llm)

def get_sources():
    return beansack.get_sources()

def get_beans_for_nugget(nugget_id: str, content_types: str|list[str], last_ndays: int, topn: int):
    nuggets_and_beans = beansack.get_beans_by_nuggets(nugget_ids=nugget_id, filter=_create_filter(content_types, last_ndays), limit=topn)
    return (nuggets_and_beans[0][1] if nuggets_and_beans else None) or []

def highlights(query, last_ndays: int, topn: int):
    """Retrieves the trending news highlights that match user interest, topic or query."""
    return _retrieve_queries(query, lambda q: beansack.trending_nuggets(query=q, filter=timewindow_filter(last_ndays), limit=topn, projection={K_EMBEDDING: 0, K_TEXT:0}))
    
def trending(query, content_types: str|list[str], last_ndays: int, topn: int):
    """Retrieves the trending news articles, social media posts, blog articles that match user interest, topic or query."""
    return _retrieve_queries(query, lambda q: beansack.trending_beans(query=q, filter=_create_filter(content_types, last_ndays), limit=topn, projection={K_EMBEDDING: 0, K_TEXT:0}))
    
def search(query: str, content_types: str|list[str], last_ndays: int, topn: int):
    """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
    return _retrieve_queries(query, lambda q: beansack.search_beans(query=q, filter=_create_filter(content_types, last_ndays), limit=topn, sort_by=LATEST, projection={K_EMBEDDING: 0, K_TEXT:0}))

def _retrieve_queries(query_items, retrieval_func):
    query_items = query_items if isinstance(query_items, list) else [query_items]
    results = []
    for q in query_items:
        nugs = retrieval_func(q)
        if nugs:
            results.extend(nugs)
    return results

def search_all(query: str, last_ndays: int, topn: int):
    """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
    filter = timewindow_filter(last_ndays)
    return ((beansack.search_beans(query=query, filter=filter, limit=topn, sort_by=LATEST, projection={K_EMBEDDING: 0, K_TEXT:0}) or []), \
        (beansack.search_nuggets(query=query, filter=filter, limit=topn, sort_by=LATEST, projection={K_EMBEDDING: 0, K_TEXT:0}) or []))

def write(topic: str, content_type: str, last_ndays: int, stream: bool = False):
        """Writes a newsletter, blogs, social media posts from trending news articles, social media posts blog articles or news highlights on user interest/topic"""
        nuggets_and_beans = beansack.search_nuggets_with_beans(query=topic, filter=timewindow_filter(last_ndays), limit=5)    
        if nuggets_and_beans:    
            highlights = [item[0].digest() for item in nuggets_and_beans]
            makecontents = lambda beans: [f"## {bean.title}\n{bean.text}" for bean in beans]
            initial_content = [makecontents(item[1]) for item in nuggets_and_beans]
            makesources = lambda beans: [(bean.source, bean.url) for bean in beans]
            sources = [makesources(item[1]) for item in nuggets_and_beans]
                 
            return article_writer.write_article(highlights, initial_content, sources, content_type)  
        
def _create_filter(content_types: str|list[str], last_ndays: int):
    filter = timewindow_filter(last_ndays)
    if isinstance(content_types, str):
        filter.update({K_KIND: content_types})
    elif isinstance(content_types, list):
        filter.update({K_KIND: { "$in": content_types } })
    return filter

