from icecream import ic
from retry import retry
from nlp.chains import combine_texts
from shared.utils import create_logger
from enum import Enum
from beanops.beansack import Beansack, get_timevalue
from beanops.datamodels import *
from langchain_core.tools import tool
from langchain.pydantic_v1 import BaseModel, Field
from typing import Optional

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
    ALL = "all"

class TrendingInputs(BaseModel):
    topic: Optional[str] = Field(description="The topic, domain, query or content to search for in trending news/posts/blogs/highlights.")
    content_type: Optional[ContentType] = Field(description="[Optional] type of content such as news articles, social media posts, blogs, highlights, highlights etc.", default=None)
    last_ndays: Optional[int] = Field(description = "[Optional] The time window of the published items. Represented in the last N days from today's date.", default=None)
    source: Optional[str] = Field(description="[Optional] The source of the content such as Reddit, Verge, Hacker news etc.")

class WriteInput(BaseModel):
    topic: Optional[str] = Field(description="The topic, domain, query or content to search for in trending items.")
    content_type: Optional[ContentType] = Field(description="[Optional] type of content such as news articles, social media posts, blogs, highlights, nuggets etc.", default=None)
    last_ndays: Optional[int] = Field(description = "[Optional] The time window of the published items. Represented in the last N days from today's date.", default=None)
    source: Optional[str] = Field(description="[Optional] The source of the content such as Reddit, Verge, Hacker news etc.")

class Settings(BaseModel):
    topics: Optional[str|list[str]] = Field(description="Users topics, domains, areas of interest/preference.", default = None)
    content_types: Optional[str|list[ContentType]] = Field(description="The list of content types the user is interested in such as social media posts, news articles, blogs etc.", default = None)
    last_n_days: Optional[int] = Field(description="The last N number of days of content user is interested in.", default = None)
    # sources: Optional[list[str]] = Field(description="The list of news or social media sources the user is interested in such as Hackernews, Verge, Reddit.", default = None)

def _translate_ctype(ctype: ContentType):
    if ctype == ContentType.POSTS:
        return POST
    elif ctype in [ContentType.NEWS, ContentType.BLOGS, ContentType.NEWSLETTER]:
        return ARTICLE
    elif ctype == ContentType.COMMENTS:
        return COMMENT    
    
#############
## Session ##
#############

class InteractSession:
    def __init__(self, bsack: Beansack, llm_api_key: str, default_limit = DEFAULT_LIMIT, default_ndays = DEFAULT_LAST_N_DAYS, default_topics = None, default_content_types = None):
        self.beansack = bsack
        self.article_writer = ArticleWriter(llm_api_key)
        self.limit = default_limit
        self.settings = Settings(last_n_days = default_ndays, content_types = default_content_types, topics = default_topics)

    # @tool("trending", args_schema=TrendingInputs)
    def trending(self, topics: None, content_type: ContentType = None, last_n_days: int = None):
        """Retrieves the trending news articles, social media posts, blog articles or news highlights that match user interest, topic or query."""
        query = topics or self.settings.topics
        is_highlight = lambda content_type: (not content_type) or (content_type == ContentType.HIGHLIGHTS)
        trend_func = self.beansack.trending_nuggets if is_highlight(content_type) else self.beansack.trending_beans
        filter = self._create_time_filter(last_n_days) if is_highlight(content_type) else self._create_filters(content_type, last_n_days)
        if (not query) or isinstance(query, str):
            return trend_func(query=query, filter=ic(filter), limit=self.limit)               
        else:
            # run multiple queries
            return {item: trend_func(query=item, filter=filter, limit=self.limit) for item in query}

    # @tool("search", args_schema=TrendingInputs)     
    def search(self, topic: str, content_type: ContentType = None, last_n_days: int = None):
        """Searches and looks for news articles, social media posts, blog articles that match user interest, topic or query represented by `topic`."""
        return self.beansack.search_beans(query=topic, filter=self._create_filters(content_type, last_n_days), limit=self.limit)
    
    # vector search beans
    # get nuggets that map to the urls
    # use the description of the nugget as a topic
    # use the body of the beans as draft materials
    # @tool("write", args_schema=WriteInput)    
    def write(self, topic: str, content_type: str = DEFAULT_CTYPE_TO_WRITE, last_n_days: int = None, stream: bool = False):
        """Writes a newsletter, blogs, social media posts from trending news articles, social media posts blog articles or news highlights on user interest/topic"""
        nuggets_and_beans = self.beansack.search_nuggets_with_beans(query=topic, filter=self._create_time_filter(last_n_days), limit=3)    
        if nuggets_and_beans:    
            highlights = [item[0].digest() for item in nuggets_and_beans]
            makecontents = lambda beans: [f"## {bean.title}\n{bean.text}" for bean in beans]
            initial_content = [makecontents(item[1]) for item in nuggets_and_beans]
            makesources = lambda beans: [(bean.source, bean.url) for bean in beans]
            sources = [makesources(item[1]) for item in nuggets_and_beans]

            if stream:
                for segment in self.article_writer.stream_article(highlights, initial_content, sources, content_type):
                    yield segment
            else:
                return self.article_writer.write_article(highlights, initial_content, sources, content_type)        
    
    # @tool("configure_settings", args_schema=Settings)
    def configure(self, topics: str|list[str]=None, content_types: ContentType|list[ContentType] = None, last_n_days: int = None, top_n: int = None):
        """Changes/updates application settings settings so that by default all future contents follow the change directive."""
        if topics:
            self.settings.topics=topics
        if content_types:
            self.settings.content_types=content_types
        if last_n_days:
            self.settings.last_n_days=last_n_days    
        if top_n:
            self.limit=top_n        
        return self.settings.json(indent=2)
    
    def _create_filters(self, content_types: ContentType|list[ContentType], last_n_days: int):
        filter = {}
        filter.update(self._create_ctype_filter(content_types) or {})        
        filter.update(self._create_time_filter(last_n_days) or {})
        return filter

    def _create_ctype_filter(self, content_types):
        content_types = content_types or self.settings.content_types
        if content_types and content_types != ContentType.ALL:
           return {K_KIND: { "$in": [_translate_ctype(item) for item in ([content_types] if isinstance(content_types, ContentType) else content_types)] } }

    def _create_time_filter(self, last_n_days):
        last_n_days = last_n_days or self.settings.last_n_days
        if last_n_days:
            return { K_UPDATED: { "$gte": get_timevalue(last_n_days) } }

############################################
## USER INPUT PARSER FOR STRUCTURED INPUT ##
############################################

# this is a test application.
import argparse
import shlex
class InteractiveInputParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('task', help='The main task')
        self.parser.add_argument('-q', '--query', help='The search query')
        self.parser.add_argument('-t', '--type', help='The type of content to search or to create.')    
        self.parser.add_argument('-d', '--ndays', help='The last N days of data to retrieve. N should be between 1 - 30')
        self.parser.add_argument('-n', '--topn', help='The top N items to retrieve. Must be a positive int')
        self.parser.add_argument('-s', '--source', help='Data source to pull from')
        
    def parse(self, params: str, known_task = None):
        try:
            params = params if not known_task else f"{known_task} {params}"
            args = self.parser.parse_args(shlex.split(params.lower()))            
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


####################
## FUNCTION CALLS ##
####################

# @tool("trending", args_schema=TrendingInputs)
# def trending(topic: str=None, content_type: ContentType = None, last_ndays: int = None, source: str = None) -> str:
#     """Retrieves the trending news articles, social media posts, blog articles or highlights represented by `content_type`
#     on specified user interest, topic or query represented by `topic`
#     that were published in the last N number of days represented by `last_ndays`
#     from publishers such as Reddit, Verge, Hacker news represented by `source`"""
#     return "Nothing trending"

# @tool("search", args_schema=TrendingInputs)
# def search(topic: str, content_type: ContentType = None, last_ndays: int = None, source: str = None) -> str:
#     """Retrieves the trending news articles, social media posts, blog articles or highlights represented by `content_type`
#     on specified user interest, topic or query represented by `topic`
#     that were published in the last N number of days represented by `last_ndays`
#     from publishers such as Reddit, Verge, Hacker news represented by `source`"""
#     return "Nothing trending"


        
# @tool("write", args_schema=WriteInput)
# def write(topic: str=None, content_type: ContentType = None, last_ndays: int = None, source: str = None, tone: str = None) -> str:
#     """Writes a summary, newsletter, blogs, social media posts from trending news articles, social media posts, blog articles or highlights
#     on specified user interest, topic or query represented by `topic`
#     that were published in the last N number of days represented by `last_ndays`
#     from publishers such as Reddit, Verge, Hacker news represented by `source`"""
#     return "Cant generate"
        
# settings = Settings(
#     topics = ["Generative AI", "Start-ups", "Cyber security"],
#     last_n_days = 7,
#     content_types = [ContentType.nuggets, ContentType.blogs, ContentType.posts]
# )

# @tool("configure_settings", args_schema=Settings)
# def configure(topics: list[str]=None, content_types: list[ContentType] = None, last_ndays: int = None, source: list[str] = None) -> str:
#     """Changes the system configuration and settings so that by default all future contents follow the change directive."""
    
#     if topics:
#         settings.topics = topics
#     if content_types:
#         settings.content_types=content_types
#     if last_ndays:
#         settings.last_n_days=last_ndays
    
#     return f"Settings updated:\n{settings.json(exclude_none=True)}"


# tools = [trending, search, write, configure]


####################
## ARTICLE WRITER ##
####################

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_community.llms.deepinfra import DeepInfra

WRITER_TEMPLATE = """<|start_header_id|>system<|end_header_id|>
You are a {content_type} writer. Your task is to rewrite one section of a {content_type} on a given topic from the drafts provided by the user. 
From the drafts extract ONLY the contents that are strictly relevant to the topic and write the section based on ONLY that. You MUST NOT use your own knowledge for this. 
The section should have a title and body. The section should be less that 400 words. Output MUST be in markdown format.
<|eot_id|><|start_header_id|>user<|end_header_id|>
Rewrite a section on topic '{topic}' ONLY based on the following drafts:\n{drafts}
<|eot_id|><|start_header_id|>assistant<|end_header_id|>"""
WRITER_MODEL = "meta-llama/Meta-Llama-3-8B-Instruct"
WRITER_BATCH_SIZE = 6144
DEFAULT_CONTENT_TYPE = "blog"

class ArticleWriter:
    def __init__(self, api_key: str):
        prompt = PromptTemplate.from_template(template=WRITER_TEMPLATE)
        llm = DeepInfra(deepinfra_api_token=api_key, model_id=WRITER_MODEL, verbose=False)
        self.chain = prompt | llm | StrOutputParser()

    # highlights, coontents and sources should havethe same number of items
    def write_article(self, highlights: list, drafts:list, sources: list, content_type: str = DEFAULT_CONTENT_TYPE):                  
        article = "## Trending Highlights\n"+"\n".join(['- '+item for item in highlights])+"\n\n"
        for i in range(len(drafts)):                                         
            article += (
                self.write_section(highlights[i], drafts[i], content_type) +
                "\n**Sources:** "+ 
                ", ".join({src[0]:f"[{src[0]}]({src[1]})" for src in sources[i]}.values()) + 
                "\n\n")   
        return article

    # highlights, coontents and sources should havethe same number of items
    def stream_article(self, highlights: list, drafts:list, sources: list, content_type: str = DEFAULT_CONTENT_TYPE):                  
        yield "## Trending Highlights\n"+"\n".join(['- '+item for item in highlights])
        for i in range(len(drafts)):                                   
           yield self.write_section(highlights[i], drafts[i], content_type)
           yield "**Sources:** "+ ", ".join({src[0]:f"[{src[0]}]({src[1]})" for src in sources[i]}.values())

    @retry(tries=3, jitter=10, delay=10, logger=create_logger("article writer"))
    def write_section(self, topic: str, drafts: list[str], content_type: str = DEFAULT_CONTENT_TYPE) -> str:        
        while True:         
            # run it once at least   
            texts = combine_texts(drafts, WRITER_BATCH_SIZE, "\n\n\n")
            # these are the new drafts
            drafts = [self.chain.invoke({"content_type": content_type, "topic": topic, "drafts": text}) for text in texts]                           
            if len(drafts) <= 1:
                return drafts[0]
            
