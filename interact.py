from itertools import chain
from icecream import ic


from nlp.chains import ArticleWriter
from beanops.beansack import Beansack
from beanops.datamodels import *

class InteractSession:
    def __init__(self, bsack: Beansack, llm_api_key: str):
        self.sack = bsack
        self.article_writer = ArticleWriter(llm_api_key)

    # vector search beans
    # get nuggets that map to the urls
    # use the description of the nugget as a topic
    # use the body of the beans as contents
    def write(self, query: str, content_type="newsletter"):
        nuggets_and_beans = self.sack.search_nuggets_with_beans(query = query, limit = 5)    
        ic(len(nuggets_and_beans))

        makecontents = lambda beans: [f"## {item.title}\n{item.text}" for item in beans]
        makesources = lambda beans: [(item.source, item.url) for item in beans]

        return self.article_writer.writer_article(
            highlights = [item[0].digest() for item in nuggets_and_beans], 
            contents = [makecontents(item[1]) for item in nuggets_and_beans],
            sources = [makesources(item[1]) for item in nuggets_and_beans],
            content_type=content_type)

    def trending(self, query: str):
        print("Not implemented")
