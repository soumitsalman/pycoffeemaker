from itertools import chain
from icecream import ic


from nlp.chains import ArticleSectionGenerator
from beanops.beansack import Beansack
from beanops.datamodels import *

sack: Beansack = None
sec_gen: ArticleSectionGenerator = None

def initialize(bsack: Beansack, llm_api_key: str):
    global sack
    sack = bsack
    global sec_gen
    sec_gen = ArticleSectionGenerator(llm_api_key)
    

class Article:
    def __init__(self):
        self.highlights = []
        self.sections = []

    def markdown(self) -> str:
        content = "## Trending Highlights\n"
        content += "".join([f"- {item}\n" for item in self.highlights])+"\n"
        make_src = lambda sources: ", ".join({src[0]:f"[{src[0]}]({src[1]})" for src in sources}.values())
        make_section = lambda sec: f"## {sec.title}\n{sec.body}\n\n**Sources:** {make_src(sec.sources)}\n"
        return content+"\n".join([make_section(sec) for sec in self.sections])


# vector search beans
# get nuggets that map to the urls
# use the description of the nugget as a topic
# use the body of the beans as contents
def generate(query: str):
    nuggets_and_beans = sack.search_nuggets_with_beans(query = query, limit = 3, bean_projection={K_URL: 1, K_SOURCE: 1, K_TEXT: 1})    

    article = Article()    
    article.highlights = [f"{nwb['nugget'].keyphrase}: {nwb['nugget'].event}" for nwb in nuggets_and_beans]
    for nwb in nuggets_and_beans:    
        topic = nwb['nugget'].description
        contents = [bean.text for bean in nwb['beans']]
        section = sec_gen(topic=topic, contents=contents, content_type="blog", tone="heavily sarcastic")
        section.sources = [(bean.source, bean.url) for bean in nwb['beans']]
        article.sections.append(section)
    return nuggets_and_beans, article

