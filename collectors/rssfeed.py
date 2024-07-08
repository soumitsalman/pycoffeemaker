################
## RSS Reader ##
################

import feedparser
from beanops.datamodels import Bean, ARTICLE
from datetime import datetime
from bs4 import BeautifulSoup
from shared.utils import create_logger, load_text_from_url
import time
import tldextract
from icecream import ic

DEFAULT_FEED_SOURCES = "newscollector/feedsources.txt"
T_LINK = "link"
T_TITLE = "title"
T_TAGS = "tags"
T_SUMMARY = "summary"
T_CONTENT = "content"
T_PUBLISHED = 'published_parsed'
T_AUTHOR = 'author'

logger = create_logger("rssfeed collector")

# reads the list of feeds from a file path and collects
# if sources is a string then it will be treated as a file path or else it will be a an array
def collect(sources: str|list[str] = DEFAULT_FEED_SOURCES, store_func = None):
    if isinstance(sources, str):
        # if sources are not provided, assume that there is sources_file provided
        with open(sources, 'r') as file:
            sources = file.readlines()
    # santize the urls and start collecting
    sources = [url.strip() for url in sources if url.strip()]
    for url in sources:
        try:
            beans = collect_from(url)
            beans = [bean for bean in beans if (bean and bean.text)]
            logger.info("%d beans collected: %s", len(beans) if beans else 0, url)
            if beans and store_func:
                store_func(beans)                          
        except Exception as err:
            logger.warning("Failed storing beans from: %s. Error: %s", url, str(err))

def collect_from(url):
    feed = feedparser.parse(url)  
    updated = int(datetime.now().timestamp())
    make_bean = lambda entry: Bean(
        url=entry[T_LINK],
        updated = updated,
        source = tldextract.extract(entry[T_LINK]).domain,
        title=entry[T_TITLE],
        kind = ARTICLE,
        text=parse_description(entry),
        author=entry.get(T_AUTHOR),
        created=int(time.mktime(entry[T_PUBLISHED])),
        tags=[tag.term for tag in entry.get(T_TAGS, [])]
    )    
    return [make_bean(entry) for entry in feed.entries]

MIN_PULL_LIMIT = 1000
# the main body usually lives in <description> or <content:encoded>
# load the largest one, then check if this is above min_pull_limit. 
# if not, load_html
def parse_description(entry):    
    if T_CONTENT in entry:        
        html = entry[T_CONTENT][0]['value']
    else:
        html = entry.get(T_SUMMARY)
    
    if html:  
        text = BeautifulSoup(html, "html.parser").get_text(strip=True)      
        if len(text) < MIN_PULL_LIMIT:
            _temp_text = load_text_from_url(entry[T_LINK])
            # it may be that the url does not allow pulling in the content
            # in that case just stay with what we got from the feed 
            text = _temp_text if len(_temp_text) > len(text) else text
        return text 

def _sanitation_check(beans: list[Bean]):        
    for bean in beans:
        res = []
        if not bean.text:
            res.append("text")
        if not bean.created:
            res.append("created")            
        if not bean.author:
            res.append("author")            
        if not bean.tags:
            res.append("keywords")
        if res:
            ic(bean.url, res)