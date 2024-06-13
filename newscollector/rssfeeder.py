################
## RSS Reader ##
################

import feedparser
from beanops.datamodels import Bean, ARTICLE
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from shared.utils import create_logger
import time
import tldextract
from icecream import ic

FEED_SOURCES = "newscollector/feedsources.txt"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59"
T_LINK = "link"
T_TITLE = "title"
T_TAGS = "tags"
T_SUMMARY = "summary"
T_CONTENT = "content"
T_PUBLISHED = 'published_parsed'
T_AUTHOR = 'author'

logger = create_logger("newscollector")

# reads the list of feeds from a file path and collects
# if sources is a string then it will be treated as a file path or else it will be a an array
def collect(sources: str|list[str] = FEED_SOURCES, store_func = None):
    if isinstance(sources, str):
        # if sources are not provided, assume that there is sources_file provided
        with open(sources, 'r') as file:
            sources = file.readlines()
    # santize the urls and start collecting
    sources = [url.strip() for url in sources if url.strip()]
    for url in sources:
        try:
            beans = collect_from(url)
            logger.info("%d beans collected: %s", len(beans) if beans else 0, url)
            if beans and store_func:
                store_func(beans)                          
        except Exception as err:
            logger.warning("Failed storing beans from: %s. Error: %s", url, str(err))

def collect_from(url):
    feed = feedparser.parse(url, agent=USER_AGENT)  
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
            # TODO: load the body
            resp = requests.get(entry[T_LINK], headers={"User-Agent": USER_AGENT})
            if resp.status_code == requests.codes["ok"]:
                soup = BeautifulSoup(resp.text, "html.parser")
                text = "\n\n".join([section.get_text(separator="\n", strip=True) for section in soup.find_all(["post", "content", "article", "main"])])
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