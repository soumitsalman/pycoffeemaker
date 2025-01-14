import newspaper
import re
from bs4 import BeautifulSoup
from icecream import ic
import tldextract
from memoization import cached

USER_AGENT = "Cafecito-Coffeemaker"

def collect_url(url: str):
    if is_non_text(url):
        return None
    try:
        article = newspaper.Article(url)
        article.download()
        article.parse()
        return article if article.text else None
    except newspaper.article.ArticleException:
        pass
    
def collect_html(partial_html) -> str:
    if partial_html:
        return BeautifulSoup(partial_html, "lxml").get_text(separator="\n", strip=True).strip()

def is_non_text(url: str):
    return url.endswith((".png",".jpeg", ".jpg", ".gif", ".webp", ".mp4", ".avi", ".mkv", ".mp3", ".wav", ".pdf")) or \
        re.search(r'(v\.redd\.it|i\.redd\.it|www\.reddit\.com\/gallery|youtube\.com|youtu\.be)', url)

@cached(max_size=2000)
def extract_source(url):
    extracted = tldextract.extract(url)
    return extracted.domain, extracted.registered_domain

def site_name(article: newspaper.Article):
    if article and ('og' in article.meta_data):
        return article.meta_data['og'].get('site_name')