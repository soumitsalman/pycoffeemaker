import newspaper
import re
from bs4 import BeautifulSoup
from icecream import ic
import tldextract

USER_AGENT = "Cafecito-Coffeemaker"

def load_from_url(url) -> tuple[str, str, str]:
    if is_non_text(url):
        return None
    try:
        article = newspaper.Article(url)
        article.download()
        article.parse()
        return article.text.strip(), article.top_image, article.meta_data['og'].get('site_name') if 'og' in article.meta_data else None
    except newspaper.article.ArticleException:
        return None

def load_from_html(partial_html) -> str:
    if partial_html:
        return BeautifulSoup(partial_html, "lxml").get_text(separator="\n", strip=True).strip()

def is_non_text(url: str):
    return url.endswith((".png",".jpeg", ".jpg", ".gif", ".webp", ".mp4", ".avi", ".mkv", ".mp3", ".wav", ".pdf")) or \
        re.search(r'(v\.redd\.it|i\.redd\.it|www\.reddit\.com\/gallery|youtube\.com|youtu\.be)', url)

def extract_source(url):
    extracted = tldextract.extract(url)
    return extracted.domain, extracted.registered_domain