import newspaper
import re
from bs4 import BeautifulSoup
from icecream import ic

USER_AGENT = "Cafecito-Coffeemaker"

def load_from_url(url) -> str:
    if is_non_text(url):
        return None
    try:
        article = newspaper.Article(url)
        article.download()
        article.parse()
        return article.text
    except newspaper.article.ArticleException:
        return None

def load_from_html(partial_html) -> str:
    if partial_html:
        return BeautifulSoup(partial_html, "lxml").get_text(separator="\n", strip=True)

def is_non_text(url: str):
    return url.endswith((".png",".jpeg", ".jpg", ".gif", ".webp", ".mp4", ".avi", ".mkv", ".mp3", ".wav", ".pdf")) or \
        re.search(r'(v\.redd\.it|i\.redd\.it|www\.reddit\.com\/gallery|youtube\.com|youtu\.be)', url)