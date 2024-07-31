import newspaper
from bs4 import BeautifulSoup
from icecream import ic

USER_AGENT = "Cafecito-Collector"

def load_from_url(url) -> str:
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
