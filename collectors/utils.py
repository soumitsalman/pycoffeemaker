## URL/HTML LOADING RELATED
# import requests
# USER_AGENT = "Cafecito"
import newspaper
from bs4 import BeautifulSoup


def load_text_from_url(url):
    try:
        article = newspaper.Article(url)
        article.download()
        article.parse()
        return article.text
    except newspaper.article.ArticleException:
        return None

def load_text_from_html(partial_html):
    if partial_html:
        return BeautifulSoup(partial_html, "lxml").get_text(separator="\n", strip=True)
