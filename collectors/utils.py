## URL/HTML LOADING RELATED
import requests
from bs4 import BeautifulSoup
USER_AGENT = "Cafecito"
def load_text_from_url(url):
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=(3, 10))
    if resp.status_code == requests.codes["ok"]:
        soup = BeautifulSoup(resp.text, "html.parser")
        return "\n\n".join([section.get_text(separator="\n", strip=True) for section in soup.find_all(["post", "content", "article", "main", "div"])])
    return ""  