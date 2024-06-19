from icecream import ic

## LOGGING RELATED
import logging

_LOGGER_PATH = "app.log"

def set_logger_path(logger_path: str):
    global _LOGGER_PATH
    _LOGGER_PATH = logger_path

def create_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(_LOGGER_PATH)
    file_handler.setFormatter(logging.Formatter(f'[%(asctime)s] {name} - %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M"))
    logger.addHandler(file_handler)
    return logger


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