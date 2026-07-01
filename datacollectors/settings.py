import os

USER_AGENT = "Cafecito-Coffeemaker/v0.9.8+https://github.com/soumitsalman/pycoffeemaker"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
TIMEOUT = int(os.getenv("COLLECTOR_TIMEOUT", 120))
RATELIMIT_WAIT = 300
BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count() * os.cpu_count()))
RETRY_COUNT = 3
RETRY_JITTER = (1, 30)
MAX_HTML_SIZE = int(os.getenv("MAX_HTML_SIZE", 4 << 20))
MAX_PDF_SIZE = int(os.getenv("MAX_PDF_SIZE", 16 << 20))
