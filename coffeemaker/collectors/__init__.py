USER_AGENT = "Cafecito-Coffeemaker/v0.2.0+https://github.com/soumitsalman/pycoffeemaker"
TIMEOUT =  60 # 1 minute
RATELIMIT_WAIT = 600 # 600 seconds / 10 minutes


# from .rssfeed import *
# from .ychackernews import *
# from .redditor import *
# from .espresso import *
# from .individual import *
from .collector import *

__all__ = ['extract_base_url', 'extract_domain', 'parse_date', 'parse_sources', 'APICollector', 'WebScraper', 'REDDIT', 'HACKERNEWS']  # Specify modules to be exported
