__all__ = ['collector', 'rssfeed', 'ychackernews', 'redditor', 'espresso', 'individual']  # Specify modules to be exported

USER_AGENT = "ubuntu:Cafecito-Coffeemaker:v0.1.21"
TIMEOUT = 10 # 10 seconds
RATELIMIT_WAIT = 600 # 600 seconds / 10 minutes
RSSFEEDS = "./coffeemaker/collectors/rssfeedsources.txt"
SUBREDDITS = "./coffeemaker/collectors/redditsources.txt"

from .rssfeed import *
from .ychackernews import *
from .redditor import *
from .espresso import *
from .individual import *
from .collector import *