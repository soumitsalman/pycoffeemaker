__all__ = ['collector', 'rssfeed', 'ychackernews', 'redditor', 'espresso', 'individual']  # Specify modules to be exported

USER_AGENT = "ubuntu:Cafecito-Coffeemaker:v0.1.21"
TIMEOUT = 5 # seconds
RATELIMIT_WAIT = 120 # seconds

from .rssfeed import *
from .ychackernews import *
from .redditor import *
from .espresso import *
from .individual import *
from .collector import *