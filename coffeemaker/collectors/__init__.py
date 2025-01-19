__all__ = ['rssfeed', 'ychackernews', 'redditor', 'espresso', 'individual']  # Specify modules to be exported

import os
USER_AGENT = os.getenv("COLLECTORS_USER_AGENT", "github.com/soumitsalman/pycoffeemaker/v0.1.20")
TIMEOUT = int(os.getenv("COLLECTORS_TIMEOUT", 5)) # default timeout is 5 seconds

from .rssfeed import *
from .ychackernews import *
from .redditor import *
from .espresso import *
from .individual import *