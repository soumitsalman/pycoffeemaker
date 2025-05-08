USER_AGENT = "Cafecito-Coffeemaker/v0.2.0+https://github.com/soumitsalman/pycoffeemaker"
TIMEOUT =  60 # 1 minute
RATELIMIT_WAIT = 600 # 600 seconds / 10 minutes

__all__ = ['collector']  # Specify modules to be exported

# from .rssfeed import *
# from .ychackernews import *
# from .redditor import *
# from .espresso import *
# from .individual import *
from .collector import *