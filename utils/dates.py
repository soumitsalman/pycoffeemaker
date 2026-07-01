from datetime import datetime, timedelta

now = datetime.now
ndays_ago = lambda days: (now() - timedelta(days=days)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
