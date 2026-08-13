from datetime import datetime, timedelta, timezone

MAX_CREATED_AGE_DAYS = 365 * 5


def now() -> datetime:
    return datetime.now(timezone.utc)

def now_str() -> str:
    return now().strftime("%Y-%m-%d-%H-%M-%S")

def ndays_ago(days: int) -> datetime:
    return now() - timedelta(days=days)

def usable_created(dt) -> bool:
    if not isinstance(dt, datetime):
        return False
    dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return dt >= ndays_ago(MAX_CREATED_AGE_DAYS)

def ndays_ago_str(days: int) -> str:
    return ndays_ago(days).strftime("%Y-%m-%d")

def date_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
