from datetime import datetime, timedelta, timezone


def now() -> datetime:
    return datetime.now(timezone.utc)


def ndays_ago(days: int) -> datetime:
    return now() - timedelta(days=days)


def ndays_ago_str(days: int) -> str:
    return ndays_ago(days).strftime("%Y-%m-%d")


def ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
