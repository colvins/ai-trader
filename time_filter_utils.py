from __future__ import annotations

from datetime import datetime, timedelta


def _local_now() -> datetime:
    return datetime.now().astimezone()


def parse_datetime(value: str, default_tz=None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None

    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=default_tz or _local_now().tzinfo)
        return dt
    except Exception:
        return None


def is_within_hours(value: str, hours: int, default_tz=None, now: datetime | None = None) -> bool:
    dt = parse_datetime(value, default_tz=default_tz)
    if dt is None:
        return False

    current = now or _local_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=default_tz or _local_now().tzinfo)
    return current - dt <= timedelta(hours=hours)


def is_within_days(value: str, days: int, default_tz=None, now: datetime | None = None) -> bool:
    dt = parse_datetime(value, default_tz=default_tz)
    if dt is None:
        return False

    current = now or _local_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=default_tz or _local_now().tzinfo)
    return current - dt <= timedelta(days=days)


def is_within_natural_days(value: str, days: int, default_tz=None, now: datetime | None = None) -> bool:
    dt = parse_datetime(value, default_tz=default_tz)
    if dt is None:
        return False

    current = now or _local_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=default_tz or _local_now().tzinfo)
    return (current.date() - dt.date()).days <= days
