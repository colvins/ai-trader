from __future__ import annotations

from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

from time_filter_utils import is_within_hours


USER_AGENT = "Mozilla/5.0 (compatible; ai-trader-reuters/1.0)"
REUTERS_RSS_URLS = [
    "https://feeds.reuters.com/reuters/worldNews",
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/topNews",
]
LOCAL_TZ = datetime.now().astimezone().tzinfo


def _fetch_bytes(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=15) as response:
        return response.read()


def _normalize_published_at(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    try:
        return parsedate_to_datetime(text).isoformat()
    except Exception:
        return ""


def _parse_rss(xml_bytes: bytes) -> list[dict]:
    root = ET.fromstring(xml_bytes)
    items = []

    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        url = (item.findtext("link") or "").strip()
        published_at = _normalize_published_at(item.findtext("pubDate") or "")
        if not title or not published_at:
            continue
        items.append(
            {
                "title": title,
                "url": url,
                "published_at": published_at,
                "source": "reuters",
            }
        )

    return items


def _filter_recent_reuters_items(items: list[dict]) -> list[dict]:
    return [
        item
        for item in items
        if is_within_hours(item.get("published_at", ""), hours=72, default_tz=LOCAL_TZ)
    ]


def fetch_reuters_news(limit: int = 10) -> list[dict]:
    for rss_url in REUTERS_RSS_URLS:
        try:
            items = _filter_recent_reuters_items(_parse_rss(_fetch_bytes(rss_url)))
        except Exception:
            continue

        if items:
            return items[:limit]

    return []


if __name__ == "__main__":
    items = fetch_reuters_news(limit=5)
    print(f"reuters_recent_count={len(items)}")
    for item in items:
        print(item)
