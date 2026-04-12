"""News fetching and relevance filtering for candidate stocks."""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf


MARKETAUX_API_KEY = os.getenv("MARKETAUX_API_KEY")

DATA_DIR = Path("data")
UNIVERSE_FILE = DATA_DIR / "universe_jp.csv"

_NAME_CACHE = None


def _load_name_map():
    global _NAME_CACHE
    if _NAME_CACHE is not None:
        return _NAME_CACHE

    name_map = {}

    if UNIVERSE_FILE.exists():
        try:
            df = pd.read_csv(UNIVERSE_FILE, encoding="utf-8-sig")
            if "symbol" in df.columns and "name" in df.columns:
                df["symbol"] = df["symbol"].astype(str).str.replace(".0", "", regex=False).str.strip()
                df["name"] = df["name"].astype(str).str.strip()
                for _, row in df.iterrows():
                    symbol = row["symbol"]
                    name = row["name"]
                    if symbol and name and name.lower() != "nan":
                        name_map[symbol] = name
        except Exception:
            pass

    _NAME_CACHE = name_map
    return _NAME_CACHE


def _company_name(symbol: str) -> str:
    return _load_name_map().get(str(symbol).strip(), "")


def _normalize_text(s):
    return str(s or "").strip()


def _extract_symbol(stock) -> str:
    if isinstance(stock, dict):
        return str(stock.get("symbol", "")).strip()
    return str(stock).strip()


def _company_tokens(company: str):
    text = _normalize_text(company)
    if not text:
        return []

    ascii_tokens = [x.lower() for x in re.split(r"[^A-Za-z0-9]+", text) if len(x.strip()) >= 3]

    tokens = []
    if ascii_tokens:
        tokens.extend(ascii_tokens[:3])
    elif text:
        tokens.append(text.lower())

    return list(dict.fromkeys(tokens))


def _normalize_published_at(value):
    if value is None or value == "":
        return ""

    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
            return dt.isoformat()

        text = str(value).strip()
        if not text:
            return ""
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return str(value).strip()


def _extract_marketaux_published_at(item):
    for key in ("published_at", "publishedAt", "datetime", "date"):
        if key in item and item.get(key):
            return _normalize_published_at(item.get(key))
    return ""


def _extract_yfinance_published_at(item, content):
    for key in ("pubDate", "published_at", "publishedAt", "datetime", "date"):
        if isinstance(content, dict) and content.get(key):
            return _normalize_published_at(content.get(key))
        if isinstance(item, dict) and item.get(key):
            return _normalize_published_at(item.get(key))

    provider_time = None
    if isinstance(content, dict):
        provider_time = content.get("providerPublishTime")
    if provider_time is None and isinstance(item, dict):
        provider_time = item.get("providerPublishTime")

    if provider_time is not None:
        return _normalize_published_at(provider_time)
    return ""


def _published_sort_value(item) -> str:
    return _normalize_text(item.get("published_at", ""))


def _relevance_score(item, symbol: str, company: str) -> float:
    title = _normalize_text(item.get("title", ""))
    summary = _normalize_text(item.get("summary", ""))
    source = _normalize_text(item.get("source", "")).lower()
    haystack = f"{title} {summary}".lower()

    if not haystack:
        return 0.0

    score = 0.4
    tokens = _company_tokens(company)

    if company and company.lower() in haystack:
        score += 0.5
    elif any(token in haystack for token in tokens):
        score += 0.35

    if symbol and (symbol.lower() in haystack or f"{symbol}.t".lower() in haystack):
        score += 0.25

    if source.startswith("marketaux_symbol"):
        score += 0.25
    elif source == "yfinance":
        score += 0.15

    return max(0.0, min(1.0, score))


def _dedupe_news(items, limit=5):
    seen = set()
    result = []

    for item in items:
        title = _normalize_text(item.get("title"))
        if not title:
            continue

        key = " ".join(title.lower().split())
        if key in seen:
            continue

        seen.add(key)
        result.append(item)
        if len(result) >= limit:
            break

    return result


def _fetch_marketaux_by_symbol(symbol: str, limit: int = 5):
    if not MARKETAUX_API_KEY:
        return []

    url = "https://api.marketaux.com/v1/news/all"
    params = {
        "api_token": MARKETAUX_API_KEY,
        "symbols": f"{symbol}.T",
        "language": "en",
        "limit": limit,
    }

    try:
        response = requests.get(url, params=params, timeout=8)
        if not response.ok:
            return []
        data = response.json()
    except Exception:
        return []

    items = []
    for item in data.get("data", []):
        title = _normalize_text(item.get("title"))
        link = _normalize_text(item.get("url"))
        summary = _normalize_text(item.get("description") or item.get("snippet") or "")
        published_at = _extract_marketaux_published_at(item)
        if title:
            items.append(
                {
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "source": "marketaux_symbol",
                    "published_at": published_at,
                }
            )
    return items


def _fetch_marketaux_by_keyword(keyword: str, limit: int = 5):
    if not MARKETAUX_API_KEY or not keyword:
        return []

    url = "https://api.marketaux.com/v1/news/all"
    params = {
        "api_token": MARKETAUX_API_KEY,
        "search": keyword,
        "language": "en",
        "limit": limit,
    }

    try:
        response = requests.get(url, params=params, timeout=8)
        if not response.ok:
            return []
        data = response.json()
    except Exception:
        return []

    items = []
    for item in data.get("data", []):
        title = _normalize_text(item.get("title"))
        link = _normalize_text(item.get("url"))
        summary = _normalize_text(item.get("description") or item.get("snippet") or "")
        published_at = _extract_marketaux_published_at(item)
        if title:
            items.append(
                {
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "source": f"marketaux_search:{keyword}",
                    "published_at": published_at,
                }
            )
    return items


def _fetch_yfinance_news(symbol: str, limit: int = 5):
    try:
        ticker = yf.Ticker(f"{symbol}.T")
        raw_news = getattr(ticker, "news", []) or []
    except Exception:
        return []

    items = []
    for item in raw_news[: limit * 3]:
        try:
            content = item.get("content", {}) if isinstance(item, dict) else {}
            title = _normalize_text(content.get("title") or item.get("title") or "")
            summary = _normalize_text(content.get("summary") or content.get("description") or "")

            link = ""
            canonical = content.get("canonicalUrl")
            if isinstance(canonical, dict):
                link = _normalize_text(canonical.get("url"))
            if not link:
                link = _normalize_text(item.get("link") or "")

            published_at = _extract_yfinance_published_at(item, content)
            if title:
                items.append(
                    {
                        "title": title,
                        "link": link,
                        "summary": summary,
                        "source": "yfinance",
                        "published_at": published_at,
                    }
                )
        except Exception:
            continue

        if len(items) >= limit:
            break

    return items


def _filter_news(items, symbol: str, company: str, limit: int = 5):
    scored = []
    for item in items:
        relevance = _relevance_score(item, symbol, company)
        source = _normalize_text(item.get("source", "")).lower()

        if source.startswith("marketaux_search") and relevance < 0.75:
            continue
        if relevance < 0.45:
            continue

        new_item = dict(item)
        new_item["relevance"] = round(relevance, 3)
        scored.append(new_item)

    scored.sort(
        key=lambda x: (
            float(x.get("relevance", 0.0)),
            _published_sort_value(x),
        ),
        reverse=True,
    )
    return _dedupe_news(scored, limit=limit)


def get_news_for_stock(stock, max_items: int = 5):
    symbol = _extract_symbol(stock)
    if not symbol:
        return []

    company = _company_name(symbol)
    items = []
    items.extend(_fetch_marketaux_by_symbol(symbol, limit=max_items))
    time.sleep(0.3)

    if company:
        items.extend(_fetch_marketaux_by_keyword(company, limit=max_items))
        time.sleep(0.3)

    items.extend(_fetch_yfinance_news(symbol, limit=max_items))
    return _filter_news(items, symbol=symbol, company=company, limit=max_items)


def get_news_for_stocks(stocks, max_items: int = 5):
    news_map = {}

    for stock in stocks or []:
        symbol = _extract_symbol(stock)
        if not symbol:
            continue

        try:
            news_map[symbol] = get_news_for_stock(stock, max_items=max_items)
        except Exception:
            news_map[symbol] = []

        time.sleep(0.5)

    return news_map
