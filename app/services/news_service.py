"""Compatibility layer for news loading; later removable."""

from __future__ import annotations

from analysis.news_service import get_news_for_stock, get_news_for_stocks


__all__ = ["get_news_for_stock", "get_news_for_stocks"]
