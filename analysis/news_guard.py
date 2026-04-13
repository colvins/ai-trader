"""Lightweight news risk veto layer for execution decisions.

This module does not affect stock scoring or ranking. It only provides a
negative-news filter that can veto an otherwise valid execution signal.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


NEGATIVE = "NEGATIVE"
NEUTRAL = "NEUTRAL"
POSITIVE = "POSITIVE"

NEGATIVE_TDNET_KEYWORDS = [
    "下方修正",
    "減配",
    "赤字",
]

NEGATIVE_NEWS_KEYWORDS = [
    "downgrade",
    "miss",
    "fraud",
    "investigation",
]


@dataclass
class NewsGuardDecision:
    news_risk_level: str
    news_reason: str


def _normalize_titles(values: list[str] | str | None) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        text = values.strip()
        return [text] if text else []
    return [str(item).strip() for item in values if str(item or "").strip()]


def evaluate_news_guard(
    symbol: str,
    *,
    tdnet_titles: list[str] | str | None = None,
    news_titles: list[str] | str | None = None,
) -> NewsGuardDecision:
    current_symbol = str(symbol or "").strip()
    normalized_tdnet_titles = _normalize_titles(tdnet_titles)
    normalized_news_titles = _normalize_titles(news_titles)

    for title in normalized_tdnet_titles:
        for keyword in NEGATIVE_TDNET_KEYWORDS:
            if keyword in title:
                return NewsGuardDecision(
                    news_risk_level=NEGATIVE,
                    news_reason=f"{current_symbol or '该股票'}最近公告出现“{keyword}”相关表述，触发消息风控否决。",
                )

    for title in normalized_news_titles:
        lowered = title.lower()
        for keyword in NEGATIVE_NEWS_KEYWORDS:
            if keyword in lowered:
                return NewsGuardDecision(
                    news_risk_level=NEGATIVE,
                    news_reason=f"{current_symbol or '该股票'}相关新闻标题出现“{keyword}”，触发消息风控否决。",
                )

    return NewsGuardDecision(
        news_risk_level=NEUTRAL,
        news_reason="未发现明显负面公告或新闻关键词。",
    )
