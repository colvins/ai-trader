"""Compatibility layer for local ranking; later removable."""

from __future__ import annotations

from analysis.local_ranker import (
    DEFAULT_MODE,
    SUPPORTED_MODES,
    analyze_stocks,
    build_reason,
    news_score,
    normalize_mode,
    symbol_bias,
    technical_score,
)


__all__ = [
    "DEFAULT_MODE",
    "SUPPORTED_MODES",
    "analyze_stocks",
    "build_reason",
    "news_score",
    "normalize_mode",
    "symbol_bias",
    "technical_score",
]
