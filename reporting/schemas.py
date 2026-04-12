"""Standardized report schemas shared by CLI, Telegram, and future APIs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NewsItem:
    title: str = ""
    link: str = ""
    summary: str = ""
    source: str = ""
    published_at: str = ""
    relevance: float | None = None


@dataclass
class DataStatus:
    ok: bool
    title: str
    text: str
    data_date: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class MarketState:
    state: str = ""
    mode: str = ""
    up_ratio: float = 0.0
    avg_change_pct: float = 0.0
    total: int = 0
    data_date: str = ""


@dataclass
class StockPick:
    symbol: str
    close: Any = None
    prev_close: Any = None
    score: float | None = None
    reason: str = ""
    mode: str = ""
    level: str = "C"
    action: str = "ignore"
    option_bias: str = ""
    option_horizon: str = ""
    option_reason: str = ""
    option_risk: str = ""
    ai_prompt: str = ""
    day_change_pct: float | None = None
    intraday_pct: float | None = None
    amplitude_pct: float | None = None
    amount_ratio_5: float | None = None
    momentum_3_pct: float | None = None
    momentum_5_pct: float | None = None
    dist_to_high_5_pct: float | None = None
    dist_to_high_20_pct: float | None = None
    close_position: float | None = None
    tech_score: float | None = None
    news_score: float | None = None
    bias_score: float | None = None
    tech_parts: list[str] = field(default_factory=list)
    news_parts: list[str] = field(default_factory=list)
    news_items: list[NewsItem] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class PickResult:
    mode: str
    status: DataStatus
    market_state: MarketState = field(default_factory=MarketState)
    mode_source: str = "manual"
    picks: list[StockPick] = field(default_factory=list)
    candidate_count: int = 0
    scored_count: int = 0
    candidate_limit: int = 0
    limit: int = 0
    generated_at: str = ""
