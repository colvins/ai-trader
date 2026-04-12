"""Trend mode candidate filtering and sorting."""

from __future__ import annotations


def passes_filter(x: dict) -> bool:
    return (
        x["amount"] > 2_000_000_000
        and x["momentum_5_pct"] > 3
        and -5 < x["dist_to_high_20_pct"] < -0.5
        and x["day_change_pct"] < 5
        and x["intraday_pct"] > 0
        and x["dist_to_high_60_pct"] > -20
    )


def sort_key(x: dict) -> tuple:
    return (
        x["momentum_5_pct"],
        x["dist_to_high_20_pct"],
        x["amount"],
        x["close_position"],
    )
