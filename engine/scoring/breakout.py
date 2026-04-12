"""Breakout mode candidate filtering and sorting."""

from __future__ import annotations


def passes_filter(x: dict) -> bool:
    return (
        x["day_change_pct"] > 3
        and x["day_change_pct"] < 12
        and x["amount"] > 2_000_000_000
        and x["amount_ratio_5"] > 1.3
        and x["close_position"] > 0.7
        and x["dist_to_high_60_pct"] > -25
    )


def sort_key(x: dict) -> tuple:
    return (
        x["day_change_pct"],
        x["amount_ratio_5"],
        x["amount"],
        x["close_position"],
    )
