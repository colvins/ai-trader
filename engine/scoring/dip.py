"""Dip mode candidate filtering and sorting."""

from __future__ import annotations


def passes_filter(x: dict) -> bool:
    return (
        x["amount"] > 1_500_000_000
        and -20 < x["dist_to_high_20_pct"] < -5
        and (
            x["day_change_pct"] >= 1.5
            or x["intraday_pct"] >= 1.0
        )
        and x["intraday_pct"] > 0
        and x["intraday_pct"] < 4
        and x["dist_to_high_5_pct"] < -0.5
        and x["day_change_pct"] < 7
        and x["close_position"] > 0.5
        and x["amount_ratio_5"] > 0.8
        and x["momentum_5_pct"] > -10
        and x["dist_to_high_60_pct"] > -40
    )


def sort_key(x: dict) -> tuple:
    return (
        x["close_position"],
        x["amount_ratio_5"],
        x["amount"],
        x["dist_to_high_20_pct"],
    )
