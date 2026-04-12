"""Shared helpers for mode-based candidate filtering."""

from __future__ import annotations

from engine.scoring import breakout, dip, trend


FILTERS = {
    "breakout": breakout.passes_filter,
    "trend": trend.passes_filter,
    "dip": dip.passes_filter,
}

SORT_KEYS = {
    "breakout": breakout.sort_key,
    "trend": trend.sort_key,
    "dip": dip.sort_key,
}


def normalize_mode(mode: str) -> str:
    mode = str(mode or "trend").strip().lower()
    if mode not in FILTERS:
        return "trend"
    return mode


def filter_candidates(results: list[dict], mode: str) -> list[dict]:
    current_mode = normalize_mode(mode)
    predicate = FILTERS[current_mode]
    return [item for item in results if predicate(item)]


def sort_candidates(results: list[dict], mode: str) -> list[dict]:
    current_mode = normalize_mode(mode)
    sorter = SORT_KEYS[current_mode]
    return sorted(results, key=sorter, reverse=True)
