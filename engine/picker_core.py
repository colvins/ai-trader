"""Candidate stock selection entry point built on local daily cache."""

from __future__ import annotations

from engine.filters import filter_candidates, normalize_mode, sort_candidates
from engine.indicators import calc_features_from_history, load_recent_history


def get_candidate_stocks(limit: int = 30, mode: str = "trend") -> list[dict]:
    current_mode = normalize_mode(mode)
    full_df, today_df, latest_name = load_recent_history(days=60)

    print(f"使用行情文件: {latest_name}")
    print(f"有效股票数: {today_df['code'].nunique()}")

    results = []
    grouped = full_df.groupby("code", sort=False)

    for symbol in today_df["code"].drop_duplicates().tolist():
        try:
            hist_df = grouped.get_group(symbol)
            feat = calc_features_from_history(symbol, hist_df)
            if feat:
                results.append(feat)
        except Exception:
            continue

    filtered = filter_candidates(results, current_mode)
    print(f"初筛股票数量: {len(filtered)}")
    ordered = sort_candidates(filtered, current_mode)
    return ordered[:limit]
