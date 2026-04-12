"""Market state detection for automatic strategy selection."""

from __future__ import annotations

from engine.indicators import load_recent_history


def analyze_market_state(days: int = 5) -> dict:
    full_df, today_df, _ = load_recent_history(days=max(days, 3))
    grouped = full_df.groupby("code", sort=False)

    changes = []
    up_count = 0

    for symbol in today_df["code"].drop_duplicates().tolist():
        try:
            hist_df = grouped.get_group(symbol).sort_values("date", ascending=False).reset_index(drop=True)
            if len(hist_df) < 2:
                continue

            close = float(hist_df.iloc[0]["close"])
            prev_close = float(hist_df.iloc[1]["close"])
            if prev_close <= 0:
                continue

            day_change_pct = (close - prev_close) / prev_close * 100.0
            changes.append(day_change_pct)
            if day_change_pct > 0:
                up_count += 1
        except Exception:
            continue

    total = len(changes)
    up_ratio = (up_count / total) if total > 0 else 0.0
    avg_change_pct = (sum(changes) / total) if total > 0 else 0.0

    if up_ratio < 0.4:
        mode = "dip"
        state = "弱市"
    elif up_ratio > 0.6:
        mode = "trend"
        state = "强市"
    else:
        mode = "breakout"
        state = "震荡市"

    data_date = ""
    try:
        latest_date = today_df["date"].max()
        data_date = latest_date.strftime("%Y-%m-%d")
    except Exception:
        data_date = ""

    return {
        "state": state,
        "mode": mode,
        "up_ratio": round(up_ratio, 4),
        "avg_change_pct": round(avg_change_pct, 4),
        "total": total,
        "data_date": data_date,
    }


def choose_mode_by_market_state(days: int = 5) -> str:
    return analyze_market_state(days=days).get("mode", "trend")
