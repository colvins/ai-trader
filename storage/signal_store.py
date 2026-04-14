"""Persistence helpers for pick signals used by lightweight backtesting."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from reporting.schemas import PickResult


BACKTEST_DIR = Path("data/backtest")
SIGNALS_FILE = BACKTEST_DIR / "signals.csv"


SIGNAL_COLUMNS = [
    "signal_id",
    "run_date",
    "generated_at",
    "market_state",
    "market_up_ratio",
    "market_avg_change_pct",
    "selected_mode",
    "strategy_source",
    "symbol",
    "rank",
    "score",
    "level",
    "action",
    "option_bias",
    "option_horizon",
    "option_reason",
    "option_risk",
    "tdnet_signal",
    "tdnet_score",
    "tdnet_title",
    "news_title",
    "news_source",
    "news_published_at",
    "execution_result",
    "execution_checked_at",
    "news_risk_level",
    "close",
    "prev_close",
    "day_change_pct",
    "intraday_pct",
    "amplitude_pct",
    "amount_ratio_5",
    "momentum_3_pct",
    "momentum_5_pct",
    "dist_to_high_5_pct",
    "dist_to_high_20_pct",
    "close_position",
    "is_repeat_signal",
    "consecutive_days",
]


def build_signal_id(
    *,
    run_date: str,
    selected_mode: str,
    strategy_source: str,
    symbol: str,
    rank: int,
) -> str:
    return "|".join(
        [
            str(run_date).strip(),
            str(selected_mode).strip(),
            str(strategy_source).strip(),
            str(symbol).strip(),
            str(rank),
        ]
    )


def _signal_run_date(result: PickResult) -> str:
    if result.market_state.data_date:
        return result.market_state.data_date
    if result.status.data_date:
        return result.status.data_date
    if result.generated_at:
        return str(result.generated_at).split("T", 1)[0]
    return ""


def _ensure_signal_columns(df: pd.DataFrame) -> pd.DataFrame:
    current = df.copy()
    for col in SIGNAL_COLUMNS:
        if col not in current.columns:
            current[col] = pd.NA

    current["tdnet_signal"] = current["tdnet_signal"].fillna("无").replace("", "无")
    current["tdnet_score"] = pd.to_numeric(current["tdnet_score"], errors="coerce").fillna(0.0)
    current["tdnet_title"] = current["tdnet_title"].fillna("")
    current["news_title"] = current["news_title"].fillna("")
    current["news_source"] = current["news_source"].fillna("")
    current["news_published_at"] = current["news_published_at"].fillna("")
    current["execution_result"] = current["execution_result"].fillna("")
    current["execution_checked_at"] = current["execution_checked_at"].fillna("")
    current["news_risk_level"] = current["news_risk_level"].fillna("")
    current["is_repeat_signal"] = pd.to_numeric(current["is_repeat_signal"], errors="coerce").fillna(0).astype(int)
    current["consecutive_days"] = pd.to_numeric(current["consecutive_days"], errors="coerce").fillna(1).astype(int)
    return current


def _pick_primary_news_fields(pick) -> tuple[str, str, str]:
    items = getattr(pick, "news_items", []) or []
    for item in items:
        title = str(getattr(item, "title", "") or "").strip()
        if not title:
            continue
        source = str(getattr(item, "source", "") or "").strip()
        published_at = str(getattr(item, "published_at", "") or "").strip()
        return title, source, published_at
    return "", "", ""


def attach_repeat_signal_markers(df: pd.DataFrame) -> pd.DataFrame:
    current = _ensure_signal_columns(df)
    if current.empty:
        return current

    current["_run_date_dt"] = pd.to_datetime(current["run_date"], errors="coerce").dt.normalize()
    current["_symbol_key"] = current["symbol"].astype(str).str.replace(".0", "", regex=False).str.strip()
    current["_mode_key"] = current["selected_mode"].astype(str).str.strip().str.lower()

    valid_dates = (
        current["_run_date_dt"].dropna().drop_duplicates().sort_values().tolist()
    )
    previous_date_map = {
        valid_dates[index]: (valid_dates[index - 1] if index > 0 else pd.NaT)
        for index in range(len(valid_dates))
    }

    current["is_repeat_signal"] = 0
    current["consecutive_days"] = 1

    unique_signals = current.dropna(subset=["_run_date_dt"]).copy()
    unique_signals = unique_signals.drop_duplicates(subset=["_symbol_key", "_mode_key", "_run_date_dt"])
    unique_signals = unique_signals.sort_values(["_symbol_key", "_mode_key", "_run_date_dt"])

    consecutive_map: dict[tuple[str, str, pd.Timestamp], int] = {}
    for (symbol_key, mode_key), group in unique_signals.groupby(["_symbol_key", "_mode_key"], sort=False):
        streak = 0
        previous_seen_date = pd.NaT

        for _, row in group.iterrows():
            run_date = row["_run_date_dt"]
            expected_previous = previous_date_map.get(run_date, pd.NaT)
            if pd.notna(expected_previous) and previous_seen_date == expected_previous:
                streak += 1
            else:
                streak = 1
            consecutive_map[(symbol_key, mode_key, run_date)] = streak
            previous_seen_date = run_date

    def _lookup_consecutive_days(row: pd.Series) -> int:
        run_date = row["_run_date_dt"]
        if pd.isna(run_date):
            return 1
        return int(
            consecutive_map.get(
                (row["_symbol_key"], row["_mode_key"], run_date),
                1,
            )
        )

    current["consecutive_days"] = current.apply(_lookup_consecutive_days, axis=1)
    current["is_repeat_signal"] = (current["consecutive_days"] > 1).astype(int)
    return current.drop(columns=["_run_date_dt", "_symbol_key", "_mode_key"], errors="ignore")


def build_signal_rows(result: PickResult) -> list[dict]:
    run_date = _signal_run_date(result)
    rows = []

    for rank, pick in enumerate(result.picks, start=1):
        news_title, news_source, news_published_at = _pick_primary_news_fields(pick)
        signal_id = build_signal_id(
            run_date=run_date,
            selected_mode=result.mode,
            strategy_source=result.mode_source,
            symbol=pick.symbol,
            rank=rank,
        )
        rows.append(
            {
                "signal_id": signal_id,
                "run_date": run_date,
                "generated_at": result.generated_at,
                "market_state": result.market_state.state,
                "market_up_ratio": result.market_state.up_ratio,
                "market_avg_change_pct": result.market_state.avg_change_pct,
                "selected_mode": result.mode,
                "strategy_source": result.mode_source,
                "symbol": pick.symbol,
                "rank": rank,
                "score": pick.score,
                "level": pick.level,
                "action": pick.action,
                "option_bias": pick.option_bias,
                "option_horizon": pick.option_horizon,
                "option_reason": pick.option_reason,
                "option_risk": pick.option_risk,
                "tdnet_signal": str(pick.raw.get("tdnet_signal", "无") or "无"),
                "tdnet_score": pick.raw.get("tdnet_score", 0) or 0,
                "tdnet_title": str(pick.raw.get("tdnet_title", "") or ""),
                "news_title": news_title,
                "news_source": news_source,
                "news_published_at": news_published_at,
                "execution_result": str(pick.raw.get("execution_result", "") or ""),
                "execution_checked_at": str(pick.raw.get("execution_checked_at", "") or ""),
                "news_risk_level": str(pick.raw.get("news_risk_level", "") or ""),
                "close": pick.close,
                "prev_close": pick.prev_close,
                "day_change_pct": pick.day_change_pct,
                "intraday_pct": pick.intraday_pct,
                "amplitude_pct": pick.amplitude_pct,
                "amount_ratio_5": pick.amount_ratio_5,
                "momentum_3_pct": pick.momentum_3_pct,
                "momentum_5_pct": pick.momentum_5_pct,
                "dist_to_high_5_pct": pick.dist_to_high_5_pct,
                "dist_to_high_20_pct": pick.dist_to_high_20_pct,
                "close_position": pick.close_position,
                "is_repeat_signal": int(pick.raw.get("is_repeat_signal", 0) or 0),
                "consecutive_days": int(pick.raw.get("consecutive_days", 1) or 1),
            }
        )

    return rows


def save_pick_result_signals(result: PickResult, path: Path | None = None) -> Path:
    target = path or SIGNALS_FILE
    target.parent.mkdir(parents=True, exist_ok=True)

    new_rows = build_signal_rows(result)
    new_df = pd.DataFrame(new_rows, columns=SIGNAL_COLUMNS)

    if target.exists():
        try:
            old_df = pd.read_csv(target, encoding="utf-8-sig")
        except Exception:
            old_df = pd.DataFrame(columns=SIGNAL_COLUMNS)

        if not old_df.empty:
            if "signal_id" not in old_df.columns:
                old_df["signal_id"] = pd.NA
            missing_mask = old_df["signal_id"].isna() | (old_df["signal_id"].astype(str).str.strip() == "")
            if missing_mask.any():
                old_df.loc[missing_mask, "signal_id"] = old_df.loc[missing_mask].apply(
                    lambda row: build_signal_id(
                        run_date=row.get("run_date", ""),
                        selected_mode=row.get("selected_mode", ""),
                        strategy_source=row.get("strategy_source", ""),
                        symbol=row.get("symbol", ""),
                        rank=int(row.get("rank", 0) or 0),
                    ),
                    axis=1,
                )
        combined = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = attach_repeat_signal_markers(combined)
    combined = combined[SIGNAL_COLUMNS].copy()
    combined = combined.drop_duplicates(
        subset=["signal_id"],
        keep="last",
    )
    combined = combined.sort_values(["run_date", "selected_mode", "rank", "symbol"]).reset_index(drop=True)
    combined.to_csv(target, index=False, encoding="utf-8-sig")

    signal_marker_map = (
        combined.set_index("signal_id")[["is_repeat_signal", "consecutive_days"]].to_dict(orient="index")
        if not combined.empty
        else {}
    )
    for pick, row in zip(result.picks, new_rows):
        markers = signal_marker_map.get(row["signal_id"], {})
        pick.raw["is_repeat_signal"] = int(markers.get("is_repeat_signal", 0) or 0)
        pick.raw["consecutive_days"] = int(markers.get("consecutive_days", 1) or 1)

    return target
