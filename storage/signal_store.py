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


def build_signal_rows(result: PickResult) -> list[dict]:
    run_date = _signal_run_date(result)
    rows = []

    for rank, pick in enumerate(result.picks, start=1):
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

    for col in SIGNAL_COLUMNS:
        if col not in combined.columns:
            combined[col] = pd.NA

    combined["tdnet_signal"] = combined["tdnet_signal"].fillna("无").replace("", "无")
    combined["tdnet_score"] = pd.to_numeric(combined["tdnet_score"], errors="coerce").fillna(0.0)
    combined["tdnet_title"] = combined["tdnet_title"].fillna("")

    combined = combined[SIGNAL_COLUMNS].copy()
    combined = combined.drop_duplicates(
        subset=["signal_id"],
        keep="last",
    )
    combined = combined.sort_values(["run_date", "selected_mode", "rank", "symbol"]).reset_index(drop=True)
    combined.to_csv(target, index=False, encoding="utf-8-sig")
    return target
