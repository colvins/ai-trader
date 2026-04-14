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
SIGNAL_ID_REQUIRED_COLUMNS = ["run_date", "selected_mode", "strategy_source", "symbol", "rank"]
SIGNAL_MARKER_COLUMNS = [
    "signal_id",
    "run_date",
    "selected_mode",
    "symbol",
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


def _ensure_signal_ids(df: pd.DataFrame) -> pd.DataFrame:
    current = df.copy()
    if "signal_id" not in current.columns:
        current["signal_id"] = ""
    else:
        current["signal_id"] = current["signal_id"].fillna("").astype(str).str.strip()

    missing_mask = current["signal_id"] == ""
    if not bool(missing_mask.any()):
        return current

    run_date = current["run_date"].fillna("").astype(str).str.strip() if "run_date" in current.columns else ""
    selected_mode = current["selected_mode"].fillna("").astype(str).str.strip() if "selected_mode" in current.columns else ""
    strategy_source = current["strategy_source"].fillna("").astype(str).str.strip() if "strategy_source" in current.columns else ""
    symbol = current["symbol"].fillna("").astype(str).str.strip() if "symbol" in current.columns else ""
    rank = (
        pd.to_numeric(current["rank"], errors="coerce").fillna(0).astype(int).astype(str)
        if "rank" in current.columns
        else "0"
    )
    generated_ids = run_date + "|" + selected_mode + "|" + strategy_source + "|" + symbol + "|" + rank
    current.loc[missing_mask, "signal_id"] = generated_ids.loc[missing_mask]
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
    current = _ensure_signal_ids(_ensure_signal_columns(df))
    if current.empty:
        return current

    current["_run_date_dt"] = pd.to_datetime(current["run_date"], errors="coerce").dt.normalize()
    current["_symbol_key"] = current["symbol"].astype(str).str.replace(".0", "", regex=False).str.strip()
    current["_mode_key"] = current["selected_mode"].astype(str).str.strip().str.lower()

    current["is_repeat_signal"] = 0
    current["consecutive_days"] = 1
    valid_dates = (
        current["_run_date_dt"]
        .dropna()
        .drop_duplicates()
        .sort_values()
        .to_frame(name="_run_date_dt")
    )
    if valid_dates.empty:
        return current.drop(columns=["_run_date_dt", "_symbol_key", "_mode_key"], errors="ignore")

    valid_dates["_expected_previous"] = valid_dates["_run_date_dt"].shift(1)

    unique_signals = (
        current.loc[current["_run_date_dt"].notna(), ["_symbol_key", "_mode_key", "_run_date_dt"]]
        .drop_duplicates(subset=["_symbol_key", "_mode_key", "_run_date_dt"])
        .sort_values(["_symbol_key", "_mode_key", "_run_date_dt"])
        .merge(valid_dates, on="_run_date_dt", how="left")
    )

    group_keys = ["_symbol_key", "_mode_key"]
    unique_signals["_previous_seen_date"] = unique_signals.groupby(group_keys, sort=False)["_run_date_dt"].shift(1)
    unique_signals["_is_consecutive"] = unique_signals["_previous_seen_date"].eq(unique_signals["_expected_previous"])
    unique_signals["_streak_break"] = (~unique_signals["_is_consecutive"]).astype(int)
    unique_signals["_streak_id"] = unique_signals.groupby(group_keys, sort=False)["_streak_break"].cumsum()
    unique_signals["consecutive_days"] = (
        unique_signals.groupby(group_keys + ["_streak_id"], sort=False).cumcount() + 1
    )

    marker_lookup = unique_signals[["_symbol_key", "_mode_key", "_run_date_dt", "consecutive_days"]]
    current = current.merge(
        marker_lookup,
        on=["_symbol_key", "_mode_key", "_run_date_dt"],
        how="left",
        suffixes=("", "_computed"),
    )
    current["consecutive_days"] = (
        pd.to_numeric(current["consecutive_days_computed"], errors="coerce").fillna(1).astype(int)
    )
    current["is_repeat_signal"] = (current["consecutive_days"] > 1).astype(int)
    return current.drop(
        columns=[
            "_run_date_dt",
            "_symbol_key",
            "_mode_key",
            "consecutive_days_computed",
        ],
        errors="ignore",
    )


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


def _signal_marker_map(df: pd.DataFrame) -> dict[str, dict[str, int]]:
    if df.empty:
        return {}
    current = _ensure_signal_ids(df)
    marker_frame = (
        current[["signal_id", "is_repeat_signal", "consecutive_days"]]
        .drop_duplicates(subset=["signal_id"], keep="last")
        .copy()
    )
    return marker_frame.set_index("signal_id")[["is_repeat_signal", "consecutive_days"]].to_dict(orient="index")


def _apply_signal_markers_to_result(result: PickResult, marker_map: dict[str, dict[str, int]], rows: list[dict]) -> None:
    for pick, row in zip(result.picks, rows):
        markers = marker_map.get(row["signal_id"], {})
        pick.raw["is_repeat_signal"] = int(markers.get("is_repeat_signal", 0) or 0)
        pick.raw["consecutive_days"] = int(markers.get("consecutive_days", 1) or 1)


def _read_signal_header(target: Path) -> list[str]:
    try:
        return list(pd.read_csv(target, encoding="utf-8-sig", nrows=0).columns)
    except Exception:
        return []


def _can_append_without_rewrite(target: Path, new_df: pd.DataFrame) -> bool:
    if not target.exists():
        return False
    header_columns = _read_signal_header(target)
    if not header_columns:
        return False
    if set(header_columns) != set(SIGNAL_COLUMNS):
        return False

    try:
        existing_meta = pd.read_csv(
            target,
            encoding="utf-8-sig",
            usecols=["signal_id", "run_date"],
        )
    except Exception:
        return False

    existing_meta = _ensure_signal_ids(existing_meta)
    if existing_meta.empty:
        return True

    existing_ids = set(existing_meta["signal_id"].astype(str).str.strip().tolist())
    new_ids = set(new_df["signal_id"].astype(str).str.strip().tolist())
    if existing_ids.intersection(new_ids):
        return False

    existing_max_date = pd.to_datetime(existing_meta["run_date"], errors="coerce").max()
    new_min_date = pd.to_datetime(new_df["run_date"], errors="coerce").min()
    if pd.notna(existing_max_date) and pd.notna(new_min_date) and new_min_date < existing_max_date:
        return False
    return True


def _compute_append_markers(target: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    existing_meta = pd.read_csv(
        target,
        encoding="utf-8-sig",
        usecols=SIGNAL_MARKER_COLUMNS,
    )
    marker_source = pd.concat(
        [
            _ensure_signal_ids(_ensure_signal_columns(existing_meta))[SIGNAL_MARKER_COLUMNS],
            new_df[SIGNAL_MARKER_COLUMNS],
        ],
        ignore_index=True,
    )
    marked = attach_repeat_signal_markers(marker_source)
    new_markers = (
        marked[marked["signal_id"].isin(new_df["signal_id"])]
        [["signal_id", "is_repeat_signal", "consecutive_days"]]
        .drop_duplicates(subset=["signal_id"], keep="last")
    )
    appended = new_df.drop(columns=["is_repeat_signal", "consecutive_days"], errors="ignore").merge(
        new_markers,
        on="signal_id",
        how="left",
    )
    appended["is_repeat_signal"] = pd.to_numeric(appended["is_repeat_signal"], errors="coerce").fillna(0).astype(int)
    appended["consecutive_days"] = pd.to_numeric(appended["consecutive_days"], errors="coerce").fillna(1).astype(int)
    return appended[SIGNAL_COLUMNS].copy()


def _rewrite_signal_file(target: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    if target.exists():
        try:
            old_df = pd.read_csv(target, encoding="utf-8-sig")
        except Exception:
            old_df = pd.DataFrame(columns=SIGNAL_COLUMNS)
    else:
        old_df = pd.DataFrame(columns=SIGNAL_COLUMNS)

    old_df = _ensure_signal_ids(_ensure_signal_columns(old_df))
    combined = pd.concat([old_df, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["signal_id"], keep="last")
    combined = attach_repeat_signal_markers(combined)
    combined = combined[SIGNAL_COLUMNS].copy()
    combined = combined.sort_values(["run_date", "selected_mode", "rank", "symbol"]).reset_index(drop=True)
    combined.to_csv(target, index=False, encoding="utf-8-sig")
    return combined


def save_pick_result_signals(result: PickResult, path: Path | None = None) -> Path:
    target = path or SIGNALS_FILE
    target.parent.mkdir(parents=True, exist_ok=True)

    new_rows = build_signal_rows(result)
    new_df = _ensure_signal_ids(_ensure_signal_columns(pd.DataFrame(new_rows, columns=SIGNAL_COLUMNS)))

    if not target.exists():
        combined = attach_repeat_signal_markers(new_df)
        combined = combined[SIGNAL_COLUMNS].copy()
        combined.to_csv(target, index=False, encoding="utf-8-sig")
        _apply_signal_markers_to_result(result, _signal_marker_map(combined), new_rows)
        return target

    if _can_append_without_rewrite(target, new_df):
        appended = _compute_append_markers(target, new_df)
        header_columns = _read_signal_header(target)
        appended = appended.reindex(columns=header_columns)
        appended.to_csv(target, mode="a", header=False, index=False, encoding="utf-8-sig")
        _apply_signal_markers_to_result(result, _signal_marker_map(appended), new_rows)
        return target

    combined = _rewrite_signal_file(target, new_df)
    _apply_signal_markers_to_result(result, _signal_marker_map(combined), new_rows)

    return target
