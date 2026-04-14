"""Helpers for loading local price cache and enriching signals with outcomes."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from storage.signal_store import attach_repeat_signal_markers


BACKTEST_DIR = Path("data/backtest")
SIGNALS_FILE = BACKTEST_DIR / "signals.csv"
RESULTS_FILE = BACKTEST_DIR / "signals_with_results.csv"
JQ_DAILY_DIR = Path("data/jq_daily")


RESULT_COLUMNS = [
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "max_up_5d",
    "max_down_5d",
]


def load_signals(path: Path | None = None) -> pd.DataFrame:
    target = path or SIGNALS_FILE
    if not target.exists():
        return pd.DataFrame()
    df = pd.read_csv(target, encoding="utf-8-sig")
    if "signal_id" not in df.columns:
        from storage.signal_store import build_signal_id

        df["signal_id"] = df.apply(
            lambda row: build_signal_id(
                run_date=row.get("run_date", ""),
                selected_mode=row.get("selected_mode", ""),
                strategy_source=row.get("strategy_source", ""),
                symbol=row.get("symbol", ""),
                rank=int(row.get("rank", 0) or 0),
            ),
            axis=1,
        )

    if "tdnet_signal" not in df.columns:
        df["tdnet_signal"] = "无"
    else:
        df["tdnet_signal"] = df["tdnet_signal"].fillna("无").replace("", "无")

    if "tdnet_score" not in df.columns:
        df["tdnet_score"] = 0.0
    else:
        df["tdnet_score"] = pd.to_numeric(df["tdnet_score"], errors="coerce").fillna(0.0)

    if "tdnet_title" not in df.columns:
        df["tdnet_title"] = ""
    else:
        df["tdnet_title"] = df["tdnet_title"].fillna("")

    if "news_title" not in df.columns:
        df["news_title"] = ""
    else:
        df["news_title"] = df["news_title"].fillna("")

    if "news_source" not in df.columns:
        df["news_source"] = ""
    else:
        df["news_source"] = df["news_source"].fillna("")

    if "news_published_at" not in df.columns:
        df["news_published_at"] = ""
    else:
        df["news_published_at"] = df["news_published_at"].fillna("")

    return attach_repeat_signal_markers(df)


def load_daily_price_index(data_dir: Path | None = None) -> dict[str, pd.DataFrame]:
    source_dir = data_dir or JQ_DAILY_DIR
    rows = []

    for path in sorted(source_dir.glob("*.csv")):
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            continue

        required = {"code", "date", "close", "high", "low"}
        if not required.issubset(set(df.columns)):
            continue

        current = df[["code", "date", "close", "high", "low"]].copy()
        current["code"] = current["code"].astype(str).str.replace(".0", "", regex=False).str.strip()
        current["date"] = current["date"].astype(str).str.strip()
        for col in ["close", "high", "low"]:
            current[col] = pd.to_numeric(current[col], errors="coerce")
        current = current.dropna(subset=["close", "high", "low"])
        rows.append(current)

    if not rows:
        return {}

    full = pd.concat(rows, ignore_index=True)
    full["date"] = pd.to_datetime(full["date"], errors="coerce")
    full = full.dropna(subset=["date"]).copy()
    full = full.sort_values(["code", "date"]).reset_index(drop=True)

    index: dict[str, pd.DataFrame] = {}
    for code, frame in full.groupby("code", sort=False):
        index[str(code)] = frame.reset_index(drop=True)
    return index


def _future_return(history: pd.DataFrame, start_idx: int, offset: int) -> float | None:
    target_idx = start_idx + offset
    if target_idx >= len(history):
        return None
    entry_close = float(history.iloc[start_idx]["close"])
    target_close = float(history.iloc[target_idx]["close"])
    if entry_close <= 0:
        return None
    return round((target_close - entry_close) / entry_close * 100.0, 4)


def _future_range(history: pd.DataFrame, start_idx: int, window: int) -> tuple[float | None, float | None]:
    end_idx = start_idx + window
    if end_idx >= len(history):
        return None, None

    entry_close = float(history.iloc[start_idx]["close"])
    future = history.iloc[start_idx + 1 : end_idx + 1]
    if future.empty or entry_close <= 0:
        return None, None

    max_high = float(future["high"].max())
    min_low = float(future["low"].min())
    max_up = round((max_high - entry_close) / entry_close * 100.0, 4)
    max_down = round((min_low - entry_close) / entry_close * 100.0, 4)
    return max_up, max_down


def enrich_signals_with_results(
    signals_df: pd.DataFrame,
    *,
    price_index: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    if signals_df.empty:
        output = signals_df.copy()
        for col in RESULT_COLUMNS:
            output[col] = pd.NA
        return output

    price_index = price_index or load_daily_price_index()
    records = []

    for _, row in signals_df.iterrows():
        current = dict(row)
        symbol = str(current.get("symbol", "")).strip()
        run_date = pd.to_datetime(current.get("run_date"), errors="coerce")
        history = price_index.get(symbol)

        for col in RESULT_COLUMNS:
            current[col] = pd.NA

        if history is None or pd.isna(run_date):
            records.append(current)
            continue

        matches = history.index[history["date"] == run_date].tolist()
        if not matches:
            records.append(current)
            continue

        start_idx = int(matches[0])
        current["ret_1d"] = _future_return(history, start_idx, 1)
        current["ret_3d"] = _future_return(history, start_idx, 3)
        current["ret_5d"] = _future_return(history, start_idx, 5)
        max_up, max_down = _future_range(history, start_idx, 5)
        current["max_up_5d"] = max_up
        current["max_down_5d"] = max_down
        records.append(current)

    return pd.DataFrame(records)


def save_signals_with_results(df: pd.DataFrame, path: Path | None = None) -> Path:
    target = path or RESULTS_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, index=False, encoding="utf-8-sig")
    return target
