"""Chart and signal APIs backed by local CSV files."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException


ROUTER = APIRouter()
BASE_DIR = Path(__file__).resolve().parents[2]
JQ_DAILY_DIR = BASE_DIR / "data" / "jq_daily"
BACKTEST_DIR = BASE_DIR / "data" / "backtest"
SIGNALS_WITH_RESULTS_FILE = BACKTEST_DIR / "signals_with_results.csv"
SIGNALS_FILE = BACKTEST_DIR / "signals.csv"

SIGNAL_FIELDS = [
    "signal_id",
    "run_date",
    "selected_mode",
    "strategy_source",
    "market_state",
    "rank",
    "score",
    "level",
    "action",
    "option_bias",
    "option_horizon",
    "option_reason",
    "option_risk",
    "tdnet_signal",
    "tdnet_title",
    "news_title",
    "news_source",
    "news_published_at",
    "close",
    "day_change_pct",
    "intraday_pct",
    "amplitude_pct",
    "amount_ratio_5",
    "momentum_3_pct",
    "momentum_5_pct",
    "dist_to_high_20_pct",
    "close_position",
    "is_repeat_signal",
    "consecutive_days",
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "max_up_5d",
    "max_down_5d",
]


def _to_native(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _records_to_native(df: pd.DataFrame, fields: list[str] | None = None) -> list[dict]:
    if df.empty:
        return []

    current = df.copy()
    if fields is not None:
        for field in fields:
            if field not in current.columns:
                current[field] = pd.NA
        current = current[fields]

    records = current.to_dict(orient="records")
    output = []
    for record in records:
        output.append({str(key): _to_native(value) for key, value in record.items()})
    return output


def _dedupe_display_signals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    current = df.copy()
    for field in ["run_date", "selected_mode", "symbol", "level", "action", "strategy_source", "generated_at"]:
        if field not in current.columns:
            current[field] = pd.NA

    strategy_priority = {"manual": 0, "auto": 1}
    current["_strategy_priority"] = (
        current["strategy_source"].astype(str).str.strip().str.lower().map(strategy_priority).fillna(2)
    )
    if "generated_at" in current.columns:
        current["generated_at"] = pd.to_datetime(current["generated_at"], errors="coerce")

    current = current.sort_values(
        ["run_date", "selected_mode", "symbol", "level", "action", "_strategy_priority", "generated_at"],
        ascending=[False, True, True, True, True, True, False],
        na_position="last",
    )
    current = current.drop_duplicates(
        subset=["run_date", "selected_mode", "symbol", "level", "action"],
        keep="first",
    )
    current = current.drop(columns=["_strategy_priority"], errors="ignore")
    return current


def _read_signals_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    return df


def load_signals_df() -> pd.DataFrame:
    results_df = _read_signals_file(SIGNALS_WITH_RESULTS_FILE)
    signals_df = _read_signals_file(SIGNALS_FILE)

    if results_df.empty and signals_df.empty:
        return pd.DataFrame()

    combined_frames = []
    if not results_df.empty:
        combined_frames.append(results_df)
    if not signals_df.empty:
        combined_frames.append(signals_df)

    df = pd.concat(combined_frames, ignore_index=True) if combined_frames else pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    if "signal_id" not in df.columns:
        df["signal_id"] = pd.NA

    # Keep the backtest-enriched record when the same signal_id exists in both files.
    df["_source_priority"] = 1
    if not results_df.empty:
        result_ids = set(results_df.get("signal_id", pd.Series(dtype=str)).astype(str).tolist())
        df.loc[df["signal_id"].astype(str).isin(result_ids), "_source_priority"] = 0

    if "generated_at" in df.columns:
        df["generated_at"] = pd.to_datetime(df["generated_at"], errors="coerce")
    if "run_date" in df.columns:
        df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce")
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.replace(".0", "", regex=False).str.strip()
    df = df.sort_values(["_source_priority", "generated_at"], ascending=[True, False], na_position="last")
    df = df.drop_duplicates(subset=["signal_id"], keep="first")
    df = df.drop(columns=["_source_priority"], errors="ignore")
    return _dedupe_display_signals(df)


def load_symbol_history(symbol: str) -> list[dict]:
    symbol = str(symbol).strip()
    rows = []

    for path in sorted(JQ_DAILY_DIR.glob("*.csv")):
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            continue

        required = {"code", "date", "open", "high", "low", "close", "volume"}
        if not required.issubset(set(df.columns)):
            continue

        current = df[df["code"].astype(str).str.replace(".0", "", regex=False).str.strip() == symbol].copy()
        if current.empty:
            continue

        current["date"] = current["date"].astype(str).str.strip()
        for col in ["open", "high", "low", "close", "volume"]:
            current[col] = pd.to_numeric(current[col], errors="coerce")
        current = current.dropna(subset=["open", "high", "low", "close", "volume"])
        rows.append(current[["date", "open", "high", "low", "close", "volume"]])

    if not rows:
        return []

    full = pd.concat(rows, ignore_index=True)
    full["date"] = pd.to_datetime(full["date"], errors="coerce")
    full = full.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    full["date"] = full["date"].dt.strftime("%Y-%m-%d")
    return _records_to_native(full, ["date", "open", "high", "low", "close", "volume"])


@ROUTER.get("/api/signals/symbols")
def list_signal_symbols():
    df = load_signals_df()
    if df.empty:
        return []

    sort_columns = []
    ascending = []
    if "run_date" in df.columns:
        sort_columns.append("run_date")
        ascending.append(False)
    if "score" in df.columns:
        sort_columns.append("score")
        ascending.append(False)
    if "generated_at" in df.columns:
        sort_columns.append("generated_at")
        ascending.append(False)
    if sort_columns:
        df = df.sort_values(sort_columns, ascending=ascending)

    for field in ["signal_id", "symbol", "run_date", "selected_mode", "level", "action", "score"]:
        if field not in df.columns:
            df[field] = pd.NA

    latest = df[["signal_id", "symbol", "run_date", "selected_mode", "level", "action", "score"]].copy()
    latest["run_date"] = latest["run_date"].dt.strftime("%Y-%m-%d")
    latest["symbol"] = latest["symbol"].astype(str).str.strip()
    return _records_to_native(
        latest,
        ["signal_id", "symbol", "run_date", "selected_mode", "level", "action", "score"],
    )


@ROUTER.get("/api/chart/{symbol}")
def get_chart_data(symbol: str):
    data = load_symbol_history(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="No chart data for symbol")
    return data


@ROUTER.get("/api/signals/{symbol}")
def get_symbol_signals(symbol: str):
    df = load_signals_df()
    if df.empty:
        return []

    current = df[df["symbol"].astype(str).str.strip() == str(symbol).strip()].copy()
    if current.empty:
        return []

    sort_columns = []
    ascending = []
    if "generated_at" in current.columns:
        sort_columns.append("generated_at")
        ascending.append(False)
    if "run_date" in current.columns:
        sort_columns.append("run_date")
        ascending.append(False)
    if "rank" in current.columns:
        sort_columns.append("rank")
        ascending.append(True)
    if sort_columns:
        current = current.sort_values(sort_columns, ascending=ascending)
    if "run_date" in current.columns:
        current["run_date"] = current["run_date"].dt.strftime("%Y-%m-%d")

    fields = [field for field in SIGNAL_FIELDS if field in current.columns]
    return _records_to_native(current, fields)
