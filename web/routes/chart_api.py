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
    "close",
    "day_change_pct",
    "intraday_pct",
    "amplitude_pct",
    "amount_ratio_5",
    "momentum_3_pct",
    "momentum_5_pct",
    "dist_to_high_20_pct",
    "close_position",
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


def load_signals_df() -> pd.DataFrame:
    source = SIGNALS_WITH_RESULTS_FILE if SIGNALS_WITH_RESULTS_FILE.exists() else SIGNALS_FILE
    if not source.exists():
        return pd.DataFrame()

    df = pd.read_csv(source, encoding="utf-8-sig")
    if df.empty:
        return df

    if "generated_at" in df.columns:
        df["generated_at"] = pd.to_datetime(df["generated_at"], errors="coerce")
    if "run_date" in df.columns:
        df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce")
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.replace(".0", "", regex=False).str.strip()
    return df


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

    sort_cols = [col for col in ["generated_at", "run_date", "rank"] if col in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[False] * len(sort_cols))

    latest = df.drop_duplicates(subset=["symbol"], keep="first")
    latest = latest[["symbol", "run_date", "level", "action", "score"]].copy()
    latest = latest.rename(
        columns={
            "run_date": "latest_run_date",
            "level": "latest_level",
            "action": "latest_action",
            "score": "latest_score",
        }
    )
    latest["latest_run_date"] = latest["latest_run_date"].dt.strftime("%Y-%m-%d")
    latest["symbol"] = latest["symbol"].astype(str).str.strip()
    latest = latest.sort_values(["latest_run_date", "latest_score"], ascending=[False, False])
    return _records_to_native(
        latest,
        ["symbol", "latest_run_date", "latest_level", "latest_action", "latest_score"],
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
