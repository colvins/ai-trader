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
EXECUTION_FEEDBACK_COLUMNS = [
    "execution_result",
    "execution_checked_at",
    "news_risk_level",
]


def _safe_str_series(df: pd.DataFrame, column: str, *, lower: bool = False, strip_decimal: bool = False) -> pd.Series:
    if column not in df.columns:
        return pd.Series("", index=df.index, dtype="object")
    series = df[column].fillna("").astype(str).str.strip()
    if strip_decimal:
        series = series.str.replace(".0", "", regex=False)
    if lower:
        series = series.str.lower()
    return series


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

    if "execution_result" not in df.columns:
        df["execution_result"] = ""
    else:
        df["execution_result"] = df["execution_result"].fillna("")

    if "execution_checked_at" not in df.columns:
        df["execution_checked_at"] = ""
    else:
        df["execution_checked_at"] = df["execution_checked_at"].fillna("")

    if "news_risk_level" not in df.columns:
        df["news_risk_level"] = ""
    else:
        df["news_risk_level"] = df["news_risk_level"].fillna("")

    return attach_repeat_signal_markers(df)


def _ensure_execution_feedback_columns(df: pd.DataFrame) -> pd.DataFrame:
    current = df.copy()
    for column in EXECUTION_FEEDBACK_COLUMNS:
        if column not in current.columns:
            current[column] = ""
        else:
            current[column] = current[column].fillna("")
    return current


def _normalize_feedback_updates(updates: list[dict]) -> list[dict]:
    normalized = []
    for update in updates or []:
        current = {
            "signal_id": str(update.get("signal_id", "") or "").strip(),
            "run_date": str(update.get("run_date", "") or "").strip(),
            "selected_mode": str(update.get("selected_mode", "") or "").strip().lower(),
            "strategy_source": str(update.get("strategy_source", "") or "").strip().lower(),
            "symbol": str(update.get("symbol", "") or "").strip(),
            "execution_result": str(update.get("execution_result", "") or "").strip().upper(),
            "execution_checked_at": str(update.get("execution_checked_at", "") or "").strip(),
            "news_risk_level": str(update.get("news_risk_level", "") or "").strip().upper(),
        }
        if not current["execution_result"]:
            continue
        if not current["signal_id"] and not (
            current["run_date"] and current["selected_mode"] and current["symbol"]
        ):
            continue
        normalized.append(current)
    return normalized


def _apply_execution_feedback_updates(df: pd.DataFrame, updates: list[dict]) -> tuple[pd.DataFrame, int]:
    current = _ensure_execution_feedback_columns(df)
    if "signal_id" not in current.columns:
        current["signal_id"] = ""

    if current.empty or not updates:
        return current, 0

    updated_rows = 0
    current["_signal_id_key"] = _safe_str_series(current, "signal_id")
    current["_run_date_key"] = _safe_str_series(current, "run_date")
    current["_mode_key"] = _safe_str_series(current, "selected_mode", lower=True)
    current["_strategy_key"] = _safe_str_series(current, "strategy_source", lower=True)
    current["_symbol_key"] = _safe_str_series(current, "symbol", strip_decimal=True)

    for update in updates:
        mask = pd.Series(False, index=current.index)
        if update["signal_id"]:
            mask = current["_signal_id_key"] == update["signal_id"]

        if not bool(mask.any()):
            mask = (
                (current["_run_date_key"] == update["run_date"])
                & (current["_mode_key"] == update["selected_mode"])
                & (current["_symbol_key"] == update["symbol"])
            )
            if update["strategy_source"] and "_strategy_key" in current.columns:
                strategy_mask = current["_strategy_key"] == update["strategy_source"]
                if bool((mask & strategy_mask).any()):
                    mask = mask & strategy_mask

        if not bool(mask.any()):
            append_row = {column: "" for column in current.columns if not column.startswith("_")}
            append_row["signal_id"] = update["signal_id"]
            if "run_date" in append_row:
                append_row["run_date"] = update["run_date"]
            if "selected_mode" in append_row:
                append_row["selected_mode"] = update["selected_mode"]
            if "strategy_source" in append_row:
                append_row["strategy_source"] = update["strategy_source"]
            if "symbol" in append_row:
                append_row["symbol"] = update["symbol"]
            append_row["execution_result"] = update["execution_result"]
            append_row["execution_checked_at"] = update["execution_checked_at"]
            append_row["news_risk_level"] = update["news_risk_level"]
            current = pd.concat([current, pd.DataFrame([append_row])], ignore_index=True)
            updated_rows += 1
            current["_signal_id_key"] = _safe_str_series(current, "signal_id")
            current["_run_date_key"] = _safe_str_series(current, "run_date")
            current["_mode_key"] = _safe_str_series(current, "selected_mode", lower=True)
            current["_strategy_key"] = _safe_str_series(current, "strategy_source", lower=True)
            current["_symbol_key"] = _safe_str_series(current, "symbol", strip_decimal=True)
            continue

        current.loc[mask, "execution_result"] = update["execution_result"]
        current.loc[mask, "execution_checked_at"] = update["execution_checked_at"]
        current.loc[mask, "news_risk_level"] = update["news_risk_level"]
        updated_rows += int(mask.sum())

    current = current.drop(
        columns=["_signal_id_key", "_run_date_key", "_mode_key", "_strategy_key", "_symbol_key"],
        errors="ignore",
    )
    return current, updated_rows


def _dedupe_execution_feedback_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    current = _ensure_execution_feedback_columns(df)
    if current.empty:
        return current, 0

    current = current.copy()
    current["_row_order"] = range(len(current))
    current["_signal_id_key"] = _safe_str_series(current, "signal_id")
    current["_run_date_key"] = _safe_str_series(current, "run_date")
    current["_mode_key"] = _safe_str_series(current, "selected_mode", lower=True)
    current["_strategy_key"] = _safe_str_series(current, "strategy_source", lower=True)
    current["_symbol_key"] = _safe_str_series(current, "symbol", strip_decimal=True)
    current["_combo_key"] = (
        current["_run_date_key"]
        + "|"
        + current["_mode_key"]
        + "|"
        + current["_strategy_key"]
        + "|"
        + current["_symbol_key"]
    )
    current["_dedupe_key"] = current["_signal_id_key"]
    empty_signal_mask = current["_dedupe_key"] == ""
    current.loc[empty_signal_mask, "_dedupe_key"] = current.loc[empty_signal_mask, "_combo_key"]
    invalid_key_mask = (
        (current["_run_date_key"] == "")
        | (current["_mode_key"] == "")
        | (current["_symbol_key"] == "")
    )
    current.loc[invalid_key_mask & empty_signal_mask, "_dedupe_key"] = ""

    current["_has_execution"] = current["execution_result"].fillna("").astype(str).str.strip().ne("").astype(int)
    current["_has_checked_at"] = current["execution_checked_at"].fillna("").astype(str).str.strip().ne("").astype(int)
    current["_has_news_risk"] = current["news_risk_level"].fillna("").astype(str).str.strip().ne("").astype(int)

    dedupe_target = current[current["_dedupe_key"] != ""].copy()
    keep_free = current[current["_dedupe_key"] == ""].copy()
    if dedupe_target.empty:
        return current.drop(
            columns=[
                "_row_order",
                "_signal_id_key",
                "_run_date_key",
                "_mode_key",
                "_strategy_key",
                "_symbol_key",
                "_combo_key",
                "_dedupe_key",
                "_has_execution",
                "_has_checked_at",
                "_has_news_risk",
            ],
            errors="ignore",
        ), 0

    dedupe_target = dedupe_target.sort_values(
        [
            "_dedupe_key",
            "_has_execution",
            "_has_checked_at",
            "_has_news_risk",
            "_row_order",
        ],
        ascending=[True, False, False, False, False],
    )
    deduped = dedupe_target.drop_duplicates(subset=["_dedupe_key"], keep="first")
    removed = int(len(dedupe_target) - len(deduped))
    combined = pd.concat([deduped, keep_free], ignore_index=True)
    combined = combined.sort_values("_row_order").drop(
        columns=[
            "_row_order",
            "_signal_id_key",
            "_run_date_key",
            "_mode_key",
            "_strategy_key",
            "_symbol_key",
            "_combo_key",
            "_dedupe_key",
            "_has_execution",
            "_has_checked_at",
            "_has_news_risk",
        ],
        errors="ignore",
    )
    return combined.reset_index(drop=True), removed


def save_execution_feedback(
    updates: list[dict],
    *,
    results_path: Path | None = None,
    signals_path: Path | None = None,
) -> dict:
    normalized = _normalize_feedback_updates(updates)
    result = {
        "ok": True,
        "results_updated": 0,
        "signals_updated": 0,
        "results_deduped": 0,
        "signals_deduped": 0,
        "results_path": str(results_path or RESULTS_FILE),
        "signals_path": str(signals_path or SIGNALS_FILE),
    }
    if not normalized:
        return result

    target_results = results_path or RESULTS_FILE
    if target_results.exists():
        results_df = pd.read_csv(target_results, encoding="utf-8-sig")
        results_df, updated = _apply_execution_feedback_updates(results_df, normalized)
        results_df, deduped = _dedupe_execution_feedback_rows(results_df)
        results_df.to_csv(target_results, index=False, encoding="utf-8-sig")
        result["results_updated"] = updated
        result["results_deduped"] = deduped

    target_signals = signals_path or SIGNALS_FILE
    if target_signals.exists():
        signals_df = pd.read_csv(target_signals, encoding="utf-8-sig")
        signals_df, updated = _apply_execution_feedback_updates(signals_df, normalized)
        signals_df, deduped = _dedupe_execution_feedback_rows(signals_df)
        signals_df.to_csv(target_signals, index=False, encoding="utf-8-sig")
        result["signals_updated"] = updated
        result["signals_deduped"] = deduped

    return result


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
