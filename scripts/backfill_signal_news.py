from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from analysis.news_service import get_news_for_stock
from storage.backtest_store import RESULTS_FILE, SIGNALS_FILE


NEWS_COLUMNS = ["news_title", "news_source", "news_published_at"]


def _normalize_text(value) -> str:
    return str(value or "").strip()


def _ensure_news_columns(df: pd.DataFrame) -> pd.DataFrame:
    current = df.copy()
    for col in NEWS_COLUMNS:
        if col not in current.columns:
            current[col] = ""
        else:
            current[col] = current[col].fillna("").astype(str)
    return current


def _pick_primary_news_fields(items: list[dict]) -> tuple[str, str, str]:
    for item in items or []:
        title = _normalize_text(item.get("title"))
        if not title:
            continue
        source = _normalize_text(item.get("source"))
        published_at = _normalize_text(item.get("published_at"))
        return title, source, published_at
    return "", "", ""


def _tdnet_fallback_fields(row: pd.Series) -> tuple[str, str, str]:
    tdnet_title = _normalize_text(row.get("tdnet_title"))
    if not tdnet_title:
        return "", "", ""
    first_title = tdnet_title.split("|", 1)[0].strip()
    if not first_title:
        return "", "", ""
    return first_title, "TDnet公告", ""


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()


def _save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _target_mask(df: pd.DataFrame, run_date: str) -> pd.Series:
    return (
        df["run_date"].astype(str).str.strip().eq(run_date)
        & df["strategy_source"].astype(str).str.strip().str.lower().eq("scan")
        & df["news_title"].astype(str).str.strip().eq("")
    )


def backfill_signal_news(*, run_date: str | None = None) -> dict:
    target_date = _normalize_text(run_date) or date.today().isoformat()
    signals_df = _load_csv(SIGNALS_FILE)
    if signals_df.empty:
        return {
            "ok": False,
            "message": "未找到 signals.csv",
            "run_date": target_date,
            "updated": 0,
            "skipped": 0,
            "results_synced": 0,
        }

    signals_df = _ensure_news_columns(signals_df)
    if "run_date" not in signals_df.columns or "strategy_source" not in signals_df.columns:
        return {
            "ok": False,
            "message": "signals.csv 缺少必要字段 run_date / strategy_source",
            "run_date": target_date,
            "updated": 0,
            "skipped": 0,
            "results_synced": 0,
        }

    mask = _target_mask(signals_df, target_date)
    target_indexes = signals_df.index[mask].tolist()
    if not target_indexes:
        return {
            "ok": True,
            "message": "没有需要回填的当日 scan 信号",
            "run_date": target_date,
            "updated": 0,
            "skipped": 0,
            "results_synced": 0,
        }

    updated = 0
    skipped = 0

    for idx in target_indexes:
        row = signals_df.loc[idx]
        symbol = _normalize_text(row.get("symbol"))
        if not symbol:
            skipped += 1
            continue

        title, source, published_at = "", "", ""
        try:
            news_items = get_news_for_stock({"symbol": symbol}, max_items=1)
        except Exception:
            news_items = []

        title, source, published_at = _pick_primary_news_fields(news_items)
        if not title:
            title, source, published_at = _tdnet_fallback_fields(row)

        if not title:
            skipped += 1
            continue

        signals_df.at[idx, "news_title"] = title
        signals_df.at[idx, "news_source"] = source
        signals_df.at[idx, "news_published_at"] = published_at
        updated += 1

    _save_csv(signals_df, SIGNALS_FILE)

    results_synced = 0
    results_df = _load_csv(RESULTS_FILE)
    if not results_df.empty and "signal_id" in results_df.columns:
        results_df = _ensure_news_columns(results_df)
        signal_news = signals_df.set_index("signal_id")[NEWS_COLUMNS]
        common_ids = results_df["signal_id"].astype(str).isin(signal_news.index.astype(str))
        if common_ids.any():
            for idx in results_df.index[common_ids]:
                signal_id = str(results_df.at[idx, "signal_id"])
                if signal_id not in signal_news.index:
                    continue
                for col in NEWS_COLUMNS:
                    results_df.at[idx, col] = signal_news.at[signal_id, col]
                results_synced += 1
            _save_csv(results_df, RESULTS_FILE)

    return {
        "ok": True,
        "message": "回填完成",
        "run_date": target_date,
        "updated": updated,
        "skipped": skipped,
        "results_synced": results_synced,
    }


def main() -> None:
    result = backfill_signal_news()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
