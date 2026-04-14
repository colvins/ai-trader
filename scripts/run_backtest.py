#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.backtest_store import RESULTS_FILE


SUMMARY_FILE = Path("data/backtest/summary.json")
EXECUTION_STATUSES = ("BUY_READY", "WATCH", "SKIP")
MODE_ORDER = ("trend", "dip", "breakout")
MODE_LABELS = {
    "trend": "趋势跟随",
    "dip": "低吸反弹",
    "breakout": "短线打板",
}


def _metric_block(df: pd.DataFrame) -> dict:
    out = {"count": int(len(df))}

    for horizon in ["ret_1d", "ret_3d", "ret_5d"]:
        raw_series = df[horizon] if horizon in df.columns else pd.Series(dtype="float64")
        series = pd.to_numeric(raw_series, errors="coerce")
        valid = series.dropna()
        suffix = horizon.replace("ret_", "")
        out[f"{horizon}_mean"] = round(float(valid.mean()), 4) if not valid.empty else None
        out[f"winrate_{suffix}"] = round(float((valid > 0).mean()), 4) if not valid.empty else None

    return out


def _empty_metric_block() -> dict:
    return _metric_block(pd.DataFrame())


def _group_summary(df: pd.DataFrame, column: str) -> dict:
    summary = {}
    if column not in df.columns:
        return summary

    for key, group in df.groupby(column, dropna=False):
        normalized_key = "(empty)" if pd.isna(key) else str(key)
        summary[normalized_key] = _metric_block(group)
    return summary


def _filter_recent_rows(df: pd.DataFrame, window_days: int) -> pd.DataFrame:
    current = df.copy()
    current["ret_1d"] = pd.to_numeric(current.get("ret_1d"), errors="coerce")
    current = current[current["ret_1d"].notna()].copy()
    if current.empty:
        return current

    current["run_date_dt"] = pd.to_datetime(current.get("run_date"), errors="coerce")
    current = current[current["run_date_dt"].notna()].copy()
    if current.empty:
        return current

    anchor_date = current["run_date_dt"].max().normalize()
    cutoff_date = anchor_date - pd.Timedelta(days=max(window_days - 1, 0))
    return current[current["run_date_dt"] >= cutoff_date].copy()


def _resolve_execution_column(df: pd.DataFrame) -> tuple[str, pd.Series]:
    for column in ("execution_result", "execution_status"):
        if column not in df.columns:
            continue
        series = (
            df[column]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
        )
        series = series.where(series.isin(EXECUTION_STATUSES), "")
        return column, series
    return "", pd.Series("", index=df.index, dtype="object")


def _build_execution_backtest(df: pd.DataFrame, *, window_days: int = 30) -> dict:
    recent = _filter_recent_rows(df, window_days)
    execution_field, execution_series = _resolve_execution_column(recent)
    result = {
        "window_days": int(window_days),
        "execution_field": execution_field,
        "sample_count": int(len(recent)),
        "by_execution_result": {
            status: _empty_metric_block() for status in EXECUTION_STATUSES
        },
        "by_mode_execution": [],
    }

    if recent.empty or not execution_field:
        return result

    current = recent.copy()
    current["execution_result_norm"] = execution_series
    current = current[current["execution_result_norm"].isin(EXECUTION_STATUSES)].copy()
    result["sample_count"] = int(len(current))
    if current.empty:
        return result

    for status in EXECUTION_STATUSES:
        group = current[current["execution_result_norm"] == status]
        result["by_execution_result"][status] = _metric_block(group)

    if "selected_mode" not in current.columns:
        return result

    current["selected_mode_norm"] = (
        current["selected_mode"].fillna("").astype(str).str.strip().str.lower()
    )

    rows = []
    for mode in MODE_ORDER:
        for status in EXECUTION_STATUSES:
            group = current[
                (current["selected_mode_norm"] == mode)
                & (current["execution_result_norm"] == status)
            ]
            if group.empty:
                continue
            stats = _metric_block(group)
            rows.append(
                {
                    "selected_mode": mode,
                    "selected_mode_label": MODE_LABELS.get(mode, mode),
                    "execution_result": status,
                    "count": stats["count"],
                    "ret_1d_mean": stats["ret_1d_mean"],
                    "winrate_1d": stats["winrate_1d"],
                }
            )

    result["by_mode_execution"] = rows
    return result


def _print_group_summary(title: str, grouped: dict) -> None:
    print(f"\n=== 按 {title} ===")
    for key, value in grouped.items():
        print(f"{key}: {value}")


def _fmt_return(value) -> str:
    if value is None:
        return "暂无"
    try:
        number = float(value)
    except Exception:
        return "暂无"
    return f"{number:+.2f}%"


def _fmt_winrate(value) -> str:
    if value is None:
        return "暂无"
    try:
        number = float(value) * 100.0
    except Exception:
        return "暂无"
    return f"{number:.1f}%"


def _print_execution_summary(execution_summary: dict) -> None:
    window_days = int(execution_summary.get("window_days", 30) or 30)
    print(f"\n🧠 执行层统计（近{window_days}天）")
    by_execution = execution_summary.get("by_execution_result", {}) or {}
    for status, label in (
        ("BUY_READY", "BUY_READY（可执行）"),
        ("WATCH", "WATCH（观察）"),
        ("SKIP", "SKIP（放弃）"),
    ):
        stats = by_execution.get(status, {}) or {}
        print(label)
        print(f"样本：{stats.get('count', 0)}")
        print(f"次日均收益：{_fmt_return(stats.get('ret_1d_mean'))}")
        print(f"次日胜率：{_fmt_winrate(stats.get('winrate_1d'))}")
        print(f"3日收益：{_fmt_return(stats.get('ret_3d_mean'))}")
        print(f"5日收益：{_fmt_return(stats.get('ret_5d_mean'))}")
        print()

    print(f"📊 策略 × 执行（近{window_days}天）")
    rows = execution_summary.get("by_mode_execution", []) or []
    if not rows:
        print("暂无 execution_result 数据")
        return
    for row in rows:
        print(f"{row.get('selected_mode_label', row.get('selected_mode', ''))} + {row.get('execution_result', '')}")
        print(
            f"样本 {row.get('count', 0)}｜次日 {_fmt_return(row.get('ret_1d_mean'))}｜胜率 {_fmt_winrate(row.get('winrate_1d'))}"
        )


def build_backtest_summary() -> dict | None:
    if not RESULTS_FILE.exists():
        return None

    df = pd.read_csv(RESULTS_FILE, encoding="utf-8-sig")
    if df.empty:
        return None

    overall = _metric_block(df)
    return {
        "overall": overall,
        "by_market_state": _group_summary(df, "market_state"),
        "by_strategy_source": _group_summary(df, "strategy_source"),
        "by_selected_mode": _group_summary(df, "selected_mode"),
        "by_level": _group_summary(df, "level"),
        "by_action": _group_summary(df, "action"),
        "by_option_bias": _group_summary(df, "option_bias"),
        "by_tdnet_signal": _group_summary(df, "tdnet_signal"),
        "execution_backtest": _build_execution_backtest(df, window_days=30),
    }


def save_backtest_summary(summary: dict) -> Path:
    SUMMARY_FILE.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_FILE.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return SUMMARY_FILE


def main():
    summary = build_backtest_summary()
    if summary is None:
        print("未找到 signals_with_results.csv，请先运行 scripts/update_backtest_results.py")
        return

    print("=== Backtest Summary ===")
    overall = summary["overall"]
    print(f"总信号数: {overall['count']}")
    print(f"ret_1d 平均: {overall['ret_1d_mean']}")
    print(f"ret_3d 平均: {overall['ret_3d_mean']}")
    print(f"ret_5d 平均: {overall['ret_5d_mean']}")
    print(f"ret_1d 胜率: {overall['winrate_1d']}")
    print(f"ret_3d 胜率: {overall['winrate_3d']}")
    print(f"ret_5d 胜率: {overall['winrate_5d']}")

    _print_group_summary("market_state", summary["by_market_state"])
    _print_group_summary("strategy_source", summary["by_strategy_source"])
    _print_group_summary("selected_mode", summary["by_selected_mode"])
    _print_group_summary("level", summary["by_level"])
    _print_group_summary("action", summary["by_action"])
    _print_group_summary("option_bias", summary["by_option_bias"])
    _print_group_summary("tdnet_signal", summary["by_tdnet_signal"])
    _print_execution_summary(summary.get("execution_backtest", {}))

    path = save_backtest_summary(summary)
    print(f"\n已保存汇总文件: {path}")


if __name__ == "__main__":
    main()
