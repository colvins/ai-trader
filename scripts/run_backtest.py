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


def _metric_block(df: pd.DataFrame) -> dict:
    out = {"count": int(len(df))}

    for horizon in ["ret_1d", "ret_3d", "ret_5d"]:
        series = pd.to_numeric(df.get(horizon), errors="coerce")
        valid = series.dropna()
        suffix = horizon.replace("ret_", "")
        out[f"{horizon}_mean"] = round(float(valid.mean()), 4) if not valid.empty else None
        out[f"winrate_{suffix}"] = round(float((valid > 0).mean()), 4) if not valid.empty else None

    return out


def _group_summary(df: pd.DataFrame, column: str) -> dict:
    summary = {}
    if column not in df.columns:
        return summary

    for key, group in df.groupby(column, dropna=False):
        normalized_key = "(empty)" if pd.isna(key) else str(key)
        summary[normalized_key] = _metric_block(group)
    return summary


def _print_group_summary(title: str, grouped: dict) -> None:
    print(f"\n=== 按 {title} ===")
    for key, value in grouped.items():
        print(f"{key}: {value}")


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

    path = save_backtest_summary(summary)
    print(f"\n已保存汇总文件: {path}")


if __name__ == "__main__":
    main()
