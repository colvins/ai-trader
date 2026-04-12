#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.backtest_store import (
    RESULTS_FILE,
    enrich_signals_with_results,
    load_daily_price_index,
    load_signals,
    save_signals_with_results,
)


def update_backtest_results() -> dict:
    signals_df = load_signals()
    if signals_df.empty:
        return {"ok": False, "message": "未找到可回填的 signals.csv", "path": str(RESULTS_FILE), "count": 0, "ret_5d_ready": 0}

    price_index = load_daily_price_index()
    enriched = enrich_signals_with_results(signals_df, price_index=price_index)
    path = save_signals_with_results(enriched)
    available_5d = enriched["ret_5d"].notna().sum() if "ret_5d" in enriched.columns else 0
    return {
        "ok": True,
        "message": "已更新回测结果",
        "path": str(path),
        "count": int(len(enriched)),
        "ret_5d_ready": int(available_5d),
    }


def main():
    result = update_backtest_results()
    if not result["ok"]:
        print(result["message"])
        return

    print(f"已生成回测结果文件: {result['path']}")
    print(f"信号数: {result['count']}")
    print(f"已补全 ret_5d 的记录数: {result['ret_5d_ready']}")


if __name__ == "__main__":
    main()
