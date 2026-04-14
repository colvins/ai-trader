from __future__ import annotations

import json
import os
import sys
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# 关键：把项目根目录加入路径，解决 No module named 'app'
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from telegram import BotCommand, ReplyKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from analysis.execution_guard import (
    ExecutionSignalInput,
    apply_news_veto,
    build_execution_decision,
    evaluate_execution_guard,
    is_execution_candidate,
)
from analysis.intraday_data import fetch_opening_intraday_bars
from analysis.news_guard import evaluate_news_guard
from reporting.schemas import DataStatus, MarketState, NewsItem, PickResult, StockPick
from scripts.run_backtest import SUMMARY_FILE, build_backtest_summary, save_backtest_summary
from storage.backtest_store import load_signals, save_execution_feedback


BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

STRATEGY_LABELS = {
    "breakout": "短线打板 / 追涨",
    "trend": "趋势跟随",
    "dip": "低吸反弹",
}

ACTION_LABELS = {
    "buy": "买入",
    "watch": "观察",
    "ignore": "忽略",
}

LEVEL_LABELS = {
    "A": "A级",
    "B": "B级",
    "C": "C级",
}

MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        ["🤖 运行今日策略", "📌 今日结论"],
        ["💡 买卖建议", "🧠 AI精选分析"],
        ["📊 回测汇总"],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="请选择功能…",
)

LAST_SCAN_RESULTS = None
SCAN_MODES = ("dip", "trend", "breakout")


def log(msg: str):
    print(msg, flush=True)


def _signal_repeat_tag(raw: dict) -> str:
    consecutive_days = int((raw or {}).get("consecutive_days", 1) or 1)
    if consecutive_days <= 1:
        return "新"
    return f"连{consecutive_days}"


def _stored_pick_reason(row: dict) -> str:
    tdnet_signal = str(row.get("tdnet_signal", "") or "").strip()
    option_reason = str(row.get("option_reason", "") or "").strip()
    if tdnet_signal and tdnet_signal != "无":
        return f"公告信号：{tdnet_signal}"
    if option_reason:
        return option_reason
    return "已落盘信号"


def _build_pick_from_row(row: dict) -> StockPick:
    raw = dict(row)
    news_items = []
    news_title = str(row.get("news_title", "") or "").strip()
    if news_title:
        news_items.append(
            NewsItem(
                title=news_title,
                source=str(row.get("news_source", "") or "").strip(),
                published_at=str(row.get("news_published_at", "") or "").strip(),
            )
        )
    return StockPick(
        symbol=str(row.get("symbol", "") or "").strip(),
        close=row.get("close"),
        prev_close=row.get("prev_close"),
        score=row.get("score"),
        reason=_stored_pick_reason(row),
        mode=str(row.get("selected_mode", "") or "").strip(),
        level=str(row.get("level", "C") or "C").strip(),
        action=str(row.get("action", "ignore") or "ignore").strip(),
        option_bias=str(row.get("option_bias", "") or "").strip(),
        option_horizon=str(row.get("option_horizon", "") or "").strip(),
        option_reason=str(row.get("option_reason", "") or "").strip(),
        option_risk=str(row.get("option_risk", "") or "").strip(),
        day_change_pct=row.get("day_change_pct"),
        intraday_pct=row.get("intraday_pct"),
        amplitude_pct=row.get("amplitude_pct"),
        amount_ratio_5=row.get("amount_ratio_5"),
        momentum_3_pct=row.get("momentum_3_pct"),
        momentum_5_pct=row.get("momentum_5_pct"),
        dist_to_high_5_pct=row.get("dist_to_high_5_pct"),
        dist_to_high_20_pct=row.get("dist_to_high_20_pct"),
        close_position=row.get("close_position"),
        news_items=news_items,
        raw=raw,
    )


def _build_result_from_rows(mode: str, run_date: str, frame: pd.DataFrame) -> PickResult:
    rows = frame.sort_values(["rank", "score", "symbol"], ascending=[True, False, True], na_position="last")
    first = rows.iloc[0].to_dict() if not rows.empty else {}
    picks = [_build_pick_from_row(row) for row in rows.to_dict(orient="records")]
    market_state = MarketState(
        state=str(first.get("market_state", "") or "").strip(),
        up_ratio=float(first.get("market_up_ratio") or 0.0),
        avg_change_pct=float(first.get("market_avg_change_pct") or 0.0),
        data_date=run_date,
    )
    return PickResult(
        mode=mode,
        status=DataStatus(
            ok=True,
            title="",
            text="",
            data_date=run_date,
            raw={"source": "data/backtest/signals.csv", "strategy_source": "scan"},
        ),
        market_state=market_state,
        mode_source="scan",
        picks=picks,
        candidate_count=len(picks),
        scored_count=len(picks),
        candidate_limit=len(picks),
        limit=len(picks),
        generated_at=str(first.get("generated_at", "") or ""),
    )


def load_latest_stored_scan_results() -> tuple[str, list[PickResult]]:
    df = load_signals()
    if df.empty:
        return "", []

    current = df.copy()
    current["run_date"] = current["run_date"].astype(str).str.strip()
    current["strategy_source"] = current["strategy_source"].astype(str).str.strip().str.lower()
    current["selected_mode"] = current["selected_mode"].astype(str).str.strip().str.lower()
    current = current[current["strategy_source"] == "scan"].copy()
    if current.empty:
        return "", []

    latest_run_date = current["run_date"].max()
    current = current[current["run_date"] == latest_run_date].copy()
    if current.empty:
        return "", []

    results = []
    for mode in SCAN_MODES:
        mode_frame = current[current["selected_mode"] == mode].copy()
        results.append(_build_result_from_rows(mode, latest_run_date, mode_frame))
    return latest_run_date, results


def _group_picks(result):
    groups = {"A": [], "B": [], "C": []}
    for pick in result.picks:
        groups.setdefault(pick.level, []).append(pick)
    return groups


def _iter_scan_picks(scan_results):
    for result in scan_results or []:
        for pick in result.picks:
            yield result, pick


def build_multi_mode_run_message(scan_results) -> str:
    if not scan_results:
        return "当前无可展示的三模式扫描结果。"

    lines = ["🤖 今日策略已运行", "以下为三模式系统扫描结果"]
    for result in scan_results:
        groups = _group_picks(result)
        lines.extend(
            [
                "",
                f"【{STRATEGY_LABELS.get(result.mode, result.mode)}】",
                f"市场状态: {result.market_state.state or '-'} | 扫描结果",
                f"A/B/C 数量: {len(groups['A'])}/{len(groups['B'])}/{len(groups['C'])}",
            ]
        )

        if groups["A"]:
            lines.append("A级（买入）")
            for pick in groups["A"]:
                repeat_tag = _signal_repeat_tag(getattr(pick, "raw", {}) or {})
                lines.append(
                    f"- {pick.symbol}（{repeat_tag}） | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
                )
                lines.append(f"  新闻：{_pick_news_summary(pick)}")

        if groups["B"]:
            lines.append("B级（观察）")
            for pick in groups["B"][:3]:
                repeat_tag = _signal_repeat_tag(getattr(pick, "raw", {}) or {})
                lines.append(
                    f"- {pick.symbol}（{repeat_tag}） | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
                )
                lines.append(f"  新闻：{_pick_news_summary(pick)}")

        if not result.picks:
            lines.append("- 暂无候选")
        else:
            lines.append(f"C级（忽略）数量: {len(groups['C'])}")

    return "\n".join(lines)


def build_today_summary(scan_results) -> str:
    if not scan_results:
        return "当前暂无当日三模式信号，请先确认 signals.csv 已落盘。"

    total_a = 0
    total_b = 0
    total_c = 0
    watch_list = []
    market_state = None

    for result in scan_results:
        groups = _group_picks(result)
        total_a += len(groups["A"])
        total_b += len(groups["B"])
        total_c += len(groups["C"])
        market_state = market_state or result.market_state
        for pick in groups["A"] + groups["B"]:
            watch_list.append((result.mode, pick))

    watch_list = sorted(
        watch_list,
        key=lambda item: (
            0 if str(item[1].level or "").upper() == "A" else 1,
            -(float(item[1].score or 0.0)),
            str(item[1].symbol or ""),
        ),
    )[:5]

    lines = [
        "📌 今日结论",
        f"市场状态: {market_state.state or '-'} | 上涨占比: {round((market_state.up_ratio or 0.0) * 100.0, 1)}%",
        "信号范围: 当日已落盘三模式信号",
        f"A/B/C 数量: {total_a}/{total_b}/{total_c}",
    ]

    if total_a == 0:
        lines.append("⚠️ 今日无明确买点，建议观望")
    else:
        lines.append(f"✅ 今日共有 {total_a} 个 A级买点")

    if watch_list:
        lines.extend(["", "重点关注"])
        for mode, pick in watch_list:
            repeat_tag = _signal_repeat_tag(getattr(pick, "raw", {}) or {})
            lines.append(
                f"- {STRATEGY_LABELS.get(mode, mode)} | {pick.symbol}（{repeat_tag}） | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)}"
            )
            lines.append(f"  新闻：{_pick_news_summary(pick)}")

    lines.append(f"\nC级数量: {total_c}")
    return "\n".join(lines)


def build_run_result_message(result) -> str:
    groups = _group_picks(result)
    a_count = len(groups["A"])
    b_count = len(groups["B"])
    c_count = len(groups["C"])
    market_state = result.market_state
    lines = [
        "🤖 今日策略已运行",
        f"市场状态: {market_state.state or '-'} | 上涨占比: {round(market_state.up_ratio * 100.0, 1)}% | 平均涨幅: {market_state.avg_change_pct}%",
        f"策略类型: {STRATEGY_LABELS.get(result.mode, result.mode)}",
        f"策略来源: {'自动选择' if result.mode_source == 'auto' else '手动指定'}",
        f"A/B/C 数量: {a_count}/{b_count}/{c_count}",
    ]

    if a_count == 0:
        lines.append("⚠️ 今日无明确买点，建议观望")
    else:
        lines.append("✅ 今日存在明确买点")

    if groups["A"]:
        lines.extend(["", "A级（买入）"])
        for pick in groups["A"]:
            lines.append(
                f"- {pick.symbol} | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
            )
            lines.append(
                f"  期权方向: {pick.option_bias or '暂无'} | 参考周期: {pick.option_horizon or '暂无'}"
            )
            lines.append(
                f"  期权逻辑: {pick.option_reason or '暂无'} | 主要风险: {pick.option_risk or '暂无'}"
            )
            lines.append(
                f"  公告信号: {str(pick.raw.get('tdnet_signal', '') or '暂无')} | 公告标题: {str(pick.raw.get('tdnet_title', '') or '暂无')}"
            )

    if groups["B"]:
        lines.extend(["", "B级（观察）"])
        for pick in groups["B"][:3]:
            lines.append(
                f"- {pick.symbol} | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
            )
            lines.append(
                f"  期权方向: {pick.option_bias or '暂无'} | 参考周期: {pick.option_horizon or '暂无'}"
            )
            lines.append(
                f"  期权逻辑: {pick.option_reason or '暂无'} | 主要风险: {pick.option_risk or '暂无'}"
            )
            lines.append(
                f"  公告信号: {str(pick.raw.get('tdnet_signal', '') or '暂无')} | 公告标题: {str(pick.raw.get('tdnet_title', '') or '暂无')}"
            )

    lines.append(f"\nC级（忽略）数量: {c_count}")
    return "\n".join(lines)


def _pick_extra_value(pick, key: str, default=None):
    value = getattr(pick, key, None)
    if value not in (None, ""):
        return value
    raw = getattr(pick, "raw", {}) or {}
    return raw.get(key, default)


def _news_risk_level(pick) -> str:
    value = str(_pick_extra_value(pick, "news_risk_level", "NEUTRAL") or "NEUTRAL").strip().upper()
    return value or "NEUTRAL"


def _execution_status(pick) -> str:
    value = str(_pick_extra_value(pick, "execution_status", "") or "").strip().upper()
    return value


def _extract_tdnet_titles(pick) -> list[str]:
    raw_title = str(_pick_extra_value(pick, "tdnet_title", "") or "").strip()
    if not raw_title:
        return []
    return [part.strip() for part in raw_title.split("|") if part.strip()]


def _extract_news_titles(pick) -> list[str]:
    items = getattr(pick, "news_items", []) or []
    titles = []
    for item in items:
        title = str(getattr(item, "title", "") or "").strip()
        if title:
            titles.append(title)
    return titles


def _pick_news_summary(pick) -> str:
    items = getattr(pick, "news_items", []) or []
    for item in items:
        title = str(getattr(item, "title", "") or "").strip()
        if not title:
            continue
        source = str(getattr(item, "source", "") or "").strip() or "新闻"
        published_at = str(getattr(item, "published_at", "") or "").strip()
        relative_time = _format_relative_time_text(published_at)
        if relative_time:
            return f"{source}｜{title}｜{relative_time}"
        return f"{source}｜{title}"

    tdnet_titles = _extract_tdnet_titles(pick)
    if tdnet_titles:
        return f"TDnet公告｜{tdnet_titles[0]}"

    return "无近期有效新闻"


def _build_execution_signal_input(pick) -> ExecutionSignalInput:
    return ExecutionSignalInput(
        symbol=str(pick.symbol or "").strip(),
        run_date=str(_pick_extra_value(pick, "run_date", "") or "").strip(),
        level=str(pick.level or "").strip(),
        action=str(pick.action or "").strip(),
        option_bias=str(pick.option_bias or "").strip(),
        prev_close=pick.prev_close,
        score=pick.score,
        raw=getattr(pick, "raw", {}) or {},
    )


def _format_relative_time_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        delta_days = int((now - dt.astimezone(timezone.utc)).total_seconds() // 86400)
        if delta_days < 0:
            return "今天"
        if delta_days == 0:
            return "今天"
        if delta_days == 1:
            return "1天前"
        return f"{delta_days}天前"
    except Exception:
        return ""


def _evaluate_pick_guards(pick, *, intraday_cache: dict | None = None):
    signal_input = _build_execution_signal_input(pick)
    tdnet_titles = _extract_tdnet_titles(pick)
    news_titles = _extract_news_titles(pick)
    fetch_result = None

    if is_execution_candidate(signal_input):
        cache_key = (
            str(signal_input.symbol or "").strip(),
            str(signal_input.run_date or "").strip(),
            15,
        )
        if intraday_cache is not None and cache_key in intraday_cache:
            fetch_result = intraday_cache[cache_key]
        else:
            fetch_result = fetch_opening_intraday_bars(
                signal_input.symbol,
                target_date=signal_input.run_date or None,
                window_minutes=15,
            )
            if intraday_cache is not None:
                intraday_cache[cache_key] = fetch_result
        if fetch_result.bars:
            execution_decision = evaluate_execution_guard(
                signal_input,
                fetch_result.bars,
                window_minutes=15,
            )
            execution_decision.metrics["intraday_interval"] = fetch_result.interval
            execution_decision.metrics["used_live_data"] = True
            execution_decision.metrics["intraday_fetch_reason"] = fetch_result.fetch_reason
        else:
            fallback_reason = fetch_result.fetch_reason or "盘中数据不足，当前按日线信号观察。"
            execution_decision = build_execution_decision(
                "WATCH",
                fallback_reason,
                metrics={
                    "used_live_data": False,
                    "intraday_interval": fetch_result.interval,
                    "intraday_fetch_reason": fallback_reason,
                },
            )
    else:
        execution_decision = build_execution_decision(
            "",
            "当前不属于开盘确认重点候选，按日线信号处理。",
            metrics={"used_live_data": False},
        )

    news_decision = evaluate_news_guard(
        signal_input.symbol,
        tdnet_titles=tdnet_titles,
        news_titles=news_titles,
    )
    execution_decision = apply_news_veto(execution_decision, news_decision)
    return execution_decision, news_decision, fetch_result


def _build_trade_advice(pick, execution_decision, news_decision) -> tuple[str, str, str]:
    level = str(pick.level or "").strip().upper()
    option_bias = str(pick.option_bias or "").strip().upper()
    execution_status = str(execution_decision.execution_status or _execution_status(pick) or "").strip().upper()
    news_risk_level = str(news_decision.news_risk_level or _news_risk_level(pick) or "NEUTRAL").strip().upper()
    day_change_pct = pick.day_change_pct
    intraday_pct = pick.intraday_pct
    used_live_data = bool(execution_decision.metrics.get("used_live_data"))

    if execution_status == "SKIP" or news_risk_level == "NEGATIVE" or level == "C":
        buy_advice = "放弃"
    elif (
        level in {"A", "B"}
        and option_bias == "CALL"
        and news_risk_level != "NEGATIVE"
        and (not execution_status or execution_status == "BUY_READY")
    ):
        buy_advice = "可买"
    elif level == "B" or option_bias == "WATCH" or execution_status == "WATCH":
        buy_advice = "观察"
    else:
        buy_advice = "观察"

    if execution_status == "SKIP" or news_risk_level == "NEGATIVE":
        sell_advice = "止损"
    elif (
        isinstance(day_change_pct, (int, float))
        and day_change_pct >= 4
    ) or (
        isinstance(day_change_pct, (int, float))
        and isinstance(intraday_pct, (int, float))
        and day_change_pct >= 2
        and intraday_pct <= 0.3
    ):
        sell_advice = "止盈"
    else:
        sell_advice = "持有"

    reason_parts = []
    if used_live_data:
        if execution_status == "BUY_READY":
            reason_parts.append("开盘确认已通过")
        elif execution_status == "WATCH":
            reason_parts.append("开盘确认未通过")
        elif execution_status == "SKIP":
            reason_parts.append("开盘确认失败")
    else:
        reason_parts.append(execution_decision.execution_reason or "盘中数据不足，当前按日线信号观察。")

    if news_risk_level == "NEGATIVE":
        reason_parts.append("存在明显利空")
    elif option_bias == "WATCH":
        reason_parts.append("期权方向偏观察")
    elif level in {"A", "B"} and option_bias == "CALL" and buy_advice != "放弃":
        reason_parts.append("方向偏多")

    if sell_advice == "止盈":
        reason_parts.append("已有涨幅或冲高回落")
    elif sell_advice == "止损":
        reason_parts.append("风险控制优先")

    reason = "，".join([part for part in reason_parts if part][:3])
    return buy_advice, sell_advice, reason


def _trade_buy_priority(value: str) -> int:
    mapping = {"可买": 0, "观察": 1, "放弃": 2}
    return mapping.get(str(value or "").strip(), 9)


def _trade_level_priority(value: str) -> int:
    mapping = {"A": 0, "B": 1, "C": 2}
    return mapping.get(str(value or "").strip().upper(), 9)


def _execution_priority(value: str) -> int:
    mapping = {"BUY_READY": 0, "WATCH": 1, "": 2, "暂无": 2, "SKIP": 3}
    return mapping.get(str(value or "").strip().upper(), 9)


def _mode_display_name(mode: str) -> str:
    return STRATEGY_LABELS.get(str(mode or "").strip(), str(mode or "").strip())


def _fmt_ai_field(value, digits: int = 2) -> str:
    if value in (None, ""):
        return "暂无"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        text = str(value).strip()
        return text or "暂无"


def _is_trade_candidate(pick) -> int:
    try:
        return 1 if is_execution_candidate(_build_execution_signal_input(pick)) else 0
    except Exception:
        return 0


def _build_compact_trade_reason(reason: str) -> str:
    text = str(reason or "").strip()
    if not text:
        return "暂无"
    parts = [part.strip() for part in text.split("，") if part.strip()]
    return "，".join(parts[:2]) if parts else text


def _pick_execution_result(pick) -> str:
    raw = getattr(pick, "raw", {}) or {}
    value = str(raw.get("execution_result", "") or raw.get("execution_status", "") or "").strip().upper()
    return value


def _collect_symbol_mode_entries(scan_results) -> OrderedDict[str, list[dict]]:
    grouped: OrderedDict[str, list[dict]] = OrderedDict()
    for mode_index, result in enumerate(scan_results or []):
        for pick_index, pick in enumerate(result.picks or []):
            symbol = str(pick.symbol or "").strip()
            if not symbol:
                continue
            grouped.setdefault(symbol, []).append(
                {
                    "symbol": symbol,
                    "mode": result.mode,
                    "mode_index": mode_index,
                    "pick_index": pick_index,
                    "pick": pick,
                    "score": float(pick.score or 0.0),
                    "level": str(pick.level or "").strip().upper(),
                }
            )
    return grouped


def _pick_ai_primary_entry(entries: list[dict]) -> dict | None:
    if not entries:
        return None
    ordered_entries = sorted(
        entries,
        key=lambda item: (
            _execution_priority(_pick_execution_result(item["pick"])),
            _trade_level_priority(item["level"]),
            -int(((getattr(item["pick"], "raw", {}) or {}).get("consecutive_days", 1) or 1)),
            -item["score"],
            item["mode_index"],
            item["pick_index"],
        ),
    )
    return ordered_entries[0]


def _select_ai_focus_picks(scan_results, limit: int = 5) -> list[dict]:
    symbol_entries = _collect_symbol_mode_entries(scan_results)
    selected = []
    for symbol, entries in symbol_entries.items():
        primary = _pick_ai_primary_entry(entries)
        if primary is None:
            continue
        pick = primary["pick"]
        level = str(primary["level"] or "").upper()
        execution_result = _pick_execution_result(pick)
        if level == "C" or execution_result == "SKIP":
            continue
        selected.append(
            {
                "symbol": symbol,
                "mode": primary["mode"],
                "pick": pick,
                "score": float(primary["score"] or 0.0),
                "level": level,
                "execution_result": execution_result,
                "consecutive_days": int(((getattr(pick, "raw", {}) or {}).get("consecutive_days", 1) or 1)),
            }
        )

    selected.sort(
        key=lambda item: (
            _execution_priority(item["execution_result"]),
            _trade_level_priority(item["level"]),
            -item["consecutive_days"],
            -item["score"],
            item["symbol"],
        )
    )
    return selected[:limit]


def build_ai_focus_prompt_message(scan_results) -> str:
    if not scan_results:
        return "当前暂无可生成的 AI精选分析。"

    focus_picks = _select_ai_focus_picks(scan_results, limit=5)
    if not focus_picks:
        return "当前暂无符合条件的 AI精选分析标的。"

    lines = [
        "🧠 AI分析输入（精选）",
        "",
        "===== AI分析输入（复制到ChatGPT）=====",
        "",
        "请作为短线交易分析师，基于技术面 + 市场行为，判断以下股票：",
        "",
    ]

    for index, item in enumerate(focus_picks, start=1):
        pick = item["pick"]
        repeat_tag = _signal_repeat_tag(getattr(pick, "raw", {}) or {})
        lines.extend(
            [
                f"【股票{index}】",
                f"模式: {item['mode']}",
                f"代码: {pick.symbol}",
                f"当前价格: {_fmt_ai_field(pick.close)}",
                f"本地评分: {_fmt_ai_field(pick.score, digits=4)}",
                f"信号: {repeat_tag}",
                "",
                "技术面:",
                "",
                f"* 今日涨幅: {_fmt_ai_field(pick.day_change_pct)}",
                f"* 振幅: {_fmt_ai_field(pick.amplitude_pct)}",
                f"* 量比: {_fmt_ai_field(pick.amount_ratio_5)}",
                f"* 3日动量: {_fmt_ai_field(pick.momentum_3_pct)}",
                f"* 5日动量: {_fmt_ai_field(pick.momentum_5_pct)}",
                f"* 距20日高点: {_fmt_ai_field(pick.dist_to_high_20_pct)}",
                f"* 收盘位置: {_fmt_ai_field(pick.close_position)}",
                "",
            ]
        )

    lines.extend(
        [
            "（最多5只）",
            "",
            "---",
            "",
            "请逐只回答：",
            "",
            "1. 是否值得关注（是/否）",
            "2. 更偏哪种模式（breakout/trend/dip/不明确）",
            "3. 核心逻辑（一句话）",
            "4. 最大风险（一句话）",
            "",
            "===== 结束 =====",
        ]
    )
    return "\n".join(lines)


def _pick_guard_representative(entries: list[dict]):
    if not entries:
        return None
    ordered_entries = sorted(
        entries,
        key=lambda item: (
            -_is_trade_candidate(item["pick"]),
            _trade_level_priority(item["level"]),
            -item["score"],
            item["mode_index"],
            item["pick_index"],
        ),
    )
    return ordered_entries[0]["pick"]


def _collect_trade_advice_entries(scan_results) -> list[dict]:
    symbol_entries = _collect_symbol_mode_entries(scan_results)
    intraday_cache: dict[tuple[str, str, int], object] = {}
    checked_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    entries = []
    for symbol, grouped_entries in symbol_entries.items():
        representative_pick = _pick_guard_representative(grouped_entries)
        if representative_pick is None:
            continue

        execution_decision, news_decision, fetch_result = _evaluate_pick_guards(
            representative_pick,
            intraday_cache=intraday_cache,
        )

        for item in grouped_entries:
            pick = item["pick"]
            buy_advice, sell_advice, reason = _build_trade_advice(pick, execution_decision, news_decision)
            entries.append(
                {
                    "symbol": symbol,
                    "mode": item["mode"],
                    "mode_index": item["mode_index"],
                    "pick_index": item["pick_index"],
                    "pick": pick,
                    "buy_advice": buy_advice,
                    "sell_advice": sell_advice,
                    "reason": _build_compact_trade_reason(reason),
                    "execution_status": execution_decision.execution_status or "暂无",
                    "news_risk_level": news_decision.news_risk_level or "NEUTRAL",
                    "execution_checked_at": checked_at,
                    "score": item["score"],
                    "level": item["level"],
                }
            )
    return entries


def _build_execution_feedback_updates(entries: list[dict]) -> list[dict]:
    updates = []
    for entry in entries or []:
        pick = entry.get("pick")
        raw = getattr(pick, "raw", {}) or {}
        updates.append(
            {
                "signal_id": str(raw.get("signal_id", "") or "").strip(),
                "run_date": str(raw.get("run_date", "") or "").strip(),
                "selected_mode": str(raw.get("selected_mode", "") or entry.get("mode", "") or "").strip(),
                "strategy_source": str(raw.get("strategy_source", "") or "scan").strip(),
                "symbol": str(getattr(pick, "symbol", "") or entry.get("symbol", "") or "").strip(),
                "execution_result": str(entry.get("execution_status", "") or "").strip().upper(),
                "execution_checked_at": str(entry.get("execution_checked_at", "") or "").strip(),
                "news_risk_level": str(entry.get("news_risk_level", "") or "").strip().upper(),
            }
        )
    return updates


def _merge_trade_advice_entries(entries: list[dict]) -> list[dict]:
    grouped: OrderedDict[str, list[dict]] = OrderedDict()
    for entry in entries:
        symbol = entry["symbol"]
        if not symbol:
            continue
        grouped.setdefault(symbol, []).append(entry)

    merged = []
    for symbol, symbol_entries in grouped.items():
        ordered_entries = sorted(
            symbol_entries,
            key=lambda item: (
                _trade_buy_priority(item["buy_advice"]),
                _trade_level_priority(item["level"]),
                -item["score"],
                item["mode_index"],
                item["pick_index"],
            ),
        )
        primary = ordered_entries[0]
        mode_names = []
        for item in ordered_entries:
            mode_name = _mode_display_name(item["mode"])
            if mode_name not in mode_names:
                mode_names.append(mode_name)

        primary["mode_names"] = mode_names
        primary["secondary_mode_names"] = mode_names[1:]
        merged.append(primary)

    merged.sort(
        key=lambda item: (
            _trade_buy_priority(item["buy_advice"]),
            _trade_level_priority(item["level"]),
            -item["score"],
            item["mode_index"],
            item["pick_index"],
        )
    )
    return merged


def build_trade_advice_message(scan_results, *, entries: list[dict] | None = None) -> str:
    if not scan_results:
        return "当前无可生成的买卖建议。"

    raw_entries = entries if entries is not None else _collect_trade_advice_entries(scan_results)
    merged_entries = _merge_trade_advice_entries(raw_entries)
    if not merged_entries:
        return "当前无可生成的买卖建议。"

    lines = ["💡 买卖建议"]
    for entry in merged_entries:
        pick = entry["pick"]
        repeat_tag = _signal_repeat_tag(getattr(pick, "raw", {}) or {})
        lines.extend(
            [
                "",
                f"{pick.symbol}（{repeat_tag}）｜买入: {entry['buy_advice']}｜卖出: {entry['sell_advice']}｜开盘: {entry['execution_status']}｜消息: {entry['news_risk_level']}",
                f"理由：{entry['reason']}",
                f"新闻：{_pick_news_summary(pick)}",
            ]
        )
        if entry["secondary_mode_names"]:
            lines.append(
                f"主模式：{entry['mode_names'][0]}｜同时命中：{' / '.join(entry['secondary_mode_names'])}"
            )
        else:
            lines.append(f"命中模式：{entry['mode_names'][0]}")
    return "\n".join(lines)


def _fmt_return_pct(value):
    if value is None:
        return "暂无"
    try:
        return f"{float(value):.2f}%"
    except Exception:
        return "暂无"


def _fmt_signed_return_pct(value):
    if value is None:
        return "暂无"
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return "暂无"


def _fmt_winrate_pct(value):
    if value is None:
        return "暂无"
    try:
        return f"{float(value) * 100.0:.1f}%"
    except Exception:
        return "暂无"


def _fmt_group_key(group_name: str, value):
    text = str(value or "").strip()
    if not text or text == "(empty)":
        return "暂无"

    if group_name == "by_selected_mode":
        return STRATEGY_LABELS.get(text, text)
    if group_name == "by_strategy_source":
        return "自动选择" if text == "auto" else "手动指定" if text == "manual" else text
    if group_name == "by_level":
        return LEVEL_LABELS.get(text, text)
    if group_name == "by_action":
        return ACTION_LABELS.get(text, text)
    return text


def _append_group(lines: list[str], title: str, grouped: dict, *, key_name: str, ret_field: str, winrate_field: str):
    lines.extend(["", title])
    if not grouped:
        lines.append("- 暂无：样本 0｜次日均收益 暂无｜次日胜率 暂无")
        return

    for key, value in grouped.items():
        lines.append(
            f"- {_fmt_group_key(key_name, key)}：样本 {value.get('count', 0)}｜"
            f"次日均收益 {_fmt_return_pct(value.get(ret_field))}｜"
            f"次日胜率 {_fmt_winrate_pct(value.get(winrate_field))}"
        )


def _append_execution_backtest(lines: list[str], execution_summary: dict):
    window_days = int(execution_summary.get("window_days", 30) or 30)
    by_execution = execution_summary.get("by_execution_result", {}) or {}
    by_mode_execution = execution_summary.get("by_mode_execution", []) or []

    lines.extend(["", f"🧠 执行层统计（近{window_days}天）"])
    for status, label in (
        ("BUY_READY", "BUY_READY（可执行）"),
        ("WATCH", "WATCH（观察）"),
        ("SKIP", "SKIP（放弃）"),
    ):
        stats = by_execution.get(status, {}) or {}
        lines.extend(
            [
                "",
                label,
                f"样本：{stats.get('count', 0)}",
                f"次日均收益：{_fmt_signed_return_pct(stats.get('ret_1d_mean'))}",
                f"次日胜率：{_fmt_winrate_pct(stats.get('winrate_1d'))}",
                f"3日收益：{_fmt_signed_return_pct(stats.get('ret_3d_mean'))}",
                f"5日收益：{_fmt_signed_return_pct(stats.get('ret_5d_mean'))}",
            ]
        )

    lines.extend(["", f"📊 策略 × 执行（近{window_days}天）"])
    if not by_mode_execution:
        lines.append("暂无 execution_result 数据")
        return

    for row in by_mode_execution:
        mode_label = str(row.get("selected_mode_label", "") or row.get("selected_mode", "") or "").strip()
        execution_result = str(row.get("execution_result", "") or "").strip()
        lines.extend(
            [
                "",
                f"{mode_label} + {execution_result}",
                f"样本 {row.get('count', 0)}｜次日 {_fmt_signed_return_pct(row.get('ret_1d_mean'))}｜胜率 {_fmt_winrate_pct(row.get('winrate_1d'))}",
            ]
        )


def build_backtest_summary_message(summary: dict | None) -> str:
    if not summary:
        return "暂无回测汇总，请先等待每日自动回测刷新。"

    overall = summary.get("overall", {})
    no_future = (
        overall.get("ret_1d_mean") is None
        and overall.get("ret_3d_mean") is None
        and overall.get("ret_5d_mean") is None
    )

    lines = [
        "📊 回测汇总",
        f"总样本：{overall.get('count', 0)}",
        f"次日 / 3日 / 5日均收益：{_fmt_return_pct(overall.get('ret_1d_mean'))} / {_fmt_return_pct(overall.get('ret_3d_mean'))} / {_fmt_return_pct(overall.get('ret_5d_mean'))}",
        f"次日 / 3日 / 5日胜率：{_fmt_winrate_pct(overall.get('winrate_1d'))} / {_fmt_winrate_pct(overall.get('winrate_3d'))} / {_fmt_winrate_pct(overall.get('winrate_5d'))}",
    ]

    _append_group(
        lines,
        "按策略类型",
        summary.get("by_selected_mode", {}),
        key_name="by_selected_mode",
        ret_field="ret_1d_mean",
        winrate_field="winrate_1d",
    )
    _append_group(
        lines,
        "按信号等级",
        summary.get("by_level", {}),
        key_name="by_level",
        ret_field="ret_1d_mean",
        winrate_field="winrate_1d",
    )
    _append_group(
        lines,
        "按操作建议",
        summary.get("by_action", {}),
        key_name="by_action",
        ret_field="ret_1d_mean",
        winrate_field="winrate_1d",
    )
    _append_group(
        lines,
        "按期权方向",
        summary.get("by_option_bias", {}),
        key_name="by_option_bias",
        ret_field="ret_1d_mean",
        winrate_field="winrate_1d",
    )
    _append_group(
        lines,
        "按公告信号",
        summary.get("by_tdnet_signal", {}),
        key_name="by_tdnet_signal",
        ret_field="ret_1d_mean",
        winrate_field="winrate_1d",
    )
    _append_execution_backtest(lines, summary.get("execution_backtest", {}) or {})

    if no_future:
        lines.extend(["", "暂无未来交易日结果"])

    return "\n".join(lines)


def load_saved_summary() -> dict | None:
    if not SUMMARY_FILE.exists():
        return None
    try:
        return json.loads(SUMMARY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None


async def setup_bot_commands(app):
    commands = [
        BotCommand("start", "显示主菜单"),
        BotCommand("run", "运行今日策略"),
        BotCommand("summary", "查看今日结论"),
    ]
    await app.bot.set_my_commands(commands)
    log("[BOT] commands set")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log(f"[MSG] /start chat_id={update.effective_chat.id}")
    text = (
        "AI 股票 Bot 已启动。\n\n"
        "当前版本只聚焦：运行策略、查看结论、回测汇总。"
    )
    await update.message.reply_text(text, reply_markup=MAIN_MENU)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "首页功能：\n"
        "🤖 运行今日策略\n"
        "📌 今日结论\n"
        "💡 买卖建议\n"
        "🧠 AI精选分析\n"
        "📊 回测汇总\n"
    )
    await update.message.reply_text(text, reply_markup=MAIN_MENU)


async def ensure_scan_results(update: Update, *, force_run: bool = False):
    global LAST_SCAN_RESULTS
    if LAST_SCAN_RESULTS is not None and not force_run:
        return LAST_SCAN_RESULTS

    _, scan_results = load_latest_stored_scan_results()
    if not scan_results:
        raise RuntimeError("未找到当日已落盘三模式信号，请先确认 data/backtest/signals.csv 已生成 scan 结果。")
    LAST_SCAN_RESULTS = scan_results
    return LAST_SCAN_RESULTS


async def handle_run_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LAST_SCAN_RESULTS
    chat_id = update.effective_chat.id
    log(f"[RUN] chat_id={chat_id} mode=multi_scan")
    try:
        await update.message.reply_text("读取当日已落盘三模式信号，请稍候…", reply_markup=MAIN_MENU)
        scan_results = await ensure_scan_results(update, force_run=True)
        await update.message.reply_text(build_multi_mode_run_message(scan_results), reply_markup=MAIN_MENU)
        log(f"[RUN] done chat_id={chat_id} multi_mode_count={len(scan_results)}")
    except Exception as exc:
        await update.message.reply_text(f"执行失败: {exc}", reply_markup=MAIN_MENU)


async def handle_today_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        scan_results = await ensure_scan_results(update, force_run=True)
        await update.message.reply_text(build_today_summary(scan_results), reply_markup=MAIN_MENU)
    except Exception as exc:
        await update.message.reply_text(f"读取今日结论失败: {exc}", reply_markup=MAIN_MENU)


async def handle_ai_focus_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scan_results = await ensure_scan_results(update)
    await update.message.reply_text(build_ai_focus_prompt_message(scan_results), reply_markup=MAIN_MENU)


async def handle_trade_advice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scan_results = await ensure_scan_results(update)
    entries = _collect_trade_advice_entries(scan_results)
    feedback_result = save_execution_feedback(_build_execution_feedback_updates(entries))
    if feedback_result.get("results_updated", 0) or feedback_result.get("signals_updated", 0):
        log(
            f"[TRADE_ADVICE] execution feedback saved results={feedback_result.get('results_updated', 0)} "
            f"signals={feedback_result.get('signals_updated', 0)}"
        )
    await update.message.reply_text(
        build_trade_advice_message(scan_results, entries=entries),
        reply_markup=MAIN_MENU,
    )


async def handle_backtest_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    summary = load_saved_summary()
    if summary is None:
        summary = build_backtest_summary()
        if summary:
            save_backtest_summary(summary)
    await update.message.reply_text(build_backtest_summary_message(summary), reply_markup=MAIN_MENU)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    log(f"[TEXT] chat_id={update.effective_chat.id} text={text}")

    if text == "🤖 运行今日策略":
        await handle_run_today(update, context)
        return
    if text == "📌 今日结论":
        await handle_today_summary(update, context)
        return
    if text == "📊 回测汇总":
        await handle_backtest_summary(update, context)
        return
    if text == "💡 买卖建议":
        await handle_trade_advice(update, context)
        return
    if text == "🧠 AI精选分析":
        await handle_ai_focus_input(update, context)
        return

    if text.lower() == "run":
        await handle_run_today(update, context)
        return
    if text.lower() == "summary":
        await handle_today_summary(update, context)
        return

    await update.message.reply_text("请使用菜单按钮操作。", reply_markup=MAIN_MENU)


async def run_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await handle_run_today(update, context)


async def summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await handle_today_summary(update, context)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("未配置 TELEGRAM_BOT_TOKEN，请先加载 .env 再运行。")

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(setup_bot_commands).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("run", run_command))
    app.add_handler(CommandHandler("summary", summary_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    log("Telegram Bot 已启动")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
