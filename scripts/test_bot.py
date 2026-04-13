from __future__ import annotations

import json
import os
import sys
from pathlib import Path

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
from app.services.picker import run_multi_mode_scan_results, run_picker_result
from reporting.formatters.telegram_formatter import format_ai_prompt, format_pick_result
from scripts.run_backtest import SUMMARY_FILE, build_backtest_summary, save_backtest_summary
from scripts.update_backtest_results import update_backtest_results
from storage.runtime_state import get_bot_mode, save_bot_mode_state


BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

MODE_LABELS = {
    "auto": "自动模式",
    "breakout": "强制短线打板 / 追涨",
    "trend": "强制趋势跟随",
    "dip": "强制低吸反弹",
}

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

HOME_BUTTON = "⬅ 返回首页"

MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        ["🤖 运行今日策略", "📌 今日结论"],
        ["📊 回测汇总", "⚙ 模式切换"],
        ["📝 更多"],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="请选择功能…",
)

MORE_MENU = ReplyKeyboardMarkup(
    keyboard=[
        ["🟢 A级信号", "🟡 B级观察"],
        ["💡 买卖建议", "🧠 AI分析输入"],
        ["📈 更新回测结果"],
        [HOME_BUTTON],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="更多功能…",
)

MODE_MENU = ReplyKeyboardMarkup(
    keyboard=[
        ["自动模式", "强制低吸反弹"],
        ["强制趋势跟随", "强制短线打板 / 追涨"],
        ["查看当前模式"],
        [HOME_BUTTON],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="请选择模式…",
)

MODE_BUTTON_TO_STATE = {
    "自动模式": "auto",
    "强制低吸反弹": "dip",
    "强制趋势跟随": "trend",
    "强制短线打板 / 追涨": "breakout",
}

LAST_RESULT = None
LAST_SCAN_RESULTS = None


def log(msg: str):
    print(msg, flush=True)


def current_mode_label() -> str:
    return MODE_LABELS.get(get_bot_mode(), "自动模式")


def resolve_picker_mode() -> str | None:
    current = get_bot_mode()
    return None if current == "auto" else current


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
                lines.append(
                    f"- {pick.symbol} | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
                )

        if groups["B"]:
            lines.append("B级（观察）")
            for pick in groups["B"][:3]:
                lines.append(
                    f"- {pick.symbol} | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
                )

        if not result.picks:
            lines.append("- 暂无候选")
        else:
            lines.append(f"C级（忽略）数量: {len(groups['C'])}")

    return "\n".join(lines)


def build_today_summary(result) -> str:
    groups = _group_picks(result)
    a_count = len(groups["A"])
    b_picks = groups["B"][:3]
    c_count = len(groups["C"])
    market_state = result.market_state

    lines = [
        "📌 今日结论",
        f"市场状态: {market_state.state or '-'} | 上涨占比: {round(market_state.up_ratio * 100.0, 1)}%",
        f"策略类型: {STRATEGY_LABELS.get(result.mode, result.mode)}",
        f"策略来源: {'自动选择' if result.mode_source == 'auto' else '手动指定'}",
        f"A/B/C 数量: {a_count}/{len(groups['B'])}/{c_count}",
    ]

    if a_count == 0:
        lines.append("⚠️ 今日无明确买点，建议观望")
    else:
        lines.append(f"✅ 今日有 {a_count} 个 A级买点")

    if b_picks:
        lines.extend(["", "B级前3"])
        for pick in b_picks:
            lines.append(
                f"- {pick.symbol} | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
            )

    lines.append(f"\nC级数量: {c_count}")
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


def build_a_signals_message(result) -> str:
    a_picks = _group_picks(result)["A"]
    if not a_picks:
        return "当前无A级信号"

    lines = ["🟢 A级信号"]
    for pick in a_picks:
        lines.append(
            f"- {pick.symbol} | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
        )
        lines.append(
            f"  期权方向: {pick.option_bias or '暂无'} | 公告信号: {str(pick.raw.get('tdnet_signal', '') or '暂无')}"
        )
    return "\n".join(lines)


def build_b_signals_message(result) -> str:
    b_picks = _group_picks(result)["B"][:3]
    if not b_picks:
        return "当前无B级观察信号"

    lines = ["🟡 B级观察"]
    for pick in b_picks:
        lines.append(
            f"- {pick.symbol} | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
        )
        lines.append(
            f"  期权方向: {pick.option_bias or '暂无'} | 公告信号: {str(pick.raw.get('tdnet_signal', '') or '暂无')}"
        )
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


def _evaluate_pick_guards(pick):
    signal_input = _build_execution_signal_input(pick)
    tdnet_titles = _extract_tdnet_titles(pick)
    news_titles = _extract_news_titles(pick)
    fetch_result = None

    if is_execution_candidate(signal_input):
        fetch_result = fetch_opening_intraday_bars(
            signal_input.symbol,
            target_date=signal_input.run_date or None,
            window_minutes=15,
        )
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


def build_trade_advice_message(scan_results) -> str:
    if not scan_results:
        return "当前无可生成的买卖建议。"

    lines = ["💡 买卖建议"]
    for result in scan_results:
        lines.extend(["", f"【{STRATEGY_LABELS.get(result.mode, result.mode)}】"])
        if not result.picks:
            lines.append("暂无候选")
            continue

        for pick in result.picks:
            execution_decision, news_decision, fetch_result = _evaluate_pick_guards(pick)
            buy_advice, sell_advice, reason = _build_trade_advice(pick, execution_decision, news_decision)
            execution_status = execution_decision.execution_status or "暂无"
            news_risk_level = news_decision.news_risk_level or "NEUTRAL"
            interval_text = ""
            if fetch_result and fetch_result.used_live_data and fetch_result.interval:
                interval_text = f" ({fetch_result.interval})"
            lines.extend(
                [
                    "",
                    str(pick.symbol),
                    f"买入：{buy_advice}",
                    f"卖出：{sell_advice}",
                    f"开盘确认：{execution_status}{interval_text}",
                    f"消息风控：{news_risk_level}",
                    f"理由：{reason}",
                ]
            )
    return "\n".join(lines)


def build_mode_status_message() -> str:
    mode = get_bot_mode()
    if mode == "auto":
        return "当前模式: 自动模式\n运行今日策略时将根据市场状态自动选择低吸反弹、趋势跟随或短线打板 / 追涨。"
    return f"当前模式: {MODE_LABELS.get(mode, mode)}\n运行今日策略时将强制使用该模式。"


def _fmt_metric(value):
    return "暂无" if value is None else value


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
        lines.append("- 暂无: count=0 ret_1d=暂无 winrate_1d=暂无")
        return

    for key, value in grouped.items():
        lines.append(
            f"- {_fmt_group_key(key_name, key)}: count={value.get('count', 0)} "
            f"ret_1d={_fmt_metric(value.get(ret_field))} "
            f"winrate_1d={_fmt_metric(value.get(winrate_field))}"
        )


def build_backtest_summary_message(summary: dict | None) -> str:
    if not summary:
        return "暂无回测汇总，请先点击“📈 更新回测结果”。"

    overall = summary.get("overall", {})
    no_future = (
        overall.get("ret_1d_mean") is None
        and overall.get("ret_3d_mean") is None
        and overall.get("ret_5d_mean") is None
    )

    lines = [
        "📊 回测汇总",
        f"总信号数: {overall.get('count', 0)}",
        f"1日 / 3日 / 5日平均收益: {_fmt_metric(overall.get('ret_1d_mean'))} / {_fmt_metric(overall.get('ret_3d_mean'))} / {_fmt_metric(overall.get('ret_5d_mean'))}",
        f"1日 / 3日 / 5日胜率: {_fmt_metric(overall.get('winrate_1d'))} / {_fmt_metric(overall.get('winrate_3d'))} / {_fmt_metric(overall.get('winrate_5d'))}",
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
        "当前版本只聚焦：运行策略、查看结论、回测汇总、模式切换。"
    )
    await update.message.reply_text(text, reply_markup=MAIN_MENU)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "首页功能：\n"
        "🤖 运行今日策略\n"
        "📌 今日结论\n"
        "📊 回测汇总\n"
        "⚙ 模式切换\n"
        "📝 更多"
    )
    await update.message.reply_text(text, reply_markup=MAIN_MENU)


async def ensure_result(update: Update, *, force_run: bool = False):
    global LAST_RESULT
    if LAST_RESULT is not None and not force_run:
        return LAST_RESULT

    mode = resolve_picker_mode()
    await update.message.reply_text("开始执行今日策略，请稍候…", reply_markup=MAIN_MENU)
    LAST_RESULT = run_picker_result(limit=5, candidate_limit=30, mode=mode)
    return LAST_RESULT


async def ensure_scan_results(update: Update, *, force_run: bool = False):
    global LAST_SCAN_RESULTS
    if LAST_SCAN_RESULTS is not None and not force_run:
        return LAST_SCAN_RESULTS

    await update.message.reply_text("开始执行三模式扫描，请稍候…", reply_markup=MAIN_MENU)
    LAST_SCAN_RESULTS = run_multi_mode_scan_results(limit=5, candidate_limit=30)
    return LAST_SCAN_RESULTS


async def handle_run_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LAST_SCAN_RESULTS
    chat_id = update.effective_chat.id
    log(f"[RUN] chat_id={chat_id} mode={get_bot_mode()}")
    try:
        scan_results = await ensure_scan_results(update, force_run=True)
        await update.message.reply_text(build_multi_mode_run_message(scan_results), reply_markup=MAIN_MENU)
        log(f"[RUN] done chat_id={chat_id} multi_mode_count={len(scan_results)}")
    except Exception as exc:
        await update.message.reply_text(f"执行失败: {exc}", reply_markup=MAIN_MENU)


async def handle_today_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        result = await ensure_result(update)
        await update.message.reply_text(build_today_summary(result), reply_markup=MAIN_MENU)
    except Exception as exc:
        await update.message.reply_text(f"读取今日结论失败: {exc}", reply_markup=MAIN_MENU)


async def handle_more_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("更多功能", reply_markup=MORE_MENU)


async def handle_mode_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(build_mode_status_message(), reply_markup=MODE_MENU)


async def handle_a_signals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = await ensure_result(update)
    await update.message.reply_text(build_a_signals_message(result), reply_markup=MORE_MENU)


async def handle_b_signals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = await ensure_result(update)
    await update.message.reply_text(build_b_signals_message(result), reply_markup=MORE_MENU)


async def handle_ai_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = await ensure_result(update)
    await update.message.reply_text(format_ai_prompt(result), reply_markup=MORE_MENU)


async def handle_trade_advice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scan_results = await ensure_scan_results(update)
    await update.message.reply_text(build_trade_advice_message(scan_results), reply_markup=MORE_MENU)


async def handle_update_backtest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("开始更新回测结果，请稍候…", reply_markup=MORE_MENU)
    try:
        result = update_backtest_results()
        if not result["ok"]:
            await update.message.reply_text(f"执行失败: {result['message']}", reply_markup=MORE_MENU)
            return

        summary = build_backtest_summary()
        if summary:
            save_backtest_summary(summary)

        msg = (
            "回测结果更新完成\n"
            f"信号数: {result['count']}\n"
            f"已补全 ret_5d: {result['ret_5d_ready']}"
        )
        await update.message.reply_text(msg, reply_markup=MORE_MENU)
    except Exception as exc:
        await update.message.reply_text(f"执行失败: {exc}", reply_markup=MORE_MENU)


async def handle_backtest_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    summary = load_saved_summary()
    if summary is None:
        summary = build_backtest_summary()
        if summary:
            save_backtest_summary(summary)
    await update.message.reply_text(build_backtest_summary_message(summary), reply_markup=MAIN_MENU)


async def handle_mode_set(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str):
    save_bot_mode_state(mode)
    await update.message.reply_text(f"已切换为：{MODE_LABELS.get(mode, mode)}", reply_markup=MODE_MENU)


async def handle_show_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(build_mode_status_message(), reply_markup=MODE_MENU)


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
    if text == "⚙ 模式切换":
        await handle_mode_menu(update, context)
        return
    if text == "📝 更多":
        await handle_more_menu(update, context)
        return
    if text == "🟢 A级信号":
        await handle_a_signals(update, context)
        return
    if text == "🟡 B级观察":
        await handle_b_signals(update, context)
        return
    if text == "💡 买卖建议":
        await handle_trade_advice(update, context)
        return
    if text == "🧠 AI分析输入":
        await handle_ai_input(update, context)
        return
    if text == "📈 更新回测结果":
        await handle_update_backtest(update, context)
        return
    if text == HOME_BUTTON:
        await update.message.reply_text("返回首页", reply_markup=MAIN_MENU)
        return
    if text == "查看当前模式":
        await handle_show_mode(update, context)
        return
    if text in MODE_BUTTON_TO_STATE:
        await handle_mode_set(update, context, MODE_BUTTON_TO_STATE[text])
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
