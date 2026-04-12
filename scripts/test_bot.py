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

from app.services.picker import run_picker_result
from reporting.formatters.telegram_formatter import format_ai_prompt, format_pick_result
from scripts.run_backtest import SUMMARY_FILE, build_backtest_summary, save_backtest_summary
from scripts.update_backtest_results import update_backtest_results
from storage.runtime_state import get_bot_mode, save_bot_mode_state


BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

MODE_LABELS = {
    "auto": "自动模式",
    "breakout": "强制 breakout",
    "trend": "强制 trend",
    "dip": "强制 dip",
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
        ["🧠 AI分析输入", "📈 更新回测结果"],
        [HOME_BUTTON],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="更多功能…",
)

MODE_MENU = ReplyKeyboardMarkup(
    keyboard=[
        ["自动模式", "强制 dip"],
        ["强制 trend", "强制 breakout"],
        ["查看当前模式"],
        [HOME_BUTTON],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="请选择模式…",
)

MODE_BUTTON_TO_STATE = {
    "自动模式": "auto",
    "强制 dip": "dip",
    "强制 trend": "trend",
    "强制 breakout": "breakout",
}

LAST_RESULT = None


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


def build_today_summary(result) -> str:
    groups = _group_picks(result)
    a_count = len(groups["A"])
    b_picks = groups["B"][:3]
    c_count = len(groups["C"])
    market_state = result.market_state

    lines = [
        "📌 今日结论",
        f"市场状态: {market_state.state or '-'} | 上涨占比: {round(market_state.up_ratio * 100.0, 1)}%",
        f"当前策略: {result.mode} | {'自动选择' if result.mode_source == 'auto' else '手动指定'}",
        f"A/B/C 数量: {a_count}/{len(groups['B'])}/{c_count}",
    ]

    if a_count == 0:
        lines.append("⚠️ 今日无明确买点，建议观望")
    else:
        lines.append(f"✅ 今日有 {a_count} 个 A级买点")

    if b_picks:
        lines.extend(["", "B级前3"])
        for pick in b_picks:
            lines.append(f"- {pick.symbol} | score={pick.score} | {pick.reason}")

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
        f"当前策略: {result.mode} | {'自动选择' if result.mode_source == 'auto' else '手动指定'}",
        f"A/B/C 数量: {a_count}/{b_count}/{c_count}",
    ]

    if a_count == 0:
        lines.append("⚠️ 今日无明确买点，建议观望")
    else:
        lines.append("✅ 今日存在明确买点")

    if groups["A"]:
        lines.extend(["", "A级（buy）"])
        for pick in groups["A"]:
            lines.append(f"- {pick.symbol} | score={pick.score} | {pick.reason}")

    if groups["B"]:
        lines.extend(["", "B级（watch）"])
        for pick in groups["B"][:3]:
            lines.append(f"- {pick.symbol} | score={pick.score} | {pick.reason}")

    lines.append(f"\nC级（ignore）数量: {c_count}")
    return "\n".join(lines)


def build_a_signals_message(result) -> str:
    a_picks = _group_picks(result)["A"]
    if not a_picks:
        return "当前无A级信号"

    lines = ["🟢 A级信号"]
    for pick in a_picks:
        lines.append(f"- {pick.symbol} | score={pick.score} | action={pick.action} | {pick.reason}")
    return "\n".join(lines)


def build_b_signals_message(result) -> str:
    b_picks = _group_picks(result)["B"][:3]
    if not b_picks:
        return "当前无B级观察信号"

    lines = ["🟡 B级观察"]
    for pick in b_picks:
        lines.append(f"- {pick.symbol} | score={pick.score} | action={pick.action} | {pick.reason}")
    return "\n".join(lines)


def build_mode_status_message() -> str:
    mode = get_bot_mode()
    if mode == "auto":
        return "当前模式: 自动模式\n运行今日策略时将根据市场状态自动选择 dip / trend / breakout。"
    return f"当前模式: {MODE_LABELS.get(mode, mode)}\n运行今日策略时将强制使用该模式。"


def _fmt_metric(value):
    return "暂无" if value is None else value


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
        f"ret_1d / ret_3d / ret_5d: {_fmt_metric(overall.get('ret_1d_mean'))} / {_fmt_metric(overall.get('ret_3d_mean'))} / {_fmt_metric(overall.get('ret_5d_mean'))}",
        f"胜率1d / 3d / 5d: {_fmt_metric(overall.get('winrate_1d'))} / {_fmt_metric(overall.get('winrate_3d'))} / {_fmt_metric(overall.get('winrate_5d'))}",
        "",
        "按 selected_mode:",
    ]

    for key, value in summary.get("by_selected_mode", {}).items():
        lines.append(f"- {key}: count={value.get('count')} ret_5d={_fmt_metric(value.get('ret_5d_mean'))} winrate_5d={_fmt_metric(value.get('winrate_5d'))}")

    lines.extend(["", "按 level:"])
    for key, value in summary.get("by_level", {}).items():
        lines.append(f"- {key}: count={value.get('count')} ret_5d={_fmt_metric(value.get('ret_5d_mean'))}")

    lines.extend(["", "按 action:"])
    for key, value in summary.get("by_action", {}).items():
        lines.append(f"- {key}: count={value.get('count')} ret_5d={_fmt_metric(value.get('ret_5d_mean'))}")

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


async def handle_run_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LAST_RESULT
    chat_id = update.effective_chat.id
    log(f"[RUN] chat_id={chat_id} mode={get_bot_mode()}")
    try:
        result = await ensure_result(update, force_run=True)
        await update.message.reply_text(build_run_result_message(result), reply_markup=MAIN_MENU)
        log(f"[RUN] done chat_id={chat_id} mode={result.mode} count={len(result.picks)}")
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
