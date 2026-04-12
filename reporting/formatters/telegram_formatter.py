"""Telegram-oriented formatting for PickResult."""

from __future__ import annotations

from collections import OrderedDict

from reporting.schemas import PickResult


MODE_LABELS = {
    "breakout": "🔥 短线打板 / 追涨",
    "trend": "📈 趋势跟随",
    "dip": "🧲 低吸反弹",
}


def format_pick_result(result: PickResult) -> str:
    mode_text = MODE_LABELS.get(result.mode, result.mode)
    market_state = result.market_state
    up_ratio_pct = round(market_state.up_ratio * 100.0, 1)
    lines = [
        "📌 交易决策提示",
        f"市场状态: {market_state.state or '-'} | 上涨占比: {up_ratio_pct}% | 平均涨幅: {market_state.avg_change_pct}%",
        f"当前策略: {result.mode} ({mode_text}) | {'自动选择' if result.mode_source == 'auto' else '手动指定'}",
        "",
    ]

    if not result.picks:
        lines.append("当前没有可用推荐。")
        return "\n".join(lines)

    grouped = OrderedDict([("A", []), ("B", []), ("C", [])])
    for pick in result.picks:
        grouped.setdefault(pick.level, []).append(pick)

    a_picks = grouped.get("A", [])
    b_picks = grouped.get("B", [])[:3]
    c_count = len(grouped.get("C", []))

    if not a_picks:
        lines.append("⚠️ 今日无明确买点，建议观望")
        lines.append("")

    if a_picks:
        lines.append("A级（buy）")
        for pick in a_picks:
            lines.append(
                f"- {pick.symbol} | ¥{pick.close} | score={pick.score} | {pick.reason}"
            )
            lines.append(
                f"  期权: {pick.option_bias or '-'} | {pick.option_horizon or '-'} | {pick.option_reason or '-'}"
            )
        lines.append("")

    if b_picks:
        lines.append("B级（watch）")
        for pick in b_picks:
            lines.append(
                f"- {pick.symbol} | ¥{pick.close} | score={pick.score} | {pick.reason}"
            )
            lines.append(
                f"  期权: {pick.option_bias or '-'} | {pick.option_horizon or '-'} | {pick.option_reason or '-'}"
            )
        lines.append("")

    lines.append(f"C级（ignore）数量: {c_count}")

    return "\n".join(lines)


def format_ai_prompt(result: PickResult) -> str:
    if not result.picks:
        return "当前没有可生成的 AI 分析输入。"

    first = result.picks[0]
    if first.ai_prompt:
        return first.ai_prompt
    return "当前结果没有 AI 分析输入。"
