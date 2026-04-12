"""Console-friendly formatting for PickResult."""

from __future__ import annotations

from collections import OrderedDict

from reporting.schemas import PickResult


MODE_LABELS = {
    "breakout": "短线打板/追涨",
    "trend": "趋势跟随",
    "dip": "低吸反弹",
}


def format_pick_result(result: PickResult) -> str:
    mode_text = MODE_LABELS.get(result.mode, result.mode)
    market_state = result.market_state
    up_ratio_pct = round(market_state.up_ratio * 100.0, 1)
    lines = [
        "📌 Pick 结果",
        f"策略模式: {mode_text}",
        f"市场状态: {market_state.state or '-'} | 上涨占比: {up_ratio_pct}% | 平均涨幅: {market_state.avg_change_pct}%",
        f"策略来源: {'自动选择' if result.mode_source == 'auto' else '手动指定'}",
        "",
        result.status.title,
        result.status.text,
        "",
        "=== 推荐股票 ===",
    ]

    if not result.picks:
        lines.append("当前没有可用推荐。")
        return "\n".join(lines)

    grouped = OrderedDict([("A", []), ("B", []), ("C", [])])
    for pick in result.picks:
        grouped.setdefault(pick.level, []).append(pick)

    rank = 1
    for level, picks in grouped.items():
        if not picks:
            continue
        lines.append(f"[{level}级]")
        for pick in picks:
            lines.append(
                f"{rank}. {pick.symbol} | ¥{pick.close} | score={pick.score} | action={pick.action} | {pick.reason}"
            )
            lines.append(
                f"   期权建议: {pick.option_bias or '-'} | {pick.option_horizon or '-'} | {pick.option_reason or '-'}"
            )
            rank += 1
        lines.append("")

    if lines[-1] == "":
        lines.pop()

    first = result.picks[0]
    if first.ai_prompt:
        lines.extend(["", "=== AI分析输入（复制给ChatGPT）===", first.ai_prompt])
    return "\n".join(lines)
