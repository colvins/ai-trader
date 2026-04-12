"""Lightweight option direction hints derived from existing stock signals."""

from __future__ import annotations


def advise_option_signal(stock_pick) -> dict:
    score = float(stock_pick.score or 0.0)
    day_change_pct = float(stock_pick.day_change_pct or 0.0)
    momentum_3_pct = float(stock_pick.momentum_3_pct or 0.0)
    amount_ratio_5 = float(stock_pick.amount_ratio_5 or 0.0)
    level = str(stock_pick.level or "").strip().upper()

    if (
        score >= 0.55
        and day_change_pct > 1.5
        and momentum_3_pct > 3
        and amount_ratio_5 > 1
        and level in {"A", "B"}
    ):
        return {
            "option_bias": "CALL",
            "option_horizon": "1-2w",
            "option_reason": "反弹启动 + 动量增强 + 量能配合",
            "option_risk": "若短线冲高后回落，时间价值损耗会放大",
        }

    if level == "C" or score < 0.45:
        return {
            "option_bias": "WATCH",
            "option_horizon": "-",
            "option_reason": "当前更适合观望",
            "option_risk": "信号较弱，不适合主动做期权",
        }

    return {
        "option_bias": "WATCH",
        "option_horizon": "-",
        "option_reason": "有反弹迹象，但期权入场确定性不足",
        "option_risk": "方向未充分确认，时间损耗风险较高",
    }
