"""Telegram-oriented formatting for PickResult."""

from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timezone

from reporting.schemas import PickResult


MODE_LABELS = {
    "breakout": "🔥 短线打板 / 追涨",
    "trend": "📈 趋势跟随",
    "dip": "🧲 低吸反弹",
}

ACTION_LABELS = {
    "buy": "买入",
    "watch": "观察",
    "ignore": "忽略",
}


def _signal_repeat_tag(raw: dict) -> str:
    consecutive_days = int(raw.get("consecutive_days", 1) or 1)
    if consecutive_days <= 1:
        return "新"
    return f"连{consecutive_days}"


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


def _pick_news_summary(pick) -> str:
    news_items = getattr(pick, "news_items", []) or []
    for item in news_items:
        title = str(getattr(item, "title", "") or "").strip()
        if not title:
            continue
        source = str(getattr(item, "source", "") or "").strip() or "新闻"
        published_at = str(getattr(item, "published_at", "") or "").strip()
        relative_time = _format_relative_time_text(published_at)
        if relative_time:
            return f"{source}｜{title}｜{relative_time}"
        return f"{source}｜{title}"

    raw = getattr(pick, "raw", {}) or {}
    tdnet_title = str(raw.get("tdnet_title", "") or "").strip()
    if tdnet_title:
        first_title = tdnet_title.split("|", 1)[0].strip()
        if first_title:
            return f"TDnet公告｜{first_title}"

    return "无近期有效新闻"


def format_pick_result(result: PickResult) -> str:
    mode_text = MODE_LABELS.get(result.mode, result.mode)
    market_state = result.market_state
    up_ratio_pct = round(market_state.up_ratio * 100.0, 1)
    lines = [
        "📌 交易决策提示",
        f"市场状态: {market_state.state or '-'} | 上涨占比: {up_ratio_pct}% | 平均涨幅: {market_state.avg_change_pct}%",
        f"策略类型: {mode_text}",
        f"策略来源: {'自动选择' if result.mode_source == 'auto' else '手动指定'}",
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
        lines.append("A级（买入）")
        for pick in a_picks:
            repeat_tag = _signal_repeat_tag(pick.raw)
            lines.append(
                f"- {pick.symbol}（{repeat_tag}） | ¥{pick.close} | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
            )
            lines.append(f"  新闻：{_pick_news_summary(pick)}")
            lines.append(
                f"  期权方向: {pick.option_bias or '暂无'} | 参考周期: {pick.option_horizon or '暂无'}"
            )
            lines.append(
                f"  期权逻辑: {pick.option_reason or '暂无'} | 主要风险: {pick.option_risk or '暂无'}"
            )
            lines.append(
                f"  公告信号: {str(pick.raw.get('tdnet_signal', '') or '暂无')} | 公告标题: {str(pick.raw.get('tdnet_title', '') or '暂无')}"
            )
        lines.append("")

    if b_picks:
        lines.append("B级（观察）")
        for pick in b_picks:
            repeat_tag = _signal_repeat_tag(pick.raw)
            lines.append(
                f"- {pick.symbol}（{repeat_tag}） | ¥{pick.close} | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
            )
            lines.append(f"  新闻：{_pick_news_summary(pick)}")
            lines.append(
                f"  期权方向: {pick.option_bias or '暂无'} | 参考周期: {pick.option_horizon or '暂无'}"
            )
            lines.append(
                f"  期权逻辑: {pick.option_reason or '暂无'} | 主要风险: {pick.option_risk or '暂无'}"
            )
            lines.append(
                f"  公告信号: {str(pick.raw.get('tdnet_signal', '') or '暂无')} | 公告标题: {str(pick.raw.get('tdnet_title', '') or '暂无')}"
            )
        lines.append("")

    lines.append(f"C级（忽略）数量: {c_count}")

    return "\n".join(lines)


def format_ai_prompt(result: PickResult) -> str:
    if not result.picks:
        return "当前没有可生成的 AI 分析输入。"

    first = result.picks[0]
    if first.ai_prompt:
        return first.ai_prompt
    return "当前结果没有 AI 分析输入。"
