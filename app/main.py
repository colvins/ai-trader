from __future__ import annotations

import argparse

from app.services.ai_service import DEFAULT_MODE, SUPPORTED_MODES
from app.services.notify_service import send_telegram_message, telegram_configured
from app.services.picker import run_picker_result
from reuters_source import fetch_reuters_news
from reporting.formatters.console_formatter import format_pick_result as format_console_pick_result


def build_pick_message(result) -> str:
    return format_console_pick_result(result)


def _build_console_output(result) -> str:
    text = format_console_pick_result(result)
    try:
        reuters_items = fetch_reuters_news(limit=1)
    except Exception:
        reuters_items = []
    latest_news_title = str(reuters_items[0].get("title", "")).strip() if reuters_items else ""

    ordered_picks = []
    for level in ("A", "B", "C"):
        ordered_picks.extend([pick for pick in result.picks if pick.level == level])

    lines = text.splitlines()
    output = []
    pick_index = 0

    for line in lines:
        output.append(line)
        if pick_index >= len(ordered_picks):
            continue
        if line[:1].isdigit() and " | score=" in line:
            signal = ordered_picks[pick_index].raw.get("tdnet_signal", "无")
            output.append(f"   TDnet: {signal}")
            output.append(f"   News: {latest_news_title or '-'}")
            pick_index += 1

    return "\n".join(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="运行选股并显示/可选推送结果")
    parser.add_argument("--push", action="store_true", help="将数据状态和 pick 结果推送到 Telegram")
    parser.add_argument(
        "--mode",
        default=None,
        choices=sorted(SUPPORTED_MODES),
        help="选股模式：breakout=短线打板/追涨，trend=趋势跟随，dip=低吸反弹；不传则自动选择",
    )
    parser.add_argument("--limit", type=int, default=5, help="最终输出推荐数量")
    parser.add_argument("--candidate-limit", type=int, default=30, help="初筛股票数量")
    args = parser.parse_args()

    result = run_picker_result(limit=args.limit, candidate_limit=args.candidate_limit, mode=args.mode)
    print(_build_console_output(result))

    if args.push:
        msg = build_pick_message(result)
        ok, err = send_telegram_message(msg)
        print("Telegram 推送成功" if ok else f"推送失败: {err}")
        if (not ok) and (not telegram_configured()):
            print("请先加载 .env，或在定时任务中先 source /opt/ai-trader/.env")
