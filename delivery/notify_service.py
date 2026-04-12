"""Outbound delivery helpers. Currently Telegram only."""

from __future__ import annotations

import os
import re

import requests


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()


def telegram_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def _compress_pick_message_for_telegram(text: str) -> str:
    raw = str(text or "")
    if "📌 Pick 结果" not in raw:
        return raw

    lines = [line.rstrip() for line in raw.splitlines()]
    strategy_line = ""
    market_line = ""
    source_line = ""
    warning = ""
    groups: dict[str, list[str]] = {"A": [], "B": [], "C": []}
    current_group = None

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("策略模式:"):
            strategy_line = stripped
        elif stripped.startswith("市场状态:"):
            market_line = stripped
        elif stripped.startswith("策略来源:"):
            source_line = stripped
        elif stripped == "[A级]":
            current_group = "A"
        elif stripped == "[B级]":
            current_group = "B"
        elif stripped == "[C级]":
            current_group = "C"
        elif re.match(r"^\d+\.\s", stripped) and current_group in groups:
            groups[current_group].append(re.sub(r"^\d+\.\s*", "", stripped))

    out = ["📌 交易决策提示"]
    if market_line:
        out.append(market_line)
    if strategy_line or source_line:
        strategy_text = strategy_line.replace("策略模式:", "当前策略:").strip() if strategy_line else "当前策略: -"
        if source_line:
            strategy_text = f"{strategy_text} | {source_line.replace('策略来源:', '').strip()}"
        out.append(strategy_text)
    out.append("")

    if not groups["A"]:
        warning = "⚠️ 今日无明确买点，建议观望"
        out.append(warning)
        out.append("")

    if groups["A"]:
        out.append("A级（buy）")
        for item in groups["A"]:
            out.append(f"- {item}")
        out.append("")

    if groups["B"]:
        out.append("B级（watch）")
        for item in groups["B"][:3]:
            out.append(f"- {item}")
        out.append("")

    out.append(f"C级（ignore）数量: {len(groups['C'])}")
    return "\n".join(out)


def send_telegram_message(text: str, parse_mode: str | None = None) -> tuple[bool, str]:
    if not telegram_configured():
        return False, "未配置 TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID"

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": _compress_pick_message_for_telegram(text),
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        response = requests.post(url, json=payload, timeout=15)
        if response.ok:
            return True, "ok"
        return False, f"HTTP {response.status_code}: {response.text[:300]}"
    except Exception as exc:
        return False, str(exc)
