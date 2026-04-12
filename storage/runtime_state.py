"""Runtime state readers shared by picker and delivery layers."""

from __future__ import annotations

import json
from pathlib import Path


RUNTIME_DIR = Path("data/runtime")
SUMMARY_FILE = RUNTIME_DIR / "update_summary.json"
BOT_MODE_FILE = RUNTIME_DIR / "bot_mode.json"


def load_update_summary() -> dict:
    if not SUMMARY_FILE.exists():
        return {}
    try:
        return json.loads(SUMMARY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def get_data_status_summary() -> dict:
    data = load_update_summary()
    if not data:
        return {
            "ok": False,
            "title": "⚠️ 数据状态",
            "text": "未找到 update_summary.json，当前无法确认行情数据是否已成功更新。",
            "raw": {},
        }

    status = str(data.get("status", "")).strip().lower()
    date = data.get("date", "-")
    mode_days = data.get("mode_days", "-")
    total = data.get("total", "-")
    success = data.get("success", "-")
    skip = data.get("skip", "-")
    fail = data.get("fail", "-")
    duration_seconds = data.get("duration_seconds", 0)
    message = data.get("message", "") or "无"

    try:
        duration_minutes = round(float(duration_seconds) / 60.0, 1)
        duration_text = f"{duration_minutes} 分钟"
    except Exception:
        duration_text = str(duration_seconds)

    if status in {"ok", "fresh", "success"}:
        title = "✅ 数据状态"
        intro = "当前 pick 基于今日最新更新数据。"
        ok = True
    elif status in {"warn", "warning", "partial"}:
        title = "⚠️ 数据状态"
        intro = "今日数据部分更新成功，pick 结果可参考，但建议留意数据完整性。"
        ok = True
    else:
        title = "❌ 数据状态"
        intro = "今日数据更新状态异常，pick 结果可能不可靠。"
        ok = False

    text = "\n".join(
        [
            intro,
            f"日期: {date}",
            f"更新窗口: {mode_days} 天",
            f"股票总数: {total}",
            f"成功: {success}",
            f"跳过: {skip}",
            f"失败: {fail}",
            f"耗时: {duration_text}",
            f"备注: {message}",
        ]
    )

    return {
        "ok": ok,
        "title": title,
        "text": text,
        "raw": data,
    }


def load_bot_mode_state() -> dict:
    if not BOT_MODE_FILE.exists():
        return {"mode": "auto"}
    try:
        return json.loads(BOT_MODE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"mode": "auto"}


def save_bot_mode_state(mode: str) -> dict:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    state = {"mode": str(mode or "auto").strip().lower() or "auto"}
    BOT_MODE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return state


def get_bot_mode() -> str:
    state = load_bot_mode_state()
    mode = str(state.get("mode", "auto")).strip().lower()
    if mode not in {"auto", "dip", "trend", "breakout"}:
        return "auto"
    return mode
