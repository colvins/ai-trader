"""Compatibility layer for outbound notifications; later removable."""

from __future__ import annotations

from delivery.notify_service import send_telegram_message, telegram_configured


__all__ = ["send_telegram_message", "telegram_configured"]
