"""Intraday opening data loader backed by yfinance."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

from analysis.execution_guard import IntradayBar


JST = ZoneInfo("Asia/Tokyo")
MARKET_OPEN = time(9, 0)
CONFIRMATION_MIN_MINUTES = 10
DEFAULT_WINDOW_MINUTES = 15


@dataclass
class IntradayFetchResult:
    symbol: str
    ticker: str
    target_date: str
    bars: list[IntradayBar]
    interval: str = ""
    fetch_reason: str = ""
    used_live_data: bool = False


def to_yfinance_ticker(symbol: str) -> str:
    text = str(symbol or "").strip()
    if not text:
        return ""
    return text if "." in text else f"{text}.T"


def _normalize_target_date(value: str | date | datetime | None) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return datetime.now(JST).date()
    try:
        return datetime.fromisoformat(text[:10]).date()
    except Exception:
        return datetime.now(JST).date()


def _normalize_index_timezone(index: pd.Index) -> pd.DatetimeIndex:
    dt_index = pd.DatetimeIndex(index)
    if dt_index.tz is None:
        return dt_index.tz_localize(JST)
    return dt_index.tz_convert(JST)


def _window_end_for_date(target_date: date, now: datetime, window_minutes: int) -> datetime | None:
    open_dt = datetime.combine(target_date, MARKET_OPEN, JST)
    min_ready_dt = open_dt + timedelta(minutes=CONFIRMATION_MIN_MINUTES)
    full_window_dt = open_dt + timedelta(minutes=window_minutes)

    if target_date > now.date():
        return None
    if target_date == now.date():
        if now < open_dt:
            return None
        if now < min_ready_dt:
            return None
        return min(now, full_window_dt)
    return full_window_dt


def fetch_opening_intraday_bars(
    symbol: str,
    *,
    target_date: str | date | datetime | None = None,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    now: datetime | None = None,
) -> IntradayFetchResult:
    current_now = now.astimezone(JST) if now else datetime.now(JST)
    target = _normalize_target_date(target_date)
    ticker = to_yfinance_ticker(symbol)
    window_end = _window_end_for_date(target, current_now, window_minutes)

    if not ticker:
        return IntradayFetchResult(
            symbol=str(symbol or ""),
            ticker="",
            target_date=target.isoformat(),
            bars=[],
            fetch_reason="股票代码无效，无法获取盘中数据。",
        )

    if window_end is None:
        open_dt = datetime.combine(target, MARKET_OPEN, JST)
        if target == current_now.date() and current_now < open_dt:
            reason = "当前未到开盘时段，暂无法做开盘确认。"
        elif target == current_now.date():
            reason = "当前未到开盘确认时段，暂按观察处理。"
        else:
            reason = "目标日期尚未进入可确认时段，暂按观察处理。"
        return IntradayFetchResult(
            symbol=str(symbol or ""),
            ticker=ticker,
            target_date=target.isoformat(),
            bars=[],
            fetch_reason=reason,
        )

    start_dt = datetime.combine(target, MARKET_OPEN, JST)
    fetch_start = target.isoformat()
    fetch_end = (target + timedelta(days=1)).isoformat()

    for interval in ("1m", "5m"):
        try:
            history = yf.Ticker(ticker).history(
                interval=interval,
                start=fetch_start,
                end=fetch_end,
                auto_adjust=False,
                actions=False,
                prepost=False,
            )
        except Exception:
            history = pd.DataFrame()

        if history.empty:
            continue

        history = history.copy()
        history.index = _normalize_index_timezone(history.index)
        history = history[(history.index >= start_dt) & (history.index <= window_end)]
        if history.empty:
            continue

        bars: list[IntradayBar] = []
        for timestamp, row in history.iterrows():
            open_price = row.get("Open")
            high_price = row.get("High")
            low_price = row.get("Low")
            close_price = row.get("Close")
            volume = row.get("Volume")
            if any(pd.isna(value) for value in [open_price, high_price, low_price, close_price, volume]):
                continue
            bars.append(
                IntradayBar(
                    timestamp=timestamp.isoformat(),
                    open=float(open_price),
                    high=float(high_price),
                    low=float(low_price),
                    close=float(close_price),
                    volume=float(volume),
                )
            )

        if bars:
            return IntradayFetchResult(
                symbol=str(symbol or ""),
                ticker=ticker,
                target_date=target.isoformat(),
                bars=bars,
                interval=interval,
                fetch_reason=f"已使用 yfinance {interval} 盘中数据进行开盘确认。",
                used_live_data=True,
            )

    return IntradayFetchResult(
        symbol=str(symbol or ""),
        ticker=ticker,
        target_date=target.isoformat(),
        bars=[],
        fetch_reason="yfinance 盘中数据不足，暂按观察处理。",
    )


def demo_intraday_fetch_result() -> dict[str, Any]:
    result = fetch_opening_intraday_bars("7826")
    return asdict(result)
