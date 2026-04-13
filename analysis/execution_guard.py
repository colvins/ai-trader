"""Execution-layer opening confirmation rules for next-day decision support.

This module adds a lightweight confirmation layer after the prior-day signal
is generated. It does not change scoring, ranking, or backtest logic.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

try:
    from analysis.news_guard import NEGATIVE, NewsGuardDecision, evaluate_news_guard
except ModuleNotFoundError:  # pragma: no cover - allows direct script execution
    from news_guard import NEGATIVE, NewsGuardDecision, evaluate_news_guard


BUY_READY = "BUY_READY"
WATCH = "WATCH"
SKIP = "SKIP"


@dataclass
class ExecutionSignalInput:
    symbol: str
    run_date: str = ""
    level: str = ""
    action: str = ""
    option_bias: str = ""
    prev_close: float | None = None
    score: float | None = None
    raw: dict[str, Any] | None = None


@dataclass
class IntradayBar:
    timestamp: str = ""
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0


@dataclass
class ExecutionDecision:
    execution_status: str
    execution_reason: str
    metrics: dict[str, Any]


def build_execution_decision(
    execution_status: str,
    execution_reason: str,
    *,
    metrics: dict[str, Any] | None = None,
) -> ExecutionDecision:
    return ExecutionDecision(
        execution_status=execution_status,
        execution_reason=execution_reason,
        metrics=dict(metrics or {}),
    )


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _parse_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _normalize_signal(signal: ExecutionSignalInput | dict[str, Any]) -> ExecutionSignalInput:
    if isinstance(signal, ExecutionSignalInput):
        return signal
    data = dict(signal or {})
    return ExecutionSignalInput(
        symbol=str(data.get("symbol", "")).strip(),
        run_date=str(data.get("run_date", "")).strip(),
        level=str(data.get("level", "")).strip(),
        action=str(data.get("action", "")).strip(),
        option_bias=str(data.get("option_bias", "")).strip(),
        prev_close=_to_float(data.get("prev_close")),
        score=_to_float(data.get("score")),
        raw=data,
    )


def _normalize_bars(bars: list[IntradayBar | dict[str, Any]]) -> list[IntradayBar]:
    normalized: list[IntradayBar] = []
    for item in bars or []:
        if isinstance(item, IntradayBar):
            normalized.append(item)
            continue
        current = dict(item or {})
        normalized.append(
            IntradayBar(
                timestamp=str(current.get("timestamp", "") or current.get("datetime", "") or current.get("time", "")).strip(),
                open=_to_float(current.get("open")) or 0.0,
                high=_to_float(current.get("high")) or 0.0,
                low=_to_float(current.get("low")) or 0.0,
                close=_to_float(current.get("close")) or 0.0,
                volume=_to_float(current.get("volume")) or 0.0,
            )
        )
    normalized.sort(key=lambda bar: (_parse_timestamp(bar.timestamp) is None, _parse_timestamp(bar.timestamp) or datetime.min))
    return normalized


def is_execution_candidate(signal: ExecutionSignalInput | dict[str, Any]) -> bool:
    current = _normalize_signal(signal)
    return current.option_bias.upper() == "CALL" or current.level.upper() in {"A", "B"}


def evaluate_execution_guard(
    signal: ExecutionSignalInput | dict[str, Any],
    intraday_bars: list[IntradayBar | dict[str, Any]],
    *,
    window_minutes: int = 15,
) -> ExecutionDecision:
    """Evaluate opening confirmation using the first 10-15 minutes of data.

    The caller should pass bars that already cover the desired opening window,
    such as the first 10-15 one-minute bars or the first 2-3 five-minute bars.
    """

    current_signal = _normalize_signal(signal)
    bars = _normalize_bars(intraday_bars)

    if not is_execution_candidate(current_signal):
        return build_execution_decision(
            SKIP,
            "不属于 CALL 或 B级以上候选，暂不进入开盘确认买入规则。",
            metrics={"window_minutes": window_minutes, "bar_count": len(bars)},
        )

    if len(bars) < 2:
        return build_execution_decision(
            WATCH,
            "盘中数据不足，暂按观察处理。",
            metrics={"window_minutes": window_minutes, "bar_count": len(bars)},
        )

    open_price = bars[0].open
    current_price = bars[-1].close
    prev_close = current_signal.prev_close
    early_span = max(1, len(bars) // 3)
    initial_low = min(bar.low for bar in bars[:early_span])
    later_bars = bars[early_span:] or bars[-1:]
    later_low = min(bar.low for bar in later_bars)

    holds_initial_low = later_low >= initial_low * 0.997
    reclaims_open = current_price >= open_price

    if prev_close and prev_close > 0:
        gap_down = open_price < prev_close
        recovered_gap_ratio = 0.0
        if gap_down:
            recovered_gap_ratio = (current_price - open_price) / (prev_close - open_price) if prev_close > open_price else 0.0
        above_prev_close_or_recovered_gap = current_price >= prev_close or (gap_down and recovered_gap_ratio >= 0.6)
    else:
        gap_down = False
        recovered_gap_ratio = None
        above_prev_close_or_recovered_gap = current_price >= open_price

    first_half = bars[: max(1, len(bars) // 2)]
    second_half = bars[max(1, len(bars) // 2) :]
    first_half_volume = sum(bar.volume for bar in first_half)
    second_half_volume = sum(bar.volume for bar in second_half)
    volume_expanding = second_half_volume > first_half_volume * 1.1 if first_half_volume > 0 else False

    positive_conditions = {
        "holds_initial_low": holds_initial_low,
        "reclaims_open": reclaims_open,
        "above_prev_close_or_recovered_gap": above_prev_close_or_recovered_gap,
        "volume_expanding": volume_expanding,
    }
    positive_count = sum(1 for value in positive_conditions.values() if value)

    sustained_weakness = (
        (not holds_initial_low)
        and (not reclaims_open)
        and (
            prev_close is None
            or current_price < prev_close * 0.995
        )
    )

    metrics = {
        "window_minutes": window_minutes,
        "bar_count": len(bars),
        "open_price": round(open_price, 4),
        "current_price": round(current_price, 4),
        "prev_close": round(prev_close, 4) if prev_close is not None else None,
        "initial_low": round(initial_low, 4),
        "later_low": round(later_low, 4),
        "gap_down": gap_down,
        "recovered_gap_ratio": round(recovered_gap_ratio, 4) if recovered_gap_ratio is not None else None,
        "first_half_volume": round(first_half_volume, 4),
        "second_half_volume": round(second_half_volume, 4),
        "positive_count": positive_count,
        **positive_conditions,
    }

    if positive_count >= 3:
        return build_execution_decision(
            BUY_READY,
            "低开后企稳并收复开盘价，量能配合，短线确认成立。",
            metrics=metrics,
        )

    if sustained_weakness:
        return build_execution_decision(
            SKIP,
            "开盘后承接不足，短线确认失败。",
            metrics=metrics,
        )

    return build_execution_decision(
        WATCH,
        "仍在观察，方向确认不足。",
        metrics=metrics,
    )


def apply_news_veto(
    execution_decision: ExecutionDecision,
    news_decision: NewsGuardDecision,
) -> ExecutionDecision:
    if news_decision.news_risk_level != NEGATIVE:
        return execution_decision

    combined_metrics = {
        **execution_decision.metrics,
        "news_risk_level": news_decision.news_risk_level,
        "news_reason": news_decision.news_reason,
    }
    return build_execution_decision(
        SKIP,
        f"{execution_decision.execution_reason} 但消息风控触发否决：{news_decision.news_reason}",
        metrics=combined_metrics,
    )


def evaluate_execution_with_news_guard(
    signal: ExecutionSignalInput | dict[str, Any],
    intraday_bars: list[IntradayBar | dict[str, Any]],
    *,
    tdnet_titles: list[str] | str | None = None,
    news_titles: list[str] | str | None = None,
    window_minutes: int = 15,
) -> tuple[ExecutionDecision, NewsGuardDecision]:
    execution_decision = evaluate_execution_guard(
        signal,
        intraday_bars,
        window_minutes=window_minutes,
    )
    current_signal = _normalize_signal(signal)
    news_decision = evaluate_news_guard(
        current_signal.symbol,
        tdnet_titles=tdnet_titles,
        news_titles=news_titles,
    )
    return apply_news_veto(execution_decision, news_decision), news_decision


def demo_execution_guard() -> dict[str, Any]:
    """Small CLI-friendly example using synthetic opening bars."""

    signal = ExecutionSignalInput(
        symbol="7826",
        run_date="2026-04-10",
        level="B",
        action="watch",
        option_bias="CALL",
        prev_close=6810.0,
        score=0.62,
    )
    bars = [
        {"timestamp": "2026-04-11T09:00:00+09:00", "open": 6750, "high": 6760, "low": 6705, "close": 6725, "volume": 120000},
        {"timestamp": "2026-04-11T09:05:00+09:00", "open": 6725, "high": 6740, "low": 6710, "close": 6738, "volume": 98000},
        {"timestamp": "2026-04-11T09:10:00+09:00", "open": 6738, "high": 6790, "low": 6730, "close": 6782, "volume": 168000},
        {"timestamp": "2026-04-11T09:15:00+09:00", "open": 6782, "high": 6825, "low": 6775, "close": 6818, "volume": 220000},
    ]
    decision = evaluate_execution_guard(signal, bars, window_minutes=15)
    return {
        "signal": asdict(signal),
        "decision": asdict(decision),
    }


def demo_execution_with_news_guard() -> dict[str, Any]:
    signal = ExecutionSignalInput(
        symbol="7826",
        run_date="2026-04-10",
        level="B",
        action="watch",
        option_bias="CALL",
        prev_close=6810.0,
        score=0.62,
    )
    bars = [
        {"timestamp": "2026-04-11T09:00:00+09:00", "open": 6750, "high": 6760, "low": 6705, "close": 6725, "volume": 120000},
        {"timestamp": "2026-04-11T09:05:00+09:00", "open": 6725, "high": 6740, "low": 6710, "close": 6738, "volume": 98000},
        {"timestamp": "2026-04-11T09:10:00+09:00", "open": 6738, "high": 6790, "low": 6730, "close": 6782, "volume": 168000},
        {"timestamp": "2026-04-11T09:15:00+09:00", "open": 6782, "high": 6825, "low": 6775, "close": 6818, "volume": 220000},
    ]
    execution_decision, news_decision = evaluate_execution_with_news_guard(
        signal,
        bars,
        tdnet_titles=["業績予想の下方修正に関するお知らせ"],
        news_titles=["Broker downgrade follows earnings miss"],
        window_minutes=15,
    )
    return {
        "signal": asdict(signal),
        "execution_decision": asdict(execution_decision),
        "news_decision": asdict(news_decision),
    }


if __name__ == "__main__":
    import json

    print(json.dumps(demo_execution_with_news_guard(), ensure_ascii=False, indent=2))
