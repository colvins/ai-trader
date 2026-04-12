"""Builders that assemble structured pick results from engine and analysis outputs."""

from __future__ import annotations

from datetime import datetime, timezone

from analysis.option_advisor import advise_option_signal
from analysis.prompt_builder import build_ai_prompt
from reporting.schemas import DataStatus, MarketState, NewsItem, PickResult, StockPick


def build_data_status(status: dict) -> DataStatus:
    return DataStatus(
        ok=bool(status.get("ok")),
        title=str(status.get("title", "")),
        text=str(status.get("text", "")),
        data_date=str(dict(status.get("raw", {})).get("date", "")),
        raw=dict(status.get("raw", {})),
    )


def build_market_state(market_state: dict | None) -> MarketState:
    market_state = market_state or {}
    return MarketState(
        state=str(market_state.get("state", "")),
        mode=str(market_state.get("mode", "")),
        up_ratio=float(market_state.get("up_ratio") or 0.0),
        avg_change_pct=float(market_state.get("avg_change_pct") or 0.0),
        total=int(market_state.get("total") or 0),
        data_date=str(market_state.get("data_date", "")),
    )


def _resolve_level_and_action(merged: dict) -> tuple[str, str]:
    score = float(merged.get("score") or 0.0)
    dist_to_high_20_pct = float(merged.get("dist_to_high_20_pct") or 0.0)
    amount_ratio_5 = float(merged.get("amount_ratio_5") or 0.0)

    if score > 0.6 and dist_to_high_20_pct < -10 and amount_ratio_5 > 1:
        return "A", "buy"
    if score > 0.5:
        return "B", "watch"
    return "C", "ignore"


def build_stock_pick(base: dict, scored: dict, news_items: list[dict], mode: str) -> StockPick:
    merged = {
        **dict(base or {}),
        **dict(scored or {}),
        "news_items": list(news_items or []),
    }
    merged["ai_prompt"] = build_ai_prompt(merged, mode=mode)
    level, action = _resolve_level_and_action(merged)

    normalized_news = [
        NewsItem(
            title=str(item.get("title", "")),
            link=str(item.get("link", "")),
            summary=str(item.get("summary", "")),
            source=str(item.get("source", "")),
            published_at=str(item.get("published_at", "")),
            relevance=item.get("relevance"),
        )
        for item in news_items or []
    ]

    pick = StockPick(
        symbol=str(merged.get("symbol", "")),
        close=merged.get("close"),
        prev_close=merged.get("prev_close"),
        score=merged.get("score"),
        reason=str(merged.get("reason", "")),
        mode=str(merged.get("mode", mode)),
        level=level,
        action=action,
        ai_prompt=str(merged.get("ai_prompt", "")),
        day_change_pct=merged.get("day_change_pct"),
        intraday_pct=merged.get("intraday_pct"),
        amplitude_pct=merged.get("amplitude_pct"),
        amount_ratio_5=merged.get("amount_ratio_5"),
        momentum_3_pct=merged.get("momentum_3_pct"),
        momentum_5_pct=merged.get("momentum_5_pct"),
        dist_to_high_5_pct=merged.get("dist_to_high_5_pct"),
        dist_to_high_20_pct=merged.get("dist_to_high_20_pct"),
        close_position=merged.get("close_position"),
        tech_score=merged.get("tech_score"),
        news_score=merged.get("news_score"),
        bias_score=merged.get("bias_score"),
        tech_parts=list(merged.get("tech_parts", []) or []),
        news_parts=list(merged.get("news_parts", []) or []),
        news_items=normalized_news,
        raw=merged,
    )
    option_view = advise_option_signal(pick)
    pick.option_bias = option_view["option_bias"]
    pick.option_horizon = option_view["option_horizon"]
    pick.option_reason = option_view["option_reason"]
    pick.option_risk = option_view["option_risk"]
    return pick


def build_pick_result(
    *,
    mode: str,
    status: dict,
    candidates: list[dict],
    scored: list[dict],
    news_map: dict[str, list[dict]],
    limit: int,
    candidate_limit: int | None = None,
    market_state: dict | None = None,
    mode_source: str = "manual",
) -> PickResult:
    candidate_map = {str(item.get("symbol", "")).strip(): item for item in candidates}
    picks = []

    for item in scored[:limit]:
        symbol = str(item.get("symbol", "")).strip()
        picks.append(
            build_stock_pick(
                base=candidate_map.get(symbol, {}),
                scored=item,
                news_items=news_map.get(symbol, []),
                mode=mode,
            )
        )

    return PickResult(
        mode=mode,
        status=build_data_status(status),
        market_state=build_market_state(market_state),
        mode_source=mode_source,
        picks=picks,
        candidate_count=len(candidates),
        scored_count=len(scored),
        candidate_limit=int(candidate_limit or len(candidates)),
        limit=limit,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )
