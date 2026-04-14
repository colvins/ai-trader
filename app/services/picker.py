"""Compatibility orchestration layer; now delegates to new modules."""

from __future__ import annotations

from analysis.local_ranker import DEFAULT_MODE, SUPPORTED_MODES
from analysis.local_ranker import analyze_stocks
from analysis.news_service import get_news_for_stocks
from engine.market_state import analyze_market_state
from engine.picker_core import get_candidate_stocks
from reporting.builders import build_pick_result
from storage.signal_store import save_pick_result_signals
from storage.runtime_state import get_data_status_summary


SCAN_MODE_SOURCE = "scan"
SCAN_MODES = ("dip", "trend", "breakout")


def _build_result(
    limit=5,
    candidate_limit=30,
    mode=DEFAULT_MODE,
    mode_source_override: str | None = None,
    *,
    status=None,
    market_state=None,
    candidates: list[dict] | None = None,
    news_map: dict | None = None,
):
    status = status or get_data_status_summary()
    requested_mode = str(mode).strip().lower() if mode is not None else ""
    market_state = market_state or analyze_market_state()
    resolved_mode = requested_mode or market_state.get("mode", DEFAULT_MODE)
    mode_source = mode_source_override or ("manual" if requested_mode else "auto")

    candidates = candidates if candidates is not None else get_candidate_stocks(limit=candidate_limit, mode=resolved_mode)
    if not candidates:
        return build_pick_result(
            mode=resolved_mode,
            status=status,
            candidates=[],
            scored=[],
            news_map={},
            limit=limit,
            candidate_limit=candidate_limit,
            market_state=market_state,
            mode_source=mode_source,
        )

    news_map = news_map if news_map is not None else get_news_for_stocks(candidates, max_items=5)
    scored = analyze_stocks(candidates, news_map=news_map, mode=resolved_mode)
    return build_pick_result(
        mode=resolved_mode,
        status=status,
        candidates=candidates,
        scored=scored,
        news_map=news_map,
        limit=limit,
        candidate_limit=candidate_limit,
        market_state=market_state,
        mode_source=mode_source,
    )


def _pick_to_legacy_dict(pick):
    return {
        **pick.raw,
        "symbol": pick.symbol,
        "close": pick.close,
        "prev_close": pick.prev_close,
        "score": pick.score,
        "reason": pick.reason,
        "mode": pick.mode,
        "option_bias": pick.option_bias,
        "option_horizon": pick.option_horizon,
        "option_reason": pick.option_reason,
        "option_risk": pick.option_risk,
        "ai_prompt": pick.ai_prompt,
        "day_change_pct": pick.day_change_pct,
        "intraday_pct": pick.intraday_pct,
        "amplitude_pct": pick.amplitude_pct,
        "amount_ratio_5": pick.amount_ratio_5,
        "momentum_3_pct": pick.momentum_3_pct,
        "momentum_5_pct": pick.momentum_5_pct,
        "dist_to_high_5_pct": pick.dist_to_high_5_pct,
        "dist_to_high_20_pct": pick.dist_to_high_20_pct,
        "close_position": pick.close_position,
        "tech_score": pick.tech_score,
        "news_score": pick.news_score,
        "bias_score": pick.bias_score,
        "tech_parts": list(pick.tech_parts),
        "news_parts": list(pick.news_parts),
        "news_items": [item.__dict__ for item in pick.news_items],
    }


def run_picker_result(limit=5, candidate_limit=30, mode=DEFAULT_MODE, mode_source_override: str | None = None):
    result = _build_result(
        limit=limit,
        candidate_limit=candidate_limit,
        mode=mode,
        mode_source_override=mode_source_override,
    )
    save_pick_result_signals(result)
    return result


def run_multi_mode_scan_results(limit=5, candidate_limit=30, modes: tuple[str, ...] = SCAN_MODES):
    valid_modes = [mode for mode in modes if mode in SUPPORTED_MODES]
    if not valid_modes:
        return []

    status = get_data_status_summary()
    market_state = analyze_market_state()
    candidates_by_mode: dict[str, list[dict]] = {}
    merged_candidates: dict[str, dict] = {}

    for mode in valid_modes:
        candidates = get_candidate_stocks(limit=candidate_limit, mode=mode)
        candidates_by_mode[mode] = candidates
        for candidate in candidates:
            symbol = str(candidate.get("symbol", "") or "").strip()
            if symbol and symbol not in merged_candidates:
                merged_candidates[symbol] = candidate

    shared_news_map = (
        get_news_for_stocks(list(merged_candidates.values()), max_items=5)
        if merged_candidates
        else {}
    )

    results = []
    for mode in valid_modes:
        result = _build_result(
                limit=limit,
                candidate_limit=candidate_limit,
                mode=mode,
                mode_source_override=SCAN_MODE_SOURCE,
                status=status,
                market_state=market_state,
                candidates=candidates_by_mode.get(mode, []),
                news_map=shared_news_map,
            )
        save_pick_result_signals(result)
        results.append(result)
    return results


def build_pick_result_payload(limit=5, candidate_limit=30, mode=DEFAULT_MODE):
    return run_picker_result(limit=limit, candidate_limit=candidate_limit, mode=mode)


def pick_stocks(limit=5, candidate_limit=30, mode=DEFAULT_MODE):
    print("开始选股流程...")
    result = _build_result(limit=limit, candidate_limit=candidate_limit, mode=mode)
    print(f"初筛股票数量: {result.candidate_count}")
    return [_pick_to_legacy_dict(pick) for pick in result.picks]


def run_picker(limit=5, candidate_limit=30, mode=DEFAULT_MODE):
    """Legacy adapter returning list[dict]; prefer run_picker_result()."""
    result = _build_result(limit=limit, candidate_limit=candidate_limit, mode=mode)
    return [_pick_to_legacy_dict(pick) for pick in result.picks]


def run_picker_legacy_dicts(limit=5, candidate_limit=30, mode=DEFAULT_MODE):
    """Explicit legacy adapter returning list[dict]."""
    return run_picker(limit=limit, candidate_limit=candidate_limit, mode=mode)


__all__ = [
    "build_pick_result_payload",
    "get_data_status_summary",
    "pick_stocks",
    "run_picker",
    "run_picker_legacy_dicts",
    "run_multi_mode_scan_results",
    "run_picker_result",
]
