from __future__ import annotations

import argparse
from collections import OrderedDict
from datetime import date

import pandas as pd

from app.services.ai_service import DEFAULT_MODE, SUPPORTED_MODES
from app.services.notify_service import send_telegram_message, telegram_configured
from app.services.picker import run_multi_mode_scan_results, run_picker_result
from reuters_source import fetch_reuters_news
from reporting.formatters.console_formatter import format_pick_result as format_console_pick_result
from reporting.formatters.telegram_formatter import format_pick_result as format_telegram_pick_result
from reporting.schemas import DataStatus, MarketState, PickResult, StockPick
from storage.backtest_store import load_signals


MODE_LABELS = {
    "dip": "低吸反弹",
    "trend": "趋势跟随",
    "breakout": "短线打板 / 追涨",
}
ACTION_LABELS = {
    "buy": "买入",
    "watch": "观察",
    "ignore": "忽略",
}
SCAN_MODES = ("dip", "trend", "breakout")
TELEGRAM_MULTI_MODE_MIN_SCORE = 0.52
TELEGRAM_MULTI_MODE_MAX_PICKS = 3

_REUTERS_NEWS_TITLE_CACHE: str | None = None


def _signal_repeat_tag(raw: dict) -> str:
    consecutive_days = int((raw or {}).get("consecutive_days", 1) or 1)
    if consecutive_days <= 1:
        return "新"
    return f"连{consecutive_days}"


def _stored_pick_reason(row: dict) -> str:
    tdnet_signal = str(row.get("tdnet_signal", "") or "").strip()
    option_reason = str(row.get("option_reason", "") or "").strip()
    if tdnet_signal and tdnet_signal != "无":
        return f"公告信号：{tdnet_signal}"
    if option_reason:
        return option_reason
    return "已落盘信号"


def _build_pick_from_row(row: dict) -> StockPick:
    raw = dict(row)
    return StockPick(
        symbol=str(row.get("symbol", "") or "").strip(),
        close=row.get("close"),
        prev_close=row.get("prev_close"),
        score=row.get("score"),
        reason=_stored_pick_reason(row),
        mode=str(row.get("selected_mode", "") or "").strip(),
        level=str(row.get("level", "C") or "C").strip(),
        action=str(row.get("action", "ignore") or "ignore").strip(),
        option_bias=str(row.get("option_bias", "") or "").strip(),
        option_horizon=str(row.get("option_horizon", "") or "").strip(),
        option_reason=str(row.get("option_reason", "") or "").strip(),
        option_risk=str(row.get("option_risk", "") or "").strip(),
        day_change_pct=row.get("day_change_pct"),
        intraday_pct=row.get("intraday_pct"),
        amplitude_pct=row.get("amplitude_pct"),
        amount_ratio_5=row.get("amount_ratio_5"),
        momentum_3_pct=row.get("momentum_3_pct"),
        momentum_5_pct=row.get("momentum_5_pct"),
        dist_to_high_5_pct=row.get("dist_to_high_5_pct"),
        dist_to_high_20_pct=row.get("dist_to_high_20_pct"),
        close_position=row.get("close_position"),
        raw=raw,
    )


def _build_result_from_rows(mode: str, run_date: str, mode_source: str, frame: pd.DataFrame) -> PickResult:
    rows = frame.sort_values(["rank", "score", "symbol"], ascending=[True, False, True], na_position="last")
    first = rows.iloc[0].to_dict() if not rows.empty else {}
    picks = [_build_pick_from_row(row) for row in rows.to_dict(orient="records")]
    market_state = MarketState(
        state=str(first.get("market_state", "") or "").strip(),
        up_ratio=float(first.get("market_up_ratio") or 0.0),
        avg_change_pct=float(first.get("market_avg_change_pct") or 0.0),
        data_date=run_date,
    )
    return PickResult(
        mode=mode,
        status=DataStatus(
            ok=True,
            title="",
            text="",
            data_date=run_date,
            raw={"source": "data/backtest/signals.csv", "strategy_source": mode_source},
        ),
        market_state=market_state,
        mode_source=mode_source,
        picks=picks,
        candidate_count=len(picks),
        scored_count=len(picks),
        candidate_limit=len(picks),
        limit=len(picks),
        generated_at=str(first.get("generated_at", "") or ""),
    )


def _load_today_signals_df() -> pd.DataFrame:
    df = load_signals()
    if df.empty:
        return pd.DataFrame()

    current = df.copy()
    current["run_date"] = current["run_date"].astype(str).str.strip()
    today = date.today().isoformat()
    current = current[current["run_date"] == today].copy()
    if current.empty:
        return pd.DataFrame()

    current["selected_mode"] = current["selected_mode"].astype(str).str.strip().str.lower()
    current["strategy_source"] = current["strategy_source"].astype(str).str.strip().str.lower()
    if "generated_at" in current.columns:
        current["generated_at"] = pd.to_datetime(current["generated_at"], errors="coerce")
    return current


def _load_stored_scan_results_for_today() -> list[PickResult]:
    current = _load_today_signals_df()
    if current.empty:
        return []

    current = current[current["strategy_source"] == "scan"].copy()
    if current.empty:
        return []

    modes_present = set(current["selected_mode"].dropna().tolist())
    if not all(mode in modes_present for mode in SCAN_MODES):
        return []

    results = []
    for mode in SCAN_MODES:
        mode_frame = current[current["selected_mode"] == mode].copy()
        results.append(_build_result_from_rows(mode, date.today().isoformat(), "scan", mode_frame))
    return results


def _load_stored_single_mode_result_for_today(mode: str) -> PickResult | None:
    current = _load_today_signals_df()
    if current.empty:
        return None

    mode_key = str(mode).strip().lower()
    current = current[current["selected_mode"] == mode_key].copy()
    if current.empty:
        return None

    strategy_priority = {"manual": 0, "auto": 1, "scan": 2}
    current["_strategy_priority"] = current["strategy_source"].map(strategy_priority).fillna(9)
    current = current.sort_values(
        ["_strategy_priority", "generated_at", "rank", "symbol"],
        ascending=[True, False, True, True],
        na_position="last",
    )
    chosen_source = str(current.iloc[0]["strategy_source"] or "manual").strip()
    chosen_frame = current[current["strategy_source"] == chosen_source].copy()
    chosen_frame = chosen_frame.drop(columns=["_strategy_priority"], errors="ignore")
    return _build_result_from_rows(mode_key, date.today().isoformat(), chosen_source, chosen_frame)


def _get_latest_reuters_news_title() -> str:
    global _REUTERS_NEWS_TITLE_CACHE
    if _REUTERS_NEWS_TITLE_CACHE is not None:
        return _REUTERS_NEWS_TITLE_CACHE

    try:
        reuters_items = fetch_reuters_news(limit=1)
    except Exception:
        reuters_items = []
    _REUTERS_NEWS_TITLE_CACHE = str(reuters_items[0].get("title", "")).strip() if reuters_items else ""
    return _REUTERS_NEWS_TITLE_CACHE


def build_pick_message(result) -> str:
    return format_telegram_pick_result(result)


def _filter_multi_mode_display_picks(picks, *, min_score=TELEGRAM_MULTI_MODE_MIN_SCORE, max_picks=TELEGRAM_MULTI_MODE_MAX_PICKS):
    filtered = []
    for pick in picks:
        score = float(pick.score or 0.0)
        if score < min_score:
            continue
        filtered.append(pick)
        if len(filtered) >= max_picks:
            break
    return filtered


def build_multi_mode_push_message(results) -> str:
    lines = ["📌 今日三模式系统扫描结果", ""]

    for index, result in enumerate(results):
        mode_text = MODE_LABELS.get(result.mode, result.mode)
        groups = OrderedDict([("A", []), ("B", []), ("C", [])])
        display_picks = _filter_multi_mode_display_picks(result.picks)
        for pick in display_picks:
            groups.setdefault(pick.level, []).append(pick)

        lines.append(f"【{mode_text}】")
        lines.append(f"市场状态: {result.market_state.state or '-'} | 扫描结果")

        if not display_picks:
            lines.append("- 暂无候选")
        else:
            for level in ("A", "B", "C"):
                picks = groups.get(level, [])
                if not picks:
                    continue
                if level == "C":
                    lines.append(f"- C级数量: {len(picks)}")
                    continue
                for pick in picks:
                    repeat_tag = _signal_repeat_tag(getattr(pick, "raw", {}) or {})
                    lines.append(
                        f"- {level}级 | {pick.symbol}（{repeat_tag}） | 得分={pick.score} | 操作建议={ACTION_LABELS.get(pick.action, pick.action)} | {pick.reason}"
                    )
        if index != len(results) - 1:
            lines.append("")

    return "\n".join(lines)


def _resolve_cli_strategy_source_label(*, reused_from_signals: bool, reran: bool, requested_mode: str | None, result: PickResult) -> str:
    mode_key = str(requested_mode or "").strip().lower()
    if reused_from_signals:
        if not mode_key and str(result.mode_source or "").strip().lower() == "scan":
            return "scan 结果复用"
        return "当日已落盘信号"

    if reran:
        if mode_key:
            return "手动指定"
        return "实时计算"

    return "实时计算"


def _build_console_output(result, *, strategy_source_label: str | None = None) -> str:
    text = format_console_pick_result(result)
    latest_news_title = _get_latest_reuters_news_title()

    ordered_picks = []
    for level in ("A", "B", "C"):
        ordered_picks.extend([pick for pick in result.picks if pick.level == level])

    lines = text.splitlines()
    output = []
    pick_index = 0

    for line in lines:
        if strategy_source_label and line.startswith("策略来源: "):
            output.append(f"策略来源: {strategy_source_label}")
            continue
        output.append(line)
        if pick_index >= len(ordered_picks):
            continue
        if line[:1].isdigit() and " | score=" in line:
            signal = ordered_picks[pick_index].raw.get("tdnet_signal", "无")
            output.append(f"   TDnet: {signal}")
            output.append(f"   News: {latest_news_title or '-'}")
            pick_index += 1

    return "\n".join(output)


def _get_multi_mode_results_for_cli(limit: int, candidate_limit: int) -> tuple[list[PickResult], bool]:
    stored_results = _load_stored_scan_results_for_today()
    if stored_results:
        return stored_results, False
    return run_multi_mode_scan_results(limit=limit, candidate_limit=candidate_limit), True


def _get_single_mode_result_for_cli(mode: str | None, limit: int, candidate_limit: int) -> tuple[PickResult, bool]:
    mode_key = str(mode).strip().lower() if mode else ""
    if mode_key:
        stored_result = _load_stored_single_mode_result_for_today(mode_key)
        if stored_result is not None:
            return stored_result, False
        return run_picker_result(limit=limit, candidate_limit=candidate_limit, mode=mode_key), True

    preferred_mode = DEFAULT_MODE
    stored_result = _load_stored_single_mode_result_for_today(preferred_mode)
    if stored_result is not None:
        return stored_result, False
    return run_picker_result(limit=limit, candidate_limit=candidate_limit, mode=mode), True


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

    if args.push and args.mode is None:
        results, reran = _get_multi_mode_results_for_cli(limit=args.limit, candidate_limit=args.candidate_limit)
        reused_from_signals = not reran
        if reran:
            print("未找到当日 scan 信号，已回退为重新执行三模式筛选。")
        else:
            print("已复用当日已落盘 scan 三模式信号。")
        for result in results:
            strategy_source_label = _resolve_cli_strategy_source_label(
                reused_from_signals=reused_from_signals,
                reran=reran,
                requested_mode=args.mode,
                result=result,
            )
            print(_build_console_output(result, strategy_source_label=strategy_source_label))
            print("")
        msg = build_multi_mode_push_message(results)
        ok, err = send_telegram_message(msg)
        print("Telegram 推送成功" if ok else f"推送失败: {err}")
        if (not ok) and (not telegram_configured()):
            print("请先加载 .env，或在定时任务中先 source /opt/ai-trader/.env")
    else:
        result, reran = _get_single_mode_result_for_cli(
            mode=args.mode,
            limit=args.limit,
            candidate_limit=args.candidate_limit,
        )
        reused_from_signals = not reran
        if reran:
            print("未找到当日已落盘同模式信号，已回退为重新执行筛选。")
        else:
            print("已复用当日已落盘同模式信号。")
        strategy_source_label = _resolve_cli_strategy_source_label(
            reused_from_signals=reused_from_signals,
            reran=reran,
            requested_mode=args.mode,
            result=result,
        )
        print(_build_console_output(result, strategy_source_label=strategy_source_label))

        if args.push:
            msg = build_pick_message(result)
            ok, err = send_telegram_message(msg)
            print("Telegram 推送成功" if ok else f"推送失败: {err}")
            if (not ok) and (not telegram_configured()):
                print("请先加载 .env，或在定时任务中先 source /opt/ai-trader/.env")
