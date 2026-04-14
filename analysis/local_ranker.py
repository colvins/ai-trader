"""Local deterministic ranking logic; no remote LLM calls here."""

from __future__ import annotations

from datetime import datetime, timezone

from tdnet_source import fetch_tdnet


DEFAULT_MODE = "trend"
SUPPORTED_MODES = {"breakout", "trend", "dip"}
TDNET_BULLISH_KEYWORDS = ["上方修正", "増配", "自社株買い", "取得"]
TDNET_BEARISH_KEYWORDS = ["下方修正", "減配", "赤字"]


def _safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _normalize_news_items(raw):
    if raw is None:
        return []
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        if isinstance(raw.get("items"), list):
            items = raw.get("items", [])
        elif isinstance(raw.get("news"), list):
            items = raw.get("news", [])
        else:
            items = [raw]
    else:
        return []
    return [x for x in items if isinstance(x, dict)]


def _normalize_stock_payload(stock):
    if isinstance(stock, dict):
        payload = dict(stock)
    else:
        payload = {"symbol": str(stock).strip()}

    payload["symbol"] = str(payload.get("symbol", "")).strip()
    numeric_fields = [
        "day_change_pct",
        "intraday_pct",
        "amplitude_pct",
        "amount",
        "amount_ratio_5",
        "momentum_3_pct",
        "momentum_5_pct",
        "dist_to_high_5_pct",
        "dist_to_high_20_pct",
        "close_position",
        "body_pct",
        "history_days",
    ]
    for field in numeric_fields:
        payload[field] = _safe_float(payload.get(field))
    return payload


def _clamp(value, low, high):
    return max(low, min(high, value))


def _build_tdnet_map():
    tdnet_map = {}

    try:
        items = fetch_tdnet() or []
    except Exception:
        return tdnet_map

    for item in items:
        symbol = str(item.get("symbol", "")).strip()
        title = str(item.get("title", "")).strip()
        if not symbol or not title:
            continue

        delta = 0.0
        if any(keyword in title for keyword in TDNET_BULLISH_KEYWORDS):
            delta += 0.1
        if any(keyword in title for keyword in TDNET_BEARISH_KEYWORDS):
            delta -= 0.1
        if delta == 0.0:
            continue

        bucket = tdnet_map.setdefault(symbol, {"score": 0.0, "titles": []})
        bucket["score"] += delta
        if title not in bucket["titles"]:
            bucket["titles"].append(title)

    return tdnet_map


def normalize_mode(mode):
    mode = str(mode or DEFAULT_MODE).strip().lower()
    if mode not in SUPPORTED_MODES:
        return DEFAULT_MODE
    return mode


def _append_reason(parts, delta, reason, positive_threshold=0.025, negative_threshold=-0.025):
    if delta >= positive_threshold:
        parts.append(reason)
    elif delta <= negative_threshold:
        parts.append(reason)


BULLISH_KEYWORDS = [
    "upgrade", "beat", "growth", "record", "profit", "strong",
    "partnership", "contract", "expansion", "surge", "raises",
    "buyback", "guidance raised", "forecast raised", "investment",
    "order", "approval", "acquisition", "launch",
]

BEARISH_KEYWORDS = [
    "downgrade", "miss", "weak", "loss", "fall", "drop",
    "investigation", "lawsuit", "cut", "guidance cut", "recall",
    "warning", "delay", "probe", "fraud", "decline", "slump",
]

LEADER_SYMBOLS = {
    "7203",
    "6758",
    "9984",
    "8035",
    "6501",
    "6857",
    "9432",
    "8306",
}


def _news_time_weight(item):
    ts = item.get("published_at") or item.get("datetime") or item.get("date") or ""
    if not ts:
        return 0.5

    try:
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        else:
            text = str(ts).strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        delta_days = (now - dt).total_seconds() / 86400.0
        if delta_days < 0:
            return 1.0
        if delta_days <= 3:
            return 1.0
        if delta_days <= 7:
            return 0.5
        return 0.0
    except Exception:
        return 0.5


def _news_item_weight(item):
    source = str(item.get("source", "")).lower()
    relevance = _safe_float(item.get("relevance", 1.0), 1.0)

    weight = 1.0
    if source.startswith("marketaux_search"):
        weight *= 0.7
    if source == "yfinance":
        weight *= 0.85

    time_weight = _news_time_weight(item)
    if time_weight <= 0:
        return 0.0

    weight *= _clamp(relevance, 0.0, 1.0)
    weight *= time_weight
    return _clamp(weight, 0.0, 1.0)


def news_score(news_items, mode="trend"):
    if not news_items:
        return 0.0, ["无有效新闻"]

    mode = normalize_mode(mode)
    if mode == "breakout":
        base_cap = 4
        pos_unit = 0.055
        neg_unit = 0.065
        max_total = 0.24
    elif mode == "dip":
        base_cap = 2
        pos_unit = 0.03
        neg_unit = 0.07
        max_total = 0.16
    else:
        base_cap = 3
        pos_unit = 0.04
        neg_unit = 0.06
        max_total = 0.20

    valid_news = []
    for item in news_items[:8]:
        if _news_item_weight(item) > 0:
            valid_news.append(item)
    if not valid_news:
        return 0.0, ["无近期有效新闻"]

    total = min(len(valid_news), base_cap) * 0.018
    positive_hits = 0
    negative_hits = 0
    reasons = []

    for item in valid_news[:5]:
        title = str(item.get("title", "")).lower()
        weight = _news_item_weight(item)
        if weight <= 0:
            continue

        matched = False
        for kw in BULLISH_KEYWORDS:
            if kw in title:
                total += pos_unit * weight
                positive_hits += 1
                matched = True
                break

        if not matched:
            for kw in BEARISH_KEYWORDS:
                if kw in title:
                    total -= neg_unit * weight
                    negative_hits += 1
                    matched = True
                    break

    if positive_hits > 0:
        reasons.append("新闻偏利好")
    if negative_hits > 0:
        reasons.append("含利空新闻")
    if positive_hits == 0 and negative_hits == 0:
        reasons.append("新闻中性")

    total = _clamp(total, -0.14, max_total)
    return total, reasons


def symbol_bias(symbol, mode="trend"):
    if symbol not in LEADER_SYMBOLS:
        return 0.0, ""
    if mode == "breakout":
        return 0.012, "龙头加成"
    if mode == "trend":
        return 0.02, "龙头加成"
    return 0.004, "龙头微调"


def _score_breakout_intraday(day_pct, body_pct):
    if day_pct <= -1:
        return -0.10, "今日走弱"
    if day_pct < 2:
        return -0.03, "今日强度不足"
    if day_pct < 5:
        return 0.10, "今日涨幅良好"
    if day_pct < 8:
        return 0.15, "今日强势上涨"
    if day_pct < 11:
        return 0.04, "涨幅偏热"
    return -0.10, "涨幅过热"


def _score_breakout_amplitude(pct):
    if pct < 2:
        return -0.02, "振幅偏小"
    if pct < 4:
        return 0.04, "振幅适中"
    if pct < 8:
        return 0.10, "振幅活跃"
    if pct < 11:
        return 0.05, "高波动活跃"
    return -0.04, "振幅过大"


def _score_breakout_amount(amount, amount_ratio_5):
    score = 0.0
    reasons = []
    if amount >= 10_000_000_000:
        score += 0.12
        reasons.append("成交额很强")
    elif amount >= 5_000_000_000:
        score += 0.09
        reasons.append("成交额较强")
    elif amount >= 2_000_000_000:
        score += 0.05
        reasons.append("成交额达标")

    if amount_ratio_5 >= 2.8:
        score += 0.11
        reasons.append("明显放量")
    elif amount_ratio_5 >= 2.0:
        score += 0.08
        reasons.append("温和放量")
    elif amount_ratio_5 >= 1.3:
        score += 0.03
        reasons.append("量能改善")
    elif amount_ratio_5 < 0.9:
        score -= 0.06
        reasons.append("量能偏弱")
    return _clamp(score, -0.06, 0.22), " + ".join(reasons[:2]) or "量能中性"


def _score_breakout_momentum(m3, m5, history_days):
    if history_days < 6:
        return 0.01, "历史数据较少"

    score = 0.0
    reasons = []
    if m3 >= 8:
        score += 0.13
        reasons.append("3日动量强")
    elif m3 >= 5:
        score += 0.10
        reasons.append("3日动量良好")
    elif m3 >= 2:
        score += 0.05
        reasons.append("3日动量偏强")
    elif m3 <= -3:
        score -= 0.08
        reasons.append("3日动量转弱")

    if m5 >= 12:
        score += 0.10
        reasons.append("5日趋势强")
    elif m5 >= 7:
        score += 0.07
        reasons.append("5日趋势良好")
    elif m5 >= 3:
        score += 0.03
        reasons.append("5日趋势偏强")
    elif m5 <= -5:
        score -= 0.08
        reasons.append("5日趋势走弱")
    return _clamp(score, -0.10, 0.23), " + ".join(reasons[:2]) or "动量中性"


def _score_breakout_high(dist_5, dist_20):
    score = 0.0
    reasons = []
    if dist_5 >= -1.0:
        score += 0.08
        reasons.append("接近5日高点")
    elif dist_5 >= -2.5:
        score += 0.04
        reasons.append("靠近5日高点")
    else:
        score -= 0.03
        reasons.append("离5日高点偏远")

    if dist_20 >= -2.0:
        score += 0.12
        reasons.append("接近20日高点")
    elif dist_20 >= -4.5:
        score += 0.06
        reasons.append("靠近20日高点")
    else:
        score -= 0.05
        reasons.append("离20日高点偏远")
    return _clamp(score, -0.08, 0.22), " + ".join(reasons[:2])


def _score_breakout_candle(close_position, body_pct):
    if close_position >= 0.85 and body_pct > 0:
        return 0.08, "收盘靠近最高位"
    if close_position >= 0.7 and body_pct > 0:
        return 0.05, "收盘位置较好"
    if close_position <= 0.25:
        return -0.09, "冲高回落明显"
    if close_position <= 0.4:
        return -0.04, "收盘偏弱"
    return 0.0, "收盘中性"


def _technical_score_breakout(stock):
    parts = []
    total = 0.0
    for scorer in (
        lambda: _score_breakout_intraday(stock.get("day_change_pct", 0.0), stock.get("body_pct", 0.0)),
        lambda: _score_breakout_amplitude(stock.get("amplitude_pct", 0.0)),
        lambda: _score_breakout_amount(stock.get("amount", 0.0), stock.get("amount_ratio_5", 1.0)),
        lambda: _score_breakout_momentum(stock.get("momentum_3_pct", 0.0), stock.get("momentum_5_pct", 0.0), stock.get("history_days", 0.0)),
        lambda: _score_breakout_high(stock.get("dist_to_high_5_pct", 0.0), stock.get("dist_to_high_20_pct", 0.0)),
        lambda: _score_breakout_candle(stock.get("close_position", 0.5), stock.get("body_pct", 0.0)),
    ):
        delta, reason = scorer()
        total += delta
        _append_reason(parts, delta, reason)
    total = _clamp(total, 0.0, 0.80)
    return total, parts[:4] or ["技术面中性"]


def _score_trend_intraday(day_pct):
    if day_pct <= -2:
        return -0.08, "今日走弱"
    if day_pct < 0:
        return -0.03, "今日偏弱"
    if day_pct < 3:
        return 0.05, "今日温和走强"
    if day_pct < 6:
        return 0.09, "今日涨幅良好"
    if day_pct < 9:
        return 0.05, "今日强势上涨"
    return -0.05, "涨幅偏热"


def _score_trend_amplitude(pct):
    if pct < 1.5:
        return -0.01, "振幅偏小"
    if pct < 4:
        return 0.04, "振幅适中"
    if pct < 6.5:
        return 0.07, "振幅活跃"
    if pct < 9:
        return 0.03, "波动偏大"
    return -0.04, "振幅过大"


def _score_trend_amount(amount, amount_ratio_5):
    score = 0.0
    reasons = []
    if amount >= 10_000_000_000:
        score += 0.10
        reasons.append("成交额很强")
    elif amount >= 5_000_000_000:
        score += 0.08
        reasons.append("成交额较强")
    elif amount >= 2_000_000_000:
        score += 0.05
        reasons.append("成交额达标")

    if amount_ratio_5 >= 2.2:
        score += 0.06
        reasons.append("明显放量")
    elif amount_ratio_5 >= 1.4:
        score += 0.05
        reasons.append("温和放量")
    elif amount_ratio_5 >= 1.0:
        score += 0.02
        reasons.append("量能稳定")
    elif amount_ratio_5 < 0.8:
        score -= 0.05
        reasons.append("量能偏弱")
    return _clamp(score, -0.05, 0.18), " + ".join(reasons[:2]) or "量能中性"


def _score_trend_momentum(m3, m5, history_days):
    if history_days < 6:
        return 0.01, "历史数据较少"

    score = 0.0
    reasons = []
    if m3 >= 5:
        score += 0.07
        reasons.append("3日动量良好")
    elif m3 >= 2:
        score += 0.05
        reasons.append("3日动量偏强")
    elif m3 >= 0:
        score += 0.02
        reasons.append("3日维持强势")
    elif m3 <= -4:
        score -= 0.07
        reasons.append("3日动量转弱")

    if m5 >= 10:
        score += 0.13
        reasons.append("5日趋势强")
    elif m5 >= 6:
        score += 0.10
        reasons.append("5日趋势良好")
    elif m5 >= 3:
        score += 0.05
        reasons.append("5日趋势偏强")
    elif m5 <= -5:
        score -= 0.08
        reasons.append("5日趋势走弱")

    if m5 > m3 and m5 >= 5:
        score += 0.03
        reasons.append("趋势连续性较好")
    return _clamp(score, -0.10, 0.24), " + ".join(reasons[:2]) or "动量中性"


def _score_trend_high(dist_5, dist_20):
    score = 0.0
    reasons = []
    if dist_5 >= -2.0:
        score += 0.04
        reasons.append("接近5日高点")
    elif dist_5 >= -4.0:
        score += 0.02
        reasons.append("靠近5日高点")

    if dist_20 >= -1.5:
        score += 0.15
        reasons.append("接近20日高点")
    elif dist_20 >= -4.0:
        score += 0.10
        reasons.append("靠近20日高点")
    elif dist_20 >= -7.0:
        score += 0.04
        reasons.append("中期位置尚可")
    else:
        score -= 0.05
        reasons.append("离20日高点偏远")
    return _clamp(score, -0.06, 0.22), " + ".join(reasons[:2])


def _score_trend_candle(close_position, body_pct):
    if close_position >= 0.8 and body_pct > 0:
        return 0.06, "收盘靠近高位"
    if close_position >= 0.65 and body_pct >= 0:
        return 0.04, "收盘位置较好"
    if close_position <= 0.25:
        return -0.06, "冲高回落明显"
    if close_position <= 0.4:
        return -0.03, "收盘偏弱"
    return 0.0, "收盘中性"


def _technical_score_trend(stock):
    parts = []
    total = 0.0
    for scorer in (
        lambda: _score_trend_intraday(stock.get("day_change_pct", 0.0)),
        lambda: _score_trend_amplitude(stock.get("amplitude_pct", 0.0)),
        lambda: _score_trend_amount(stock.get("amount", 0.0), stock.get("amount_ratio_5", 1.0)),
        lambda: _score_trend_momentum(stock.get("momentum_3_pct", 0.0), stock.get("momentum_5_pct", 0.0), stock.get("history_days", 0.0)),
        lambda: _score_trend_high(stock.get("dist_to_high_5_pct", 0.0), stock.get("dist_to_high_20_pct", 0.0)),
        lambda: _score_trend_candle(stock.get("close_position", 0.5), stock.get("body_pct", 0.0)),
    ):
        delta, reason = scorer()
        total += delta
        _append_reason(parts, delta, reason)
    total = _clamp(total, 0.0, 0.76)
    return total, parts[:4] or ["技术面中性"]


def _score_dip_intraday(day_pct):
    if day_pct <= -5:
        return -0.10, "今日仍偏弱"
    if day_pct < -2:
        return -0.05, "抛压仍在"
    if day_pct < 1.5:
        return 0.05, "今日企稳"
    if day_pct < 4:
        return 0.08, "今日反弹"
    if day_pct < 6:
        return 0.04, "反弹力度良好"
    return -0.03, "反弹略急"


def _score_dip_day_change(day_pct):
    if day_pct <= -2:
        return -0.08, "今日涨幅偏弱"
    if day_pct < 0:
        return -0.03, "今日修复有限"
    if day_pct < 1.5:
        return 0.03, "今日温和反弹"
    if day_pct < 2.5:
        return 0.06, "今日反弹增强"
    if day_pct < 4:
        return 0.10, "今日明显反弹"
    if day_pct < 6:
        return 0.07, "今日反弹较强"
    return 0.02, "今日涨幅偏急"


def _score_dip_day_change_boost(day_pct):
    normalized = max(0.0, min(day_pct / 5.0, 1.6))
    boost = (normalized ** 1.5) * 0.14

    if boost >= 0.10:
        reason = "今日涨幅强势放大"
    elif boost >= 0.06:
        reason = "今日涨幅强化"
    elif boost > 0:
        reason = "今日涨幅加分"
    else:
        reason = "今日涨幅无强化"
    return boost, reason


def _score_dip_amplitude(pct):
    if pct < 1.5:
        return 0.01, "振幅温和"
    if pct < 4:
        return 0.04, "振幅适中"
    if pct < 6.5:
        return 0.05, "波动活跃"
    if pct < 9:
        return 0.01, "波动偏大"
    return -0.05, "振幅过大"


def _score_dip_amount(amount, amount_ratio_5):
    score = 0.0
    reasons = []
    if amount >= 8_000_000_000:
        score += 0.08
        reasons.append("成交额较强")
    elif amount >= 2_000_000_000:
        score += 0.05
        reasons.append("成交额达标")

    if 1.1 <= amount_ratio_5 <= 2.0:
        score += 0.07
        reasons.append("量能回暖")
    elif 2.0 < amount_ratio_5 <= 3.0:
        score += 0.05
        reasons.append("放量反弹")
    elif amount_ratio_5 < 0.8:
        score -= 0.05
        reasons.append("量能不足")
    return _clamp(score, -0.05, 0.16), " + ".join(reasons[:2]) or "量能中性"


def _score_dip_momentum(m3, m5, history_days):
    if history_days < 6:
        return 0.01, "历史数据较少"

    score = 0.0
    reasons = []
    if -2 <= m3 <= 2:
        score += 0.05
        reasons.append("3日跌势放缓")
    elif 2 < m3 <= 6:
        score += 0.07
        reasons.append("3日开始修复")
    elif m3 > 6:
        score += 0.03
        reasons.append("3日修复较快")
    elif m3 < -5:
        score -= 0.08
        reasons.append("3日仍在走弱")

    if -10 <= m5 <= -3:
        score += 0.10
        reasons.append("5日回撤充分")
    elif -3 < m5 <= 2:
        score += 0.05
        reasons.append("5日走势趋稳")
    elif 2 < m5 <= 7:
        score += 0.03
        reasons.append("5日修复中")
    elif m5 < -12:
        score -= 0.06
        reasons.append("5日过弱")
    elif m5 > 9:
        score -= 0.05
        reasons.append("已脱离低吸区")
    return _clamp(score, -0.12, 0.18), " + ".join(reasons[:2]) or "动量中性"


def _score_dip_momentum_3_only(m3):
    if m3 <= -5:
        return -0.08, "3日修复不足"
    if m3 < -1:
        return -0.03, "3日仍偏弱"
    if m3 < 1:
        return 0.02, "3日止跌企稳"
    if m3 < 3:
        return 0.07, "3日开始转强"
    if m3 < 6:
        return 0.10, "3日反弹明确"
    return 0.06, "3日修复较快"


def _score_dip_momentum_3_boost(m3):
    normalized = max(0.0, min(m3 / 5.0, 1.8))
    boost = (normalized ** 1.3) * 0.10

    if boost >= 0.07:
        reason = "3日动量强势放大"
    elif boost >= 0.04:
        reason = "3日动量强化"
    elif boost > 0:
        reason = "3日动量加分"
    else:
        reason = "3日动量无强化"
    return boost, reason


def _score_dip_high(dist_5, dist_20):
    score = 0.0
    reasons = []
    if -9 <= dist_5 <= -2:
        score += 0.05
        reasons.append("短线回撤后企稳")
    elif dist_5 > -1.5:
        score -= 0.04
        reasons.append("离短线高点过近")
    elif dist_5 < -13:
        score -= 0.04
        reasons.append("短线位置偏弱")

    if -15 <= dist_20 <= -5:
        score += 0.13
        reasons.append("中期回撤充分")
    elif -5 < dist_20 <= -2:
        score += 0.03
        reasons.append("中期位置尚可")
    elif -20 <= dist_20 < -15:
        score += 0.04
        reasons.append("回撤偏深")
    elif dist_20 < -20:
        score -= 0.08
        reasons.append("离20日高点过远")
    elif dist_20 > -2:
        score -= 0.05
        reasons.append("已接近高位")
    return _clamp(score, -0.10, 0.18), " + ".join(reasons[:2])


def _score_dip_position_bias(dist_20):
    if dist_20 > -10:
        return -0.10, "中期位置偏高"
    if dist_20 < -20:
        return 0.06, "中期低位加分"
    return 0.0, "中期位置中性"


def _score_dip_candle(close_position, body_pct):
    if close_position >= 0.75 and body_pct >= 0:
        return 0.07, "收盘出现承接"
    if close_position >= 0.58 and body_pct >= 0:
        return 0.04, "收盘位置改善"
    if close_position <= 0.25:
        return -0.07, "尾盘仍弱"
    if close_position <= 0.4 and body_pct < 0:
        return -0.05, "收盘偏弱"
    return 0.0, "收盘中性"


def _score_dip_close_position_boost(close_position):
    normalized = max(0.0, min(close_position, 1.0))
    boost = (normalized ** 2) * 0.08

    if boost >= 0.05:
        reason = "收盘位置强势放大"
    elif boost >= 0.03:
        reason = "收盘位置强化"
    elif boost > 0:
        reason = "收盘位置加分"
    else:
        reason = "收盘位置无强化"
    return boost, reason


def _score_dip_amount_ratio_bias(amount_ratio_5):
    if amount_ratio_5 < 0.8:
        return -0.06, "量能仍弱"
    if amount_ratio_5 < 1.0:
        return -0.03, "量能未明显回暖"
    if amount_ratio_5 > 1.8:
        return 0.06, "量能明显放大"
    if amount_ratio_5 > 1.2:
        return 0.04, "量能回暖确认"
    return 0.0, "量能中性"


def _dip_score_multiplier(dist_20, amount_ratio_5):
    multiplier = 1.0
    reasons = []

    if dist_20 > -5:
        multiplier *= 0.6
        reasons.append("高位反弹乘法降权")
    elif dist_20 > -10:
        multiplier *= 0.75
        reasons.append("偏高位置乘法降权")
    elif dist_20 < -20:
        multiplier *= 1.1
        reasons.append("低位反弹乘法加权")

    if amount_ratio_5 < 1:
        multiplier *= 0.85
        reasons.append("量能不足乘法降权")

    return multiplier, reasons


def _dip_entry_quality_multiplier(dist_20, close_position, day_pct, momentum_5_pct):
    multiplier = 1.0
    reasons = []

    if close_position > 0.85:
        multiplier *= 0.9
        reasons.append("收盘位置过高降权")

    if day_pct > 4:
        multiplier *= 0.85
        reasons.append("涨幅过大降权")

    if momentum_5_pct > 8:
        multiplier *= 0.7
        reasons.append("趋势过强降权")

    if dist_20 < -15:
        multiplier *= 1.2
        reasons.append("低位启动加权")

    return multiplier, reasons


def _technical_score_dip(stock):
    parts = []
    total = 0.0
    for scorer in (
        lambda: _score_dip_intraday(stock.get("day_change_pct", 0.0)),
        lambda: _score_dip_day_change(stock.get("day_change_pct", 0.0)),
        lambda: _score_dip_day_change_boost(stock.get("day_change_pct", 0.0)),
        lambda: _score_dip_amplitude(stock.get("amplitude_pct", 0.0)),
        lambda: _score_dip_amount(stock.get("amount", 0.0), stock.get("amount_ratio_5", 1.0)),
        lambda: _score_dip_momentum(stock.get("momentum_3_pct", 0.0), stock.get("momentum_5_pct", 0.0), stock.get("history_days", 0.0)),
        lambda: _score_dip_momentum_3_only(stock.get("momentum_3_pct", 0.0)),
        lambda: _score_dip_momentum_3_boost(stock.get("momentum_3_pct", 0.0)),
        lambda: _score_dip_high(stock.get("dist_to_high_5_pct", 0.0), stock.get("dist_to_high_20_pct", 0.0)),
        lambda: _score_dip_candle(stock.get("close_position", 0.5), stock.get("body_pct", 0.0)),
        lambda: _score_dip_close_position_boost(stock.get("close_position", 0.5)),
    ):
        delta, reason = scorer()
        total += delta
        _append_reason(parts, delta, reason)

    multiplier, multiplier_reasons = _dip_score_multiplier(
        stock.get("dist_to_high_20_pct", 0.0),
        stock.get("amount_ratio_5", 1.0),
    )
    total *= multiplier
    parts.extend(multiplier_reasons)

    entry_multiplier, entry_reasons = _dip_entry_quality_multiplier(
        stock.get("dist_to_high_20_pct", 0.0),
        stock.get("close_position", 0.5),
        stock.get("day_change_pct", 0.0),
        stock.get("momentum_5_pct", 0.0),
    )
    total *= entry_multiplier
    parts.extend(entry_reasons)
    total = _clamp(total, 0.0, 0.95)
    return total, parts[:5] or ["技术面中性"]


def technical_score(stock, mode="trend"):
    mode = normalize_mode(mode)
    if mode == "breakout":
        return _technical_score_breakout(stock)
    if mode == "dip":
        return _technical_score_dip(stock)
    return _technical_score_trend(stock)


def build_reason(tech_parts, news_parts, bias_reason, mode="trend"):
    merged = []
    for part in tech_parts + news_parts:
        part = str(part).strip()
        if part and part not in merged:
            merged.append(part)

    if bias_reason and len(merged) < 4:
        merged.append(bias_reason)
    if not merged:
        return "技术面中性"

    negative_words = ["走弱", "偏弱", "回落", "利空", "过热", "不足", "抛压", "过远", "过大"]
    positive_first = []
    negative_later = []
    neutral = []

    for value in merged:
        if any(word in value for word in negative_words):
            negative_later.append(value)
        elif "中性" in value:
            neutral.append(value)
        else:
            positive_first.append(value)

    ordered = positive_first + negative_later + neutral
    limit = 5 if mode == "dip" else 4
    return " + ".join(ordered[:limit])


def analyze_stocks(stocks, news_map=None, mode=DEFAULT_MODE):
    news_map = news_map or {}
    mode = normalize_mode(mode)
    results = []
    tdnet_map = _build_tdnet_map()

    for stock in stocks or []:
        payload = _normalize_stock_payload(stock)
        symbol = payload.get("symbol", "")
        if not symbol:
            continue

        raw_news = news_map.get(symbol, [])
        news_items = _normalize_news_items(raw_news)

        tech_score, tech_parts = technical_score(payload, mode=mode)
        current_news_score, news_parts = news_score(news_items, mode=mode)
        bias_score, bias_reason = symbol_bias(symbol, mode=mode)
        tdnet_info = tdnet_map.get(symbol, {})
        tdnet_score = float(tdnet_info.get("score", 0.0))
        tdnet_title = " | ".join(tdnet_info.get("titles", []))

        score = _clamp(tech_score + current_news_score + bias_score + tdnet_score, 0.0, 0.99)
        reason = build_reason(tech_parts, news_parts, bias_reason, mode=mode)
        tdnet_signal = "利好" if tdnet_score > 0 else "利空" if tdnet_score < 0 else "无"

        results.append(
            {
                "symbol": symbol,
                "score": round(score, 4),
                "reason": reason,
                "mode": mode,
                "tech_score": round(tech_score, 4),
                "news_score": round(current_news_score, 4),
                "bias_score": round(bias_score, 4),
                "tdnet_score": round(tdnet_score, 4),
                "tdnet_signal": tdnet_signal,
                "tdnet_title": tdnet_title,
                "tech_parts": tech_parts,
                "news_parts": news_parts,
            }
        )

    results.sort(key=lambda x: x["score"], reverse=True)
    return results
