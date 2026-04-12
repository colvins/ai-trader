"""Indicator loading and calculation for local daily CSV market data."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


DATA_DIR = Path("data/jq_daily")


def list_recent_files(days: int = 60) -> list[Path]:
    files = sorted(DATA_DIR.glob("*.csv"), reverse=True)
    return files[:days]


def read_daily_file(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty or "code" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["code"] = df["code"].astype(str).str.replace(".0", "", regex=False).str.strip()

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        if col not in df.columns:
            return pd.DataFrame()
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=numeric_cols)
    df = df[
        (df["open"] > 0)
        & (df["high"] > 0)
        & (df["low"] > 0)
        & (df["close"] > 0)
        & (df["volume"] > 0)
    ].copy()

    if "date" not in df.columns:
        df["date"] = path.stem

    return df[["code", "date", "open", "high", "low", "close", "volume"]]


def load_recent_history(days: int = 60) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    files = list_recent_files(days=days)
    if not files:
        raise RuntimeError("没有行情数据")

    all_frames = []
    latest_name = files[0].name

    for path in files:
        try:
            df = read_daily_file(path)
            if not df.empty:
                all_frames.append(df)
        except Exception:
            continue

    if not all_frames:
        raise RuntimeError("最近行情文件均不可用")

    full_df = pd.concat(all_frames, ignore_index=True)
    full_df["date"] = pd.to_datetime(full_df["date"], errors="coerce")
    full_df = full_df.dropna(subset=["date"]).copy()
    full_df = full_df.sort_values(["code", "date"], ascending=[True, False]).reset_index(drop=True)

    latest_date = full_df["date"].max()
    today_df = full_df[full_df["date"] == latest_date].copy()
    return full_df, today_df, latest_name


def calc_features_from_history(symbol: str, hist_df: pd.DataFrame) -> dict | None:
    if hist_df.empty or len(hist_df) < 6:
        return None

    hist_df = hist_df.sort_values("date", ascending=False).reset_index(drop=True)
    row = hist_df.iloc[0]

    close = float(row["close"])
    open_ = float(row["open"])
    high = float(row["high"])
    low = float(row["low"])
    volume = float(row["volume"])

    prev_close = None
    if len(hist_df) >= 2:
        prev_close = float(hist_df.iloc[1]["close"])

    intraday_pct = (close - open_) / open_ * 100.0
    day_change_pct = ((close - prev_close) / prev_close * 100.0) if prev_close and prev_close > 0 else intraday_pct
    amplitude_pct = (high - low) / open_ * 100.0
    amount = close * volume

    closes = hist_df["close"].tolist()
    highs = hist_df["high"].tolist()
    volumes = hist_df["volume"].tolist()

    idx3 = min(3, len(closes) - 1)
    idx5 = min(5, len(closes) - 1)

    prev_close_3 = closes[idx3]
    prev_close_5 = closes[idx5]

    momentum_3_pct = ((close - prev_close_3) / prev_close_3 * 100.0) if prev_close_3 > 0 else 0.0
    momentum_5_pct = ((close - prev_close_5) / prev_close_5 * 100.0) if prev_close_5 > 0 else 0.0

    high_5 = max(highs[:5]) if len(highs) >= 5 else max(highs)
    high_20 = max(highs[:20]) if len(highs) >= 20 else max(highs)
    high_60 = max(highs[:60]) if len(highs) >= 60 else max(highs)

    dist_to_high_5_pct = ((close - high_5) / high_5 * 100.0) if high_5 > 0 else 0.0
    dist_to_high_20_pct = ((close - high_20) / high_20 * 100.0) if high_20 > 0 else 0.0
    dist_to_high_60_pct = ((close - high_60) / high_60 * 100.0) if high_60 > 0 else 0.0

    recent_volumes = volumes[1:6] if len(volumes) > 1 else []
    vol_mean_5 = sum(recent_volumes) / len(recent_volumes) if recent_volumes else volume
    amount_ratio_5 = (volume / vol_mean_5) if vol_mean_5 > 0 else 1.0

    close_position = (close - low) / (high - low + 1e-6)
    body_pct = (close - open_) / open_ * 100.0

    return {
        "symbol": str(symbol).strip(),
        "close": round(close, 4),
        "prev_close": round(prev_close, 4) if prev_close is not None else None,
        "open": round(open_, 4),
        "high": round(high, 4),
        "low": round(low, 4),
        "volume": round(volume, 4),
        "day_change_pct": round(day_change_pct, 4),
        "intraday_pct": round(intraday_pct, 4),
        "amplitude_pct": round(amplitude_pct, 4),
        "amount": round(amount, 4),
        "amount_ratio_5": round(amount_ratio_5, 4),
        "momentum_3_pct": round(momentum_3_pct, 4),
        "momentum_5_pct": round(momentum_5_pct, 4),
        "dist_to_high_5_pct": round(dist_to_high_5_pct, 4),
        "dist_to_high_20_pct": round(dist_to_high_20_pct, 4),
        "dist_to_high_60_pct": round(dist_to_high_60_pct, 4),
        "close_position": round(close_position, 4),
        "body_pct": round(body_pct, 4),
        "history_days": int(len(hist_df)),
    }
