#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf

BASE_DIR = Path("/opt/ai-trader")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.services.notify_service import send_telegram_message, telegram_configured


DATA_DIR = BASE_DIR / "data"
CACHE_DIR = DATA_DIR / "jq_daily"
LOG_DIR = DATA_DIR / "logs"
STATE_DIR = DATA_DIR / "runtime"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = DATA_DIR / "update_price_cache.log"
STATE_FILE = STATE_DIR / "update_price_cache_state.json"
SUMMARY_FILE = STATE_DIR / "update_summary.json"

JST = timezone(timedelta(hours=9))

DEFAULT_SYMBOLS = [
    "7203",
    "6758",
    "7974",
    "8035",
    "8306",
]

CSV_COLUMNS = ["code", "date", "open", "high", "low", "close", "adj_close", "volume"]


def log_line(text: str) -> None:
    ts = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {text}"
    print(line)
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def today_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d")


def now_jst_iso() -> str:
    return datetime.now(JST).isoformat()


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def save_summary(summary: dict) -> None:
    SUMMARY_FILE.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def get_today_completed_symbols() -> set[str]:
    state = load_state()
    run_date = today_jst_str()
    completed = state.get("completed_by_date", {}).get(run_date, [])
    return set(str(x).strip() for x in completed if str(x).strip())


def mark_symbol_completed(symbol: str) -> None:
    state = load_state()
    run_date = today_jst_str()

    completed_by_date = state.setdefault("completed_by_date", {})
    completed_list = completed_by_date.setdefault(run_date, [])

    symbol = str(symbol).strip()
    if symbol not in completed_list:
        completed_list.append(symbol)

    all_dates = sorted(completed_by_date.keys(), reverse=True)
    keep_dates = set(all_dates[:10])
    completed_by_date = {k: v for k, v in completed_by_date.items() if k in keep_dates}
    state["completed_by_date"] = completed_by_date
    state["last_run_date"] = run_date
    state["last_updated_at"] = now_jst_iso()

    save_state(state)


def clear_today_state() -> None:
    state = load_state()
    run_date = today_jst_str()
    completed_by_date = state.get("completed_by_date", {})
    if run_date in completed_by_date:
        completed_by_date.pop(run_date, None)
        state["completed_by_date"] = completed_by_date
        save_state(state)


def normalize_symbol(symbol: str) -> str:
    s = str(symbol).strip().upper()
    if not s:
        return ""
    if s.endswith(".T"):
        return s
    if s.isdigit():
        return f"{s}.T"
    return s


def denormalize_symbol(symbol: str) -> str:
    s = str(symbol).strip().upper()
    if s.endswith(".T"):
        return s[:-2]
    return s


def read_symbols_from_txt(path: Path) -> list[str]:
    symbols = []
    with path.open("r", encoding="utf-8-sig") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "," in s:
                s = s.split(",")[0].strip()
            symbols.append(s)
    return symbols


def read_symbols_from_csv(path: Path) -> list[str]:
    df = pd.read_csv(path, encoding="utf-8-sig")
    candidates = ["code", "symbol", "ticker", "证券代码", "股票代码"]
    col = None
    for c in candidates:
        if c in df.columns:
            col = c
            break

    if col is None:
        raise ValueError(f"股票池文件 {path} 没找到可识别列，需包含 {candidates} 之一")

    values = []
    for x in df[col].tolist():
        if pd.isna(x):
            continue
        s = str(x).strip()
        if not s:
            continue
        if s.endswith(".0") and s.replace(".0", "").isdigit():
            s = s[:-2]
        values.append(s)
    return values


def load_symbol_pool(pool_file: str | None, symbols_arg: str | None) -> list[str]:
    if symbols_arg:
        symbols = [x.strip() for x in symbols_arg.split(",") if x.strip()]
    elif pool_file:
        path = Path(pool_file)
        if not path.is_absolute():
            path = BASE_DIR / path

        if not path.exists():
            raise FileNotFoundError(f"股票池文件不存在: {path}")

        if path.suffix.lower() == ".csv":
            symbols = read_symbols_from_csv(path)
        else:
            symbols = read_symbols_from_txt(path)
    else:
        auto_candidates = [
            DATA_DIR / "universe_jp.csv",
            DATA_DIR / "stock_pool.csv",
            DATA_DIR / "stock_pool.txt",
            BASE_DIR / "stock_pool.csv",
            BASE_DIR / "stock_pool.txt",
        ]

        found = None
        for p in auto_candidates:
            if p.exists():
                found = p
                break

        if found:
            log_line(f"自动使用股票池文件: {found}")
            if found.suffix.lower() == ".csv":
                symbols = read_symbols_from_csv(found)
            else:
                symbols = read_symbols_from_txt(found)
        else:
            log_line("未找到股票池文件，使用默认股票池")
            symbols = DEFAULT_SYMBOLS

    normalized = []
    seen = set()
    for s in symbols:
        ns = normalize_symbol(s)
        if not ns:
            continue
        if ns not in seen:
            normalized.append(ns)
            seen.add(ns)

    if not normalized:
        raise ValueError("股票池为空，无法更新缓存")

    return normalized


def fetch_symbol_history(symbol: str, days: int) -> pd.DataFrame:
    if days <= 5:
        buffer_days = 10
    elif days <= 30:
        buffer_days = 20
    else:
        buffer_days = 40

    start = datetime.now(JST).date() - timedelta(days=days + buffer_days)
    end = datetime.now(JST).date() + timedelta(days=1)

    df = yf.download(
        symbol,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df is None or df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        flat_cols = []
        for c in df.columns:
            if isinstance(c, tuple):
                flat_cols.append(str(c[0]))
            else:
                flat_cols.append(str(c))
        df.columns = flat_cols

    df = df.loc[:, ~pd.Index(df.columns).duplicated()].copy()

    rename_map = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    df = df.rename(columns=rename_map)

    for col in CSV_COLUMNS[2:]:
        if col not in df.columns:
            df[col] = pd.NA

    fixed = {}
    for col in ["open", "high", "low", "close", "adj_close", "volume"]:
        val = df[col]
        if isinstance(val, pd.DataFrame):
            val = val.iloc[:, 0]
        fixed[col] = pd.to_numeric(val, errors="coerce")

    out = pd.DataFrame(fixed, index=df.index)

    if isinstance(out.index, pd.MultiIndex):
        out.index = out.index.get_level_values(0)

    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[~out.index.isna()].copy()
    out = out.sort_index()

    if out.empty:
        return pd.DataFrame()

    out = out.reset_index()
    first_col = out.columns[0]
    out = out.rename(columns={first_col: "date"})

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.dropna(subset=["date"])

    out["code"] = denormalize_symbol(symbol)
    out = out.dropna(subset=["close"])
    out = out[["code", "date", "open", "high", "low", "close", "adj_close", "volume"]]

    return out


def daily_csv_path(date_str: str) -> Path:
    return CACHE_DIR / f"{date_str}.csv"


def load_daily_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=CSV_COLUMNS)

    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame(columns=CSV_COLUMNS)

    for col in CSV_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[CSV_COLUMNS].copy()
    df["code"] = df["code"].astype(str).str.replace(".0", "", regex=False).str.strip()
    df["date"] = df["date"].astype(str).str.strip()

    for col in ["open", "high", "low", "close", "adj_close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def save_daily_file(path: Path, df: pd.DataFrame) -> None:
    df = df.copy()
    for col in CSV_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[CSV_COLUMNS].copy()
    df["code"] = df["code"].astype(str)
    df["date"] = df["date"].astype(str)
    df = df.drop_duplicates(subset=["code"], keep="last")
    df = df.sort_values(by=["code"]).reset_index(drop=True)

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def merge_records_to_daily_cache(df: pd.DataFrame) -> None:
    if df.empty:
        return

    for date_str, group in df.groupby("date"):
        path = daily_csv_path(str(date_str))
        old_df = load_daily_file(path)
        merged = pd.concat([old_df, group[CSV_COLUMNS]], ignore_index=True)
        merged = merged.drop_duplicates(subset=["code"], keep="last")
        save_daily_file(path, merged)


def update_one_symbol(symbol: str, days: int) -> dict:
    code = denormalize_symbol(symbol)

    try:
        df = fetch_symbol_history(symbol, days=days)
    except Exception as e:
        return {
            "symbol": symbol,
            "ok": False,
            "rows": 0,
            "df": None,
            "message": f"{code} 下载失败: {e}",
        }

    if df.empty:
        return {
            "symbol": symbol,
            "ok": False,
            "rows": 0,
            "df": None,
            "message": f"{code} 无数据",
        }

    return {
        "symbol": symbol,
        "ok": True,
        "rows": len(df),
        "df": df,
        "message": f"{code} 成功: {len(df)} 行",
    }


def should_mark_failed_as_completed(message: str) -> bool:
    msg = str(message or "")
    keywords = [
        "无数据",
        "Not Found",
        "possibly delisted",
        "no timezone found",
        "Quote not found",
    ]
    return any(k in msg for k in keywords)


def batched(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def build_status_label(summary: dict) -> str:
    total = max(1, int(summary.get("total", 0)))
    pending = int(summary.get("pending", 0) or 0)
    success = int(summary.get("success", 0) or 0)
    skip = int(summary.get("skip", 0) or 0)
    fail = int(summary.get("fail", 0) or 0)

    # 今日没有待更新任务，且没有失败：说明已是最新
    if pending == 0 and fail == 0:
        return "ok"

    # 所有任务都已被成功或跳过，也算正常
    handled = success + skip
    if handled >= total and fail == 0:
        return "ok"

    success_rate = success / total
    fail_rate = fail / total

    if success_rate >= 0.95 and fail_rate <= 0.03:
        return "ok"
    if success_rate >= 0.80 and fail_rate <= 0.15:
        return "warn"
    return "error"


def build_summary_message(summary: dict) -> str:
    status = str(summary.get("status", "unknown")).lower()
    pending = int(summary.get("pending", 0) or 0)
    success = int(summary.get("success", 0) or 0)
    skip = int(summary.get("skip", 0) or 0)
    fail = int(summary.get("fail", 0) or 0)

    if pending == 0 and fail == 0:
        title = "🔵 行情数据已是最新"
    elif status == "ok":
        title = "🟢 每日行情更新完成"
    elif status == "warn":
        title = "🟡 每日行情更新完成（部分失败）"
    else:
        title = "🔴 每日行情更新异常"

    duration_seconds = float(summary.get("duration_seconds", 0) or 0)
    duration_text = f"{round(duration_seconds / 60, 1)} 分钟" if duration_seconds >= 60 else f"{round(duration_seconds, 1)} 秒"

    lines = [
        title,
        f"日期: {summary.get('date', '未知')}",
        f"股票总数: {summary.get('total', 0)}",
        f"待更新: {pending}",
        f"成功: {success}",
        f"跳过: {skip}",
        f"失败: {fail}",
        f"更新窗口: {summary.get('mode_days', 0)} 天",
        f"耗时: {duration_text}",
    ]

    msg = str(summary.get("message", "")).strip()
    if msg:
        lines.append(f"备注: {msg}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="生产增强并发版 yfinance 日线缓存更新")
    parser.add_argument("--days", type=int, default=180, help="回拉最近多少天，默认 180")
    parser.add_argument("--pool-file", type=str, default=None, help="股票池文件路径")
    parser.add_argument("--symbols", type=str, default=None, help="直接传股票代码，逗号分隔")
    parser.add_argument("--workers", type=int, default=6, help="并发线程数，默认 6")
    parser.add_argument("--batch-size", type=int, default=60, help="每批处理股票数，默认 60")
    parser.add_argument("--force", action="store_true", help="忽略当日完成状态，强制重跑")
    parser.add_argument("--reset-today-state", action="store_true", help="清空今日断点状态后再跑")
    args = parser.parse_args()

    run_started_at = now_jst_iso()
    run_date = today_jst_str()

    if args.reset_today_state:
        clear_today_state()
        log_line("已清空今日断点状态")

    start_ts = time.time()
    log_line("=== 开始更新 yfinance 价格缓存（生产增强并发版） ===")

    try:
        symbols = load_symbol_pool(args.pool_file, args.symbols)
    except Exception as e:
        log_line(f"读取股票池失败: {e}")
        summary = {
            "date": run_date,
            "started_at": run_started_at,
            "finished_at": now_jst_iso(),
            "mode_days": args.days,
            "total": 0,
            "pending": 0,
            "success": 0,
            "skip": 0,
            "fail": 0,
            "total_rows": 0,
            "duration_seconds": 0,
            "status": "error",
            "message": f"读取股票池失败: {e}",
        }
        save_summary(summary)
        if telegram_configured():
            send_telegram_message(build_summary_message(summary))
        sys.exit(1)

    completed_today = set() if args.force else get_today_completed_symbols()

    pending = []
    skip_count = 0
    for symbol in symbols:
        code = denormalize_symbol(symbol)
        if not args.force and code in completed_today:
            skip_count += 1
            continue
        pending.append(symbol)

    log_line(f"股票数量: {len(symbols)}")
    log_line(f"待更新数量: {len(pending)}")
    log_line(f"已跳过数量: {skip_count}")
    log_line(f"回拉天数: {args.days}")
    log_line(f"并发线程数: {args.workers}")
    log_line(f"批大小: {args.batch_size}")
    log_line(f"缓存目录: {CACHE_DIR}")
    log_line(f"今日已完成数量: {len(completed_today)}")

    ok_count = 0
    fail_count = 0
    total_rows = 0
    failed_items = []

    done_so_far = skip_count

    for batch_no, batch_symbols in enumerate(batched(pending, args.batch_size), start=1):
        log_line(f"--- 开始第 {batch_no} 批，批量大小 {len(batch_symbols)} ---")

        futures = {}
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for symbol in batch_symbols:
                fut = executor.submit(update_one_symbol, symbol, args.days)
                futures[fut] = symbol

            for fut in as_completed(futures):
                symbol = futures[fut]
                code = denormalize_symbol(symbol)

                try:
                    result = fut.result()
                except Exception as e:
                    result = {
                        "symbol": symbol,
                        "ok": False,
                        "rows": 0,
                        "df": None,
                        "message": f"{code} 线程异常: {e}",
                    }

                done_so_far += 1
                log_line(f"[{done_so_far}/{len(symbols)}] {result['message']}")

                if result["ok"]:
                    try:
                        merge_records_to_daily_cache(result["df"])
                        ok_count += 1
                        total_rows += result["rows"]
                        mark_symbol_completed(code)
                    except Exception as e:
                        fail_count += 1
                        msg = f"{code} 写缓存失败: {e}"
                        failed_items.append((code, msg))
                        log_line(msg)
                else:
                    fail_count += 1
                    failed_items.append((code, result["message"]))

                    if should_mark_failed_as_completed(result["message"]):
                        mark_symbol_completed(code)
                        log_line(f"{code} 记为今日已处理（原因: 无数据/退市/不可用代码）")

        log_line(f"--- 第 {batch_no} 批完成 ---")

    cost = round(time.time() - start_ts, 2)

    summary = {
        "date": run_date,
        "started_at": run_started_at,
        "finished_at": now_jst_iso(),
        "mode_days": args.days,
        "total": len(symbols),
        "pending": len(pending),
        "success": ok_count,
        "skip": skip_count,
        "fail": fail_count,
        "total_rows": total_rows,
        "duration_seconds": cost,
        "status": "unknown",
        "message": "",
    }
    summary["status"] = build_status_label(summary)
    summary["message"] = failed_items[0][1] if failed_items else "更新完成"

    save_summary(summary)

    if telegram_configured():
        ok, msg = send_telegram_message(build_summary_message(summary))
        if ok:
            log_line("Telegram 更新摘要推送成功")
        else:
            log_line(f"Telegram 更新摘要推送失败: {msg}")

    log_line("=== 更新完成 ===")
    log_line(f"成功股票数: {ok_count}")
    log_line(f"跳过股票数: {skip_count}")
    log_line(f"失败股票数: {fail_count}")
    log_line(f"合计拉取行数: {total_rows}")
    log_line(f"耗时: {cost} 秒")
    log_line(f"摘要状态: {summary['status']}")

    print()
    print(f"股票数量: {len(symbols)}")
    print(f"成功: {ok_count} | 跳过: {skip_count} | 失败: {fail_count}")

    print()
    print("=== 失败股票（最多20条） ===")
    if failed_items:
        for item in failed_items[:20]:
            print(item)
    else:
        print("无")


if __name__ == "__main__":
    main()
