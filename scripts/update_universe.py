#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

JQUANTS_API_KEY = os.getenv("JQUANTS_API_KEY")

BASE_DIR = Path("/opt/ai-trader")
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

UNIVERSE_FILE = DATA_DIR / "universe_jp.csv"
LOG_FILE = DATA_DIR / "update_universe.log"

API_URL = "https://api.jquants.com/v2/equities/master"


def log(msg: str):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def check_env():
    if not JQUANTS_API_KEY:
        raise RuntimeError("缺少环境变量: JQUANTS_API_KEY")


def jq_headers():
    check_env()
    return {
        "x-api-key": JQUANTS_API_KEY,
        "Accept": "application/json",
    }


def jq_get(url: str, params=None, max_retries: int = 3):
    last_error = None

    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=jq_headers(), params=params, timeout=30)

            if r.status_code == 429:
                wait_sec = 2 + attempt * 2
                log(f"限流 429，等待 {wait_sec}s 后重试")
                time.sleep(wait_sec)
                continue

            if not r.ok:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:500]}")

            return r.json()

        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(1.5)
            else:
                raise last_error


def normalize_local_code_to_symbol(code: str) -> str:
    code = str(code).strip()
    if len(code) >= 4 and code[:4].isdigit():
        return code[:4]
    return code


def _clean_text(v):
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def _flatten_leaf_values(obj, prefix=""):
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            out.update(_flatten_leaf_values(v, key))
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:5]):
            key = f"{prefix}[{i}]"
            out.update(_flatten_leaf_values(v, key))
    else:
        out[prefix] = obj
    return out


def _normalize_key(s: str) -> str:
    return (
        str(s)
        .lower()
        .replace("_", "")
        .replace(".", "")
        .replace("[", "")
        .replace("]", "")
        .strip()
    )


def _find_first_value(flat_map: dict, candidate_keywords: list[str]) -> str:
    normalized = []
    for path, value in flat_map.items():
        val = _clean_text(value)
        if not val:
            continue
        normalized.append((path, _normalize_key(path), val))

    for kw in candidate_keywords:
        kw_norm = _normalize_key(kw)
        for _, key_norm, val in normalized:
            if key_norm == kw_norm:
                return val

    for kw in candidate_keywords:
        kw_norm = _normalize_key(kw)
        for _, key_norm, val in normalized:
            if key_norm.endswith(kw_norm):
                return val

    for kw in candidate_keywords:
        kw_norm = _normalize_key(kw)
        for _, key_norm, val in normalized:
            if kw_norm in key_norm:
                return val

    return ""


def _extract_item_list(data: dict):
    for key in ["info", "data", "items", "results", "listed", "listed_info", "equities", "master"]:
        v = data.get(key)
        if isinstance(v, list) and v:
            return v

    for _, v in data.items():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v

    return []


def update_universe():
    rows = []
    pagination_key = None
    page = 0
    sample_logged = False

    while True:
        params = {}
        if pagination_key:
            params["pagination_key"] = pagination_key

        data = jq_get(API_URL, params=params)
        page += 1

        items = _extract_item_list(data)
        if not items:
            log(f"第 {page} 页没有 items，顶层 keys: {list(data.keys())}")
            break

        if not sample_logged and items:
            flat_sample = _flatten_leaf_values(items[0])
            log("样本记录字段路径（前80个）:")
            for k in list(flat_sample.keys())[:80]:
                log(f"  - {k}")
            log(f"样本记录内容片段: {json.dumps(items[0], ensure_ascii=False)[:1200]}")
            sample_logged = True

        for x in items:
            flat = _flatten_leaf_values(x)

            raw_code = _find_first_value(flat, [
                "Code", "code", "LocalCode", "local_code", "IssueCode", "ticker", "symbol"
            ])
            if not raw_code:
                continue

            symbol = normalize_local_code_to_symbol(raw_code)

            name = _find_first_value(flat, [
                "CoName", "CoNameEn", "CompanyName", "company_name",
                "IssueName", "SecurityName", "name"
            ])

            market_code = _find_first_value(flat, [
                "Mkt", "MarketCode", "market_code", "MarketSegmentCode", "section"
            ])

            sector17 = _find_first_value(flat, [
                "S17", "Sector17Code", "sector17_code", "sector_17_code"
            ])

            rows.append({
                "symbol": _clean_text(symbol),
                "local_code_raw": _clean_text(raw_code),
                "name": _clean_text(name),
                "market_code": _clean_text(market_code),
                "sector17_code": _clean_text(sector17),
            })

        log(f"第 {page} 页读取完成，累计 {len(rows)} 条")

        pagination_key = data.get("pagination_key")
        if not pagination_key:
            break

    if not rows:
        raise RuntimeError("股票池返回为空")

    df = pd.DataFrame(rows)
    df = df[df["symbol"].astype(str).str.match(r"^\d{4}$", na=False)].copy()
    df = df.drop_duplicates(subset=["symbol", "local_code_raw"]).sort_values(["symbol", "local_code_raw"])

    log(f"name 非空数: {(df['name'].astype(str).str.strip() != '').sum()}")
    log(f"market_code 非空数: {(df['market_code'].astype(str).str.strip() != '').sum()}")
    log(f"sector17_code 非空数: {(df['sector17_code'].astype(str).str.strip() != '').sum()}")

    df.to_csv(UNIVERSE_FILE, index=False, encoding="utf-8-sig")
    log(f"股票池更新完成，写入 {len(df)} 条到 {UNIVERSE_FILE}")


if __name__ == "__main__":
    update_universe()
