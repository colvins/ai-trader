from __future__ import annotations

import re
from datetime import datetime
from html import unescape
from typing import List, Dict
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from time_filter_utils import is_within_natural_days


BASE_URL = "https://www.release.tdnet.info/inbs/"
MAIN_URL = urljoin(BASE_URL, "I_main_00.html")
USER_AGENT = "Mozilla/5.0 (compatible; ai-trader-tdnet/1.0)"
LOCAL_TZ = datetime.now().astimezone().tzinfo


def _fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=20) as response:
        return response.read().decode("utf-8", errors="ignore")


def _clean_text(value: str) -> str:
    text = unescape(value)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\xa0", " ")
    return " ".join(text.split())


def _extract_candidate_list_paths(main_html: str) -> List[str]:
    paths: List[str] = []

    match = re.search(r'<iframe[^>]+id="main_list"[^>]+src="([^"]+)"', main_html, re.IGNORECASE)
    if match:
        paths.append(match.group(1))

    options = re.findall(r'<option[^>]+value="(I_list_\d{3}_\d{8}\.html)"', main_html, re.IGNORECASE)
    for path in options:
        if path not in paths:
            paths.append(path)

    if not paths:
        raise ValueError("Failed to locate TDnet list page.")

    return paths


def _extract_list_date(list_html: str) -> str:
    match = re.search(r'<div id="kaiji-date-1">(\d{4})年(\d{2})月(\d{2})日</div>', list_html)
    if not match:
        raise ValueError("Failed to locate TDnet list date.")
    year, month, day = match.groups()
    return f"{year}-{month}-{day}"


def _extract_page_paths(list_html: str, current_path: str) -> List[str]:
    paths = [current_path]
    matches = re.findall(r"I_list_\d{3}_\d{8}\.html", list_html)
    for path in matches:
        if path not in paths:
            paths.append(path)
    return paths


def _parse_rows(list_html: str, list_date: str) -> List[Dict[str, str]]:
    rows = re.findall(r"<tr>(.*?)</tr>", list_html, re.DOTALL | re.IGNORECASE)
    results: List[Dict[str, str]] = []

    for row in rows:
        time_match = re.search(r'class="[^"]*kjTime[^"]*"[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
        symbol_match = re.search(r'class="[^"]*kjCode[^"]*"[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
        title_match = re.search(
            r'class="[^"]*kjTitle[^"]*"[^>]*>.*?<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
            row,
            re.DOTALL | re.IGNORECASE,
        )

        if not (time_match and symbol_match and title_match):
            continue

        time_text = _clean_text(time_match.group(1))
        symbol = _clean_text(symbol_match.group(1))
        pdf_path, raw_title = title_match.groups()
        title = _clean_text(raw_title)

        results.append(
            {
                "symbol": symbol,
                "title": title,
                "date": f"{list_date} {time_text}",
                "url": urljoin(BASE_URL, pdf_path),
            }
        )

    return results


def _filter_recent_tdnet_items(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    return [
        item
        for item in items
        if is_within_natural_days(item.get("date", ""), days=2, default_tz=LOCAL_TZ)
    ]


def fetch_tdnet_disclosures() -> List[Dict[str, str]]:
    main_html = _fetch_text(MAIN_URL)
    candidate_paths = _extract_candidate_list_paths(main_html)

    first_list_path = ""
    first_list_html = ""
    list_date = ""
    page_paths: List[str] = []

    for path in candidate_paths:
        page_html = _fetch_text(urljoin(BASE_URL, path))
        page_date = _extract_list_date(page_html)
        page_results = _parse_rows(page_html, page_date)
        if page_results:
            first_list_path = path
            first_list_html = page_html
            list_date = page_date
            page_paths = _extract_page_paths(page_html, path)
            break

    if not first_list_path:
        return []

    results: List[Dict[str, str]] = []
    for path in page_paths:
        page_html = first_list_html if path == first_list_path else _fetch_text(urljoin(BASE_URL, path))
        results.extend(_parse_rows(page_html, list_date))

    return _filter_recent_tdnet_items(results)


def fetch_tdnet() -> List[Dict[str, str]]:
    return fetch_tdnet_disclosures()


def test_fetch_tdnet() -> None:
    items = fetch_tdnet()
    print(f"tdnet_recent_count={len(items)}")
    for item in items:
        print(item)


if __name__ == "__main__":
    test_fetch_tdnet()
