"""Prompt and text block builders for downstream AI/manual review."""

from __future__ import annotations


def news_titles(news_items: list[dict], max_items: int = 2) -> list[str]:
    titles = []
    for item in news_items or []:
        title = str(item.get("title", "")).strip()
        if title:
            titles.append(title)
        if len(titles) >= max_items:
            break
    return titles


def build_ai_prompt(stock: dict, mode: str) -> str:
    news_block_items = news_titles(stock.get("news_items", []), max_items=2)
    news_block = "\n".join([f"- {item}" for item in news_block_items]) if news_block_items else "- 无近期有效新闻"

    return (
        "===== AI分析输入（复制到ChatGPT）=====\n\n"
        f"模式: {mode}\n"
        f"股票: {stock.get('symbol', '-')}\n"
        f"当前价格: {stock.get('close')}\n"
        f"昨收价格: {stock.get('prev_close')}\n"
        f"本地评分: {stock.get('score', '-')}\n"
        f"本地理由: {stock.get('reason', '')}\n\n"
        "技术面:\n"
        f"- 今日涨幅: {stock.get('day_change_pct', '-')}%\n"
        f"- 日内涨幅(开收): {stock.get('intraday_pct', '-')}%\n"
        f"- 振幅: {stock.get('amplitude_pct', '-')}%\n"
        f"- 量比(5日): {stock.get('amount_ratio_5', '-')}\n"
        f"- 3日动量: {stock.get('momentum_3_pct', '-')}%\n"
        f"- 5日动量: {stock.get('momentum_5_pct', '-')}%\n"
        f"- 距5日高点: {stock.get('dist_to_high_5_pct', '-')}%\n"
        f"- 距20日高点: {stock.get('dist_to_high_20_pct', '-')}%\n"
        f"- 收盘位置: {stock.get('close_position', '-')}\n\n"
        "新闻:\n"
        f"{news_block}\n\n"
        "请判断：\n"
        "1. 是否值得关注（是/否）\n"
        "2. 更偏哪种模式（breakout/trend/dip/不明确）\n"
        "3. 核心逻辑（一句话）\n"
        "4. 最大风险（一句话）\n\n"
        "===== 结束 ====="
    )
