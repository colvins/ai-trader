#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/opt/ai-trader"
LOG_DIR="$BASE_DIR/data/logs"
mkdir -p "$LOG_DIR"

cd "$BASE_DIR"
source "$BASE_DIR/venv/bin/activate"

echo "[$(date '+%F %T')] === daily update start ==="

python scripts/update_universe.py
python scripts/update_price_cache.py --days 3 --pool-file data/universe_jp.csv --workers 6 --batch-size 60
python scripts/update_backtest_results.py
python scripts/run_backtest.py

echo "[$(date '+%F %T')] === daily update done ==="
