#!/usr/bin/env bash
# One command demo:  ./scripts/run_all.sh [rows]     (default 2,000,000 rows)
set -e
cd "$(dirname "$0")/.."
ROWS="${1:-2000000}"
pip install -q -r requirements.txt
python -m etl.generate_data --rows "$ROWS" --reset     # 1. create raw data in cloud storage (bronze)
python -m etl.pipeline                                  # 2. clean -> partitioned Parquet (silver)
python app.py                                           # 3. API + dashboard on http://localhost:5000
