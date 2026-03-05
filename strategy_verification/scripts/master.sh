#!/usr/bin/env bash
# master.sh — raw → processed → chapters を一気通貫で再現
# 使い方: cd strategy_verification && bash scripts/master.sh
set -euo pipefail

echo "=== Strategy Verification Pipeline ==="
echo "Start: $(date)"

# TODO: 各ステップを追加
# python3 scripts/01_prepare_raw_data.py
# python3 scripts/02_detect_anomalies.py
# python3 scripts/03_generate_clean_archive.py
# python3 scripts/04_generate_reports.py

echo "Done: $(date)"
