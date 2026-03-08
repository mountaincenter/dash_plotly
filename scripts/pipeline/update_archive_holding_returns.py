#!/usr/bin/env python3
"""
update_archive_holding_returns.py
archiveの各レコードに d1〜d5 保持リターン（SHORT利益）を追記

d0 = backtest_date の始値→終値（既存の phase2）
d1〜d5 = backtest_date の始値 → +N日終値 でのSHORT利益

実行タイミング: 16:45パイプライン（save_backtest_to_archive の後）
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import upload_file
from common_cfg.s3cfg import load_s3_config

ARCHIVE_PATH = PARQUET_DIR / "backtest" / "grok_trending_archive.parquet"
PRICES_PATH = PARQUET_DIR / "grok_prices_max_1d.parquet"

HOLD_DAYS = [1, 2, 3, 5]


def main() -> int:
    print("=== Update archive with d1-d5 holding returns ===")

    if not ARCHIVE_PATH.exists():
        print("[SKIP] Archive not found")
        return 0

    archive = pd.read_parquet(ARCHIVE_PATH)
    archive["backtest_date"] = pd.to_datetime(archive["backtest_date"])

    if not PRICES_PATH.exists():
        print("[SKIP] grok_prices_max_1d.parquet not found")
        return 0

    prices = pd.read_parquet(PRICES_PATH)
    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices[prices["Close"].notna()].copy()

    # d1〜d5カラムが未計算のレコードだけ更新（NaN のもの）
    cols_needed = [f"short_profit_d{d}" for d in HOLD_DAYS]
    for col in cols_needed:
        if col not in archive.columns:
            archive[col] = None

    # 未計算の行を特定
    mask = archive[cols_needed].isna().any(axis=1)
    to_update = archive[mask]
    print(f"[INFO] Total: {len(archive)}, needs update: {len(to_update)}")

    if len(to_update) == 0:
        print("[SKIP] All records already have holding returns")
        return 0

    updated = 0
    for idx, row in to_update.iterrows():
        ticker = row["ticker"]
        bd = row["backtest_date"]
        buy_price = row["buy_price"]

        if pd.isna(buy_price) or buy_price <= 0:
            continue

        stock = prices[
            (prices["ticker"] == ticker) & (prices["date"] > bd)
        ].sort_values("date")

        if len(stock) == 0:
            continue

        for d in HOLD_DAYS:
            col = f"short_profit_d{d}"
            if pd.notna(archive.at[idx, col]):
                continue
            if d <= len(stock):
                close_d = stock.iloc[d - 1]["Close"]
                archive.at[idx, col] = (buy_price - close_d) * 100
                updated += 1

    print(f"[INFO] Updated {updated} cells")

    # 保存
    archive["backtest_date"] = archive["backtest_date"].astype(str)
    if "selection_date" in archive.columns:
        archive["selection_date"] = archive["selection_date"].astype(str)

    archive.to_parquet(ARCHIVE_PATH, index=False)
    print(f"[OK] Saved: {ARCHIVE_PATH}")

    # S3アップロード
    cfg = load_s3_config()
    if cfg:
        try:
            upload_file(cfg, ARCHIVE_PATH, "backtest/grok_trending_archive.parquet")
            print("[OK] Uploaded to S3")
        except Exception as e:
            print(f"[WARN] S3 upload failed: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
