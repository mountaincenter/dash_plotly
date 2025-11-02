#!/usr/bin/env python3
"""
update_topix_jquants.py
J-Quants APIでTOPIX系指数データを取得してparquet保存

実行方法:
    python3 scripts/pipeline/update_topix_jquants.py
"""
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_fetcher import JQuantsFetcher

print("=" * 60)
print("Update TOPIX Indices (J-Quants)")
print("=" * 60)

# J-Quantsクライアント初期化
fetcher = JQuantsFetcher()

# 最新営業日を取得
print("\n[1] Getting latest trading day...")
latest_trading_day = fetcher.get_latest_trading_day()
print(f"Latest trading day: {latest_trading_day}")

# TOPIX系指数のコード定義
TOPIX_CODES = {
    "0000": "TOPIX",
    "0500": "TOPIX-Prime",
    "0501": "TOPIX-Standard",
    "0502": "TOPIX-Growth",
}

print(f"\n[2] Fetching TOPIX indices...")
print(f"Target codes: {list(TOPIX_CODES.keys())}")

# 全データを取得（codeを指定しないと全指数）
all_frames = []
for code, name in TOPIX_CODES.items():
    try:
        print(f"  Fetching {name} ({code})...", end=" ", flush=True)
        df = fetcher.get_indices(code=code, from_date=latest_trading_day, to_date=latest_trading_day)

        if df.empty:
            print(f"[WARN] No data")
            continue

        # メタデータを追加
        df["ticker"] = code
        df["name"] = name
        df["source"] = "jquants"

        all_frames.append(df)
        print(f"OK (Close: {df.iloc[0]['Close']:.2f})")

    except Exception as e:
        print(f"[ERROR] {e}")
        continue

if not all_frames:
    print("\n[ERROR] No data retrieved. Exiting.")
    sys.exit(1)

# データ結合
print(f"\n[3] Combining data...")
result = pd.concat(all_frames, ignore_index=True)

# カラム順序を整理
column_order = ["date", "ticker", "name", "open", "high", "low", "close", "source"]
# 大文字小文字を統一（Date -> date, Close -> close等）
result.columns = result.columns.str.lower()
result = result[[col for col in column_order if col in result.columns]]

print(f"Total rows: {len(result)}")
print(f"Columns: {result.columns.tolist()}")

# 最新データを表示
print("\n[4] Latest data:")
print(result[["ticker", "name", "date", "close"]])

# Parquet保存
output_file = ROOT / "data" / "parquet" / "topix_max_1d.parquet"
print(f"\n[5] Saving to {output_file.name}...")

# 既存ファイルがあれば読み込んで追記
if output_file.exists():
    existing = pd.read_parquet(output_file)
    print(f"  Existing rows: {len(existing)}")

    # 同じ日付のデータは上書き
    existing = existing[existing["date"] < result["date"].min()]
    result = pd.concat([existing, result], ignore_index=True)
    print(f"  After merge: {len(result)} rows")

result.to_parquet(output_file, index=False)

# ファイルサイズ確認
file_size = output_file.stat().st_size
print(f"  Saved: {file_size:,} bytes")

print("\n" + "=" * 60)
print("Update Completed")
print("=" * 60)
