#!/usr/bin/env python3
"""
update_option_prices.py
J-Quants Standard APIで日経225オプション四本値データを取得してparquet保存

取得データ: 日経225指数オプション期近限月のみ（流動性が高く市場心理を反映）

実行方法:
    python3 scripts/pipeline/update_option_prices.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_fetcher import JQuantsFetcher

print("=" * 60)
print("Update Nikkei 225 Options - Front Month Only")
print("=" * 60)

# J-Quantsクライアント初期化
fetcher = JQuantsFetcher()

# 最新営業日を取得
print("\n[1] Getting latest trading day...")
latest_trading_day = fetcher.get_latest_trading_day()
print(f"Latest trading day: {latest_trading_day}")

print(f"\n[2] Fetching Nikkei 225 options data...")

try:
    df = fetcher.get_index_options(date=latest_trading_day)

    if df.empty:
        print("[WARN] No options data retrieved. Exiting.")
        sys.exit(1)

    print(f"Retrieved {len(df)} option contracts (all maturities)")

except Exception as e:
    print(f"[ERROR] {e}")
    sys.exit(1)

# カラム名を統一（Date -> date, Close -> close等）
df.columns = df.columns.str.lower()

# 期近限月のみを抽出
if "contractmonth" in df.columns:
    # 最も近い限月を特定
    front_month = df["contractmonth"].min()
    df = df[df["contractmonth"] == front_month].copy()
    print(f"Filtered to front month only: {front_month}")
    print(f"Front month contracts: {len(df)}")
else:
    print("[ERROR] contractmonth column not found")
    sys.exit(1)

# 重要なカラムを選択
important_cols = [
    "date", "code", "contractmonth", "strikeprice", "putcalldivision",
    "wholedayopen", "wholedayhigh", "wholedaylow", "wholedayclose",
    "volume", "openinterest", "turnovervalue",
    "settlementprice", "theoreticalprice", "underlyingprice",
    "impliedvolatility", "basevolatility"
]
available_cols = [col for col in important_cols if col in df.columns]
result = df[available_cols].copy()

# Put/Call区分を文字列に変換
if "putcalldivision" in result.columns:
    result["putcalldivision"] = result["putcalldivision"].map({"1": "Put", "2": "Call"})

print(f"\n[3] Data summary:")
print(f"Total contracts: {len(result)}")
print(f"Contract month: {front_month}")
print(f"Date: {result['date'].iloc[0] if 'date' in result.columns else 'N/A'}")

# Put/Call別の集計
if "putcalldivision" in result.columns:
    print("\nContract breakdown:")
    print(result["putcalldivision"].value_counts().to_string())

# サンプルデータを表示
print("\n[4] Sample data (first 5 contracts):")
display_cols = ["date", "contractmonth", "strikeprice", "putcalldivision", "wholedayclose", "volume"]
display_available = [col for col in display_cols if col in result.columns]
print(result[display_available].head(5).to_string(index=False))

# Parquet保存
output_file = ROOT / "data" / "parquet" / "option_prices_max_1d.parquet"
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
