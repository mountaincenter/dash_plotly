#!/usr/bin/env python3
"""
test_jquants_topix.py
J-Quants APIでTOPIX指数データを取得してテスト

実行方法:
    python3 scripts/pipeline/test_jquants_topix.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.jquants_fetcher import JQuantsFetcher

print("=" * 60)
print("J-Quants TOPIX Indices Test")
print("=" * 60)

# J-Quantsクライアント初期化
fetcher = JQuantsFetcher()

# 最新営業日を取得
print("\n[1] Getting latest trading day...")
latest_trading_day = fetcher.get_latest_trading_day()
print(f"Latest trading day: {latest_trading_day}")

# TOPIX指数データ取得（raw responseを確認）
print(f"\n[2] Fetching TOPIX indices for {latest_trading_day}...")
print("[DEBUG] Checking raw API response...")

from scripts.lib.jquants_client import JQuantsClient
client = JQuantsClient()
raw_data = client.request("/indices/topix", params={"from": latest_trading_day, "to": latest_trading_day})

print(f"[DEBUG] API Response keys: {raw_data.keys()}")
print(f"[DEBUG] First item: {raw_data.get('topix', [{}])[0] if raw_data.get('topix') else 'No data'}")

df = fetcher.get_indices_topix(from_date=latest_trading_day, to_date=latest_trading_day)

if df.empty:
    print("[ERROR] No data retrieved")
    sys.exit(1)

print(f"\n[3] Data shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")

# データの内容を確認
print("\n[4] Sample data:")
print(df.head(10))

# IndexCodeの種類を確認
if "IndexCode" in df.columns:
    unique_codes = df["IndexCode"].unique()
    print(f"\n[5] Unique IndexCode count: {len(unique_codes)}")
    print(f"Sample codes: {unique_codes[:10].tolist()}")

    # 主要指数を抽出
    print("\n[6] Main indices:")
    main_codes = {
        "0000": "TOPIX",
        "0010": "TOPIX Core30",
        "0020": "TOPIX Large70",
        "0050": "TOPIX Mid400",
        "0060": "TOPIX Small",
        "0300": "TOPIX-Prime",
        "0400": "TOPIX-Standard",
        "0500": "TOPIX-Growth",
    }

    for code, name in main_codes.items():
        row = df[df["IndexCode"] == code]
        if not row.empty:
            close = row.iloc[0]["Close"]
            print(f"  {name} ({code}): {close}")
        else:
            print(f"  {name} ({code}): [NOT FOUND]")

    # 33業種別指数を抽出
    print("\n[7] Sector indices (33 industries):")
    sector_df = df[df["IndexCode"].str.startswith("1")].copy()

    if not sector_df.empty:
        # 前日比を計算（Close - Open を簡易的に使用、本来は前日終値との比較が必要）
        sector_df["Change"] = sector_df["Close"] - sector_df["Open"]
        sector_df["ChangeRate"] = (sector_df["Change"] / sector_df["Open"] * 100).round(2)
        sector_df = sector_df.sort_values("ChangeRate", ascending=False)

        print(f"\n  Total sector indices: {len(sector_df)}")
        print("\n  Top 3 (rising):")
        for i, row in sector_df.head(3).iterrows():
            print(f"    {row['IndexCode']}: {row['ChangeRate']:+.2f}% (Close: {row['Close']})")

        print("\n  Bottom 3 (falling):")
        for i, row in sector_df.tail(3).iterrows():
            print(f"    {row['IndexCode']}: {row['ChangeRate']:+.2f}% (Close: {row['Close']})")
    else:
        print("  [NOT FOUND]")

print("\n" + "=" * 60)
print("Test Completed")
print("=" * 60)
