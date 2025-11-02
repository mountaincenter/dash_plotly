#!/usr/bin/env python3
"""
update_sectors_prices.py
J-Quants Standard APIで33業種別指数データを取得してparquet保存

取得データ: 東証33業種別指数（水産・農林業〜サービス業）

実行方法:
    python3 scripts/pipeline/update_sectors_prices.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_fetcher import JQuantsFetcher


def main():
    """メイン処理"""
    print("=" * 60)
    print("Update 33 Sector Indices (J-Quants Standard)")
    print("=" * 60)

    fetcher = JQuantsFetcher()

    print("\n[1] Getting latest trading day...")
    latest_trading_day = fetcher.get_latest_trading_day()
    print(f"Latest trading day: {latest_trading_day}")

    SECTOR_CODES = {
        "0040": "水産・農林業", "0041": "鉱業", "0042": "建設業", "0043": "食料品",
        "0044": "繊維製品", "0045": "パルプ・紙", "0046": "化学", "0047": "医薬品",
        "0048": "石油・石炭製品", "0049": "ゴム製品", "004A": "ガラス・土石製品",
        "004B": "鉄鋼", "004C": "非鉄金属", "004D": "金属製品", "004E": "機械",
        "004F": "電気機器", "0050": "輸送用機器", "0051": "精密機器", "0052": "その他製品",
        "0053": "電気・ガス業", "0054": "陸運業", "0055": "海運業", "0056": "空運業",
        "0057": "倉庫・運輸関連業", "0058": "情報・通信業", "0059": "卸売業",
        "005A": "小売業", "005B": "銀行業", "005C": "証券・商品先物取引業",
        "005D": "保険業", "005E": "その他金融業", "005F": "不動産業", "0060": "サービス業",
    }

    print(f"\n[2] Fetching 33 sector indices...")
    print(f"Target sectors: {len(SECTOR_CODES)}")

    all_frames = []
    for code, name in SECTOR_CODES.items():
        try:
            print(f"  Fetching {name} ({code})...", end=" ", flush=True)
            df = fetcher.get_indices(code=code, from_date=latest_trading_day, to_date=latest_trading_day)

            if df.empty:
                print(f"[WARN] No data")
                continue

            df["ticker"] = code
            df["name"] = name
            all_frames.append(df)
            print(f"OK (Close: {df.iloc[0]['Close']:.2f})")

        except Exception as e:
            print(f"[ERROR] {e}")
            continue

    if not all_frames:
        print("\n[ERROR] No data retrieved. Exiting.")
        return 1

    print(f"\n[3] Combining data...")
    result = pd.concat(all_frames, ignore_index=True)
    result.columns = result.columns.str.lower()

    column_order = ["date", "ticker", "name", "code", "open", "high", "low", "close"]
    result = result[[col for col in column_order if col in result.columns]]

    print(f"Total rows: {len(result)}")
    print(f"Columns: {result.columns.tolist()}")

    print("\n[4] Latest data (sample):")
    print("First 5 sectors:")
    print(result[["date", "ticker", "name", "close"]].head(5))
    print("\nLast 5 sectors:")
    print(result[["date", "ticker", "name", "close"]].tail(5))

    output_file = ROOT / "data" / "parquet" / "sectors_prices_max_1d.parquet"
    print(f"\n[5] Saving to {output_file.name}...")

    if output_file.exists():
        existing = pd.read_parquet(output_file)
        print(f"  Existing rows: {len(existing)}")
        existing = existing[existing["date"] < result["date"].min()]
        result = pd.concat([existing, result], ignore_index=True)
        print(f"  After merge: {len(result)} rows")

    result.to_parquet(output_file, index=False)

    file_size = output_file.stat().st_size
    print(f"  Saved: {file_size:,} bytes")

    print("\n" + "=" * 60)
    print("Update Completed")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
