#!/usr/bin/env python3
"""
update_series_prices.py
J-Quants Standard APIで17業種別指数データを取得してparquet保存

取得データ: TOPIX-17シリーズ（食品〜不動産）

実行方法:
    python3 scripts/pipeline/update_series_prices.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_fetcher import JQuantsFetcher
from common_cfg.s3io import download_file
from common_cfg.s3cfg import load_s3_config


def main():
    """メイン処理"""
    print("=" * 60)
    print("Update 17 Series Indices (J-Quants Standard)")
    print("=" * 60)

    fetcher = JQuantsFetcher()

    # S3から既存ファイルをダウンロード（GitHub Actions対応）
    output_file = ROOT / "data" / "parquet" / "series_prices_max_1d.parquet"
    print("\n[0] Downloading existing file from S3 (if exists)...")
    try:
        cfg = load_s3_config()
        success = download_file(cfg, "series_prices_max_1d.parquet", output_file)
        if success:
            df_existing = pd.read_parquet(output_file)
            print(f"  ✓ Downloaded from S3: {len(df_existing)} rows")
        else:
            print("  [INFO] No existing file in S3, will create new")
    except Exception as e:
        print(f"  [WARN] S3 download failed: {e}")
        print("  [INFO] Will create new file")

    print("\n[1] Getting latest trading day...")
    latest_trading_day = fetcher.get_latest_trading_day()
    print(f"Latest trading day: {latest_trading_day}")

    SERIES_CODES = {
        "0080": "食品", "0081": "エネルギー資源", "0082": "建設・資材",
        "0083": "素材・化学", "0084": "医薬品", "0085": "自動車・輸送機",
        "0086": "鉄鋼・非鉄", "0087": "機械", "0088": "電機・精密",
        "0089": "情報通信・サービスその他", "008A": "電力・ガス",
        "008B": "運輸・物流", "008C": "商社・卸売", "008D": "小売",
        "008E": "銀行", "008F": "金融(除く銀行)", "0090": "不動産",
    }

    print(f"\n[2] Fetching 17 series indices...")
    print(f"Target series: {len(SERIES_CODES)}")
    print(f"Fetching latest trading day data")

    all_frames = []
    for code, name in SERIES_CODES.items():
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

    print("\n[4] Latest data:")
    print(result[["date", "ticker", "name", "close"]])

    print(f"\n[5] Saving to {output_file.name}...")

    # 既存ファイルがあれば読み込んで追記
    if output_file.exists():
        existing = pd.read_parquet(output_file)
        print(f"  Existing rows: {len(existing)}")
        # 同じ日付のデータは削除
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
