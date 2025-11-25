#!/usr/bin/env python3
"""
fetch_prices_v2_1_0.py
grok_analysis_merged_20251121.parquetの131銘柄に対して既存のpricesファイルを更新

更新するファイル:
- prices_max_1d.parquet (既存を131銘柄でフィルタ + 不足銘柄を追加取得)
- prices_60d_5m.parquet (既存を131銘柄でフィルタ + 不足銘柄を追加取得)
- prices_max_5m.parquet (新規作成)
- prices_60d_1d.parquet (新規作成)
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Set

ROOT = Path(__file__).resolve().parents[2]  # improvement/scripts/ から2階層上
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.yfinance_fetcher import fetch_prices_for_tickers

# パス設定
IMPROVEMENT_DATA_DIR = ROOT / "improvement" / "data"
GROK_ANALYSIS_FILE = IMPROVEMENT_DATA_DIR / "grok_analysis_merged_20251121.parquet"

# 更新する価格データの設定
PRICE_CONFIGS = [
    {"period": "max", "interval": "1d", "filename": "prices_max_1d.parquet", "exists": True},
    {"period": "60d", "interval": "5m", "filename": "prices_60d_5m.parquet", "exists": True},
    {"period": "max", "interval": "5m", "filename": "prices_max_5m.parquet", "exists": False},
    {"period": "60d", "interval": "1d", "filename": "prices_60d_1d.parquet", "exists": False},
]


def load_tickers_from_grok_analysis() -> List[str]:
    """grok_analysis_merged_20251121.parquetからティッカーリストを取得"""
    if not GROK_ANALYSIS_FILE.exists():
        raise FileNotFoundError(
            f"grok_analysis_merged_20251121.parquet not found: {GROK_ANALYSIS_FILE}"
        )

    print(f"[INFO] Loading grok_analysis_merged_20251121.parquet: {GROK_ANALYSIS_FILE}")
    df = pd.read_parquet(GROK_ANALYSIS_FILE)
    tickers = df["ticker"].unique().tolist()
    print(f"  ✓ Loaded {len(tickers)} unique tickers")
    return tickers


def update_or_create_prices(
    target_tickers: Set[str], period: str, interval: str, output_path: Path, file_exists: bool
) -> bool:
    """
    既存ファイルを131銘柄でフィルタ、または新規作成

    Args:
        target_tickers: 対象銘柄セット (131銘柄)
        period: データ期間
        interval: データ間隔
        output_path: 保存先パス
        file_exists: 既存ファイルが存在するか

    Returns:
        成功時True、失敗時False
    """
    print(f"[INFO] Processing: period={period}, interval={interval}, exists={file_exists}")

    if file_exists and output_path.exists():
        # 既存ファイルを読み込み
        print(f"  📂 Loading existing file...")
        existing_df = pd.read_parquet(output_path)
        existing_tickers = set(existing_df["ticker"].unique())

        print(f"  📊 Existing: {len(existing_tickers)} tickers, {len(existing_df)} rows")

        # 131銘柄でフィルタ
        filtered_df = existing_df[existing_df["ticker"].isin(target_tickers)].copy()
        filtered_tickers = set(filtered_df["ticker"].unique())

        print(f"  🔍 After filter: {len(filtered_tickers)} tickers, {len(filtered_df)} rows")

        # 不足銘柄を特定
        missing_tickers = target_tickers - filtered_tickers

        if missing_tickers:
            print(f"  ⚠ Missing {len(missing_tickers)} tickers, fetching from yfinance...")
            try:
                new_df = fetch_prices_for_tickers(list(missing_tickers), period, interval, None)
                if not new_df.empty:
                    # 既存データと結合
                    combined_df = pd.concat([filtered_df, new_df], ignore_index=True)
                    print(f"  ✓ Fetched {len(new_df)} rows for missing tickers")
                else:
                    combined_df = filtered_df
                    print(f"  ⚠ No data retrieved for missing tickers")
            except Exception as e:
                print(f"  ✗ Failed to fetch missing tickers: {e}")
                combined_df = filtered_df
        else:
            print(f"  ✅ All 131 tickers present in existing file")
            combined_df = filtered_df

        # 保存
        combined_df.to_parquet(output_path, engine="pyarrow", index=False)
        final_tickers = combined_df["ticker"].nunique()
        print(f"  💾 Saved: {output_path} ({final_tickers} tickers, {len(combined_df)} rows)")
        return True

    else:
        # 新規作成
        print(f"  🆕 Creating new file from yfinance...")
        try:
            df = fetch_prices_for_tickers(list(target_tickers), period, interval, None)

            if df.empty:
                print(f"  ⚠ No data retrieved")
                df = pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])

            # 保存
            IMPROVEMENT_DATA_DIR.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_path, engine="pyarrow", index=False)
            print(f"  💾 Saved: {output_path} ({df['ticker'].nunique()} tickers, {len(df)} rows)")
            return True

        except Exception as e:
            print(f"  ✗ Failed: {e}")
            # エラー時は空ファイルを作成
            empty_df = pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])
            empty_df.to_parquet(output_path, engine="pyarrow", index=False)
            print(f"  ⚠ Created empty file due to error")
            return False


def main() -> int:
    print("=" * 60)
    print("Update Price Files for v2.1.0 Analysis (131 tickers)")
    print("=" * 60)

    # [STEP 1] ティッカーリスト取得
    print("\n[STEP 1] Loading tickers from grok_analysis_merged_20251121.parquet...")
    try:
        tickers = load_tickers_from_grok_analysis()
        target_tickers = set(tickers)
        print(f"  ✓ {len(target_tickers)} unique tickers")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 2] 価格データ更新/作成（4パターン）
    print("\n[STEP 2] Updating/creating price files...")
    success_count = 0

    for i, config in enumerate(PRICE_CONFIGS, 1):
        print(f"\n  [{i}/{len(PRICE_CONFIGS)}] {config['filename']}")
        output_path = IMPROVEMENT_DATA_DIR / config["filename"]

        if update_or_create_prices(
            target_tickers, config["period"], config["interval"], output_path, config["exists"]
        ):
            success_count += 1

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Target tickers: {len(target_tickers)}")
    print(f"Price datasets: {success_count}/{len(PRICE_CONFIGS)} successful")
    print("=" * 60)

    if success_count == len(PRICE_CONFIGS):
        print("\n✅ All price files updated successfully!")
        return 0
    else:
        print(f"\n⚠ Only {success_count}/{len(PRICE_CONFIGS)} datasets updated")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
