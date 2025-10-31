#!/usr/bin/env python3
"""
fetch_index_prices.py
マーケット指数・ETF（為替・先物除く）の価格データを取得
index_prices_{period}_{interval}.parquet を生成
futures_prices_{period}_{interval}.parquet を生成（先物用）
prices_{period}_{interval}.parquet と同じカラム構造
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[2]  # scripts/pipeline/ から2階層上 = プロジェクトルート
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.yfinance_fetcher import fetch_prices_for_tickers
from common_cfg.paths import PARQUET_DIR

# 取得する指数・ETF（為替・先物除く、東証取引時間帯のみ）
INDEX_TICKERS = [
    # 主要指数
    "^N225",      # 日経平均株価

    # プライム市場
    "1306.T",     # TOPIX連動ETF
    "1311.T",     # TOPIX Core30 ETF
    "1591.T",     # JPX日経400 ETF
    "1489.T",     # 日経高配当株50 ETF

    # グロース市場
    "2516.T",     # グロース250指数 ETF
    "1563.T",     # マザーズコア指数 ETF
    "1554.T",     # JASDAQ-TOP20 ETF

    # センチメント
    "1570.T",     # 日経レバレッジETF
    "1357.T",     # 日経ダブルインバース
]

# 先物（24時間取引のため別ファイル）
FUTURES_TICKERS = [
    "NKD=F",      # 日経225先物
]

# 取得する価格データの設定（既存のprices_*と同じパターン）
INDEX_PRICE_CONFIGS = [
    {"period": "60d", "interval": "15m", "filename": "index_prices_60d_15m.parquet"},
    {"period": "60d", "interval": "5m", "filename": "index_prices_60d_5m.parquet"},
    {"period": "730d", "interval": "1h", "filename": "index_prices_730d_1h.parquet", "fallback_period": "max"},
    {"period": "max", "interval": "1d", "filename": "index_prices_max_1d.parquet"},
    {"period": "max", "interval": "1mo", "filename": "index_prices_max_1mo.parquet"},
]

FUTURES_PRICE_CONFIGS = [
    {"period": "60d", "interval": "15m", "filename": "futures_prices_60d_15m.parquet"},
    {"period": "60d", "interval": "5m", "filename": "futures_prices_60d_5m.parquet"},
    {"period": "730d", "interval": "1h", "filename": "futures_prices_730d_1h.parquet", "fallback_period": "max"},
    {"period": "max", "interval": "1d", "filename": "futures_prices_max_1d.parquet"},
    {"period": "max", "interval": "1mo", "filename": "futures_prices_max_1mo.parquet"},
]


def fetch_and_save_index_prices(
    tickers: List[str],
    period: str,
    interval: str,
    output_path: Path,
    fallback_period: str = None
) -> bool:
    """
    指定された指数・ETFの価格データを取得して保存

    Args:
        tickers: ティッカーリスト
        period: データ期間
        interval: データ間隔
        output_path: 保存先パス
        fallback_period: エラー時のフォールバック期間（730d→maxなど）

    Returns:
        成功時True、失敗時False
    """
    print(f"[INFO] Fetching index prices: period={period}, interval={interval}")

    try:
        df = fetch_prices_for_tickers(tickers, period, interval, fallback_period)

        if df.empty:
            print(f"  ⚠ No data retrieved, creating empty file")
            df = pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])

        # 保存
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, engine="pyarrow", index=False)
        print(f"  ✓ Saved: {output_path} ({len(df)} rows)")
        return True

    except Exception as e:
        print(f"  ✗ Failed: {e}")

        # fallback_periodがあればリトライ（全体のフォールバック）
        if fallback_period:
            print(f"  [INFO] Retrying entire batch with fallback period={fallback_period}")
            try:
                df = fetch_prices_for_tickers(tickers, fallback_period, interval, None)
                PARQUET_DIR.mkdir(parents=True, exist_ok=True)
                df.to_parquet(output_path, engine="pyarrow", index=False)
                print(f"  ✓ Saved with fallback: {output_path} ({len(df)} rows)")
                return True
            except Exception as e2:
                print(f"  ✗ Fallback also failed: {e2}")

        # エラー時は空ファイルを作成（パイプライン継続のため）
        empty_df = pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])
        empty_df.to_parquet(output_path, engine="pyarrow", index=False)
        print(f"  ⚠ Created empty file due to error")
        return False


def main() -> int:
    print("=" * 80)
    print("Fetch Index/ETF & Futures Prices (yfinance) - 為替除く")
    print("=" * 80)

    print(f"\n[INFO] Target tickers:")
    print(f"  - Index/ETF: {len(INDEX_TICKERS)} tickers")
    print(f"    {', '.join(INDEX_TICKERS)}")
    print(f"  - Futures: {len(FUTURES_TICKERS)} tickers")
    print(f"    {', '.join(FUTURES_TICKERS)}")

    # [STEP 1] 指数・ETF価格データ取得
    print("\n[STEP 1] Fetching index/ETF prices from yfinance...")
    index_success_count = 0

    for i, config in enumerate(INDEX_PRICE_CONFIGS, 1):
        print(f"\n  [{i}/{len(INDEX_PRICE_CONFIGS)}] {config['filename']}")
        output_path = PARQUET_DIR / config["filename"]
        fallback = config.get("fallback_period")

        if fetch_and_save_index_prices(
            INDEX_TICKERS,
            config["period"],
            config["interval"],
            output_path,
            fallback
        ):
            index_success_count += 1

    print(f"\n  ✓ Successfully fetched {index_success_count}/{len(INDEX_PRICE_CONFIGS)} index/ETF datasets")

    # [STEP 2] 先物価格データ取得
    print("\n[STEP 2] Fetching futures prices from yfinance...")
    futures_success_count = 0

    for i, config in enumerate(FUTURES_PRICE_CONFIGS, 1):
        print(f"\n  [{i}/{len(FUTURES_PRICE_CONFIGS)}] {config['filename']}")
        output_path = PARQUET_DIR / config["filename"]
        fallback = config.get("fallback_period")

        if fetch_and_save_index_prices(
            FUTURES_TICKERS,
            config["period"],
            config["interval"],
            output_path,
            fallback
        ):
            futures_success_count += 1

    print(f"\n  ✓ Successfully fetched {futures_success_count}/{len(FUTURES_PRICE_CONFIGS)} futures datasets")

    # サマリー
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Index/ETF tickers: {len(INDEX_TICKERS)}")
    print(f"Futures tickers: {len(FUTURES_TICKERS)}")
    print(f"Index/ETF datasets: {index_success_count}/{len(INDEX_PRICE_CONFIGS)} successful")
    print(f"Futures datasets: {futures_success_count}/{len(FUTURES_PRICE_CONFIGS)} successful")

    # 生成されたファイル一覧
    print("\n[Generated files - Index/ETF]")
    for config in INDEX_PRICE_CONFIGS:
        filepath = PARQUET_DIR / config["filename"]
        if filepath.exists():
            size_kb = filepath.stat().st_size / 1024
            print(f"  ✓ {config['filename']} ({size_kb:.1f} KB)")
        else:
            print(f"  ✗ {config['filename']} (not created)")

    print("\n[Generated files - Futures]")
    for config in FUTURES_PRICE_CONFIGS:
        filepath = PARQUET_DIR / config["filename"]
        if filepath.exists():
            size_kb = filepath.stat().st_size / 1024
            print(f"  ✓ {config['filename']} ({size_kb:.1f} KB)")
        else:
            print(f"  ✗ {config['filename']} (not created)")

    print("=" * 80)
    print("  ℹ S3アップロードは update_manifest.py で一括実行されます")

    print("\n✅ Index/ETF & Futures price fetch completed!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
