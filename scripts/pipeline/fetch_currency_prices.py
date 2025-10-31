#!/usr/bin/env python3
"""
fetch_currency_prices.py
為替レートの価格データを取得
currency_prices_{period}_{interval}.parquet を生成
prices_{period}_{interval}.parquet と同じカラム構造

注意:
- yfinanceではBid/Askは取得不可（Open/High/Low/Closeのみ）
- Volumeは常に0（為替には出来高の概念がない）
- 24時間取引だが、LightGBM特徴量用に日足・時間足のみ取得
- 5m/15mは株価と時間帯が合わないため除外
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

# 取得する為替ペア
CURRENCY_TICKERS = [
    "JPY=X",      # USD/JPY（ドル円）
    "EURJPY=X",   # EUR/JPY（ユーロ円）
]

# 取得する価格データの設定（LightGBM特徴量用）
# 5m/15mは除外（株価との時間帯不一致のため）
CURRENCY_PRICE_CONFIGS = [
    {"period": "730d", "interval": "1h", "filename": "currency_prices_730d_1h.parquet", "fallback_period": "max"},
    {"period": "max", "interval": "1d", "filename": "currency_prices_max_1d.parquet"},
    {"period": "max", "interval": "1mo", "filename": "currency_prices_max_1mo.parquet"},
]


def fetch_and_save_currency_prices(
    tickers: List[str],
    period: str,
    interval: str,
    output_path: Path,
    fallback_period: str = None
) -> bool:
    """
    指定された為替ペアの価格データを取得して保存

    Args:
        tickers: ティッカーリスト
        period: データ期間
        interval: データ間隔
        output_path: 保存先パス
        fallback_period: エラー時のフォールバック期間（730d→maxなど）

    Returns:
        成功時True、失敗時False
    """
    print(f"[INFO] Fetching currency prices: period={period}, interval={interval}")

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
    print("Fetch Currency Prices (yfinance) - LightGBM特徴量用")
    print("=" * 80)

    print(f"\n[INFO] Target currency pairs: {len(CURRENCY_TICKERS)}")
    print("  " + ", ".join(CURRENCY_TICKERS))
    print("\n  ℹ 5m/15mは除外（株価との時間帯不一致のため）")
    print("  ℹ yfinanceではBid/Ask取得不可（Open/High/Low/Closeのみ）")

    # 為替価格データ取得
    print("\n[STEP 1] Fetching currency prices from yfinance...")
    success_count = 0

    for i, config in enumerate(CURRENCY_PRICE_CONFIGS, 1):
        print(f"\n  [{i}/{len(CURRENCY_PRICE_CONFIGS)}] {config['filename']}")
        output_path = PARQUET_DIR / config["filename"]
        fallback = config.get("fallback_period")

        if fetch_and_save_currency_prices(
            CURRENCY_TICKERS,
            config["period"],
            config["interval"],
            output_path,
            fallback
        ):
            success_count += 1

    print(f"\n  ✓ Successfully fetched {success_count}/{len(CURRENCY_PRICE_CONFIGS)} currency datasets")

    # サマリー
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Currency pairs: {len(CURRENCY_TICKERS)}")
    print(f"Currency datasets: {success_count}/{len(CURRENCY_PRICE_CONFIGS)} successful")

    # 生成されたファイル一覧
    print("\n[Generated files - Currency]")
    for config in CURRENCY_PRICE_CONFIGS:
        filepath = PARQUET_DIR / config["filename"]
        if filepath.exists():
            size_kb = filepath.stat().st_size / 1024
            print(f"  ✓ {config['filename']} ({size_kb:.1f} KB)")
        else:
            print(f"  ✗ {config['filename']} (not created)")

    print("=" * 80)
    print("  ℹ S3アップロードは update_manifest.py で一括実行されます")

    print("\n✅ Currency price fetch completed!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
