#!/usr/bin/env python3
"""
03_generate_scalping.py
J-Quants APIからスキャルピング銘柄を選定してscalping_{entry,active}.parquetを生成
GitHub Actions対応: S3優先、meta_jquants.parquetを使用
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_client import JQuantsClient
from scripts.lib.jquants_fetcher import JQuantsFetcher
from scripts.lib.screener import ScalpingScreener
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import download_file
from common_cfg.s3cfg import load_s3_config

META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"
SCALPING_ENTRY_PATH = PARQUET_DIR / "scalping_entry.parquet"
SCALPING_ACTIVE_PATH = PARQUET_DIR / "scalping_active.parquet"


def download_meta_jquants_from_s3() -> bool:
    """S3からmeta_jquants.parquetをダウンロード"""
    try:
        cfg = load_s3_config()
        print("[INFO] Trying to download meta_jquants.parquet from S3...")
        success = download_file(cfg, "meta_jquants.parquet", META_JQUANTS_PATH)
        if success:
            print(f"[OK] Downloaded from S3: {META_JQUANTS_PATH}")
            return True
        else:
            print("[WARN] meta_jquants.parquet not found in S3")
            return False
    except Exception as e:
        print(f"[WARN] S3 download failed: {e}")
        return False


def load_meta_jquants() -> pd.DataFrame:
    """meta_jquants.parquetを読み込み（02が作成したローカルファイル）"""
    # ローカルファイル確認（02が作成済み）
    if META_JQUANTS_PATH.exists():
        print(f"[INFO] Loading meta_jquants.parquet: {META_JQUANTS_PATH}")
        return pd.read_parquet(META_JQUANTS_PATH)

    # ファイルがない場合はエラー
    raise FileNotFoundError(
        f"meta_jquants.parquet not found: {META_JQUANTS_PATH}\n"
        "Please run 02_create_meta_jquants.py first."
    )


def fetch_stock_prices(tickers: list[str], fetcher: JQuantsFetcher, lookback_days: int = 60) -> pd.DataFrame:
    """
    指定銘柄の株価データを取得（J-Quants API）

    Args:
        tickers: ティッカーリスト（例: ["7203.T", "6758.T"]）
        fetcher: JQuantsFetcher インスタンス
        lookback_days: 何日分のデータを取得するか

    Returns:
        株価データのDataFrame
    """
    print(f"[INFO] Fetching stock prices for {len(tickers)} stocks (last {lookback_days} days)...")

    # ティッカーを4桁コードに変換（例: "7203.T" -> "7203"）
    codes = [ticker.replace(".T", "") for ticker in tickers]

    # 期間設定
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=lookback_days)

    # 株価データ取得（バッチ処理）
    df = fetcher.get_prices_daily_batch(codes, from_date=from_date, to_date=to_date, batch_delay=0.5)

    if df.empty:
        print("[ERROR] No price data retrieved")
        return pd.DataFrame()

    # yfinance互換形式に変換
    df = fetcher.convert_to_yfinance_format(df)

    print(f"[OK] Retrieved price data: {len(df)} rows, {df['ticker'].nunique()} stocks")
    return df


def main() -> int:
    """スキャルピング銘柄リストを生成"""
    print("=" * 60)
    print("Generate Scalping Lists (J-Quants)")
    print("=" * 60)

    # [STEP 1] meta_jquants.parquet読み込み
    print("\n[STEP 1] Loading meta_jquants.parquet...")
    try:
        meta_df = load_meta_jquants()
        print(f"  ✓ Loaded {len(meta_df)} stocks")

        # J-Quants障害時対応: meta_jquantsが空なら空のscalpingファイルを作成して終了
        if meta_df.empty:
            print("  ⚠ meta_jquants.parquetが空です（J-Quants障害時対応）")
            print("  → 空のscalping_*.parquetを作成します")

            # 空のDataFrameを正しいスキーマで作成
            empty_df = pd.DataFrame(columns=[
                'ticker', 'stock_name', 'market', 'sectors', 'date',
                'Close', 'change_pct', 'Volume', 'vol_ratio',
                'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
            ])

            PARQUET_DIR.mkdir(parents=True, exist_ok=True)
            empty_df.to_parquet(SCALPING_ENTRY_PATH, engine="pyarrow", index=False)
            empty_df.to_parquet(SCALPING_ACTIVE_PATH, engine="pyarrow", index=False)

            print(f"  ✓ Saved empty: {SCALPING_ENTRY_PATH}")
            print(f"  ✓ Saved empty: {SCALPING_ACTIVE_PATH}")
            print("\n✅ Empty scalping lists created (J-Quants障害時対応)")
            return 0

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 2] J-Quants APIで株価データ取得
    print("\n[STEP 2] Fetching stock prices from J-Quants API...")
    try:
        client = JQuantsClient()
        fetcher = JQuantsFetcher(client)

        # 全銘柄の株価を取得（60日分）
        tickers = meta_df["ticker"].tolist()
        df_prices = fetch_stock_prices(tickers, fetcher, lookback_days=60)

        if df_prices.empty:
            print("  ✗ No price data retrieved")
            print("  ⚠ J-Quants障害時対応: 空のscalping_*.parquetを作成します")

            # 空のDataFrameを正しいスキーマで作成
            empty_df = pd.DataFrame(columns=[
                'ticker', 'stock_name', 'market', 'sectors', 'date',
                'Close', 'change_pct', 'Volume', 'vol_ratio',
                'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
            ])

            PARQUET_DIR.mkdir(parents=True, exist_ok=True)
            empty_df.to_parquet(SCALPING_ENTRY_PATH, engine="pyarrow", index=False)
            empty_df.to_parquet(SCALPING_ACTIVE_PATH, engine="pyarrow", index=False)

            print(f"  ✓ Saved empty: {SCALPING_ENTRY_PATH}")
            print(f"  ✓ Saved empty: {SCALPING_ACTIVE_PATH}")
            print("\n✅ Empty scalping lists created (J-Quants障害時対応)")
            return 0

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        print("  ⚠ J-Quants障害時対応: 空のscalping_*.parquetを作成します")

        # 空のDataFrameを正しいスキーマで作成
        empty_df = pd.DataFrame(columns=[
            'ticker', 'stock_name', 'market', 'sectors', 'date',
            'Close', 'change_pct', 'Volume', 'vol_ratio',
            'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
        ])

        PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        empty_df.to_parquet(SCALPING_ENTRY_PATH, engine="pyarrow", index=False)
        empty_df.to_parquet(SCALPING_ACTIVE_PATH, engine="pyarrow", index=False)

        print(f"  ✓ Saved empty: {SCALPING_ENTRY_PATH}")
        print(f"  ✓ Saved empty: {SCALPING_ACTIVE_PATH}")
        print("\n✅ Empty scalping lists created (J-Quants障害時対応)")
        return 0

    # [STEP 3] テクニカル指標計算
    print("\n[STEP 3] Calculating technical indicators...")
    try:
        screener = ScalpingScreener(fetcher)
        df_with_tech = screener.calculate_technical_indicators(df_prices)
        print(f"  ✓ Calculated indicators for {df_with_tech['ticker'].nunique()} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 4] テクニカル評価（overall_rating）
    print("\n[STEP 4] Evaluating technical ratings...")
    try:
        df_with_ratings = screener.evaluate_technical_ratings(df_with_tech)
        print(f"  ✓ Evaluated ratings")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 5] 最新日のデータのみ抽出
    print("\n[STEP 5] Extracting latest data...")
    try:
        df_latest = df_with_ratings.sort_values(['ticker', 'date']).groupby('ticker', as_index=False).last()
        print(f"  ✓ Latest data: {len(df_latest)} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 6] エントリー向け銘柄リスト生成
    print("\n[STEP 6] Generating entry list...")
    try:
        df_entry = screener.generate_entry_list(df_latest, meta_df, top_n=20)

        if df_entry.empty:
            print("  ⚠ No entry stocks found (creating empty file)")
            df_entry = pd.DataFrame(columns=[
                'ticker', 'stock_name', 'market', 'sectors', 'date',
                'Close', 'change_pct', 'Volume', 'vol_ratio',
                'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
            ])

        print(f"  ✓ Entry list: {len(df_entry)} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 7] アクティブ向け銘柄リスト生成
    print("\n[STEP 7] Generating active list...")
    try:
        entry_tickers = set(df_entry['ticker'].tolist()) if not df_entry.empty else set()
        df_active = screener.generate_active_list(df_latest, meta_df, entry_tickers, top_n=20)

        if df_active.empty:
            print("  ⚠ No active stocks found (creating empty file)")
            df_active = pd.DataFrame(columns=[
                'ticker', 'stock_name', 'market', 'sectors', 'date',
                'Close', 'change_pct', 'Volume', 'vol_ratio',
                'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
            ])

        print(f"  ✓ Active list: {len(df_active)} stocks")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # [STEP 8] 保存
    print("\n[STEP 8] Saving scalping lists...")
    try:
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)

        df_entry.to_parquet(SCALPING_ENTRY_PATH, engine="pyarrow", index=False)
        print(f"  ✓ Saved: {SCALPING_ENTRY_PATH}")

        df_active.to_parquet(SCALPING_ACTIVE_PATH, engine="pyarrow", index=False)
        print(f"  ✓ Saved: {SCALPING_ACTIVE_PATH}")

        print("  ℹ S3アップロードは 06_update_manifest.py で一括実行されます")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return 1

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Entry stocks: {len(df_entry)}")
    print(f"Active stocks: {len(df_active)}")
    print("=" * 60)

    print("\n✅ Scalping lists generated successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
