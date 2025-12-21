#!/usr/bin/env python3
"""
surge_candidates_5m.parquet の最新化スクリプト

既存の surge_candidates_5m.parquet から銘柄リストを取得し、
yfinance で最新の5分足データを取得して上書き保存する。

バッチ処理で分割取得（API制限対策）
"""

import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime
import time
import sys

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_PATH = DATA_DIR / "surge_candidates_5m.parquet"

# バッチ設定
BATCH_SIZE = 50  # 1バッチあたりの銘柄数
SLEEP_BETWEEN_TICKERS = 0.2  # 銘柄間の待機秒数
SLEEP_BETWEEN_BATCHES = 5  # バッチ間の待機秒数


def get_target_tickers() -> list:
    """既存ファイルから銘柄リストを取得"""
    if OUTPUT_PATH.exists():
        df = pd.read_parquet(OUTPUT_PATH)
        tickers = sorted(df['ticker'].unique().tolist())
        print(f"[INFO] 既存ファイルから {len(tickers)} 銘柄を取得")
        return tickers
    else:
        print(f"[ERROR] {OUTPUT_PATH} が存在しません")
        return []


def fetch_batch(tickers: list, batch_num: int, total_batches: int) -> pd.DataFrame:
    """1バッチ分の銘柄データを取得"""
    print(f"\n{'='*60}")
    print(f"バッチ {batch_num}/{total_batches} ({len(tickers)}銘柄)")
    print(f"{'='*60}")

    all_data = []
    success = 0
    failed = []

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{len(tickers)}] {ticker}...", end=" ", flush=True)

        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="60d", interval="5m")

            if df.empty:
                print("SKIP (no data)")
                failed.append(ticker)
                continue

            df = df.reset_index()
            df['ticker'] = ticker

            # Datetime列の処理
            if 'Datetime' in df.columns:
                df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize(None)

            all_data.append(df)
            print(f"OK ({len(df)} rows)")
            success += 1

        except Exception as e:
            print(f"ERROR: {e}")
            failed.append(ticker)

        time.sleep(SLEEP_BETWEEN_TICKERS)

    print(f"\n  バッチ結果: 成功 {success}/{len(tickers)}, 失敗 {len(failed)}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def main():
    print("=" * 60)
    print("surge_candidates_5m.parquet 最新化")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 銘柄リスト取得
    tickers = get_target_tickers()
    if not tickers:
        return 1

    # バッチ分割
    batches = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    total_batches = len(batches)
    print(f"\n[INFO] {len(tickers)}銘柄 → {total_batches}バッチに分割 (1バッチ{BATCH_SIZE}銘柄)")

    # バッチ処理
    all_results = []

    for batch_num, batch_tickers in enumerate(batches, 1):
        df_batch = fetch_batch(batch_tickers, batch_num, total_batches)

        if not df_batch.empty:
            all_results.append(df_batch)

        # 最終バッチ以外は待機
        if batch_num < total_batches:
            print(f"\n  次のバッチまで {SLEEP_BETWEEN_BATCHES}秒 待機...")
            time.sleep(SLEEP_BETWEEN_BATCHES)

    # 結合して保存
    if all_results:
        print(f"\n{'='*60}")
        print("結果を保存中...")
        print(f"{'='*60}")

        df_final = pd.concat(all_results, ignore_index=True)
        df_final = df_final.sort_values(['ticker', 'Datetime']).reset_index(drop=True)

        # 保存
        df_final.to_parquet(OUTPUT_PATH, index=False)

        print(f"\n[OK] 保存完了: {OUTPUT_PATH}")
        print(f"  総件数: {len(df_final):,} rows")
        print(f"  銘柄数: {df_final['ticker'].nunique()}")
        print(f"  期間: {df_final['Datetime'].min()} 〜 {df_final['Datetime'].max()}")
    else:
        print("\n[ERROR] データが取得できませんでした")
        return 1

    print(f"\n{'='*60}")
    print("完了!")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
