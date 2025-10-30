#!/usr/bin/env python3
"""
既存のバックテストアーカイブに前場詳細データを追加

既存の grok_trending_archive.parquet に以下のカラムを追加:
- profit_per_100_shares: 100株あたりの損益
- morning_high: 前場の最高値
- morning_low: 前場の最安値
- morning_volume: 前場の出来高
- max_gain_pct: 始値からの最大上昇率
- max_drawdown_pct: 始値からの最大下落率
"""

import sys
from pathlib import Path
from datetime import datetime, date
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

# save_backtest_to_archive.py から関数をインポート
sys.path.insert(0, str(ROOT / "scripts" / "pipeline"))
from save_backtest_to_archive import get_morning_session_details


def process_file(file_path: Path, df_prices_5m: pd.DataFrame) -> int:
    """単一のファイルを処理"""

    print(f"\n{'=' * 80}")
    print(f"Processing: {file_path.name}")
    print(f"{'=' * 80}")

    if not file_path.exists():
        print(f"⚠️  File not found: {file_path}")
        return 1

    # データ読み込み
    print(f"[INFO] Loading archive: {file_path}")
    df_archive = pd.read_parquet(file_path)
    print(f"  ✓ Loaded {len(df_archive)} records")

    # 新しいカラムが既に存在するかチェック
    new_columns = [
        'profit_per_100_shares',
        'morning_high',
        'morning_low',
        'morning_volume',
        'max_gain_pct',
        'max_drawdown_pct'
    ]

    existing_new_cols = [col for col in new_columns if col in df_archive.columns]
    if existing_new_cols:
        print(f"[INFO] Following columns already exist: {existing_new_cols}")
        print(f"[INFO] Will overwrite with recalculated values")

    # 各レコードに対して前場詳細データを計算
    print(f"[INFO] Calculating morning session details for {len(df_archive)} records...")

    results = []
    for idx, row in df_archive.iterrows():
        ticker = row['ticker']
        backtest_date = pd.to_datetime(row['backtest_date']).date()

        # 前場詳細データを取得
        morning_details = get_morning_session_details(
            df_prices_5m, ticker, backtest_date
        )

        # 100株あたりの損益を計算
        profit_per_100_shares = None
        if pd.notna(row.get('buy_price')) and pd.notna(row.get('sell_price')):
            buy_price = float(row['buy_price'])
            sell_price = float(row['sell_price'])
            profit_per_100_shares = (sell_price - buy_price) * 100

        # 新しいデータを追加
        new_row = row.to_dict()
        new_row.update({
            'profit_per_100_shares': profit_per_100_shares,
            'morning_high': morning_details['morning_high'],
            'morning_low': morning_details['morning_low'],
            'morning_volume': morning_details['morning_volume'],
            'max_gain_pct': morning_details['max_gain_pct'],
            'max_drawdown_pct': morning_details['max_drawdown_pct'],
        })

        results.append(new_row)

        # 進捗表示
        if (idx + 1) % 10 == 0 or (idx + 1) == len(df_archive):
            print(f"  Progress: {idx + 1}/{len(df_archive)}", end='\r')

    print()  # 改行

    # DataFrameに変換
    df_updated = pd.DataFrame(results)

    # 保存
    print(f"[INFO] Saving updated archive...")
    df_updated.to_parquet(file_path, index=False)
    print(f"  ✓ Saved: {file_path}")

    # サマリー表示
    print(f"\nSummary:")
    print(f"  Total records: {len(df_updated)}")
    for col in new_columns:
        non_null_count = df_updated[col].notna().sum()
        print(f"  - {col}: {non_null_count}/{len(df_updated)} records with data")

    return 0


def add_morning_details_to_archive():
    """既存のアーカイブに前場詳細データを追加"""

    print("=" * 80)
    print("Add Morning Details to Existing Backtest Archives")
    print("=" * 80)

    # 5分足データを読み込み
    prices_5m_file = PARQUET_DIR / "prices_60d_5m.parquet"
    if not prices_5m_file.exists():
        print(f"⚠️  5分足データが見つかりません: {prices_5m_file}")
        return 1

    print(f"[INFO] Loading 5m prices: {prices_5m_file}")
    df_prices_5m = pd.read_parquet(prices_5m_file)

    # インデックスリセット
    if df_prices_5m.index.name == 'date' or 'date' in df_prices_5m.index.names:
        df_prices_5m = df_prices_5m.reset_index()

    print(f"  ✓ Loaded {len(df_prices_5m):,} records")

    # 処理するファイルのリスト（バックテストアーカイブのみ）
    backtest_dir = PARQUET_DIR / "backtest"
    files_to_process = [
        backtest_dir / "grok_trending_archive.parquet",
    ]

    # 既存ファイルのみ処理
    existing_files = [f for f in files_to_process if f.exists()]
    print(f"\n[INFO] Found {len(existing_files)} backtest archive file(s) to process")

    # 各ファイルを処理
    success_count = 0
    for file_path in existing_files:
        result = process_file(file_path, df_prices_5m)
        if result == 0:
            success_count += 1

    print(f"\n{'=' * 80}")
    print(f"✅ Completed! {success_count}/{len(existing_files)} files processed successfully")
    print(f"{'=' * 80}")

    return 0


if __name__ == "__main__":
    sys.exit(add_morning_details_to_archive())
