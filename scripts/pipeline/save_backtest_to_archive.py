#!/usr/bin/env python3
"""
GROK銘柄のバックテストアーカイブ保存

昨日23:00に選定されたGROK銘柄について、今日の前場パフォーマンスを計算して保存
- 9:00寄付買い → 11:30前引け売却 (Phase1戦略)
- 結果をdata/parquet/backtest/grok_trending_YYYYMMDD.parquetに保存
"""

import sys
from pathlib import Path
from datetime import datetime, date, time, timedelta
import pandas as pd
import numpy as np

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR


def get_open_price(df_1d: pd.DataFrame, ticker: str, target_date: date) -> float | None:
    """日足データから指定日の始値（寄付価格）を取得"""
    ticker_data = df_1d[
        (df_1d['ticker'] == ticker) &
        (df_1d['date'].dt.date == target_date)
    ]

    if len(ticker_data) > 0 and pd.notna(ticker_data['Open'].iloc[0]):
        return float(ticker_data['Open'].iloc[0])
    return None


def get_price_at_time(
    df_5m: pd.DataFrame,
    ticker: str,
    target_date: date,
    target_time: time,
    tolerance_minutes: int = 10
) -> float | None:
    """5分足データから指定時刻の価格を取得（前後tolerance_minutes分の範囲で最も近い時刻）"""
    df_ticker = df_5m[
        (df_5m['ticker'] == ticker) &
        (df_5m['date'].dt.date == target_date)
    ].copy()

    if len(df_ticker) == 0:
        return None

    # 目標時刻との差分を計算（分単位）
    target_minutes = target_time.hour * 60 + target_time.minute
    df_ticker['time_diff'] = df_ticker['date'].apply(
        lambda x: abs((x.hour * 60 + x.minute) - target_minutes)
    )

    # tolerance_minutes以内の最も近い時刻のデータを取得
    closest = df_ticker[df_ticker['time_diff'] <= tolerance_minutes].nsmallest(1, 'time_diff')

    if len(closest) > 0 and pd.notna(closest['Close'].iloc[0]):
        return float(closest['Close'].iloc[0])
    return None


def calculate_phase1_backtest(
    df_grok: pd.DataFrame,
    df_prices_1d: pd.DataFrame,
    df_prices_5m: pd.DataFrame,
    target_date: date
) -> pd.DataFrame:
    """
    Phase1バックテスト計算: 9:00寄付買い → 11:30前引け売却

    Args:
        df_grok: GROK選定銘柄データ（前日23:00選定）
        df_prices_1d: 日足データ（寄付価格用）
        df_prices_5m: 5分足データ（売却価格用）
        target_date: バックテスト対象日

    Returns:
        バックテスト結果DataFrame
    """
    results = []

    for _, row in df_grok.iterrows():
        ticker = row['ticker']

        # 寄付価格（買値）を取得
        buy_price = get_open_price(df_prices_1d, ticker, target_date)

        # 11:30の売却価格を取得
        sell_price = get_price_at_time(
            df_prices_5m, ticker, target_date, time(11, 30), tolerance_minutes=10
        )

        # リターン計算
        phase1_return = None
        if buy_price is not None and sell_price is not None and buy_price > 0:
            phase1_return = (sell_price - buy_price) / buy_price * 100

        result = {
            'ticker': ticker,
            'stock_name': row.get('stock_name', ''),
            'selection_score': row.get('selection_score', None),
            'grok_rank': row.get('grok_rank', None),
            'reason': row.get('reason', ''),
            'selected_time': row.get('selected_time', ''),
            'backtest_date': target_date,
            'buy_price': buy_price,
            'sell_price': sell_price,
            'phase1_return': phase1_return,
            'phase1_win': phase1_return > 0 if phase1_return is not None else None,
        }

        results.append(result)

    return pd.DataFrame(results)


def main():
    """メイン処理"""
    print("=" * 80)
    print("GROK銘柄バックテストアーカイブ保存")
    print("=" * 80)

    # 1. 昨日選定されたGROK銘柄を読み込み
    grok_file = PARQUET_DIR / "grok_trending.parquet"

    if not grok_file.exists():
        print(f"⚠️  GROK選定ファイルが見つかりません: {grok_file}")
        print("→ スキップ（23:00実行後にのみ作成されます）")
        sys.exit(0)

    df_grok = pd.read_parquet(grok_file)
    print(f"✅ GROK選定銘柄を読み込み: {len(df_grok)}銘柄")
    print(f"   選定時刻: {df_grok['selected_time'].iloc[0] if 'selected_time' in df_grok.columns else 'N/A'}")

    # 2. 価格データを読み込み
    prices_1d_file = PARQUET_DIR / "prices_max_1d.parquet"
    prices_5m_file = PARQUET_DIR / "prices_60d_5m.parquet"

    if not prices_1d_file.exists():
        print(f"⚠️  日足データが見つかりません: {prices_1d_file}")
        sys.exit(1)

    if not prices_5m_file.exists():
        print(f"⚠️  5分足データが見つかりません: {prices_5m_file}")
        sys.exit(1)

    df_prices_1d = pd.read_parquet(prices_1d_file)
    df_prices_5m = pd.read_parquet(prices_5m_file)

    print(f"✅ 日足データ読み込み: {len(df_prices_1d):,}件")
    print(f"✅ 5分足データ読み込み: {len(df_prices_5m):,}件")

    # 3. バックテスト対象日（今日）
    target_date = date.today()
    print(f"\n📅 バックテスト対象日: {target_date}")

    # 4. Phase1バックテスト実行
    print("\n⏳ Phase1バックテスト計算中...")
    df_backtest = calculate_phase1_backtest(
        df_grok, df_prices_1d, df_prices_5m, target_date
    )

    # 5. 結果集計
    valid_results = df_backtest['phase1_return'].notna().sum()
    total_stocks = len(df_backtest)

    print(f"✅ バックテスト完了: {valid_results}/{total_stocks}銘柄で計算成功")

    if valid_results > 0:
        avg_return = df_backtest['phase1_return'].mean()
        win_rate = (df_backtest['phase1_win'] == True).sum() / valid_results * 100

        print(f"\n📊 Phase1結果サマリー:")
        print(f"   平均リターン: {avg_return:+.2f}%")
        print(f"   勝率: {win_rate:.1f}%")

        # Top5の結果
        df_top5 = df_backtest[df_backtest['grok_rank'] <= 5]
        if len(df_top5) > 0:
            top5_valid = df_top5['phase1_return'].notna().sum()
            if top5_valid > 0:
                top5_avg = df_top5['phase1_return'].mean()
                top5_win_rate = (df_top5['phase1_win'] == True).sum() / top5_valid * 100
                print(f"\n   Top5平均リターン: {top5_avg:+.2f}%")
                print(f"   Top5勝率: {top5_win_rate:.1f}%")

    # 6. アーカイブディレクトリに保存
    archive_dir = PARQUET_DIR / "backtest"
    archive_dir.mkdir(parents=True, exist_ok=True)

    output_file = archive_dir / f"grok_trending_{target_date.strftime('%Y%m%d')}.parquet"
    df_backtest.to_parquet(output_file, index=False)

    print(f"\n✅ バックテストアーカイブを保存: {output_file}")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
