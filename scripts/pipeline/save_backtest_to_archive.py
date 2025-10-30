#!/usr/bin/env python3
"""
GROK銘柄のバックテストアーカイブ保存

昨日23:00に選定されたGROK銘柄について、今日の前場パフォーマンスを計算して保存
- 9:00寄付買い → 11:30以降の最初の有効価格で売却 (Phase1戦略)
- 結果を data/parquet/backtest/grok_trending_archive.parquet に追記（append-only）
- 同じ日付のデータは上書き（再実行時の重複防止）
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


def get_morning_session_details(
    df_5m: pd.DataFrame,
    ticker: str,
    target_date: date
) -> dict:
    """
    5分足データから前場（9:00-11:30）の詳細情報を取得

    Returns:
        dict: {
            'morning_high': 前場の最高値,
            'morning_low': 前場の最安値,
            'morning_volume': 前場の出来高,
            'max_gain_pct': 始値からの最大上昇率 (morning_high/open - 1),
            'max_drawdown_pct': 始値からの最大下落率 (morning_low/open - 1)
        }
    """
    result = {
        'morning_high': None,
        'morning_low': None,
        'morning_volume': None,
        'max_gain_pct': None,
        'max_drawdown_pct': None,
    }

    if 'date' not in df_5m.columns:
        return result

    if not pd.api.types.is_datetime64_any_dtype(df_5m['date']):
        return result

    # 対象銘柄・日付でフィルタ
    df_ticker = df_5m[
        (df_5m['ticker'] == ticker) &
        (df_5m['date'].dt.date == target_date)
    ].copy()

    if len(df_ticker) == 0:
        return result

    # 時刻を分単位に変換
    df_ticker['time_minutes'] = df_ticker['date'].dt.hour * 60 + df_ticker['date'].dt.minute

    # 前場（9:00-11:30 = 540-690分）のデータに絞り込み
    df_morning = df_ticker[
        (df_ticker['time_minutes'] >= 540) &
        (df_ticker['time_minutes'] <= 690)
    ]

    if len(df_morning) == 0:
        return result

    # 前場の最高値・最安値・出来高
    result['morning_high'] = float(df_morning['High'].max()) if df_morning['High'].notna().any() else None
    result['morning_low'] = float(df_morning['Low'].min()) if df_morning['Low'].notna().any() else None
    result['morning_volume'] = int(df_morning['Volume'].sum()) if df_morning['Volume'].notna().any() else None

    # 始値（9:00の最初のOpen）を取得
    df_morning_sorted = df_morning.sort_values('time_minutes')
    open_price = None
    for _, row in df_morning_sorted.iterrows():
        if pd.notna(row['Open']):
            open_price = float(row['Open'])
            break

    # 始値からの最大上昇率・最大下落率を計算
    if open_price and open_price > 0:
        if result['morning_high']:
            result['max_gain_pct'] = (result['morning_high'] / open_price) - 1.0
        if result['morning_low']:
            result['max_drawdown_pct'] = (result['morning_low'] / open_price) - 1.0

    return result


def get_sell_price_after_1130(
    df_5m: pd.DataFrame,
    ticker: str,
    target_date: date
) -> tuple[float | None, str | None]:
    """
    5分足データから11:30以降の最初の有効な終値を取得

    Returns:
        (売却価格, 売却時刻) のタプル
    """
    # dateカラムの存在確認
    if 'date' not in df_5m.columns:
        print(f"⚠️  Warning: 'date' column not found in df_5m. Columns: {df_5m.columns.tolist()}")
        return None, None

    # フィルタリング前に date カラムが datetime 型であることを確認
    if not pd.api.types.is_datetime64_any_dtype(df_5m['date']):
        print(f"⚠️  Warning: 'date' column is not datetime type. Type: {df_5m['date'].dtype}")
        return None, None

    df_ticker = df_5m[
        (df_5m['ticker'] == ticker) &
        (df_5m['date'].dt.date == target_date)
    ].copy()

    if len(df_ticker) == 0:
        return None, None

    # 時刻を分単位に変換
    df_ticker['time_minutes'] = df_ticker['date'].dt.hour * 60 + df_ticker['date'].dt.minute

    # 11:30以降のデータに絞り込み（690分 = 11:30）
    df_after_1130 = df_ticker[df_ticker['time_minutes'] >= 690].sort_values('time_minutes')

    # NaNでない最初のClose価格を探す
    valid_closes = df_after_1130[df_after_1130['Close'].notna()]

    if len(valid_closes) > 0:
        sell_price = float(valid_closes['Close'].iloc[0])
        sell_time = valid_closes['date'].iloc[0].strftime('%H:%M')
        return sell_price, sell_time

    return None, None


def calculate_phase1_backtest(
    df_grok: pd.DataFrame,
    df_prices_1d: pd.DataFrame,
    df_prices_5m: pd.DataFrame,
    target_date: date
) -> pd.DataFrame:
    """
    Phase1バックテスト計算: 9:00寄付買い → 11:30以降の最初の有効価格で売却

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

        # 11:30以降の最初の有効な売却価格を取得
        sell_price, sell_time = get_sell_price_after_1130(
            df_prices_5m, ticker, target_date
        )

        # 前場の詳細データを取得
        morning_details = get_morning_session_details(
            df_prices_5m, ticker, target_date
        )

        # リターン計算
        phase1_return = None
        phase1_win = None
        profit_per_100_shares = None
        if buy_price is not None and sell_price is not None and buy_price > 0:
            phase1_return = (sell_price - buy_price) / buy_price
            phase1_win = phase1_return > 0
            profit_per_100_shares = (sell_price - buy_price) * 100

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
            'phase1_win': phase1_win,
            'profit_per_100_shares': profit_per_100_shares,
            'morning_high': morning_details['morning_high'],
            'morning_low': morning_details['morning_low'],
            'morning_volume': morning_details['morning_volume'],
            'max_gain_pct': morning_details['max_gain_pct'],
            'max_drawdown_pct': morning_details['max_drawdown_pct'],
            'prompt_version': row.get('prompt_version', 'v1_0_baseline'),  # プロンプトバージョン
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

    # インデックスに date がある場合はリセット
    if df_prices_1d.index.name == 'date' or 'date' in df_prices_1d.index.names:
        df_prices_1d = df_prices_1d.reset_index()

    if df_prices_5m.index.name == 'date' or 'date' in df_prices_5m.index.names:
        df_prices_5m = df_prices_5m.reset_index()

    # date カラムが存在することを確認
    if 'date' not in df_prices_1d.columns:
        print(f"⚠️  エラー: 日足データに 'date' カラムがありません。カラム: {df_prices_1d.columns.tolist()}")
        sys.exit(1)

    if 'date' not in df_prices_5m.columns:
        print(f"⚠️  エラー: 5分足データに 'date' カラムがありません。カラム: {df_prices_5m.columns.tolist()}")
        sys.exit(1)

    print(f"✅ 日足データ読み込み: {len(df_prices_1d):,}件")
    print(f"✅ 5分足データ読み込み: {len(df_prices_5m):,}件")

    # 3. バックテスト対象日（引数 or 今日）
    if len(sys.argv) > 1:
        target_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    else:
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
        avg_return = df_backtest['phase1_return'].mean() * 100
        win_rate = (df_backtest['phase1_win'] == True).sum() / valid_results * 100

        print(f"\n📊 Phase1結果サマリー:")
        print(f"   平均リターン: {avg_return:+.2f}%")
        print(f"   勝率: {win_rate:.1f}%")

        # Top5の結果
        df_top5 = df_backtest[df_backtest['grok_rank'] <= 5]
        if len(df_top5) > 0:
            top5_valid = df_top5['phase1_return'].notna().sum()
            if top5_valid > 0:
                top5_avg = df_top5['phase1_return'].mean() * 100
                top5_win_rate = (df_top5['phase1_win'] == True).sum() / top5_valid * 100
                print(f"\n   Top5平均リターン: {top5_avg:+.2f}%")
                print(f"   Top5勝率: {top5_win_rate:.1f}%")

    # 6. アーカイブに追記
    archive_dir = PARQUET_DIR / "backtest"
    archive_dir.mkdir(parents=True, exist_ok=True)

    archive_file = archive_dir / "grok_trending_archive.parquet"

    # 既存アーカイブを読み込み
    if archive_file.exists():
        df_archive = pd.read_parquet(archive_file)
        original_count = len(df_archive)
        print(f"\n📂 既存アーカイブを読み込み: {original_count}件")

        # 同じ日付のデータを除外（再実行時の重複防止）
        df_archive_filtered = df_archive[df_archive['backtest_date'] != target_date]
        excluded_count = original_count - len(df_archive_filtered)
        print(f"   {target_date}のデータを除外: {excluded_count}件 (残り: {len(df_archive_filtered)}件)")

        # 新データを追加
        df_combined = pd.concat([df_archive_filtered, df_backtest], ignore_index=True)
        print(f"   新データを追加: {len(df_backtest)}件")
    else:
        print(f"\n📂 新規アーカイブを作成")
        df_combined = df_backtest

    # アーカイブを保存
    df_combined.to_parquet(archive_file, index=False)

    # 日付ごとの内訳を表示
    date_counts = df_combined.groupby('backtest_date').size().sort_index()
    unique_dates = len(date_counts)

    print(f"\n✅ アーカイブを保存: {archive_file}")
    print(f"   総レコード数: {len(df_combined)}件 ({unique_dates}日分)")
    print(f"   日付別内訳:")
    for date_val, count in date_counts.items():
        print(f"      {date_val}: {count}銘柄")

    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
