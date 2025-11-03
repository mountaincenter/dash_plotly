"""
既存のバックテストparquetファイルに不足しているカラムを追加するスクリプト

追加するカラム:
- morning_high: 前場高値 (9:00-11:30の最高値)
- morning_low: 前場安値 (9:00-11:30の最安値)
- morning_max_gain_pct: 前場最大上昇率 ((morning_high - open) / open * 100)
- morning_max_drawdown_pct: 前場最大下落率 ((morning_low - open) / open * 100)
- daily_max_gain_pct: 全日最大上昇率 ((high - open) / open * 100)
- daily_max_drawdown_pct: 全日最大下落率 ((low - open) / open * 100)
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import yfinance as yf
from typing import Optional, Tuple
import time

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# データディレクトリ
DATA_DIR = project_root / "data" / "parquet" / "backtest"

def fetch_intraday_data(code: str, date: datetime) -> Optional[pd.DataFrame]:
    """
    yfinanceを使用して5分足の株価データを取得

    Args:
        code: 銘柄コード (例: "9984.T")
        date: 取得する日付

    Returns:
        5分足データのDataFrame、または取得失敗時はNone
    """
    try:
        # 日付の前後1日分のデータを取得
        start_date = date - timedelta(days=1)
        end_date = date + timedelta(days=2)

        ticker = yf.Ticker(code)
        df = ticker.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval="5m"
        )

        if df.empty:
            print(f"  警告: {code} の {date.strftime('%Y-%m-%d')} データが取得できませんでした")
            return None

        # タイムゾーンを日本時間に変換
        df.index = df.index.tz_convert('Asia/Tokyo')

        # 指定日のデータのみを抽出
        date_str = date.strftime("%Y-%m-%d")
        df = df[df.index.date.astype(str) == date_str]

        if df.empty:
            print(f"  警告: {code} の {date.strftime('%Y-%m-%d')} に該当するデータがありません")
            return None

        return df

    except Exception as e:
        print(f"  エラー: {code} のデータ取得に失敗: {e}")
        return None


def calculate_morning_metrics(df: pd.DataFrame, open_price: float) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    前場（9:00-11:30）のメトリクスを計算

    Args:
        df: 5分足データのDataFrame
        open_price: 始値

    Returns:
        (morning_high, morning_low, morning_max_gain_pct, morning_max_drawdown_pct)
    """
    if df.empty or open_price is None or open_price == 0:
        return None, None, None, None

    try:
        # 前場の時間帯でフィルタ (9:00-11:30)
        morning_data = df.between_time("09:00", "11:30")

        if morning_data.empty:
            print(f"    警告: 前場データが存在しません")
            return None, None, None, None

        morning_high = morning_data['High'].max()
        morning_low = morning_data['Low'].min()

        morning_max_gain_pct = ((morning_high - open_price) / open_price * 100) if morning_high else None
        morning_max_drawdown_pct = ((morning_low - open_price) / open_price * 100) if morning_low else None

        return morning_high, morning_low, morning_max_gain_pct, morning_max_drawdown_pct

    except Exception as e:
        print(f"    エラー: 前場メトリクス計算に失敗: {e}")
        return None, None, None, None


def calculate_daily_metrics(df: pd.DataFrame, open_price: float) -> Tuple[Optional[float], Optional[float]]:
    """
    全日（9:00-15:30）の最大上昇率・下落率を計算

    Args:
        df: 5分足データのDataFrame
        open_price: 始値

    Returns:
        (daily_max_gain_pct, daily_max_drawdown_pct)
    """
    if df.empty or open_price is None or open_price == 0:
        return None, None

    try:
        # 全日の時間帯でフィルタ (9:00-15:30)
        daily_data = df.between_time("09:00", "15:30")

        if daily_data.empty:
            print(f"    警告: 全日データが存在しません")
            return None, None

        daily_high = daily_data['High'].max()
        daily_low = daily_data['Low'].min()

        daily_max_gain_pct = ((daily_high - open_price) / open_price * 100) if daily_high else None
        daily_max_drawdown_pct = ((daily_low - open_price) / open_price * 100) if daily_low else None

        return daily_max_gain_pct, daily_max_drawdown_pct

    except Exception as e:
        print(f"    エラー: 全日メトリクス計算に失敗: {e}")
        return None, None


def add_missing_columns_to_parquet(file_path: Path) -> bool:
    """
    parquetファイルに不足しているカラムを追加

    Args:
        file_path: 処理するparquetファイルのパス

    Returns:
        成功時True、失敗時False
    """
    print(f"\n処理中: {file_path.name}")

    try:
        # parquetファイルを読み込み
        df = pd.read_parquet(file_path)
        print(f"  レコード数: {len(df)}")

        # 新しいカラムを初期化
        df['morning_high'] = None
        df['morning_low'] = None
        df['morning_max_gain_pct'] = None
        df['morning_max_drawdown_pct'] = None
        df['daily_max_gain_pct'] = None
        df['daily_max_drawdown_pct'] = None

        # 各レコードを処理
        for idx, row in df.iterrows():
            ticker = row['ticker']
            backtest_date = pd.to_datetime(row['backtest_date'])
            open_price = row.get('buy_price')  # 始値 = buy_price

            print(f"  [{idx+1}/{len(df)}] {ticker} ({backtest_date.strftime('%Y-%m-%d')})")

            # yfinanceから5分足データを取得
            intraday_df = fetch_intraday_data(ticker, backtest_date)

            if intraday_df is not None:
                # 前場メトリクスを計算
                morning_high, morning_low, morning_max_gain_pct, morning_max_drawdown_pct = \
                    calculate_morning_metrics(intraday_df, open_price)

                # 全日メトリクスを計算
                daily_max_gain_pct, daily_max_drawdown_pct = \
                    calculate_daily_metrics(intraday_df, open_price)

                # データフレームに設定
                df.at[idx, 'morning_high'] = morning_high
                df.at[idx, 'morning_low'] = morning_low
                df.at[idx, 'morning_max_gain_pct'] = morning_max_gain_pct
                df.at[idx, 'morning_max_drawdown_pct'] = morning_max_drawdown_pct
                df.at[idx, 'daily_max_gain_pct'] = daily_max_gain_pct
                df.at[idx, 'daily_max_drawdown_pct'] = daily_max_drawdown_pct

                print(f"    前場: 高値={morning_high}, 安値={morning_low}, "
                      f"上昇率={morning_max_gain_pct:.2f}%, 下落率={morning_max_drawdown_pct:.2f}%")
                print(f"    全日: 上昇率={daily_max_gain_pct:.2f}%, 下落率={daily_max_drawdown_pct:.2f}%")

            # API制限を考慮して待機
            time.sleep(0.5)

        # 更新されたデータフレームを保存
        df.to_parquet(file_path, index=False)
        print(f"  ✓ {file_path.name} を更新しました")

        return True

    except Exception as e:
        print(f"  ✗ エラー: {file_path.name} の処理に失敗: {e}")
        return False


def main():
    """メイン処理"""
    print("=" * 80)
    print("バックテストparquetファイルへのカラム追加スクリプト")
    print("=" * 80)

    # 処理対象ファイル
    files = [
        "grok_trending_20251029.parquet",
        "grok_trending_20251030.parquet",
        "grok_trending_20251031.parquet",
        "grok_trending_archive.parquet"
    ]

    success_count = 0
    failed_count = 0

    for file_name in files:
        file_path = DATA_DIR / file_name

        if not file_path.exists():
            print(f"\n警告: {file_name} が見つかりません")
            failed_count += 1
            continue

        if add_missing_columns_to_parquet(file_path):
            success_count += 1
        else:
            failed_count += 1

    # 結果サマリー
    print("\n" + "=" * 80)
    print("処理完了")
    print("=" * 80)
    print(f"成功: {success_count} ファイル")
    print(f"失敗: {failed_count} ファイル")

    if failed_count == 0:
        print("\n✓ すべてのファイルが正常に更新されました")
    else:
        print(f"\n⚠ {failed_count} ファイルの処理に失敗しました")


if __name__ == "__main__":
    main()
