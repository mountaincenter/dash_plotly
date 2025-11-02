"""
load_data.py
Grok バックテスト分析用のデータ読み込みユーティリティ
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR


class DataLoader:
    """Grok バックテスト分析用データローダー"""

    def __init__(self, parquet_dir: Path = PARQUET_DIR):
        self.parquet_dir = parquet_dir

    def load_grok_archive(self) -> pd.DataFrame:
        """
        Grok選定銘柄のバックテストアーカイブを読み込み

        Returns:
            columns: backtest_date, selected_time, ticker, stock_name, reason, tags,
                     open_price, close_price, return_pct, win
        """
        archive_path = self.parquet_dir / "backtest" / "grok_trending_archive.parquet"

        if not archive_path.exists():
            print(f"⚠️  アーカイブファイルが見つかりません: {archive_path}")
            return pd.DataFrame()

        df = pd.read_parquet(archive_path)
        print(f"✓ Grok archive loaded: {len(df)} records")
        return df

    def load_grok_backtest_meta(self) -> pd.DataFrame:
        """
        バックテストメタ情報を読み込み

        Returns:
            columns: metric, value (key-value形式)
        """
        meta_path = self.parquet_dir / "grok_backtest_meta.parquet"

        if not meta_path.exists():
            print(f"⚠️  メタファイルが見つかりません: {meta_path}")
            return pd.DataFrame()

        df = pd.read_parquet(meta_path)
        print(f"✓ Backtest meta loaded: {len(df)} metrics")
        return df

    def load_grok_trending(self) -> pd.DataFrame:
        """
        最新のGrok選定結果を読み込み

        Returns:
            columns: date, selected_time, ticker, stock_name, reason, tags, rank
        """
        trending_path = self.parquet_dir / "grok_trending.parquet"

        if not trending_path.exists():
            print(f"⚠️  Trendingファイルが見つかりません: {trending_path}")
            return pd.DataFrame()

        df = pd.read_parquet(trending_path)
        print(f"✓ Grok trending loaded: {len(df)} stocks")
        return df

    def load_stock_prices(self, interval: str = "1d") -> pd.DataFrame:
        """
        株価データを読み込み

        Args:
            interval: データ間隔 (1d, 1h, 15m, 5m, 1mo)

        Returns:
            columns: date, Open, High, Low, Close, Volume, ticker
        """
        file_mapping = {
            "1d": "prices_max_1d.parquet",
            "1h": "prices_730d_1h.parquet",
            "15m": "prices_60d_15m.parquet",
            "5m": "prices_60d_5m.parquet",
            "1mo": "prices_max_1mo.parquet",
        }

        filename = file_mapping.get(interval)
        if not filename:
            raise ValueError(f"Invalid interval: {interval}. Choose from {list(file_mapping.keys())}")

        prices_path = self.parquet_dir / filename

        if not prices_path.exists():
            print(f"⚠️  価格データが見つかりません: {prices_path}")
            return pd.DataFrame()

        df = pd.read_parquet(prices_path)
        print(f"✓ Stock prices loaded: {len(df)} rows, interval={interval}")
        return df

    def load_index_prices(self, interval: str = "1d") -> pd.DataFrame:
        """
        指数・ETF価格データを読み込み

        Args:
            interval: データ間隔 (1d, 1h, 15m, 5m, 1mo)

        Returns:
            columns: date, Open, High, Low, Close, Volume, ticker
        """
        file_mapping = {
            "1d": "index_prices_max_1d.parquet",
            "1h": "index_prices_730d_1h.parquet",
            "15m": "index_prices_60d_15m.parquet",
            "5m": "index_prices_60d_5m.parquet",
            "1mo": "index_prices_max_1mo.parquet",
        }

        filename = file_mapping.get(interval)
        if not filename:
            raise ValueError(f"Invalid interval: {interval}")

        index_path = self.parquet_dir / filename

        if not index_path.exists():
            print(f"⚠️  指数データが見つかりません: {index_path}")
            return pd.DataFrame()

        df = pd.read_parquet(index_path)
        print(f"✓ Index prices loaded: {len(df)} rows, interval={interval}")
        return df

    def load_futures_prices(self, interval: str = "1d") -> pd.DataFrame:
        """
        先物価格データを読み込み

        Args:
            interval: データ間隔 (1d, 1h, 15m, 5m, 1mo)

        Returns:
            columns: date, Open, High, Low, Close, Volume, ticker
        """
        file_mapping = {
            "1d": "futures_prices_max_1d.parquet",
            "1h": "futures_prices_730d_1h.parquet",
            "15m": "futures_prices_60d_15m.parquet",
            "5m": "futures_prices_60d_5m.parquet",
            "1mo": "futures_prices_max_1mo.parquet",
        }

        filename = file_mapping.get(interval)
        if not filename:
            raise ValueError(f"Invalid interval: {interval}")

        futures_path = self.parquet_dir / filename

        if not futures_path.exists():
            print(f"⚠️  先物データが見つかりません: {futures_path}")
            return pd.DataFrame()

        df = pd.read_parquet(futures_path)
        print(f"✓ Futures prices loaded: {len(df)} rows, interval={interval}")
        return df

    def load_currency_prices(self, interval: str = "1d") -> pd.DataFrame:
        """
        為替レートデータを読み込み

        Args:
            interval: データ間隔 (1d, 1h, 1mo) ※5m/15mは存在しない

        Returns:
            columns: date, Open, High, Low, Close, Volume, ticker
        """
        file_mapping = {
            "1d": "currency_prices_max_1d.parquet",
            "1h": "currency_prices_730d_1h.parquet",
            "1mo": "currency_prices_max_1mo.parquet",
        }

        filename = file_mapping.get(interval)
        if not filename:
            raise ValueError(f"Invalid interval: {interval}. Choose from {list(file_mapping.keys())}")

        currency_path = self.parquet_dir / filename

        if not currency_path.exists():
            print(f"⚠️  為替データが見つかりません: {currency_path}")
            return pd.DataFrame()

        df = pd.read_parquet(currency_path)
        print(f"✓ Currency prices loaded: {len(df)} rows, interval={interval}")
        return df

    def load_all_stocks(self) -> pd.DataFrame:
        """
        全銘柄メタ情報を読み込み

        Returns:
            columns: ticker, stock_name, sector, market, ...
        """
        all_stocks_path = self.parquet_dir / "all_stocks.parquet"

        if not all_stocks_path.exists():
            print(f"⚠️  all_stocks.parquetが見つかりません: {all_stocks_path}")
            return pd.DataFrame()

        df = pd.read_parquet(all_stocks_path)
        print(f"✓ All stocks loaded: {len(df)} stocks")
        return df


# 使用例
if __name__ == "__main__":
    loader = DataLoader()

    print("\n" + "=" * 70)
    print("データ読み込みテスト")
    print("=" * 70)

    # バックテストデータ
    print("\n[1] Grok Archive")
    archive = loader.load_grok_archive()
    if not archive.empty:
        print(f"  期間: {archive['backtest_date'].min()} ～ {archive['backtest_date'].max()}")
        print(f"  ユニーク日数: {archive['backtest_date'].nunique()}")

    # メタ情報
    print("\n[2] Backtest Meta")
    meta = loader.load_grok_backtest_meta()
    if not meta.empty:
        print(f"  メトリクス数: {len(meta)}")
        # メトリクスをkey-value形式で表示
        for _, row in meta.head(5).iterrows():
            print(f"    {row['metric']}: {row['value']}")

    # マーケットデータ
    print("\n[3] Market Data")
    index_1d = loader.load_index_prices("1d")
    if not index_1d.empty:
        print(f"  指数・ETF: {index_1d['ticker'].nunique()} tickers")

    currency_1d = loader.load_currency_prices("1d")
    if not currency_1d.empty:
        print(f"  為替: {currency_1d['ticker'].nunique()} pairs")

    print("\n" + "=" * 70)
    print("✅ データ読み込み完了")
