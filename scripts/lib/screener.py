#!/usr/bin/env python3
"""
J-Quants Scalping Screener
スキャルピング銘柄選定ロジック（J-Quantsデータ使用）
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Set
import pandas as pd
import numpy as np

# tech_utils_v2をインポート
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.jquants_fetcher import JQuantsFetcher
from server.services.tech_utils_v2 import evaluate_latest_snapshot


class ScalpingScreener:
    """J-Quantsデータを使用したスキャルピング銘柄スクリーニング"""

    def __init__(self, fetcher: JQuantsFetcher | None = None):
        """
        Args:
            fetcher: JQuantsFetcher インスタンス。Noneの場合は自動生成
        """
        self.fetcher = fetcher or JQuantsFetcher()

    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """テクニカル指標を計算"""
        df = df.sort_values(['ticker', 'date']).copy()

        # Previous close
        df['prevClose'] = df.groupby('ticker')['Close'].shift(1)

        # Change %
        df['change_pct'] = ((df['Close'] - df['prevClose']) / df['prevClose'] * 100).round(2)

        # True Range and ATR(14)
        hl = df['High'] - df['Low']
        hp = (df['High'] - df['prevClose']).abs()
        lp = (df['Low'] - df['prevClose']).abs()
        df['tr'] = pd.concat([hl, hp, lp], axis=1).max(axis=1)

        df['atr14'] = (
            df.groupby('ticker', group_keys=False)['tr']
            .apply(lambda s: s.ewm(span=14, adjust=False).mean())
        )

        df['atr14_pct'] = (df['atr14'] / df['Close'] * 100.0).round(2)

        # Moving Averages
        df['ma5'] = df.groupby('ticker', group_keys=False)['Close'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        df['ma25'] = df.groupby('ticker', group_keys=False)['Close'].transform(
            lambda x: x.rolling(window=25, min_periods=1).mean()
        )

        # RSI(14)
        def calculate_rsi(series, period=14):
            delta = series.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi

        df['rsi14'] = df.groupby('ticker', group_keys=False)['Close'].transform(
            lambda x: calculate_rsi(x, 14)
        ).round(2)

        # Volume MA(10) and ratio
        if 'Volume' in df.columns:
            df['vol_ma10'] = df.groupby('ticker', group_keys=False)['Volume'].transform(
                lambda x: x.rolling(window=10, min_periods=1).mean()
            )
            df['vol_ratio'] = (df['Volume'] / df['vol_ma10'] * 100).round(2)
        else:
            df['Volume'] = np.nan
            df['vol_ma10'] = np.nan
            df['vol_ratio'] = np.nan

        return df

    def evaluate_technical_ratings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        各銘柄のテクニカル評価を実行してoverall_ratingを追加

        Args:
            df: 株価データ（ticker, date, Close, High, Low, Volumeを含む）

        Returns:
            overall_rating列が追加されたDataFrame
        """
        print("[INFO] Evaluating technical ratings...")

        df = df.sort_values(['ticker', 'date']).copy()
        ratings = []

        for ticker, grp in df.groupby('ticker', sort=False):
            grp = grp.dropna(subset=['Close']).copy()
            if grp.empty or len(grp) < 20:  # 最低20営業日分のデータが必要
                continue

            # dateをindexに設定
            grp_indexed = grp.set_index('date')

            try:
                snapshot = evaluate_latest_snapshot(grp_indexed)
                overall_label = snapshot['overall']['label']
                ratings.append({'ticker': ticker, 'overall_rating': overall_label})
            except Exception as e:
                print(f"[WARN] Failed to evaluate {ticker}: {e}")
                ratings.append({'ticker': ticker, 'overall_rating': '中立'})

        if not ratings:
            df['overall_rating'] = '中立'
            return df

        rating_df = pd.DataFrame(ratings)
        df = df.merge(rating_df, on='ticker', how='left')
        df['overall_rating'] = df['overall_rating'].fillna('中立')

        print(f"[OK] Evaluated {len(ratings)} stocks")
        return df

    def generate_entry_list(
        self,
        df_latest: pd.DataFrame,
        meta_df: pd.DataFrame,
        top_n: int = 20,
    ) -> pd.DataFrame:
        """
        エントリー向けスキャルピング銘柄リストを生成（初心者向け）

        Args:
            df_latest: 最新日のテクニカル指標付き株価データ（overall_rating必須）
            meta_df: 銘柄メタ情報
            top_n: 上位何件を取得するか

        Returns:
            エントリー向け銘柄リスト
        """
        print("[INFO] Generating entry list (J-Quants)...")

        # テクニカル評価フィルタ: 「買い」「強い買い」のみ
        if 'overall_rating' not in df_latest.columns:
            print("[ERROR] overall_rating column not found. Call evaluate_technical_ratings() first.")
            return pd.DataFrame()

        df_entry = df_latest[
            (df_latest['Close'] >= 100) &
            (df_latest['Close'] <= 1500) &
            (df_latest['Volume'] * df_latest['Close'] >= 100_000_000) &
            (df_latest['atr14_pct'] >= 1.0) &
            (df_latest['atr14_pct'] <= 3.5) &
            (df_latest['change_pct'] >= -3.0) &
            (df_latest['change_pct'] <= 3.0) &
            (df_latest['overall_rating'].isin(['買い', '強い買い']))  # テクニカルフィルタ追加
        ].copy()

        if df_entry.empty:
            print("[WARN] No stocks met entry criteria")
            return pd.DataFrame()

        # Calculate score
        df_entry['score'] = 50.0  # Base score

        # Price appropriateness (300-800 is ideal)
        df_entry['score'] += df_entry['Close'].apply(
            lambda p: 30 if 300 <= p <= 800 else 15
        )

        # Volume stability
        df_entry['score'] += df_entry['vol_ratio'].apply(
            lambda v: 25 if 90 <= v <= 130 else 10
        )

        # Tags
        def get_tags_entry(row):
            tags = []
            if not pd.isna(row['ma5']) and not pd.isna(row['ma25']) and row['ma5'] > row['ma25']:
                tags.append('trend')
            if not pd.isna(row['rsi14']):
                if row['rsi14'] < 40:
                    tags.append('oversold')
                elif row['rsi14'] > 60:
                    tags.append('overbought')
            if not pd.isna(row['vol_ratio']) and 95 <= row['vol_ratio'] <= 120:
                tags.append('stable_volume')
            return tags

        df_entry['tags'] = df_entry.apply(get_tags_entry, axis=1)

        # Key signal
        df_entry['key_signal'] = df_entry.apply(
            lambda r: f"¥{r['Close']:.0f} | {r['change_pct']:+.1f}% | Vol {r['vol_ratio']:.0f}% | ATR {r['atr14_pct']:.1f}%",
            axis=1
        )

        # Merge meta if available
        if meta_df is not None and not meta_df.empty:
            meta_cols = [c for c in ['ticker', 'stock_name', 'market', 'sectors', 'series', 'topixnewindexseries'] if c in meta_df.columns]
            if meta_cols:
                df_entry = df_entry.merge(meta_df[meta_cols], on='ticker', how='left')
                # "-" を null に統一（高市銘柄との整合性）
                if 'topixnewindexseries' in df_entry.columns:
                    df_entry['topixnewindexseries'] = df_entry['topixnewindexseries'].replace('-', None)

        # Sort by score and select top N
        df_entry = df_entry.sort_values('score', ascending=False).drop_duplicates(
            subset=['ticker'], keep='first'
        ).head(top_n)

        # Select columns
        entry_cols = [
            'ticker', 'stock_name', 'market', 'sectors', 'date',
            'Close', 'change_pct', 'Volume', 'vol_ratio',
            'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
        ]
        df_entry = df_entry[[c for c in entry_cols if c in df_entry.columns]]

        print(f"[OK] Entry list: {len(df_entry)} stocks")
        return df_entry

    def generate_active_list(
        self,
        df_latest: pd.DataFrame,
        meta_df: pd.DataFrame,
        entry_tickers: Set[str],
        top_n: int = 20,
    ) -> pd.DataFrame:
        """
        アクティブ向けスキャルピング銘柄リストを生成（上級者向け）

        Args:
            df_latest: 最新日のテクニカル指標付き株価データ（overall_rating必須）
            meta_df: 銘柄メタ情報
            entry_tickers: エントリーリストに含まれる銘柄（除外対象）
            top_n: 上位何件を取得するか

        Returns:
            アクティブ向け銘柄リスト
        """
        print("[INFO] Generating active list (J-Quants)...")

        # テクニカル評価フィルタ: 「買い」「強い買い」のみ
        if 'overall_rating' not in df_latest.columns:
            print("[ERROR] overall_rating column not found. Call evaluate_technical_ratings() first.")
            return pd.DataFrame()

        df_active = df_latest[
            ~df_latest['ticker'].isin(entry_tickers) &
            (df_latest['Close'] >= 100) &
            (df_latest['Close'] <= 3000) &
            ((df_latest['Volume'] * df_latest['Close'] >= 50_000_000) | (df_latest['vol_ratio'] >= 150)) &
            (df_latest['atr14_pct'] >= 2.5) &
            (df_latest['change_pct'].abs() >= 2.0) &
            (df_latest['overall_rating'].isin(['買い', '強い買い']))  # テクニカルフィルタ追加
        ].copy()

        if df_active.empty:
            print("[WARN] No stocks met active criteria")
            return pd.DataFrame()

        # Calculate score
        df_active['score'] = 50.0  # Base score

        # Change score (bigger is better)
        df_active['score'] += df_active['change_pct'].apply(
            lambda c: min(35, abs(c) / 7.0 * 35)
        )

        # Volume surge score
        df_active['score'] += df_active['vol_ratio'].apply(
            lambda v: 30 if v >= 200 else max(0, v / 150 * 30)
        )

        # Tags
        def get_tags_active(row):
            tags = []
            if not pd.isna(row['ma5']) and not pd.isna(row['ma25']) and row['ma5'] > row['ma25']:
                tags.append('trend')
            if not pd.isna(row['rsi14']):
                if row['rsi14'] < 30:
                    tags.append('oversold')
                elif row['rsi14'] > 70:
                    tags.append('overbought')
            if not pd.isna(row['vol_ratio']) and row['vol_ratio'] >= 200:
                tags.append('volume_surge')
            return tags

        df_active['tags'] = df_active.apply(get_tags_active, axis=1)

        # Key signal
        df_active['key_signal'] = df_active.apply(
            lambda r: f"¥{r['Close']:.0f} | {r['change_pct']:+.1f}% | Vol {r['vol_ratio']:.0f}% | ATR {r['atr14_pct']:.1f}%",
            axis=1
        )

        # Merge meta if available
        if meta_df is not None and not meta_df.empty:
            meta_cols = [c for c in ['ticker', 'stock_name', 'market', 'sectors', 'series', 'topixnewindexseries'] if c in meta_df.columns]
            if meta_cols:
                df_active = df_active.merge(meta_df[meta_cols], on='ticker', how='left')
                # "-" を null に統一（高市銘柄との整合性）
                if 'topixnewindexseries' in df_active.columns:
                    df_active['topixnewindexseries'] = df_active['topixnewindexseries'].replace('-', None)

        # Sort by score and select top N
        df_active = df_active.sort_values('score', ascending=False).drop_duplicates(
            subset=['ticker'], keep='first'
        ).head(top_n)

        # Select columns
        active_cols = [
            'ticker', 'stock_name', 'market', 'sectors', 'date',
            'Close', 'change_pct', 'Volume', 'vol_ratio',
            'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal'
        ]
        df_active = df_active[[c for c in active_cols if c in df_active.columns]]

        print(f"[OK] Active list: {len(df_active)} stocks")
        return df_active
