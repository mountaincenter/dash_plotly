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
        target_n: int = 15,
        min_score_threshold: float = 75.0,
        fallback_min_n: int = 5,
    ) -> pd.DataFrame:
        """
        エントリー向けスキャルピング銘柄リストを生成（初心者向け）

        品質保証付き動的選定:
        1. スコア >= min_score_threshold の銘柄を優先選定
        2. target_n件に満たない場合、基準を緩和
        3. 最低でも fallback_min_n 件を確保（0件回避）

        Args:
            df_latest: 最新日のテクニカル指標付き株価データ（overall_rating必須）
            meta_df: 銘柄メタ情報
            target_n: 目標件数（デフォルト15件）
            min_score_threshold: 最低品質基準スコア（デフォルト75点）
            fallback_min_n: 最低保証件数（デフォルト5件）

        Returns:
            エントリー向け銘柄リスト
        """
        print("[INFO] Generating entry list (J-Quants)...")

        # テクニカル評価フィルタ: 売り系を除外（60%合意の王道）
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
            (~df_latest['overall_rating'].isin(['売り', '強い売り']))  # 売り系以外を許容（買い・強い買い・中立）
        ].copy()

        if df_entry.empty:
            print("[WARN] No stocks met entry criteria")
            return pd.DataFrame()

        # Calculate score (100点満点: データ相関分析に基づく修正版)
        df_entry['score'] = 0.0

        # 1. RSI（売られ過ぎからの回復を重視）: 30点 ← overall_rating 40点を再配分
        # 相関分析: RSI高い→損失（-0.1003）→ 逆転が必要
        def score_rsi(rsi):
            if pd.isna(rsi):
                return 10
            # RSI低い（売られ過ぎから回復）を高評価
            if 30 <= rsi <= 45:
                return 30  # 最高点
            elif 45 < rsi <= 55:
                return 20  # 中立域
            elif 25 <= rsi < 30:
                return 25  # やや売られ過ぎ
            elif 55 < rsi <= 65:
                return 10  # やや買われ過ぎ
            else:
                return 5   # 極端な値は低評価

        df_entry['score'] += df_entry['rsi14'].apply(score_rsi)

        # 2. ATR（適度なボラティリティ）: 30点 ← 10点から増強
        # 相関分析: atr14_pct効果なし（-0.0336）だが、唯一正だった指標なので強化
        def score_atr(atr_pct):
            if pd.isna(atr_pct):
                return 10
            # 適度なボラティリティを評価
            if 1.5 <= atr_pct <= 2.5:
                return 30
            elif 2.5 < atr_pct <= 3.0:
                return 25
            elif 1.0 <= atr_pct < 1.5:
                return 20
            elif 3.0 < atr_pct <= 3.5:
                return 15
            else:
                return 10

        df_entry['score'] += df_entry['atr14_pct'].apply(score_atr)

        # 3. 変動率（小さめの変動を評価）: 20点
        # 相関分析: change_pct高い→損失（-0.0730）→ 小さい方が良い
        def score_change(change_pct):
            abs_change = abs(change_pct)
            # 小さめの変動を高評価
            if 0.0 <= abs_change <= 0.5:
                return 20
            elif 0.5 < abs_change <= 1.0:
                return 15
            elif 1.0 < abs_change <= 1.5:
                return 10
            elif 1.5 < abs_change <= 2.0:
                return 5
            else:
                return 0  # 大きな変動は減点

        df_entry['score'] += df_entry['change_pct'].apply(score_change)

        # 4. 出来高比率（安定した出来高を評価）: 20点
        # 相関分析: vol_ratio効果なし（-0.0021）→ 安定域を評価
        def score_vol_ratio(vol_ratio):
            if pd.isna(vol_ratio):
                return 8
            # 安定した出来高（100-130%）を高評価
            if 100 <= vol_ratio <= 130:
                return 20
            elif 80 <= vol_ratio < 100:
                return 15
            elif 130 < vol_ratio <= 150:
                return 12
            elif 150 < vol_ratio <= 180:
                return 8
            else:
                return 5  # 極端な出来高は低評価

        df_entry['score'] += df_entry['vol_ratio'].apply(score_vol_ratio)

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

        # Sort by score (descending)
        df_entry = df_entry.sort_values('score', ascending=False).drop_duplicates(
            subset=['ticker'], keep='first'
        )

        # 品質保証付き動的選定
        high_quality = df_entry[df_entry['score'] >= min_score_threshold]

        if len(high_quality) >= target_n:
            # 十分な高品質銘柄がある → target_n件選定
            df_entry = high_quality.head(target_n)
            print(f"[OK] Selected {len(df_entry)} high-quality stocks (score >= {min_score_threshold})")
        elif len(high_quality) >= fallback_min_n:
            # 高品質は少ないが最低保証以上 → 全て採用
            df_entry = high_quality
            print(f"[INFO] Only {len(df_entry)} stocks met quality threshold (>= {min_score_threshold})")
        else:
            # 高品質が少なすぎる → fallback_min_n 件確保（基準緩和）
            df_entry = df_entry.head(fallback_min_n)
            if len(df_entry) > 0:
                min_actual = df_entry['score'].min()
                print(f"[WARN] Quality threshold not met. Selected top {len(df_entry)} stocks (min score: {min_actual:.1f})")
            else:
                print(f"[WARN] No stocks available for entry")

        # Add selected_date (選定日: 実行日を記録)
        from datetime import datetime
        df_entry['selected_date'] = pd.Timestamp(datetime.now().date())

        # Select columns
        entry_cols = [
            'ticker', 'stock_name', 'market', 'sectors', 'date',
            'Close', 'change_pct', 'Volume', 'vol_ratio',
            'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal', 'selected_date'
        ]
        df_entry = df_entry[[c for c in entry_cols if c in df_entry.columns]]

        print(f"[OK] Entry list: {len(df_entry)} stocks")
        return df_entry

    def generate_active_list(
        self,
        df_latest: pd.DataFrame,
        meta_df: pd.DataFrame,
        entry_tickers: Set[str],
        target_n: int = 15,
        min_score_threshold: float = 85.0,
        fallback_min_n: int = 5,
    ) -> pd.DataFrame:
        """
        アクティブ向けスキャルピング銘柄リストを生成（上級者向け）

        品質保証付き動的選定:
        1. スコア >= min_score_threshold の銘柄を優先選定
        2. target_n件に満たない場合、基準を緩和
        3. 最低でも fallback_min_n 件を確保（0件回避）

        Args:
            df_latest: 最新日のテクニカル指標付き株価データ（overall_rating必須）
            meta_df: 銘柄メタ情報
            entry_tickers: エントリーリストに含まれる銘柄（除外対象）
            target_n: 目標件数（デフォルト15件）
            min_score_threshold: 最低品質基準スコア（デフォルト85点）
            fallback_min_n: 最低保証件数（デフォルト5件）

        Returns:
            アクティブ向け銘柄リスト
        """
        print("[INFO] Generating active list (J-Quants)...")

        # overall_rating条件なし（機械的・ボラティリティ重視）
        # 王道を外れない範囲でリスク許容 = 価格・流動性・ATRのみで選別
        if 'overall_rating' not in df_latest.columns:
            print("[WARN] overall_rating column not found, but continuing without it for Active list")

        df_active = df_latest[
            ~df_latest['ticker'].isin(entry_tickers) &
            (df_latest['Close'] >= 100) &
            (df_latest['Close'] <= 3000) &
            ((df_latest['Volume'] * df_latest['Close'] >= 50_000_000) | (df_latest['vol_ratio'] >= 150)) &
            (df_latest['atr14_pct'] >= 2.5) &
            (df_latest['change_pct'].abs() >= 2.0)
            # overall_rating条件削除: スキャルピングはボラティリティが利益源泉
        ].copy()

        if df_active.empty:
            print("[WARN] No stocks met active criteria")
            return pd.DataFrame()

        # Calculate score (100点満点: 前提に基づく正しいスコアリング)
        df_active['score'] = 0.0

        # 1. ATR（高ボラティリティ重視）: 40点
        def score_atr_active(atr_pct):
            if pd.isna(atr_pct):
                return 5
            if atr_pct >= 5.0:
                return 40
            elif 4.0 <= atr_pct < 5.0:
                return 35
            elif 3.0 <= atr_pct < 4.0:
                return 25
            elif 2.5 <= atr_pct < 3.0:
                return 15
            else:
                return 5

        df_active['score'] += df_active['atr14_pct'].apply(score_atr_active)

        # 2. 変動率（大きな動き）: 30点
        def score_change_active(change_pct):
            abs_change = abs(change_pct)
            if abs_change >= 5.0:
                return 30
            elif 4.0 <= abs_change < 5.0:
                return 25
            elif 3.0 <= abs_change < 4.0:
                return 20
            elif 2.0 <= abs_change < 3.0:
                return 15
            else:
                return 5

        df_active['score'] += df_active['change_pct'].apply(score_change_active)

        # 3. 出来高比率（急騰・急落の兆候）: 20点
        def score_vol_ratio_active(vol_ratio):
            if pd.isna(vol_ratio):
                return 5
            if vol_ratio >= 200:
                return 20
            elif 150 <= vol_ratio < 200:
                return 15
            elif 100 <= vol_ratio < 150:
                return 10
            else:
                return 5

        df_active['score'] += df_active['vol_ratio'].apply(score_vol_ratio_active)

        # 4. RSI（極端な状態を許容）: 10点
        def score_rsi_active(rsi):
            if pd.isna(rsi):
                return 4
            # 反転期待（買われすぎ・売られすぎ）
            if (20 <= rsi <= 30) or (70 <= rsi <= 80):
                return 10
            elif (30 < rsi <= 40) or (60 <= rsi < 70):
                return 8
            elif 40 < rsi < 60:  # 中立は低評価
                return 6
            else:
                return 4

        df_active['score'] += df_active['rsi14'].apply(score_rsi_active)

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

        # Sort by score (descending)
        df_active = df_active.sort_values('score', ascending=False).drop_duplicates(
            subset=['ticker'], keep='first'
        )

        # 品質保証付き動的選定
        high_quality = df_active[df_active['score'] >= min_score_threshold]

        if len(high_quality) >= target_n:
            # 十分な高品質銘柄がある → target_n件選定
            df_active = high_quality.head(target_n)
            print(f"[OK] Selected {len(df_active)} high-quality stocks (score >= {min_score_threshold})")
        elif len(high_quality) >= fallback_min_n:
            # 高品質は少ないが最低保証以上 → 全て採用
            df_active = high_quality
            print(f"[INFO] Only {len(df_active)} stocks met quality threshold (>= {min_score_threshold})")
        else:
            # 高品質が少なすぎる → fallback_min_n 件確保（基準緩和）
            df_active = df_active.head(fallback_min_n)
            if len(df_active) > 0:
                min_actual = df_active['score'].min()
                print(f"[WARN] Quality threshold not met. Selected top {len(df_active)} stocks (min score: {min_actual:.1f})")
            else:
                print(f"[WARN] No stocks available for active")

        # Add selected_date (選定日: 実行日を記録)
        from datetime import datetime
        df_active['selected_date'] = pd.Timestamp(datetime.now().date())

        # Select columns
        active_cols = [
            'ticker', 'stock_name', 'market', 'sectors', 'date',
            'Close', 'change_pct', 'Volume', 'vol_ratio',
            'atr14_pct', 'rsi14', 'score', 'tags', 'key_signal', 'selected_date'
        ]
        df_active = df_active[[c for c in active_cols if c in df_active.columns]]

        print(f"[OK] Active list: {len(df_active)} stocks")
        return df_active
