#!/usr/bin/env python3
"""
analyze_market_segments.py
マーケット要因別のパフォーマンスを分析

実行方法:
    python3 scripts/analyze_market_segments.py

分析内容:
    - 日経平均の騰落 × 戦略
    - TOPIX騰落 × 戦略
    - 為替変動 × 戦略
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
from common_cfg.paths import PARQUET_DIR

# パス定義
ARCHIVE_WITH_MARKET_PATH = PARQUET_DIR / "backtest" / "grok_trending_archive_with_market.parquet"


def analyze_segment_performance(df: pd.DataFrame, segment_name: str, segment_column: str):
    """
    セグメント別のパフォーマンスを分析

    Args:
        df: バックテストデータ
        segment_name: セグメント名（表示用）
        segment_column: セグメント列名
    """
    print("=" * 80)
    print(f"【{segment_name}】")
    print("=" * 80)
    print()

    strategies = [
        ('Phase1（前場勝負）', 'profit_per_100_shares_phase1', 'phase1_win'),
        ('Phase2（大引勝負）', 'profit_per_100_shares_phase2', 'phase2_win'),
        ('Phase3（±1%利確損切）', 'profit_per_100_shares_phase3_1pct', 'phase3_1pct_win'),
        ('Phase3（±2%利確損切）', 'profit_per_100_shares_phase3_2pct', 'phase3_2pct_win'),
        ('Phase3（±3%利確損切）', 'profit_per_100_shares_phase3_3pct', 'phase3_3pct_win'),
    ]

    for segment_value in sorted(df[segment_column].unique()):
        segment_df = df[df[segment_column] == segment_value]

        if len(segment_df) < 3:
            # サンプル数が少ない場合はスキップ
            continue

        print(f"📊 {segment_value} ({len(segment_df)}銘柄)")
        print()

        results = []

        for strategy_name, profit_col, win_col in strategies:
            # 累積利益
            cumulative_profit = segment_df[profit_col].sum()

            # 累積投資額
            cumulative_investment = (segment_df['buy_price'] * 100).sum()

            # 累積利益率
            cumulative_return_pct = (cumulative_profit / cumulative_investment * 100) if cumulative_investment > 0 else 0

            # 勝率
            win_rate = (segment_df[win_col].sum() / len(segment_df) * 100)

            # 平均利益
            avg_profit = segment_df[profit_col].mean()

            results.append({
                '戦略': strategy_name,
                '累積利益': cumulative_profit,
                '累積利益率(%)': cumulative_return_pct,
                '勝率(%)': win_rate,
                '平均利益': avg_profit
            })

        # ベスト戦略を表示
        df_results = pd.DataFrame(results)
        best_strategy = df_results.loc[df_results['累積利益'].idxmax()]

        for _, row in df_results.iterrows():
            marker = "⭐" if row['戦略'] == best_strategy['戦略'] else "  "
            print(f"  {marker} {row['戦略']:<25} 利益:¥{row['累積利益']:>8,.0f} ({row['累積利益率(%)']:>+6.2f}%)  勝率:{row['勝率(%)']:>5.1f}%  平均:¥{row['平均利益']:>7,.0f}")

        print()
        print(f"  💡 ベスト: {best_strategy['戦略']} (¥{best_strategy['累積利益']:,.0f})")
        print()

    print()


def main():
    """マーケット要因別分析のメイン処理"""

    if not ARCHIVE_WITH_MARKET_PATH.exists():
        print(f"[ERROR] マーケットデータ付きアーカイブが見つかりません: {ARCHIVE_WITH_MARKET_PATH}")
        print("先に scripts/add_market_data_to_archive.py を実行してください")
        return

    # データ読み込み
    df = pd.read_parquet(ARCHIVE_WITH_MARKET_PATH)

    if df.empty:
        print("[WARN] バックテストデータが空です")
        return

    # 期間
    start_date = df['backtest_date'].min()
    end_date = df['backtest_date'].max()
    total_stocks = len(df)

    print("=" * 80)
    print(f"Grokファンド マーケット要因別パフォーマンス分析")
    print(f"期間: {start_date} 〜 {end_date}")
    print(f"総取引数: {total_stocks}銘柄")
    print("=" * 80)
    print()

    # データが少ない場合の警告
    if total_stocks < 50:
        print("⚠️  警告: データ量が少ないため、統計的に有意でない可能性があります")
        print(f"   推奨: 最低100銘柄（現在{total_stocks}銘柄）")
        print()

    # 1. 日経平均騰落 × 戦略（Phase2用）
    if 'daily_nikkei_return' in df.columns:
        df_with_nikkei = df[df['daily_nikkei_return'].notna()].copy()

        if len(df_with_nikkei) > 0:
            df_with_nikkei['nikkei_direction'] = df_with_nikkei['daily_nikkei_return'].apply(
                lambda x: '日経上昇（プラス）' if x > 0 else '日経下落（マイナス）'
            )
            analyze_segment_performance(df_with_nikkei, "日経平均騰落 × 戦略（Phase2用）", "nikkei_direction")

    # 2. TOPIX騰落 × 戦略（Phase2用）
    if 'daily_topix_return' in df.columns:
        df_with_topix = df[df['daily_topix_return'].notna()].copy()

        if len(df_with_topix) > 0:
            df_with_topix['topix_direction'] = df_with_topix['daily_topix_return'].apply(
                lambda x: 'TOPIX上昇（プラス）' if x > 0 else 'TOPIX下落（マイナス）'
            )
            analyze_segment_performance(df_with_topix, "TOPIX騰落 × 戦略（Phase2用）", "topix_direction")

    # 3. マザーズ騰落 × 戦略（Phase2用）
    if 'daily_mothers_return' in df.columns:
        df_with_mothers = df[df['daily_mothers_return'].notna()].copy()

        if len(df_with_mothers) > 0:
            df_with_mothers['mothers_direction'] = df_with_mothers['daily_mothers_return'].apply(
                lambda x: 'マザーズ上昇（プラス）' if x > 0 else 'マザーズ下落（マイナス）'
            )
            analyze_segment_performance(df_with_mothers, "マザーズ指数騰落 × 戦略（Phase2用）", "mothers_direction")

    # 4. 日経平均の前場騰落 × 戦略（Phase1用）
    if 'morning_nikkei_return' in df.columns:
        df_with_morning = df[df['morning_nikkei_return'].notna()].copy()

        if len(df_with_morning) > 0:
            df_with_morning['morning_direction'] = df_with_morning['morning_nikkei_return'].apply(
                lambda x: '前場上昇（プラス）' if x > 0 else '前場下落（マイナス）'
            )
            analyze_segment_performance(df_with_morning, "前場（9:00-11:30）日経平均騰落 × 戦略（Phase1用）", "morning_direction")

    # 5. 日経平均の変動幅 × 戦略（Phase2用）
    if 'daily_nikkei_return' in df.columns:
        df_with_nikkei = df[df['daily_nikkei_return'].notna()].copy()

        if len(df_with_nikkei) > 0:
            # 変動幅で分類（±1%）
            df_with_nikkei['nikkei_volatility'] = df_with_nikkei['daily_nikkei_return'].apply(
                lambda x: '大幅上昇（+1%以上）' if x >= 0.01
                else '大幅下落（-1%以下）' if x <= -0.01
                else '安定（±1%未満）'
            )
            analyze_segment_performance(df_with_nikkei, "日経平均変動幅 × 戦略（Phase2用）", "nikkei_volatility")

    print("=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
