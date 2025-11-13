#!/usr/bin/env python3
"""
analyze_grok_segments.py
Grokファンドのセグメント別パフォーマンスを分析

実行方法:
    python3 scripts/analyze_grok_segments.py

出力:
    - category × 戦略（どのトピックでどの戦略が勝つか）
    - 株価帯 × 戦略（1500円以上/以下でどちらが有利か）
    - grok_rank × 戦略（上位5銘柄 vs 下位5銘柄）
    - 時価総額 × 戦略（500億以上/以下で戦略を分析）※データ追加後
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
BACKTEST_ARCHIVE_PATH = PARQUET_DIR / "backtest" / "grok_trending_archive.parquet"


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
    """セグメント別分析のメイン処理"""

    if not BACKTEST_ARCHIVE_PATH.exists():
        print(f"[ERROR] バックテストアーカイブが見つかりません: {BACKTEST_ARCHIVE_PATH}")
        return

    # データ読み込み
    df = pd.read_parquet(BACKTEST_ARCHIVE_PATH)

    if df.empty:
        print("[WARN] バックテストデータが空です")
        return

    # 期間
    start_date = df['backtest_date'].min()
    end_date = df['backtest_date'].max()
    total_stocks = len(df)

    print("=" * 80)
    print(f"Grokファンド セグメント別パフォーマンス分析")
    print(f"期間: {start_date} 〜 {end_date}")
    print(f"総取引数: {total_stocks}銘柄")
    print("=" * 80)
    print()

    # データが少ない場合の警告
    if total_stocks < 50:
        print("⚠️  警告: データ量が少ないため、統計的に有意でない可能性があります")
        print(f"   推奨: 最低100銘柄（現在{total_stocks}銘柄）")
        print()

    # 1. カテゴリ × 戦略
    if 'category' in df.columns:
        analyze_segment_performance(df, "カテゴリ（トピック）× 戦略", "category")

    # 2. 株価帯 × 戦略
    df['price_range'] = pd.cut(
        df['buy_price'],
        bins=[0, 1500, 5000, float('inf')],
        labels=['1500円以下', '1500-5000円', '5000円以上']
    )
    analyze_segment_performance(df, "株価帯 × 戦略", "price_range")

    # 3. Grokランク × 戦略
    df['rank_group'] = pd.cut(
        df['grok_rank'],
        bins=[0, 5, 10, float('inf')],
        labels=['上位5銘柄', '6-10位', '11位以下']
    )
    analyze_segment_performance(df, "Grokランク × 戦略", "rank_group")

    # 4. 時価総額 × 戦略（データが追加されている場合）
    if 'market_cap' in df.columns:
        # 時価総額が取得できている銘柄のみでフィルタ
        df_with_market_cap = df[df['market_cap'].notna()].copy()

        if len(df_with_market_cap) > 0:
            # 中央値を計算
            median_market_cap = df_with_market_cap['market_cap'].median()
            median_oku = median_market_cap / 1e8

            # 中央値で分類
            df_with_market_cap['market_cap_group'] = pd.cut(
                df_with_market_cap['market_cap'],
                bins=[0, median_market_cap, float('inf')],
                labels=[f'{median_oku:.0f}億円未満（小型株）', f'{median_oku:.0f}億円以上（大型株）']
            )

            print("=" * 80)
            print(f"【時価総額 × 戦略】（中央値: ¥{median_oku:.0f}億円）")
            print("=" * 80)
            print(f"データ取得済み銘柄: {len(df_with_market_cap)}/{len(df)}銘柄")
            print()

            analyze_segment_performance(df_with_market_cap, f"時価総額 × 戦略（中央値: {median_oku:.0f}億円）", "market_cap_group")
        else:
            print("=" * 80)
            print("【時価総額 × 戦略】")
            print("=" * 80)
            print("⚠️  時価総額データが取得できていません")
            print()
    else:
        print("=" * 80)
        print("【時価総額 × 戦略】")
        print("=" * 80)
        print("⚠️  時価総額データが未追加です")
        print("   次のステップ: save_backtest_to_archive.py に時価総額データを追加")
        print()

    print("=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
