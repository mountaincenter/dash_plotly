#!/usr/bin/env python3
"""
change_pct / ATR / market_cap 分布分析（PNG出力）
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

# 日本語フォント設定
plt.rcParams['font.family'] = 'Hiragino Sans'


def plot_distribution(df: pd.DataFrame, col: str, title: str, bins: list, labels: list, output_path: str):
    """分布PNGを生成"""

    # 統計
    stats = df[col].describe()
    total = len(df)
    na_count = df[col].isna().sum()
    valid = df[col].dropna()

    # ビン集計
    df_valid = df[df[col].notna()].copy()
    df_valid['bin'] = pd.cut(df_valid[col], bins=bins, labels=labels, include_lowest=True)
    bin_counts = df_valid['bin'].value_counts().sort_index()

    # プロット
    fig, ax = plt.subplots(figsize=(12, 6), facecolor='#1a1a2e')
    ax.set_facecolor('#1a1a2e')

    x = range(len(labels))
    counts = [bin_counts.get(label, 0) for label in labels]

    bars = ax.bar(x, counts, color='#4a90d9', edgecolor='#74c0fc', linewidth=1)

    # バーの上に件数表示
    for bar, count in zip(bars, counts):
        if count > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(counts)*0.02,
                   str(count), ha='center', va='bottom', color='white', fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right', color='#888')
    ax.set_ylabel('件数', color='#888')
    ax.tick_params(axis='y', colors='#888')

    # タイトルと統計情報
    title_text = f'{title}\n総件数: {total}件 | 平均: {stats["mean"]:.2f} | 中央値: {stats["50%"]:.2f} | 標準偏差: {stats["std"]:.2f}'
    ax.set_title(title_text, color='white', fontsize=12, pad=15)

    # グリッド
    ax.grid(axis='y', alpha=0.3, color='#444')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color('#444')
    ax.spines['left'].set_color('#444')

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor='#1a1a2e', edgecolor='none')
    plt.close()
    print(f"Saved: {output_path}")


def main():
    # データ読み込み
    archive_path = 'data/parquet/backtest/grok_trending_archive.parquet'
    df = pd.read_parquet(archive_path)
    print(f"Archive: {len(df)} records")

    # 1. change_pct 分布
    print("\n=== change_pct 分布 ===")
    print(df['change_pct'].describe())

    change_bins = [-100, -10, -5, -3, -1, 0, 1, 3, 5, 10, 100]
    change_labels = ['<-10', '-10~-5', '-5~-3', '-3~-1', '-1~0', '0~1', '1~3', '3~5', '5~10', '>10']

    plot_distribution(df, 'change_pct', '変化率 (change_pct) 分布', change_bins, change_labels,
                     'improvement/output/change_pct_distribution.png')

    # 2. ATR 分布
    print("\n=== atr14_pct 分布 ===")
    print(df['atr14_pct'].describe())

    atr_bins = [0, 2, 3, 4, 5, 6, 7, 8, 10, 15, 100]
    atr_labels = ['0-2', '2-3', '3-4', '4-5', '5-6', '6-7', '7-8', '8-10', '10-15', '>15']

    plot_distribution(df, 'atr14_pct', 'ATR14% 分布', atr_bins, atr_labels,
                     'improvement/output/atr_distribution.png')

    # 3. market_cap 分布（億円単位）
    print("\n=== market_cap 分布 ===")
    df['market_cap_oku'] = df['market_cap'] / 1e8  # 億円に変換
    print(df['market_cap_oku'].describe())

    cap_bins = [0, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 1e10]
    cap_labels = ['<50億', '50-100億', '100-200億', '200-500億', '500-1000億', '1000-2000億', '2000-5000億', '5000億-1兆', '>1兆']

    plot_distribution(df, 'market_cap_oku', '時価総額 分布', cap_bins, cap_labels,
                     'improvement/output/market_cap_distribution.png')

    print("\n[DONE]")


if __name__ == "__main__":
    main()
