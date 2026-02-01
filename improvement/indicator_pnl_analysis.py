#!/usr/bin/env python3
"""
change_pct / ATR / market_cap 帯別 損益・勝率分析（PNG出力）
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

plt.rcParams['font.family'] = 'Hiragino Sans'


def plot_pnl_winrate(df: pd.DataFrame, col: str, title: str, bins: list, labels: list, output_path: str):
    """帯別の損益・勝率をPNGで出力（大引のみ）"""

    # ビン分け
    df_valid = df[df[col].notna()].copy()
    df_valid['bin'] = pd.cut(df_valid[col], bins=bins, labels=labels, include_lowest=True)

    # 帯別集計（ショート戦略: 損益反転、勝率反転）
    results = []
    for label in labels:
        band_df = df_valid[df_valid['bin'] == label]
        count = len(band_df)
        # ショート戦略: 損益を反転
        pnl_series = -band_df['profit_per_100_shares_phase2'].fillna(0)
        pnl_plus = pnl_series[pnl_series > 0].sum()
        pnl_minus = pnl_series[pnl_series < 0].sum()
        pnl = pnl_series.sum()
        # ショート戦略: 勝率を反転
        win_rate = (~band_df['phase2_win'].fillna(True)).mean() * 100 if count > 0 else 0
        results.append({'label': label, 'count': count, 'pnl': pnl, 'pnl_plus': pnl_plus, 'pnl_minus': pnl_minus, 'win_rate': win_rate})

    df_results = pd.DataFrame(results)
    total_count = df_results['count'].sum()
    total_pnl = df_results['pnl'].sum()

    # プロット（2段: 上=勝率、下=損益）
    fig, axes = plt.subplots(2, 1, figsize=(14, 10), facecolor='#1a1a2e')

    x = range(len(labels))

    # --- 上段: 勝率 ---
    ax1 = axes[0]
    ax1.set_facecolor('#1a1a2e')

    win_values = df_results['win_rate']
    colors_win = ['#4ecdc4' if v >= 50 else '#e74c3c' for v in win_values]
    bars1 = ax1.bar(x, df_results['count'], color=colors_win)

    # バーの上に件数と勝率を表示
    for i, (bar, count, win) in enumerate(zip(bars1, df_results['count'], win_values)):
        if count > 0:
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                    f'{count}\n{win:.0f}%', ha='center', va='bottom', color='white', fontsize=9)

    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, color='#888', fontsize=10)
    ax1.set_ylabel('件数', color='#888')
    ax1.tick_params(axis='y', colors='#888')
    ax1.set_title(f'{title}（{total_count}件）', color='white', fontsize=14, pad=10)

    ax1.grid(axis='y', alpha=0.3, color='#444')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['bottom'].set_color('#444')
    ax1.spines['left'].set_color('#444')

    # --- 下段: 損益（プラス/マイナス別） ---
    ax2 = axes[1]
    ax2.set_facecolor('#1a1a2e')

    # プラス損益（緑、上向き）
    bars_plus = ax2.bar(x, df_results['pnl_plus'], color='#4ecdc4', label='利益')
    # マイナス損益（赤、下向き）
    bars_minus = ax2.bar(x, df_results['pnl_minus'], color='#e74c3c', label='損失')

    # バーの上に損益を表示
    for i in range(len(labels)):
        if df_results.loc[i, 'count'] > 0:
            pnl_plus = df_results.loc[i, 'pnl_plus']
            pnl_minus = df_results.loc[i, 'pnl_minus']
            if pnl_plus > 0:
                ax2.text(i, pnl_plus + 5000, f'+{pnl_plus/1000:.0f}k',
                        ha='center', va='bottom', color='#4ecdc4', fontsize=8)
            if pnl_minus < 0:
                ax2.text(i, pnl_minus - 5000, f'{pnl_minus/1000:.0f}k',
                        ha='center', va='top', color='#e74c3c', fontsize=8)

    ax2.axhline(y=0, color='#666', linewidth=0.5)
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, color='#888', fontsize=10)
    ax2.set_xlabel(col, color='#888')
    ax2.set_ylabel('損益（円）', color='#888')
    ax2.tick_params(axis='y', colors='#888')

    sign = '+' if total_pnl >= 0 else ''
    ax2.set_title(f'{title} 損益（計{sign}{total_pnl:,.0f}円）', color='white', fontsize=14, pad=10)

    ax2.grid(axis='y', alpha=0.3, color='#444')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['bottom'].set_color('#444')
    ax2.spines['left'].set_color('#444')

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, facecolor='#1a1a2e', edgecolor='none')
    plt.close()
    print(f"Saved: {output_path}")

    # コンソールにも出力
    print(f"\n=== {title} 帯別損益・勝率 ===")
    print(df_results.to_string(index=False))


def main():
    # データ読み込み
    archive_path = 'data/parquet/backtest/grok_trending_archive.parquet'
    df = pd.read_parquet(archive_path)
    print(f"Archive: {len(df)} records")

    # analysisと同じフィルタリング
    df = df[df['buy_price'].notna()].copy()
    df['date'] = pd.to_datetime(df['backtest_date']).dt.normalize()
    df = df[df['date'] >= '2025-11-04']
    df = df[(df['shortable'] == True) | ((df['day_trade'] == True) & (df['shortable'] == False))]
    print(f"After filter: {len(df)} records")

    # 1. change_pct
    change_bins = [-100, -10, -5, -3, -1, 0, 1, 3, 5, 10, 100]
    change_labels = ['<-10', '-10~-5', '-5~-3', '-3~-1', '-1~0', '0~1', '1~3', '3~5', '5~10', '>10']
    plot_pnl_winrate(df, 'change_pct', '変化率', change_bins, change_labels,
                     'improvement/output/change_pct_pnl.png')

    # 2. ATR（全体）
    atr_bins = [0, 2, 3, 4, 5, 6, 7, 8, 10, 15, 100]
    atr_labels = ['0-2', '2-3', '3-4', '4-5', '5-6', '6-7', '7-8', '8-10', '10-15', '>15']
    plot_pnl_winrate(df, 'atr14_pct', 'ATR14%', atr_bins, atr_labels,
                     'improvement/output/atr_pnl.png')

    # 2a. ATR（制度信用）
    df_seido = df[df['shortable'] == True]
    plot_pnl_winrate(df_seido, 'atr14_pct', 'ATR14%（制度信用）', atr_bins, atr_labels,
                     'improvement/output/atr_pnl_seido.png')

    # 2b. ATR（いちにち信用）- 8,9を分ける
    df_ichi = df[(df['shortable'] == False) & (df['day_trade'] == True)]
    atr_bins_ichi = [0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 100]
    atr_labels_ichi = ['0-2', '2-3', '3-4', '4-5', '5-6', '6-7', '7-8', '8-9', '9-10', '10-15', '>15']
    plot_pnl_winrate(df_ichi, 'atr14_pct', 'ATR14%（いちにち信用）', atr_bins_ichi, atr_labels_ichi,
                     'improvement/output/atr_pnl_ichinichi.png')

    # 3. market_cap（億円）
    df['market_cap_oku'] = df['market_cap'] / 1e8
    cap_bins = [0, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 1e10]
    cap_labels = ['<50億', '50-100億', '100-200億', '200-500億', '500-1000億', '1000-2000億', '2000-5000億', '5000億-1兆', '>1兆']
    plot_pnl_winrate(df, 'market_cap_oku', '時価総額', cap_bins, cap_labels,
                     'improvement/output/market_cap_pnl.png')

    print("\n[DONE]")


if __name__ == "__main__":
    main()
