"""
Grok銘柄分析の可視化HTMLレポート生成

日本語フォントを確実に使用してグラフを生成し、HTMLレポートを作成します。
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # バックエンド設定
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from pathlib import Path
import base64
from io import BytesIO
import warnings

warnings.filterwarnings('ignore')

# 日本語フォント設定
import japanize_matplotlib

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / 'test_output' / 'test_grok_analysis_base_20251107_v3.parquet'
OUTPUT_PATH = BASE_DIR / 'test_output' / 'grok_analysis_report.html'


def fig_to_base64(fig):
    """matplotlibのfigureをbase64エンコードされた画像に変換"""
    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    img_base64 = base64.b64encode(buf.read()).decode('utf-8')
    plt.close(fig)
    return f'data:image/png;base64,{img_base64}'


def create_phase_comparison_chart(df):
    """Phase別勝率比較グラフ"""
    phases = {
        'Phase1\n(前場終了時)': ('phase1_return_pct', 'phase1_win'),
        'Phase2\n(当日終値)': ('phase2_return_pct', 'phase2_win'),
        'Phase3-1%\n(損切-1%)': ('phase3_1pct_return_pct', 'phase3_1pct_win'),
        'Phase3-2%\n(損切-2%)': ('phase3_2pct_return_pct', 'phase3_2pct_win'),
        'Phase3-3%\n(損切-3%)': ('phase3_3pct_return_pct', 'phase3_3pct_win'),
    }

    win_rates = []
    phase_names = []

    for phase_name, (_, win_col) in phases.items():
        win_rate = df[win_col].sum() / len(df) * 100
        win_rates.append(win_rate)
        phase_names.append(phase_name)

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = ['green' if wr >= 50 else 'red' for wr in win_rates]
    bars = ax.bar(phase_names, win_rates, color=colors, alpha=0.7, edgecolor='black')

    for bar, wr in zip(bars, win_rates):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 1,
                f'{wr:.1f}%', ha='center', va='bottom', fontsize=11, fontweight='bold')

    ax.axhline(50, color='black', linestyle='--', linewidth=1.5, label='50%基準線')
    ax.set_title('Phase別勝率比較', fontsize=14, fontweight='bold')
    ax.set_ylabel('勝率 (%)', fontsize=12)
    ax.set_ylim(0, 100)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()

    return fig_to_base64(fig)


def create_return_distribution(df):
    """リターン分布ヒストグラム"""
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    axes = axes.flatten()

    return_cols = [
        ('phase1_return_pct', 'Phase1 (前場終了時)'),
        ('phase2_return_pct', 'Phase2 (当日終値)'),
        ('phase3_1pct_return_pct', 'Phase3-1% (損切-1%)'),
        ('phase3_2pct_return_pct', 'Phase3-2% (損切-2%)'),
        ('phase3_3pct_return_pct', 'Phase3-3% (損切-3%)')
    ]

    for idx, (col, title) in enumerate(return_cols):
        ax = axes[idx]
        ax.hist(df[col].dropna(), bins=20, color='steelblue', alpha=0.7, edgecolor='black')

        mean_val = df[col].mean()
        ax.axvline(mean_val, color='red', linestyle='--', linewidth=2, label=f'平均: {mean_val:.2f}%')
        ax.axvline(0, color='gray', linestyle='-', linewidth=1)

        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.set_xlabel('リターン (%)', fontsize=10)
        ax.set_ylabel('件数', fontsize=10)
        ax.legend()
        ax.grid(True, alpha=0.3)

    axes[-1].axis('off')
    plt.tight_layout()

    return fig_to_base64(fig)


def create_category_analysis(df):
    """カテゴリー別分析"""
    category_stats = df.groupby('category').agg({
        'phase2_win': ['sum', 'count', lambda x: x.sum() / len(x) * 100],
        'phase2_return_pct': 'mean'
    }).round(2)

    category_stats.columns = ['勝ち数', '総数', '勝率(%)', '平均リターン(%)']
    category_stats = category_stats.sort_values('勝率(%)', ascending=False)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # 勝率
    category_stats['勝率(%)'].plot(kind='barh', ax=ax1, color='steelblue', alpha=0.7, edgecolor='black')
    ax1.axvline(50, color='red', linestyle='--', linewidth=1.5)
    ax1.set_title('カテゴリー別 勝率', fontsize=12, fontweight='bold')
    ax1.set_xlabel('勝率 (%)', fontsize=10)
    ax1.set_ylabel('カテゴリー', fontsize=10)
    ax1.grid(True, alpha=0.3, axis='x')

    # 平均リターン
    category_stats_sorted = category_stats.sort_values('平均リターン(%)', ascending=False)
    category_stats_sorted['平均リターン(%)'].plot(kind='barh', ax=ax2, color='coral', alpha=0.7, edgecolor='black')
    ax2.axvline(0, color='black', linestyle='--', linewidth=1.5)
    ax2.set_title('カテゴリー別 平均リターン', fontsize=12, fontweight='bold')
    ax2.set_xlabel('平均リターン (%)', fontsize=10)
    ax2.set_ylabel('カテゴリー', fontsize=10)
    ax2.grid(True, alpha=0.3, axis='x')

    plt.tight_layout()

    return fig_to_base64(fig)


def create_daily_analysis(df):
    """日別分析"""
    daily_stats = df.groupby('backtest_date').agg({
        'phase1_return_pct': 'mean',
        'phase2_return_pct': 'mean',
        'phase3_2pct_return_pct': 'mean',
        'ticker': 'count'
    }).round(2)

    daily_stats.columns = ['Phase1平均(%)', 'Phase2平均(%)', 'Phase3-2%平均(%)', '銘柄数']

    fig, ax = plt.subplots(figsize=(14, 7))

    daily_stats[['Phase1平均(%)', 'Phase2平均(%)', 'Phase3-2%平均(%)']].plot(
        ax=ax, marker='o', linewidth=2.5, markersize=8
    )

    ax.axhline(0, color='black', linestyle='--', linewidth=1.5)
    ax.set_title('日別 平均リターン推移', fontsize=14, fontweight='bold')
    ax.set_xlabel('日付', fontsize=12)
    ax.set_ylabel('平均リターン (%)', fontsize=12)
    ax.legend(loc='best')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    return fig_to_base64(fig)


def create_previous_day_analysis(df):
    """前日変化率による勝率分析"""
    # 前日変化率を4分位に分割
    df_valid = df[df['prev_day_change_pct'].notna()].copy()

    if len(df_valid) == 0:
        return None

    df_valid['prev_change_quartile'] = pd.qcut(
        df_valid['prev_day_change_pct'],
        q=4,
        labels=['Q1(大幅下落)', 'Q2(下落)', 'Q3(上昇)', 'Q4(大幅上昇)'],
        duplicates='drop'
    )

    # 区分別の勝率
    quartile_stats = df_valid.groupby('prev_change_quartile').agg({
        'phase2_win': ['sum', 'count', lambda x: x.sum() / len(x) * 100],
        'phase2_return_pct': 'mean',
        'prev_day_change_pct': 'mean'
    }).round(2)

    quartile_stats.columns = ['勝ち数', '総数', '勝率(%)', '平均リターン(%)', '平均前日変化率(%)']

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # 勝率
    quartile_stats['勝率(%)'].plot(kind='bar', ax=ax1, color='steelblue', alpha=0.7, edgecolor='black')
    ax1.axhline(50, color='red', linestyle='--', linewidth=1.5)
    ax1.set_title('前日変化率区分別 勝率', fontsize=12, fontweight='bold')
    ax1.set_ylabel('勝率 (%)', fontsize=10)
    ax1.set_xlabel('前日変化率区分', fontsize=10)
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45, ha='right')

    # 平均リターン
    quartile_stats['平均リターン(%)'].plot(kind='bar', ax=ax2, color='coral', alpha=0.7, edgecolor='black')
    ax2.axhline(0, color='black', linestyle='--', linewidth=1.5)
    ax2.set_title('前日変化率区分別 平均リターン', fontsize=12, fontweight='bold')
    ax2.set_ylabel('平均リターン (%)', fontsize=10)
    ax2.set_xlabel('前日変化率区分', fontsize=10)
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.set_xticklabels(ax2.get_xticklabels(), rotation=45, ha='right')

    plt.tight_layout()

    return fig_to_base64(fig)


def create_volume_ratio_analysis(df):
    """前日出来高比による勝率分析"""
    # 前日出来高比が有効なデータのみ
    df_valid = df[df['prev_day_volume_ratio'].notna()].copy()

    if len(df_valid) == 0:
        return None

    # 出来高比を4分位に分割
    df_valid['volume_ratio_quartile'] = pd.qcut(
        df_valid['prev_day_volume_ratio'],
        q=4,
        labels=['Q1(低)', 'Q2', 'Q3', 'Q4(高)'],
        duplicates='drop'
    )

    # 区分別の勝率
    quartile_stats = df_valid.groupby('volume_ratio_quartile').agg({
        'phase2_win': ['sum', 'count', lambda x: x.sum() / len(x) * 100],
        'phase2_return_pct': 'mean',
        'prev_day_volume_ratio': 'mean'
    }).round(2)

    quartile_stats.columns = ['勝ち数', '総数', '勝率(%)', '平均リターン(%)', '平均出来高比']

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # 勝率
    quartile_stats['勝率(%)'].plot(kind='bar', ax=ax1, color='steelblue', alpha=0.7, edgecolor='black')
    ax1.axhline(50, color='red', linestyle='--', linewidth=1.5)
    ax1.set_title('前日出来高比区分別 勝率', fontsize=12, fontweight='bold')
    ax1.set_ylabel('勝率 (%)', fontsize=10)
    ax1.set_xlabel('前日出来高比区分', fontsize=10)
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.set_xticklabels(ax1.get_xticklabels(), rotation=0)

    # 平均リターン
    quartile_stats['平均リターン(%)'].plot(kind='bar', ax=ax2, color='coral', alpha=0.7, edgecolor='black')
    ax2.axhline(0, color='black', linestyle='--', linewidth=1.5)
    ax2.set_title('前日出来高比区分別 平均リターン', fontsize=12, fontweight='bold')
    ax2.set_ylabel('平均リターン (%)', fontsize=10)
    ax2.set_xlabel('前日出来高比区分', fontsize=10)
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.set_xticklabels(ax2.get_xticklabels(), rotation=0)

    plt.tight_layout()

    return fig_to_base64(fig)


def create_plus_minus_comparison(df):
    """前日プラス vs マイナス銘柄の比較"""
    # 前日変化率が有効なデータのみ
    df_valid = df[df['prev_day_change_pct'].notna()].copy()

    if len(df_valid) == 0:
        return None

    # 前日プラス/マイナスで分類
    df_valid['prev_direction'] = df_valid['prev_day_change_pct'].apply(
        lambda x: '前日プラス' if x >= 0 else '前日マイナス'
    )

    # 集計
    summary = df_valid.groupby('prev_direction').agg({
        'ticker': 'count',
        'phase1_win': lambda x: x.sum() / len(x) * 100,
        'phase2_win': lambda x: x.sum() / len(x) * 100,
        'phase1_return_pct': 'mean',
        'phase2_return_pct': 'mean',
    }).round(2)

    summary.columns = ['銘柄数', 'Phase1勝率(%)', 'Phase2勝率(%)', 'Phase1平均リターン(%)', 'Phase2平均リターン(%)']

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # 勝率比較
    categories = summary.index.tolist()
    phase1_wins = summary['Phase1勝率(%)'].tolist()
    phase2_wins = summary['Phase2勝率(%)'].tolist()

    x = range(len(categories))
    width = 0.35

    ax1.bar([i - width/2 for i in x], phase1_wins, width, label='Phase1勝率', alpha=0.7, edgecolor='black')
    ax1.bar([i + width/2 for i in x], phase2_wins, width, label='Phase2勝率', alpha=0.7, edgecolor='black')
    ax1.axhline(50, color='red', linestyle='--', linewidth=1.5, label='50%基準線')
    ax1.set_ylabel('勝率 (%)', fontsize=10)
    ax1.set_xlabel('前日動向', fontsize=10)
    ax1.set_title('前日プラス vs マイナス - 勝率比較', fontsize=12, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(categories)
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')

    # 平均リターン比較
    phase1_returns = summary['Phase1平均リターン(%)'].tolist()
    phase2_returns = summary['Phase2平均リターン(%)'].tolist()

    ax2.bar([i - width/2 for i in x], phase1_returns, width, label='Phase1平均リターン', alpha=0.7, edgecolor='black')
    ax2.bar([i + width/2 for i in x], phase2_returns, width, label='Phase2平均リターン', alpha=0.7, edgecolor='black')
    ax2.axhline(0, color='black', linestyle='--', linewidth=1.5)
    ax2.set_ylabel('平均リターン (%)', fontsize=10)
    ax2.set_xlabel('前日動向', fontsize=10)
    ax2.set_title('前日プラス vs マイナス - 平均リターン比較', fontsize=12, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(categories)
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()

    return fig_to_base64(fig), summary


def create_grok_rank_analysis(df):
    """Grokランク別勝率分析"""
    rank_stats = df.groupby('grok_rank').agg({
        'phase2_win': ['sum', 'count', lambda x: x.sum() / len(x) * 100],
        'phase2_return_pct': 'mean'
    }).round(2)

    rank_stats.columns = ['勝ち数', '総数', '勝率(%)', '平均リターン(%)']
    rank_stats = rank_stats[rank_stats['総数'] >= 2]  # 2件以上

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # 勝率
    x_pos = range(len(rank_stats))
    ax1.bar(x_pos, rank_stats['勝率(%)'], color='steelblue', alpha=0.7, edgecolor='black')
    ax1.axhline(50, color='red', linestyle='--', linewidth=1.5, label='50%基準線')
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels([f'Rank {i}' for i in rank_stats.index])
    ax1.set_title('Grokランク別 勝率（Phase2）', fontsize=12, fontweight='bold')
    ax1.set_xlabel('Grokランク', fontsize=10)
    ax1.set_ylabel('勝率 (%)', fontsize=10)
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')

    # 件数表示
    for i, (idx, row) in enumerate(rank_stats.iterrows()):
        ax1.text(i, row['勝率(%)'] + 2, f"n={int(row['総数'])}", ha='center', fontsize=8)

    # 平均リターン
    colors = ['green' if x > 0 else 'red' for x in rank_stats['平均リターン(%)']]
    ax2.bar(x_pos, rank_stats['平均リターン(%)'], color=colors, alpha=0.7, edgecolor='black')
    ax2.axhline(0, color='black', linestyle='--', linewidth=1.5)
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels([f'Rank {i}' for i in rank_stats.index])
    ax2.set_title('Grokランク別 平均リターン（Phase2）', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Grokランク', fontsize=10)
    ax2.set_ylabel('平均リターン (%)', fontsize=10)
    ax2.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()

    return fig_to_base64(fig), rank_stats


def create_risk_reward_analysis(df):
    """リスクリワード比の詳細分析"""

    # _pctカラムがない場合は作成
    if 'phase2_return_pct' not in df.columns:
        df['phase2_return_pct'] = df['phase2_return'] * 100

    # 勝ち/負けで分ける
    winners = df[df['phase2_win'] == True]
    losers = df[df['phase2_win'] == False]

    # 統計計算
    stats = {
        '勝ちトレード数': len(winners),
        '負けトレード数': len(losers),
        '勝率': len(winners) / len(df) * 100,
        '平均勝ちリターン': winners['phase2_return_pct'].mean() if len(winners) > 0 else 0,
        '平均負けリターン': losers['phase2_return_pct'].mean() if len(losers) > 0 else 0,
        '最大利益': df['daily_max_gain_pct'].max(),
        '最大損失': df['daily_max_drawdown_pct'].min(),
        '平均最大利益': df['daily_max_gain_pct'].mean(),
        '平均最大損失': df['daily_max_drawdown_pct'].mean(),
    }

    # リスクリワード比
    avg_win = stats['平均勝ちリターン']
    avg_loss = abs(stats['平均負けリターン'])
    stats['リスクリワード比'] = avg_win / avg_loss if avg_loss > 0 else 0

    # シャープレシオ
    returns = df['phase2_return_pct']
    stats['シャープレシオ'] = returns.mean() / returns.std() if returns.std() > 0 else 0
    stats['平均リターン'] = returns.mean()
    stats['標準偏差'] = returns.std()

    # グラフ作成
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)

    # 1. 勝ち/負けトレードの分布
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.hist([winners['phase2_return_pct'], losers['phase2_return_pct']],
             bins=20, label=['勝ちトレード', '負けトレード'], color=['green', 'red'], alpha=0.6)
    ax1.axvline(0, color='black', linestyle='--', linewidth=1.5)
    ax1.set_xlabel('リターン (%)', fontsize=10)
    ax1.set_ylabel('件数', fontsize=10)
    ax1.set_title('勝ち/負けトレードのリターン分布', fontsize=11, fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # 2. 平均リターン比較
    ax2 = fig.add_subplot(gs[0, 1])
    categories = ['平均勝ち', '平均負け']
    values = [stats['平均勝ちリターン'], stats['平均負けリターン']]
    colors_bar = ['green', 'red']
    ax2.bar(categories, values, color=colors_bar, alpha=0.7, edgecolor='black')
    ax2.axhline(0, color='black', linestyle='--', linewidth=1.5)
    ax2.set_ylabel('リターン (%)', fontsize=10)
    ax2.set_title('平均勝ち vs 平均負け', fontsize=11, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    for i, v in enumerate(values):
        ax2.text(i, v + 0.2, f'{v:.2f}%', ha='center', fontweight='bold')

    # 3. 最大利益 vs 最大損失
    ax3 = fig.add_subplot(gs[1, 0])
    categories2 = ['平均最大利益', '平均最大損失']
    values2 = [stats['平均最大利益'], stats['平均最大損失']]
    colors_bar2 = ['green', 'red']
    ax3.bar(categories2, values2, color=colors_bar2, alpha=0.7, edgecolor='black')
    ax3.axhline(0, color='black', linestyle='--', linewidth=1.5)
    ax3.set_ylabel('利益/損失 (%)', fontsize=10)
    ax3.set_title('平均最大利益 vs 平均最大損失', fontsize=11, fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')
    for i, v in enumerate(values2):
        ax3.text(i, v + 0.3, f'{v:.2f}%', ha='center', fontweight='bold')

    # 4. リスクリワード比
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.bar(['リスクリワード比'], [stats['リスクリワード比']],
            color='steelblue' if stats['リスクリワード比'] > 1 else 'coral',
            alpha=0.7, edgecolor='black', width=0.5)
    ax4.axhline(1.0, color='red', linestyle='--', linewidth=1.5, label='1.0基準線')
    ax4.set_ylabel('比率', fontsize=10)
    ax4.set_title(f"リスクリワード比: {stats['リスクリワード比']:.2f}", fontsize=11, fontweight='bold')
    ax4.legend()
    ax4.grid(True, alpha=0.3, axis='y')
    ax4.text(0, stats['リスクリワード比'] + 0.05, f"{stats['リスクリワード比']:.2f}",
             ha='center', fontweight='bold', fontsize=12)

    # 5. シャープレシオ
    ax5 = fig.add_subplot(gs[2, 0])
    ax5.bar(['シャープレシオ'], [stats['シャープレシオ']],
            color='green' if stats['シャープレシオ'] > 0 else 'red',
            alpha=0.7, edgecolor='black', width=0.5)
    ax5.axhline(0, color='black', linestyle='--', linewidth=1.5)
    ax5.set_ylabel('比率', fontsize=10)
    ax5.set_title(f"シャープレシオ: {stats['シャープレシオ']:.3f}", fontsize=11, fontweight='bold')
    ax5.grid(True, alpha=0.3, axis='y')
    ax5.text(0, stats['シャープレシオ'] + 0.01, f"{stats['シャープレシオ']:.3f}",
             ha='center', fontweight='bold', fontsize=12)

    # 6. 統計サマリー（テキスト）
    ax6 = fig.add_subplot(gs[2, 1])
    ax6.axis('off')
    summary_text = f"""
    【統計サマリー】

    勝率: {stats['勝率']:.1f}% ({stats['勝ちトレード数']}勝 / {stats['負けトレード数']}敗)

    平均リターン: {stats['平均リターン']:.2f}%
    標準偏差: {stats['標準偏差']:.2f}%

    平均勝ちリターン: {stats['平均勝ちリターン']:.2f}%
    平均負けリターン: {stats['平均負けリターン']:.2f}%

    リスクリワード比: {stats['リスクリワード比']:.2f}
    シャープレシオ: {stats['シャープレシオ']:.3f}

    最大利益: {stats['最大利益']:.2f}%
    最大損失: {stats['最大損失']:.2f}%
    """
    ax6.text(0.1, 0.5, summary_text, fontsize=11, verticalalignment='center',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

    plt.tight_layout()

    return fig_to_base64(fig), stats


def create_volatility_analysis(df):
    """ボラティリティと勝率の関係分析"""

    # _pctカラムがない場合は作成
    if 'phase2_return_pct' not in df.columns:
        df['phase2_return_pct'] = df['phase2_return'] * 100

    # ボラティリティ計算（日中最大変動幅 = max_gain - max_drawdown）
    df['daily_volatility'] = df['daily_max_gain_pct'] - df['daily_max_drawdown_pct']

    # ボラティリティで3分位に分割
    df['volatility_group'] = pd.qcut(
        df['daily_volatility'],
        q=3,
        labels=['低ボラ', '中ボラ', '高ボラ'],
        duplicates='drop'
    )

    # グループ別統計
    vol_stats = df.groupby('volatility_group').agg({
        'phase2_win': ['sum', 'count', lambda x: x.sum() / len(x) * 100],
        'phase2_return_pct': 'mean',
        'daily_volatility': 'mean',
        'daily_max_gain_pct': 'mean',
        'daily_max_drawdown_pct': 'mean'
    }).round(2)

    vol_stats.columns = ['勝ち数', '総数', '勝率(%)', '平均リターン(%)', '平均ボラ(%)', '平均最大利益(%)', '平均最大損失(%)']

    # グラフ作成
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)

    # 1. ボラティリティグループ別勝率
    ax1 = fig.add_subplot(gs[0, 0])
    x_pos = range(len(vol_stats))
    ax1.bar(x_pos, vol_stats['勝率(%)'], color='steelblue', alpha=0.7, edgecolor='black')
    ax1.axhline(50, color='red', linestyle='--', linewidth=1.5, label='50%基準線')
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(vol_stats.index)
    ax1.set_title('ボラティリティグループ別勝率', fontsize=11, fontweight='bold')
    ax1.set_ylabel('勝率 (%)', fontsize=10)
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')

    # 件数表示
    for i, (idx, row) in enumerate(vol_stats.iterrows()):
        ax1.text(i, row['勝率(%)'] + 2, f"n={int(row['総数'])}", ha='center', fontsize=8)

    # 2. ボラティリティグループ別平均リターン
    ax2 = fig.add_subplot(gs[0, 1])
    colors = ['green' if x > 0 else 'red' for x in vol_stats['平均リターン(%)']]
    ax2.bar(x_pos, vol_stats['平均リターン(%)'], color=colors, alpha=0.7, edgecolor='black')
    ax2.axhline(0, color='black', linestyle='--', linewidth=1.5)
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(vol_stats.index)
    ax2.set_title('ボラティリティグループ別平均リターン', fontsize=11, fontweight='bold')
    ax2.set_ylabel('平均リターン (%)', fontsize=10)
    ax2.grid(True, alpha=0.3, axis='y')

    # 3. ボラティリティ vs リターン散布図
    ax3 = fig.add_subplot(gs[1, 0])
    colors_scatter = ['green' if w else 'red' for w in df['phase2_win']]
    ax3.scatter(df['daily_volatility'], df['phase2_return_pct'],
                c=colors_scatter, alpha=0.5, edgecolors='black', linewidths=0.5)
    ax3.axhline(0, color='black', linestyle='--', linewidth=1)
    ax3.axvline(df['daily_volatility'].median(), color='blue', linestyle='--', linewidth=1, label='中央値')
    ax3.set_xlabel('日中ボラティリティ (%)', fontsize=10)
    ax3.set_ylabel('Phase2リターン (%)', fontsize=10)
    ax3.set_title('ボラティリティ vs リターン', fontsize=11, fontweight='bold')
    ax3.legend(['勝ち', '負け', '中央値'])
    ax3.grid(True, alpha=0.3)

    # 4. 平均ボラティリティ比較
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.bar(x_pos, vol_stats['平均ボラ(%)'], color='orange', alpha=0.7, edgecolor='black')
    ax4.set_xticks(x_pos)
    ax4.set_xticklabels(vol_stats.index)
    ax4.set_title('グループ別平均ボラティリティ', fontsize=11, fontweight='bold')
    ax4.set_ylabel('平均ボラティリティ (%)', fontsize=10)
    ax4.grid(True, alpha=0.3, axis='y')
    for i, (idx, row) in enumerate(vol_stats.iterrows()):
        ax4.text(i, row['平均ボラ(%)'] + 0.5, f"{row['平均ボラ(%)']:.1f}%", ha='center', fontweight='bold')

    # 5. 最大利益 vs 最大損失
    ax5 = fig.add_subplot(gs[2, 0])
    width = 0.35
    x_arr = np.arange(len(vol_stats))
    ax5.bar(x_arr - width/2, vol_stats['平均最大利益(%)'], width, label='平均最大利益',
            color='green', alpha=0.7, edgecolor='black')
    ax5.bar(x_arr + width/2, vol_stats['平均最大損失(%)'], width, label='平均最大損失',
            color='red', alpha=0.7, edgecolor='black')
    ax5.axhline(0, color='black', linestyle='--', linewidth=1)
    ax5.set_xticks(x_arr)
    ax5.set_xticklabels(vol_stats.index)
    ax5.set_title('グループ別最大利益/損失', fontsize=11, fontweight='bold')
    ax5.set_ylabel('利益/損失 (%)', fontsize=10)
    ax5.legend()
    ax5.grid(True, alpha=0.3, axis='y')

    # 6. 統計サマリー
    ax6 = fig.add_subplot(gs[2, 1])
    ax6.axis('off')

    # 相関係数
    corr_vol_return = df['daily_volatility'].corr(df['phase2_return_pct'])
    corr_vol_win = df['daily_volatility'].corr(df['phase2_win'].astype(int))

    summary_text = f"""
    【ボラティリティ分析サマリー】

    全体平均ボラティリティ: {df['daily_volatility'].mean():.2f}%

    低ボラグループ:
      勝率: {vol_stats.loc['低ボラ', '勝率(%)']:.1f}%
      平均リターン: {vol_stats.loc['低ボラ', '平均リターン(%)']:.2f}%

    中ボラグループ:
      勝率: {vol_stats.loc['中ボラ', '勝率(%)']:.1f}%
      平均リターン: {vol_stats.loc['中ボラ', '平均リターン(%)']:.2f}%

    高ボラグループ:
      勝率: {vol_stats.loc['高ボラ', '勝率(%)']:.1f}%
      平均リターン: {vol_stats.loc['高ボラ', '平均リターン(%)']:.2f}%

    相関係数（ボラ vs リターン): {corr_vol_return:.3f}
    相関係数（ボラ vs 勝率): {corr_vol_win:.3f}
    """
    ax6.text(0.1, 0.5, summary_text, fontsize=10, verticalalignment='center',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

    plt.tight_layout()

    return fig_to_base64(fig), vol_stats, corr_vol_return, corr_vol_win


def create_category_detail_analysis(df):
    """カテゴリー詳細分析（セクション4の深掘り）"""

    # _pctカラムがない場合は作成
    if 'phase2_return_pct' not in df.columns:
        df['phase2_return_pct'] = df['phase2_return'] * 100

    # カテゴリー別の詳細統計
    cat_stats = df.groupby('category').agg({
        'phase2_win': ['sum', 'count', lambda x: x.sum() / len(x) * 100],
        'phase2_return_pct': ['mean', 'median', 'std'],
        'daily_max_gain_pct': 'mean',
        'daily_max_drawdown_pct': 'mean'
    }).round(2)

    cat_stats.columns = ['勝ち数', '総数', '勝率(%)', '平均リターン(%)', '中央値リターン(%)',
                         '標準偏差(%)', '平均最大利益(%)', '平均最大損失(%)']
    cat_stats = cat_stats.sort_values('勝率(%)', ascending=False)

    # グラフ作成
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)

    # 1. カテゴリー別 勝率 vs 平均リターン散布図
    ax1 = fig.add_subplot(gs[0, 0])
    for idx, row in cat_stats.iterrows():
        color = 'green' if row['勝率(%)'] >= 50 and row['平均リターン(%)'] > 0 else 'red'
        ax1.scatter(row['勝率(%)'], row['平均リターン(%)'], s=row['総数']*20,
                   alpha=0.6, color=color, edgecolors='black', linewidths=1)
        ax1.text(row['勝率(%)'], row['平均リターン(%)'], idx, fontsize=8, ha='center', va='center')

    ax1.axhline(0, color='black', linestyle='--', linewidth=1)
    ax1.axvline(50, color='red', linestyle='--', linewidth=1)
    ax1.set_xlabel('勝率 (%)', fontsize=10)
    ax1.set_ylabel('平均リターン (%)', fontsize=10)
    ax1.set_title('カテゴリー別 勝率 vs 平均リターン\n(バブルサイズ=データ数)', fontsize=11, fontweight='bold')
    ax1.grid(True, alpha=0.3)

    # 2. カテゴリー別 標準偏差（リスク）
    ax2 = fig.add_subplot(gs[0, 1])
    colors2 = ['orange' if x > cat_stats['標準偏差(%)'].median() else 'steelblue'
               for x in cat_stats['標準偏差(%)']]
    ax2.barh(range(len(cat_stats)), cat_stats['標準偏差(%)'], color=colors2,
             alpha=0.7, edgecolor='black')
    ax2.set_yticks(range(len(cat_stats)))
    ax2.set_yticklabels(cat_stats.index, fontsize=9)
    ax2.set_xlabel('標準偏差 (%)', fontsize=10)
    ax2.set_title('カテゴリー別リスク（標準偏差）', fontsize=11, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='x')

    # 3. カテゴリー別 最大利益 vs 最大損失
    ax3 = fig.add_subplot(gs[1, 0])
    x_pos = range(len(cat_stats))
    width = 0.35
    ax3.bar([i - width/2 for i in x_pos], cat_stats['平均最大利益(%)'], width,
            label='平均最大利益', color='green', alpha=0.7, edgecolor='black')
    ax3.bar([i + width/2 for i in x_pos], cat_stats['平均最大損失(%)'], width,
            label='平均最大損失', color='red', alpha=0.7, edgecolor='black')
    ax3.axhline(0, color='black', linestyle='--', linewidth=1)
    ax3.set_xticks(x_pos)
    ax3.set_xticklabels(cat_stats.index, rotation=45, ha='right', fontsize=8)
    ax3.set_ylabel('利益/損失 (%)', fontsize=10)
    ax3.set_title('カテゴリー別 最大利益 vs 最大損失', fontsize=11, fontweight='bold')
    ax3.legend()
    ax3.grid(True, alpha=0.3, axis='y')

    # 4. カテゴリー別 平均 vs 中央値リターン
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.bar([i - width/2 for i in x_pos], cat_stats['平均リターン(%)'], width,
            label='平均リターン', color='steelblue', alpha=0.7, edgecolor='black')
    ax4.bar([i + width/2 for i in x_pos], cat_stats['中央値リターン(%)'], width,
            label='中央値リターン', color='coral', alpha=0.7, edgecolor='black')
    ax4.axhline(0, color='black', linestyle='--', linewidth=1)
    ax4.set_xticks(x_pos)
    ax4.set_xticklabels(cat_stats.index, rotation=45, ha='right', fontsize=8)
    ax4.set_ylabel('リターン (%)', fontsize=10)
    ax4.set_title('カテゴリー別 平均 vs 中央値リターン', fontsize=11, fontweight='bold')
    ax4.legend()
    ax4.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()

    return fig_to_base64(fig), cat_stats


def create_exit_strategy_analysis(df):
    """手仕舞い理由統計（利確 vs 損切り）"""

    # Phase別勝率
    phase_win_rates = {
        'Phase1': df['phase1_win'].sum() / len(df) * 100,
        'Phase2': df['phase2_win'].sum() / len(df) * 100,
        'Phase3-1%': df['phase3_1pct_win'].sum() / len(df) * 100,
        'Phase3-2%': df['phase3_2pct_win'].sum() / len(df) * 100,
        'Phase3-3%': df['phase3_3pct_win'].sum() / len(df) * 100,
    }

    # Phase別平均リターン
    phase_returns = {
        'Phase1': df['phase1_return_pct'].mean(),
        'Phase2': df['phase2_return_pct'].mean(),
        'Phase3-1%': df['phase3_1pct_return_pct'].mean(),
        'Phase3-2%': df['phase3_2pct_return_pct'].mean(),
        'Phase3-3%': df['phase3_3pct_return_pct'].mean(),
    }

    # Phase1 vs Phase2比較（Phase1で利確すべきか、Phase2まで待つべきか）
    phase1_winners = df[df['phase1_win'] == True]
    phase1_losers = df[df['phase1_win'] == False]

    # Phase1勝ち組がPhase2でどうなったか
    phase1_win_phase2_result = {
        'Phase1勝ち→Phase2も勝ち': ((phase1_winners['phase2_win'] == True).sum()),
        'Phase1勝ち→Phase2は負け': ((phase1_winners['phase2_win'] == False).sum()),
    }

    # Phase1負け組がPhase2でどうなったか
    phase1_lose_phase2_result = {
        'Phase1負け→Phase2で逆転': ((phase1_losers['phase2_win'] == True).sum()),
        'Phase1負け→Phase2も負け': ((phase1_losers['phase2_win'] == False).sum()),
    }

    # 損切りライン比較
    stoploss_comparison = []
    for _, row in df.iterrows():
        phase2_return = row['phase2_return_pct']
        phase3_1 = row['phase3_1pct_return_pct']
        phase3_2 = row['phase3_2pct_return_pct']
        phase3_3 = row['phase3_3pct_return_pct']

        best_phase = max([
            ('Phase2', phase2_return),
            ('Phase3-1%', phase3_1),
            ('Phase3-2%', phase3_2),
            ('Phase3-3%', phase3_3)
        ], key=lambda x: x[1])

        stoploss_comparison.append(best_phase[0])

    stoploss_counts = pd.Series(stoploss_comparison).value_counts()

    # グラフ作成
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)

    # 1. Phase別勝率比較（既存と同じだが再掲）
    ax1 = fig.add_subplot(gs[0, 0])
    phases = list(phase_win_rates.keys())
    win_rates_vals = list(phase_win_rates.values())
    colors1 = ['green' if x >= 50 else 'red' for x in win_rates_vals]
    ax1.bar(phases, win_rates_vals, color=colors1, alpha=0.7, edgecolor='black')
    ax1.axhline(50, color='black', linestyle='--', linewidth=1.5, label='50%基準線')
    ax1.set_ylabel('勝率 (%)', fontsize=10)
    ax1.set_title('Phase別勝率比較', fontsize=11, fontweight='bold')
    ax1.set_xticklabels(phases, rotation=45, ha='right')
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')
    for i, v in enumerate(win_rates_vals):
        ax1.text(i, v + 1, f'{v:.1f}%', ha='center', fontweight='bold', fontsize=9)

    # 2. Phase1勝ち組 → Phase2の結果
    ax2 = fig.add_subplot(gs[0, 1])
    labels2 = list(phase1_win_phase2_result.keys())
    values2 = list(phase1_win_phase2_result.values())
    colors2 = ['green', 'orange']
    wedges, texts, autotexts = ax2.pie(values2, labels=labels2, autopct='%1.1f%%',
                                        colors=colors2, startangle=90)
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
    ax2.set_title(f'Phase1勝ち組 → Phase2結果\n(n={len(phase1_winners)})', fontsize=11, fontweight='bold')

    # 3. Phase1負け組 → Phase2の結果
    ax3 = fig.add_subplot(gs[1, 0])
    labels3 = list(phase1_lose_phase2_result.keys())
    values3 = list(phase1_lose_phase2_result.values())
    colors3 = ['lightgreen', 'red']
    wedges, texts, autotexts = ax3.pie(values3, labels=labels3, autopct='%1.1f%%',
                                        colors=colors3, startangle=90)
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
    ax3.set_title(f'Phase1負け組 → Phase2結果\n(n={len(phase1_losers)})', fontsize=11, fontweight='bold')

    # 4. 最適手仕舞いPhase分布
    ax4 = fig.add_subplot(gs[1, 1])
    stoploss_phases = list(stoploss_counts.index)
    stoploss_vals = list(stoploss_counts.values)
    colors4 = ['steelblue' if 'Phase2' in p else 'coral' for p in stoploss_phases]
    ax4.bar(stoploss_phases, stoploss_vals, color=colors4, alpha=0.7, edgecolor='black')
    ax4.set_ylabel('銘柄数', fontsize=10)
    ax4.set_title('最適手仕舞いPhase分布\n(最もリターンが高かったPhase)', fontsize=11, fontweight='bold')
    ax4.set_xticklabels(stoploss_phases, rotation=45, ha='right')
    ax4.grid(True, alpha=0.3, axis='y')
    for i, v in enumerate(stoploss_vals):
        ax4.text(i, v + 0.5, f'{v}', ha='center', fontweight='bold', fontsize=9)

    plt.tight_layout()

    # 統計サマリー
    exit_stats = {
        'Phase1勝ち組がPhase2も勝つ確率': phase1_win_phase2_result['Phase1勝ち→Phase2も勝ち'] / len(phase1_winners) * 100 if len(phase1_winners) > 0 else 0,
        'Phase1負け組がPhase2で逆転する確率': phase1_lose_phase2_result['Phase1負け→Phase2で逆転'] / len(phase1_losers) * 100 if len(phase1_losers) > 0 else 0,
        '最適Phase': stoploss_counts.index[0],
        '最適Phase選択率': stoploss_counts.values[0] / len(df) * 100,
    }

    return fig_to_base64(fig), phase_win_rates, exit_stats


def generate_html_report():
    """HTMLレポート生成"""

    # データ読み込み
    df = pd.read_parquet(DATA_PATH)
    df['backtest_date'] = pd.to_datetime(df['backtest_date'])
    df['selection_date'] = pd.to_datetime(df['selection_date'])

    # 基本統計
    phases = {
        'Phase1 (前場終了時)': ('phase1_return_pct', 'phase1_win'),
        'Phase2 (当日終値)': ('phase2_return_pct', 'phase2_win'),
        'Phase3-1% (損切-1%)': ('phase3_1pct_return_pct', 'phase3_1pct_win'),
        'Phase3-2% (損切-2%)': ('phase3_2pct_return_pct', 'phase3_2pct_win'),
        'Phase3-3% (損切-3%)': ('phase3_3pct_return_pct', 'phase3_3pct_win'),
    }

    summary_rows = []
    for phase_name, (return_col, win_col) in phases.items():
        win_rate = df[win_col].sum() / len(df) * 100
        avg_return = df[return_col].mean()
        median_return = df[return_col].median()

        summary_rows.append(f"""
            <tr>
                <td>{phase_name}</td>
                <td class="num">{win_rate:.1f}%</td>
                <td class="num">{avg_return:.2f}%</td>
                <td class="num">{median_return:.2f}%</td>
                <td class="num">{df[win_col].sum()}</td>
                <td class="num">{len(df) - df[win_col].sum()}</td>
            </tr>
        """)

    # グラフ生成
    chart1 = create_phase_comparison_chart(df)
    chart2 = create_return_distribution(df)
    chart3 = create_category_analysis(df)
    chart4 = create_daily_analysis(df)
    chart5 = create_previous_day_analysis(df)
    chart6 = create_volume_ratio_analysis(df)
    chart7, plus_minus_summary = create_plus_minus_comparison(df)
    chart8, rank_stats = create_grok_rank_analysis(df)
    chart9, risk_stats = create_risk_reward_analysis(df)
    chart10, vol_stats, corr_vol_return, corr_vol_win = create_volatility_analysis(df)
    chart11, cat_detail_stats = create_category_detail_analysis(df)
    chart12, phase_win_rates, exit_stats = create_exit_strategy_analysis(df)

    # 全銘柄（phase2リターン降順）
    all_stocks = df.sort_values('phase2_return_pct', ascending=False)[
        ['ticker', 'company_name', 'category', 'backtest_date', 'phase1_return_pct', 'phase2_return_pct',
         'morning_volume', 'prev_day_change_pct', 'prev_day_volume_ratio']
    ]
    stock_rows = []
    for _, row in all_stocks.iterrows():
        prev_change = f"{row['prev_day_change_pct']:.2f}%" if not pd.isna(row['prev_day_change_pct']) else "N/A"
        volume_ratio = f"{row['prev_day_volume_ratio']:.2f}" if not pd.isna(row['prev_day_volume_ratio']) else "N/A"

        stock_rows.append(f"""
            <tr>
                <td>{row['ticker']}</td>
                <td>{row['company_name']}</td>
                <td>{row['category']}</td>
                <td class="num">{row['backtest_date'].strftime('%Y-%m-%d')}</td>
                <td class="num">{row['phase1_return_pct']:.2f}%</td>
                <td class="num">{row['phase2_return_pct']:.2f}%</td>
                <td class="num">{row['morning_volume']:,.0f}</td>
                <td class="num">{prev_change}</td>
                <td class="num">{volume_ratio}</td>
            </tr>
        """)

    # 前日プラス vs マイナスの表を生成
    plus_minus_rows = []
    if plus_minus_summary is not None:
        for direction, row in plus_minus_summary.iterrows():
            plus_minus_rows.append(f"""
                <tr>
                    <td>{direction}</td>
                    <td class="num">{int(row['銘柄数'])}</td>
                    <td class="num">{row['Phase1勝率(%)']:.1f}%</td>
                    <td class="num">{row['Phase2勝率(%)']:.1f}%</td>
                    <td class="num">{row['Phase1平均リターン(%)']:.2f}%</td>
                    <td class="num">{row['Phase2平均リターン(%)']:.2f}%</td>
                </tr>
            """)

    # HTML生成
    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Grok銘柄分析レポート</title>
        <style>
            body {{
                font-family: 'Hiragino Sans', 'Hiragino Kaku Gothic ProN', 'YuGothic', sans-serif;
                max-width: 1400px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f5f5f5;
            }}
            h1 {{
                color: #333;
                border-bottom: 3px solid #4CAF50;
                padding-bottom: 10px;
            }}
            h2 {{
                color: #555;
                margin-top: 40px;
                border-left: 5px solid #2196F3;
                padding-left: 10px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                background-color: white;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            th, td {{
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }}
            td.num {{
                text-align: right;
            }}
            th {{
                background-color: #4CAF50;
                color: white;
                font-weight: bold;
            }}
            tr:hover {{
                background-color: #f5f5f5;
            }}
            .chart {{
                margin: 30px 0;
                background-color: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .chart img {{
                width: 100%;
                height: auto;
            }}
            .info-box {{
                background-color: #E3F2FD;
                padding: 15px;
                border-left: 4px solid #2196F3;
                margin: 20px 0;
            }}
        </style>
    </head>
    <body>
        <h1>Grok銘柄分析レポート</h1>

        <div class="info-box">
            <strong>データ期間:</strong> {df['backtest_date'].min().date()} ~ {df['backtest_date'].max().date()}<br>
            <strong>分析銘柄数:</strong> {len(df)}銘柄<br>
            <strong>ユニーク銘柄数:</strong> {df['ticker'].nunique()}銘柄
        </div>

        <h2>1. 基本統計サマリー</h2>
        <table>
            <thead>
                <tr>
                    <th>Phase</th>
                    <th>勝率</th>
                    <th>平均リターン</th>
                    <th>中央値</th>
                    <th>勝ち数</th>
                    <th>負け数</th>
                </tr>
            </thead>
            <tbody>
                {''.join(summary_rows)}
            </tbody>
        </table>

        <h2>2. Phase別勝率比較</h2>
        <div class="chart">
            <img src="{chart1}" alt="Phase別勝率比較">
        </div>

        <h2>3. リターン分布</h2>
        <div class="chart">
            <img src="{chart2}" alt="リターン分布">
        </div>

        <h2>4. カテゴリー別分析</h2>
        <div class="chart">
            <img src="{chart3}" alt="カテゴリー別分析">
        </div>

        <h2>5. 日別平均リターン推移</h2>
        <div class="chart">
            <img src="{chart4}" alt="日別分析">
        </div>

        <h2>6. 前日変化率による分析</h2>
        <div class="chart">
            <img src="{chart5}" alt="前日変化率分析">
        </div>

        <h2>7. 前日出来高比による分析</h2>
        <div class="chart">
            <img src="{chart6}" alt="前日出来高比分析">
        </div>

        <h2>8. 前日プラス vs マイナス銘柄の比較</h2>
        <div class="info-box" style="background-color: #FFF3E0; border-left: 4px solid #FF9800;">
            <strong>結論：</strong> 前日マイナス銘柄の方がパフォーマンスが良い（Phase2勝率: 28.0% vs 23.8%、平均リターン: -1.65% vs -2.38%）<br>
            <strong>解釈：</strong> 前日下落した銘柄はリバウンド効果や割安感から買いが入りやすく、前日上昇した銘柄は利益確定売りに押される傾向
        </div>

        <table>
            <thead>
                <tr>
                    <th>前日動向</th>
                    <th>銘柄数</th>
                    <th>Phase1勝率</th>
                    <th>Phase2勝率</th>
                    <th>Phase1平均リターン</th>
                    <th>Phase2平均リターン</th>
                </tr>
            </thead>
            <tbody>
                {''.join(plus_minus_rows)}
            </tbody>
        </table>

        <div class="chart">
            <img src="{chart7}" alt="前日プラスvsマイナス比較">
        </div>

        <h2>8. Grokランク別分析 ⭐ NEW</h2>
        <p><strong>重要な発見:</strong> Grokランク1位が必ずしも勝率が高いわけではありません。ランク別の傾向を確認してください。</p>

        <div class="chart">
            <img src="{chart8}" alt="Grokランク別勝率分析">
        </div>

        <table>
            <thead>
                <tr>
                    <th>Grokランク</th>
                    <th>データ数</th>
                    <th>勝率</th>
                    <th>平均リターン</th>
                </tr>
            </thead>
            <tbody>
                {''.join([f'''
                <tr style="background-color: {'#E8F5E9' if row['勝率(%)'] >= 60 else '#FFEBEE' if row['勝率(%)'] <= 40 else 'white'};">
                    <td>Rank {idx}</td>
                    <td class="num">{int(row['総数'])}</td>
                    <td class="num" style="font-weight: bold;">{row['勝率(%)']:.1f}%</td>
                    <td class="num" style="color: {'green' if row['平均リターン(%)'] > 0 else 'red'};">{row['平均リターン(%)']:.2f}%</td>
                </tr>
                ''' for idx, row in rank_stats.iterrows()])}
            </tbody>
        </table>

        <h2>9. リスクリワード分析 ⭐ NEW</h2>
        <p><strong>リスクリワード比:</strong> 平均勝ちトレード / 平均負けトレード = {risk_stats['リスクリワード比']:.2f}</p>
        <p><strong>シャープレシオ:</strong> 平均リターン / 標準偏差 = {risk_stats['シャープレシオ']:.3f} (0以上が望ましい)</p>

        <div class="chart">
            <img src="{chart9}" alt="リスクリワード分析">
        </div>

        <div style="background-color: #E3F2FD; border-left: 4px solid #2196F3; padding: 15px; margin: 20px 0;">
            <h3>解釈:</h3>
            <ul>
                <li><strong>リスクリワード比 {risk_stats['リスクリワード比']:.2f}:</strong> {'1.0以上で良好。勝ちトレードの平均利益が負けトレードの平均損失を上回っています。' if risk_stats['リスクリワード比'] >= 1 else '1.0未満。勝ちトレードの平均利益が負けトレードの平均損失を下回っています。'}</li>
                <li><strong>シャープレシオ {risk_stats['シャープレシオ']:.3f}:</strong> {'正の値でリターンに対してリスクが許容範囲。' if risk_stats['シャープレシオ'] > 0 else '負の値。リターンに対してリスクが大きすぎます。'}</li>
                <li><strong>勝率 {risk_stats['勝率']:.1f}%:</strong> 勝率が50%未満のため、リスクリワード比1.5以上が理想的です。</li>
            </ul>
        </div>

        <h2>10. ボラティリティと勝率の関係 ⭐ NEW</h2>
        <p><strong>目的:</strong> 値動きの激しい銘柄（高ボラティリティ）は勝ちやすいのか？損切りライン設定に役立つか？</p>

        <div class="chart">
            <img src="{chart10}" alt="ボラティリティ分析">
        </div>

        <table>
            <thead>
                <tr>
                    <th>ボラティリティ</th>
                    <th>データ数</th>
                    <th>勝率</th>
                    <th>平均リターン</th>
                    <th>平均ボラ</th>
                    <th>平均最大利益</th>
                    <th>平均最大損失</th>
                </tr>
            </thead>
            <tbody>
                {''.join([f'''
                <tr style="background-color: {'#E8F5E9' if row['勝率(%)'] >= 50 else '#FFEBEE' if row['勝率(%)'] <= 35 else 'white'};">
                    <td>{idx}</td>
                    <td class="num">{int(row['総数'])}</td>
                    <td class="num" style="font-weight: bold;">{row['勝率(%)']:.1f}%</td>
                    <td class="num" style="color: {'green' if row['平均リターン(%)'] > 0 else 'red'};">{row['平均リターン(%)']:.2f}%</td>
                    <td class="num">{row['平均ボラ(%)']:.2f}%</td>
                    <td class="num" style="color: green;">{row['平均最大利益(%)']:.2f}%</td>
                    <td class="num" style="color: red;">{row['平均最大損失(%)']:.2f}%</td>
                </tr>
                ''' for idx, row in vol_stats.iterrows()])}
            </tbody>
        </table>

        <div style="background-color: #FFF3E0; border-left: 4px solid #FF9800; padding: 15px; margin: 20px 0;">
            <h3>解釈:</h3>
            <ul>
                <li><strong>相関係数（ボラ vs リターン）{corr_vol_return:.3f}:</strong> {'正の相関。ボラティリティが高いほどリターンも高い傾向。' if corr_vol_return > 0.2 else '負の相関。ボラティリティが高いほどリターンが低い傾向。' if corr_vol_return < -0.2 else 'ほぼ相関なし。ボラティリティとリターンに明確な関係はありません。'}</li>
                <li><strong>相関係数（ボラ vs 勝率）{corr_vol_win:.3f}:</strong> {'正の相関。ボラティリティが高いほど勝率も高い傾向。' if corr_vol_win > 0.2 else '負の相関。ボラティリティが高いほど勝率が低い傾向。' if corr_vol_win < -0.2 else 'ほぼ相関なし。ボラティリティと勝率に明確な関係はありません。'}</li>
                <li><strong>実践的示唆:</strong> ボラティリティグループ別の平均最大損失を見て、適切な損切りライン（-2% vs -3%など）を設定すると良いでしょう。</li>
            </ul>
        </div>

        <h2>11. カテゴリー詳細分析 ⭐ NEW</h2>
        <p><strong>目的:</strong> セクション4のカテゴリー分析をさらに深掘り。各カテゴリーのリスク・リターン特性を詳細に分析</p>

        <div class="chart">
            <img src="{chart11}" alt="カテゴリー詳細分析">
        </div>

        <table>
            <thead>
                <tr>
                    <th>カテゴリー</th>
                    <th>データ数</th>
                    <th>勝率</th>
                    <th>平均リターン</th>
                    <th>中央値リターン</th>
                    <th>標準偏差</th>
                    <th>平均最大利益</th>
                    <th>平均最大損失</th>
                </tr>
            </thead>
            <tbody>
                {''.join([f'''
                <tr style="background-color: {'#E8F5E9' if row['勝率(%)'] >= 50 and row['平均リターン(%)'] > 0 else '#FFEBEE' if row['勝率(%)'] < 40 else 'white'};">
                    <td>{idx}</td>
                    <td class="num">{int(row['総数'])}</td>
                    <td class="num" style="font-weight: bold;">{row['勝率(%)']:.1f}%</td>
                    <td class="num" style="color: {'green' if row['平均リターン(%)'] > 0 else 'red'};">{row['平均リターン(%)']:.2f}%</td>
                    <td class="num">{row['中央値リターン(%)']:.2f}%</td>
                    <td class="num">{row['標準偏差(%)']:.2f}%</td>
                    <td class="num" style="color: green;">{row['平均最大利益(%)']:.2f}%</td>
                    <td class="num" style="color: red;">{row['平均最大損失(%)']:.2f}%</td>
                </tr>
                ''' for idx, row in cat_detail_stats.iterrows()])}
            </tbody>
        </table>

        <div style="background-color: #E3F2FD; border-left: 4px solid #2196F3; padding: 15px; margin: 20px 0;">
            <h3>解釈:</h3>
            <ul>
                <li><strong>勝率 vs 平均リターン散布図:</strong> 右上のカテゴリーが理想的（高勝率 × 高リターン）</li>
                <li><strong>標準偏差（リスク）:</strong> 高いほどリターンのブレが大きい。安定志向なら低標準偏差のカテゴリーを選ぶ</li>
                <li><strong>平均 vs 中央値リターン:</strong> 大きく乖離している場合、外れ値（大勝ち/大負け）に注意</li>
                <li><strong>最大利益/損失:</strong> カテゴリーごとの損切りラインの目安になる</li>
            </ul>
        </div>

        <h2>12. 手仕舞い戦略分析 ⭐ NEW</h2>
        <p><strong>目的:</strong> Phase1で利確すべきか、Phase2まで待つべきか？損切りラインはどこが最適か？</p>

        <div class="chart">
            <img src="{chart12}" alt="手仕舞い戦略分析">
        </div>

        <div style="background-color: #E3F2FD; border-left: 4px solid #2196F3; padding: 15px; margin: 20px 0;">
            <h3>重要な発見:</h3>
            <ul>
                <li><strong>Phase1勝ち組がPhase2も勝つ確率:</strong> {exit_stats['Phase1勝ち組がPhase2も勝つ確率']:.1f}%</li>
                <li><strong>Phase1負け組がPhase2で逆転する確率:</strong> {exit_stats['Phase1負け組がPhase2で逆転する確率']:.1f}%</li>
                <li><strong>最適手仕舞いPhase:</strong> {exit_stats['最適Phase']} ({exit_stats['最適Phase選択率']:.1f}%の銘柄で最高リターン)</li>
            </ul>
        </div>

        <div style="background-color: #FFF3E0; border-left: 4px solid #FF9800; padding: 15px; margin: 20px 0;">
            <h3>実践的な戦略:</h3>
            <ol>
                <li><strong>Phase1で勝っている場合:</strong>
                    <ul>
                        <li>Phase2まで保有すると{exit_stats['Phase1勝ち組がPhase2も勝つ確率']:.1f}%の確率で勝ち続ける</li>
                        <li>{'Phase1で利確した方が安全' if exit_stats['Phase1勝ち組がPhase2も勝つ確率'] < 70 else 'Phase2まで保有を推奨'}</li>
                    </ul>
                </li>
                <li><strong>Phase1で負けている場合:</strong>
                    <ul>
                        <li>Phase2で逆転する確率は{exit_stats['Phase1負け組がPhase2で逆転する確率']:.1f}%</li>
                        <li>{'損切りを検討' if exit_stats['Phase1負け組がPhase2で逆転する確率'] < 30 else '逆転の可能性あり、様子見も検討'}</li>
                    </ul>
                </li>
                <li><strong>損切りライン設定:</strong>
                    <ul>
                        <li>最も多くの銘柄で最高リターンを記録したのは「{exit_stats['最適Phase']}」</li>
                        <li>このPhaseの設定を基本戦略として採用することを推奨</li>
                    </ul>
                </li>
            </ol>
        </div>

        <h2>13. 全銘柄一覧（Phase2リターン順）</h2>
        <table>
            <thead>
                <tr>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th>カテゴリー</th>
                    <th>日付</th>
                    <th>Phase1リターン</th>
                    <th>Phase2リターン</th>
                    <th>前場出来高</th>
                    <th>前日変化率</th>
                    <th>前日出来高比</th>
                </tr>
            </thead>
            <tbody>
                {''.join(stock_rows)}
            </tbody>
        </table>

        <footer style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ddd; text-align: center; color: #777;">
            <p>生成日時: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </footer>
    </body>
    </html>
    """

    # 保存
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"HTMLレポートを生成しました: {OUTPUT_PATH}")
    print(f"ファイルサイズ: {OUTPUT_PATH.stat().st_size / 1024:.1f} KB")

    # PDF生成
    try:
        from weasyprint import HTML
        pdf_path = OUTPUT_PATH.with_suffix('.pdf')
        HTML(string=html_content).write_pdf(pdf_path)
        print(f"PDFレポートを生成しました: {pdf_path}")
        print(f"ファイルサイズ: {pdf_path.stat().st_size / 1024:.1f} KB")
    except Exception as e:
        print(f"PDF生成に失敗しました: {e}")


if __name__ == '__main__':
    generate_html_report()
