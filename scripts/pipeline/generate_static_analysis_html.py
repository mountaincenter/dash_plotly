#!/usr/bin/env python3
"""
generate_static_analysis_html.py
Static銘柄バックテスト結果の分析HTMLを生成

実行方法:
    python scripts/pipeline/generate_static_analysis_html.py

出力:
    improvement/data/static_v2_final.html
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR))

import pandas as pd
import numpy as np

# パス設定
BACKTEST_FILE = BASE_DIR / "data" / "parquet" / "backtest" / "static_backtest.parquet"
OUTPUT_FILE = BASE_DIR / "improvement" / "data" / "static_v2_final.html"


def generate_html(df: pd.DataFrame) -> str:
    """分析HTMLを生成"""
    sb = df[df['signal'] == 'STRONG_BUY'].copy()

    # 1日 vs 5日の統計
    stats_1d = {
        'count': len(sb[sb['return_1d'].notna()]),
        'win_rate': (sb['return_1d'] > 0).mean() * 100 if 'return_1d' in sb.columns else 0,
        'avg_return': sb['return_1d'].mean() if 'return_1d' in sb.columns else 0,
        'total_profit': sb['profit_100_1d'].sum() if 'profit_100_1d' in sb.columns else 0,
    }
    stats_5d = {
        'count': len(sb[sb['return_5d'].notna()]),
        'win_rate': (sb['return_5d'] > 0).mean() * 100 if 'return_5d' in sb.columns else 0,
        'avg_return': sb['return_5d'].mean() if 'return_5d' in sb.columns else 0,
        'total_profit': sb['profit_100_5d'].sum() if 'profit_100_5d' in sb.columns else 0,
    }

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Static v2 Analysis - Final</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0a0a0a; color: #e0e0e0; padding: 20px; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ color: #4ade80; margin-bottom: 10px; }}
        h2 {{ color: #60a5fa; margin: 30px 0 15px; border-bottom: 1px solid #333; padding-bottom: 10px; }}
        .subtitle {{ color: #888; margin-bottom: 30px; }}
        .card-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 20px; margin: 20px 0; }}
        .card {{ background: #1a1a1a; border-radius: 12px; padding: 20px; border: 1px solid #333; }}
        .card h3 {{ color: #888; font-size: 14px; margin-bottom: 8px; }}
        .card .value {{ font-size: 32px; font-weight: bold; }}
        .card .value.green {{ color: #4ade80; }}
        .card .value.blue {{ color: #60a5fa; }}
        .card .value.yellow {{ color: #fbbf24; }}
        .card .detail {{ color: #888; font-size: 14px; margin-top: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; background: #1a1a1a; border-radius: 8px; overflow: hidden; }}
        th {{ background: #252525; color: #888; font-weight: 500; text-align: left; padding: 12px; font-size: 13px; }}
        td {{ padding: 12px; border-top: 1px solid #2a2a2a; font-size: 14px; }}
        tr:hover {{ background: #252525; }}
        .positive {{ color: #4ade80; }}
        .negative {{ color: #f87171; }}
        .comparison-box {{ background: linear-gradient(135deg, #1a2e1a 0%, #1a1a2e 100%); border: 1px solid #4ade80; border-radius: 12px; padding: 25px; margin: 20px 0; }}
        .comparison-grid {{ display: grid; grid-template-columns: 1fr auto 1fr; gap: 20px; align-items: center; }}
        .comparison-item {{ text-align: center; }}
        .comparison-item h4 {{ color: #888; margin-bottom: 10px; }}
        .comparison-item .big {{ font-size: 36px; font-weight: bold; }}
        .comparison-vs {{ font-size: 24px; color: #666; }}
        .winner {{ background: rgba(74, 222, 128, 0.1); border-radius: 8px; padding: 15px; }}
        .rules-box {{ background: #1a2e1a; border: 1px solid #4ade80; border-radius: 8px; padding: 20px; margin: 20px 0; }}
        .rules-box h3 {{ color: #4ade80; margin-bottom: 15px; }}
        .rules-box ul {{ list-style: none; }}
        .rules-box li {{ padding: 5px 0; color: #a0a0a0; }}
        .rules-box li::before {{ content: "✓ "; color: #4ade80; }}
        .exclude-box {{ background: #2e1a1a; border: 1px solid #f87171; border-radius: 8px; padding: 20px; margin: 20px 0; }}
        .exclude-box h3 {{ color: #f87171; margin-bottom: 15px; }}
        .exclude-box li::before {{ content: "✗ "; color: #f87171; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Static銘柄 v2 Analysis</h1>
        <p class="subtitle">最終ルール適用版 | 生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>

        <h2>1日保有 vs 5日保有 比較</h2>
        <div class="comparison-box">
            <div class="comparison-grid">
                <div class="comparison-item">
                    <h4>1日保有</h4>
                    <div class="big {'green' if stats_1d['avg_return'] > 0 else 'negative'}">{stats_1d['avg_return']:+.2f}%</div>
                    <div style="color:#888; margin-top:10px;">勝率 {stats_1d['win_rate']:.1f}%</div>
                    <div style="color:#888;">利益 ¥{stats_1d['total_profit']:,.0f}</div>
                </div>
                <div class="comparison-vs">vs</div>
                <div class="comparison-item {'winner' if stats_5d['total_profit'] > stats_1d['total_profit'] else ''}">
                    <h4>5日保有 {'👑' if stats_5d['total_profit'] > stats_1d['total_profit'] else ''}</h4>
                    <div class="big {'green' if stats_5d['avg_return'] > 0 else 'negative'}">{stats_5d['avg_return']:+.2f}%</div>
                    <div style="color:#888; margin-top:10px;">勝率 {stats_5d['win_rate']:.1f}%</div>
                    <div style="color:#888;">利益 ¥{stats_5d['total_profit']:,.0f}</div>
                </div>
            </div>
            <div style="text-align:center; margin-top:20px; padding-top:15px; border-top:1px solid #333;">
                <span style="color:#888;">5日保有の優位性:</span>
                <span class="{'positive' if stats_5d['avg_return'] - stats_1d['avg_return'] > 0 else 'negative'}" style="font-size:20px; margin-left:10px;">
                    {stats_5d['avg_return'] - stats_1d['avg_return']:+.2f}%
                </span>
                <span style="color:#888; margin-left:20px;">利益差:</span>
                <span class="{'positive' if stats_5d['total_profit'] - stats_1d['total_profit'] > 0 else 'negative'}" style="font-size:20px; margin-left:10px;">
                    ¥{stats_5d['total_profit'] - stats_1d['total_profit']:+,.0f}
                </span>
            </div>
        </div>

        <div class="rules-box">
            <h3>適用ルール</h3>
            <ul>
                <li>Score ≥ 50 (STRONG_BUY)</li>
                <li>RSI ≥ 12 (落ちるナイフ回避)</li>
                <li>株価 < 20,000円 (信用余力効率化)</li>
                <li>前回-7%以下の銘柄はスキップ</li>
            </ul>
        </div>

        <div class="exclude-box">
            <h3>除外対象</h3>
            <ul>
                <li>電気・ガス業セクター (東京電力、関西電力、電源開発)</li>
                <li>7012.T 川崎重工</li>
                <li>6367.T ダイキン</li>
                <li>6723.T ルネサス</li>
            </ul>
        </div>

        <h2>パフォーマンス詳細</h2>
        <table>
            <tr>
                <th>保有日数</th>
                <th>件数</th>
                <th>勝率</th>
                <th>平均リターン</th>
                <th>合計利益</th>
                <th>1シグナルあたり</th>
            </tr>
"""

    # 保有日数別テーブル
    for days, col_ret, col_profit, label in [
        (1, 'return_1d', 'profit_100_1d', '1日'),
        (5, 'return_5d', 'profit_100_5d', '5日'),
    ]:
        if col_ret in sb.columns:
            valid = sb[sb[col_ret].notna()]
            if len(valid) > 0:
                win_rate = (valid[col_ret] > 0).mean() * 100
                avg_ret = valid[col_ret].mean()
                total = valid[col_profit].sum()
                per_sig = total / len(valid)
                html += f"""
            <tr>
                <td>{label}</td>
                <td>{len(valid)}</td>
                <td>{win_rate:.1f}%</td>
                <td class="{'positive' if avg_ret > 0 else 'negative'}">{avg_ret:+.2f}%</td>
                <td class="{'positive' if total > 0 else 'negative'}">¥{total:,.0f}</td>
                <td>¥{per_sig:,.0f}</td>
            </tr>
"""

    html += """
        </table>

        <h2>銘柄別パフォーマンス (5日保有)</h2>
        <table>
            <tr>
                <th>銘柄</th>
                <th>件数</th>
                <th>勝率</th>
                <th>平均リターン</th>
                <th>合計利益</th>
            </tr>
"""

    # 銘柄別集計
    if 'return_5d' in sb.columns:
        ticker_stats = sb.groupby(['ticker', 'stock_name']).agg({
            'return_5d': ['count', 'mean', lambda x: (x > 0).mean()],
            'profit_100_5d': 'sum'
        }).round(2)
        ticker_stats.columns = ['件数', '平均リターン', '勝率', '合計利益']
        ticker_stats = ticker_stats.sort_values('合計利益', ascending=False)

        for idx, row in ticker_stats.iterrows():
            ticker, name = idx
            html += f"""
            <tr>
                <td>{ticker} {name}</td>
                <td>{row['件数']:.0f}</td>
                <td>{row['勝率']*100:.0f}%</td>
                <td class="{'positive' if row['平均リターン'] > 0 else 'negative'}">{row['平均リターン']:+.1f}%</td>
                <td class="{'positive' if row['合計利益'] > 0 else 'negative'}">¥{row['合計利益']:+,.0f}</td>
            </tr>
"""

    html += """
        </table>

        <h2>直近シグナル一覧</h2>
        <table>
            <tr>
                <th>日付</th>
                <th>銘柄</th>
                <th>スコア</th>
                <th>1日リターン</th>
                <th>5日リターン</th>
                <th>5日損益</th>
            </tr>
"""

    # 直近シグナル
    recent = sb.sort_values('signal_date', ascending=False).head(30)
    for _, row in recent.iterrows():
        ret_1d = row.get('return_1d', np.nan)
        ret_5d = row.get('return_5d', np.nan)
        profit = row.get('profit_100_5d', np.nan)

        ret_1d_str = f"{ret_1d:+.1f}%" if pd.notna(ret_1d) else "-"
        ret_5d_str = f"{ret_5d:+.1f}%" if pd.notna(ret_5d) else "-"
        profit_str = f"¥{profit:+,.0f}" if pd.notna(profit) else "-"

        ret_1d_class = 'positive' if pd.notna(ret_1d) and ret_1d > 0 else 'negative' if pd.notna(ret_1d) else ''
        ret_5d_class = 'positive' if pd.notna(ret_5d) and ret_5d > 0 else 'negative' if pd.notna(ret_5d) else ''
        profit_class = 'positive' if pd.notna(profit) and profit > 0 else 'negative' if pd.notna(profit) else ''

        html += f"""
            <tr>
                <td>{row['signal_date']}</td>
                <td>{row['ticker']} {row['stock_name']}</td>
                <td>{row['score']}</td>
                <td class="{ret_1d_class}">{ret_1d_str}</td>
                <td class="{ret_5d_class}">{ret_5d_str}</td>
                <td class="{profit_class}">{profit_str}</td>
            </tr>
"""

    html += """
        </table>

        <h2>最適戦略</h2>
        <div class="rules-box">
            <h3>推奨トレード</h3>
            <ul>
                <li><strong>エントリー:</strong> STRONG_BUYシグナル翌日の寄付成行買い</li>
                <li><strong>エグジット:</strong> 5営業日後の大引け成行売り</li>
                <li><strong>損切り:</strong> なし（5日間ホールド）</li>
                <li><strong>利確:</strong> なし（5日間ホールド）</li>
            </ul>
        </div>
    </div>
</body>
</html>
"""
    return html


def main():
    print("=" * 60)
    print("Static Analysis HTML Generator")
    print("=" * 60)

    if not BACKTEST_FILE.exists():
        print(f"Error: Backtest file not found: {BACKTEST_FILE}")
        return

    df = pd.read_parquet(BACKTEST_FILE)
    print(f"Loaded {len(df)} records")

    html = generate_html(df)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(html)
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
