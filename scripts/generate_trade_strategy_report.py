"""
信用売買戦略レポート生成

勝率0%/100%のパターンを特定し、信用売買の戦略を提示する
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path
import base64
from io import BytesIO

# 日本語フォント設定
import japanize_matplotlib

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_PATH = BASE_DIR / 'test_output' / 'test_grok_analysis_base_20251107_v3.parquet'
OUTPUT_PATH = BASE_DIR / 'test_output' / 'trade_strategy_report.html'


def fig_to_base64(fig):
    """matplotlibのfigureをbase64エンコードされた画像に変換"""
    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    img_base64 = base64.b64encode(buf.read()).decode('utf-8')
    plt.close(fig)
    return f'data:image/png;base64,{img_base64}'


def generate_report():
    """信用売買戦略レポート生成"""

    # データ読み込み
    df = pd.read_parquet(DATA_PATH)
    df['backtest_date'] = pd.to_datetime(df['backtest_date'])
    df['prev_direction'] = df['prev_day_change_pct'].apply(lambda x: 'プラス' if x >= 0 else 'マイナス')

    # 勝率0%のパターン抽出
    combo_stats = df.groupby(['categories', 'prev_direction']).agg({
        'phase2_win': ['sum', 'count', lambda x: x.sum() / len(x) * 100],
        'phase2_return_pct': 'mean'
    }).round(2)
    combo_stats.columns = ['勝ち数', '総数', '勝率(%)', '平均リターン(%)']
    combo_stats = combo_stats[combo_stats['総数'] >= 2]

    zero_win = combo_stats[combo_stats['勝率(%)'] == 0.0].sort_values('総数', ascending=False)
    perfect_win = combo_stats[combo_stats['勝率(%)'] == 100.0].sort_values('総数', ascending=False)

    # 信用売り候補（勝率0%）のHTML行生成
    short_rows = []
    if len(zero_win) > 0:
        for (category, prev_dir), row in zero_win.iterrows():
            short_rows.append(f"""
                <tr style="background-color: #FFEBEE;">
                    <td>{category}</td>
                    <td>{prev_dir}</td>
                    <td class="num">{int(row['総数'])}</td>
                    <td class="num" style="font-weight: bold; color: red;">{row['勝率(%)']:.0f}%</td>
                    <td class="num">{row['平均リターン(%)']:.2f}%</td>
                    <td>✓ 信用売り</td>
                </tr>
            """)

    # 信用買い候補（勝率100%）のHTML行生成
    long_rows = []
    if len(perfect_win) > 0:
        for (category, prev_dir), row in perfect_win.iterrows():
            long_rows.append(f"""
                <tr style="background-color: #E8F5E9;">
                    <td>{category}</td>
                    <td>{prev_dir}</td>
                    <td class="num">{int(row['総数'])}</td>
                    <td class="num" style="font-weight: bold; color: green;">{row['勝率(%)']:.0f}%</td>
                    <td class="num">{row['平均リターン(%)']:.2f}%</td>
                    <td>✓ 信用買い</td>
                </tr>
            """)

    # 低勝率パターン（30%以下）
    low_win = combo_stats[(combo_stats['勝率(%)'] > 0) & (combo_stats['勝率(%)'] <= 30)].sort_values('勝率(%)')
    low_win_rows = []
    for (category, prev_dir), row in low_win.iterrows():
        low_win_rows.append(f"""
            <tr>
                <td>{category}</td>
                <td>{prev_dir}</td>
                <td class="num">{int(row['総数'])}</td>
                <td class="num">{row['勝率(%)']:.1f}%</td>
                <td class="num">{row['平均リターン(%)']:.2f}%</td>
            </tr>
        """)

    # 個別銘柄詳細（勝率0%パターンに該当する銘柄）
    detail_rows = []
    if len(zero_win) > 0:
        for (category, prev_dir), _ in zero_win.iterrows():
            matched = df[(df['categories'] == category) & (df['prev_direction'] == prev_dir)]
            for _, row in matched.iterrows():
                detail_rows.append(f"""
                    <tr>
                        <td>{row['ticker']}</td>
                        <td>{row['stock_name']}</td>
                        <td>{category}</td>
                        <td>{prev_dir}</td>
                        <td class="num">{row['backtest_date'].strftime('%Y-%m-%d')}</td>
                        <td class="num">{row['prev_day_change_pct']:.2f}%</td>
                        <td class="num">{row['buy_price']:.0f}円</td>
                        <td class="num" style="color: red; font-weight: bold;">{row['phase2_return_pct']:.2f}%</td>
                    </tr>
                """)

    # HTML生成
    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>信用売買戦略レポート</title>
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
                border-bottom: 3px solid #F44336;
                padding-bottom: 10px;
            }}
            h2 {{
                color: #555;
                margin-top: 40px;
                border-left: 5px solid #FF5722;
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
                background-color: #F44336;
                color: white;
                font-weight: bold;
            }}
            tr:hover {{
                background-color: #f5f5f5;
            }}
            .warning-box {{
                background-color: #FFF3E0;
                border-left: 4px solid #FF9800;
                padding: 15px;
                margin: 20px 0;
            }}
            .danger-box {{
                background-color: #FFEBEE;
                border-left: 4px solid #F44336;
                padding: 15px;
                margin: 20px 0;
            }}
            .success-box {{
                background-color: #E8F5E9;
                border-left: 4px solid #4CAF50;
                padding: 15px;
                margin: 20px 0;
            }}
            .info-box {{
                background-color: #E3F2FD;
                border-left: 4px solid #2196F3;
                padding: 15px;
                margin: 20px 0;
            }}
        </style>
    </head>
    <body>
        <h1>📊 信用売買戦略レポート</h1>

        <div class="info-box">
            <strong>データ期間:</strong> {df['backtest_date'].min().date()} ~ {df['backtest_date'].max().date()}<br>
            <strong>分析銘柄数:</strong> {len(df)}銘柄<br>
            <strong>目的:</strong> 勝率0%/100%のパターンを特定し、信用売買の戦略を立てる
        </div>

        <div class="warning-box">
            <strong>⚠️ 重要な注意事項</strong><br>
            - 現状のデータ数は{len(df)}件と少なく、統計的信頼性は限定的<br>
            - 各パターンは2-3件のみのサンプル<br>
            - 必ず損切りライン（例: ±3%）を設定すること<br>
            - より多くのデータ（100件以上）で検証が必要
        </div>

        <h2>🔴 勝率0%パターン（信用売り候補）</h2>

        {'<div class="danger-box"><strong>勝率0%のパターンが見つかりました！</strong><br>以下のパターンは全て下落しています。信用売りのチャンスです。</div>' if len(short_rows) > 0 else '<p>勝率0%のパターンは見つかりませんでした。</p>'}

        {f'''
        <table>
            <thead>
                <tr>
                    <th>カテゴリー</th>
                    <th>前日動向</th>
                    <th>件数</th>
                    <th>勝率</th>
                    <th>平均リターン</th>
                    <th>戦略</th>
                </tr>
            </thead>
            <tbody>
                {''.join(short_rows)}
            </tbody>
        </table>
        ''' if len(short_rows) > 0 else ''}

        <h2>🟢 勝率100%パターン（信用買い候補）</h2>

        {'<div class="success-box"><strong>勝率100%のパターンが見つかりました！</strong><br>以下のパターンは全て上昇しています。信用買いのチャンスです。</div>' if len(long_rows) > 0 else '<p>勝率100%のパターンは見つかりませんでした。</p>'}

        {f'''
        <table>
            <thead>
                <tr>
                    <th>カテゴリー</th>
                    <th>前日動向</th>
                    <th>件数</th>
                    <th>勝率</th>
                    <th>平均リターン</th>
                    <th>戦略</th>
                </tr>
            </thead>
            <tbody>
                {''.join(long_rows)}
            </tbody>
        </table>
        ''' if len(long_rows) > 0 else ''}

        <h2>📉 低勝率パターン（30%以下）</h2>
        <p>勝率0%ではないが、低勝率のパターン（信用売り検討）</p>

        {f'''
        <table>
            <thead>
                <tr>
                    <th>カテゴリー</th>
                    <th>前日動向</th>
                    <th>件数</th>
                    <th>勝率</th>
                    <th>平均リターン</th>
                </tr>
            </thead>
            <tbody>
                {''.join(low_win_rows)}
            </tbody>
        </table>
        ''' if len(low_win_rows) > 0 else '<p>該当パターンなし</p>'}

        <h2>📋 勝率0%パターンの詳細（個別銘柄）</h2>

        {f'''
        <table>
            <thead>
                <tr>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th>カテゴリー</th>
                    <th>前日動向</th>
                    <th>日付</th>
                    <th>前日変化率</th>
                    <th>買付価格</th>
                    <th>Phase2リターン</th>
                </tr>
            </thead>
            <tbody>
                {''.join(detail_rows)}
            </tbody>
        </table>
        ''' if len(detail_rows) > 0 else '<p>該当データなし</p>'}

        <h2>📖 使い方</h2>

        <div class="info-box">
            <h3>信用売りの手順</h3>
            <ol>
                <li><strong>朝の確認:</strong> 対象銘柄のカテゴリーと前日終値を確認</li>
                <li><strong>パターンマッチ:</strong> 勝率0%パターンに該当するか判定</li>
                <li><strong>前場で売り:</strong> 9:00-11:30の前場で信用売り仕掛け</li>
                <li><strong>終値でクローズ:</strong> 15:00の終値で買い戻し</li>
                <li><strong>損切り設定:</strong> +3%上昇したら損切り</li>
            </ol>

            <h3>具体例</h3>
            <p>「テーマ連動+株クラバズ」の銘柄が前日プラスで終えた場合：</p>
            <ul>
                <li>勝率0%（3件中0勝）のため、信用売りチャンス</li>
                <li>前場（9:00-11:30）で売り仕掛け</li>
                <li>当日終値（15:00）で買い戻し</li>
                <li>損切りライン: 寄り付き価格から+3%上昇で損切り</li>
            </ul>
        </div>

        <h2>⚠️ リスク管理</h2>

        <div class="danger-box">
            <strong>必ず守るべきルール:</strong>
            <ul>
                <li>損切りラインを必ず設定（推奨: ±3%）</li>
                <li>1銘柄への投資は資金の10%以内</li>
                <li>データ数が少ないため過信しない</li>
                <li>継続的にデータを蓄積し、パターンの再現性を検証</li>
                <li>市場環境の変化に注意（相場全体の暴落時は機能しない）</li>
            </ul>
        </div>

        <h2>📈 次のステップ</h2>

        <div class="info-box">
            <h3>推奨する改善策</h3>
            <ol>
                <li><strong>データ蓄積:</strong> 11/08以降も毎日データを追加（目標: 100件以上）</li>
                <li><strong>カテゴリー統合:</strong> 細かいカテゴリーを大分類にまとめる</li>
                <li><strong>ボラティリティ追加:</strong> 前場の値動きが大きい銘柄は後場で反落しやすい</li>
                <li><strong>Phase3分析:</strong> 損切りライン別のパフォーマンスを分析</li>
                <li><strong>バックテスト:</strong> 過去データでパターンの再現性を検証</li>
            </ol>
        </div>

        <footer style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ddd; text-align: center; color: #777;">
            <p>生成日時: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p style="color: #F44336; font-weight: bold;">投資は自己責任で行ってください。このレポートは投資助言ではありません。</p>
        </footer>
    </body>
    </html>
    """

    # 保存
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"信用売買戦略レポートを生成しました: {OUTPUT_PATH}")
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

    # サマリー
    print(f"\n=== サマリー ===")
    print(f"勝率0%パターン: {len(zero_win)}件")
    print(f"勝率100%パターン: {len(perfect_win)}件")
    print(f"低勝率パターン(30%以下): {len(low_win)}件")


if __name__ == '__main__':
    generate_report()
