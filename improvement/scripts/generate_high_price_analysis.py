"""
10,000円超え銘柄の損失分析HTML生成
- RSI(14)
- 前日終値・騰落率
- 選定理由
- 勝ち vs 負けパターン比較
"""
import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime, timedelta
import yfinance as yf

# パス設定
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "parquet" / "backtest"
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"

def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    """RSI計算"""
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def fetch_daily_data(ticker: str, date: str) -> dict:
    """前日の日足データとRSIを取得"""
    try:
        target_date = pd.Timestamp(date)
        start_date = target_date - timedelta(days=30)

        df = yf.download(ticker, start=start_date.strftime("%Y-%m-%d"),
                        end=(target_date + timedelta(days=1)).strftime("%Y-%m-%d"),
                        progress=False)

        if df.empty:
            return {}

        # MultiIndex対応
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # RSI計算
        df['RSI'] = calculate_rsi(df['Close'])

        # 前日データ取得
        df.index = pd.to_datetime(df.index)
        target_idx = df.index.get_indexer([target_date], method='ffill')[0]

        if target_idx <= 0:
            return {}

        prev_idx = target_idx - 1
        prev_row = df.iloc[prev_idx]
        target_row = df.iloc[target_idx]

        # 連騰日数計算
        consecutive_up = 0
        for i in range(prev_idx, -1, -1):
            if i > 0 and df.iloc[i]['Close'] > df.iloc[i-1]['Close']:
                consecutive_up += 1
            else:
                break

        return {
            'prev_close': float(prev_row['Close']),
            'prev_change_pct': float((prev_row['Close'] - df.iloc[prev_idx-1]['Close']) / df.iloc[prev_idx-1]['Close'] * 100) if prev_idx > 0 else None,
            'prev_rsi': float(prev_row['RSI']) if not pd.isna(prev_row['RSI']) else None,
            'open_price': float(target_row['Open']),
            'gap_pct': float((target_row['Open'] - prev_row['Close']) / prev_row['Close'] * 100),
            'consecutive_up': consecutive_up
        }
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return {}

def fetch_5min_data(ticker: str, date: str) -> pd.DataFrame:
    """5分足データ取得"""
    try:
        target_date = pd.Timestamp(date)
        start = target_date
        end = target_date + timedelta(days=1)

        df = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                        end=end.strftime("%Y-%m-%d"), interval="5m", progress=False)

        if df.empty:
            return pd.DataFrame()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # JSTに変換
        df.index = df.index.tz_convert('Asia/Tokyo')

        # 9:00-15:30のみ
        df = df.between_time('09:00', '15:25')

        return df
    except Exception as e:
        print(f"Error fetching 5min {ticker}: {e}")
        return pd.DataFrame()

def main():
    # バックテストデータ読み込み
    archive_path = DATA_DIR / "grok_trending_archive.parquet"
    df = pd.read_parquet(archive_path)

    print(f"データ件数: {len(df)}")
    print(f"期間: {df['backtest_date'].min()} - {df['backtest_date'].max()}")

    # 10,000円超え銘柄を抽出
    high_price = df[df['buy_price'] >= 10000].copy()
    print(f"10,000円超え: {len(high_price)}件")

    # 損益計算
    high_price['pnl'] = (high_price['buy_price'] - high_price['daily_close']) * 100  # 100株
    high_price['pnl_pct'] = (high_price['buy_price'] - high_price['daily_close']) / high_price['buy_price'] * 100

    # 勝ち負け分類
    wins = high_price[high_price['pnl'] > 0].copy()
    losses = high_price[high_price['pnl'] < 0].copy()

    print(f"勝ち: {len(wins)}件, 負け: {len(losses)}件")

    # 追加データ取得
    all_data = []
    for _, row in high_price.iterrows():
        # tickerに既に.Tが含まれている場合は追加しない
        ticker_raw = str(row['ticker'])
        ticker = ticker_raw if ticker_raw.endswith('.T') else f"{ticker_raw}.T"
        date = str(row['backtest_date'])[:10]

        print(f"Fetching: {ticker} {date}")

        daily = fetch_daily_data(ticker, date)
        fivemin = fetch_5min_data(ticker, date)

        # 5分足から詳細を取得
        if not fivemin.empty:
            morning = fivemin.between_time('09:00', '11:30')
            afternoon = fivemin.between_time('12:30', '15:25')

            high_time = fivemin['High'].idxmax() if not fivemin.empty else None
            low_time = fivemin['Low'].idxmin() if not fivemin.empty else None

            # 寄底かどうか
            is_yorisoko = (low_time == fivemin.index[0]) if low_time is not None else False

            # 高値引けかどうか
            is_takane_hike = (high_time == fivemin.index[-1]) if high_time is not None else False

            # チャート用データ（時刻とOHLC）
            chart_data = []
            for idx, r in fivemin.iterrows():
                chart_data.append({
                    'time': idx.strftime('%H:%M'),
                    'open': float(r['Open']),
                    'high': float(r['High']),
                    'low': float(r['Low']),
                    'close': float(r['Close']),
                })

            fivemin_data = {
                'high': float(fivemin['High'].max()),
                'low': float(fivemin['Low'].min()),
                'high_time': str(high_time.time()) if high_time is not None else None,
                'low_time': str(low_time.time()) if low_time is not None else None,
                'is_yorisoko': is_yorisoko,
                'is_takane_hike': is_takane_hike,
                'chart_data': chart_data,
            }
        else:
            fivemin_data = {'chart_data': []}

        # HTML表示用に.Tを除去
        ticker_display = ticker_raw.replace('.T', '')

        all_data.append({
            'ticker': ticker_display,
            'stock_name': row.get('stock_name', ''),
            'date': date,
            'reason': row.get('reason', ''),
            'grok_rank': row.get('grok_rank'),
            'buy_price': row['buy_price'],
            'daily_close': row['daily_close'],
            'pnl': row['pnl'],
            'pnl_pct': row['pnl_pct'],
            **daily,
            **fivemin_data,
        })

    # 勝ち負けでグループ化
    win_data = [d for d in all_data if d['pnl'] > 0]
    loss_data = [d for d in all_data if d['pnl'] < 0]

    # 統計計算
    def calc_stats(data_list):
        if not data_list:
            return {}

        rsi_list = [d['prev_rsi'] for d in data_list if d.get('prev_rsi')]
        change_list = [d['prev_change_pct'] for d in data_list if d.get('prev_change_pct') is not None]
        gap_list = [d['gap_pct'] for d in data_list if d.get('gap_pct') is not None]

        return {
            'count': len(data_list),
            'avg_rsi': np.mean(rsi_list) if rsi_list else None,
            'avg_prev_change': np.mean(change_list) if change_list else None,
            'avg_gap': np.mean(gap_list) if gap_list else None,
            'prev_positive_rate': sum(1 for c in change_list if c > 0) / len(change_list) * 100 if change_list else 0,
            'total_pnl': sum(d['pnl'] for d in data_list),
        }

    win_stats = calc_stats(win_data)
    loss_stats = calc_stats(loss_data)

    print(f"\n勝ち統計: {win_stats}")
    print(f"負け統計: {loss_stats}")

    # HTML生成
    html = generate_html(all_data, win_data, loss_data, win_stats, loss_stats)

    output_path = OUTPUT_DIR / "high_price_loss_analysis.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n出力: {output_path}")

def generate_html(all_data, win_data, loss_data, win_stats, loss_stats):
    """HTML生成"""

    total_pnl = sum(d['pnl'] for d in all_data)
    total_count = len(all_data)
    win_count = len(win_data)
    loss_count = len(loss_data)

    # 損失銘柄のみ詳細表示
    loss_cards = ""
    chart_scripts = ""
    for idx, d in enumerate(sorted(loss_data, key=lambda x: x['pnl'])):
        prev_rsi_str = f"{d['prev_rsi']:.1f}" if d.get('prev_rsi') else "N/A"
        prev_change_str = f"{d['prev_change_pct']:+.2f}%" if d.get('prev_change_pct') is not None else "N/A"
        gap_str = f"{d['gap_pct']:+.2f}%" if d.get('gap_pct') is not None else "N/A"
        prev_close_str = f"{d['prev_close']:,.0f}円" if d.get('prev_close') else "N/A"

        # パターン判定
        pattern = "不明"
        pattern_class = ""
        if d.get('is_yorisoko'):
            pattern = "寄底→上昇型"
            pattern_class = "pattern-yorisoko"
        elif d.get('is_takane_hike'):
            pattern = "高値引け型"
            pattern_class = "pattern-高値"
        else:
            pattern = "途中安値型"
            pattern_class = "pattern-途中"

        reason_text = d.get('reason', 'N/A')
        if len(reason_text) > 200:
            reason_text = reason_text[:200] + "..."

        loss_cards += f"""
        <div class="stock-card">
            <div class="stock-header">
                <div>
                    <span class="stock-title">{d.get('stock_name', '')} ({d['ticker']}.T)</span>
                    <span class="stock-date">{d['date']}</span>
                </div>
                <div class="stock-loss">{d['pnl']:+,.0f}円</div>
            </div>
            <div class="stock-body">
                <div class="info-grid">
                    <div class="info-item">
                        <div class="info-label">前日終値</div>
                        <div class="info-value">{prev_close_str}</div>
                    </div>
                    <div class="info-item">
                        <div class="info-label">前日騰落</div>
                        <div class="info-value {'up' if d.get('prev_change_pct', 0) > 0 else 'down'}">{prev_change_str}</div>
                    </div>
                    <div class="info-item">
                        <div class="info-label">前日RSI</div>
                        <div class="info-value {'up' if d.get('prev_rsi', 50) > 70 else ''}">{prev_rsi_str}</div>
                    </div>
                    <div class="info-item">
                        <div class="info-label">ギャップ</div>
                        <div class="info-value {'up' if d.get('gap_pct', 0) > 0 else 'down'}">{gap_str}</div>
                    </div>
                </div>
                <div class="info-grid">
                    <div class="info-item">
                        <div class="info-label">始値（空売り価格）</div>
                        <div class="info-value">{d['buy_price']:,.0f}円</div>
                    </div>
                    <div class="info-item">
                        <div class="info-label">終値</div>
                        <div class="info-value">{d['daily_close']:,.0f}円</div>
                    </div>
                    <div class="info-item">
                        <div class="info-label">高値時刻</div>
                        <div class="info-value">{d.get('high_time', 'N/A')}</div>
                    </div>
                    <div class="info-item">
                        <div class="info-label">安値時刻</div>
                        <div class="info-value">{d.get('low_time', 'N/A')}</div>
                    </div>
                </div>
                <div style="margin-top: 15px;">
                    <span class="pattern-badge {pattern_class}">{pattern}</span>
                </div>
                <div class="chart-container">
                    <canvas id="chart_{idx}"></canvas>
                </div>
                <div class="analysis">
                    <h4>選定理由</h4>
                    <p>{reason_text}</p>
                </div>
            </div>
        </div>
        """

        # チャート用スクリプト生成
        chart_data = d.get('chart_data', [])
        if chart_data:
            buy_price = d['buy_price']
            chart_scripts += f"""
            (function() {{
                const ctx = document.getElementById('chart_{idx}').getContext('2d');
                const data = {json.dumps(chart_data)};
                const labels = data.map(d => d.time);
                const closeData = data.map(d => d.close);

                new Chart(ctx, {{
                    type: 'line',
                    data: {{
                        labels: labels,
                        datasets: [
                            {{
                                label: '株価',
                                data: closeData,
                                borderColor: '#60a5fa',
                                backgroundColor: 'rgba(96, 165, 250, 0.1)',
                                fill: true,
                                tension: 0.1,
                                pointRadius: 0,
                            }},
                            {{
                                label: '空売り価格 ({buy_price:,.0f}円)',
                                data: Array(labels.length).fill({buy_price}),
                                borderColor: '#f87171',
                                borderDash: [5, 5],
                                pointRadius: 0,
                                fill: false,
                            }}
                        ]
                    }},
                    options: {{
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {{
                            legend: {{ display: true, position: 'top', labels: {{ color: '#ccc', font: {{ size: 11 }} }} }},
                        }},
                        scales: {{
                            x: {{
                                ticks: {{ color: '#888', maxRotation: 45, font: {{ size: 10 }} }},
                                grid: {{ color: '#333' }},
                            }},
                            y: {{
                                ticks: {{ color: '#888', font: {{ size: 10 }} }},
                                grid: {{ color: '#333' }},
                            }}
                        }}
                    }}
                }});
            }})();
            """

    # 値を安全に取得
    def safe_fmt(val, fmt=".1f", suffix=""):
        if val is None:
            return "N/A"
        try:
            return f"{val:{fmt}}{suffix}"
        except:
            return "N/A"

    win_rsi = safe_fmt(win_stats.get('avg_rsi'))
    loss_rsi = safe_fmt(loss_stats.get('avg_rsi'))
    win_change = safe_fmt(win_stats.get('avg_prev_change'), "+.2f", "%")
    loss_change = safe_fmt(loss_stats.get('avg_prev_change'), "+.2f", "%")
    win_gap = safe_fmt(win_stats.get('avg_gap'), "+.2f", "%")
    loss_gap = safe_fmt(loss_stats.get('avg_gap'), "+.2f", "%")
    win_pos_rate = safe_fmt(win_stats.get('prev_positive_rate'), ".0f", "%")
    loss_pos_rate = safe_fmt(loss_stats.get('prev_positive_rate'), ".0f", "%")

    # 比較テーブル
    comparison_table = f"""
    <div class="comparison-section">
        <h2>勝ち vs 負け パターン比較</h2>
        <table class="comparison-table">
            <thead>
                <tr>
                    <th>指標</th>
                    <th class="win">勝ち ({win_stats.get('count', 0)}件)</th>
                    <th class="loss">負け ({loss_stats.get('count', 0)}件)</th>
                    <th>差異</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>平均RSI</td>
                    <td class="win">{win_rsi}</td>
                    <td class="loss">{loss_rsi}</td>
                    <td>{'勝ちの方が高い' if (win_stats.get('avg_rsi') or 0) > (loss_stats.get('avg_rsi') or 0) else '負けの方が高い'}</td>
                </tr>
                <tr>
                    <td>前日騰落率</td>
                    <td class="win">{win_change}</td>
                    <td class="loss">{loss_change}</td>
                    <td>{'勝ちの方が上昇' if (win_stats.get('avg_prev_change') or 0) > (loss_stats.get('avg_prev_change') or 0) else '負けの方が上昇'}</td>
                </tr>
                <tr>
                    <td>平均ギャップ</td>
                    <td class="win">{win_gap}</td>
                    <td class="loss">{loss_gap}</td>
                    <td>{'勝ちがGD' if (win_stats.get('avg_gap') or 0) < (loss_stats.get('avg_gap') or 0) else '負けがGD'}</td>
                </tr>
                <tr>
                    <td>前日陽線率</td>
                    <td class="win">{win_pos_rate}</td>
                    <td class="loss">{loss_pos_rate}</td>
                    <td>-</td>
                </tr>
                <tr>
                    <td>合計損益</td>
                    <td class="win">{win_stats.get('total_pnl', 0):+,.0f}円</td>
                    <td class="loss">{loss_stats.get('total_pnl', 0):+,.0f}円</td>
                    <td>-</td>
                </tr>
            </tbody>
        </table>
    </div>
    """

    # フィルタリング提案
    filter_suggestion = """
    <div class="conclusion">
        <h3>フィルタリング提案</h3>
        <ul>
            <li><strong>RSI &lt; 50:</strong> 過熱感がなく、空売りリスク高い → スキップ検討</li>
            <li><strong>前日陰線:</strong> 下落トレンド継続の可能性 → エントリー優先</li>
            <li><strong>ギャップアップ &gt; 1%:</strong> 強い買い圧力 → スキップ検討</li>
            <li><strong>連騰3日以上:</strong> 過熱感高い → エントリー優先（ただし損切り厳格に）</li>
        </ul>
        <p style="color: #888; font-size: 0.85rem; margin-top: 15px;">
            ※ 10,000円超え銘柄は1ティックの損益が大きいため、フィルタリングで損失回避を優先
        </p>
    </div>
    """

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>10,000円超え銘柄 損失分析</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        .chart-container {{ height: 250px; margin-top: 15px; background: #0f0f1a; border-radius: 8px; padding: 10px; }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0a0a0f; color: #e0e0e0; padding: 20px; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{ color: #fff; margin-bottom: 10px; font-size: 1.4rem; }}
        h2 {{ color: #fff; margin: 30px 0 15px 0; font-size: 1.2rem; }}
        .subtitle {{ color: #888; font-size: 0.9rem; margin-bottom: 30px; }}
        .summary-grid {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 15px; margin-bottom: 30px; }}
        .summary-card {{ background: #1a1a2e; border: 1px solid #333; border-radius: 12px; padding: 15px; text-align: center; }}
        .summary-label {{ color: #888; font-size: 0.8rem; margin-bottom: 5px; }}
        .summary-value {{ font-size: 1.3rem; font-weight: bold; }}
        .summary-value.negative {{ color: #f87171; }}
        .summary-value.positive {{ color: #4ade80; }}
        .stock-card {{ background: #1a1a2e; border: 1px solid #333; border-radius: 12px; margin-bottom: 20px; overflow: hidden; }}
        .stock-header {{ background: #222244; padding: 15px 20px; display: flex; justify-content: space-between; align-items: center; }}
        .stock-title {{ font-weight: bold; color: #fff; font-size: 1.1rem; }}
        .stock-date {{ color: #888; font-size: 0.9rem; margin-left: 15px; }}
        .stock-loss {{ font-weight: bold; font-size: 1.2rem; color: #f87171; }}
        .stock-body {{ padding: 20px; }}
        .info-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 15px; }}
        .info-item {{ background: #0f0f1a; border-radius: 8px; padding: 12px; }}
        .info-label {{ color: #888; font-size: 0.75rem; margin-bottom: 3px; }}
        .info-value {{ font-size: 1rem; font-weight: bold; color: #fff; }}
        .info-value.up {{ color: #4ade80; }}
        .info-value.down {{ color: #f87171; }}
        .pattern-badge {{ display: inline-block; padding: 6px 12px; border-radius: 20px; font-size: 0.85rem; font-weight: bold; }}
        .pattern-yorisoko {{ background: #dc262633; color: #f87171; }}
        .pattern-途中 {{ background: #f59e0b33; color: #fbbf24; }}
        .pattern-高値 {{ background: #22c55e33; color: #4ade80; }}
        .analysis {{ background: #0f0f1a; border-radius: 8px; padding: 15px; margin-top: 15px; }}
        .analysis h4 {{ color: #fff; font-size: 0.9rem; margin-bottom: 10px; }}
        .analysis p {{ color: #aaa; font-size: 0.85rem; line-height: 1.6; }}
        .comparison-section {{ background: #1a1a2e; border: 1px solid #333; border-radius: 12px; padding: 20px; margin: 30px 0; }}
        .comparison-table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        .comparison-table th, .comparison-table td {{ padding: 12px; text-align: center; border-bottom: 1px solid #333; }}
        .comparison-table th {{ background: #222244; color: #fff; font-size: 0.85rem; }}
        .comparison-table td {{ color: #ccc; font-size: 0.9rem; }}
        .comparison-table td.win {{ color: #4ade80; font-weight: bold; }}
        .comparison-table td.loss {{ color: #f87171; font-weight: bold; }}
        .conclusion {{ background: linear-gradient(135deg, #1a1a2e 0%, #2a2a4e 100%); border: 1px solid #4a90d9; border-radius: 12px; padding: 20px; margin-top: 30px; }}
        .conclusion h3 {{ color: #4a90d9; margin-bottom: 15px; }}
        .conclusion ul {{ list-style: none; padding-left: 0; }}
        .conclusion li {{ color: #ccc; font-size: 0.9rem; padding: 8px 0; border-bottom: 1px solid #333; }}
        .conclusion li:last-child {{ border-bottom: none; }}
        .conclusion li strong {{ color: #fff; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>10,000円超え銘柄 損益分析レポート</h1>
        <p class="subtitle">寄付ショート戦略における高価格帯銘柄のパターン分析（RSI・前日終値・選定理由）</p>

        <div class="summary-grid">
            <div class="summary-card">
                <div class="summary-label">分析対象</div>
                <div class="summary-value">{total_count}件</div>
            </div>
            <div class="summary-card">
                <div class="summary-label">勝ち</div>
                <div class="summary-value positive">{win_count}件</div>
            </div>
            <div class="summary-card">
                <div class="summary-label">負け</div>
                <div class="summary-value negative">{loss_count}件</div>
            </div>
            <div class="summary-card">
                <div class="summary-label">勝率</div>
                <div class="summary-value">{win_count/total_count*100:.0f}%</div>
            </div>
            <div class="summary-card">
                <div class="summary-label">合計損益</div>
                <div class="summary-value {'positive' if total_pnl > 0 else 'negative'}">{total_pnl:+,.0f}円</div>
            </div>
        </div>

        {comparison_table}

        <h2>負け銘柄 詳細分析</h2>
        {loss_cards}

        {filter_suggestion}

        <p style="color: #666; font-size: 0.75rem; margin-top: 30px; text-align: center;">
            生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>
    </div>
    <script>
    {chart_scripts}
    </script>
</body>
</html>
"""
    return html

if __name__ == "__main__":
    main()
