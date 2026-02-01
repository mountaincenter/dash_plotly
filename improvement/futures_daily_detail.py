"""
先物データ × Grok戦略 日別詳細データ出力

データソース明確化:
- 先物: yfinance NKD=F（CME日経225先物、USD建て）
  - fetch_index_prices.py で16:45に取得
  - futures_prices_max_1d.parquet に保存
- Grok: grok_trending_archive.parquet
  - 23:00に銘柄選定、翌営業日のバックテスト結果

計算方法:
- gap_pct = (当日Open - 前日Close) / 前日Close * 100
- 前日Close = 先物日足の前営業日のClose値
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "parquet"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_futures() -> pd.DataFrame:
    """先物データを読み込み"""
    path = DATA_DIR / "futures_prices_max_1d.parquet"
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


def load_grok() -> pd.DataFrame:
    """Grokデータを読み込み"""
    path = DATA_DIR / "backtest" / "grok_trending_archive.parquet"
    df = pd.read_parquet(path)
    return df


def prepare_futures(df: pd.DataFrame) -> pd.DataFrame:
    """先物データを準備"""
    df = df.sort_values("date").copy()

    # 前日終値
    df["prev_close"] = df["Close"].shift(1)

    # ギャップ計算
    df["gap_pct"] = (df["Open"] - df["prev_close"]) / df["prev_close"] * 100

    # 日中変動
    df["intraday_pct"] = (df["Close"] - df["Open"]) / df["Open"] * 100

    # 前日比
    df["daily_change_pct"] = (df["Close"] - df["prev_close"]) / df["prev_close"] * 100

    return df


def prepare_grok(df: pd.DataFrame) -> pd.DataFrame:
    """Grokデータを準備"""
    df = df.copy()

    # 日付
    df["date"] = pd.to_datetime(df["selection_date"]).dt.normalize()

    # 2025-11-04以降
    df = df[df["date"] >= "2025-11-04"]

    # 制度信用 or いちにち信用
    df = df[(df["shortable"] == True) | ((df["day_trade"] == True) & (df["shortable"] == False))]

    # ショート利益（符号反転）
    df["short_p2"] = -df["profit_per_100_shares_phase2"].fillna(0)

    return df


def generate_html(df_daily: pd.DataFrame) -> str:
    """HTML生成"""

    html = """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>先物 × Grok 日別詳細データ</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 20px;
            background: #f5f5f5;
        }
        h1 { color: #333; border-bottom: 2px solid #333; padding-bottom: 10px; }
        h2 { color: #555; margin-top: 30px; }
        .info-box {
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 15px;
            margin: 20px 0;
        }
        .info-box h3 { margin-top: 0; color: #333; }
        .info-box ul { margin: 0; padding-left: 20px; }
        .info-box li { margin: 5px 0; }
        .highlight { background: #fff3cd; padding: 2px 5px; border-radius: 3px; }
        table {
            border-collapse: collapse;
            width: 100%;
            background: #fff;
            margin: 20px 0;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px 12px;
            text-align: right;
        }
        th {
            background: #333;
            color: #fff;
            font-weight: 600;
        }
        td:first-child, th:first-child { text-align: left; }
        tr:nth-child(even) { background: #f9f9f9; }
        tr:hover { background: #f0f0f0; }
        .positive { color: #28a745; font-weight: 600; }
        .negative { color: #dc3545; font-weight: 600; }
        .warning { background: #fff3cd !important; }
        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        .summary-card {
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
        }
        .summary-card .value {
            font-size: 24px;
            font-weight: 700;
        }
        .summary-card .label { color: #666; font-size: 14px; }
    </style>
</head>
<body>
    <h1>先物 × Grok戦略 日別詳細データ</h1>

    <div class="info-box">
        <h3>📊 データソース</h3>
        <ul>
            <li><strong>先物データ:</strong> yfinance <span class="highlight">NKD=F</span>（CME日経225先物、USD建て）</li>
            <li><strong>取得タイミング:</strong> fetch_index_prices.py で <span class="highlight">16:45 JST</span> に取得</li>
            <li><strong>保存先:</strong> futures_prices_max_1d.parquet</li>
            <li><strong>Grokデータ:</strong> grok_trending_archive.parquet（23:00選定、翌営業日結果）</li>
        </ul>
    </div>

    <div class="info-box">
        <h3>📐 計算方法</h3>
        <ul>
            <li><strong>gap_pct:</strong> (当日Open - 前日Close) / 前日Close × 100</li>
            <li><strong>前日Close:</strong> NKD=F日足の<span class="highlight">前営業日</span>のClose値</li>
            <li><strong>ショート利益:</strong> Grok推奨銘柄のショート戦略利益（符号反転）</li>
        </ul>
    </div>

    <div class="info-box" style="background: #fff3cd;">
        <h3>⚠️ 注意事項</h3>
        <ul>
            <li>NKD=FはCME（シカゴ）の先物で、<strong>米国東部時間</strong>で日付が区切られる</li>
            <li>日本の祝日は米国市場が開いていればデータあり</li>
            <li>日本市場の寄付き前のギャップ予測には、<strong>23:00時点のリアルタイム値</strong>が必要</li>
        </ul>
    </div>
"""

    # サマリー
    total_profit = df_daily["short_profit_sum"].sum()
    total_days = len(df_daily)
    win_days = (df_daily["short_profit_sum"] > 0).sum()

    gap_positive = df_daily[df_daily["gap_pct"] >= 1]
    gap_negative = df_daily[df_daily["gap_pct"] <= -1]

    html += f"""
    <h2>📈 サマリー</h2>
    <div class="summary">
        <div class="summary-card">
            <div class="value {'positive' if total_profit > 0 else 'negative'}">{int(total_profit):+,}円</div>
            <div class="label">総利益</div>
        </div>
        <div class="summary-card">
            <div class="value">{total_days}日</div>
            <div class="label">営業日数</div>
        </div>
        <div class="summary-card">
            <div class="value">{win_days}/{total_days}</div>
            <div class="label">勝ち日数</div>
        </div>
        <div class="summary-card">
            <div class="value {'negative' if len(gap_positive) > 0 and gap_positive['short_profit_sum'].sum() < 0 else ''}">{int(gap_positive['short_profit_sum'].sum()):+,}円</div>
            <div class="label">Gap+1%以上の日 ({len(gap_positive)}日)</div>
        </div>
    </div>
"""

    # 日別テーブル
    html += """
    <h2>📅 日別データ</h2>
    <table>
        <tr>
            <th>日付</th>
            <th>曜日</th>
            <th>前日Close</th>
            <th>当日Open</th>
            <th>当日Close</th>
            <th>Gap%</th>
            <th>日中%</th>
            <th>銘柄数</th>
            <th>ショート利益</th>
        </tr>
"""

    weekday_map = {0: "月", 1: "火", 2: "水", 3: "木", 4: "金", 5: "土", 6: "日"}

    for _, row in df_daily.iterrows():
        date_str = row["date"].strftime("%Y-%m-%d")
        weekday = weekday_map[row["date"].weekday()]

        gap_class = "warning" if row["gap_pct"] >= 1 else ""
        profit_class = "positive" if row["short_profit_sum"] > 0 else "negative"
        gap_value_class = "positive" if row["gap_pct"] < 0 else ("negative" if row["gap_pct"] > 1 else "")

        html += f"""
        <tr class="{gap_class}">
            <td>{date_str}</td>
            <td>{weekday}</td>
            <td>{row['prev_close']:,.0f}</td>
            <td>{row['Open']:,.0f}</td>
            <td>{row['Close']:,.0f}</td>
            <td class="{gap_value_class}">{row['gap_pct']:+.2f}%</td>
            <td>{row['intraday_pct']:+.2f}%</td>
            <td>{int(row['stock_count'])}</td>
            <td class="{profit_class}">{int(row['short_profit_sum']):+,}円</td>
        </tr>
"""

    html += """
    </table>

    <div class="info-box">
        <h3>🔍 分析ポイント</h3>
        <ul>
            <li><span style="background: #fff3cd; padding: 2px 5px;">黄色行</span>: Gap +1%以上（見送り候補）</li>
            <li><span class="positive">緑字</span>: プラス / <span class="negative">赤字</span>: マイナス</li>
            <li>Gap +1%以上の日の利益合計を確認 → 見送りルールの有効性</li>
        </ul>
    </div>
"""

    html += f"""
    <p style="color: #999; font-size: 12px; margin-top: 40px;">
        生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    </p>
</body>
</html>
"""
    return html


def main():
    print("=" * 60)
    print("先物 × Grok 日別詳細データ出力")
    print("=" * 60)

    # データ読み込み
    df_futures = load_futures()
    df_grok = load_grok()

    print(f"\n先物データ: {len(df_futures)}件 (NKD=F)")
    print(f"Grokデータ: {len(df_grok)}件")

    # 前処理
    df_futures = prepare_futures(df_futures)
    df_grok = prepare_grok(df_grok)

    print(f"Grok（フィルタ後）: {len(df_grok)}件")

    # 日次集計
    grok_daily = df_grok.groupby("date").agg(
        stock_count=("ticker", "count"),
        short_profit_sum=("short_p2", "sum"),
    ).reset_index()

    # マージ
    df_futures["date"] = pd.to_datetime(df_futures["date"]).dt.normalize()
    grok_daily["date"] = pd.to_datetime(grok_daily["date"]).dt.normalize()

    df_daily = grok_daily.merge(
        df_futures[["date", "Open", "Close", "prev_close", "gap_pct", "intraday_pct"]],
        on="date",
        how="left"
    )

    # 先物データがない日を除外
    df_daily = df_daily[df_daily["gap_pct"].notna()]
    df_daily = df_daily.sort_values("date")

    print(f"\n日別データ: {len(df_daily)}日")

    # HTML生成
    html = generate_html(df_daily)

    output_file = OUTPUT_DIR / "futures_daily_detail.html"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✓ HTML出力: {output_file}")

    # コンソールにも表示
    print("\n" + "=" * 60)
    print("日別データ（コンソール版）")
    print("=" * 60)
    print(df_daily[["date", "prev_close", "Open", "gap_pct", "stock_count", "short_profit_sum"]].to_string())


if __name__ == "__main__":
    main()
