"""
先物5分足 × Grokショート利益 日別詳細データ

データソース:
- futures_prices_60d_5m.parquet: yfinance NKD=F 5分足（JST変換済み）
- grok_trending_archive.parquet: Grok推奨銘柄バックテスト結果

計算方法:
- 23:00 JST時点の先物価格を5分足から取得
- 前営業日の23:00 JST価格との変動率を計算
- Grokショート利益との相関を分析
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "parquet"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_futures_5m() -> pd.DataFrame:
    """先物5分足データを読み込み"""
    path = DATA_DIR / "futures_prices_60d_5m.parquet"
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_grok() -> pd.DataFrame:
    """Grokデータを読み込み"""
    path = DATA_DIR / "backtest" / "grok_trending_archive.parquet"
    df = pd.read_parquet(path)
    return df


def extract_2300_prices(df_5m: pd.DataFrame) -> pd.DataFrame:
    """各日の23:00 JST付近の価格を抽出"""
    df = df_5m.copy()

    # 日付と時刻を分離
    df["trade_date"] = df["date"].dt.date
    df["hour"] = df["date"].dt.hour
    df["minute"] = df["date"].dt.minute

    # 22:55〜23:05の範囲で最も23:00に近いデータを取得
    df_2300 = df[(df["hour"] == 22) & (df["minute"] >= 55) |
                 (df["hour"] == 23) & (df["minute"] <= 5)]

    # 各日で23:00に最も近いものを選択
    result = []
    for trade_date, group in df_2300.groupby("trade_date"):
        # 23:00との差が最小のものを選択
        group = group.copy()
        group["diff_to_2300"] = abs(group["hour"] * 60 + group["minute"] - 23 * 60)
        closest = group.loc[group["diff_to_2300"].idxmin()]
        result.append({
            "date": pd.Timestamp(trade_date),
            "time_2300": closest["date"],
            "price_2300": closest["Close"],
        })

    return pd.DataFrame(result)


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

    # 先物変動の区間別集計
    bins = [-np.inf, -1, 0, 1, 2, np.inf]
    labels = ["<-1%", "-1~0%", "0~+1%", "+1~+2%", ">+2%"]
    df_daily["change_range"] = pd.cut(df_daily["futures_change_pct"], bins=bins, labels=labels)

    range_summary = df_daily.groupby("change_range", observed=True).agg(
        日数=("short_profit_sum", "count"),
        利益合計=("short_profit_sum", "sum"),
        利益平均=("short_profit_sum", "mean"),
    ).round(0)

    total_profit = df_daily["short_profit_sum"].sum()
    total_days = len(df_daily)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>先物5分足 × Grokショート利益 相関分析</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 20px;
            background: #f5f5f5;
        }}
        h1 {{ color: #333; border-bottom: 2px solid #333; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        .info-box {{
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 15px;
            margin: 20px 0;
        }}
        .info-box h3 {{ margin-top: 0; color: #333; }}
        .info-box ul {{ margin: 0; padding-left: 20px; }}
        .info-box li {{ margin: 5px 0; }}
        .highlight {{ background: #fff3cd; padding: 2px 5px; border-radius: 3px; }}
        table {{
            border-collapse: collapse;
            width: 100%;
            background: #fff;
            margin: 20px 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 8px 12px;
            text-align: right;
        }}
        th {{
            background: #333;
            color: #fff;
            font-weight: 600;
        }}
        td:first-child, th:first-child {{ text-align: left; }}
        tr:nth-child(even) {{ background: #f9f9f9; }}
        tr:hover {{ background: #f0f0f0; }}
        .positive {{ color: #28a745; font-weight: 600; }}
        .negative {{ color: #dc3545; font-weight: 600; }}
        .warning {{ background: #fff3cd !important; }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .summary-card {{
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
        }}
        .summary-card .value {{
            font-size: 24px;
            font-weight: 700;
        }}
        .summary-card .label {{ color: #666; font-size: 14px; }}
    </style>
</head>
<body>
    <h1>先物5分足 × Grokショート利益 相関分析</h1>

    <div class="info-box">
        <h3>📊 データソース</h3>
        <ul>
            <li><strong>先物データ:</strong> <span class="highlight">futures_prices_60d_5m.parquet</span></li>
            <li><strong>ティッカー:</strong> NKD=F（CME日経225先物、USD建て）</li>
            <li><strong>タイムゾーン:</strong> <span class="highlight">JST（日本時間）に変換済み</span></li>
            <li><strong>取得タイミング:</strong> 23:00 JST パイプライン</li>
        </ul>
    </div>

    <div class="info-box">
        <h3>📐 計算方法</h3>
        <ul>
            <li><strong>先物変動率:</strong> (当日23:00価格 - 前営業日23:00価格) / 前営業日23:00価格 × 100</li>
            <li><strong>ショート利益:</strong> Grok推奨銘柄の大引けショート利益（符号反転）</li>
            <li><strong>対象期間:</strong> 2025-11-04 以降</li>
        </ul>
    </div>

    <h2>📈 先物変動率別 ショート利益サマリー</h2>
    <table>
        <tr>
            <th>先物変動率</th>
            <th>日数</th>
            <th>利益合計</th>
            <th>利益平均</th>
        </tr>
"""

    for idx, row in range_summary.iterrows():
        profit_class = "positive" if row["利益合計"] > 0 else "negative"
        html += f"""
        <tr>
            <td>{idx}</td>
            <td>{int(row['日数'])}</td>
            <td class="{profit_class}">{int(row['利益合計']):+,}円</td>
            <td class="{profit_class}">{int(row['利益平均']):+,}円</td>
        </tr>
"""

    html += f"""
    </table>

    <div class="summary">
        <div class="summary-card">
            <div class="value {'positive' if total_profit > 0 else 'negative'}">{int(total_profit):+,}円</div>
            <div class="label">総利益（{total_days}日）</div>
        </div>
    </div>

    <h2>📅 日別データ</h2>
    <table>
        <tr>
            <th>日付</th>
            <th>曜日</th>
            <th>前日23:00価格</th>
            <th>当日23:00価格</th>
            <th>先物変動率</th>
            <th>銘柄数</th>
            <th>ショート利益</th>
        </tr>
"""

    weekday_map = {0: "月", 1: "火", 2: "水", 3: "木", 4: "金", 5: "土", 6: "日"}

    for _, row in df_daily.iterrows():
        date_str = row["date"].strftime("%Y-%m-%d")
        weekday = weekday_map[row["date"].weekday()]

        warning_class = "warning" if row["futures_change_pct"] >= 1 else ""
        profit_class = "positive" if row["short_profit_sum"] > 0 else "negative"
        change_class = "positive" if row["futures_change_pct"] < 0 else ("negative" if row["futures_change_pct"] >= 1 else "")

        prev_price = f"{row['prev_price_2300']:,.0f}" if pd.notna(row.get('prev_price_2300')) else "-"
        curr_price = f"{row['price_2300']:,.0f}" if pd.notna(row.get('price_2300')) else "-"
        change_pct = f"{row['futures_change_pct']:+.2f}%" if pd.notna(row.get('futures_change_pct')) else "-"

        html += f"""
        <tr class="{warning_class}">
            <td>{date_str}</td>
            <td>{weekday}</td>
            <td>{prev_price}</td>
            <td>{curr_price}</td>
            <td class="{change_class}">{change_pct}</td>
            <td>{int(row['stock_count'])}</td>
            <td class="{profit_class}">{int(row['short_profit_sum']):+,}円</td>
        </tr>
"""

    html += f"""
    </table>

    <div class="info-box">
        <h3>🔍 分析ポイント</h3>
        <ul>
            <li><span style="background: #fff3cd; padding: 2px 5px;">黄色行</span>: 先物変動 +1%以上（見送り候補）</li>
            <li><span class="positive">緑字</span>: プラス / <span class="negative">赤字</span>: マイナス</li>
            <li>先物+1%以上の日のショート利益を確認 → 見送りルールの有効性</li>
        </ul>
    </div>

    <p style="color: #999; font-size: 12px; margin-top: 40px;">
        生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    </p>
</body>
</html>
"""
    return html


def main():
    print("=" * 60)
    print("先物5分足 × Grokショート利益 相関分析")
    print("=" * 60)

    # データ読み込み
    df_5m = load_futures_5m()
    df_grok = load_grok()

    print(f"\n先物5分足: {len(df_5m):,}件")
    print(f"期間: {df_5m['date'].min()} ~ {df_5m['date'].max()}")

    # 23:00の価格を抽出
    df_2300 = extract_2300_prices(df_5m)
    print(f"\n23:00価格データ: {len(df_2300)}日分")

    # 前営業日の価格を追加
    df_2300 = df_2300.sort_values("date")
    df_2300["prev_price_2300"] = df_2300["price_2300"].shift(1)
    df_2300["futures_change_pct"] = (df_2300["price_2300"] - df_2300["prev_price_2300"]) / df_2300["prev_price_2300"] * 100

    # Grokデータ準備
    df_grok_prep = prepare_grok(df_grok)
    print(f"Grok（フィルタ後）: {len(df_grok_prep)}件")

    # 日次集計
    grok_daily = df_grok_prep.groupby("date").agg(
        stock_count=("ticker", "count"),
        short_profit_sum=("short_p2", "sum"),
    ).reset_index()

    # マージ
    df_2300["date"] = pd.to_datetime(df_2300["date"]).dt.normalize()
    grok_daily["date"] = pd.to_datetime(grok_daily["date"]).dt.normalize()

    df_daily = grok_daily.merge(df_2300, on="date", how="left")
    df_daily = df_daily[df_daily["futures_change_pct"].notna()]
    df_daily = df_daily.sort_values("date")

    print(f"\n日別データ: {len(df_daily)}日")

    # HTML生成
    html = generate_html(df_daily)

    output_file = OUTPUT_DIR / "futures_grok_correlation.html"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✓ HTML出力: {output_file}")

    # コンソールにサマリー表示
    print("\n" + "=" * 60)
    print("先物変動率別 ショート利益サマリー")
    print("=" * 60)

    bins = [-np.inf, -1, 0, 1, 2, np.inf]
    labels = ["<-1%", "-1~0%", "0~+1%", "+1~+2%", ">+2%"]
    df_daily["change_range"] = pd.cut(df_daily["futures_change_pct"], bins=bins, labels=labels)

    summary = df_daily.groupby("change_range", observed=True).agg(
        日数=("short_profit_sum", "count"),
        利益合計=("short_profit_sum", "sum"),
        利益平均=("short_profit_sum", "mean"),
    ).round(0)

    print(summary.to_string())

    print(f"\n総利益: {int(df_daily['short_profit_sum'].sum()):+,}円")


if __name__ == "__main__":
    main()
