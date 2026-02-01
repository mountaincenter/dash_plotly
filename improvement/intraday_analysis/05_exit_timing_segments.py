"""
イグジット時間帯セグメント分析

yfinance 1時間足を使用
時間区分:
- 9-10時 (10:00終値)
- 前場引け (11:00終値 ※1時間足のため)
- 12:30-13:30 (14:00終値 ※1時間足のため)
- 13:30-14:30 (15:00終値)
- 大引け (daily_close from archive)

セグメント: 曜日 × 信用区分 × 価格帯

dev_analysis.py と同じロジックを使用:
- 2025-11-04以降のデータのみ
- shortable=True → 制度信用
- day_trade=True & shortable=False → いちにち信用
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import yfinance as yf
import time

BACKTEST_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "parquet" / "backtest"
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "output" / "intraday_analysis"

# 価格帯定義 (dev_analysis.pyと同じ)
PRICE_RANGES = [
    ("~1,000円", 0, 1000),
    ("1,000~3,000円", 1000, 3000),
    ("3,000~5,000円", 3000, 5000),
    ("5,000~10,000円", 5000, 10000),
    ("10,000円~", 10000, float("inf")),
]

# イグジット時間帯 (1時間足対応)
EXIT_SEGMENTS = ["9-10時", "前場引", "12:30-13:30", "13:30-14:30", "大引け"]

# 1時間足での対応時刻 (yfinance: interval=1h)
# 日本市場: 9:00-11:30, 12:30-15:00
# yfinanceの1時間足は hour=9 (9:00-10:00), hour=10 (10:00-11:00), ...
EXIT_HOUR_MAP = {
    "9-10時": 10,       # 10:00終値 = 9-10時足のclose
    "前場引": 11,       # 11:00終値 = 10-11時足のclose (前場は11:30まで)
    "12:30-13:30": 14,  # 14:00終値 = 13-14時足のclose
    "13:30-14:30": 15,  # 15:00終値 = 14-15時足のclose
}


def load_archive():
    """grok_trending_archive.parquet読み込み"""
    archive = pd.read_parquet(BACKTEST_DIR / "grok_trending_archive.parquet")
    archive["selection_date"] = pd.to_datetime(archive["selection_date"])

    # 2025-11-04以降のみ (dev_analysis.pyと同じ)
    archive = archive[archive["selection_date"] >= "2025-11-04"]

    # buy_priceがあるもののみ
    archive = archive[archive["buy_price"].notna()]

    # 制度信用 or いちにち信用のみ (dev_analysis.pyと同じロジック)
    archive = archive[
        (archive["shortable"] == True) |
        ((archive["day_trade"] == True) & (archive["shortable"] == False))
    ]

    return archive


def get_price_range(price):
    """価格帯取得"""
    for name, low, high in PRICE_RANGES:
        if low <= price < high:
            return name
    return "10,000円~"


def fetch_hourly_data(ticker: str, start_date: str, end_date: str) -> tuple[pd.DataFrame | None, dict]:
    """
    yfinanceで1時間足を取得

    period="730d" でエラーの場合は period="max" で再取得

    Returns:
        (DataFrame, splits_dict): 1時間足データと分割情報
        splits_dict: {date: split_ratio} 形式
    """
    # tickerは既に "XXXX.T" 形式
    yf_ticker = ticker
    splits_dict = {}

    try:
        # まず730d
        stock = yf.Ticker(yf_ticker)
        df = stock.history(period="730d", interval="1h")

        if df.empty:
            # fallback to max
            df = stock.history(period="max", interval="1h")

        if df.empty:
            return None, {}

        # 分割情報を取得
        try:
            splits = stock.splits
            if not splits.empty:
                for split_date, ratio in splits.items():
                    if split_date.tz is not None:
                        split_date = split_date.tz_convert("Asia/Tokyo")
                    splits_dict[split_date.date()] = ratio
        except:
            pass

        # タイムゾーン処理
        if df.index.tz is not None:
            df.index = df.index.tz_convert("Asia/Tokyo")

        df = df.reset_index()
        df = df.rename(columns={"Datetime": "datetime", "Date": "datetime"})

        if "datetime" not in df.columns:
            if df.index.name == "Datetime":
                df = df.reset_index()
                df = df.rename(columns={"Datetime": "datetime"})

        df["date"] = pd.to_datetime(df["datetime"]).dt.date
        df["hour"] = pd.to_datetime(df["datetime"]).dt.hour

        return df, splits_dict

    except Exception as e:
        print(f"  {ticker}: エラー - {e}")
        return None, {}


def calc_split_adjustment(sel_date, splits_dict: dict) -> float:
    """
    選定日に対する分割調整係数を計算

    yfinanceは分割調整後の価格を返すため、
    選定日より後に分割があった場合、yfinance価格 × 調整係数 = 実際の価格
    """
    adjustment = 1.0
    for split_date, ratio in splits_dict.items():
        if sel_date < split_date:
            # 選定日が分割日より前なら、yfinance価格は分割調整されている
            adjustment *= ratio
    return adjustment


def calc_exit_returns(archive: pd.DataFrame) -> pd.DataFrame:
    """各イグジット時間帯の損益を計算"""
    results = []
    tickers = archive["ticker"].unique()

    print(f"銘柄数: {len(tickers)}")

    # 全銘柄のyfinanceデータを取得
    hourly_cache = {}
    splits_cache = {}

    for i, ticker in enumerate(tickers):
        if (i + 1) % 10 == 0:
            print(f"  データ取得中... {i+1}/{len(tickers)}")

        hourly, splits = fetch_hourly_data(ticker, "2025-11-04", datetime.now().strftime("%Y-%m-%d"))
        if hourly is not None:
            hourly_cache[ticker] = hourly
            splits_cache[ticker] = splits

        # API制限対策
        if (i + 1) % 50 == 0:
            time.sleep(1)

    print(f"\n取得成功: {len(hourly_cache)}/{len(tickers)} 銘柄")

    # 各選定日の損益計算
    for _, row in archive.iterrows():
        ticker = row["ticker"]
        sel_date = row["selection_date"].date()
        entry_price = row["buy_price"]
        daily_close = row.get("daily_close")
        shortable = row.get("shortable", False)
        day_trade = row.get("day_trade", False)

        if ticker not in hourly_cache:
            continue

        hourly = hourly_cache[ticker]
        splits = splits_cache.get(ticker, {})
        day_data = hourly[hourly["date"] == sel_date]

        if len(day_data) == 0:
            continue

        # 分割調整係数
        adjustment = calc_split_adjustment(sel_date, splits)

        # 信用区分 (dev_analysis.pyと同じ)
        margin_type = "制度信用" if shortable else "いちにち信用"

        # 曜日
        weekday = pd.Timestamp(sel_date).day_name()
        weekday_jp = {
            "Monday": "月曜日", "Tuesday": "火曜日", "Wednesday": "水曜日",
            "Thursday": "木曜日", "Friday": "金曜日"
        }.get(weekday, weekday)

        price_range = get_price_range(entry_price)

        result = {
            "ticker": ticker,
            "date": sel_date,
            "weekday": weekday_jp,
            "margin_type": margin_type,
            "price_range": price_range,
            "entry_price": entry_price,
        }

        # 各時間帯の損益計算（ショート）
        for segment in EXIT_SEGMENTS[:-1]:  # 大引け以外
            target_hour = EXIT_HOUR_MAP.get(segment)
            if target_hour is None:
                continue

            hour_data = day_data[day_data["hour"] == target_hour]
            if len(hour_data) > 0:
                # yfinance価格を分割調整
                exit_price = hour_data["Close"].iloc[0] * adjustment
                # ショート損益 = (エントリー価格 - イグジット価格) × 100株
                profit = (entry_price - exit_price) * 100
                result[f"profit_{segment}"] = profit
            else:
                result[f"profit_{segment}"] = np.nan

        # 大引け (archiveから)
        if not pd.isna(daily_close) and daily_close > 0:
            result["profit_大引け"] = (entry_price - daily_close) * 100
        else:
            result["profit_大引け"] = np.nan

        results.append(result)

    return pd.DataFrame(results)


def aggregate_by_segment(df, weekday, margin_type):
    """曜日・信用区分ごとに価格帯別集計"""
    seg_df = df[(df["weekday"] == weekday) & (df["margin_type"] == margin_type)]

    results = []
    for pr_name, pr_low, pr_high in PRICE_RANGES:
        pr_df = seg_df[seg_df["price_range"] == pr_name]
        count = len(pr_df)

        if count == 0:
            continue

        row = {"price_range": pr_name, "count": count}

        for segment in EXIT_SEGMENTS:
            col = f"profit_{segment}"
            if col in pr_df.columns:
                profits = pr_df[col].dropna()
                if len(profits) > 0:
                    row[f"{segment}_profit"] = profits.sum()
                    row[f"{segment}_win_rate"] = (profits > 0).sum() / len(profits) * 100
                else:
                    row[f"{segment}_profit"] = 0
                    row[f"{segment}_win_rate"] = 0
            else:
                row[f"{segment}_profit"] = 0
                row[f"{segment}_win_rate"] = 0

        results.append(row)

    return pd.DataFrame(results)


def generate_html(df):
    """HTML生成 (1カード1行レイアウト)"""
    weekdays = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日"]
    margin_types = ["制度信用", "いちにち信用"]

    # 全体集計
    total_profits = {}
    total_wins = {}
    for segment in EXIT_SEGMENTS:
        col = f"profit_{segment}"
        if col in df.columns:
            profits = df[col].dropna()
            total_profits[segment] = profits.sum()
            total_wins[segment] = (profits > 0).sum() / len(profits) * 100 if len(profits) > 0 else 0

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Grok分析 - イグジット時間帯別</title>
    <style>
        :root {{
            --bg-color: #0a0a0a;
            --card-bg: #1a1a1a;
            --text-color: #e0e0e0;
            --text-muted: #888;
            --border-color: #333;
            --positive-color: #22c55e;
            --negative-color: #ef4444;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: var(--bg-color);
            color: var(--text-color);
            padding: 2rem;
            line-height: 1.5;
        }}
        h1 {{ font-size: 1.5rem; margin-bottom: 0.5rem; }}
        .subtitle {{ color: var(--text-muted); font-size: 0.875rem; margin-bottom: 2rem; }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 1rem;
            margin-bottom: 2rem;
        }}
        .summary-card {{
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 1rem;
            text-align: center;
        }}
        .summary-card .label {{ font-size: 0.75rem; color: var(--text-muted); }}
        .summary-card .value {{ font-size: 1.25rem; font-weight: bold; margin-top: 0.25rem; }}
        .summary-card .rate {{ font-size: 0.875rem; color: var(--text-muted); }}
        .positive {{ color: var(--positive-color); }}
        .negative {{ color: var(--negative-color); }}
        .weekday-section {{ margin-bottom: 2rem; }}
        .weekday-title {{ font-size: 1.125rem; font-weight: 600; margin-bottom: 1rem; }}
        .margin-card {{
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 1rem;
        }}
        .margin-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid var(--border-color);
        }}
        .margin-title {{ font-weight: 600; }}
        .margin-count {{ color: var(--text-muted); font-size: 0.875rem; }}
        .totals {{
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 0.5rem;
            margin-bottom: 1rem;
            text-align: center;
            font-size: 0.875rem;
        }}
        .totals .segment-label {{ color: var(--text-muted); font-size: 0.75rem; }}
        .totals .segment-value {{ font-weight: bold; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.8rem;
        }}
        th, td {{
            padding: 0.5rem;
            text-align: right;
            border-bottom: 1px solid var(--border-color);
        }}
        th {{ color: var(--text-muted); font-weight: 500; }}
        th:first-child, td:first-child {{ text-align: left; }}
        td:nth-child(2) {{ text-align: center; }}
    </style>
</head>
<body>
    <h1>Grok分析 - イグジット時間帯別</h1>
    <p class="subtitle">生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')} | 総件数: {len(df)} | データソース: yfinance 1時間足</p>

    <div class="summary">
"""

    # 全体サマリー
    for segment in EXIT_SEGMENTS:
        profit = total_profits.get(segment, 0)
        win_rate = total_wins.get(segment, 0)
        profit_class = "positive" if profit > 0 else "negative"
        html += f"""
        <div class="summary-card">
            <div class="label">{segment}</div>
            <div class="value {profit_class}">{profit:+,.0f}円</div>
            <div class="rate">勝率 {win_rate:.1f}%</div>
        </div>
"""

    html += """
    </div>
"""

    # 曜日別セクション (1カード1行)
    for weekday in weekdays:
        weekday_df = df[df["weekday"] == weekday]
        if len(weekday_df) == 0:
            continue

        html += f"""
    <div class="weekday-section">
        <div class="weekday-title">{weekday}</div>
"""

        for margin in margin_types:
            margin_df = weekday_df[weekday_df["margin_type"] == margin]
            count = len(margin_df)

            # 信用区分ごとの合計
            margin_totals = {}
            for segment in EXIT_SEGMENTS:
                col = f"profit_{segment}"
                if col in margin_df.columns:
                    margin_totals[segment] = margin_df[col].dropna().sum()
                else:
                    margin_totals[segment] = 0

            html += f"""
        <div class="margin-card">
            <div class="margin-header">
                <span class="margin-title">{margin}</span>
                <span class="margin-count">{count}件</span>
            </div>
            <div class="totals">
"""
            for segment in EXIT_SEGMENTS:
                profit = margin_totals.get(segment, 0)
                profit_class = "positive" if profit > 0 else "negative"
                html += f"""
                <div>
                    <div class="segment-label">{segment}</div>
                    <div class="segment-value {profit_class}">{profit:+,.0f}</div>
                </div>
"""
            html += """
            </div>
"""

            # 価格帯別テーブル
            agg = aggregate_by_segment(df, weekday, margin)
            if len(agg) > 0:
                html += """
            <table>
                <thead>
                    <tr>
                        <th>価格帯</th>
                        <th>件</th>
"""
                for segment in EXIT_SEGMENTS:
                    html += f"<th>{segment}</th>"
                html += """
                    </tr>
                </thead>
                <tbody>
"""
                for _, row in agg.iterrows():
                    html += f"""
                    <tr>
                        <td>{row['price_range']}</td>
                        <td>{row['count']}</td>
"""
                    for segment in EXIT_SEGMENTS:
                        profit = row.get(f"{segment}_profit", 0)
                        win_rate = row.get(f"{segment}_win_rate", 0)
                        profit_class = "positive" if profit > 0 else "negative" if profit < 0 else ""
                        html += f'<td class="{profit_class}">{profit:+,.0f}<br><small>{win_rate:.0f}%</small></td>'
                    html += """
                    </tr>
"""
                html += """
                </tbody>
            </table>
"""
            html += """
        </div>
"""

        html += """
    </div>
"""

    html += """
</body>
</html>
"""

    return html


def main():
    print("=== イグジット時間帯セグメント分析 (yfinance 1時間足) ===")

    print("\nアーカイブデータ読み込み中...")
    archive = load_archive()
    print(f"選定: {len(archive)}件 (2025-11-04以降)")

    print("\nyfinance 1時間足取得 & 損益計算中...")
    df = calc_exit_returns(archive)
    print(f"分析対象: {len(df)}件")

    if len(df) == 0:
        print("分析対象データがありません")
        return

    print("\n=== 全体サマリー ===")
    for segment in EXIT_SEGMENTS:
        col = f"profit_{segment}"
        if col in df.columns:
            profits = df[col].dropna()
            total = profits.sum()
            win_rate = (profits > 0).sum() / len(profits) * 100 if len(profits) > 0 else 0
            print(f"{segment}: {total:+,.0f}円 (勝率{win_rate:.1f}%)")

    print("\nHTML生成中...")
    html = generate_html(df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "exit_timing_segments.html"
    output_path.write_text(html, encoding="utf-8")

    print(f"\n完了: {output_path}")


if __name__ == "__main__":
    main()
