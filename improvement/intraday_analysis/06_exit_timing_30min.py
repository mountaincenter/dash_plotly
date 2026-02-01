"""
イグジット時間帯分析 - 30分刻み版

5分足データ (grok_5m_60d_20251230.parquet) を使用
時間帯: 30分刻み × 10スロット

セグメント: 曜日 × 信用区分 × 価格帯
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

BACKTEST_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "parquet" / "backtest"
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "output" / "intraday_analysis"

# 価格帯定義
PRICE_RANGES = [
    ("~1,000円", 0, 1000),
    ("1,000~3,000円", 1000, 3000),
    ("3,000~5,000円", 3000, 5000),
    ("5,000~10,000円", 5000, 10000),
    ("10,000円~", 10000, float("inf")),
]

# 30分刻み時間帯
TIME_SLOTS = [
    ("9:00-9:30", "09:00", "09:25"),
    ("9:30-10:00", "09:30", "09:55"),
    ("10:00-10:30", "10:00", "10:25"),
    ("10:30-11:00", "10:30", "10:55"),
    ("11:00-11:30", "11:00", "11:25"),
    ("12:30-13:00", "12:30", "12:55"),
    ("13:00-13:30", "13:00", "13:25"),
    ("13:30-14:00", "13:30", "13:55"),
    ("14:00-14:30", "14:00", "14:25"),
    ("14:30-15:00", "14:30", "14:55"),
]

WEEKDAYS = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日"]
MARGIN_TYPES = ["制度信用", "いちにち信用"]


def load_data():
    """データ読み込み"""
    # 5分足データ
    prices = pd.read_parquet(BACKTEST_DIR / "grok_5m_60d_20251230.parquet")
    prices["datetime"] = pd.to_datetime(prices["datetime"]).dt.tz_convert("Asia/Tokyo")
    prices["date"] = prices["datetime"].dt.date
    prices["time"] = prices["datetime"].dt.strftime("%H:%M")
    prices = prices.rename(columns={
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "volume": "Volume"
    })

    # アーカイブ
    archive = pd.read_parquet(BACKTEST_DIR / "grok_trending_archive.parquet")
    archive["selection_date"] = pd.to_datetime(archive["selection_date"])

    # 2025-11-04以降
    archive = archive[archive["selection_date"] >= "2025-11-04"]
    archive = archive[archive["buy_price"].notna()]
    archive = archive[
        (archive["shortable"] == True) |
        ((archive["day_trade"] == True) & (archive["shortable"] == False))
    ]

    return prices, archive


def get_price_range(price):
    """価格帯取得"""
    for name, low, high in PRICE_RANGES:
        if low <= price < high:
            return name
    return "10,000円~"


def get_slot_close(day_data, start_time, end_time):
    """
    時間帯の最後の終値を取得
    例: 9:00-9:30 → 9:25の終値
    """
    slot_data = day_data[
        (day_data["time"] >= start_time) &
        (day_data["time"] <= end_time)
    ]
    if len(slot_data) > 0:
        return slot_data.iloc[-1]["Close"]
    return None


def calc_adjustment_factor(day_data, daily_close):
    """
    5分足データの分割調整係数を計算

    5分足データが分割調整済みの場合、archiveのdaily_closeと比較して係数算出
    調整係数 = daily_close / 5分足の終値
    """
    if pd.isna(daily_close) or daily_close <= 0:
        return 1.0

    # 当日の最後の終値を取得 (14:55-15:20あたり)
    last_data = day_data[day_data["time"] >= "14:55"]
    if len(last_data) == 0:
        last_data = day_data[day_data["time"] >= "14:30"]

    if len(last_data) > 0:
        last_close = last_data.iloc[-1]["Close"]
        if last_close > 0:
            ratio = daily_close / last_close
            # 分割調整が必要な場合 (1.5倍以上または0.7倍以下)
            if ratio > 1.5 or ratio < 0.7:
                return ratio
    return 1.0


def calc_all_returns(prices, archive):
    """全データの損益を計算"""
    results = []

    for _, row in archive.iterrows():
        ticker = row["ticker"]
        sel_date = row["selection_date"].date()
        entry_price = row["buy_price"]
        daily_close = row.get("daily_close")
        shortable = row.get("shortable", False)

        # 5分足データ取得
        day_data = prices[
            (prices["ticker"] == ticker) &
            (prices["date"] == sel_date)
        ].copy()

        if len(day_data) == 0:
            continue

        # 分割調整係数を計算
        adjustment = calc_adjustment_factor(day_data, daily_close)

        # 信用区分
        margin_type = "制度信用" if shortable else "いちにち信用"

        # 曜日
        weekday = pd.Timestamp(sel_date).weekday()
        weekday_jp = WEEKDAYS[weekday] if weekday < 5 else None
        if weekday_jp is None:
            continue

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
        for slot_name, start_time, end_time in TIME_SLOTS:
            exit_price = get_slot_close(day_data, start_time, end_time)
            if exit_price is not None:
                # 分割調整を適用
                exit_price_adjusted = exit_price * adjustment
                profit = (entry_price - exit_price_adjusted) * 100
                result[f"profit_{slot_name}"] = profit
            else:
                result[f"profit_{slot_name}"] = np.nan

        # 大引け (archiveから確実な値を使用)
        if not pd.isna(daily_close) and daily_close > 0:
            result["profit_大引け"] = (entry_price - daily_close) * 100
        else:
            result["profit_大引け"] = np.nan

        results.append(result)

    return pd.DataFrame(results)


def aggregate_by_segment(df, weekday, margin_type):
    """曜日・信用区分ごとに価格帯別集計"""
    seg_df = df[(df["weekday"] == weekday) & (df["margin_type"] == margin_type)]

    slot_names = [s[0] for s in TIME_SLOTS] + ["大引け"]

    results = []
    for pr_name, pr_low, pr_high in PRICE_RANGES:
        pr_df = seg_df[seg_df["price_range"] == pr_name]
        count = len(pr_df)

        if count == 0:
            continue

        row = {"price_range": pr_name, "count": count}

        for slot_name in slot_names:
            col = f"profit_{slot_name}"
            if col in pr_df.columns:
                profits = pr_df[col].dropna()
                if len(profits) > 0:
                    row[f"{slot_name}_profit"] = profits.sum()
                    row[f"{slot_name}_win_rate"] = (profits > 0).sum() / len(profits) * 100
                    row[f"{slot_name}_count"] = len(profits)
                else:
                    row[f"{slot_name}_profit"] = 0
                    row[f"{slot_name}_win_rate"] = 0
                    row[f"{slot_name}_count"] = 0

        results.append(row)

    return pd.DataFrame(results)


def generate_html(df):
    """HTML生成"""
    slot_names = [s[0] for s in TIME_SLOTS] + ["大引け"]

    # 全体集計
    total_profits = {}
    total_wins = {}
    for slot_name in slot_names:
        col = f"profit_{slot_name}"
        if col in df.columns:
            profits = df[col].dropna()
            total_profits[slot_name] = profits.sum()
            total_wins[slot_name] = (profits > 0).sum() / len(profits) * 100 if len(profits) > 0 else 0

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Grok分析 - イグジット時間帯別 (30分刻み)</title>
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
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            margin-bottom: 2rem;
        }}
        .summary-card {{
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            padding: 0.5rem 0.75rem;
            text-align: center;
            min-width: 80px;
        }}
        .summary-card .label {{ font-size: 0.65rem; color: var(--text-muted); }}
        .summary-card .value {{ font-size: 0.9rem; font-weight: bold; }}
        .summary-card .rate {{ font-size: 0.7rem; color: var(--text-muted); }}
        .positive {{ color: var(--positive-color); }}
        .negative {{ color: var(--negative-color); }}
        .weekday-section {{ margin-bottom: 2rem; }}
        .weekday-title {{ font-size: 1.125rem; font-weight: 600; margin-bottom: 1rem; }}
        .margin-card {{
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1rem;
            margin-bottom: 1rem;
            overflow-x: auto;
        }}
        .margin-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 0.75rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid var(--border-color);
        }}
        .margin-title {{ font-weight: 600; }}
        .margin-count {{ color: var(--text-muted); font-size: 0.875rem; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.7rem;
            white-space: nowrap;
        }}
        th, td {{
            padding: 0.4rem 0.3rem;
            text-align: right;
            border-bottom: 1px solid var(--border-color);
        }}
        th {{ color: var(--text-muted); font-weight: 500; font-size: 0.65rem; }}
        th:first-child, td:first-child {{ text-align: left; }}
        td:nth-child(2) {{ text-align: center; }}
        .slot-header {{ writing-mode: vertical-rl; text-orientation: mixed; }}
    </style>
</head>
<body>
    <h1>Grok分析 - イグジット時間帯別 (30分刻み)</h1>
    <p class="subtitle">生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')} | 総件数: {len(df)} | データソース: 5分足</p>

    <div class="summary">
"""

    # 全体サマリー
    for slot_name in slot_names:
        profit = total_profits.get(slot_name, 0)
        win_rate = total_wins.get(slot_name, 0)
        profit_class = "positive" if profit > 0 else "negative"
        html += f"""
        <div class="summary-card">
            <div class="label">{slot_name}</div>
            <div class="value {profit_class}">{profit/10000:+,.1f}万</div>
            <div class="rate">{win_rate:.0f}%</div>
        </div>
"""

    html += """
    </div>
"""

    # 曜日別セクション
    for weekday in WEEKDAYS:
        weekday_df = df[df["weekday"] == weekday]
        if len(weekday_df) == 0:
            continue

        html += f"""
    <div class="weekday-section">
        <div class="weekday-title">{weekday}</div>
"""

        for margin in MARGIN_TYPES:
            margin_df = weekday_df[weekday_df["margin_type"] == margin]
            count = len(margin_df)

            html += f"""
        <div class="margin-card">
            <div class="margin-header">
                <span class="margin-title">{margin}</span>
                <span class="margin-count">{count}件</span>
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
                for slot_name in slot_names:
                    html += f"<th>{slot_name}</th>"
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
                    for slot_name in slot_names:
                        profit = row.get(f"{slot_name}_profit", 0)
                        win_rate = row.get(f"{slot_name}_win_rate", 0)
                        slot_count = row.get(f"{slot_name}_count", 0)
                        profit_class = "positive" if profit > 0 else "negative" if profit < 0 else ""
                        if slot_count > 0:
                            html += f'<td class="{profit_class}">{profit/1000:+,.0f}k<br><small>{win_rate:.0f}%</small></td>'
                        else:
                            html += '<td>-</td>'
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
    print("=== イグジット時間帯分析 (30分刻み・5分足データ) ===")

    print("\nデータ読み込み中...")
    prices, archive = load_data()
    print(f"5分足データ: {len(prices)}行")
    print(f"アーカイブ: {len(archive)}件 (2025-11-04以降)")

    print("\n損益計算中...")
    df = calc_all_returns(prices, archive)
    print(f"分析対象: {len(df)}件")

    if len(df) == 0:
        print("分析対象データがありません")
        return

    slot_names = [s[0] for s in TIME_SLOTS] + ["大引け"]

    print("\n=== 全体サマリー ===")
    for slot_name in slot_names:
        col = f"profit_{slot_name}"
        if col in df.columns:
            profits = df[col].dropna()
            total = profits.sum()
            win_rate = (profits > 0).sum() / len(profits) * 100 if len(profits) > 0 else 0
            print(f"{slot_name}: {total:+,.0f}円 (勝率{win_rate:.1f}%)")

    print("\nHTML生成中...")
    html = generate_html(df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "exit_timing_30min.html"
    output_path.write_text(html, encoding="utf-8")

    print(f"\n完了: {output_path}")


if __name__ == "__main__":
    main()
