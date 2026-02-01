"""
イグジット時間帯分析 - 4区分版

5分足データ使用
時間区分:
- 前場前半: 9:00-10:30
- 前場引け: 10:30-11:30
- 後場前半: 12:30-14:00
- 大引け: 14:00-15:00

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

# 4区分
FOUR_SEGMENTS = [
    ("前場前半", "09:00", "10:25"),   # 9:00-10:30
    ("前場引け", "10:30", "11:25"),   # 10:30-11:30
    ("後場前半", "12:30", "13:55"),   # 12:30-14:00
    ("大引け", "14:00", "14:55"),     # 14:00-15:00
]

SEGMENT_NAMES = [s[0] for s in FOUR_SEGMENTS] + ["引成"]

WEEKDAYS = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日"]
MARGIN_TYPES = ["制度信用", "いちにち信用"]


def load_data():
    """データ読み込み"""
    prices = pd.read_parquet(BACKTEST_DIR / "grok_5m_60d_20251230.parquet")
    prices["datetime"] = pd.to_datetime(prices["datetime"]).dt.tz_convert("Asia/Tokyo")
    prices["date"] = prices["datetime"].dt.date
    prices["time"] = prices["datetime"].dt.strftime("%H:%M")
    prices = prices.rename(columns={"close": "Close"})

    archive = pd.read_parquet(BACKTEST_DIR / "grok_trending_archive.parquet")
    archive["selection_date"] = pd.to_datetime(archive["selection_date"])
    archive = archive[archive["selection_date"] >= "2025-11-04"]
    archive = archive[archive["buy_price"].notna()]
    archive = archive[
        (archive["shortable"] == True) |
        ((archive["day_trade"] == True) & (archive["shortable"] == False))
    ]

    # 除0フラグ (いちにち信用で0株を除外)
    archive["is_ex0"] = archive.apply(
        lambda r: True if r["shortable"] else (
            pd.isna(r.get("day_trade_available_shares")) or r.get("day_trade_available_shares", 0) > 0
        ),
        axis=1
    )

    return prices, archive


def get_price_range(price):
    for name, low, high in PRICE_RANGES:
        if low <= price < high:
            return name
    return "10,000円~"


def get_slot_close(day_data, start_time, end_time):
    slot_data = day_data[(day_data["time"] >= start_time) & (day_data["time"] <= end_time)]
    return slot_data.iloc[-1]["Close"] if len(slot_data) > 0 else None


def calc_adjustment_factor(day_data, daily_close):
    if pd.isna(daily_close) or daily_close <= 0:
        return 1.0
    last_data = day_data[day_data["time"] >= "14:55"]
    if len(last_data) == 0:
        last_data = day_data[day_data["time"] >= "14:30"]
    if len(last_data) > 0:
        last_close = last_data.iloc[-1]["Close"]
        if last_close > 0:
            ratio = daily_close / last_close
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
        is_ex0 = row.get("is_ex0", True)

        day_data = prices[
            (prices["ticker"] == ticker) &
            (prices["date"] == sel_date)
        ].copy()

        if len(day_data) == 0:
            continue

        adjustment = calc_adjustment_factor(day_data, daily_close)
        margin_type = "制度信用" if shortable else "いちにち信用"

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
            "is_ex0": is_ex0,
        }

        # 4区分の損益計算
        for seg_name, start_time, end_time in FOUR_SEGMENTS:
            exit_price = get_slot_close(day_data, start_time, end_time)
            if exit_price is not None:
                exit_price_adj = exit_price * adjustment
                profit = (entry_price - exit_price_adj) * 100
                result[f"profit_{seg_name}"] = profit
            else:
                result[f"profit_{seg_name}"] = np.nan

        # 引成 (archiveから)
        if not pd.isna(daily_close) and daily_close > 0:
            result["profit_引成"] = (entry_price - daily_close) * 100
        else:
            result["profit_引成"] = np.nan

        results.append(result)

    return pd.DataFrame(results)


def aggregate_by_segment(df, weekday, margin_type, ex0_only=False):
    """曜日・信用区分ごとに価格帯別集計"""
    seg_df = df[(df["weekday"] == weekday) & (df["margin_type"] == margin_type)]
    if ex0_only:
        seg_df = seg_df[seg_df["is_ex0"] == True]

    results = []
    for pr_name, pr_low, pr_high in PRICE_RANGES:
        pr_df = seg_df[seg_df["price_range"] == pr_name]
        count = len(pr_df)

        if count == 0:
            continue

        row = {"price_range": pr_name, "count": count}

        for seg_name in SEGMENT_NAMES:
            col = f"profit_{seg_name}"
            if col in pr_df.columns:
                profits = pr_df[col].dropna()
                if len(profits) > 0:
                    row[f"{seg_name}_profit"] = profits.sum()
                    row[f"{seg_name}_win_rate"] = (profits > 0).sum() / len(profits) * 100
                    row[f"{seg_name}_count"] = len(profits)
                else:
                    row[f"{seg_name}_profit"] = 0
                    row[f"{seg_name}_win_rate"] = 0
                    row[f"{seg_name}_count"] = 0

        results.append(row)

    return pd.DataFrame(results)


def generate_html(df):
    """HTML生成"""
    # 全体集計
    total_profits = {}
    total_wins = {}
    for seg_name in SEGMENT_NAMES:
        col = f"profit_{seg_name}"
        if col in df.columns:
            profits = df[col].dropna()
            total_profits[seg_name] = profits.sum()
            total_wins[seg_name] = (profits > 0).sum() / len(profits) * 100 if len(profits) > 0 else 0

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Grok分析 - イグジット4区分</title>
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
        h1 {{ font-size: 1.75rem; margin-bottom: 0.5rem; }}
        .subtitle {{ color: var(--text-muted); font-size: 1rem; margin-bottom: 1.5rem; }}
        .time-legend {{
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 1rem;
            margin-bottom: 1.5rem;
            display: flex;
            gap: 2rem;
            font-size: 0.95rem;
        }}
        .time-legend span {{ color: var(--text-muted); }}
        .summary {{
            display: flex;
            gap: 1rem;
            margin-bottom: 2rem;
        }}
        .summary-card {{
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 1rem 1.5rem;
            text-align: center;
            flex: 1;
        }}
        .summary-card .label {{ font-size: 0.9rem; color: var(--text-muted); }}
        .summary-card .value {{ font-size: 1.5rem; font-weight: bold; margin-top: 0.25rem; }}
        .summary-card .rate {{ font-size: 1rem; color: var(--text-muted); }}
        .positive {{ color: var(--positive-color); }}
        .negative {{ color: var(--negative-color); }}
        .weekday-section {{ margin-bottom: 2rem; }}
        .weekday-title {{ font-size: 1.25rem; font-weight: 600; margin-bottom: 1rem; }}
        .margin-card {{
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.25rem;
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
        .margin-title {{ font-weight: 600; font-size: 1.1rem; }}
        .margin-count {{ color: var(--text-muted); font-size: 1rem; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 1rem;
        }}
        th, td {{
            padding: 0.75rem 0.5rem;
            text-align: right;
            border-bottom: 1px solid var(--border-color);
        }}
        th {{ color: var(--text-muted); font-weight: 500; }}
        th:first-child, td:first-child {{ text-align: left; }}
        td:nth-child(2) {{ text-align: center; }}
        .best {{ background: rgba(34, 197, 94, 0.15); }}
        small {{ font-size: 0.85rem; }}
    </style>
</head>
<body>
    <h1>Grok分析 - イグジット4区分</h1>
    <p class="subtitle">生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')} | 総件数: {len(df)} | データソース: 5分足</p>

    <div class="time-legend">
        <div><strong>前場前半</strong> <span>9:00-10:30</span></div>
        <div><strong>前場引け</strong> <span>10:30-11:30</span></div>
        <div><strong>後場前半</strong> <span>12:30-14:00</span></div>
        <div><strong>大引け</strong> <span>14:00-15:00</span></div>
        <div><strong>引成</strong> <span>15:00確定</span></div>
    </div>

    <div class="summary">
"""

    # 全体サマリー
    for seg_name in SEGMENT_NAMES:
        profit = total_profits.get(seg_name, 0)
        win_rate = total_wins.get(seg_name, 0)
        profit_class = "positive" if profit > 0 else "negative"
        html += f"""
        <div class="summary-card">
            <div class="label">{seg_name}</div>
            <div class="value {profit_class}">{profit:+,.0f}円</div>
            <div class="rate">勝率 {win_rate:.1f}%</div>
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

            # いちにち信用の場合は除0も表示
            if margin == "いちにち信用":
                ex0_count = len(margin_df[margin_df["is_ex0"] == True])
                count_label = f"{count}件 (除0: {ex0_count}件)"
            else:
                count_label = f"{count}件"

            html += f"""
        <div class="margin-card">
            <div class="margin-header">
                <span class="margin-title">{margin}</span>
                <span class="margin-count">{count_label}</span>
            </div>
"""

            # 全件テーブル
            agg = aggregate_by_segment(df, weekday, margin, ex0_only=False)
            if len(agg) > 0:
                html += """
            <table>
                <thead>
                    <tr>
                        <th>価格帯</th>
                        <th>件</th>
"""
                for seg_name in SEGMENT_NAMES:
                    html += f"<th>{seg_name}</th>"
                html += """
                    </tr>
                </thead>
                <tbody>
"""
                for _, row in agg.iterrows():
                    max_profit = -float('inf')
                    max_seg = None
                    for seg_name in SEGMENT_NAMES:
                        p = row.get(f"{seg_name}_profit", 0)
                        if p > max_profit:
                            max_profit = p
                            max_seg = seg_name

                    html += f"""
                    <tr>
                        <td>{row['price_range']}</td>
                        <td>{row['count']}</td>
"""
                    for seg_name in SEGMENT_NAMES:
                        profit = row.get(f"{seg_name}_profit", 0)
                        win_rate = row.get(f"{seg_name}_win_rate", 0)
                        seg_count = row.get(f"{seg_name}_count", 0)
                        profit_class = "positive" if profit > 0 else "negative" if profit < 0 else ""
                        best_class = " best" if seg_name == max_seg and max_profit > 0 else ""
                        if seg_count > 0:
                            html += f'<td class="{profit_class}{best_class}">{profit:+,.0f}<br><small>{win_rate:.0f}%</small></td>'
                        else:
                            html += '<td>-</td>'
                    html += """
                    </tr>
"""
                html += """
                </tbody>
            </table>
"""

            # いちにち信用の場合は除0テーブルも表示
            if margin == "いちにち信用":
                agg_ex0 = aggregate_by_segment(df, weekday, margin, ex0_only=True)
                if len(agg_ex0) > 0:
                    html += """
            <div style="margin-top: 1rem; padding-top: 1rem; border-top: 1px dashed var(--border-color);">
                <div style="font-size: 0.9rem; color: var(--text-muted); margin-bottom: 0.5rem;">除0</div>
                <table>
                    <thead>
                        <tr>
                            <th>価格帯</th>
                            <th>件</th>
"""
                    for seg_name in SEGMENT_NAMES:
                        html += f"<th>{seg_name}</th>"
                    html += """
                        </tr>
                    </thead>
                    <tbody>
"""
                    for _, row in agg_ex0.iterrows():
                        max_profit = -float('inf')
                        max_seg = None
                        for seg_name in SEGMENT_NAMES:
                            p = row.get(f"{seg_name}_profit", 0)
                            if p > max_profit:
                                max_profit = p
                                max_seg = seg_name

                        html += f"""
                        <tr>
                            <td>{row['price_range']}</td>
                            <td>{row['count']}</td>
"""
                        for seg_name in SEGMENT_NAMES:
                            profit = row.get(f"{seg_name}_profit", 0)
                            win_rate = row.get(f"{seg_name}_win_rate", 0)
                            seg_count = row.get(f"{seg_name}_count", 0)
                            profit_class = "positive" if profit > 0 else "negative" if profit < 0 else ""
                            best_class = " best" if seg_name == max_seg and max_profit > 0 else ""
                            if seg_count > 0:
                                html += f'<td class="{profit_class}{best_class}">{profit:+,.0f}<br><small>{win_rate:.0f}%</small></td>'
                            else:
                                html += '<td>-</td>'
                        html += """
                        </tr>
"""
                    html += """
                    </tbody>
                </table>
            </div>
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
    print("=== イグジット時間帯分析 (4区分・5分足データ) ===")

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

    print("\n=== 全体サマリー ===")
    for seg_name in SEGMENT_NAMES:
        col = f"profit_{seg_name}"
        if col in df.columns:
            profits = df[col].dropna()
            total = profits.sum()
            win_rate = (profits > 0).sum() / len(profits) * 100 if len(profits) > 0 else 0
            print(f"{seg_name}: {total:+,.0f}円 (勝率{win_rate:.1f}%)")

    print("\nHTML生成中...")
    html = generate_html(df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "exit_timing_4segments.html"
    output_path.write_text(html, encoding="utf-8")

    print(f"\n完了: {output_path}")


if __name__ == "__main__":
    main()
