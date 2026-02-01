#!/usr/bin/env python3
"""
11時間区分別の利益分析スクリプト

時間区分:
  -9:30, 9:30-10:00, 10:00-10:30, 10:30-11:00, 11:00-11:30 (前場)
  12:30-13:00, 13:00-13:30, 13:30-14:00, 14:00-14:30, 14:30-15:00, 15:00-15:30 (後場)

価格取得ルール:
  - 15:30 = daily_close from archive
  - 11:30 = sell_price from archive (前場引け)
  - その他 = 5分足Close（なければ次の時点）

利益計算:
  (buy_price - 各時間の価格) * 100 (ショート視点)

集計軸:
  - 曜日別
  - 信用区分別（制度/いちにち/いちにち(除0)）
  - 価格帯別
  - 時間区分別
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, time

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "parquet" / "backtest"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 時間区分の定義
TIME_SEGMENTS = [
    ("-9:30", time(9, 30)),     # 9:30時点の価格
    ("9:30-10:00", time(10, 0)),
    ("10:00-10:30", time(10, 30)),
    ("10:30-11:00", time(11, 0)),
    ("11:00-11:30", time(11, 30)),  # sell_price from archive
    ("12:30-13:00", time(13, 0)),
    ("13:00-13:30", time(13, 30)),
    ("13:30-14:00", time(14, 0)),
    ("14:00-14:30", time(14, 30)),
    ("14:30-15:00", time(15, 0)),
    ("15:00-15:30", time(15, 30)),  # daily_close from archive
]

# 価格帯の定義
PRICE_RANGES = [
    ("500円未満", 0, 500),
    ("500-1000円", 500, 1000),
    ("1000-2000円", 1000, 2000),
    ("2000-3000円", 2000, 3000),
    ("3000-5000円", 3000, 5000),
    ("5000円以上", 5000, float("inf")),
]

# 曜日の定義
WEEKDAYS = ["月", "火", "水", "木", "金"]
WEEKDAY_MAP = {0: "月", 1: "火", 2: "水", 3: "木", 4: "金"}

def load_data():
    """データ読み込み"""
    print("データ読み込み中...")

    # Archive読み込み
    archive_path = DATA_DIR / "grok_trending_archive.parquet"
    archive = pd.read_parquet(archive_path)
    print(f"  Archive: {len(archive)}件")

    # 2025-11-04以降をフィルタ
    archive["backtest_date"] = pd.to_datetime(archive["backtest_date"])
    archive = archive[archive["backtest_date"] >= "2025-11-04"].copy()
    print(f"  Archive (2025-11-04以降): {len(archive)}件")

    # 5分足データ読み込み
    m5_files = [
        DATA_DIR / "grok_5m_60d_20251230.parquet",
        DATA_DIR / "grok_5m_60d_20260130.parquet",
    ]

    m5_dfs = []
    for f in m5_files:
        if f.exists():
            df = pd.read_parquet(f)
            print(f"  5分足: {f.name} - {len(df)}件")
            m5_dfs.append(df)

    m5 = pd.concat(m5_dfs, ignore_index=True)
    # タイムゾーン処理
    m5["datetime"] = pd.to_datetime(m5["datetime"], utc=True)
    # JSTに変換してタイムゾーン情報を削除
    m5["datetime"] = m5["datetime"].dt.tz_convert("Asia/Tokyo").dt.tz_localize(None)
    # 重複除去
    m5 = m5.drop_duplicates(subset=["datetime", "ticker"])
    print(f"  5分足合計（重複除去後）: {len(m5)}件")

    return archive, m5


def get_price_range_label(price):
    """価格帯ラベルを取得"""
    for label, low, high in PRICE_RANGES:
        if low <= price < high:
            return label
    return "5000円以上"


def get_price_at_time(m5_ticker, target_date, target_time):
    """
    指定日時の価格を取得
    該当時刻がなければ次の時点のCloseを返す
    """
    if m5_ticker.empty:
        return None

    # 指定日のデータをフィルタ
    day_data = m5_ticker[m5_ticker["datetime"].dt.date == target_date]
    if day_data.empty:
        return None

    # 指定時刻以降のデータを取得
    target_dt = pd.Timestamp(datetime.combine(target_date, target_time))
    after_target = day_data[day_data["datetime"] >= target_dt]

    if after_target.empty:
        return None

    # 最初のCloseを返す
    return after_target.iloc[0]["close"]


def calculate_profits(archive, m5):
    """各時間区分の利益を計算"""
    print("\n利益計算中...")

    results = []
    m5_grouped = m5.groupby("ticker")

    total = len(archive)
    for idx, row in archive.iterrows():
        if idx % 100 == 0:
            print(f"  {idx}/{total}")

        ticker = row["ticker"]
        backtest_date = row["backtest_date"].date()
        buy_price = row["buy_price"]
        sell_price = row["sell_price"]  # 前場引け
        daily_close = row["daily_close"]  # 大引け

        # 5分足データ取得
        if ticker in m5_grouped.groups:
            m5_ticker = m5_grouped.get_group(ticker)
        else:
            m5_ticker = pd.DataFrame()

        # 各時間区分の利益を計算
        time_profits = {}
        for segment_name, segment_time in TIME_SEGMENTS:
            if segment_name == "11:00-11:30":
                # 前場引け = sell_price
                price = sell_price
            elif segment_name == "15:00-15:30":
                # 大引け = daily_close
                price = daily_close
            else:
                # 5分足から取得
                price = get_price_at_time(m5_ticker, backtest_date, segment_time)

            if price is not None and not pd.isna(price):
                # ショート利益 = (売り建値 - 決済価格) * 100
                profit = (buy_price - price) * 100
                time_profits[segment_name] = profit
            else:
                time_profits[segment_name] = None

        # 曜日
        weekday = WEEKDAY_MAP.get(row["backtest_date"].weekday(), "不明")

        # 価格帯
        price_range = get_price_range_label(buy_price)

        # 信用区分
        # shortable: 制度信用で空売り可能か
        # day_trade: いちにち信用で取引可能か
        # day_trade_available_shares: いちにち信用の在庫数
        shortable = row.get("shortable", False)
        day_trade = row.get("day_trade", False)
        day_trade_shares = row.get("day_trade_available_shares", 0) or 0

        margin_types = []
        if shortable:
            margin_types.append("制度")
        if day_trade:
            margin_types.append("いちにち")
            if day_trade_shares > 0:
                margin_types.append("いちにち(除0)")

        for margin_type in margin_types:
            result = {
                "ticker": ticker,
                "backtest_date": row["backtest_date"],
                "weekday": weekday,
                "price_range": price_range,
                "margin_type": margin_type,
                "buy_price": buy_price,
            }
            result.update(time_profits)
            results.append(result)

    return pd.DataFrame(results)


def aggregate_data(df):
    """集計"""
    print("\n集計中...")

    time_segment_names = [seg[0] for seg in TIME_SEGMENTS]

    aggregations = {}

    # 曜日別
    for weekday in WEEKDAYS:
        wd_df = df[df["weekday"] == weekday]
        if len(wd_df) == 0:
            continue

        agg = {"n": len(wd_df)}
        for seg in time_segment_names:
            valid = wd_df[seg].dropna()
            if len(valid) > 0:
                agg[f"{seg}_mean"] = valid.mean()
                agg[f"{seg}_sum"] = valid.sum()
                agg[f"{seg}_n"] = len(valid)
                agg[f"{seg}_win_rate"] = (valid > 0).mean() * 100
            else:
                agg[f"{seg}_mean"] = None
                agg[f"{seg}_sum"] = None
                agg[f"{seg}_n"] = 0
                agg[f"{seg}_win_rate"] = None

        aggregations[("weekday", weekday)] = agg

    # 信用区分別
    for margin_type in ["制度", "いちにち", "いちにち(除0)"]:
        mt_df = df[df["margin_type"] == margin_type]
        if len(mt_df) == 0:
            continue

        agg = {"n": len(mt_df)}
        for seg in time_segment_names:
            valid = mt_df[seg].dropna()
            if len(valid) > 0:
                agg[f"{seg}_mean"] = valid.mean()
                agg[f"{seg}_sum"] = valid.sum()
                agg[f"{seg}_n"] = len(valid)
                agg[f"{seg}_win_rate"] = (valid > 0).mean() * 100
            else:
                agg[f"{seg}_mean"] = None
                agg[f"{seg}_sum"] = None
                agg[f"{seg}_n"] = 0
                agg[f"{seg}_win_rate"] = None

        aggregations[("margin", margin_type)] = agg

    # 価格帯別
    for pr_label, _, _ in PRICE_RANGES:
        pr_df = df[df["price_range"] == pr_label]
        if len(pr_df) == 0:
            continue

        agg = {"n": len(pr_df)}
        for seg in time_segment_names:
            valid = pr_df[seg].dropna()
            if len(valid) > 0:
                agg[f"{seg}_mean"] = valid.mean()
                agg[f"{seg}_sum"] = valid.sum()
                agg[f"{seg}_n"] = len(valid)
                agg[f"{seg}_win_rate"] = (valid > 0).mean() * 100
            else:
                agg[f"{seg}_mean"] = None
                agg[f"{seg}_sum"] = None
                agg[f"{seg}_n"] = 0
                agg[f"{seg}_win_rate"] = None

        aggregations[("price_range", pr_label)] = agg

    # 曜日×信用区分
    for weekday in WEEKDAYS:
        for margin_type in ["制度", "いちにち", "いちにち(除0)"]:
            combo_df = df[(df["weekday"] == weekday) & (df["margin_type"] == margin_type)]
            if len(combo_df) == 0:
                continue

            agg = {"n": len(combo_df)}
            for seg in time_segment_names:
                valid = combo_df[seg].dropna()
                if len(valid) > 0:
                    agg[f"{seg}_mean"] = valid.mean()
                    agg[f"{seg}_sum"] = valid.sum()
                    agg[f"{seg}_n"] = len(valid)
                    agg[f"{seg}_win_rate"] = (valid > 0).mean() * 100
                else:
                    agg[f"{seg}_mean"] = None
                    agg[f"{seg}_sum"] = None
                    agg[f"{seg}_n"] = 0
                    agg[f"{seg}_win_rate"] = None

            aggregations[("weekday_margin", f"{weekday}_{margin_type}")] = agg

    # 曜日×価格帯
    for weekday in WEEKDAYS:
        for pr_label, _, _ in PRICE_RANGES:
            combo_df = df[(df["weekday"] == weekday) & (df["price_range"] == pr_label)]
            if len(combo_df) == 0:
                continue

            agg = {"n": len(combo_df)}
            for seg in time_segment_names:
                valid = combo_df[seg].dropna()
                if len(valid) > 0:
                    agg[f"{seg}_mean"] = valid.mean()
                    agg[f"{seg}_sum"] = valid.sum()
                    agg[f"{seg}_n"] = len(valid)
                    agg[f"{seg}_win_rate"] = (valid > 0).mean() * 100
                else:
                    agg[f"{seg}_mean"] = None
                    agg[f"{seg}_sum"] = None
                    agg[f"{seg}_n"] = 0
                    agg[f"{seg}_win_rate"] = None

            aggregations[("weekday_price", f"{weekday}_{pr_label}")] = agg

    return aggregations


def generate_html(aggregations, df):
    """HTML生成"""
    print("\nHTML生成中...")

    time_segment_names = [seg[0] for seg in TIME_SEGMENTS]

    # 全体サマリー
    total_n = len(df)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>11時間区分別利益分析（Grok選定銘柄ショート）</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #0a0a0a;
            color: #e0e0e0;
            padding: 20px;
            line-height: 1.6;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{
            font-size: 1.5rem;
            margin-bottom: 10px;
            color: #fff;
            border-bottom: 1px solid #333;
            padding-bottom: 10px;
        }}
        .description {{
            font-size: 0.85rem;
            color: #888;
            margin-bottom: 20px;
        }}
        h2 {{
            font-size: 1.1rem;
            margin: 30px 0 15px;
            color: #ccc;
            border-left: 4px solid #22c55e;
            padding-left: 10px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.8rem;
            margin-bottom: 20px;
        }}
        th, td {{
            padding: 8px 6px;
            border-bottom: 1px solid #222;
            text-align: right;
        }}
        th {{
            background: #1a1a1a;
            color: #888;
            font-weight: 500;
            position: sticky;
            top: 0;
        }}
        th:first-child, td:first-child {{
            text-align: left;
            position: sticky;
            left: 0;
            background: #0a0a0a;
            z-index: 1;
        }}
        th:first-child {{
            background: #1a1a1a;
            z-index: 2;
        }}
        tr:hover {{ background: #1a1a1a; }}
        .positive {{ color: #22c55e; }}
        .negative {{ color: #ef4444; }}
        .neutral {{ color: #888; }}
        .highlight {{ background: #1a2e1a !important; }}
        .small {{ font-size: 0.7rem; color: #666; }}
        .generated {{
            margin-top: 30px;
            font-size: 0.75rem;
            color: #666;
            text-align: right;
        }}
        .tabs {{
            display: flex;
            gap: 5px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }}
        .tab-btn {{
            padding: 8px 16px;
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 6px;
            color: #888;
            cursor: pointer;
            font-size: 0.85rem;
            transition: all 0.2s;
        }}
        .tab-btn:hover {{ background: #222; color: #ccc; }}
        .tab-btn.active {{ background: #333; color: #fff; border-color: #555; }}
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}
        .summary-card {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 20px;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }}
        .metric {{ margin-bottom: 10px; }}
        .metric-label {{ font-size: 0.75rem; color: #888; }}
        .metric-value {{ font-size: 1.1rem; font-weight: bold; }}
        .note {{
            font-size: 0.75rem;
            color: #666;
            margin-top: 5px;
            padding: 8px;
            background: #111;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>11時間区分別利益分析（Grok選定銘柄ショート）</h1>
        <div class="description">
            期間: 2025-11-04以降 | 対象: {total_n}件<br>
            利益計算: (売り建値 - 各時間の価格) × 100株<br>
            価格取得: 11:30=前場引け(sell_price), 15:30=大引け(daily_close), その他=5分足Close
        </div>

        <div class="tabs">
            <button class="tab-btn active" onclick="showTab('weekday')">曜日別</button>
            <button class="tab-btn" onclick="showTab('margin')">信用区分別</button>
            <button class="tab-btn" onclick="showTab('price')">価格帯別</button>
            <button class="tab-btn" onclick="showTab('weekday_margin')">曜日×信用区分</button>
            <button class="tab-btn" onclick="showTab('weekday_price')">曜日×価格帯</button>
        </div>
"""

    # 曜日別タブ
    html += """
        <div id="weekday" class="tab-content active">
            <h2>曜日別</h2>
            <div class="note">
                各時間区分での平均利益（円/100株）。プラス=ショート利益。
            </div>
            <div style="overflow-x: auto;">
            <table>
                <thead>
                    <tr>
                        <th>曜日</th>
                        <th>件数</th>
"""
    for seg in time_segment_names:
        html += f"                        <th>{seg}</th>\n"
    html += """                    </tr>
                </thead>
                <tbody>
"""

    for weekday in WEEKDAYS:
        key = ("weekday", weekday)
        if key not in aggregations:
            continue
        agg = aggregations[key]

        html += f"                    <tr>\n"
        html += f"                        <td><strong>{weekday}</strong></td>\n"
        html += f"                        <td>{agg['n']}</td>\n"

        for seg in time_segment_names:
            mean = agg.get(f"{seg}_mean")
            n = agg.get(f"{seg}_n", 0)
            win_rate = agg.get(f"{seg}_win_rate")

            if mean is not None:
                cls = "positive" if mean > 0 else "negative" if mean < 0 else "neutral"
                wr_cls = "positive" if win_rate and win_rate >= 50 else "negative"
                html += f'                        <td class="{cls}">{mean:+,.0f}<br><span class="small">n={n} 勝率<span class="{wr_cls}">{win_rate:.0f}%</span></span></td>\n'
            else:
                html += f'                        <td class="neutral">-</td>\n'

        html += f"                    </tr>\n"

    html += """                </tbody>
            </table>
            </div>
        </div>
"""

    # 信用区分別タブ
    html += """
        <div id="margin" class="tab-content">
            <h2>信用区分別</h2>
            <div class="note">
                制度=制度信用、いちにち=いちにち信用（在庫0含む）、いちにち(除0)=いちにち信用（在庫0除く）
            </div>
            <div style="overflow-x: auto;">
            <table>
                <thead>
                    <tr>
                        <th>信用区分</th>
                        <th>件数</th>
"""
    for seg in time_segment_names:
        html += f"                        <th>{seg}</th>\n"
    html += """                    </tr>
                </thead>
                <tbody>
"""

    for margin_type in ["制度", "いちにち", "いちにち(除0)"]:
        key = ("margin", margin_type)
        if key not in aggregations:
            continue
        agg = aggregations[key]

        html += f"                    <tr>\n"
        html += f"                        <td><strong>{margin_type}</strong></td>\n"
        html += f"                        <td>{agg['n']}</td>\n"

        for seg in time_segment_names:
            mean = agg.get(f"{seg}_mean")
            n = agg.get(f"{seg}_n", 0)
            win_rate = agg.get(f"{seg}_win_rate")

            if mean is not None:
                cls = "positive" if mean > 0 else "negative" if mean < 0 else "neutral"
                wr_cls = "positive" if win_rate and win_rate >= 50 else "negative"
                html += f'                        <td class="{cls}">{mean:+,.0f}<br><span class="small">n={n} 勝率<span class="{wr_cls}">{win_rate:.0f}%</span></span></td>\n'
            else:
                html += f'                        <td class="neutral">-</td>\n'

        html += f"                    </tr>\n"

    html += """                </tbody>
            </table>
            </div>
        </div>
"""

    # 価格帯別タブ
    html += """
        <div id="price" class="tab-content">
            <h2>価格帯別</h2>
            <div style="overflow-x: auto;">
            <table>
                <thead>
                    <tr>
                        <th>価格帯</th>
                        <th>件数</th>
"""
    for seg in time_segment_names:
        html += f"                        <th>{seg}</th>\n"
    html += """                    </tr>
                </thead>
                <tbody>
"""

    for pr_label, _, _ in PRICE_RANGES:
        key = ("price_range", pr_label)
        if key not in aggregations:
            continue
        agg = aggregations[key]

        html += f"                    <tr>\n"
        html += f"                        <td><strong>{pr_label}</strong></td>\n"
        html += f"                        <td>{agg['n']}</td>\n"

        for seg in time_segment_names:
            mean = agg.get(f"{seg}_mean")
            n = agg.get(f"{seg}_n", 0)
            win_rate = agg.get(f"{seg}_win_rate")

            if mean is not None:
                cls = "positive" if mean > 0 else "negative" if mean < 0 else "neutral"
                wr_cls = "positive" if win_rate and win_rate >= 50 else "negative"
                html += f'                        <td class="{cls}">{mean:+,.0f}<br><span class="small">n={n} 勝率<span class="{wr_cls}">{win_rate:.0f}%</span></span></td>\n'
            else:
                html += f'                        <td class="neutral">-</td>\n'

        html += f"                    </tr>\n"

    html += """                </tbody>
            </table>
            </div>
        </div>
"""

    # 曜日×信用区分タブ
    html += """
        <div id="weekday_margin" class="tab-content">
            <h2>曜日×信用区分別</h2>
            <div style="overflow-x: auto;">
            <table>
                <thead>
                    <tr>
                        <th>曜日×信用</th>
                        <th>件数</th>
"""
    for seg in time_segment_names:
        html += f"                        <th>{seg}</th>\n"
    html += """                    </tr>
                </thead>
                <tbody>
"""

    for weekday in WEEKDAYS:
        for margin_type in ["制度", "いちにち", "いちにち(除0)"]:
            key = ("weekday_margin", f"{weekday}_{margin_type}")
            if key not in aggregations:
                continue
            agg = aggregations[key]

            html += f"                    <tr>\n"
            html += f"                        <td><strong>{weekday}・{margin_type}</strong></td>\n"
            html += f"                        <td>{agg['n']}</td>\n"

            for seg in time_segment_names:
                mean = agg.get(f"{seg}_mean")
                n = agg.get(f"{seg}_n", 0)
                win_rate = agg.get(f"{seg}_win_rate")

                if mean is not None:
                    cls = "positive" if mean > 0 else "negative" if mean < 0 else "neutral"
                    wr_cls = "positive" if win_rate and win_rate >= 50 else "negative"
                    html += f'                        <td class="{cls}">{mean:+,.0f}<br><span class="small">n={n} 勝率<span class="{wr_cls}">{win_rate:.0f}%</span></span></td>\n'
                else:
                    html += f'                        <td class="neutral">-</td>\n'

            html += f"                    </tr>\n"

    html += """                </tbody>
            </table>
            </div>
        </div>
"""

    # 曜日×価格帯タブ
    html += """
        <div id="weekday_price" class="tab-content">
            <h2>曜日×価格帯別</h2>
            <div style="overflow-x: auto;">
            <table>
                <thead>
                    <tr>
                        <th>曜日×価格帯</th>
                        <th>件数</th>
"""
    for seg in time_segment_names:
        html += f"                        <th>{seg}</th>\n"
    html += """                    </tr>
                </thead>
                <tbody>
"""

    for weekday in WEEKDAYS:
        for pr_label, _, _ in PRICE_RANGES:
            key = ("weekday_price", f"{weekday}_{pr_label}")
            if key not in aggregations:
                continue
            agg = aggregations[key]

            html += f"                    <tr>\n"
            html += f"                        <td><strong>{weekday}・{pr_label}</strong></td>\n"
            html += f"                        <td>{agg['n']}</td>\n"

            for seg in time_segment_names:
                mean = agg.get(f"{seg}_mean")
                n = agg.get(f"{seg}_n", 0)
                win_rate = agg.get(f"{seg}_win_rate")

                if mean is not None:
                    cls = "positive" if mean > 0 else "negative" if mean < 0 else "neutral"
                    wr_cls = "positive" if win_rate and win_rate >= 50 else "negative"
                    html += f'                        <td class="{cls}">{mean:+,.0f}<br><span class="small">n={n} 勝率<span class="{wr_cls}">{win_rate:.0f}%</span></span></td>\n'
                else:
                    html += f'                        <td class="neutral">-</td>\n'

            html += f"                    </tr>\n"

    html += """                </tbody>
            </table>
            </div>
        </div>
"""

    html += f"""
        <div class="generated">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
    </div>

    <script>
    function showTab(tabId) {{
        document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
        document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
        document.getElementById(tabId).classList.add('active');
        event.target.classList.add('active');
    }}
    </script>
</body>
</html>
"""

    return html


def main():
    """メイン処理"""
    print("=" * 60)
    print("11時間区分別利益分析")
    print("=" * 60)

    # データ読み込み
    archive, m5 = load_data()

    # 利益計算
    df = calculate_profits(archive, m5)
    print(f"\n計算結果: {len(df)}件")

    # 集計
    aggregations = aggregate_data(df)
    print(f"集計グループ数: {len(aggregations)}")

    # HTML生成
    html = generate_html(aggregations, df)

    # 出力
    output_path = OUTPUT_DIR / "time_segment_analysis.html"
    output_path.write_text(html, encoding="utf-8")
    print(f"\n出力完了: {output_path}")

    # サマリー表示
    print("\n" + "=" * 60)
    print("サマリー（曜日別 × 11:30前場引け）")
    print("=" * 60)
    for weekday in WEEKDAYS:
        key = ("weekday", weekday)
        if key in aggregations:
            agg = aggregations[key]
            mean = agg.get("11:00-11:30_mean", 0) or 0
            n = agg.get("11:00-11:30_n", 0)
            wr = agg.get("11:00-11:30_win_rate", 0) or 0
            print(f"  {weekday}: 平均{mean:+,.0f}円 (n={n}, 勝率{wr:.1f}%)")


if __name__ == "__main__":
    main()
