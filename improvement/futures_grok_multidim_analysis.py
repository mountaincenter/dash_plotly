"""
先物騰落 × 曜日 × 価格帯 クロス集計分析

軸:
1. 先物騰落（23:00時点の変動率）
2. 曜日
3. 価格帯（銘柄の株価水準）
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "parquet"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_futures_5m() -> pd.DataFrame:
    path = DATA_DIR / "futures_prices_60d_5m.parquet"
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_nikkei_daily() -> pd.DataFrame:
    """日経平均株価（日足）を読み込む"""
    path = DATA_DIR / "index_prices_max_1d.parquet"
    df = pd.read_parquet(path)
    df = df[df["ticker"] == "^N225"]
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


def load_grok() -> pd.DataFrame:
    path = DATA_DIR / "backtest" / "grok_trending_archive.parquet"
    df = pd.read_parquet(path)
    return df


def extract_2300_prices(df_5m: pd.DataFrame) -> pd.DataFrame:
    df = df_5m.copy()
    df["trade_date"] = df["date"].dt.date
    df["hour"] = df["date"].dt.hour
    df["minute"] = df["date"].dt.minute

    df_2300 = df[(df["hour"] == 22) & (df["minute"] >= 55) |
                 (df["hour"] == 23) & (df["minute"] <= 5)]

    result = []
    for trade_date, group in df_2300.groupby("trade_date"):
        group = group.copy()
        group["diff_to_2300"] = abs(group["hour"] * 60 + group["minute"] - 23 * 60)
        closest = group.loc[group["diff_to_2300"].idxmin()]
        result.append({
            "date": pd.Timestamp(trade_date),
            "price_2300": closest["Close"],
        })

    return pd.DataFrame(result)


def prepare_grok(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["selection_date"]).dt.normalize()
    df = df[df["date"] >= "2025-11-04"]
    df = df[(df["shortable"] == True) | ((df["day_trade"] == True) & (df["shortable"] == False))]
    df["short_p2"] = -df["profit_per_100_shares_phase2"].fillna(0)

    # 価格帯
    df["price_range"] = pd.cut(
        df["buy_price"],
        bins=[0, 500, 1000, 2000, 3000, 5000, np.inf],
        labels=["~500", "500~1000", "1000~2000", "2000~3000", "3000~5000", "5000~"]
    )

    return df


def generate_html(
    df_merged: pd.DataFrame,
    df_daily: pd.DataFrame,
    df_merged_nikkei: pd.DataFrame,
    df_daily_nikkei: pd.DataFrame,
) -> str:
    weekday_map = {0: "月", 1: "火", 2: "水", 3: "木", 4: "金", 5: "土", 6: "日"}
    df_merged["weekday"] = df_merged["date"].dt.weekday.map(weekday_map)
    df_daily["weekday"] = df_daily["date"].dt.weekday.map(weekday_map)
    df_merged_nikkei["weekday"] = df_merged_nikkei["date"].dt.weekday.map(weekday_map)
    df_daily_nikkei["weekday"] = df_daily_nikkei["date"].dt.weekday.map(weekday_map)

    # 先物変動区間（4区間）
    bins = [-np.inf, -1, 0, 1, np.inf]
    labels = ["<-1%", "-1~0%", "0~+1%", ">+1%"]
    df_merged["futures_range"] = pd.cut(df_merged["futures_change_pct"], bins=bins, labels=labels)
    df_daily["futures_range"] = pd.cut(df_daily["futures_change_pct"], bins=bins, labels=labels)

    # 先物変動区間（6区間・細分化）
    bins_fine = [-np.inf, -1, -0.5, 0, 0.5, 1, np.inf]
    labels_fine = ["<-1%", "-1~-0.5%", "-0.5~0%", "0~+0.5%", "+0.5~+1%", ">+1%"]
    df_merged["futures_range_fine"] = pd.cut(df_merged["futures_change_pct"], bins=bins_fine, labels=labels_fine)
    df_daily["futures_range_fine"] = pd.cut(df_daily["futures_change_pct"], bins=bins_fine, labels=labels_fine)

    # 日経終値変動区間（4区間）
    df_merged_nikkei["nikkei_range"] = pd.cut(df_merged_nikkei["nikkei_change_pct"], bins=bins, labels=labels)
    df_daily_nikkei["nikkei_range"] = pd.cut(df_daily_nikkei["nikkei_change_pct"], bins=bins, labels=labels)

    # 日経終値変動区間（6区間・細分化）
    df_merged_nikkei["nikkei_range_fine"] = pd.cut(df_merged_nikkei["nikkei_change_pct"], bins=bins_fine, labels=labels_fine)
    df_daily_nikkei["nikkei_range_fine"] = pd.cut(df_daily_nikkei["nikkei_change_pct"], bins=bins_fine, labels=labels_fine)

    # 先物変動区間（10区間）
    bins_10 = [-np.inf, -3, -2, -1, -0.5, 0, 0.5, 1, 2, 3, np.inf]
    labels_10 = ['<-3%', '-3~-2%', '-2~-1%', '-1~-0.5%', '-0.5~0%', '0~+0.5%', '+0.5~+1%', '+1~+2%', '+2~+3%', '>+3%']
    df_merged["futures_range_10"] = pd.cut(df_merged["futures_change_pct"], bins=bins_10, labels=labels_10)
    df_daily["futures_range_10"] = pd.cut(df_daily["futures_change_pct"], bins=bins_10, labels=labels_10)

    # 日経終値変動区間（10区間）
    df_merged_nikkei["nikkei_range_10"] = pd.cut(df_merged_nikkei["nikkei_change_pct"], bins=bins_10, labels=labels_10)
    df_daily_nikkei["nikkei_range_10"] = pd.cut(df_daily_nikkei["nikkei_change_pct"], bins=bins_10, labels=labels_10)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>先物騰落 × 曜日 × 価格帯 クロス集計</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 20px;
            background: #f5f5f5;
        }}
        h1 {{ color: #333; border-bottom: 2px solid #333; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        h3 {{ color: #666; margin-top: 20px; }}
        table {{
            border-collapse: collapse;
            background: #fff;
            margin: 15px 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 6px 10px;
            text-align: right;
            font-size: 13px;
        }}
        th {{
            background: #333;
            color: #fff;
        }}
        td:first-child, th:first-child {{ text-align: left; }}
        tr:nth-child(even) {{ background: #f9f9f9; }}
        .positive {{ color: #28a745; font-weight: 600; }}
        .negative {{ color: #dc3545; font-weight: 600; }}
        .section {{
            background: #fff;
            padding: 20px;
            margin: 20px 0;
            border-radius: 8px;
            border: 1px solid #ddd;
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
        }}
    </style>
</head>
<body>
    <h1>先物騰落 × 曜日 × 価格帯 クロス集計</h1>

    <div class="section">
        <h2>1. 市場騰落別</h2>
"""

    # 1-1. 先物騰落別（4区間）
    summary_futures = df_merged.groupby("futures_range", observed=True).agg(
        件数=("short_p2", "count"),
        利益合計=("short_p2", "sum"),
        利益平均=("short_p2", "mean"),
        勝率=("short_p2", lambda x: (x > 0).mean() * 100),
    ).round(1)

    # 1-1. 先物騰落別（6区間・細分化）
    summary_futures_fine = df_merged.groupby("futures_range_fine", observed=True).agg(
        件数=("short_p2", "count"),
        利益合計=("short_p2", "sum"),
        利益平均=("short_p2", "mean"),
        勝率=("short_p2", lambda x: (x > 0).mean() * 100),
    ).round(1)

    # 1-2. 日経終値騰落別（4区間）
    summary_nikkei = df_merged_nikkei.groupby("nikkei_range", observed=True).agg(
        件数=("short_p2", "count"),
        利益合計=("short_p2", "sum"),
        利益平均=("short_p2", "mean"),
        勝率=("short_p2", lambda x: (x > 0).mean() * 100),
    ).round(1)

    # 1-2. 日経終値騰落別（6区間・細分化）
    summary_nikkei_fine = df_merged_nikkei.groupby("nikkei_range_fine", observed=True).agg(
        件数=("short_p2", "count"),
        利益合計=("short_p2", "sum"),
        利益平均=("short_p2", "mean"),
        勝率=("short_p2", lambda x: (x > 0).mean() * 100),
    ).round(1)

    # 1-1. 先物騰落別（10区間）
    summary_futures_10 = df_merged.groupby("futures_range_10", observed=True).agg(
        件数=("short_p2", "count"),
        利益合計=("short_p2", "sum"),
        利益平均=("short_p2", "mean"),
        勝率=("short_p2", lambda x: (x > 0).mean() * 100),
    ).round(1)

    # 1-2. 日経終値騰落別（10区間）
    summary_nikkei_10 = df_merged_nikkei.groupby("nikkei_range_10", observed=True).agg(
        件数=("short_p2", "count"),
        利益合計=("short_p2", "sum"),
        利益平均=("short_p2", "mean"),
        勝率=("short_p2", lambda x: (x > 0).mean() * 100),
    ).round(1)

    # --- 先物版 ---
    html += "<h3>先物版（23:00時点）</h3>"
    html += "<div class='grid'><div>"
    html += "<h4>4区間</h4>"
    html += "<table><tr><th>先物騰落</th><th>件数</th><th>利益合計</th><th>利益平均</th><th>勝率</th></tr>"
    for idx, row in summary_futures.iterrows():
        pc = "positive" if row["利益合計"] > 0 else "negative"
        html += f"<tr><td>{idx}</td><td>{int(row['件数'])}</td><td class='{pc}'>{int(row['利益合計']):+,}</td><td class='{pc}'>{int(row['利益平均']):+,}</td><td>{row['勝率']:.1f}%</td></tr>"
    html += "</table></div>"

    html += "<div>"
    html += "<h4>6区間（細分化）</h4>"
    html += "<table><tr><th>先物騰落</th><th>件数</th><th>利益合計</th><th>利益平均</th><th>勝率</th></tr>"
    for idx, row in summary_futures_fine.iterrows():
        pc = "positive" if row["利益合計"] > 0 else "negative"
        html += f"<tr><td>{idx}</td><td>{int(row['件数'])}</td><td class='{pc}'>{int(row['利益合計']):+,}</td><td class='{pc}'>{int(row['利益平均']):+,}</td><td>{row['勝率']:.1f}%</td></tr>"
    html += "</table></div>"

    html += "<div>"
    html += "<h4>10区間</h4>"
    html += "<table><tr><th>先物騰落</th><th>件数</th><th>利益合計</th><th>利益平均</th><th>勝率</th></tr>"
    for idx, row in summary_futures_10.iterrows():
        pc = "positive" if row["利益合計"] > 0 else "negative"
        html += f"<tr><td>{idx}</td><td>{int(row['件数'])}</td><td class='{pc}'>{int(row['利益合計']):+,}</td><td class='{pc}'>{int(row['利益平均']):+,}</td><td>{row['勝率']:.1f}%</td></tr>"
    html += "</table></div></div>"

    # --- 日経終値版 ---
    html += "<h3>日経終値版（15:30時点）</h3>"
    html += "<div class='grid'><div>"
    html += "<h4>4区間</h4>"
    html += "<table><tr><th>日経騰落</th><th>件数</th><th>利益合計</th><th>利益平均</th><th>勝率</th></tr>"
    for idx, row in summary_nikkei.iterrows():
        pc = "positive" if row["利益合計"] > 0 else "negative"
        html += f"<tr><td>{idx}</td><td>{int(row['件数'])}</td><td class='{pc}'>{int(row['利益合計']):+,}</td><td class='{pc}'>{int(row['利益平均']):+,}</td><td>{row['勝率']:.1f}%</td></tr>"
    html += "</table></div>"

    html += "<div>"
    html += "<h4>6区間（細分化）</h4>"
    html += "<table><tr><th>日経騰落</th><th>件数</th><th>利益合計</th><th>利益平均</th><th>勝率</th></tr>"
    for idx, row in summary_nikkei_fine.iterrows():
        pc = "positive" if row["利益合計"] > 0 else "negative"
        html += f"<tr><td>{idx}</td><td>{int(row['件数'])}</td><td class='{pc}'>{int(row['利益合計']):+,}</td><td class='{pc}'>{int(row['利益平均']):+,}</td><td>{row['勝率']:.1f}%</td></tr>"
    html += "</table></div>"

    html += "<div>"
    html += "<h4>10区間</h4>"
    html += "<table><tr><th>日経騰落</th><th>件数</th><th>利益合計</th><th>利益平均</th><th>勝率</th></tr>"
    for idx, row in summary_nikkei_10.iterrows():
        pc = "positive" if row["利益合計"] > 0 else "negative"
        html += f"<tr><td>{idx}</td><td>{int(row['件数'])}</td><td class='{pc}'>{int(row['利益合計']):+,}</td><td class='{pc}'>{int(row['利益平均']):+,}</td><td>{row['勝率']:.1f}%</td></tr>"
    html += "</table></div></div></div>"

    # 2. 曜日別
    html += "<div class='section'><h2>2. 曜日別</h2>"

    summary_weekday = df_daily.groupby("weekday", observed=True).agg(
        日数=("short_profit_sum", "count"),
        利益合計=("short_profit_sum", "sum"),
        利益平均=("short_profit_sum", "mean"),
    ).round(0)
    # 曜日順にソート
    weekday_order = ["月", "火", "水", "木", "金"]
    summary_weekday = summary_weekday.reindex(weekday_order)

    html += "<table><tr><th>曜日</th><th>日数</th><th>利益合計</th><th>利益平均</th></tr>"
    for idx, row in summary_weekday.iterrows():
        if pd.isna(row["日数"]):
            continue
        pc = "positive" if row["利益合計"] > 0 else "negative"
        html += f"<tr><td>{idx}</td><td>{int(row['日数'])}</td><td class='{pc}'>{int(row['利益合計']):+,}</td><td class='{pc}'>{int(row['利益平均']):+,}</td></tr>"
    html += "</table></div>"

    # 3. 価格帯別
    html += "<div class='section'><h2>3. 価格帯別</h2>"

    summary_price = df_merged.groupby("price_range", observed=True).agg(
        件数=("short_p2", "count"),
        利益合計=("short_p2", "sum"),
        利益平均=("short_p2", "mean"),
        勝率=("short_p2", lambda x: (x > 0).mean() * 100),
    ).round(1)

    html += "<table><tr><th>価格帯</th><th>件数</th><th>利益合計</th><th>利益平均</th><th>勝率</th></tr>"
    for idx, row in summary_price.iterrows():
        pc = "positive" if row["利益合計"] > 0 else "negative"
        html += f"<tr><td>{idx}</td><td>{int(row['件数'])}</td><td class='{pc}'>{int(row['利益合計']):+,}</td><td class='{pc}'>{int(row['利益平均']):+,}</td><td>{row['勝率']:.1f}%</td></tr>"
    html += "</table></div>"

    # 4. 先物騰落 × 曜日
    html += "<div class='section'><h2>4. 市場騰落 × 曜日</h2>"

    # --- 先物版 ---
    html += "<h3>先物版（23:00時点）</h3>"

    # 4区間版
    pivot_fw = df_daily.pivot_table(
        index="futures_range",
        columns="weekday",
        values="short_profit_sum",
        aggfunc=["sum", "count"],
    ).round(0)

    # 6区間版
    pivot_fw_fine = df_daily.pivot_table(
        index="futures_range_fine",
        columns="weekday",
        values="short_profit_sum",
        aggfunc=["sum", "count"],
    ).round(0)

    html += "<h4>4区間 - 利益合計</h4><table><tr><th>先物騰落</th>"
    for w in weekday_order:
        html += f"<th>{w}</th>"
    html += "</tr>"

    for idx in labels:
        html += f"<tr><td>{idx}</td>"
        for w in weekday_order:
            try:
                val = pivot_fw[("sum", w)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table>"

    html += "<h4>6区間 - 利益合計</h4><table><tr><th>先物騰落</th>"
    for w in weekday_order:
        html += f"<th>{w}</th>"
    html += "</tr>"

    for idx in labels_fine:
        html += f"<tr><td>{idx}</td>"
        for w in weekday_order:
            try:
                val = pivot_fw_fine[("sum", w)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table>"

    # 10区間版
    pivot_fw_10 = df_daily.pivot_table(
        index="futures_range_10",
        columns="weekday",
        values="short_profit_sum",
        aggfunc=["sum", "count"],
    ).round(0)

    html += "<h4>10区間 - 利益合計</h4><table><tr><th>先物騰落</th>"
    for w in weekday_order:
        html += f"<th>{w}</th>"
    html += "</tr>"

    for idx in labels_10:
        html += f"<tr><td>{idx}</td>"
        for w in weekday_order:
            try:
                val = pivot_fw_10[("sum", w)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table>"

    # --- 日経終値版 ---
    html += "<h3>日経終値版（15:30時点）</h3>"

    # 4区間版
    pivot_nw = df_daily_nikkei.pivot_table(
        index="nikkei_range",
        columns="weekday",
        values="short_profit_sum",
        aggfunc=["sum", "count"],
    ).round(0)

    # 6区間版
    pivot_nw_fine = df_daily_nikkei.pivot_table(
        index="nikkei_range_fine",
        columns="weekday",
        values="short_profit_sum",
        aggfunc=["sum", "count"],
    ).round(0)

    html += "<h4>4区間 - 利益合計</h4><table><tr><th>日経騰落</th>"
    for w in weekday_order:
        html += f"<th>{w}</th>"
    html += "</tr>"

    for idx in labels:
        html += f"<tr><td>{idx}</td>"
        for w in weekday_order:
            try:
                val = pivot_nw[("sum", w)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table>"

    html += "<h4>6区間 - 利益合計</h4><table><tr><th>日経騰落</th>"
    for w in weekday_order:
        html += f"<th>{w}</th>"
    html += "</tr>"

    for idx in labels_fine:
        html += f"<tr><td>{idx}</td>"
        for w in weekday_order:
            try:
                val = pivot_nw_fine[("sum", w)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table>"

    # 10区間版
    pivot_nw_10 = df_daily_nikkei.pivot_table(
        index="nikkei_range_10",
        columns="weekday",
        values="short_profit_sum",
        aggfunc=["sum", "count"],
    ).round(0)

    html += "<h4>10区間 - 利益合計</h4><table><tr><th>日経騰落</th>"
    for w in weekday_order:
        html += f"<th>{w}</th>"
    html += "</tr>"

    for idx in labels_10:
        html += f"<tr><td>{idx}</td>"
        for w in weekday_order:
            try:
                val = pivot_nw_10[("sum", w)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table></div>"

    # 5. 先物騰落 × 価格帯
    html += "<div class='section'><h2>5. 市場騰落 × 価格帯</h2>"

    price_labels = ["~500", "500~1000", "1000~2000", "2000~3000", "3000~5000", "5000~"]

    # --- 先物版 ---
    html += "<h3>先物版（23:00時点）</h3>"

    # 4区間版
    pivot_fp = df_merged.pivot_table(
        index="futures_range",
        columns="price_range",
        values="short_p2",
        aggfunc=["sum", "count"],
    ).round(0)

    # 6区間版
    pivot_fp_fine = df_merged.pivot_table(
        index="futures_range_fine",
        columns="price_range",
        values="short_p2",
        aggfunc=["sum", "count"],
    ).round(0)

    html += "<h4>4区間 - 利益合計</h4><table><tr><th>先物騰落</th>"
    for p in price_labels:
        html += f"<th>{p}</th>"
    html += "</tr>"

    for idx in labels:
        html += f"<tr><td>{idx}</td>"
        for p in price_labels:
            try:
                val = pivot_fp[("sum", p)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table>"

    html += "<h4>6区間 - 利益合計</h4><table><tr><th>先物騰落</th>"
    for p in price_labels:
        html += f"<th>{p}</th>"
    html += "</tr>"

    for idx in labels_fine:
        html += f"<tr><td>{idx}</td>"
        for p in price_labels:
            try:
                val = pivot_fp_fine[("sum", p)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table>"

    # 10区間版
    pivot_fp_10 = df_merged.pivot_table(
        index="futures_range_10",
        columns="price_range",
        values="short_p2",
        aggfunc=["sum", "count"],
    ).round(0)

    html += "<h4>10区間 - 利益合計</h4><table><tr><th>先物騰落</th>"
    for p in price_labels:
        html += f"<th>{p}</th>"
    html += "</tr>"

    for idx in labels_10:
        html += f"<tr><td>{idx}</td>"
        for p in price_labels:
            try:
                val = pivot_fp_10[("sum", p)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table>"

    # --- 日経終値版 ---
    html += "<h3>日経終値版（15:30時点）</h3>"

    # 4区間版
    pivot_np = df_merged_nikkei.pivot_table(
        index="nikkei_range",
        columns="price_range",
        values="short_p2",
        aggfunc=["sum", "count"],
    ).round(0)

    # 6区間版
    pivot_np_fine = df_merged_nikkei.pivot_table(
        index="nikkei_range_fine",
        columns="price_range",
        values="short_p2",
        aggfunc=["sum", "count"],
    ).round(0)

    html += "<h4>4区間 - 利益合計</h4><table><tr><th>日経騰落</th>"
    for p in price_labels:
        html += f"<th>{p}</th>"
    html += "</tr>"

    for idx in labels:
        html += f"<tr><td>{idx}</td>"
        for p in price_labels:
            try:
                val = pivot_np[("sum", p)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table>"

    html += "<h4>6区間 - 利益合計</h4><table><tr><th>日経騰落</th>"
    for p in price_labels:
        html += f"<th>{p}</th>"
    html += "</tr>"

    for idx in labels_fine:
        html += f"<tr><td>{idx}</td>"
        for p in price_labels:
            try:
                val = pivot_np_fine[("sum", p)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table>"

    # 10区間版
    pivot_np_10 = df_merged_nikkei.pivot_table(
        index="nikkei_range_10",
        columns="price_range",
        values="short_p2",
        aggfunc=["sum", "count"],
    ).round(0)

    html += "<h4>10区間 - 利益合計</h4><table><tr><th>日経騰落</th>"
    for p in price_labels:
        html += f"<th>{p}</th>"
    html += "</tr>"

    for idx in labels_10:
        html += f"<tr><td>{idx}</td>"
        for p in price_labels:
            try:
                val = pivot_np_10[("sum", p)].get(idx, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table></div>"

    # 6. 曜日 × 価格帯
    html += "<div class='section'><h2>6. 曜日 × 価格帯</h2>"

    pivot_wp = df_merged.pivot_table(
        index="weekday",
        columns="price_range",
        values="short_p2",
        aggfunc=["sum", "count"],
    ).round(0)

    html += "<h3>利益合計</h3><table><tr><th>曜日</th>"
    for p in price_labels:
        html += f"<th>{p}</th>"
    html += "</tr>"

    for w in weekday_order:
        html += f"<tr><td>{w}</td>"
        for p in price_labels:
            try:
                val = pivot_wp[("sum", p)].get(w, 0)
                if pd.isna(val):
                    val = 0
                pc = "positive" if val > 0 else "negative" if val < 0 else ""
                html += f"<td class='{pc}'>{int(val):+,}</td>"
            except:
                html += "<td>-</td>"
        html += "</tr>"
    html += "</table></div>"

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
    print("先物騰落 × 曜日 × 価格帯 クロス集計")
    print("=" * 60)

    # データ読み込み
    df_5m = load_futures_5m()
    df_grok = load_grok()
    df_nikkei = load_nikkei_daily()

    # ==== 先物データ処理 ====
    # 23:00価格抽出
    df_2300 = extract_2300_prices(df_5m)
    df_2300 = df_2300.sort_values("date")
    df_2300["prev_price_2300"] = df_2300["price_2300"].shift(1)
    df_2300["futures_change_pct"] = (df_2300["price_2300"] - df_2300["prev_price_2300"]) / df_2300["prev_price_2300"] * 100

    # 先物データを「翌営業日」に紐づけ（選定日の先物変動が翌営業日の取引に影響）
    # 例: 金曜の先物変動 → 月曜の取引結果に紐づけ（土日は先物休み）
    # 先物データが存在する日 = 営業日
    trading_dates = sorted(df_2300["date"].unique())

    # 各営業日の「翌営業日」を計算
    date_to_next_trading = {}
    for i, d in enumerate(trading_dates[:-1]):
        date_to_next_trading[d] = trading_dates[i + 1]

    df_2300["date_next"] = df_2300["date"].map(date_to_next_trading)

    # ==== 日経終値データ処理 ====
    df_nikkei = df_nikkei.sort_values("date")
    df_nikkei["prev_close"] = df_nikkei["Close"].shift(1)
    df_nikkei["nikkei_change_pct"] = (df_nikkei["Close"] - df_nikkei["prev_close"]) / df_nikkei["prev_close"] * 100

    # 日経データを「翌営業日」に紐づけ（先物と同じロジック）
    nikkei_trading_dates = sorted(df_nikkei["date"].unique())

    date_to_next_nikkei = {}
    for i, d in enumerate(nikkei_trading_dates[:-1]):
        date_to_next_nikkei[d] = nikkei_trading_dates[i + 1]

    df_nikkei["date_next"] = df_nikkei["date"].map(date_to_next_nikkei)

    # Grokデータ準備
    df_grok_prep = prepare_grok(df_grok)

    # 日次集計
    grok_daily = df_grok_prep.groupby("date").agg(
        stock_count=("ticker", "count"),
        short_profit_sum=("short_p2", "sum"),
    ).reset_index()

    # ==== 先物版マージ ====
    # マージ（日次）- date_nextを使って1日シフト
    df_2300["date_next"] = pd.to_datetime(df_2300["date_next"]).dt.normalize()
    grok_daily["date"] = pd.to_datetime(grok_daily["date"]).dt.normalize()

    df_daily = grok_daily.merge(
        df_2300[["date_next", "futures_change_pct"]].rename(columns={"date_next": "date"}),
        on="date", how="left"
    )
    df_daily = df_daily[df_daily["futures_change_pct"].notna()]

    # マージ（銘柄単位）- date_nextを使って1日シフト
    df_grok_prep["date"] = pd.to_datetime(df_grok_prep["date"]).dt.normalize()
    df_merged = df_grok_prep.merge(
        df_2300[["date_next", "futures_change_pct"]].rename(columns={"date_next": "date"}),
        on="date", how="left"
    )
    df_merged = df_merged[df_merged["futures_change_pct"].notna()]

    # ==== 日経終値版マージ ====
    df_nikkei["date_next"] = pd.to_datetime(df_nikkei["date_next"]).dt.normalize()

    # マージ（日次）
    df_daily_nikkei = grok_daily.merge(
        df_nikkei[["date_next", "nikkei_change_pct"]].rename(columns={"date_next": "date"}),
        on="date", how="left"
    )
    df_daily_nikkei = df_daily_nikkei[df_daily_nikkei["nikkei_change_pct"].notna()]

    # マージ（銘柄単位）- Grokデータを再準備（先物版で変更されているため）
    df_grok_prep_nikkei = prepare_grok(df_grok)
    df_grok_prep_nikkei["date"] = pd.to_datetime(df_grok_prep_nikkei["date"]).dt.normalize()
    df_merged_nikkei = df_grok_prep_nikkei.merge(
        df_nikkei[["date_next", "nikkei_change_pct"]].rename(columns={"date_next": "date"}),
        on="date", how="left"
    )
    df_merged_nikkei = df_merged_nikkei[df_merged_nikkei["nikkei_change_pct"].notna()]

    print(f"\n[先物版]")
    print(f"  銘柄単位データ: {len(df_merged)}件")
    print(f"  日次データ: {len(df_daily)}日")
    print(f"\n[日経終値版]")
    print(f"  銘柄単位データ: {len(df_merged_nikkei)}件")
    print(f"  日次データ: {len(df_daily_nikkei)}日")

    # HTML生成
    html = generate_html(df_merged, df_daily, df_merged_nikkei, df_daily_nikkei)

    output_file = OUTPUT_DIR / "futures_multidim_analysis.html"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✓ HTML出力: {output_file}")


if __name__ == "__main__":
    main()
