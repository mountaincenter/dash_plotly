#!/usr/bin/env python3
"""
株式取引結果をHTMLで出力するスクリプト
取得日/建日が2025/11/04以降のデータを抽出
日別/週別/月別の切り替え表示対応

Parquetファイル生成・S3アップロード対応
"""

import pandas as pd
import json
import sys
from pathlib import Path
from datetime import datetime

# common_cfg をインポート
sys.path.insert(0, str(Path(__file__).parent.parent))
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file, download_file

# パス設定
BASE_DIR = Path(__file__).parent.parent
CSV_PATH = BASE_DIR / "data" / "csv" / "stock_results.csv"
NIKKEIVI_CSV_PATH = BASE_DIR / "data" / "csv" / "nikkeivi.csv"
OUTPUT_PATH = BASE_DIR / "output" / "stock_results.html"
PARQUET_DIR = BASE_DIR / "data" / "parquet"
PARQUET_PATH = PARQUET_DIR / "stock_results.parquet"
NIKKEIVI_PARQUET_PATH = PARQUET_DIR / "nikkei_vi_max_1d.parquet"
MANIFEST_PATH = PARQUET_DIR / "manifest.json"

# 出力ディレクトリ作成
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# CSV読み込み
df = pd.read_csv(CSV_PATH, encoding="utf-8")

# 取得日/建日が"-"のものは、約定日で補完
df.loc[df["取得日/建日"] == "-", "取得日/建日"] = df.loc[df["取得日/建日"] == "-", "約定日"]

# 取得日/建日が"-"のものを除外してから日付型に変換
df = df[df["取得日/建日"] != "-"].copy()
df["取得日/建日"] = pd.to_datetime(df["取得日/建日"], format="%Y/%m/%d")

# 2025/11/04以降のデータを抽出
cutoff_date = datetime(2025, 11, 4)
df_filtered = df[df["取得日/建日"] >= cutoff_date].copy()

# 約定日も日付型に変換（ソート用）
df_filtered["約定日"] = pd.to_datetime(df_filtered["約定日"], format="%Y/%m/%d")

# 週・月カラム追加
df_filtered["週"] = df_filtered["約定日"].dt.strftime("%Y/W%W")
df_filtered["月"] = df_filtered["約定日"].dt.strftime("%Y/%m")

# 約定日でソート（新しい順）
df_filtered = df_filtered.sort_values("約定日", ascending=False)

# 実現損益の数値変換（カンマ除去）
df_filtered["実現損益_num"] = df_filtered["実現損益(円)"].str.replace(",", "").astype(float)

# ロング/ショート判定（売埋/売付=ロング、買埋=ショート）
df_filtered["position_type"] = df_filtered["取引"].apply(
    lambda x: "ショート" if x == "買埋" else "ロング"
)

# 数値変換（集計前に必要）
df_filtered["単価_num"] = df_filtered["単価(円)"].str.replace(",", "").astype(float)
df_filtered["取得価額_num"] = df_filtered["平均取得価額(円)"].str.replace(",", "").astype(float)

# 銘柄別日別売買別集計（先に作成）
daily_stock = df_filtered.groupby(["約定日", "コード", "銘柄名", "position_type"]).agg({
    "取得価額_num": "mean",
    "単価_num": "mean",
    "実現損益_num": "sum",
    "数量(株/口)": lambda x: x.str.replace(",", "").astype(int).sum()
}).reset_index()

daily_stock.columns = ["約定日", "コード", "銘柄名", "売買", "平均取得価額", "平均単価", "実現損益", "数量"]

# 週別集計
weekly_stock = df_filtered.groupby(["週", "コード", "銘柄名", "position_type"]).agg({
    "取得価額_num": "mean",
    "単価_num": "mean",
    "実現損益_num": "sum",
    "数量(株/口)": lambda x: x.str.replace(",", "").astype(int).sum()
}).reset_index()
weekly_stock.columns = ["週", "コード", "銘柄名", "売買", "平均取得価額", "平均単価", "実現損益", "数量"]

# 月別集計
monthly_stock = df_filtered.groupby(["月", "コード", "銘柄名", "position_type"]).agg({
    "取得価額_num": "mean",
    "単価_num": "mean",
    "実現損益_num": "sum",
    "数量(株/口)": lambda x: x.str.replace(",", "").astype(int).sum()
}).reset_index()
monthly_stock.columns = ["月", "コード", "銘柄名", "売買", "平均取得価額", "平均単価", "実現損益", "数量"]

# 全体集計（銘柄別日別売買別ベース）
total_profit = daily_stock["実現損益"].sum()
win_count = (daily_stock["実現損益"] > 0).sum()
lose_count = (daily_stock["実現損益"] < 0).sum()
total_count = len(daily_stock)
win_rate = win_count / total_count * 100 if total_count > 0 else 0

# ロング/ショート別集計（銘柄別日別売買別ベース）
long_df = daily_stock[daily_stock["売買"] == "ロング"]
short_df = daily_stock[daily_stock["売買"] == "ショート"]

long_profit = long_df["実現損益"].sum() if len(long_df) > 0 else 0
long_count = len(long_df)
long_win = (long_df["実現損益"] > 0).sum() if len(long_df) > 0 else 0
long_lose = (long_df["実現損益"] < 0).sum() if len(long_df) > 0 else 0
long_win_rate = long_win / long_count * 100 if long_count > 0 else 0

short_profit = short_df["実現損益"].sum() if len(short_df) > 0 else 0
short_count = len(short_df)
short_win = (short_df["実現損益"] > 0).sum() if len(short_df) > 0 else 0
short_lose = (short_df["実現損益"] < 0).sum() if len(short_df) > 0 else 0
short_win_rate = short_win / short_count * 100 if short_count > 0 else 0

# ソート
daily_stock = daily_stock.sort_values(["約定日", "コード", "売買"], ascending=[False, True, True])

# 価格帯別集計（平均取得価額ベース）
def price_range(price):
    if price < 500:
        return "500円未満"
    elif price < 1000:
        return "500-1000円"
    elif price < 2000:
        return "1000-2000円"
    elif price < 3000:
        return "2000-3000円"
    elif price < 5000:
        return "3000-5000円"
    else:
        return "5000円以上"

df_filtered["価格帯"] = df_filtered["取得価額_num"].apply(price_range)

price_range_order = ["500円未満", "500-1000円", "1000-2000円", "2000-3000円", "3000-5000円", "5000円以上"]
price_stats = []
for pr in price_range_order:
    pr_df = df_filtered[df_filtered["価格帯"] == pr]
    if len(pr_df) > 0:
        pr_profit = pr_df["実現損益_num"].sum()
        pr_count = len(pr_df)
        pr_win = (pr_df["実現損益_num"] > 0).sum()
        pr_lose = (pr_df["実現損益_num"] < 0).sum()
        pr_win_rate = pr_win / pr_count * 100 if pr_count > 0 else 0
        price_stats.append({
            "range": pr,
            "profit": pr_profit,
            "count": pr_count,
            "win": pr_win,
            "lose": pr_lose,
            "win_rate": pr_win_rate
        })

# 損失水準別集計（日別銘柄別売買別ベース、ロング/ショート分離）
def loss_range(pl):
    if pl >= 0:
        return "利益"
    elif pl > -10000:
        return "-1万未満"
    elif pl > -50000:
        return "-1万〜-5万"
    elif pl > -100000:
        return "-5万〜-10万"
    else:
        return "-10万以上"

daily_stock["損益区分"] = daily_stock["実現損益"].apply(loss_range)

loss_range_order = ["利益", "-1万未満", "-1万〜-5万", "-5万〜-10万", "-10万以上"]
loss_stats = []
for lr in loss_range_order:
    lr_df = daily_stock[daily_stock["損益区分"] == lr]
    if len(lr_df) > 0:
        lr_total = lr_df["実現損益"].sum()
        lr_count = len(lr_df)
        lr_long = len(lr_df[lr_df["売買"] == "ロング"])
        lr_short = len(lr_df[lr_df["売買"] == "ショート"])
        loss_stats.append({
            "range": lr,
            "total": lr_total,
            "count": lr_count,
            "long": lr_long,
            "short": lr_short
        })

# 日付を文字列に戻す
df_filtered["約定日"] = df_filtered["約定日"].dt.strftime("%Y/%m/%d")
df_filtered["取得日/建日"] = df_filtered["取得日/建日"].dt.strftime("%Y/%m/%d")
daily_stock["約定日"] = daily_stock["約定日"].dt.strftime("%Y/%m/%d")

# 勝率に応じた色クラス
def win_rate_class(rate):
    return "positive" if rate >= 50 else "negative"

# HTML生成
html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>株式取引結果 (2025/11/04以降)</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #0a0a0a;
            color: #e0e0e0;
            padding: 20px;
            line-height: 1.6;
        }}
        .container {{
            max-width: 900px;
            margin: 0 auto;
        }}
        h1 {{
            font-size: 1.5rem;
            margin-bottom: 20px;
            color: #fff;
            border-bottom: 1px solid #333;
            padding-bottom: 10px;
        }}
        h2 {{
            font-size: 1.1rem;
            margin: 25px 0 15px;
            color: #ccc;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }}
        .summary-card {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 15px;
        }}
        .summary-card .label {{
            font-size: 0.8rem;
            color: #888;
            margin-bottom: 5px;
        }}
        .summary-card .value {{
            font-size: 1.1rem;
            font-weight: bold;
        }}
        .summary-card .row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 4px 0;
            border-bottom: 1px solid #222;
        }}
        .summary-card .row:last-child {{
            border-bottom: none;
        }}
        .summary-card .row-label {{
            font-size: 0.8rem;
            color: #888;
        }}
        .positive {{ color: #22c55e; }}
        .negative {{ color: #ef4444; }}

        /* グラフセクション */
        .charts-section {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .chart-card {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 20px;
        }}
        .chart-card h3 {{
            font-size: 0.9rem;
            color: #888;
            margin-bottom: 15px;
        }}
        /* バーグラフ */
        .bar-chart {{
            display: flex;
            flex-direction: column;
            gap: 12px;
        }}
        .bar-row {{
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .bar-label {{
            width: 70px;
            font-size: 0.8rem;
            color: #aaa;
        }}
        .bar-container {{
            flex: 1;
            height: 18px;
            background: #222;
            border-radius: 4px;
            overflow: hidden;
            position: relative;
        }}
        .bar {{
            height: 100%;
            border-radius: 4px;
            transition: width 0.5s ease;
        }}
        .bar.positive-bar {{
            background: linear-gradient(90deg, #166534, #22c55e);
        }}
        .bar.negative-bar {{
            background: linear-gradient(90deg, #991b1b, #ef4444);
        }}
        .stacked-bar {{
            display: flex;
            height: 100%;
        }}
        .stacked-bar .segment {{
            height: 100%;
            transition: width 0.5s ease;
        }}
        .stacked-bar .segment.long {{
            background: linear-gradient(90deg, #c2410c, #f97316);
            border-radius: 4px 0 0 4px;
        }}
        .stacked-bar .segment.short {{
            background: linear-gradient(90deg, #0d9488, #14b8a6);
            border-radius: 0 4px 4px 0;
        }}
        .stacked-bar .segment.long:only-child {{
            border-radius: 4px;
        }}
        .stacked-bar .segment.short:first-child {{
            border-radius: 4px 0 0 4px;
        }}
        .legend {{
            display: flex;
            gap: 15px;
            margin-bottom: 10px;
            font-size: 0.75rem;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 5px;
        }}
        .legend-color {{
            width: 12px;
            height: 12px;
            border-radius: 2px;
        }}
        .legend-color.long {{
            background: #f97316;
        }}
        .legend-color.short {{
            background: #14b8a6;
        }}
        .bar-value {{
            width: 100px;
            text-align: right;
            font-size: 0.85rem;
            font-weight: bold;
        }}
        /* 中央基準バーグラフ */
        .center-bar-row {{
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 8px;
        }}
        .center-bar-label {{
            width: 60px;
            font-size: 0.8rem;
            color: #aaa;
            text-align: right;
        }}
        .center-bar-container {{
            flex: 1;
            display: flex;
            height: 28px;
        }}
        .center-bar-left {{
            flex: 1;
            display: flex;
            justify-content: flex-end;
            background: #1a1a1a;
            border-radius: 4px 0 0 4px;
        }}
        .center-bar-right {{
            flex: 1;
            display: flex;
            justify-content: flex-start;
            background: #1a1a1a;
            border-radius: 0 4px 4px 0;
        }}
        .center-bar-divider {{
            width: 2px;
            background: #444;
        }}
        .center-bar {{
            height: 100%;
            transition: width 0.5s ease;
        }}
        .center-bar.left {{
            border-radius: 4px 0 0 4px;
        }}
        .center-bar.right {{
            border-radius: 0 4px 4px 0;
        }}
        .center-bar-value {{
            width: 110px;
            font-size: 0.85rem;
            font-weight: bold;
        }}
        /* ゲージ */
        .gauge {{
            position: relative;
            height: 120px;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
        }}
        .gauge-circle {{
            width: 100px;
            height: 100px;
            border-radius: 50%;
            background: conic-gradient(
                var(--gauge-color) calc(var(--gauge-percent) * 3.6deg),
                #333 0deg
            );
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        .gauge-inner {{
            width: 70px;
            height: 70px;
            border-radius: 50%;
            background: #1a1a1a;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.2rem;
            font-weight: bold;
        }}
        .gauge-label {{
            margin-top: 10px;
            font-size: 0.8rem;
            color: #888;
        }}
        /* インサイト */
        .insight-card {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 30px;
        }}
        .insight-card h3 {{
            font-size: 0.9rem;
            color: #888;
            margin-bottom: 15px;
        }}
        .insight-list {{
            list-style: none;
            padding: 0;
        }}
        .insight-list li {{
            padding: 8px 0;
            border-bottom: 1px solid #222;
            font-size: 0.85rem;
            color: #ccc;
        }}
        .insight-list li:last-child {{
            border-bottom: none;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.85rem;
            table-layout: fixed;
        }}
        th, td {{
            padding: 10px 12px;
            border-bottom: 1px solid #222;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        th {{
            background: #1a1a1a;
            color: #888;
            font-weight: 500;
            position: sticky;
            top: 0;
            text-align: left;
        }}
        th.num, td.num {{
            text-align: right !important;
        }}
        .num {{
            text-align: right !important;
        }}
        tr:hover {{
            background: #1a1a1a;
        }}
        /* カラム幅固定 */
        .col-code {{ width: 70px; }}
        .col-name {{ width: 140px; max-width: 140px; }}
        .col-position {{ width: 70px; }}
        .col-qty {{ width: 90px; }}
        .col-price {{ width: 120px; }}
        .col-pl {{ width: 110px; }}
        .generated {{
            margin-top: 20px;
            font-size: 0.75rem;
            color: #666;
            text-align: right;
        }}
        /* タブ */
        .tabs {{
            display: flex;
            gap: 5px;
            margin-bottom: 20px;
        }}
        .tab-btn {{
            padding: 8px 20px;
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 6px;
            color: #888;
            cursor: pointer;
            font-size: 0.9rem;
            transition: all 0.2s;
        }}
        .tab-btn:hover {{
            background: #222;
            color: #ccc;
        }}
        .tab-btn.active {{
            background: #333;
            color: #fff;
            border-color: #555;
        }}
        .tab-content {{
            display: none;
        }}
        .tab-content.active {{
            display: block;
        }}
        /* 折りたたみ */
        details summary::-webkit-details-marker {{
            display: none;
        }}
        details[open] summary .arrow {{
            display: inline-block;
            transform: rotate(90deg);
        }}
        .arrow {{
            display: inline-block;
            transition: transform 0.2s;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>株式取引結果 (取得日/建日: 2025/11/04以降)</h1>

        <div class="summary">
            <div class="summary-card">
                <div class="label">損益</div>
                <div class="row"><span class="row-label">合計</span><span class="value {'positive' if total_profit >= 0 else 'negative'}">{total_profit:+,.0f}円</span></div>
                <div class="row"><span class="row-label">ロング</span><span class="value {'positive' if long_profit >= 0 else 'negative'}">{long_profit:+,.0f}円</span></div>
                <div class="row"><span class="row-label">ショート</span><span class="value {'positive' if short_profit >= 0 else 'negative'}">{short_profit:+,.0f}円</span></div>
            </div>
            <div class="summary-card">
                <div class="label">取引数</div>
                <div class="row"><span class="row-label">合計</span><span class="value">{total_count}件</span></div>
                <div class="row"><span class="row-label">ロング</span><span class="value">{long_count}件</span></div>
                <div class="row"><span class="row-label">ショート</span><span class="value">{short_count}件</span></div>
            </div>
            <div class="summary-card">
                <div class="label">勝敗</div>
                <div class="row"><span class="row-label">合計</span><span class="value"><span class="positive">{win_count}勝</span> / <span class="negative">{lose_count}敗</span></span></div>
                <div class="row"><span class="row-label">ロング</span><span class="value"><span class="positive">{long_win}勝</span> / <span class="negative">{long_lose}敗</span></span></div>
                <div class="row"><span class="row-label">ショート</span><span class="value"><span class="positive">{short_win}勝</span> / <span class="negative">{short_lose}敗</span></span></div>
            </div>
            <div class="summary-card">
                <div class="label">勝率</div>
                <div class="row"><span class="row-label">合計</span><span class="value {win_rate_class(win_rate)}">{win_rate:.1f}%</span></div>
                <div class="row"><span class="row-label">ロング</span><span class="value {win_rate_class(long_win_rate)}">{long_win_rate:.1f}%</span></div>
                <div class="row"><span class="row-label">ショート</span><span class="value {win_rate_class(short_win_rate)}">{short_win_rate:.1f}%</span></div>
            </div>
        </div>

        <!-- グラフセクション -->
        <div class="charts-section">
            <div class="chart-card">
                <h3>損益比較</h3>
                <div style="margin-bottom: 15px;">
"""

# 中央基準バーグラフ
max_abs = max(abs(long_profit), abs(short_profit))
long_pct = abs(long_profit) / max_abs * 100 if max_abs > 0 else 0
short_pct = abs(short_profit) / max_abs * 100 if max_abs > 0 else 0

# ロング行
if long_profit >= 0:
    html_content += f"""                    <div class="center-bar-row">
                        <span class="center-bar-label">ロング</span>
                        <div class="center-bar-container">
                            <div class="center-bar-left"></div>
                            <div class="center-bar-divider"></div>
                            <div class="center-bar-right">
                                <div class="center-bar right" style="width: {long_pct:.0f}%; background: linear-gradient(90deg, #166534, #22c55e);"></div>
                            </div>
                        </div>
                        <span class="center-bar-value positive">{long_profit:+,.0f}円</span>
                    </div>
"""
else:
    html_content += f"""                    <div class="center-bar-row">
                        <span class="center-bar-label">ロング</span>
                        <div class="center-bar-container">
                            <div class="center-bar-left">
                                <div class="center-bar left" style="width: {long_pct:.0f}%; background: linear-gradient(270deg, #991b1b, #ef4444);"></div>
                            </div>
                            <div class="center-bar-divider"></div>
                            <div class="center-bar-right"></div>
                        </div>
                        <span class="center-bar-value negative">{long_profit:+,.0f}円</span>
                    </div>
"""

# ショート行
if short_profit >= 0:
    html_content += f"""                    <div class="center-bar-row">
                        <span class="center-bar-label">ショート</span>
                        <div class="center-bar-container">
                            <div class="center-bar-left"></div>
                            <div class="center-bar-divider"></div>
                            <div class="center-bar-right">
                                <div class="center-bar right" style="width: {short_pct:.0f}%; background: linear-gradient(90deg, #166534, #22c55e);"></div>
                            </div>
                        </div>
                        <span class="center-bar-value positive">{short_profit:+,.0f}円</span>
                    </div>
"""
else:
    html_content += f"""                    <div class="center-bar-row">
                        <span class="center-bar-label">ショート</span>
                        <div class="center-bar-container">
                            <div class="center-bar-left">
                                <div class="center-bar left" style="width: {short_pct:.0f}%; background: linear-gradient(270deg, #991b1b, #ef4444);"></div>
                            </div>
                            <div class="center-bar-divider"></div>
                            <div class="center-bar-right"></div>
                        </div>
                        <span class="center-bar-value negative">{short_profit:+,.0f}円</span>
                    </div>
"""

html_content += f"""                </div>
            </div>

            <div class="chart-card">
                <h3>勝率ゲージ</h3>
                <div style="display: flex; justify-content: space-around;">
                    <div class="gauge">
                        <div class="gauge-circle" style="--gauge-percent: {long_win_rate}; --gauge-color: {'#22c55e' if long_win_rate >= 50 else '#ef4444'};">
                            <div class="gauge-inner {'positive' if long_win_rate >= 50 else 'negative'}">{long_win_rate:.0f}%</div>
                        </div>
                        <div class="gauge-label">ロング</div>
                    </div>
                    <div class="gauge">
                        <div class="gauge-circle" style="--gauge-percent: {short_win_rate}; --gauge-color: {'#22c55e' if short_win_rate >= 50 else '#ef4444'};">
                            <div class="gauge-inner {'positive' if short_win_rate >= 50 else 'negative'}">{short_win_rate:.0f}%</div>
                        </div>
                        <div class="gauge-label">ショート</div>
                    </div>
                </div>
            </div>

            <div class="chart-card">
                <h3>損益水準分布</h3>
                <div class="legend">
                    <div class="legend-item"><div class="legend-color long"></div><span>ロング</span></div>
                    <div class="legend-item"><div class="legend-color short"></div><span>ショート</span></div>
                </div>
                <div class="bar-chart">
"""

# 損益水準バーグラフ（スタックドバー）
max_loss_count = max([ls["count"] for ls in loss_stats]) if loss_stats else 1
for ls in loss_stats:
    total_width = ls["count"] / max_loss_count * 100
    long_pct = (ls["long"] / ls["count"] * 100) if ls["count"] > 0 else 0
    short_pct = (ls["short"] / ls["count"] * 100) if ls["count"] > 0 else 0
    total_class = "positive" if ls["total"] >= 0 else "negative"
    html_content += f"""                    <div class="bar-row">
                        <span class="bar-label" style="width: 80px;">{ls['range']}</span>
                        <div class="bar-container" style="max-width: 150px;">
                            <div class="stacked-bar" style="width: {total_width:.0f}%">
                                <div class="segment long" style="width: {long_pct:.0f}%" title="ロング {ls['long']}件"></div>
                                <div class="segment short" style="width: {short_pct:.0f}%" title="ショート {ls['short']}件"></div>
                            </div>
                        </div>
                        <span class="bar-value" style="width: 80px;"><span style="color: #f97316;">{ls['long']}L</span> / <span style="color: #14b8a6;">{ls['short']}S</span></span>
                        <span class="bar-value {total_class}" style="width: 100px;">{ls['total']:+,.0f}円</span>
                    </div>
"""

html_content += f"""                </div>
            </div>

            <div class="chart-card">
                <h3>インサイト</h3>
                <ul class="insight-list">
                    <li>ショート勝率 {short_win_rate:.0f}% - 得意パターン</li>
                    <li>合計 {'+' if total_profit >= 0 else ''}{total_profit:,.0f}円</li>
                    <li>ロング損切り-3万で圧縮可</li>
                    <li>ショートで利益を積み心理的余裕確保</li>
                </ul>
            </div>
        </div>

        <h2>取引一覧</h2>
        <div class="tabs">
            <button class="tab-btn active" onclick="showTab('daily')">日別</button>
            <button class="tab-btn" onclick="showTab('weekly')">週別</button>
            <button class="tab-btn" onclick="showTab('monthly')">月別</button>
            <button class="tab-btn" onclick="showTab('bystock')">銘柄別</button>
        </div>

        <!-- 日別ビュー -->
        <div id="daily" class="tab-content active">
"""

# 約定日別にグループ化
dates = daily_stock["約定日"].unique()
for date in dates:
    date_df = daily_stock[daily_stock["約定日"] == date]
    date_total = date_df["実現損益"].sum()
    date_long = date_df[date_df["売買"] == "ロング"]["実現損益"].sum()
    date_short = date_df[date_df["売買"] == "ショート"]["実現損益"].sum()

    total_class = "positive" if date_total >= 0 else "negative"
    long_class = "positive" if date_long >= 0 else "negative"
    short_class = "positive" if date_short >= 0 else "negative"

    html_content += f"""
        <details style="margin-bottom: 15px;">
            <summary style="font-size: 0.95rem; color: #fff; padding: 8px 0; border-bottom: 1px solid #333; cursor: pointer; list-style: none;">
                <span class="arrow" style="margin-right: 8px;">▶</span>{date}　<span class="{total_class}">{date_total:+,.0f}円</span>
                <span style="font-size: 0.8rem; color: #888; margin-left: 15px;">（ロング:<span class="{long_class}">{date_long:+,.0f}円</span>, ショート:<span class="{short_class}">{date_short:+,.0f}円</span>）</span>
            </summary>
            <table style="margin-top: 10px;">
                <thead>
                    <tr>
                        <th class="col-code">コード</th>
                        <th class="col-name">銘柄名</th>
                        <th class="col-position">売買</th>
                        <th class="col-qty num">数量(株)</th>
                        <th class="col-price num">平均取得価額(円)</th>
                        <th class="col-price num">平均単価(円)</th>
                        <th class="col-pl num">実現損益(円)</th>
                    </tr>
                </thead>
                <tbody>
"""
    for _, row in date_df.iterrows():
        profit_class = "positive" if row["実現損益"] > 0 else "negative" if row["実現損益"] < 0 else ""
        position_color = "#f97316" if row["売買"] == "ロング" else "#14b8a6"
        # -10,000以下の損失は銘柄名を赤文字
        name_style = "color: #ef4444;" if row["実現損益"] <= -10000 else ""
        # 銘柄名を12文字に制限
        name = row['銘柄名']
        name_display = name[:12] + "..." if len(name) > 12 else name
        html_content += f"""                    <tr>
                        <td class="col-code">{row['コード']}</td>
                        <td class="col-name" title="{name}" style="{name_style}">{name_display}</td>
                        <td class="col-position" style="color: {position_color};">{row['売買']}</td>
                        <td class="col-qty num">{row['数量']:,}</td>
                        <td class="col-price num">{row['平均取得価額']:,.1f}</td>
                        <td class="col-price num">{row['平均単価']:,.1f}</td>
                        <td class="col-pl num {profit_class}">{row['実現損益']:+,.0f}</td>
                    </tr>
"""
    html_content += f"""                </tbody>
            </table>
        </details>
"""

# 日別ビュー終了
html_content += """        </div>

        <!-- 週別ビュー -->
        <div id="weekly" class="tab-content">
"""

# 週別集計をソート（新しい順）
weeks = sorted(weekly_stock["週"].unique(), reverse=True)
for week in weeks:
    week_df = weekly_stock[weekly_stock["週"] == week]
    week_total = week_df["実現損益"].sum()
    week_long = week_df[week_df["売買"] == "ロング"]["実現損益"].sum()
    week_short = week_df[week_df["売買"] == "ショート"]["実現損益"].sum()

    total_class = "positive" if week_total >= 0 else "negative"
    long_class = "positive" if week_long >= 0 else "negative"
    short_class = "positive" if week_short >= 0 else "negative"

    html_content += f"""
        <details style="margin-bottom: 15px;">
            <summary style="font-size: 0.95rem; color: #fff; padding: 8px 0; border-bottom: 1px solid #333; cursor: pointer; list-style: none;">
                <span class="arrow" style="margin-right: 8px;">▶</span>{week}　<span class="{total_class}">{week_total:+,.0f}円</span>
                <span style="font-size: 0.8rem; color: #888; margin-left: 15px;">（ロング:<span class="{long_class}">{week_long:+,.0f}円</span>, ショート:<span class="{short_class}">{week_short:+,.0f}円</span>）</span>
            </summary>
            <table style="margin-top: 10px;">
                <thead>
                    <tr>
                        <th class="col-code">コード</th>
                        <th class="col-name">銘柄名</th>
                        <th class="col-position">売買</th>
                        <th class="col-qty num">数量(株)</th>
                        <th class="col-price num">平均取得価額(円)</th>
                        <th class="col-price num">平均単価(円)</th>
                        <th class="col-pl num">実現損益(円)</th>
                    </tr>
                </thead>
                <tbody>
"""
    for _, row in week_df.iterrows():
        profit_class = "positive" if row["実現損益"] > 0 else "negative" if row["実現損益"] < 0 else ""
        position_color = "#f97316" if row["売買"] == "ロング" else "#14b8a6"
        name_style = "color: #ef4444;" if row["実現損益"] <= -10000 else ""
        name = row['銘柄名']
        name_display = name[:12] + "..." if len(name) > 12 else name
        html_content += f"""                    <tr>
                        <td class="col-code">{row['コード']}</td>
                        <td class="col-name" title="{name}" style="{name_style}">{name_display}</td>
                        <td class="col-position" style="color: {position_color};">{row['売買']}</td>
                        <td class="col-qty num">{row['数量']:,}</td>
                        <td class="col-price num">{row['平均取得価額']:,.1f}</td>
                        <td class="col-price num">{row['平均単価']:,.1f}</td>
                        <td class="col-pl num {profit_class}">{row['実現損益']:+,.0f}</td>
                    </tr>
"""
    html_content += f"""                </tbody>
            </table>
        </details>
"""

# 週別ビュー終了
html_content += """        </div>

        <!-- 月別ビュー -->
        <div id="monthly" class="tab-content">
"""

# 月別集計をソート（新しい順）
months = sorted(monthly_stock["月"].unique(), reverse=True)
for month in months:
    month_df = monthly_stock[monthly_stock["月"] == month]
    month_total = month_df["実現損益"].sum()
    month_long = month_df[month_df["売買"] == "ロング"]["実現損益"].sum()
    month_short = month_df[month_df["売買"] == "ショート"]["実現損益"].sum()

    total_class = "positive" if month_total >= 0 else "negative"
    long_class = "positive" if month_long >= 0 else "negative"
    short_class = "positive" if month_short >= 0 else "negative"

    html_content += f"""
        <details style="margin-bottom: 15px;">
            <summary style="font-size: 0.95rem; color: #fff; padding: 8px 0; border-bottom: 1px solid #333; cursor: pointer; list-style: none;">
                <span class="arrow" style="margin-right: 8px;">▶</span>{month}　<span class="{total_class}">{month_total:+,.0f}円</span>
                <span style="font-size: 0.8rem; color: #888; margin-left: 15px;">（ロング:<span class="{long_class}">{month_long:+,.0f}円</span>, ショート:<span class="{short_class}">{month_short:+,.0f}円</span>）</span>
            </summary>
            <table style="margin-top: 10px;">
                <thead>
                    <tr>
                        <th class="col-code">コード</th>
                        <th class="col-name">銘柄名</th>
                        <th class="col-position">売買</th>
                        <th class="col-qty num">数量(株)</th>
                        <th class="col-price num">平均取得価額(円)</th>
                        <th class="col-price num">平均単価(円)</th>
                        <th class="col-pl num">実現損益(円)</th>
                    </tr>
                </thead>
                <tbody>
"""
    for _, row in month_df.iterrows():
        profit_class = "positive" if row["実現損益"] > 0 else "negative" if row["実現損益"] < 0 else ""
        position_color = "#f97316" if row["売買"] == "ロング" else "#14b8a6"
        name_style = "color: #ef4444;" if row["実現損益"] <= -10000 else ""
        name = row['銘柄名']
        name_display = name[:12] + "..." if len(name) > 12 else name
        html_content += f"""                    <tr>
                        <td class="col-code">{row['コード']}</td>
                        <td class="col-name" title="{name}" style="{name_style}">{name_display}</td>
                        <td class="col-position" style="color: {position_color};">{row['売買']}</td>
                        <td class="col-qty num">{row['数量']:,}</td>
                        <td class="col-price num">{row['平均取得価額']:,.1f}</td>
                        <td class="col-price num">{row['平均単価']:,.1f}</td>
                        <td class="col-pl num {profit_class}">{row['実現損益']:+,.0f}</td>
                    </tr>
"""
    html_content += f"""                </tbody>
            </table>
        </details>
"""

# 月別ビュー終了
html_content += """        </div>

        <!-- 銘柄別ビュー -->
        <div id="bystock" class="tab-content">
"""

# 銘柄別集計（銘柄ごとに日別売買別の取引を表示）
# 銘柄別の合計損益でソート
stock_totals = daily_stock.groupby(["コード", "銘柄名"]).agg({
    "実現損益": "sum"
}).reset_index()
stock_totals = stock_totals.sort_values("実現損益", ascending=False)

for _, stock_row in stock_totals.iterrows():
    code = stock_row["コード"]
    name = stock_row["銘柄名"]

    stock_df = daily_stock[(daily_stock["コード"] == code) & (daily_stock["銘柄名"] == name)].copy()
    stock_df = stock_df.sort_values("約定日", ascending=False)

    stock_total = stock_df["実現損益"].sum()
    stock_long = stock_df[stock_df["売買"] == "ロング"]["実現損益"].sum()
    stock_short = stock_df[stock_df["売買"] == "ショート"]["実現損益"].sum()

    total_class = "positive" if stock_total >= 0 else "negative"
    long_class = "positive" if stock_long >= 0 else "negative"
    short_class = "positive" if stock_short >= 0 else "negative"

    name_display = name[:12] + "..." if len(name) > 12 else name

    html_content += f"""
        <details style="margin-bottom: 15px;">
            <summary style="font-size: 0.95rem; color: #fff; padding: 8px 0; border-bottom: 1px solid #333; cursor: pointer; list-style: none;">
                <span class="arrow" style="margin-right: 8px;">▶</span>{code}　{name_display}　<span class="{total_class}">{stock_total:+,.0f}円</span>
                <span style="font-size: 0.8rem; color: #888; margin-left: 15px;">（ロング:<span class="{long_class}">{stock_long:+,.0f}円</span>, ショート:<span class="{short_class}">{stock_short:+,.0f}円</span>）</span>
            </summary>
            <table style="margin-top: 10px;">
                <thead>
                    <tr>
                        <th style="width: 100px;">日付</th>
                        <th class="col-position">売買</th>
                        <th class="col-qty num">数量(株)</th>
                        <th class="col-price num">平均取得価額(円)</th>
                        <th class="col-price num">平均単価(円)</th>
                        <th class="col-pl num">実現損益(円)</th>
                    </tr>
                </thead>
                <tbody>
"""
    for _, row in stock_df.iterrows():
        profit_class = "positive" if row["実現損益"] > 0 else "negative" if row["実現損益"] < 0 else ""
        position_color = "#f97316" if row["売買"] == "ロング" else "#14b8a6"
        html_content += f"""                    <tr>
                        <td style="width: 100px;">{row['約定日']}</td>
                        <td class="col-position" style="color: {position_color};">{row['売買']}</td>
                        <td class="col-qty num">{row['数量']:,}</td>
                        <td class="col-price num">{row['平均取得価額']:,.1f}</td>
                        <td class="col-price num">{row['平均単価']:,.1f}</td>
                        <td class="col-pl num {profit_class}">{row['実現損益']:+,.0f}</td>
                    </tr>
"""
    html_content += f"""                </tbody>
            </table>
        </details>
"""

# 銘柄別ビュー終了
html_content += """        </div>
"""

html_content += f"""
        <div class="generated">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
    </div>

    <script>
    function showTab(tabId) {{
        // すべてのタブコンテンツを非表示
        document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
        // すべてのタブボタンを非アクティブ
        document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
        // 指定タブを表示
        document.getElementById(tabId).classList.add('active');
        // クリックしたボタンをアクティブに
        event.target.classList.add('active');
    }}
    </script>
</body>
</html>
"""

# ファイル出力
OUTPUT_PATH.write_text(html_content, encoding="utf-8")
print(f"HTMLファイルを出力しました: {OUTPUT_PATH}")
print(f"抽出件数: {total_count}件")
print(f"合計損益: {total_profit:+,.0f}円")
print(f"ロング: {long_count}件 / {long_profit:+,.0f}円")
print(f"ショート: {short_count}件 / {short_profit:+,.0f}円")

# =============================================================================
# Parquetファイル生成・S3アップロード
# =============================================================================

# サマリー統計を含むDataFrameを作成
summary_data = {
    "metric": [
        "total_profit", "total_count", "win_count", "lose_count", "win_rate",
        "long_profit", "long_count", "long_win", "long_lose", "long_win_rate",
        "short_profit", "short_count", "short_win", "short_lose", "short_win_rate"
    ],
    "value": [
        total_profit, total_count, win_count, lose_count, win_rate,
        long_profit, long_count, long_win, long_lose, long_win_rate,
        short_profit, short_count, short_win, short_lose, short_win_rate
    ]
}
summary_df = pd.DataFrame(summary_data)

# daily_stock を保存用に整形（約定日を文字列から日付型に戻す）
parquet_df = daily_stock.copy()
parquet_df["約定日"] = pd.to_datetime(parquet_df["約定日"], format="%Y/%m/%d")

# Parquetファイル出力
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
parquet_df.to_parquet(PARQUET_PATH, index=False)
print(f"\nParquetファイルを出力しました: {PARQUET_PATH}")
print(f"  行数: {len(parquet_df)}行")
print(f"  カラム: {list(parquet_df.columns)}")

# サマリーも別ファイルで保存
summary_path = PARQUET_DIR / "stock_results_summary.parquet"
summary_df.to_parquet(summary_path, index=False)
print(f"サマリーファイルを出力しました: {summary_path}")

# 日経VI CSV → Parquet
if NIKKEIVI_CSV_PATH.exists():
    print(f"\n日経VIデータを処理中: {NIKKEIVI_CSV_PATH}")
    vi_df = pd.read_csv(NIKKEIVI_CSV_PATH)
    vi_df.columns = ["date", "open", "high", "low", "close"]
    vi_df["date"] = pd.to_datetime(vi_df["date"])
    vi_df = vi_df.sort_values("date").reset_index(drop=True)
    vi_df.to_parquet(NIKKEIVI_PARQUET_PATH, index=False)
    print(f"日経VI Parquet出力: {NIKKEIVI_PARQUET_PATH}")
    print(f"  行数: {len(vi_df)}行, 期間: {vi_df['date'].min().date()} ~ {vi_df['date'].max().date()}")
else:
    print(f"\n[SKIP] {NIKKEIVI_CSV_PATH} が見つかりません")
    vi_df = None

# S3からmanifest.jsonをダウンロードして最新を取得
print("\nS3からmanifest.jsonを取得中...")
s3_cfg = load_s3_config()

if s3_cfg.bucket:
    # S3から一時ファイルにダウンロード
    temp_manifest = PARQUET_DIR / "manifest.json.s3tmp"
    downloaded = download_file(s3_cfg, "manifest.json", temp_manifest)

    if downloaded and temp_manifest.exists():
        # S3版を読み込み
        with open(temp_manifest, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        temp_manifest.unlink()  # 一時ファイル削除
        print("[INFO] S3のmanifest.jsonをベースに更新します")
    else:
        # S3にない場合はローカルを使用
        if MANIFEST_PATH.exists():
            with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            print("[INFO] ローカルのmanifest.jsonをベースに更新します")
        else:
            manifest = {"generated_at": None, "files": {}}
            print("[INFO] 新規manifest.jsonを作成します")
else:
    # S3設定なしの場合はローカルを使用
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    else:
        manifest = {"generated_at": None, "files": {}}

now = datetime.now().isoformat()

# stock_results.parquet の情報追加
manifest["files"]["stock_results.parquet"] = {
    "exists": True,
    "size_bytes": PARQUET_PATH.stat().st_size,
    "row_count": len(parquet_df),
    "columns": list(parquet_df.columns),
    "updated_at": now
}

# stock_results_summary.parquet の情報追加
manifest["files"]["stock_results_summary.parquet"] = {
    "exists": True,
    "size_bytes": summary_path.stat().st_size,
    "row_count": len(summary_df),
    "columns": list(summary_df.columns),
    "updated_at": now
}

# 日経VI
if vi_df is not None and NIKKEIVI_PARQUET_PATH.exists():
    manifest["files"]["nikkei_vi_max_1d.parquet"] = {
        "exists": True,
        "size_bytes": NIKKEIVI_PARQUET_PATH.stat().st_size,
        "row_count": len(vi_df),
        "columns": list(vi_df.columns),
        "updated_at": now
    }

manifest["generated_at"] = now

with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, ensure_ascii=False)
print(f"manifest.jsonを更新しました: {MANIFEST_PATH}")

# S3アップロード
print("\nS3へアップロード中...")
if s3_cfg.bucket:
    upload_file(s3_cfg, PARQUET_PATH, "stock_results.parquet")
    upload_file(s3_cfg, summary_path, "stock_results_summary.parquet")
    if NIKKEIVI_PARQUET_PATH.exists():
        upload_file(s3_cfg, NIKKEIVI_PARQUET_PATH, "nikkei_vi_max_1d.parquet")
    upload_file(s3_cfg, MANIFEST_PATH, "manifest.json")

    # App Runnerのキャッシュをリフレッシュ
    import urllib.request
    import urllib.error

    API_URL = "https://muuq3bv2n2.ap-northeast-1.awsapprunner.com/api/dev/stock-results/refresh"
    print("\nApp Runnerのキャッシュをリフレッシュ中...")
    try:
        req = urllib.request.Request(API_URL, method="POST")
        with urllib.request.urlopen(req, timeout=30) as response:
            result = response.read().decode("utf-8")
            print(f"[OK] キャッシュリフレッシュ完了: {result}")
    except urllib.error.URLError as e:
        print(f"[WARNING] キャッシュリフレッシュ失敗: {e}")
    except Exception as e:
        print(f"[WARNING] キャッシュリフレッシュ失敗: {e}")
else:
    print("[INFO] S3バケットが設定されていません。アップロードをスキップしました。")
