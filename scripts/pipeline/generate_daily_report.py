#!/usr/bin/env python3
"""
デイリーマーケットレポートHTML生成スクリプト

data/parquetから最新データを読み込み、HTMLレポートを生成する。
- 為替（ドル円、ユーロ円）
- 先物（日経225先物）
- 指数（日経225）
- Grok銘柄選定（Top5）
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

# パス設定
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "parquet"
OUTPUT_DIR = BASE_DIR / "improvement" / "output"


def load_parquet(filename: str) -> pd.DataFrame | None:
    """parquetファイルを読み込む"""
    path = DATA_DIR / filename
    if not path.exists():
        print(f"Warning: {path} not found")
        return None
    try:
        return pd.read_parquet(str(path), engine="pyarrow")
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return None


def get_latest_price(df: pd.DataFrame, ticker: str, days_back: int = 5) -> dict[str, Any]:
    """指定ティッカーの最新価格データを取得"""
    if df is None:
        return {}

    mask = df["ticker"] == ticker
    subset = df[mask].copy()
    if subset.empty:
        return {}

    # dateでソート
    subset = subset.sort_values("date", ascending=False)

    # 最新のClose値を取得（NaNでないもの）
    latest_row = None
    prev_row = None

    for idx, row in subset.head(days_back).iterrows():
        if pd.notna(row.get("Close")):
            if latest_row is None:
                latest_row = row
            elif prev_row is None:
                prev_row = row
                break

    if latest_row is None:
        return {}

    result = {
        "ticker": ticker,
        "date": str(latest_row["date"])[:10] if pd.notna(latest_row.get("date")) else None,
        "close": latest_row.get("Close"),
        "open": latest_row.get("Open"),
        "high": latest_row.get("High"),
        "low": latest_row.get("Low"),
    }

    # 前日比
    if prev_row is not None and pd.notna(prev_row.get("Close")):
        prev_close = prev_row["Close"]
        if prev_close and prev_close != 0:
            result["prev_close"] = prev_close
            result["change"] = latest_row["Close"] - prev_close
            result["change_pct"] = (latest_row["Close"] - prev_close) / prev_close * 100

    return result


def get_grok_top_stocks(n: int = 5) -> list[dict[str, Any]]:
    """Grok銘柄選定Top N を取得"""
    df = load_parquet("grok_top_stocks.parquet")
    if df is None or df.empty:
        return []

    # 最新日付を取得
    latest_date = df["target_date"].max()
    latest = df[df["target_date"] == latest_date].copy()

    # 重複除去（rank, tickerで）
    latest = latest.drop_duplicates(subset=["rank", "ticker"])
    latest = latest.sort_values("rank").head(n)

    return [
        {
            "rank": int(row["rank"]),
            "ticker": row["ticker"],
            "stock_name": row["stock_name"],
            "score": row.get("selection_score"),
            "categories": row.get("categories"),
        }
        for _, row in latest.iterrows()
    ]


def get_grok_trending(n: int = 5) -> list[dict[str, Any]]:
    """Grok Trending データ取得（Top Stocksがない場合のフォールバック）"""
    df = load_parquet("grok_trending.parquet")
    if df is None or df.empty:
        return []

    # selection_scoreでソート
    df = df.sort_values("selection_score", ascending=False).head(n)

    return [
        {
            "rank": i + 1,
            "ticker": row["ticker"],
            "stock_name": row["stock_name"],
            "score": row.get("selection_score"),
            "reason": row.get("reason"),
            "key_signal": row.get("key_signal"),
        }
        for i, (_, row) in enumerate(df.iterrows())
    ]


def format_price(val: float | None, decimals: int = 2) -> str:
    """価格をフォーマット"""
    if val is None or pd.isna(val):
        return "—"
    if decimals == 0:
        return f"{val:,.0f}"
    return f"{val:,.{decimals}f}"


def format_change(val: float | None, pct: float | None) -> str:
    """変動をフォーマット"""
    if val is None or pd.isna(val):
        return ""
    sign = "+" if val >= 0 else ""
    result = f"{sign}{val:,.2f}"
    if pct is not None and not pd.isna(pct):
        result += f" ({sign}{pct:.2f}%)"
    return result


def get_change_class(val: float | None) -> str:
    """変動に応じたCSSクラス"""
    if val is None or pd.isna(val):
        return ""
    return "positive" if val >= 0 else "negative"


def generate_html(report_date: str = None) -> str:
    """HTMLレポートを生成"""
    if report_date is None:
        report_date = datetime.now().strftime("%Y/%m/%d")

    # データ読み込み
    currency_df = load_parquet("currency_prices_max_1d.parquet")
    futures_df = load_parquet("futures_prices_max_1d.parquet")
    index_df = load_parquet("index_prices_max_1d.parquet")

    # 各データ取得
    usdjpy = get_latest_price(currency_df, "JPY=X")
    eurjpy = get_latest_price(currency_df, "EURJPY=X")
    nkd_futures = get_latest_price(futures_df, "NKD=F")
    nikkei = get_latest_price(index_df, "^N225")

    # Grok銘柄
    grok_stocks = get_grok_top_stocks(5)
    if not grok_stocks:
        grok_stocks = get_grok_trending(5)

    # HTML生成
    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>デイリーマーケットレポート {report_date}</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: 'Helvetica Neue', Arial, 'Hiragino Sans', sans-serif;
            background: #0d1117;
            color: #e6edf3;
            line-height: 1.7;
            padding: 20px;
        }}
        .container {{ max-width: 900px; margin: 0 auto; }}

        h1 {{
            text-align: center;
            color: #fff;
            font-size: 1.8em;
            margin-bottom: 10px;
            border-bottom: 2px solid #58a6ff;
            padding-bottom: 15px;
        }}
        .subtitle {{
            text-align: center;
            color: #8b949e;
            margin-bottom: 30px;
        }}

        .section {{
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .section h2 {{
            color: #58a6ff;
            font-size: 1.2em;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid #30363d;
        }}
        .section h3 {{
            color: #7ee787;
            font-size: 1em;
            margin: 15px 0 10px 0;
        }}

        .data-row {{
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #21262d;
        }}
        .data-row:last-child {{ border-bottom: none; }}
        .data-label {{ color: #8b949e; }}
        .data-value {{ font-weight: bold; }}
        .data-value.negative {{ color: #f85149; }}
        .data-value.positive {{ color: #7ee787; }}
        .data-value.warning {{ color: #d29922; }}

        .stock-card {{
            background: #21262d;
            border-radius: 6px;
            padding: 12px 15px;
            margin-bottom: 10px;
        }}
        .stock-card .rank {{
            font-size: 1.2em;
            font-weight: bold;
            color: #f0883e;
            margin-right: 10px;
        }}
        .stock-card .ticker {{
            color: #58a6ff;
            font-weight: bold;
        }}
        .stock-card .name {{
            color: #e6edf3;
            margin-left: 8px;
        }}
        .stock-card .meta {{
            color: #8b949e;
            font-size: 0.85em;
            margin-top: 5px;
        }}
        .stock-card .score {{
            color: #7ee787;
            font-weight: bold;
        }}

        .tag {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.8em;
            margin-right: 5px;
        }}
        .tag.red {{ background: #f8514930; color: #f85149; }}
        .tag.yellow {{ background: #d2992230; color: #d29922; }}
        .tag.blue {{ background: #58a6ff30; color: #58a6ff; }}
        .tag.green {{ background: #7ee78730; color: #7ee787; }}

        .footer {{
            margin-top: 30px;
            padding: 15px;
            background: #0d1117;
            border: 1px solid #30363d;
            border-radius: 8px;
            font-size: 0.85em;
            color: #8b949e;
            text-align: center;
        }}

        .grid-2 {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        @media (max-width: 600px) {{
            .grid-2 {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>デイリーマーケットレポート</h1>
        <p class="subtitle">{report_date} 自動生成</p>

        <div class="grid-2">
            <!-- 為替 -->
            <div class="section">
                <h2>為替</h2>
                <div class="data-row">
                    <span class="data-label">ドル円</span>
                    <span class="data-value">{format_price(usdjpy.get('close'))}円</span>
                </div>
                {f'''<div class="data-row">
                    <span class="data-label">前日比</span>
                    <span class="data-value {get_change_class(usdjpy.get('change'))}">{format_change(usdjpy.get('change'), usdjpy.get('change_pct'))}</span>
                </div>''' if usdjpy.get('change') else ''}
                <div class="data-row">
                    <span class="data-label">ユーロ円</span>
                    <span class="data-value">{format_price(eurjpy.get('close'))}円</span>
                </div>
                {f'''<div class="data-row">
                    <span class="data-label">前日比</span>
                    <span class="data-value {get_change_class(eurjpy.get('change'))}">{format_change(eurjpy.get('change'), eurjpy.get('change_pct'))}</span>
                </div>''' if eurjpy.get('change') else ''}
            </div>

            <!-- 先物・指数 -->
            <div class="section">
                <h2>先物・指数</h2>
                <div class="data-row">
                    <span class="data-label">日経225先物</span>
                    <span class="data-value">{format_price(nkd_futures.get('close'), 0)}円</span>
                </div>
                {f'''<div class="data-row">
                    <span class="data-label">前日比</span>
                    <span class="data-value {get_change_class(nkd_futures.get('change'))}">{format_change(nkd_futures.get('change'), nkd_futures.get('change_pct'))}</span>
                </div>''' if nkd_futures.get('change') else ''}
                <div class="data-row">
                    <span class="data-label">日経225</span>
                    <span class="data-value">{format_price(nikkei.get('close'), 0)}円</span>
                </div>
                {f'''<div class="data-row">
                    <span class="data-label">前日比</span>
                    <span class="data-value {get_change_class(nikkei.get('change'))}">{format_change(nikkei.get('change'), nikkei.get('change_pct'))}</span>
                </div>''' if nikkei.get('change') else ''}
            </div>
        </div>

        <!-- Grok銘柄選定 -->
        <div class="section" style="border-color: #f0883e;">
            <h2 style="color: #f0883e;">Grok銘柄選定 Top 5</h2>
            {generate_grok_stocks_html(grok_stocks)}
        </div>

        <div class="footer">
            <p>Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data source: data/parquet</p>
        </div>
    </div>
</body>
</html>
"""
    return html


def generate_grok_stocks_html(stocks: list[dict[str, Any]]) -> str:
    """Grok銘柄のHTML生成"""
    if not stocks:
        return '<p style="color: #8b949e;">データがありません</p>'

    html_parts = []
    for stock in stocks:
        categories = stock.get("categories") or stock.get("reason") or ""
        key_signal = stock.get("key_signal") or ""

        html_parts.append(f'''
            <div class="stock-card">
                <span class="rank">#{stock["rank"]}</span>
                <span class="ticker">{stock["ticker"]}</span>
                <span class="name">{stock["stock_name"]}</span>
                <div class="meta">
                    {f'<span class="score">Score: {stock["score"]:.0f}</span>' if stock.get("score") else ''}
                    {f' | {categories}' if categories else ''}
                    {f' | <span class="tag blue">{key_signal}</span>' if key_signal else ''}
                </div>
            </div>
        ''')

    return "\n".join(html_parts)


def main():
    """メイン処理"""
    # 出力ディレクトリ作成
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 日付
    today = datetime.now()
    report_date = today.strftime("%Y/%m/%d")
    filename_date = today.strftime("%Y%m%d")

    # HTML生成
    html = generate_html(report_date)

    # 出力
    output_path = OUTPUT_DIR / f"daily_report_{filename_date}.html"
    output_path.write_text(html, encoding="utf-8")
    print(f"Generated: {output_path}")

    return str(output_path)


if __name__ == "__main__":
    main()
