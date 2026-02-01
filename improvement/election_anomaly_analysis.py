#!/usr/bin/env python3
"""
election_anomaly_analysis.py
衆議院解散から投票日までの株式アノマリー分析

分析対象:
- 日経225 (^N225): 1986年以降
- TOPIX ETF (1306.T): 2008年以降
- 政策銘柄・2026テーマ銘柄との相関
"""

from __future__ import annotations

import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# 出力先
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_HTML = OUTPUT_DIR / "election_anomaly.html"

# データパス
DATA_DIR = Path(__file__).parent.parent / "data" / "parquet"

# 衆議院選挙データ（38回〜50回）
ELECTIONS = [
    {"num": 38, "dissolution": "1986-06-02", "election": "1986-07-06", "note": "中曽根ダブル選挙"},
    {"num": 39, "dissolution": "1990-01-24", "election": "1990-02-18", "note": "海部内閣"},
    {"num": 40, "dissolution": "1993-06-18", "election": "1993-07-18", "note": "宮澤内閣"},
    {"num": 41, "dissolution": "1996-09-27", "election": "1996-10-20", "note": "橋本内閣"},
    {"num": 42, "dissolution": "2000-06-02", "election": "2000-06-25", "note": "森内閣"},
    {"num": 43, "dissolution": "2003-10-10", "election": "2003-11-09", "note": "小泉内閣"},
    {"num": 44, "dissolution": "2005-08-08", "election": "2005-09-11", "note": "郵政選挙"},
    {"num": 45, "dissolution": "2009-07-21", "election": "2009-08-30", "note": "政権交代"},
    {"num": 46, "dissolution": "2012-11-16", "election": "2012-12-16", "note": "安倍政権復帰"},
    {"num": 47, "dissolution": "2014-11-21", "election": "2014-12-14", "note": "アベノミクス"},
    {"num": 48, "dissolution": "2017-09-28", "election": "2017-10-22", "note": "安倍内閣"},
    {"num": 49, "dissolution": "2021-10-14", "election": "2021-10-31", "note": "岸田内閣"},
    {"num": 50, "dissolution": "2024-10-09", "election": "2024-10-27", "note": "石破内閣"},
]


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str], list[str], list[str]]:
    """データ読み込み"""
    # インデックス価格（N225, 1306.T）
    index_df = pd.read_parquet(DATA_DIR / "index_prices_max_1d.parquet")

    # 個別銘柄価格
    prices_df = pd.read_parquet(DATA_DIR / "prices_max_1d.parquet")

    # 銘柄マスタ
    stocks_df = pd.read_parquet(DATA_DIR / "all_stocks.parquet")

    # 政策銘柄を抽出
    policy_tickers = stocks_df[
        stocks_df['categories'].apply(lambda x: '政策銘柄' in x if isinstance(x, np.ndarray) else False)
    ]['ticker'].tolist()

    # 2026テーマ銘柄を抽出
    theme_tickers = stocks_df[
        stocks_df['categories'].apply(lambda x: '2026テーマ' in x if isinstance(x, np.ndarray) else False)
    ]['ticker'].tolist()

    # TOPIX Core30を抽出
    core30_tickers = stocks_df[
        stocks_df['categories'].apply(lambda x: 'TOPIX_CORE30' in x if isinstance(x, np.ndarray) else False)
    ]['ticker'].tolist()

    print(f"  政策銘柄: {len(policy_tickers)}件")
    print(f"  2026テーマ: {len(theme_tickers)}件")
    print(f"  TOPIX Core30: {len(core30_tickers)}件")

    return index_df, prices_df, stocks_df, policy_tickers, theme_tickers, core30_tickers


def get_nearest_trading_day(df: pd.DataFrame, target_date: str, direction: str = "forward") -> pd.Timestamp | None:
    """指定日に最も近い取引日を取得"""
    target = pd.Timestamp(target_date)
    if direction == "forward":
        candidates = df[df.index >= target]
        return candidates.index.min() if not candidates.empty else None
    else:
        candidates = df[df.index <= target]
        return candidates.index.max() if not candidates.empty else None


def calculate_returns_for_ticker(df: pd.DataFrame, ticker: str, elections: list[dict]) -> tuple[dict, list]:
    """特定銘柄の各選挙期間の騰落率を計算

    Returns:
        tuple: (returns_dict, details_list)
            - returns_dict: {num: return_pct} for correlation calculation
            - details_list: [{num, start_price, end_price, return_pct, profit_100}] for display
    """
    ticker_df = df[df["ticker"] == ticker].copy()
    if ticker_df.empty:
        return {}, []

    ticker_df = ticker_df.set_index("date").sort_index()
    ticker_df = ticker_df.dropna(subset=["Close"])

    returns = {}
    details = []
    for e in elections:
        dissolution = get_nearest_trading_day(ticker_df, e["dissolution"], "forward")
        election = get_nearest_trading_day(ticker_df, e["election"], "backward")

        if dissolution is None or election is None:
            continue

        try:
            start_price = float(ticker_df.loc[dissolution, "Close"])
            end_price = float(ticker_df.loc[election, "Close"])
            return_pct = (end_price / start_price - 1) * 100
            profit_100 = (end_price - start_price) * 100  # 100 shares profit

            returns[e["num"]] = round(return_pct, 2)
            details.append({
                "num": e["num"],
                "start_price": round(start_price, 1),
                "end_price": round(end_price, 1),
                "return_pct": round(return_pct, 2),
                "profit_100": round(profit_100, 0),
            })
        except (KeyError, IndexError):
            continue

    return returns, details


def calculate_correlation(n225_returns: dict, stock_returns: dict, min_samples: int = 7) -> tuple[float, int]:
    """N225と個別銘柄の相関係数を計算

    min_samples: 最低サンプル数。これ未満の場合は相関を計算しない
                 n=7以上で統計的にある程度信頼できる相関
    """
    common_nums = set(n225_returns.keys()) & set(stock_returns.keys())
    if len(common_nums) < min_samples:
        return 0.0, 0

    n225_vals = [n225_returns[n] for n in sorted(common_nums)]
    stock_vals = [stock_returns[n] for n in sorted(common_nums)]

    corr = np.corrcoef(n225_vals, stock_vals)[0, 1]
    if np.isnan(corr):
        return 0.0, 0
    return round(float(corr), 3), len(common_nums)


def analyze_stocks(
    index_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    stocks_df: pd.DataFrame,
    policy_tickers: list[str],
    theme_tickers: list[str],
    core30_tickers: list[str],
    elections: list[dict],
) -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    """政策銘柄・2026テーマ・TOPIX Core30の相関分析"""

    # N225の騰落率を計算
    n225_df = index_df[index_df["ticker"] == "^N225"].copy()
    n225_df = n225_df.set_index("date").sort_index().dropna(subset=["Close"])

    n225_returns = {}
    n225_results = []
    for e in elections:
        dissolution = get_nearest_trading_day(n225_df, e["dissolution"], "forward")
        election = get_nearest_trading_day(n225_df, e["election"], "backward")
        if dissolution is None or election is None:
            continue
        try:
            start_price = n225_df.loc[dissolution, "Close"]
            end_price = n225_df.loc[election, "Close"]
            return_pct = (end_price / start_price - 1) * 100
            n225_returns[e["num"]] = round(float(return_pct), 2)
            n225_results.append({
                "num": e["num"],
                "dissolution": dissolution.strftime("%Y-%m-%d"),
                "election": election.strftime("%Y-%m-%d"),
                "note": e["note"],
                "start_price": round(float(start_price), 2),
                "end_price": round(float(end_price), 2),
                "return_pct": round(float(return_pct), 2),
                "days": (election - dissolution).days,
                "win": return_pct > 0,
            })
        except (KeyError, IndexError):
            continue

    n225_results_df = pd.DataFrame(n225_results)

    # 各銘柄の相関を計算
    correlation_results = []
    all_tickers = set(policy_tickers + theme_tickers + core30_tickers)

    for ticker in all_tickers:
        stock_returns, stock_details = calculate_returns_for_ticker(prices_df, ticker, elections)
        if not stock_returns:
            continue

        corr, count = calculate_correlation(n225_returns, stock_returns)
        if count == 0:
            continue  # Skip stocks with insufficient data points

        # 銘柄名を取得
        stock_info = stocks_df[stocks_df["ticker"] == ticker]
        stock_name = stock_info["stock_name"].iloc[0] if not stock_info.empty else ticker

        # カテゴリ判定
        is_policy = ticker in policy_tickers
        is_theme = ticker in theme_tickers
        is_core30 = ticker in core30_tickers
        category = []
        if is_core30:
            category.append("Core30")
        if is_policy:
            category.append("政策銘柄")
        if is_theme:
            category.append("2026テーマ")

        # 平均騰落率
        avg_return = np.mean(list(stock_returns.values())) if stock_returns else 0
        win_rate = sum(1 for v in stock_returns.values() if v > 0) / len(stock_returns) * 100 if stock_returns else 0

        # 直近終値を取得
        ticker_df = prices_df[prices_df["ticker"] == ticker]
        latest_close = ticker_df["Close"].iloc[-1] if not ticker_df.empty else None

        correlation_results.append({
            "ticker": ticker,
            "stock_name": stock_name,
            "category": ", ".join(category),
            "correlation": corr,
            "count": count,
            "avg_return": round(avg_return, 2),
            "win_rate": round(win_rate, 1),
            "latest_close": round(float(latest_close), 1) if latest_close else None,
            "returns": stock_returns,
            "details": stock_details,
        })

    # 相関順にソート
    correlation_results.sort(key=lambda x: abs(x["correlation"]), reverse=True)

    return n225_results_df, n225_returns, pd.DataFrame(correlation_results)


def get_daily_returns_aligned(df: pd.DataFrame, elections: list[dict], max_days: int = 30) -> pd.DataFrame:
    """解散日を0日として揃えた日次リターン"""
    all_series = {}

    for e in elections:
        dissolution = get_nearest_trading_day(df, e["dissolution"], "forward")
        if dissolution is None:
            continue

        end_date = dissolution + timedelta(days=max_days + 10)
        period_data = df[(df.index >= dissolution) & (df.index <= end_date)].copy()

        if period_data.empty:
            continue

        close = period_data["Close"]
        normalized = (close / close.iloc[0] - 1) * 100
        normalized.index = range(len(normalized))
        all_series[e["num"]] = normalized[:max_days]

    return pd.DataFrame(all_series)


def create_html_report(
    n225_results: pd.DataFrame,
    n225_returns: dict,
    n225_daily: pd.DataFrame,
    correlation_df: pd.DataFrame,
) -> str:
    """HTMLレポート生成"""

    # 統計計算
    n225_wins = int(n225_results["win"].sum())
    n225_total = len(n225_results)
    n225_avg = n225_results["return_pct"].mean()
    n225_max = n225_results["return_pct"].max()
    n225_min = n225_results["return_pct"].min()

    # 日次リターンをJSON用に変換
    daily_data_js = []
    for num in n225_daily.columns:
        series = n225_daily[num].dropna()
        result = n225_results[n225_results["num"] == num]
        final_ret = float(result["return_pct"].iloc[0]) if not result.empty else 0.0
        note = str(result["note"].iloc[0]) if not result.empty else ""
        daily_data_js.append({
            "num": int(num),
            "note": note,
            "win": bool(final_ret > 0),
            "data": [round(float(v), 2) for v in series.tolist()],
        })

    # N225詳細テーブル行
    n225_rows = ""
    for _, r in n225_results.iterrows():
        color = "#34d399" if r["win"] else "#f87171"
        n225_rows += f"""
        <tr>
            <td class="text-foreground">{r['num']}回</td>
            <td class="text-muted">{r['dissolution']}</td>
            <td class="text-muted">{r['election']}</td>
            <td class="text-muted text-center">{r['days']}日</td>
            <td class="text-right tabular-nums">{r['start_price']:,.0f}</td>
            <td class="text-right tabular-nums">{r['end_price']:,.0f}</td>
            <td class="text-right tabular-nums font-bold" style="color:{color}">{r['return_pct']:+.2f}%</td>
            <td class="text-muted">{r['note']}</td>
        </tr>"""

    # 相関テーブル行（上位20件）
    corr_rows = ""
    for _, r in correlation_df.head(20).iterrows():
        corr_color = "#34d399" if r["correlation"] > 0.5 else "#f87171" if r["correlation"] < -0.5 else "#a1a1aa"
        ret_color = "#34d399" if r["avg_return"] > 0 else "#f87171"
        win_color = "#34d399" if r["win_rate"] > 50 else "#f87171"
        latest = f"{r['latest_close']:,.0f}" if r['latest_close'] else "-"
        corr_rows += f"""
        <tr class="clickable" data-ticker="{r['ticker']}" onclick="showStockDetail('{r['ticker']}')">
            <td class="text-foreground">{r['ticker'].replace('.T', '')}</td>
            <td class="text-foreground">{r['stock_name']}</td>
            <td class="text-right tabular-nums">{latest}</td>
            <td class="text-muted">{r['category']}</td>
            <td class="text-right tabular-nums font-bold" style="color:{corr_color}">{r['correlation']:+.3f}</td>
            <td class="text-right tabular-nums" style="color:{ret_color}">{r['avg_return']:+.2f}%</td>
            <td class="text-right tabular-nums" style="color:{win_color}">{r['win_rate']:.0f}%</td>
            <td class="text-muted text-center">{r['count']}回</td>
        </tr>"""

    # 強い正の相関（>0.6）
    strong_positive = correlation_df[correlation_df["correlation"] > 0.6]
    strong_positive_rows = ""
    for _, r in strong_positive.iterrows():
        latest = f"{r['latest_close']:,.0f}" if r['latest_close'] else "-"
        strong_positive_rows += f"""
        <tr class="clickable" data-ticker="{r['ticker']}" onclick="showStockDetail('{r['ticker']}')">
            <td class="text-foreground">{r['ticker'].replace('.T', '')}</td>
            <td class="text-foreground">{r['stock_name']}</td>
            <td class="text-right tabular-nums">{latest}</td>
            <td class="text-muted">{r['category']}</td>
            <td class="text-right tabular-nums font-bold positive">{r['correlation']:+.3f}</td>
            <td class="text-right tabular-nums">{r['avg_return']:+.2f}%</td>
            <td class="text-right tabular-nums">{r['win_rate']:.0f}%</td>
        </tr>"""

    # ピックアップ銘柄: 相関+平均騰落率の両方が高い (相関>0.7 かつ 平均騰落率>4%)
    pick_balanced = correlation_df[
        (correlation_df["correlation"] > 0.7) & (correlation_df["avg_return"] > 4.0)
    ].sort_values("avg_return", ascending=False)

    # ピックアップ銘柄: 相関が特に高い (>0.85)
    pick_high_corr = correlation_df[
        correlation_df["correlation"] > 0.85
    ].sort_values("correlation", ascending=False)

    # ピックアップ銘柄のHTML生成（詳細テーブル付き）
    def generate_pick_card(r: pd.Series, highlight_corr: bool = False) -> str:
        latest = f"{r['latest_close']:,.0f}" if r['latest_close'] else "-"
        cat_badge = ""
        if "政策" in r['category']:
            cat_badge = '<span class="pick-badge">政策</span>'
        elif "Core30" in r['category']:
            cat_badge = '<span class="pick-badge">Core30</span>'

        # 各回の損益テーブル
        details_rows = ""
        total_profit = 0
        for d in r['details']:
            color = "#34d399" if d['return_pct'] >= 0 else "#f87171"
            profit_color = "#34d399" if d['profit_100'] >= 0 else "#f87171"
            total_profit += d['profit_100']
            details_rows += f"""
                <tr>
                    <td>{d['num']}回</td>
                    <td class="text-right">{d['start_price']:,.0f}</td>
                    <td class="text-right">{d['end_price']:,.0f}</td>
                    <td class="text-right" style="color:{color}">{d['return_pct']:+.1f}%</td>
                    <td class="text-right" style="color:{profit_color}">{d['profit_100']:+,.0f}円</td>
                </tr>"""

        total_color = "#34d399" if total_profit >= 0 else "#f87171"
        corr_class = "highlight" if highlight_corr else ""

        return f"""
            <div class="pick-card-vertical">
                <div class="pick-header">
                    <div class="pick-title">
                        <span class="pick-code">{r['ticker'].replace('.T', '')}</span>
                        {cat_badge}
                        <span class="pick-name">{r['stock_name']}</span>
                    </div>
                    <div class="pick-summary">
                        <span class="pick-corr {corr_class}">相関 {r['correlation']:+.2f}</span>
                        <span class="pick-ret positive">平均 {r['avg_return']:+.1f}%</span>
                        <span class="pick-price">直近 {latest}円</span>
                    </div>
                </div>
                <table class="pick-detail-table">
                    <thead>
                        <tr>
                            <th>回</th>
                            <th class="text-right">解散日</th>
                            <th class="text-right">投票日</th>
                            <th class="text-right">騰落率</th>
                            <th class="text-right">100株損益</th>
                        </tr>
                    </thead>
                    <tbody>{details_rows}</tbody>
                    <tfoot>
                        <tr>
                            <td colspan="4" class="text-right">累計損益</td>
                            <td class="text-right font-bold" style="color:{total_color}">{total_profit:+,.0f}円</td>
                        </tr>
                    </tfoot>
                </table>
            </div>"""

    pick_balanced_items = ""
    for _, r in pick_balanced.iterrows():
        pick_balanced_items += generate_pick_card(r, highlight_corr=False)

    pick_high_corr_items = ""
    for _, r in pick_high_corr.iterrows():
        pick_high_corr_items += generate_pick_card(r, highlight_corr=True)

    daily_json = json.dumps(daily_data_js, ensure_ascii=False)

    # 銘柄詳細データをJSON用に変換
    stock_details_js = {}
    for _, r in correlation_df.iterrows():
        stock_details_js[r["ticker"]] = {
            "ticker": r["ticker"],
            "stock_name": r["stock_name"],
            "details": r["details"],
        }
    stock_details_json = json.dumps(stock_details_js, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>衆院解散 株式アノマリー分析</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{
            --bg: #0a0a0a;
            --card: #171717;
            --border: #262626;
            --foreground: #fafafa;
            --muted: #a1a1aa;
            --emerald: #34d399;
            --rose: #f87171;
            --blue: #60a5fa;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Hiragino Sans', sans-serif;
            background: var(--bg);
            color: var(--foreground);
            line-height: 1.6;
            padding: 24px;
            max-width: 1400px;
            margin: 0 auto;
        }}
        h1 {{ font-size: 24px; font-weight: 700; margin-bottom: 8px; }}
        .subtitle {{ color: var(--muted); font-size: 14px; margin-bottom: 24px; }}
        .grid {{ display: grid; gap: 16px; }}
        .grid-2 {{ grid-template-columns: repeat(2, 1fr); }}
        .grid-4 {{ grid-template-columns: repeat(4, 1fr); }}
        @media (max-width: 768px) {{ .grid-2, .grid-4 {{ grid-template-columns: 1fr; }} }}
        .card {{
            background: linear-gradient(135deg, rgba(23,23,23,0.5), rgba(23,23,23,0.8));
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px;
        }}
        .card-title {{ color: var(--muted); font-size: 14px; margin-bottom: 8px; }}
        .big-number {{ font-size: 32px; font-weight: 700; font-variant-numeric: tabular-nums; }}
        .positive {{ color: var(--emerald); }}
        .negative {{ color: var(--rose); }}
        .stat-row {{ display: flex; justify-content: space-between; margin-top: 12px; font-size: 13px; }}
        .stat-label {{ color: var(--muted); }}
        table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
        th {{ text-align: left; padding: 12px 8px; border-bottom: 1px solid var(--border); color: var(--muted); font-weight: 500; }}
        td {{ padding: 10px 8px; border-bottom: 1px solid var(--border); }}
        tr:hover {{ background: rgba(255,255,255,0.02); }}
        .text-right {{ text-align: right; }}
        .text-center {{ text-align: center; }}
        .text-muted {{ color: var(--muted); }}
        .text-foreground {{ color: var(--foreground); }}
        .tabular-nums {{ font-variant-numeric: tabular-nums; }}
        .font-bold {{ font-weight: 700; }}
        .chart-container {{ position: relative; height: 400px; margin: 24px 0; }}
        .section-title {{
            font-size: 16px;
            font-weight: 600;
            margin: 32px 0 16px 0;
            padding-bottom: 8px;
            border-bottom: 1px solid var(--border);
        }}
        .footer {{
            margin-top: 32px;
            padding-top: 16px;
            border-top: 1px solid var(--border);
            color: var(--muted);
            font-size: 12px;
            text-align: center;
        }}
        a {{ color: var(--blue); }}
        .highlight-box {{
            background: rgba(52, 211, 153, 0.1);
            border: 1px solid rgba(52, 211, 153, 0.3);
            border-radius: 8px;
            padding: 16px;
            margin: 16px 0;
        }}
        .highlight-box h3 {{ color: var(--emerald); font-size: 14px; margin-bottom: 8px; }}
        .clickable {{ cursor: pointer; }}
        .clickable:hover {{ background: rgba(96, 165, 250, 0.1); }}
        .pick-section {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 16px;
            margin: 24px 0;
        }}
        @media (max-width: 1024px) {{ .pick-section {{ grid-template-columns: 1fr; }} }}
        .pick-box {{
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 16px;
        }}
        .pick-box-title {{
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
            padding-bottom: 12px;
            border-bottom: 1px solid var(--border);
        }}
        .pick-box-title .icon {{ font-size: 16px; }}
        .pick-list {{
            display: flex;
            flex-direction: column;
            gap: 16px;
        }}
        .pick-card-vertical {{
            background: rgba(255,255,255,0.02);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 12px;
        }}
        .pick-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
            flex-wrap: wrap;
            gap: 8px;
        }}
        .pick-title {{
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .pick-code {{
            font-size: 14px;
            font-weight: 700;
            color: var(--foreground);
        }}
        .pick-name {{
            font-size: 13px;
            color: var(--muted);
        }}
        .pick-summary {{
            display: flex;
            gap: 12px;
            font-size: 12px;
        }}
        .pick-corr {{ color: var(--muted); }}
        .pick-corr.highlight {{ color: var(--emerald); font-weight: 600; }}
        .pick-ret {{ color: var(--muted); }}
        .pick-ret.positive {{ color: var(--emerald); font-weight: 600; }}
        .pick-price {{
            color: var(--muted);
            font-variant-numeric: tabular-nums;
        }}
        .pick-badge {{
            font-size: 10px;
            background: var(--border);
            color: var(--muted);
            padding: 2px 6px;
            border-radius: 4px;
        }}
        .pick-detail-table {{
            width: 100%;
            font-size: 12px;
            border-collapse: collapse;
        }}
        .pick-detail-table th {{
            text-align: left;
            padding: 6px 8px;
            border-bottom: 1px solid var(--border);
            color: var(--muted);
            font-weight: 500;
            font-size: 11px;
        }}
        .pick-detail-table td {{
            padding: 6px 8px;
            border-bottom: 1px solid rgba(255,255,255,0.05);
            font-variant-numeric: tabular-nums;
        }}
        .pick-detail-table tfoot td {{
            border-bottom: none;
            padding-top: 8px;
            font-size: 13px;
        }}
        .modal-overlay {{
            display: none;
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.8);
            z-index: 1000;
            justify-content: center;
            align-items: center;
        }}
        .modal-overlay.active {{ display: flex; }}
        .modal {{
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 24px;
            max-width: 600px;
            width: 90%;
            max-height: 80vh;
            overflow-y: auto;
        }}
        .modal-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
            padding-bottom: 12px;
            border-bottom: 1px solid var(--border);
        }}
        .modal-title {{ font-size: 18px; font-weight: 600; }}
        .modal-close {{
            background: none;
            border: none;
            color: var(--muted);
            font-size: 24px;
            cursor: pointer;
        }}
        .modal-close:hover {{ color: var(--foreground); }}
    </style>
</head>
<body>
    <h1>衆議院解散 → 投票日　株式アノマリー分析</h1>
    <p class="subtitle">1986年以降の解散から投票日までの日経225と政策銘柄・2026テーマとの相関を分析</p>

    <!-- サマリーカード -->
    <div class="grid grid-4" style="margin-bottom: 24px;">
        <div class="card">
            <div class="card-title">N225 勝率</div>
            <div class="big-number positive">{n225_wins}/{n225_total}</div>
            <div class="stat-row">
                <span class="stat-label">勝率</span>
                <span class="positive">{n225_wins/n225_total*100:.0f}%</span>
            </div>
        </div>
        <div class="card">
            <div class="card-title">N225 平均騰落率</div>
            <div class="big-number {'positive' if n225_avg > 0 else 'negative'}">{n225_avg:+.2f}%</div>
            <div class="stat-row">
                <span class="stat-label">最大 / 最小</span>
                <span><span class="positive">{n225_max:+.1f}%</span> / <span class="negative">{n225_min:+.1f}%</span></span>
            </div>
        </div>
        <div class="card">
            <div class="card-title">分析対象銘柄</div>
            <div class="big-number">{len(correlation_df)}</div>
            <div class="stat-row">
                <span class="stat-label">Core30 + 政策銘柄 + 2026テーマ</span>
            </div>
        </div>
        <div class="card">
            <div class="card-title">強い正相関 (>0.6)</div>
            <div class="big-number positive">{len(strong_positive)}</div>
            <div class="stat-row">
                <span class="stat-label">N225と連動する銘柄</span>
            </div>
        </div>
    </div>

    <!-- ピックアップ銘柄 -->
    <div class="pick-section">
        <div class="pick-box">
            <div class="pick-box-title">
                <span class="icon">⚡</span>
                相関 + 騰落率 両方高い
            </div>
            <div class="pick-list">{pick_balanced_items if pick_balanced_items else '<p style="color: var(--muted); font-size: 12px;">該当銘柄なし</p>'}</div>
        </div>
        <div class="pick-box">
            <div class="pick-box-title">
                <span class="icon">🎯</span>
                相関が特に高い (>0.85)
            </div>
            <div class="pick-list">{pick_high_corr_items if pick_high_corr_items else '<p style="color: var(--muted); font-size: 12px;">該当銘柄なし</p>'}</div>
        </div>
    </div>

    <!-- 強い相関を示す銘柄 -->
    {f'''
    <div class="highlight-box">
        <h3>N225と強い正の相関 (相関係数 > 0.6) を示す銘柄</h3>
        <p style="color: var(--muted); font-size: 13px; margin-bottom: 12px;">
            解散時にN225が上がると、これらの銘柄も上がりやすい傾向
        </p>
        <table>
            <thead>
                <tr>
                    <th>コード</th>
                    <th>銘柄名</th>
                    <th class="text-right">直近終値</th>
                    <th>カテゴリ</th>
                    <th class="text-right">相関係数</th>
                    <th class="text-right">平均騰落</th>
                    <th class="text-right">勝率</th>
                </tr>
            </thead>
            <tbody>{strong_positive_rows}</tbody>
        </table>
    </div>
    ''' if len(strong_positive) > 0 else '<p style="color: var(--muted);">強い正の相関を示す銘柄はありません</p>'}

    <!-- チャート -->
    <div class="section-title">解散日からの累積リターン推移（N225）</div>
    <div class="card">
        <div class="chart-container">
            <canvas id="returnChart"></canvas>
        </div>
    </div>

    <!-- 相関分析テーブル -->
    <div class="section-title">N225との相関分析（上位20銘柄）</div>
    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>コード</th>
                    <th>銘柄名</th>
                    <th class="text-right">直近終値</th>
                    <th>カテゴリ</th>
                    <th class="text-right">相関係数</th>
                    <th class="text-right">平均騰落</th>
                    <th class="text-right">勝率</th>
                    <th class="text-center">対象回数</th>
                </tr>
            </thead>
            <tbody>{corr_rows}</tbody>
        </table>
    </div>

    <!-- N225詳細テーブル -->
    <div class="section-title">詳細データ（N225）</div>
    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>回次</th>
                    <th>解散日</th>
                    <th>投票日</th>
                    <th class="text-center">日数</th>
                    <th class="text-right">始値</th>
                    <th class="text-right">終値</th>
                    <th class="text-right">騰落率</th>
                    <th>備考</th>
                </tr>
            </thead>
            <tbody>{n225_rows}</tbody>
        </table>
    </div>

    <div class="footer">
        データソース: 選挙日程=<a href="https://www.shugiin.go.jp/internet/itdb_annai.nsf/html/statics/shiryo/senkyolist.htm" target="_blank">衆議院公式</a> / 株価=Parquet<br>
        生成日時: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    </div>

    <!-- 銘柄詳細モーダル -->
    <div id="stockModal" class="modal-overlay" onclick="closeModal(event)">
        <div class="modal" onclick="event.stopPropagation()">
            <div class="modal-header">
                <span class="modal-title" id="modalTitle">銘柄詳細</span>
                <button class="modal-close" onclick="closeModal()">&times;</button>
            </div>
            <div id="modalContent"></div>
        </div>
    </div>

    <script>
        const stockDetails = {stock_details_json};
        const dailyData = {daily_json};
        const ctx = document.getElementById('returnChart').getContext('2d');

        const datasets = dailyData.map((d, i) => ({{
            label: d.num + '回 ' + d.note,
            data: d.data,
            borderColor: d.win ? '#34d399' : '#f87171',
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.1,
            fill: false,
        }}));

        const maxLen = Math.max(...dailyData.map(d => d.data.length));
        const avgData = [];
        for (let i = 0; i < maxLen; i++) {{
            const vals = dailyData.map(d => d.data[i]).filter(v => v !== undefined);
            avgData.push(vals.length > 0 ? vals.reduce((a,b) => a+b, 0) / vals.length : null);
        }}
        datasets.push({{
            label: '平均',
            data: avgData,
            borderColor: '#60a5fa',
            borderWidth: 3,
            borderDash: [5, 5],
            pointRadius: 0,
            tension: 0.1,
            fill: false,
        }});

        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: Array.from({{length: maxLen}}, (_, i) => i),
                datasets: datasets,
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                interaction: {{ mode: 'index', intersect: false }},
                plugins: {{
                    legend: {{
                        display: true,
                        position: 'bottom',
                        labels: {{ color: '#a1a1aa', boxWidth: 12, padding: 8, font: {{ size: 11 }} }},
                    }},
                }},
                scales: {{
                    x: {{
                        title: {{ display: true, text: '解散からの営業日数', color: '#a1a1aa' }},
                        grid: {{ color: '#262626' }},
                        ticks: {{ color: '#a1a1aa' }},
                    }},
                    y: {{
                        title: {{ display: true, text: '累積リターン (%)', color: '#a1a1aa' }},
                        grid: {{ color: '#262626' }},
                        ticks: {{ color: '#a1a1aa' }},
                    }},
                }},
            }},
        }});

        function showStockDetail(ticker) {{
            const stock = stockDetails[ticker];
            if (!stock) return;

            document.getElementById('modalTitle').textContent = stock.stock_name + ' (' + ticker.replace('.T', '') + ')';

            let html = '<table style="width:100%">';
            html += '<thead><tr>';
            html += '<th>回</th>';
            html += '<th class="text-right">解散日終値</th>';
            html += '<th class="text-right">投票日終値</th>';
            html += '<th class="text-right">上昇率</th>';
            html += '<th class="text-right">100株利益</th>';
            html += '</tr></thead><tbody>';

            stock.details.forEach(d => {{
                const color = d.return_pct >= 0 ? '#34d399' : '#f87171';
                const profitColor = d.profit_100 >= 0 ? '#34d399' : '#f87171';
                html += '<tr>';
                html += '<td class="text-foreground">' + d.num + '回</td>';
                html += '<td class="text-right tabular-nums">' + d.start_price.toLocaleString() + '</td>';
                html += '<td class="text-right tabular-nums">' + d.end_price.toLocaleString() + '</td>';
                html += '<td class="text-right tabular-nums font-bold" style="color:' + color + '">' + (d.return_pct >= 0 ? '+' : '') + d.return_pct.toFixed(2) + '%</td>';
                html += '<td class="text-right tabular-nums font-bold" style="color:' + profitColor + '">' + (d.profit_100 >= 0 ? '+' : '') + d.profit_100.toLocaleString() + '円</td>';
                html += '</tr>';
            }});

            html += '</tbody></table>';

            // 合計を追加
            const totalProfit = stock.details.reduce((sum, d) => sum + d.profit_100, 0);
            const avgReturn = stock.details.reduce((sum, d) => sum + d.return_pct, 0) / stock.details.length;
            const totalColor = totalProfit >= 0 ? '#34d399' : '#f87171';

            html += '<div style="margin-top: 16px; padding-top: 12px; border-top: 1px solid var(--border);">';
            html += '<div style="display: flex; justify-content: space-between; font-size: 14px;">';
            html += '<span class="text-muted">平均上昇率</span>';
            html += '<span class="font-bold" style="color:' + totalColor + '">' + (avgReturn >= 0 ? '+' : '') + avgReturn.toFixed(2) + '%</span>';
            html += '</div>';
            html += '<div style="display: flex; justify-content: space-between; font-size: 14px; margin-top: 8px;">';
            html += '<span class="text-muted">累計利益 (100株)</span>';
            html += '<span class="font-bold" style="color:' + totalColor + '">' + (totalProfit >= 0 ? '+' : '') + totalProfit.toLocaleString() + '円</span>';
            html += '</div>';
            html += '</div>';

            document.getElementById('modalContent').innerHTML = html;
            document.getElementById('stockModal').classList.add('active');
        }}

        function closeModal(event) {{
            if (!event || event.target.id === 'stockModal') {{
                document.getElementById('stockModal').classList.remove('active');
            }}
        }}

        document.addEventListener('keydown', (e) => {{
            if (e.key === 'Escape') closeModal();
        }});
    </script>
</body>
</html>"""
    return html


def generate_json_data(
    n225_results: pd.DataFrame,
    n225_daily: pd.DataFrame,
    correlation_df: pd.DataFrame,
) -> dict:
    """Next.js用のJSONデータを生成"""

    # N225サマリー
    n225_wins = int(n225_results["win"].sum())
    n225_total = len(n225_results)
    n225_avg = float(n225_results["return_pct"].mean())
    n225_max = float(n225_results["return_pct"].max())
    n225_min = float(n225_results["return_pct"].min())

    # N225詳細
    n225_results_list = []
    for _, r in n225_results.iterrows():
        n225_results_list.append({
            "num": int(r["num"]),
            "dissolution": r["dissolution"],
            "election": r["election"],
            "days": int(r["days"]),
            "startPrice": float(r["start_price"]),
            "endPrice": float(r["end_price"]),
            "returnPct": float(r["return_pct"]),
            "note": r["note"],
            "win": bool(r["win"]),
        })

    # N225日次リターン
    n225_daily_list = []
    for num in n225_daily.columns:
        series = n225_daily[num].dropna()
        result = n225_results[n225_results["num"] == num]
        final_ret = float(result["return_pct"].iloc[0]) if not result.empty else 0.0
        note = str(result["note"].iloc[0]) if not result.empty else ""
        n225_daily_list.append({
            "num": int(num),
            "note": note,
            "win": bool(final_ret > 0),
            "data": [round(float(v), 2) for v in series.tolist()],
        })

    # 相関データ
    correlations_list = []
    for _, r in correlation_df.iterrows():
        correlations_list.append({
            "ticker": r["ticker"],
            "stockName": r["stock_name"],
            "category": r["category"],
            "correlation": float(r["correlation"]),
            "avgReturn": float(r["avg_return"]),
            "winRate": float(r["win_rate"]),
            "latestClose": float(r["latest_close"]) if r["latest_close"] else None,
            "details": [
                {
                    "num": d["num"],
                    "startPrice": d["start_price"],
                    "endPrice": d["end_price"],
                    "returnPct": d["return_pct"],
                    "profit100": d["profit_100"],
                }
                for d in r["details"]
            ],
        })

    # ピックアップ銘柄
    pick_balanced = correlation_df[
        (correlation_df["correlation"] > 0.7) & (correlation_df["avg_return"] > 4.0)
    ]["ticker"].tolist()

    pick_high_corr = correlation_df[
        correlation_df["correlation"] > 0.85
    ]["ticker"].tolist()

    return {
        "n225Summary": {
            "wins": n225_wins,
            "total": n225_total,
            "avgReturn": round(n225_avg, 2),
            "maxReturn": round(n225_max, 2),
            "minReturn": round(n225_min, 2),
        },
        "n225Results": n225_results_list,
        "n225Daily": n225_daily_list,
        "correlations": correlations_list,
        "pickBalanced": pick_balanced,
        "pickHighCorr": pick_high_corr,
        "generatedAt": datetime.now().isoformat(),
    }


def main() -> int:
    print("=" * 60)
    print("衆院解散 株式アノマリー分析")
    print("=" * 60)

    # 1. データ読み込み
    print("\n[INFO] Loading data...")
    index_df, prices_df, stocks_df, policy_tickers, theme_tickers, core30_tickers = load_data()

    # 2. 相関分析
    print("\n[INFO] Analyzing correlations...")
    n225_results, n225_returns, correlation_df = analyze_stocks(
        index_df, prices_df, stocks_df, policy_tickers, theme_tickers, core30_tickers, ELECTIONS
    )
    print(f"  N225: {len(n225_results)} elections analyzed")
    print(f"  銘柄分析: {len(correlation_df)} stocks")

    # 3. 日次リターン（N225）
    print("\n[INFO] Calculating daily returns...")
    n225_df = index_df[index_df["ticker"] == "^N225"].copy()
    n225_df = n225_df.set_index("date").sort_index().dropna(subset=["Close"])
    n225_daily = get_daily_returns_aligned(n225_df, ELECTIONS)
    print(f"  N225 daily: {len(n225_daily.columns)} series")

    # 4. HTML生成
    print("\n[INFO] Generating HTML report...")
    html = create_html_report(n225_results, n225_returns, n225_daily, correlation_df)

    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"\n[OK] Saved: {OUTPUT_HTML}")

    # 5. JSON出力（Next.js用）
    print("\n[INFO] Generating JSON for Next.js...")
    json_data = generate_json_data(n225_results, n225_daily, correlation_df)
    json_output_dir = Path(__file__).parent.parent.parent / "stock-frontend" / "public" / "data"
    json_output_dir.mkdir(parents=True, exist_ok=True)
    json_output_path = json_output_dir / "election-data.json"
    json_output_path.write_text(json.dumps(json_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] Saved: {json_output_path}")

    # 6. サマリー表示
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    n225_wins = n225_results["win"].sum()
    print(f"N225 勝率: {n225_wins}/{len(n225_results)} ({n225_wins/len(n225_results)*100:.0f}%)")
    print(f"N225 平均騰落率: {n225_results['return_pct'].mean():+.2f}%")

    strong_positive = correlation_df[correlation_df["correlation"] > 0.6]
    print(f"\n強い正相関 (>0.6): {len(strong_positive)} 銘柄")
    for _, r in strong_positive.iterrows():
        print(f"  {r['ticker']} {r['stock_name']}: {r['correlation']:+.3f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
