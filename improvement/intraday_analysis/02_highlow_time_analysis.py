"""
検証2.2: 銘柄別・時間帯別 高値安値分析

- 日中高値/安値が出現する時間帯の頻度分布
- 銘柄別の傾向差
- 前場/後場での傾向差
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils import (
    load_prices_5m, load_all_stocks, load_grok_trending,
    get_daily_ohlc, classify_time_slot, classify_session,
    generate_html_report, OUTPUT_DIR
)


def analyze_highlow_timing(daily_df: pd.DataFrame) -> pd.DataFrame:
    """高値・安値の出現時間帯を分析"""
    df = daily_df.copy()

    # NaNを除外
    df = df.dropna(subset=["high_time", "low_time"])

    # 高値・安値の時間帯を分類
    df["high_slot"] = df["high_time"].apply(classify_time_slot)
    df["low_slot"] = df["low_time"].apply(classify_time_slot)
    df["high_session"] = df["high_time"].apply(classify_session)
    df["low_session"] = df["low_time"].apply(classify_session)

    return df


def calc_slot_distribution(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """時間帯別の高値・安値出現頻度を計算"""
    # 高値の時間帯分布
    high_dist = df["high_slot"].value_counts().reset_index()
    high_dist.columns = ["time_slot", "high_count"]
    high_dist["high_pct"] = high_dist["high_count"] / len(df) * 100

    # 安値の時間帯分布
    low_dist = df["low_slot"].value_counts().reset_index()
    low_dist.columns = ["time_slot", "low_count"]
    low_dist["low_pct"] = low_dist["low_count"] / len(df) * 100

    # マージしてソート
    dist = high_dist.merge(low_dist, on="time_slot", how="outer").fillna(0)
    dist["sort_key"] = dist["time_slot"].str[:5]
    dist = dist.sort_values("sort_key").drop(columns=["sort_key"])

    return dist


def calc_session_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """前場/後場別の高値・安値出現頻度"""
    results = []

    for session in ["前場", "後場"]:
        high_count = (df["high_session"] == session).sum()
        low_count = (df["low_session"] == session).sum()
        total = len(df)

        results.append({
            "session": session,
            "high_count": high_count,
            "high_pct": high_count / total * 100,
            "low_count": low_count,
            "low_pct": low_count / total * 100,
        })

    return pd.DataFrame(results)


def calc_ticker_patterns(df: pd.DataFrame, all_stocks: pd.DataFrame) -> pd.DataFrame:
    """銘柄別の高値・安値パターンを計算"""
    results = []

    for ticker in df["ticker"].unique():
        ticker_df = df[df["ticker"] == ticker].copy()
        n_days = len(ticker_df)

        if n_days < 10:
            continue

        # 前場高値・後場安値のパターン（ロング有利）
        long_pattern = ((ticker_df["high_session"] == "後場") &
                        (ticker_df["low_session"] == "前場")).sum()

        # 前場安値・後場高値のパターン（ショート有利）
        short_pattern = ((ticker_df["high_session"] == "前場") &
                         (ticker_df["low_session"] == "後場")).sum()

        # 寄付き（09:00-09:30）に安値
        opening_low = (ticker_df["low_slot"] == "09:00-09:30").sum()

        # 大引け（15:00-15:30）に高値
        closing_high = (ticker_df["high_slot"] == "15:00-15:30").sum()

        # 高値時間帯の分布（上位3）
        high_dist = ticker_df["high_slot"].value_counts(normalize=True) * 100
        high_top3 = ", ".join([f"{slot}({pct:.0f}%)" for slot, pct in high_dist.head(3).items()])

        # 安値時間帯の分布（上位3）
        low_dist = ticker_df["low_slot"].value_counts(normalize=True) * 100
        low_top3 = ", ".join([f"{slot}({pct:.0f}%)" for slot, pct in low_dist.head(3).items()])

        # 前場/後場での高値・安値率
        high_am = (ticker_df["high_session"] == "前場").sum() / n_days * 100
        high_pm = (ticker_df["high_session"] == "後場").sum() / n_days * 100
        low_am = (ticker_df["low_session"] == "前場").sum() / n_days * 100
        low_pm = (ticker_df["low_session"] == "後場").sum() / n_days * 100

        results.append({
            "ticker": ticker,
            "n_days": n_days,
            "long_pattern_pct": long_pattern / n_days * 100,
            "short_pattern_pct": short_pattern / n_days * 100,
            "opening_low_pct": opening_low / n_days * 100,
            "closing_high_pct": closing_high / n_days * 100,
            "high_am_pct": high_am,
            "high_pm_pct": high_pm,
            "low_am_pct": low_am,
            "low_pm_pct": low_pm,
            "high_top3": high_top3,
            "low_top3": low_top3,
        })

    result_df = pd.DataFrame(results)

    # 銘柄情報をマージ
    result_df = result_df.merge(
        all_stocks[["ticker", "stock_name", "sectors"]].drop_duplicates(),
        on="ticker", how="left"
    )

    return result_df


def create_ticker_heatmap_high(df: pd.DataFrame, all_stocks: pd.DataFrame) -> str:
    """銘柄別・時間帯別 高値出現ヒートマップ"""
    # 時間帯の順序
    time_slots = [
        "09:00-09:30", "09:30-10:00", "10:00-10:30", "10:30-11:00", "11:00-11:30",
        "12:30-13:00", "13:00-13:30", "13:30-14:00", "14:00-14:30", "14:30-15:00", "15:00-15:30"
    ]

    # 銘柄ごとの時間帯別分布を計算
    tickers = df["ticker"].unique()
    matrix = []
    ticker_names = []

    for ticker in tickers:
        ticker_df = df[df["ticker"] == ticker]
        dist = ticker_df["high_slot"].value_counts(normalize=True) * 100

        row = [dist.get(slot, 0) for slot in time_slots]
        matrix.append(row)

        # 銘柄名を取得
        name_row = all_stocks[all_stocks["ticker"] == ticker]
        name = name_row["stock_name"].values[0] if len(name_row) > 0 else ticker
        ticker_names.append(f"{ticker} {name}")

    fig = go.Figure(data=go.Heatmap(
        z=matrix,
        x=time_slots,
        y=ticker_names,
        colorscale="Reds",
        text=[[f"{v:.0f}%" if v > 5 else "" for v in row] for row in matrix],
        texttemplate="%{text}",
        textfont={"size": 10},
        hovertemplate="銘柄: %{y}<br>時間帯: %{x}<br>出現率: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="出現率(%)")
    ))

    fig.update_layout(
        title="銘柄別 高値出現時間帯ヒートマップ",
        xaxis_title="時間帯",
        yaxis_title="銘柄",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=max(400, len(tickers) * 25),
        yaxis=dict(tickfont=dict(size=10)),
        xaxis=dict(tickangle=45)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_ticker_heatmap_low(df: pd.DataFrame, all_stocks: pd.DataFrame) -> str:
    """銘柄別・時間帯別 安値出現ヒートマップ"""
    time_slots = [
        "09:00-09:30", "09:30-10:00", "10:00-10:30", "10:30-11:00", "11:00-11:30",
        "12:30-13:00", "13:00-13:30", "13:30-14:00", "14:00-14:30", "14:30-15:00", "15:00-15:30"
    ]

    tickers = df["ticker"].unique()
    matrix = []
    ticker_names = []

    for ticker in tickers:
        ticker_df = df[df["ticker"] == ticker]
        dist = ticker_df["low_slot"].value_counts(normalize=True) * 100

        row = [dist.get(slot, 0) for slot in time_slots]
        matrix.append(row)

        name_row = all_stocks[all_stocks["ticker"] == ticker]
        name = name_row["stock_name"].values[0] if len(name_row) > 0 else ticker
        ticker_names.append(f"{ticker} {name}")

    fig = go.Figure(data=go.Heatmap(
        z=matrix,
        x=time_slots,
        y=ticker_names,
        colorscale="Blues",
        text=[[f"{v:.0f}%" if v > 5 else "" for v in row] for row in matrix],
        texttemplate="%{text}",
        textfont={"size": 10},
        hovertemplate="銘柄: %{y}<br>時間帯: %{x}<br>出現率: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="出現率(%)")
    ))

    fig.update_layout(
        title="銘柄別 安値出現時間帯ヒートマップ",
        xaxis_title="時間帯",
        yaxis_title="銘柄",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=max(400, len(tickers) * 25),
        yaxis=dict(tickfont=dict(size=10)),
        xaxis=dict(tickangle=45)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_slot_distribution_chart(dist: pd.DataFrame) -> str:
    """時間帯別分布のチャート"""
    fig = go.Figure()

    fig.add_trace(go.Bar(
        name="高値出現率",
        x=dist["time_slot"],
        y=dist["high_pct"],
        marker_color="#ef4444",
        text=dist["high_pct"].apply(lambda x: f"{x:.1f}%"),
        textposition="outside"
    ))

    fig.add_trace(go.Bar(
        name="安値出現率",
        x=dist["time_slot"],
        y=dist["low_pct"],
        marker_color="#3b82f6",
        text=dist["low_pct"].apply(lambda x: f"{x:.1f}%"),
        textposition="outside"
    ))

    fig.update_layout(
        title="時間帯別 高値・安値出現率（全grok銘柄）",
        barmode="group",
        xaxis_title="時間帯",
        yaxis_title="出現率 (%)",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=450,
        legend=dict(x=0.02, y=0.98)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_session_chart(session_df: pd.DataFrame) -> str:
    """前場/後場分布のチャート"""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("高値出現", "安値出現"),
        specs=[[{"type": "pie"}, {"type": "pie"}]]
    )

    fig.add_trace(go.Pie(
        labels=session_df["session"],
        values=session_df["high_pct"],
        marker_colors=["#f97316", "#8b5cf6"],
        textinfo="label+percent",
        hole=0.4
    ), row=1, col=1)

    fig.add_trace(go.Pie(
        labels=session_df["session"],
        values=session_df["low_pct"],
        marker_colors=["#f97316", "#8b5cf6"],
        textinfo="label+percent",
        hole=0.4
    ), row=1, col=2)

    fig.update_layout(
        title="前場/後場での高値・安値出現率",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        height=350,
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_ticker_heatmap(ticker_df: pd.DataFrame) -> str:
    """銘柄別パターンのヒートマップ"""
    # ロングパターン率でソート
    df = ticker_df.sort_values("long_pattern_pct", ascending=False).head(23)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        name="ロングパターン（前場安値→後場高値）",
        x=df["ticker"],
        y=df["long_pattern_pct"],
        marker_color="#22c55e",
        text=df["long_pattern_pct"].apply(lambda x: f"{x:.0f}%"),
        textposition="outside"
    ))

    fig.add_trace(go.Bar(
        name="ショートパターン（前場高値→後場安値）",
        x=df["ticker"],
        y=df["short_pattern_pct"],
        marker_color="#ef4444",
        text=df["short_pattern_pct"].apply(lambda x: f"{x:.0f}%"),
        textposition="outside"
    ))

    fig.update_layout(
        title="銘柄別 ロング/ショートパターン出現率",
        barmode="group",
        xaxis_title="銘柄",
        yaxis_title="出現率 (%)",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=450,
        legend=dict(x=0.02, y=0.98)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_ticker_table(ticker_df: pd.DataFrame) -> str:
    """銘柄別詳細テーブル"""
    df = ticker_df.sort_values("long_pattern_pct", ascending=False)

    rows = ""
    for _, row in df.iterrows():
        long_class = "positive" if row["long_pattern_pct"] > 30 else ""
        short_class = "negative" if row["short_pattern_pct"] > 30 else ""

        rows += f"""
        <tr>
            <td>{row['ticker']}</td>
            <td>{row.get('stock_name', '-')}</td>
            <td>{row['n_days']}</td>
            <td class="{long_class}">{row['long_pattern_pct']:.1f}%</td>
            <td class="{short_class}">{row['short_pattern_pct']:.1f}%</td>
            <td>{row['high_am_pct']:.0f}% / {row['high_pm_pct']:.0f}%</td>
            <td>{row['low_am_pct']:.0f}% / {row['low_pm_pct']:.0f}%</td>
            <td style="font-size: 0.75rem;">{row['high_top3']}</td>
            <td style="font-size: 0.75rem;">{row['low_top3']}</td>
        </tr>
        """

    return f"""
    <table>
        <thead>
            <tr>
                <th>ティッカー</th>
                <th>銘柄名</th>
                <th>日数</th>
                <th>ロング型</th>
                <th>ショート型</th>
                <th>高値(前/後)</th>
                <th>安値(前/後)</th>
                <th>高値時間帯TOP3</th>
                <th>安値時間帯TOP3</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    <p style="color: #888; font-size: 0.8rem; margin-top: 1rem;">
        ロング型: 前場に安値、後場に高値が出現 → 寄付き買い戦略向き<br>
        ショート型: 前場に高値、後場に安値が出現 → 寄付き売り戦略向き
    </p>
    """


def main():
    print("=== 高値安値時間帯分析開始（grok銘柄のみ） ===")

    # データ準備
    print("データ読み込み中...")
    prices = load_prices_5m()
    all_stocks = load_all_stocks()
    grok = load_grok_trending()

    # grok銘柄のみにフィルタリング
    grok_tickers = grok["ticker"].unique().tolist()
    prices = prices[prices["ticker"].isin(grok_tickers)]
    print(f"grok銘柄数: {len(grok_tickers)}")
    print(f"データ内grok銘柄: {prices['ticker'].nunique()}")

    # 日次OHLCと高値安値時間を取得
    print("日次データを集計中...")
    daily = get_daily_ohlc(prices)
    print(f"日数: {daily['trade_date'].nunique()}")

    # 高値安値の時間帯分析
    print("高値安値タイミングを分析中...")
    daily = analyze_highlow_timing(daily)

    # 時間帯別分布
    slot_dist = calc_slot_distribution(daily)

    # 前場/後場別分布
    session_dist = calc_session_distribution(daily)

    # 銘柄別パターン
    ticker_patterns = calc_ticker_patterns(daily, all_stocks)

    # サマリー統計
    print("\n=== サマリー ===")
    print(f"分析銘柄数: {ticker_patterns['ticker'].nunique()}")
    print(f"分析日数: {daily['trade_date'].nunique()}")
    print(f"前場高値率: {session_dist[session_dist['session']=='前場']['high_pct'].values[0]:.1f}%")
    print(f"前場安値率: {session_dist[session_dist['session']=='前場']['low_pct'].values[0]:.1f}%")
    avg_long = ticker_patterns["long_pattern_pct"].mean()
    avg_short = ticker_patterns["short_pattern_pct"].mean()
    print(f"平均ロングパターン率: {avg_long:.1f}%")
    print(f"平均ショートパターン率: {avg_short:.1f}%")

    # HTMLレポート生成
    print("\nHTMLレポート生成中...")

    summary_html = f"""
    <div class="summary-grid">
        <div class="summary-card">
            <div class="label">分析銘柄数</div>
            <div class="value">{ticker_patterns['ticker'].nunique()}</div>
        </div>
        <div class="summary-card">
            <div class="label">分析日数</div>
            <div class="value">{daily['trade_date'].nunique()}</div>
        </div>
        <div class="summary-card">
            <div class="label">前場高値率</div>
            <div class="value">{session_dist[session_dist['session']=='前場']['high_pct'].values[0]:.1f}%</div>
        </div>
        <div class="summary-card">
            <div class="label">前場安値率</div>
            <div class="value">{session_dist[session_dist['session']=='前場']['low_pct'].values[0]:.1f}%</div>
        </div>
    </div>
    <div class="summary-grid" style="margin-top: 1rem;">
        <div class="summary-card">
            <div class="label">平均ロング型</div>
            <div class="value positive">{avg_long:.1f}%</div>
        </div>
        <div class="summary-card">
            <div class="label">平均ショート型</div>
            <div class="value negative">{avg_short:.1f}%</div>
        </div>
    </div>
    """

    sections = [
        {"title": "サマリー", "content": summary_html},
        {"title": "時間帯別 高値・安値出現率", "content": f'<div class="chart-container">{create_slot_distribution_chart(slot_dist)}</div>'},
        {"title": "前場/後場 分布", "content": f'<div class="chart-container">{create_session_chart(session_dist)}</div>'},
        {"title": "銘柄別 高値出現時間帯", "content": f'<div class="chart-container">{create_ticker_heatmap_high(daily, all_stocks)}</div>'},
        {"title": "銘柄別 安値出現時間帯", "content": f'<div class="chart-container">{create_ticker_heatmap_low(daily, all_stocks)}</div>'},
        {"title": "銘柄別パターン", "content": f'<div class="chart-container">{create_ticker_heatmap(ticker_patterns)}</div>'},
        {"title": "銘柄詳細", "content": create_ticker_table(ticker_patterns)},
    ]

    output_path = OUTPUT_DIR / "highlow_time_report.html"
    generate_html_report("時間帯別 高値安値分析", sections, output_path)

    print(f"\n完了: {output_path}")


if __name__ == "__main__":
    main()
