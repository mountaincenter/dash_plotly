"""
検証3: grok銘柄バックテスト分析

1. 60日間での傾向（時間帯別高値安値パターン）
2. 選定日の動き（grok選定日の日中パターン）
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

from utils import (
    classify_time_slot, classify_session,
    generate_html_report, OUTPUT_DIR
)

# データパス
BACKTEST_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "parquet" / "backtest"


def load_grok_5m() -> pd.DataFrame:
    """grok銘柄の5分足データを読み込み"""
    df = pd.read_parquet(BACKTEST_DIR / "grok_5m_60d_20251230.parquet")
    # datetimeをJSTに変換
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_convert("Asia/Tokyo")
    df["date"] = df["datetime"].dt.date
    df["time"] = df["datetime"].dt.time
    # カラム名を統一
    df = df.rename(columns={
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "volume": "Volume"
    })
    return df


def load_grok_selections() -> pd.DataFrame:
    """grok選定データを読み込み"""
    df = pd.read_parquet(BACKTEST_DIR / "grok_trending_archive.parquet")
    df["selection_date"] = pd.to_datetime(df["selection_date"]).dt.date
    df["backtest_date"] = pd.to_datetime(df["backtest_date"]).dt.date
    return df


def get_daily_ohlc_with_timing(df: pd.DataFrame) -> pd.DataFrame:
    """日次OHLCと高値安値時間を取得"""
    df = df.copy()

    # 日次集計
    daily = df.groupby(["ticker", "date"]).agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum"
    }).reset_index()

    # 高値・安値の時間を取得
    idx_high = df.groupby(["ticker", "date"])["High"].idxmax().dropna()
    idx_low = df.groupby(["ticker", "date"])["Low"].idxmin().dropna()

    high_times = df.loc[idx_high, ["ticker", "date", "datetime"]].rename(columns={"datetime": "high_time"})
    low_times = df.loc[idx_low, ["ticker", "date", "datetime"]].rename(columns={"datetime": "low_time"})

    daily = daily.merge(high_times, on=["ticker", "date"], how="left")
    daily = daily.merge(low_times, on=["ticker", "date"], how="left")

    return daily


def analyze_60d_patterns(daily: pd.DataFrame) -> dict:
    """60日間の傾向を分析"""
    df = daily.dropna(subset=["high_time", "low_time"]).copy()

    # 時間帯・セッション分類
    df["high_slot"] = df["high_time"].apply(classify_time_slot)
    df["low_slot"] = df["low_time"].apply(classify_time_slot)
    df["high_session"] = df["high_time"].apply(classify_session)
    df["low_session"] = df["low_time"].apply(classify_session)

    # 全体の時間帯別分布
    time_slots = [
        "09:00-09:30", "09:30-10:00", "10:00-10:30", "10:30-11:00", "11:00-11:30",
        "12:30-13:00", "13:00-13:30", "13:30-14:00", "14:00-14:30", "14:30-15:00", "15:00-15:30"
    ]

    high_dist = df["high_slot"].value_counts(normalize=True) * 100
    low_dist = df["low_slot"].value_counts(normalize=True) * 100

    slot_summary = pd.DataFrame({
        "time_slot": time_slots,
        "high_pct": [high_dist.get(slot, 0) for slot in time_slots],
        "low_pct": [low_dist.get(slot, 0) for slot in time_slots]
    })

    # セッション別
    session_summary = pd.DataFrame({
        "session": ["前場", "後場"],
        "high_pct": [
            (df["high_session"] == "前場").sum() / len(df) * 100,
            (df["high_session"] == "後場").sum() / len(df) * 100
        ],
        "low_pct": [
            (df["low_session"] == "前場").sum() / len(df) * 100,
            (df["low_session"] == "後場").sum() / len(df) * 100
        ]
    })

    # パターン分析
    long_pattern = ((df["high_session"] == "後場") & (df["low_session"] == "前場")).sum()
    short_pattern = ((df["high_session"] == "前場") & (df["low_session"] == "後場")).sum()

    pattern_summary = {
        "total_days": len(df),
        "long_pattern_pct": long_pattern / len(df) * 100,
        "short_pattern_pct": short_pattern / len(df) * 100,
        "opening_low_pct": (df["low_slot"] == "09:00-09:30").sum() / len(df) * 100,
        "closing_high_pct": (df["high_slot"] == "15:00-15:30").sum() / len(df) * 100,
    }

    return {
        "slot_summary": slot_summary,
        "session_summary": session_summary,
        "pattern_summary": pattern_summary,
        "daily_data": df
    }


def analyze_selection_day(prices_5m: pd.DataFrame, selections: pd.DataFrame) -> pd.DataFrame:
    """選定日の日中パターンを分析"""
    results = []

    for _, sel in selections.iterrows():
        ticker = sel["ticker"]
        sel_date = sel["selection_date"]

        # その日の5分足データを取得
        day_data = prices_5m[
            (prices_5m["ticker"] == ticker) &
            (prices_5m["date"] == sel_date)
        ].copy()

        if len(day_data) < 10:
            continue

        # 日中の動きを分析
        day_data = day_data.sort_values("datetime")
        open_price = day_data["Open"].iloc[0]
        close_price = day_data["Close"].iloc[-1]
        high_price = day_data["High"].max()
        low_price = day_data["Low"].min()

        # 高値・安値の時間
        high_time = day_data.loc[day_data["High"].idxmax(), "datetime"]
        low_time = day_data.loc[day_data["Low"].idxmin(), "datetime"]

        # リターン計算
        day_return = (close_price - open_price) / open_price * 100
        max_gain = (high_price - open_price) / open_price * 100
        max_drawdown = (low_price - open_price) / open_price * 100

        # 前場終値（11:30時点）
        morning_data = day_data[day_data["datetime"].dt.hour < 12]
        morning_close = morning_data["Close"].iloc[-1] if len(morning_data) > 0 else open_price
        morning_return = (morning_close - open_price) / open_price * 100

        results.append({
            "ticker": ticker,
            "stock_name": sel.get("stock_name", ""),
            "selection_date": sel_date,
            "grok_rank": sel.get("grok_rank", 0),
            "open": open_price,
            "close": close_price,
            "high": high_price,
            "low": low_price,
            "day_return": day_return,
            "max_gain": max_gain,
            "max_drawdown": max_drawdown,
            "morning_return": morning_return,
            "high_time": high_time,
            "low_time": low_time,
            "high_slot": classify_time_slot(high_time),
            "low_slot": classify_time_slot(low_time),
            "high_session": classify_session(high_time),
            "low_session": classify_session(low_time),
        })

    return pd.DataFrame(results)


def create_60d_slot_chart(slot_summary: pd.DataFrame) -> str:
    """60日間の時間帯別分布チャート"""
    fig = go.Figure()

    fig.add_trace(go.Bar(
        name="高値出現率",
        x=slot_summary["time_slot"],
        y=slot_summary["high_pct"],
        marker_color="#ef4444",
        text=slot_summary["high_pct"].apply(lambda x: f"{x:.1f}%"),
        textposition="outside"
    ))

    fig.add_trace(go.Bar(
        name="安値出現率",
        x=slot_summary["time_slot"],
        y=slot_summary["low_pct"],
        marker_color="#3b82f6",
        text=slot_summary["low_pct"].apply(lambda x: f"{x:.1f}%"),
        textposition="outside"
    ))

    fig.update_layout(
        title="60日間 時間帯別 高値・安値出現率（全grok銘柄）",
        barmode="group",
        xaxis_title="時間帯",
        yaxis_title="出現率 (%)",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=400,
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_selection_day_chart(sel_df: pd.DataFrame) -> str:
    """選定日の時間帯別分布チャート"""
    time_slots = [
        "09:00-09:30", "09:30-10:00", "10:00-10:30", "10:30-11:00", "11:00-11:30",
        "12:30-13:00", "13:00-13:30", "13:30-14:00", "14:00-14:30", "14:30-15:00", "15:00-15:30"
    ]

    high_dist = sel_df["high_slot"].value_counts(normalize=True) * 100
    low_dist = sel_df["low_slot"].value_counts(normalize=True) * 100

    fig = go.Figure()

    fig.add_trace(go.Bar(
        name="高値出現率",
        x=time_slots,
        y=[high_dist.get(slot, 0) for slot in time_slots],
        marker_color="#ef4444",
        text=[f"{high_dist.get(slot, 0):.1f}%" for slot in time_slots],
        textposition="outside"
    ))

    fig.add_trace(go.Bar(
        name="安値出現率",
        x=time_slots,
        y=[low_dist.get(slot, 0) for slot in time_slots],
        marker_color="#3b82f6",
        text=[f"{low_dist.get(slot, 0):.1f}%" for slot in time_slots],
        textposition="outside"
    ))

    fig.update_layout(
        title="選定日 時間帯別 高値・安値出現率",
        barmode="group",
        xaxis_title="時間帯",
        yaxis_title="出現率 (%)",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=400,
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_return_distribution_chart(sel_df: pd.DataFrame) -> str:
    """選定日のリターン分布"""
    fig = make_subplots(rows=1, cols=3, subplot_titles=("日次リターン", "最大上昇", "最大下落"))

    fig.add_trace(go.Histogram(
        x=sel_df["day_return"],
        nbinsx=30,
        marker_color="#3b82f6",
        name="日次リターン"
    ), row=1, col=1)

    fig.add_trace(go.Histogram(
        x=sel_df["max_gain"],
        nbinsx=30,
        marker_color="#22c55e",
        name="最大上昇"
    ), row=1, col=2)

    fig.add_trace(go.Histogram(
        x=sel_df["max_drawdown"],
        nbinsx=30,
        marker_color="#ef4444",
        name="最大下落"
    ), row=1, col=3)

    fig.update_layout(
        title="選定日のリターン分布",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=350,
        showlegend=False
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_60d_summary_table(pattern_summary: dict) -> str:
    """60日間サマリーテーブル"""
    return f"""
    <div class="summary-grid">
        <div class="summary-card">
            <div class="label">分析日数（銘柄×日）</div>
            <div class="value">{pattern_summary['total_days']:,}</div>
        </div>
        <div class="summary-card">
            <div class="label">ロング型（前安→後高）</div>
            <div class="value positive">{pattern_summary['long_pattern_pct']:.1f}%</div>
        </div>
        <div class="summary-card">
            <div class="label">ショート型（前高→後安）</div>
            <div class="value negative">{pattern_summary['short_pattern_pct']:.1f}%</div>
        </div>
        <div class="summary-card">
            <div class="label">寄付き安値率</div>
            <div class="value">{pattern_summary['opening_low_pct']:.1f}%</div>
        </div>
    </div>
    """


def create_selection_summary_table(sel_df: pd.DataFrame) -> str:
    """選定日サマリーテーブル"""
    # 勝率計算
    win_rate = (sel_df["day_return"] > 0).sum() / len(sel_df) * 100
    morning_win_rate = (sel_df["morning_return"] > 0).sum() / len(sel_df) * 100

    # セッション分布
    high_am = (sel_df["high_session"] == "前場").sum() / len(sel_df) * 100
    low_am = (sel_df["low_session"] == "前場").sum() / len(sel_df) * 100

    return f"""
    <div class="summary-grid">
        <div class="summary-card">
            <div class="label">選定数</div>
            <div class="value">{len(sel_df)}</div>
        </div>
        <div class="summary-card">
            <div class="label">日次勝率</div>
            <div class="value">{win_rate:.1f}%</div>
        </div>
        <div class="summary-card">
            <div class="label">前場勝率</div>
            <div class="value">{morning_win_rate:.1f}%</div>
        </div>
        <div class="summary-card">
            <div class="label">平均日次リターン</div>
            <div class="value">{sel_df['day_return'].mean():.2f}%</div>
        </div>
    </div>
    <div class="summary-grid" style="margin-top: 1rem;">
        <div class="summary-card">
            <div class="label">平均最大上昇</div>
            <div class="value positive">+{sel_df['max_gain'].mean():.2f}%</div>
        </div>
        <div class="summary-card">
            <div class="label">平均最大下落</div>
            <div class="value negative">{sel_df['max_drawdown'].mean():.2f}%</div>
        </div>
        <div class="summary-card">
            <div class="label">前場高値率</div>
            <div class="value">{high_am:.1f}%</div>
        </div>
        <div class="summary-card">
            <div class="label">前場安値率</div>
            <div class="value">{low_am:.1f}%</div>
        </div>
    </div>
    """


def create_selection_detail_table(sel_df: pd.DataFrame) -> str:
    """選定日詳細テーブル"""
    df = sel_df.sort_values("day_return", ascending=False)

    rows = ""
    for _, row in df.head(50).iterrows():
        ret_class = "positive" if row["day_return"] > 0 else "negative"

        rows += f"""
        <tr>
            <td>{row['selection_date']}</td>
            <td>{row['ticker']}</td>
            <td>{row.get('stock_name', '-')}</td>
            <td>{row.get('grok_rank', '-')}</td>
            <td class="{ret_class}">{row['day_return']:.2f}%</td>
            <td class="positive">+{row['max_gain']:.2f}%</td>
            <td class="negative">{row['max_drawdown']:.2f}%</td>
            <td>{row['morning_return']:.2f}%</td>
            <td>{row['high_slot']}</td>
            <td>{row['low_slot']}</td>
        </tr>
        """

    return f"""
    <table>
        <thead>
            <tr>
                <th>選定日</th>
                <th>ティッカー</th>
                <th>銘柄名</th>
                <th>grokランク</th>
                <th>日次リターン</th>
                <th>最大上昇</th>
                <th>最大下落</th>
                <th>前場リターン</th>
                <th>高値時間</th>
                <th>安値時間</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    """


def main():
    print("=== grok銘柄バックテスト分析開始 ===")
    print("（データ量が多いため時間がかかります）")

    # データ読み込み
    print("\nデータ読み込み中...")
    prices_5m = load_grok_5m()
    selections = load_grok_selections()

    print(f"5分足データ: {len(prices_5m):,}行, {prices_5m['ticker'].nunique()}銘柄")
    print(f"選定データ: {len(selections)}件")

    # 1. 60日間の傾向分析
    print("\n60日間の傾向を分析中...")
    daily = get_daily_ohlc_with_timing(prices_5m)
    analysis_60d = analyze_60d_patterns(daily)

    print(f"分析対象: {analysis_60d['pattern_summary']['total_days']:,}件（銘柄×日）")
    print(f"ロング型: {analysis_60d['pattern_summary']['long_pattern_pct']:.1f}%")
    print(f"ショート型: {analysis_60d['pattern_summary']['short_pattern_pct']:.1f}%")
    print(f"寄付き安値: {analysis_60d['pattern_summary']['opening_low_pct']:.1f}%")

    # 2. 選定日の分析
    print("\n選定日の動きを分析中...")
    selection_analysis = analyze_selection_day(prices_5m, selections)

    print(f"分析対象選定: {len(selection_analysis)}件")
    print(f"平均日次リターン: {selection_analysis['day_return'].mean():.2f}%")
    print(f"日次勝率: {(selection_analysis['day_return'] > 0).sum() / len(selection_analysis) * 100:.1f}%")

    # HTMLレポート生成
    print("\nHTMLレポート生成中...")

    sections = [
        # Part 1: 60日間の傾向
        {"title": "【60日間】サマリー", "content": create_60d_summary_table(analysis_60d["pattern_summary"])},
        {"title": "【60日間】時間帯別 高値・安値出現率", "content": f'<div class="chart-container">{create_60d_slot_chart(analysis_60d["slot_summary"])}</div>'},

        # Part 2: 選定日の分析
        {"title": "【選定日】サマリー", "content": create_selection_summary_table(selection_analysis)},
        {"title": "【選定日】時間帯別 高値・安値出現率", "content": f'<div class="chart-container">{create_selection_day_chart(selection_analysis)}</div>'},
        {"title": "【選定日】リターン分布", "content": f'<div class="chart-container">{create_return_distribution_chart(selection_analysis)}</div>'},
        {"title": "【選定日】詳細（リターン順上位50）", "content": create_selection_detail_table(selection_analysis)},
    ]

    output_path = OUTPUT_DIR / "grok_backtest_report.html"
    generate_html_report("grok銘柄バックテスト分析", sections, output_path)

    print(f"\n完了: {output_path}")


if __name__ == "__main__":
    main()
