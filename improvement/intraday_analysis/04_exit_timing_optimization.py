"""
検証4: イグジットタイミング最適化

エントリー: 寄付き（固定）
イグジット: 時間帯別に比較して最適タイミングを探索

セグメント: 曜日 × 信用区分 × 価格帯
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

from utils import generate_html_report, OUTPUT_DIR

# データパス
BACKTEST_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "parquet" / "backtest"

# イグジット時間帯（30分刻み）
EXIT_TIMES = [
    "09:30", "10:00", "10:30", "11:00", "11:30",  # 前場
    "12:30", "13:00", "13:30", "14:00", "14:30", "15:00", "15:30"  # 後場
]

# 価格帯定義
PRICE_RANGES = [
    ("~1,000円", 0, 1000),
    ("1,000~3,000円", 1000, 3000),
    ("3,000~5,000円", 3000, 5000),
    ("5,000~10,000円", 5000, 10000),
    ("10,000円~", 10000, float("inf")),
]


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
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

    # 選定データ
    selections = pd.read_parquet(BACKTEST_DIR / "grok_trending_archive.parquet")
    selections["selection_date"] = pd.to_datetime(selections["selection_date"]).dt.date

    return prices, selections


def get_price_range(price: float) -> str:
    """価格帯を判定"""
    for name, low, high in PRICE_RANGES:
        if low <= price < high:
            return name
    return "10,000円~"


def calc_exit_returns(prices: pd.DataFrame, selections: pd.DataFrame) -> pd.DataFrame:
    """各イグジット時間での損益を計算（ショート戦略）"""
    results = []

    for _, sel in selections.iterrows():
        ticker = sel["ticker"]
        sel_date = sel["selection_date"]
        # day_trade=Trueならいちにち信用可能、Falseなら制度信用のみ
        margin_type = "いちにち信用" if sel.get("day_trade", False) else "制度信用"

        # アーカイブから寄付き・終値を取得
        entry_price = sel.get("buy_price")
        daily_close = sel.get("daily_close")

        if pd.isna(entry_price) or entry_price <= 0:
            continue

        # その日の5分足データ
        day_data = prices[
            (prices["ticker"] == ticker) &
            (prices["date"] == sel_date)
        ].copy()

        weekday = pd.Timestamp(sel_date).day_name()
        weekday_jp = {
            "Monday": "月曜日", "Tuesday": "火曜日", "Wednesday": "水曜日",
            "Thursday": "木曜日", "Friday": "金曜日"
        }.get(weekday, weekday)

        price_range = get_price_range(entry_price)

        # 各イグジット時間での損益計算
        row = {
            "ticker": ticker,
            "stock_name": sel.get("stock_name", ""),
            "date": sel_date,
            "weekday": weekday_jp,
            "margin_type": margin_type,
            "price_range": price_range,
            "entry_price": entry_price,
            "daily_close": daily_close,
        }

        # 5分足からイグジット時間別の価格を取得（15:30以外）
        if len(day_data) > 0:
            day_data = day_data.sort_values("datetime")

            for exit_time in EXIT_TIMES[:-1]:  # 15:30以外
                exit_data = day_data[day_data["time"] == exit_time]
                if len(exit_data) > 0:
                    exit_price = exit_data["Close"].iloc[0]
                    pct_return = (entry_price - exit_price) / entry_price * 100
                    profit = (entry_price - exit_price) * 100
                else:
                    pct_return = np.nan
                    profit = np.nan

                row[f"return_{exit_time}"] = pct_return
                row[f"profit_{exit_time}"] = profit

        # 15:30（大引け）はアーカイブのdaily_closeを使用
        if not pd.isna(daily_close) and daily_close > 0:
            pct_return = (entry_price - daily_close) / entry_price * 100
            profit = (entry_price - daily_close) * 100
            row["return_15:30"] = pct_return
            row["profit_15:30"] = profit
        else:
            row["return_15:30"] = np.nan
            row["profit_15:30"] = np.nan

        results.append(row)

    return pd.DataFrame(results)


def analyze_by_segment(df: pd.DataFrame) -> pd.DataFrame:
    """セグメント別の損益・勝率を集計"""
    segments = []

    # 全体
    for weekday in df["weekday"].unique():
        for margin in df["margin_type"].unique():
            for price_range in [pr[0] for pr in PRICE_RANGES]:
                seg_df = df[
                    (df["weekday"] == weekday) &
                    (df["margin_type"] == margin) &
                    (df["price_range"] == price_range)
                ]

                if len(seg_df) < 3:
                    continue

                row = {
                    "weekday": weekday,
                    "margin_type": margin,
                    "price_range": price_range,
                    "count": len(seg_df),
                }

                for exit_time in EXIT_TIMES:
                    col = f"profit_{exit_time}"
                    if col in seg_df.columns:
                        profits = seg_df[col].dropna()
                        if len(profits) > 0:
                            row[f"profit_{exit_time}"] = profits.sum()
                            row[f"win_rate_{exit_time}"] = (profits > 0).sum() / len(profits) * 100
                        else:
                            row[f"profit_{exit_time}"] = 0
                            row[f"win_rate_{exit_time}"] = 0

                # 最適イグジット時間を特定
                profit_cols = [f"profit_{t}" for t in EXIT_TIMES if f"profit_{t}" in row]
                if profit_cols:
                    best_time = max(profit_cols, key=lambda x: row.get(x, -float("inf")))
                    row["best_exit"] = best_time.replace("profit_", "")
                    row["best_profit"] = row.get(best_time, 0)

                segments.append(row)

    return pd.DataFrame(segments)


def create_heatmap_chart(segment_df: pd.DataFrame, weekday: str) -> str:
    """曜日別ヒートマップ"""
    df = segment_df[segment_df["weekday"] == weekday].copy()

    if len(df) == 0:
        return "<p>データなし</p>"

    # 制度信用と一日信用を分けて表示
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("制度信用", "いちにち信用"),
        horizontal_spacing=0.1
    )

    for col_idx, margin in enumerate(["制度信用", "いちにち信用"], 1):
        margin_df = df[df["margin_type"] == margin]

        if len(margin_df) == 0:
            continue

        # 価格帯 × イグジット時間のマトリクス
        price_ranges = [pr[0] for pr in PRICE_RANGES]
        matrix = []
        text_matrix = []

        for pr in price_ranges:
            pr_df = margin_df[margin_df["price_range"] == pr]
            row_vals = []
            row_text = []

            for exit_time in EXIT_TIMES:
                if len(pr_df) > 0:
                    profit = pr_df[f"profit_{exit_time}"].values[0] if f"profit_{exit_time}" in pr_df.columns else 0
                    win_rate = pr_df[f"win_rate_{exit_time}"].values[0] if f"win_rate_{exit_time}" in pr_df.columns else 0
                    row_vals.append(profit)
                    row_text.append(f"{profit:,.0f}円<br>{win_rate:.0f}%")
                else:
                    row_vals.append(0)
                    row_text.append("")

            matrix.append(row_vals)
            text_matrix.append(row_text)

        fig.add_trace(go.Heatmap(
            z=matrix,
            x=EXIT_TIMES,
            y=price_ranges,
            colorscale=[[0, "#ef4444"], [0.5, "#1a1a1a"], [1, "#22c55e"]],
            zmid=0,
            text=text_matrix,
            texttemplate="%{text}",
            textfont={"size": 9},
            hovertemplate="価格帯: %{y}<br>イグジット: %{x}<br>損益: %{z:,.0f}円<extra></extra>",
            showscale=col_idx == 2,
            colorbar=dict(title="損益(円)") if col_idx == 2 else None
        ), row=1, col=col_idx)

    fig.update_layout(
        title=f"{weekday} - イグジット時間別損益",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=400,
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_summary_table(segment_df: pd.DataFrame) -> str:
    """最適イグジットサマリーテーブル"""
    # 曜日順
    weekday_order = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日"]
    df = segment_df.copy()
    df["weekday_order"] = df["weekday"].map({w: i for i, w in enumerate(weekday_order)})
    df = df.sort_values(["weekday_order", "margin_type", "price_range"])

    rows = ""
    for _, row in df.iterrows():
        profit_class = "positive" if row.get("best_profit", 0) > 0 else "negative"

        # 大引け(15:30)との比較
        close_profit = row.get("profit_15:30", 0)
        improvement = row.get("best_profit", 0) - close_profit
        imp_class = "positive" if improvement > 0 else "negative" if improvement < 0 else ""

        rows += f"""
        <tr>
            <td>{row['weekday']}</td>
            <td>{row['margin_type']}</td>
            <td>{row['price_range']}</td>
            <td>{row['count']}</td>
            <td>{close_profit:,.0f}円</td>
            <td><strong>{row.get('best_exit', '-')}</strong></td>
            <td class="{profit_class}">{row.get('best_profit', 0):,.0f}円</td>
            <td class="{imp_class}">{improvement:+,.0f}円</td>
        </tr>
        """

    return f"""
    <table>
        <thead>
            <tr>
                <th>曜日</th>
                <th>信用区分</th>
                <th>価格帯</th>
                <th>件数</th>
                <th>大引け損益</th>
                <th>最適イグジット</th>
                <th>最適時損益</th>
                <th>改善額</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    """


def create_overall_summary(segment_df: pd.DataFrame, raw_df: pd.DataFrame) -> str:
    """全体サマリー"""
    # イグジット時間別の全体損益
    totals = {}
    for exit_time in EXIT_TIMES:
        col = f"profit_{exit_time}"
        if col in raw_df.columns:
            totals[exit_time] = raw_df[col].sum()

    best_exit = max(totals, key=totals.get) if totals else "15:30"
    best_total = totals.get(best_exit, 0)
    close_total = totals.get("15:30", 0)

    return f"""
    <div class="summary-grid">
        <div class="summary-card">
            <div class="label">分析件数</div>
            <div class="value">{len(raw_df)}</div>
        </div>
        <div class="summary-card">
            <div class="label">大引け(15:30)損益</div>
            <div class="value">{close_total:+,.0f}円</div>
        </div>
        <div class="summary-card">
            <div class="label">最適イグジット</div>
            <div class="value positive">{best_exit}</div>
        </div>
        <div class="summary-card">
            <div class="label">最適時損益</div>
            <div class="value positive">{best_total:+,.0f}円</div>
        </div>
    </div>
    <div class="summary-grid" style="margin-top: 1rem;">
        <div class="summary-card">
            <div class="label">改善額</div>
            <div class="value positive">{best_total - close_total:+,.0f}円</div>
        </div>
        <div class="summary-card">
            <div class="label">改善率</div>
            <div class="value positive">{(best_total - close_total) / abs(close_total) * 100 if close_total != 0 else 0:+.1f}%</div>
        </div>
    </div>
    """


def create_exit_comparison_chart(raw_df: pd.DataFrame) -> str:
    """イグジット時間別の全体損益比較"""
    totals = []
    win_rates = []

    for exit_time in EXIT_TIMES:
        col = f"profit_{exit_time}"
        if col in raw_df.columns:
            profits = raw_df[col].dropna()
            totals.append(profits.sum())
            win_rates.append((profits > 0).sum() / len(profits) * 100 if len(profits) > 0 else 0)
        else:
            totals.append(0)
            win_rates.append(0)

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # 損益バー
    colors = ["#22c55e" if t > 0 else "#ef4444" for t in totals]
    fig.add_trace(go.Bar(
        x=EXIT_TIMES,
        y=totals,
        name="損益",
        marker_color=colors,
        text=[f"{t:,.0f}" for t in totals],
        textposition="outside"
    ), secondary_y=False)

    # 勝率ライン
    fig.add_trace(go.Scatter(
        x=EXIT_TIMES,
        y=win_rates,
        name="勝率",
        mode="lines+markers",
        line=dict(color="#3b82f6", width=2),
        marker=dict(size=8)
    ), secondary_y=True)

    fig.update_layout(
        title="イグジット時間別 損益・勝率（ショート戦略全体）",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=400,
        legend=dict(x=0.02, y=0.98)
    )

    fig.update_yaxes(title_text="損益（円）", secondary_y=False)
    fig.update_yaxes(title_text="勝率（%）", secondary_y=True)

    return fig.to_html(full_html=False, include_plotlyjs=False)


def main():
    print("=== イグジットタイミング最適化分析 ===")
    print("（データ量が多いため時間がかかります）")

    # データ読み込み
    print("\nデータ読み込み中...")
    prices, selections = load_data()
    print(f"5分足: {len(prices):,}行")
    print(f"選定: {len(selections)}件")

    # イグジット時間別損益計算
    print("\nイグジット時間別損益を計算中...")
    exit_returns = calc_exit_returns(prices, selections)
    print(f"分析対象: {len(exit_returns)}件")

    # セグメント別集計
    print("\nセグメント別集計中...")
    segment_analysis = analyze_by_segment(exit_returns)
    print(f"セグメント数: {len(segment_analysis)}")

    # サマリー出力
    print("\n=== 全体サマリー ===")
    for exit_time in EXIT_TIMES:
        col = f"profit_{exit_time}"
        if col in exit_returns.columns:
            total = exit_returns[col].sum()
            win_rate = (exit_returns[col] > 0).sum() / len(exit_returns[col].dropna()) * 100
            print(f"{exit_time}: {total:+,.0f}円 (勝率{win_rate:.1f}%)")

    # HTMLレポート生成
    print("\nHTMLレポート生成中...")

    sections = [
        {"title": "全体サマリー", "content": create_overall_summary(segment_analysis, exit_returns)},
        {"title": "イグジット時間別 損益比較", "content": f'<div class="chart-container">{create_exit_comparison_chart(exit_returns)}</div>'},
        {"title": "セグメント別 最適イグジット", "content": create_summary_table(segment_analysis)},
    ]

    # 曜日別ヒートマップ
    for weekday in ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日"]:
        sections.append({
            "title": f"{weekday} - 詳細",
            "content": f'<div class="chart-container">{create_heatmap_chart(segment_analysis, weekday)}</div>'
        })

    output_path = OUTPUT_DIR / "exit_timing_report.html"
    generate_html_report("イグジットタイミング最適化", sections, output_path)

    print(f"\n完了: {output_path}")


if __name__ == "__main__":
    main()
