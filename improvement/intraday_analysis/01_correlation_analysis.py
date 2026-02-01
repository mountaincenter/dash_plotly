"""
検証2.1: 日経平均/TOPIXとの相関分析

- 5分足リターンの相関係数
- β値（市場感応度）の算出
- 時間帯別の相関変化
"""

import pandas as pd
import numpy as np
from scipy import stats
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils import (
    load_prices_5m, load_index_prices_5m, load_all_stocks, load_grok_trending,
    calc_returns, classify_time_slot, generate_html_report, OUTPUT_DIR
)


def prepare_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """データ準備"""
    # 個別銘柄
    prices = load_prices_5m()
    prices = calc_returns(prices)

    # 指数（日経225 ETF: 1321.T or ^N225）
    idx_prices = load_index_prices_5m()

    # 日経225を抽出（^N225を優先、なければ1321.T）
    nikkei = idx_prices[idx_prices["ticker"] == "^N225"].copy()
    if len(nikkei) == 0:
        nikkei = idx_prices[idx_prices["ticker"] == "1321.T"].copy()
    nikkei = calc_returns(nikkei)
    nikkei = nikkei[["date", "return"]].rename(columns={"return": "nikkei_return"})

    # TOPIX（1306.T）
    topix = idx_prices[idx_prices["ticker"] == "1306.T"].copy()
    topix = calc_returns(topix)
    topix = topix[["date", "return"]].rename(columns={"return": "topix_return"})

    # マージ
    prices = prices.merge(nikkei, on="date", how="left")
    prices = prices.merge(topix, on="date", how="left")

    return prices, idx_prices


def calc_correlation_by_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """銘柄ごとの相関係数とβ値を計算"""
    results = []

    for ticker in df["ticker"].unique():
        ticker_df = df[df["ticker"] == ticker].dropna(subset=["return", "nikkei_return", "topix_return"])

        if len(ticker_df) < 30:
            continue

        # 相関係数
        corr_nikkei = ticker_df["return"].corr(ticker_df["nikkei_return"])
        corr_topix = ticker_df["return"].corr(ticker_df["topix_return"])

        # β値（OLS回帰）
        slope_nikkei, intercept_nikkei, r_nikkei, p_nikkei, se_nikkei = stats.linregress(
            ticker_df["nikkei_return"], ticker_df["return"]
        )
        slope_topix, intercept_topix, r_topix, p_topix, se_topix = stats.linregress(
            ticker_df["topix_return"], ticker_df["return"]
        )

        results.append({
            "ticker": ticker,
            "n_samples": len(ticker_df),
            "corr_nikkei": corr_nikkei,
            "corr_topix": corr_topix,
            "beta_nikkei": slope_nikkei,
            "beta_topix": slope_topix,
            "r2_nikkei": r_nikkei ** 2,
            "r2_topix": r_topix ** 2,
            "alpha_nikkei": intercept_nikkei,
            "alpha_topix": intercept_topix,
        })

    return pd.DataFrame(results)


def calc_correlation_by_time_slot(df: pd.DataFrame) -> pd.DataFrame:
    """時間帯別の相関係数を計算"""
    df = df.copy()
    df["time_slot"] = df["date"].apply(classify_time_slot)

    results = []
    for slot in df["time_slot"].unique():
        slot_df = df[df["time_slot"] == slot].dropna(subset=["return", "nikkei_return", "topix_return"])

        if len(slot_df) < 100:
            continue

        corr_nikkei = slot_df["return"].corr(slot_df["nikkei_return"])
        corr_topix = slot_df["return"].corr(slot_df["topix_return"])

        results.append({
            "time_slot": slot,
            "n_samples": len(slot_df),
            "corr_nikkei": corr_nikkei,
            "corr_topix": corr_topix,
        })

    result_df = pd.DataFrame(results)
    # 時間順にソート
    result_df["sort_key"] = result_df["time_slot"].str[:5]
    result_df = result_df.sort_values("sort_key").drop(columns=["sort_key"])
    return result_df


def create_correlation_heatmap(corr_df: pd.DataFrame) -> str:
    """相関係数のヒートマップを作成"""
    # 銘柄×指標のヒートマップ
    top_20 = corr_df.nlargest(20, "corr_nikkei")

    fig = go.Figure()

    fig.add_trace(go.Bar(
        name="日経相関",
        x=top_20["ticker"],
        y=top_20["corr_nikkei"],
        marker_color="#3b82f6"
    ))

    fig.add_trace(go.Bar(
        name="TOPIX相関",
        x=top_20["ticker"],
        y=top_20["corr_topix"],
        marker_color="#f97316"
    ))

    fig.update_layout(
        title="銘柄別 市場相関（上位20）",
        barmode="group",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=400,
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_beta_scatter(corr_df: pd.DataFrame) -> str:
    """β値の散布図を作成"""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=corr_df["beta_nikkei"],
        y=corr_df["beta_topix"],
        mode="markers+text",
        text=corr_df["ticker"],
        textposition="top center",
        textfont=dict(size=8),
        marker=dict(
            size=10,
            color=corr_df["r2_nikkei"],
            colorscale="Blues",
            showscale=True,
            colorbar=dict(title="R²")
        ),
        hovertemplate="<b>%{text}</b><br>β(日経): %{x:.2f}<br>β(TOPIX): %{y:.2f}<extra></extra>"
    ))

    fig.add_shape(type="line", x0=0, y0=0, x1=2, y1=2,
                  line=dict(color="gray", dash="dash"))

    fig.update_layout(
        title="β値分布（日経 vs TOPIX）",
        xaxis_title="β (日経平均)",
        yaxis_title="β (TOPIX)",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=500,
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_time_slot_chart(time_df: pd.DataFrame) -> str:
    """時間帯別相関のチャートを作成"""
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Scatter(
        x=time_df["time_slot"],
        y=time_df["corr_nikkei"],
        mode="lines+markers",
        name="日経相関",
        line=dict(color="#3b82f6", width=2),
        marker=dict(size=8)
    ))

    fig.add_trace(go.Scatter(
        x=time_df["time_slot"],
        y=time_df["corr_topix"],
        mode="lines+markers",
        name="TOPIX相関",
        line=dict(color="#f97316", width=2),
        marker=dict(size=8)
    ))

    fig.add_hline(y=0.5, line_dash="dash", line_color="gray",
                  annotation_text="相関0.5", annotation_position="right")

    fig.update_layout(
        title="時間帯別 市場相関の変化",
        xaxis_title="時間帯",
        yaxis_title="相関係数",
        template="plotly_dark",
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        height=400,
        legend=dict(x=0.02, y=0.98)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_summary_table(corr_df: pd.DataFrame, meta_df: pd.DataFrame) -> str:
    """サマリーテーブルを作成"""
    # メタデータをマージ
    df = corr_df.merge(meta_df[["ticker", "stock_name", "sectors"]], on="ticker", how="left")
    df = df.sort_values("beta_nikkei", ascending=False)

    rows = ""
    for _, row in df.head(30).iterrows():
        corr_class = "positive" if row["corr_nikkei"] > 0.5 else ""
        beta_class = "positive" if row["beta_nikkei"] > 1 else "negative" if row["beta_nikkei"] < 0.5 else ""

        rows += f"""
        <tr>
            <td>{row['ticker']}</td>
            <td>{row.get('stock_name', '-')}</td>
            <td>{row.get('sectors', '-')}</td>
            <td class="{corr_class}">{row['corr_nikkei']:.3f}</td>
            <td>{row['corr_topix']:.3f}</td>
            <td class="{beta_class}">{row['beta_nikkei']:.2f}</td>
            <td>{row['beta_topix']:.2f}</td>
            <td>{row['r2_nikkei']:.3f}</td>
        </tr>
        """

    return f"""
    <table>
        <thead>
            <tr>
                <th>ティッカー</th>
                <th>銘柄名</th>
                <th>業種</th>
                <th>日経相関</th>
                <th>TOPIX相関</th>
                <th>β(日経)</th>
                <th>β(TOPIX)</th>
                <th>R²</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    """


def main():
    print("=== 相関分析開始（grok銘柄のみ） ===")

    # データ準備
    print("データ読み込み中...")
    prices, idx_prices = prepare_data()
    all_stocks = load_all_stocks()
    grok = load_grok_trending()

    # grok銘柄のみにフィルタリング
    grok_tickers = grok["ticker"].unique().tolist()
    prices = prices[prices["ticker"].isin(grok_tickers)]
    print(f"grok銘柄数: {len(grok_tickers)}")
    print(f"データ内grok銘柄: {prices['ticker'].nunique()}")
    print(f"データ期間: {prices['date'].min()} ~ {prices['date'].max()}")

    # 銘柄別相関計算
    print("銘柄別相関を計算中...")
    corr_df = calc_correlation_by_ticker(prices)
    print(f"計算対象銘柄: {len(corr_df)}")

    # 時間帯別相関計算
    print("時間帯別相関を計算中...")
    time_df = calc_correlation_by_time_slot(prices)
    print(f"時間帯数: {len(time_df)}")

    # サマリー統計
    print("\n=== サマリー ===")
    print(f"平均日経相関: {corr_df['corr_nikkei'].mean():.3f}")
    print(f"平均TOPIX相関: {corr_df['corr_topix'].mean():.3f}")
    print(f"平均β(日経): {corr_df['beta_nikkei'].mean():.2f}")
    print(f"β>1の銘柄数: {(corr_df['beta_nikkei'] > 1).sum()}")

    # HTMLレポート生成
    print("\nHTMLレポート生成中...")

    summary_html = f"""
    <div class="summary-grid">
        <div class="summary-card">
            <div class="label">分析銘柄数</div>
            <div class="value">{len(corr_df)}</div>
        </div>
        <div class="summary-card">
            <div class="label">平均日経相関</div>
            <div class="value">{corr_df['corr_nikkei'].mean():.3f}</div>
        </div>
        <div class="summary-card">
            <div class="label">平均β(日経)</div>
            <div class="value">{corr_df['beta_nikkei'].mean():.2f}</div>
        </div>
        <div class="summary-card">
            <div class="label">β>1 銘柄数</div>
            <div class="value">{(corr_df['beta_nikkei'] > 1).sum()}</div>
        </div>
    </div>
    """

    sections = [
        {"title": "サマリー", "content": summary_html},
        {"title": "銘柄別 市場相関", "content": f'<div class="chart-container">{create_correlation_heatmap(corr_df)}</div>'},
        {"title": "β値分布", "content": f'<div class="chart-container">{create_beta_scatter(corr_df)}</div>'},
        {"title": "時間帯別 相関変化", "content": f'<div class="chart-container">{create_time_slot_chart(time_df)}</div>'},
        {"title": "銘柄詳細（β値順）", "content": create_summary_table(corr_df, all_stocks)},
    ]

    output_path = OUTPUT_DIR / "correlation_report.html"
    generate_html_report("日経/TOPIX 相関分析", sections, output_path)

    print(f"\n完了: {output_path}")


if __name__ == "__main__":
    main()
