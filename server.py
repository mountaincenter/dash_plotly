#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
本番対応:
- 環境変数がセットされていれば S3 から Parquet を取得
- 未設定 or 取得失敗時はローカル ./data/parquet をフォールバック
- Dash の起動は app.run（厳守）
- 休業日を詰める: x軸を category にすることで非取引日を表示しない（等間隔で営業日のみ）
- Tabs: Dropdown をタブの前に配置し、全タブで同じ選択を共有
- 追加: アノマリー（月次）タブ（年×月ヒートマップ：2色）
"""

import os
import sys
from io import BytesIO
from pathlib import Path
import numpy as np
import pandas as pd
from dash import Dash, dcc, html, Input, Output
from dash import dash_table
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ========= 設定 =========
# ローカル既定パス
META_PARQUET_LOCAL    = Path("./data/parquet/core30_meta.parquet")
PRICES_PARQUET_LOCAL  = Path("./data/parquet/core30_prices_1y_1d.parquet")
ANOMALY_PARQUET_LOCAL = Path("./data/parquet/core30_anomaly.parquet")  # year×monthのreturn_pct

# S3 用の環境変数
DATA_BUCKET         = os.getenv("DATA_BUCKET")
CORE30_META_KEY     = os.getenv("CORE30_META_KEY")
CORE30_PRICES_KEY   = os.getenv("CORE30_PRICES_KEY")
CORE30_ANOMALY_KEY  = os.getenv("CORE30_ANOMALY_KEY")
AWS_REGION          = os.getenv("AWS_REGION")  # 任意
AWS_PROFILE         = os.getenv("AWS_PROFILE") # 任意（~/.aws を使う場合）

def _read_parquet_local(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"not found: {path}")
    return pd.read_parquet(path, engine="pyarrow")

def _read_parquet_s3(bucket: str, key: str) -> pd.DataFrame:
    # 認証は環境変数 or プロファイル（~/.aws/credentials）を boto3 が自動選択
    import boto3
    session_kwargs = {}
    if AWS_PROFILE:
        session_kwargs["profile_name"] = AWS_PROFILE
    session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()
    s3 = session.client("s3", region_name=AWS_REGION) if AWS_REGION else session.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    return pd.read_parquet(BytesIO(data), engine="pyarrow")

def _load_meta_and_prices() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    返り値: (meta_df, prices_df, source)  # source: "s3" or "local"
    """
    use_s3 = all([DATA_BUCKET, CORE30_META_KEY, CORE30_PRICES_KEY])
    if use_s3:
        try:
            meta   = _read_parquet_s3(DATA_BUCKET, CORE30_META_KEY)
            prices = _read_parquet_s3(DATA_BUCKET, CORE30_PRICES_KEY)
            return meta, prices, "s3"
        except Exception as e:
            print(f"[WARN] S3 読み込みに失敗しました。ローカルへフォールバックします: {e}", file=sys.stderr)

    meta   = _read_parquet_local(META_PARQUET_LOCAL)
    prices = _read_parquet_local(PRICES_PARQUET_LOCAL)
    return meta, prices, "local"

def _load_anomaly() -> tuple[pd.DataFrame, str]:
    """
    返り値: (anomaly_df, source)  # source: "s3" or "local" or "none"
    """
    if DATA_BUCKET and CORE30_ANOMALY_KEY:
        try:
            df = _read_parquet_s3(DATA_BUCKET, CORE30_ANOMALY_KEY)
            return df, "s3"
        except Exception as e:
            print(f"[WARN] anomaly S3 読み込み失敗: {e}. ローカルへフォールバックします。", file=sys.stderr)

    try:
        df = _read_parquet_local(ANOMALY_PARQUET_LOCAL)
        return df, "local"
    except Exception as e:
        print(f"[WARN] anomaly ローカル読み込み失敗: {e}.", file=sys.stderr)
        return pd.DataFrame(columns=["ticker","year","month","return_pct"]), "none"

# ===== データ読み込み（起動時）=====
meta, prices, src = _load_meta_and_prices()
anomaly, an_src   = _load_anomaly()
print(f"[INFO] data source = {src}", file=sys.stderr)
print(f"[INFO] anomaly source = {an_src}", file=sys.stderr)

# 型の安全化（最小限）
for c in ("code", "stock_name", "ticker"):
    if c in meta.columns:
        meta[c] = meta[c].astype("string")
if "ticker" in anomaly.columns:
    anomaly["ticker"] = anomaly["ticker"].astype("string")

# Dropdown options
options = []
if not meta.empty and {"code", "stock_name", "ticker"}.issubset(meta.columns):
    options = [
        {"label": f"{row.code}:{row.stock_name}", "value": row.ticker}
        for row in meta.itertuples(index=False)
    ]
initial_value = options[0]["value"] if options else None

# ===== Dash App =====
app = Dash(__name__)
app.title = "TOPIX Core 30 — 1y/1d Candlestick + Monthly Matrix"

# Dropdown を Tabs の前に配置（全タブで共有）
app.layout = html.Div(
    style={"maxWidth": "1200px", "margin": "0 auto", "padding": "12px"},
    children=[
        html.H2("TOPIX Core 30 — 1年・日足 / 月次（年×月）", style={"marginBottom": "8px"}),

        dcc.Dropdown(
            id="ticker-dd",
            options=options,
            value=initial_value,
            placeholder="銘柄を選択（{code}:{stock_name}）",
            clearable=False,
            style={"marginBottom": "12px"},
        ),

        dcc.Tabs(
            id="tabs",
            value="tab-1",
            children=[
                dcc.Tab(
                    label="チャート / テーブル",
                    value="tab-1",
                    children=[
                        dcc.Graph(id="price-chart", config={"displayModeBar": True}),
                        html.Div(id="price-table", style={"marginTop": "8px"}),
                        html.Div(
                            [
                                html.Span("※ データ取得は Jupyter Notebook 側（period=1y, interval=1d）。本アプリは可視化のみ。"),
                                html.Br(),
                                html.Span(f"データソース: {src} / アノマリー: {an_src}"),
                            ],
                            style={"fontSize": "12px", "color": "#555"},
                        ),
                    ],
                ),
                dcc.Tab(
                    label="予備タブ",
                    value="tab-2",
                    children=[
                        html.Div(
                            [
                                html.Div("テスト：このタブは今後の機能追加用です。", style={"marginBottom": "6px"}),
                                html.Div(id="ticker-echo", style={"color": "#555"}),
                            ],
                            style={"padding": "12px"}
                        )
                    ],
                ),
                dcc.Tab(
                    label="アノマリー（月次：年×月）",
                    value="tab-3",
                    children=[
                        dcc.Graph(id="anomaly-heatmap"),
                        html.Div(
                            "※ 年×月リターン(%)を 2色（負=赤/正=緑）で表示。",
                            style={"fontSize": "12px", "color": "#777", "marginTop": "4px"},
                        ),
                    ],
                ),
            ],
        ),
    ],
)

# ===== コールバック =====
@app.callback(
    Output("price-chart", "figure"),
    Output("price-table", "children"),
    Output("ticker-echo", "children"),
    Output("anomaly-heatmap", "figure"),
    Input("ticker-dd", "value"),
)
def update_all(ticker: str):
    # 初期応答
    label = "選択中の銘柄：-"
    price_fig = go.Figure()
    price_tbl = html.Div()
    anom_fig = go.Figure()

    if not ticker or "ticker" not in prices.columns:
        return price_fig, price_tbl, label, anom_fig

    # ===== 既存：チャート / テーブル =====
    df = prices.loc[prices["ticker"] == ticker].copy()
    if not df.empty:
        need_ohlc = {"date", "Open", "High", "Low", "Close"}
        if need_ohlc.issubset(df.columns):
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"]).sort_values("date")
            x_cat = df["date"].dt.strftime("%Y-%m-%d")
            has_volume = "Volume" in df.columns

            price_fig = make_subplots(
                rows=2 if has_volume else 1,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                row_heights=[0.72, 0.28] if has_volume else [1.0],
                specs=[[{"type": "xy"}]] if not has_volume else [[{"type": "xy"}], [{"type": "xy"}]],
            )
            price_fig.add_trace(
                go.Candlestick(
                    x=x_cat,
                    open=df["Open"],
                    high=df["High"],
                    low=df["Low"],
                    close=df["Close"],
                    name="Price",
                ),
                row=1, col=1
            )
            if has_volume:
                price_fig.add_trace(
                    go.Bar(x=x_cat, y=df["Volume"], name="Volume", opacity=0.7),
                    row=2, col=1
                )
                price_fig.update_yaxes(title_text="Volume", row=2, col=1)

            price_fig.update_xaxes(type="category", row=1, col=1)
            if has_volume:
                price_fig.update_xaxes(type="category", row=2, col=1)

            try:
                rowm = meta.loc[meta["ticker"] == ticker].iloc[0]
                title = f"{rowm['code']} : {rowm['stock_name']} ({ticker}) — 1y / 1d"
                label = f"選択中の銘柄：{rowm['code']} : {rowm['stock_name']} ({ticker})"
            except Exception:
                title = f"{ticker} — 1y / 1d"
                label = f"選択中の銘柄：{ticker}"

            price_fig.update_layout(
                title=title,
                margin=dict(l=40, r=20, t=40, b=40),
                xaxis_title="Date" if not has_volume else None,
                yaxis_title="Price",
                xaxis_rangeslider_visible=False,
                template="plotly_white",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )

            # 直近1ヶ月テーブル
            last_date = df["date"].max()
            threshold = last_date - pd.DateOffset(months=1)
            df_last = df.loc[df["date"] >= threshold, ["date", "Open", "High", "Low", "Close"]].copy()
            df_last["Date"] = df_last["date"].dt.strftime("%m-%d")
            for c in ["Open", "High", "Low", "Close"]:
                df_last[c] = pd.to_numeric(df_last[c], errors="coerce").round(1)
            df_view = df_last[["Date", "Open", "High", "Low", "Close"]]
            price_tbl = dash_table.DataTable(
                columns=[{"name": col, "id": col} for col in df_view.columns],
                data=df_view.to_dict("records"),
                style_cell={"textAlign": "center", "padding": "6px"},
                style_header={"fontWeight": "600"},
                page_size=len(df_view),
            )

    # ===== 新規：アノマリー（月次：年×月ヒートマップ、2色固定＋白文字＋間隔）=====
    if not anomaly.empty and {"ticker","year","month","return_pct"}.issubset(anomaly.columns):
        an = anomaly.loc[anomaly["ticker"] == ticker].copy()
        if not an.empty:
            an["year"] = an["year"].astype(int)
            an["month"] = an["month"].astype(int)

            years = sorted(an["year"].unique().tolist(), reverse=True)
            months = list(range(1, 13))
            mat = (
                an.pivot_table(index="year", columns="month", values="return_pct", aggfunc="first")
                  .reindex(index=years, columns=months)
            )

            z_val = mat.values.astype(float)
            z_sign = np.where(np.isfinite(z_val), (z_val >= 0).astype(float), np.nan)

            def _fmt(v):
                return "" if not np.isfinite(v) else f"{v:+.1f}%"
            text = [[_fmt(v) for v in row] for row in z_val]

            base_h = 160
            per_row = 28
            fig_h = base_h + per_row * max(len(years), 1)

            anom_fig = go.Figure(
                data=go.Heatmap(
                    x=[f"{m:02d}" for m in months],
                    y=[str(y) for y in years],
                    z=z_sign,
                    customdata=z_val,
                    text=text,
                    texttemplate="%{text}",
                    textfont={"color": "white", "size": 12},
                    colorscale=[[0.0, "#C62828"], [1.0, "#2E7D32"]],
                    showscale=False,
                    hovertemplate="Year=%{y}<br>Month=%{x}<br>Return=%{customdata:.1f}%<extra></extra>",
                    zmin=0.0, zmax=1.0,
                    xgap=2, ygap=2,
                )
            )
            anom_fig.update_layout(
                title="月次リターン（年×月・2色）",
                xaxis_title="Month (MM)",
                yaxis_title="Year",
                template="plotly_white",
                margin=dict(l=80, r=40, t=60, b=60),
                height=fig_h,
            )
        else:
            anom_fig = go.Figure()

    return price_fig, price_tbl, label, anom_fig

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8050"))
    debug = os.getenv("DEBUG", "false").lower() == "true"
    # 厳守: app.run を使用
    app.run(host="0.0.0.0", port=port, debug=debug)
