#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
本番対応:
- 環境変数がセットされていれば S3 から Parquet を取得
- 未設定 or 取得失敗時はローカル ./data/parquet をフォールバック
- Dash の起動は app.run（厳守）
- 休業日を詰める: x軸を category にすることで非取引日を表示しない（等間隔で営業日のみ）
"""

import os
import sys
from io import BytesIO
from pathlib import Path

import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ========= 設定 =========
# ローカル既定パス
META_PARQUET_LOCAL   = Path("./data/parquet/core30_meta.parquet")
PRICES_PARQUET_LOCAL = Path("./data/parquet/core30_prices_1y_1d.parquet")

# S3 用の環境変数
DATA_BUCKET       = os.getenv("DATA_BUCKET")
CORE30_META_KEY   = os.getenv("CORE30_META_KEY")
CORE30_PRICES_KEY = os.getenv("CORE30_PRICES_KEY")
AWS_REGION        = os.getenv("AWS_REGION")  # 任意

def _read_parquet_local(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"not found: {path}")
    return pd.read_parquet(path, engine="pyarrow")

def _read_parquet_s3(bucket: str, key: str) -> pd.DataFrame:
    import boto3
    s3 = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")
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

# ===== データ読み込み（起動時）=====
meta, prices, src = _load_meta_and_prices()
print(f"[INFO] data source = {src}", file=sys.stderr)

# 型の安全化（最小限）
for c in ("code", "stock_name", "ticker"):
    if c in meta.columns:
        meta[c] = meta[c].astype("string")

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
app.title = "TOPIX Core 30 — 1y/1d Candlestick + Volume"

app.layout = html.Div(
    style={"maxWidth": "1200px", "margin": "0 auto", "padding": "12px"},
    children=[
        html.H2("TOPIX Core 30 — 1年・日足（Candlestick + Volume）", style={"marginBottom": "8px"}),
        dcc.Dropdown(
            id="ticker-dd",
            options=options,
            value=initial_value,
            placeholder="銘柄を選択（{code}:{stock_name}）",
            clearable=False,
            style={"marginBottom": "12px"},
        ),
        dcc.Graph(id="price-chart", config={"displayModeBar": True}),
        html.Div(
            [
                html.Span("※ データ取得は Jupyter Notebook 側（period=1y, interval=1d）。本アプリは可視化のみ。"),
                html.Br(),
                html.Span(f"データソース: {src}"),
            ],
            style={"fontSize": "12px", "color": "#555"},
        ),
    ],
)

@app.callback(
    Output("price-chart", "figure"),
    Input("ticker-dd", "value"),
)
def update_chart(ticker: str):
    if not ticker:
        return go.Figure()

    if "ticker" not in prices.columns:
        return go.Figure()

    df = prices.loc[prices["ticker"] == ticker].copy()
    if df.empty:
        return go.Figure()

    # 必須列チェック
    need_ohlc = {"date", "Open", "High", "Low", "Close"}
    if not need_ohlc.issubset(df.columns):
        return go.Figure()

    # 日付を厳密に時系列へ
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    # カテゴリ軸用に "YYYY-MM-DD" に変換（同日重複対策で重複があれば時刻付きに拡張しても良い）
    x_cat = df["date"].dt.strftime("%Y-%m-%d")

    # --- サブプロット（上：ローソク、下：出来高） ---
    has_volume = "Volume" in df.columns
    fig = make_subplots(
        rows=2 if has_volume else 1,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.72, 0.28] if has_volume else [1.0],
        specs=[[{"type": "xy"}]] if not has_volume else [[{"type": "xy"}], [{"type": "xy"}]],
    )

    # ローソク足
    fig.add_trace(
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

    # 出来高（任意）
    if has_volume:
        fig.add_trace(
            go.Bar(
                x=x_cat,
                y=df["Volume"],
                name="Volume",
                opacity=0.7,
            ),
            row=2, col=1
        )
        fig.update_yaxes(title_text="Volume", row=2, col=1)

    # ===== 休業日を詰める：x軸を category 化 =====
    # これにより存在しない日付は描画されず、営業日のみ等間隔で表示されます
    fig.update_xaxes(type="category", row=1, col=1)
    if has_volume:
        fig.update_xaxes(type="category", row=2, col=1)

    # レイアウト
    fig.update_layout(
        margin=dict(l=40, r=20, t=40, b=40),
        xaxis_title="Date" if not has_volume else None,
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    # タイトル：{code}:{stock_name}
    try:
        row = meta.loc[meta["ticker"] == ticker].iloc[0]
        fig.update_layout(title=f"{row['code']} : {row['stock_name']} ({ticker}) — 1y / 1d")
    except Exception:
        pass

    return fig

if __name__ == "__main__":
    # ローカル実行: python server.py
    # 本番(App Runner等): PORT を環境変数で渡す
    port = int(os.getenv("PORT", "8050"))
    debug = os.getenv("DEBUG", "false").lower() == "true"
    # 厳守: app.run を使用
    app.run(host="0.0.0.0", port=port, debug=debug)
