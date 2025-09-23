# -*- coding: utf-8 -*-
"""
app.callbacks: すべてのコールバック登録
"""
import numpy as np
import pandas as pd
from dash import Input, Output, html
from dash import dash_table
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import Dash


def register_callbacks(app: Dash, meta: pd.DataFrame, prices: pd.DataFrame, anomaly: pd.DataFrame) -> None:
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

        # ===== 価格チャート / テーブル =====
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

        # ===== アノマリー（年×月ヒートマップ）=====
        if not anomaly.empty and {"ticker", "year", "month", "return_pct"}.issubset(anomaly.columns):
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
