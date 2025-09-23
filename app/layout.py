# -*- coding: utf-8 -*-
"""
app.layout: Dash のレイアウト（タブ/ドロップダウン/補足表示含む）
"""
import numpy as np
import pandas as pd
from dash import html, dcc
from typing import Sequence


def build_dropdown_options(meta: pd.DataFrame) -> tuple[list[dict], str | None]:
    options: list[dict] = []
    if not meta.empty and {"code", "stock_name", "ticker"}.issubset(meta.columns):
        options = [
            {"label": f"{row.code}:{row.stock_name}", "value": row.ticker}
            for row in meta.itertuples(index=False)
        ]
    initial = options[0]["value"] if options else None
    return options, initial


def build_layout(src_label: str, anom_src_label: str, dropdown_options: Sequence[dict], initial_value: str | None):
    return html.Div(
        style={"maxWidth": "1200px", "margin": "0 auto", "padding": "12px"},
        children=[
            html.H2("TOPIX Core 30 — 1年・日足 / 月次（年×月）", style={"marginBottom": "8px"}),

            dcc.Dropdown(
                id="ticker-dd",
                options=list(dropdown_options),
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
                                    html.Span("※ データ取得は分析パイプライン側（1y/1d）。本アプリは可視化のみ。"),
                                    html.Br(),
                                    html.Span(f"データソース: {src_label} / アノマリー: {anom_src_label}"),
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
