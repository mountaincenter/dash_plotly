# -*- coding: utf-8 -*-
"""
app.app: アプリ生成（構成読み込み→データロード→レイアウト→コールバック登録）
"""
from dash import Dash
from .config import load_app_config
from .data_loader import load_meta_and_prices, load_anomaly
from .layout import build_layout, build_dropdown_options
from .callbacks import register_callbacks


def create_app() -> Dash:
    # 設定
    cfg = load_app_config()

    # データロード
    meta, prices, src = load_meta_and_prices(cfg)
    anomaly, an_src = load_anomaly(cfg)

    # 型の最小安全化
    for c in ("code", "stock_name", "ticker"):
        if c in meta.columns:
            meta[c] = meta[c].astype("string")
    if "ticker" in anomaly.columns:
        anomaly["ticker"] = anomaly["ticker"].astype("string")

    # レイアウト
    options, initial_value = build_dropdown_options(meta)
    app = Dash(__name__)
    app.title = "TOPIX Core 30 — 1y/1d Candlestick + Monthly Matrix"
    app.layout = build_layout(src, an_src, options, initial_value)

    # コールバック
    register_callbacks(app, meta, prices, anomaly)
    return app
