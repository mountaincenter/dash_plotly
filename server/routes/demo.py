# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import pandas as pd
from flask import Blueprint, jsonify, request, send_from_directory

from ..utils import (
    safe_demo_path,
    filter_bb_payload,
    parse_date_param,
    normalize_prices,
    to_json_records,
)

demo_bp = Blueprint("demo", __name__)

@demo_bp.get("/demo/json/<path:fname>")
def demo_json_file(fname: str):
    if not fname.lower().endswith(".json"):
        fname = fname + ".json"
    p = safe_demo_path(fname, {".json"})
    if not p:
        return jsonify({"error": "not found"}), 404
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@demo_bp.get("/demo/ichimoku/3350T")
def demo_ichimoku_3350t():
    return demo_json_file("ichimoku_3350T_demo.json")

# start/end で配列をスライス（±2σはサーバの値をそのまま使用）
@demo_bp.get("/demo/bb/3350T")
def demo_bb_3350t():
    p = safe_demo_path("bb_3350T_demo.json", {".json"})
    if not p:
        return jsonify({"error": "not found"}), 404
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    start_dt = parse_date_param(request.args.get("start"))
    end_dt = parse_date_param(request.args.get("end"))
    if start_dt and end_dt and start_dt > end_dt:
        return jsonify({"error": "start must be <= end"}), 400

    return jsonify(filter_bb_payload(data, start_dt, end_dt))

@demo_bp.get("/demo/bb30/3350T")
def demo_bb30_3350t():
    return demo_json_file("bb_3350T_demo_30d.json")

@demo_bp.get("/demo/dow-tod/3350T")
def demo_dow_tod_3350t():
    return demo_json_file("dow_tod_onecell_3350T.json")

@demo_bp.get("/demo/parquet/<path:fname>")
def demo_parquet_download(fname: str):
    if not fname.lower().endswith(".parquet"):
        fname = fname + ".parquet"
    p = safe_demo_path(fname, {".parquet"})
    if not p:
        return jsonify({"error": "not found"}), 404
    return send_from_directory(p.parent, p.name, mimetype="application/octet-stream", as_attachment=True)

@demo_bp.get("/demo/prices/max/1d/<code>")
def demo_prices_max_1d_json(code: str):
    code = code.upper().replace(".T", "T")
    fname = f"prices_max_1d_{code}_demo.parquet"
    p = safe_demo_path(fname, {".parquet"})
    if not p:
        return jsonify([])

    try:
        df = pd.read_parquet(str(p), engine="pyarrow")
        df = normalize_prices(df)
        if df.empty:
            return jsonify([])
        end_s = request.args.get("end")
        start_s = request.args.get("start")
        end_dt = pd.to_datetime(end_s).tz_localize(None) if end_s else None
        start_dt = pd.to_datetime(start_s).tz_localize(None) if start_s else None
        if start_dt is not None:
            df = df[df["date"] >= start_dt]
        if end_dt is not None:
            df = df[df["date"] <= end_dt]
        if "ticker" in df.columns:
            df["ticker"] = df["ticker"].astype(str)
        return jsonify(to_json_records(df))
    except Exception as e:
        return jsonify({"error": str(e)}), 500
