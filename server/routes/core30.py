# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd
from flask import Blueprint, jsonify, request

from ..utils import (
    load_core30_meta,
    read_prices_1d_df,
    normalize_prices,
    to_json_records,
)

core30_bp = Blueprint("core30", __name__)

@core30_bp.get("/core30/meta")
def core30_meta():
    return jsonify(load_core30_meta())

@core30_bp.get("/core30/prices/max/1d")
def core30_prices_max_1d():
    df = read_prices_1d_df()
    if df is None:
        return jsonify([])
    out = normalize_prices(df)
    if out.empty:
        return jsonify([])
    return jsonify(to_json_records(out))

@core30_bp.get("/core30/prices/1d")
def core30_prices_1d():
    ticker = (request.args.get("ticker") or "").strip()
    if not ticker:
        return jsonify({"error": "ticker is required"}), 400

    today = pd.Timestamp.utcnow().tz_localize(None).normalize()
    end_s = request.args.get("end")
    start_s = request.args.get("start")
    try:
        end_dt = pd.to_datetime(end_s).tz_localize(None) if end_s else today
    except Exception:
        return jsonify({"error": "invalid end"}), 400
    try:
        start_dt = pd.to_datetime(start_s).tz_localize(None) if start_s else end_dt - pd.Timedelta(days=365)
    except Exception:
        return jsonify({"error": "invalid start"}), 400
    if start_dt > end_dt:
        return jsonify({"error": "start must be <= end"}), 400

    df = read_prices_1d_df()
    if df is None:
        return jsonify([])

    out = normalize_prices(df)
    if out.empty:
        return jsonify([])

    sel = out[(out["ticker"] == ticker) & (out["date"] >= start_dt) & (out["date"] <= end_dt)]
    if sel.empty:
        return jsonify([])
    return jsonify(to_json_records(sel))

# 追加ルート: /core30/prices/snapshot/last2
@core30_bp.get("/core30/prices/snapshot/last2")
def core30_prices_snapshot_last2():
    """
    各tickerについて:
      - 最新日の終値 close
      - その前営業日の終値 prevClose（なければ None）
      - 差分 diff = close - prevClose（prevCloseが無ければ None）
      - 最新日の date（YYYY-MM-DD）
    を返す。
    """
    df = read_prices_1d_df()
    if df is None:
        return jsonify([])

    out = normalize_prices(df)
    if out.empty:
        return jsonify([])

    # ソートし、前日終値を groupby + shift で作る
    g = out.sort_values(["ticker", "date"]).copy()
    g["prevClose"] = g.groupby("ticker")["Close"].shift(1)

    # 各tickerの最新行だけ抽出
    snap = g.groupby("ticker", as_index=False).tail(1).copy()

    # 差分
    snap["diff"] = snap["Close"] - snap["prevClose"]

    # JSON フォーマットに整形
    # dateは YYYY-MM-DD 文字列へ
    snap["date"] = snap["date"].dt.strftime("%Y-%m-%d")

    # 欠損は None に（NaNはそのままだとJSONに出ないことがあるため）
    def _none(v):
        return None if pd.isna(v) else float(v) if isinstance(v, (int, float)) else v

    records = []
    for _, r in snap.iterrows():
        records.append({
            "ticker": str(r["ticker"]),
            "date": r["date"],
            "close": _none(r["Close"]),
            "prevClose": _none(r["prevClose"]),
            "diff": _none(r["diff"]),
        })

    return jsonify(records)
    # 追加：パフォーマンス・スナップショット
@core30_bp.get("/core30/perf/returns")
def core30_perf_returns():
    """
    例: /core30/perf/returns?windows=5d,1mo,3mo,ytd,1y,5y,all
    各tickerの最新終値を基準に、指定ウィンドウの騰落率(%)を返す。
    - 5d: 直近営業日から5営業日相当（暫定: 7暦日戻し）以前の直近終値
    - 1mo: 30日, 3mo: 90日, 1y: 365日, 5y: 1825日（暦日換算の近似）
    - ytd: 同一年度の最初の営業日の終値
    - all: その銘柄の最初の終値
    """
    df = read_prices_1d_df()
    if df is None:
        return jsonify([])

    out = normalize_prices(df)
    if out.empty:
        return jsonify([])

    # パース: windows
    win_param = (request.args.get("windows") or "").strip()
    default_wins = ["5d", "1mo", "3mo", "ytd", "1y", "5y", "all"]
    wins = [w.strip() for w in win_param.split(",") if w.strip()] or default_wins

    # ウィンドウ -> 日数
    days_map = {
        "5d": 7,
        "1w": 7,
        "1mo": 30,
        "3mo": 90,
        "6mo": 180,
        "1y": 365,
        "3y": 365 * 3,
        "5y": 365 * 5,
    }

    def pct_return(last_close, base_close):
        if last_close is None or base_close is None or pd.isna(last_close) or pd.isna(base_close) or base_close == 0:
            return None
        return float((last_close / base_close - 1.0) * 100.0)

    records = []
    for tkr, g in out.sort_values(["ticker", "date"]).groupby("ticker", as_index=False):
        g = g[["date", "Close"]].dropna(subset=["Close"])
        if g.empty:
            continue
        last_row = g.iloc[-1]
        last_date = last_row["date"]
        last_close = float(last_row["Close"])

        # ユーティリティ：target日以前の最後の終値
        def base_close_before_or_on(target_dt: pd.Timestamp):
            sel = g[g["date"] <= target_dt]
            if sel.empty:
                return None
            return float(sel.iloc[-1]["Close"])

        row = {"ticker": tkr, "date": last_date.strftime("%Y-%m-%d")}
        for w in wins:
            key = f"r_{w}"
            if w == "ytd":
                # 同年最初の営業日
                start_of_year = pd.Timestamp(year=last_date.year, month=1, day=1)
                base = base_close_before_or_on(start_of_year)
                row[key] = pct_return(last_close, base)
            elif w == "all":
                base = float(g.iloc[0]["Close"])
                row[key] = pct_return(last_close, base)
            else:
                days = days_map.get(w)
                if not days:
                    # 未知のウィンドウは無視（または None）
                    row[key] = None
                else:
                    target = last_date - pd.Timedelta(days=days)
                    base = base_close_before_or_on(target)
                    row[key] = pct_return(last_close, base)

        records.append(row)

    return jsonify(records)
