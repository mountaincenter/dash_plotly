#!/usr/bin/env python3
"""
generate_market_report_data.py
マーケットレポート用データを全parquetから集約し、1つのJSONに出力する。

出力: data/parquet/market_summary/structured/report_data_YYYY-MM-DD.json
用途: Claude がレポート作成時にこの1ファイルを読むだけで全データを参照できる。

実行:
    python scripts/pipeline/generate_market_report_data.py              # 最新営業日
    python scripts/pipeline/generate_market_report_data.py --date 2026-03-04
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

OUTPUT_DIR = PARQUET_DIR / "market_summary" / "structured"

# 曜日名マップ
WEEKDAY_JP = {0: "月曜", 1: "火曜", 2: "水曜", 3: "木曜", 4: "金曜", 5: "土曜", 6: "日曜"}


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def _safe(fn: callable, section_name: str) -> Any:
    """セクション関数を実行し、例外時は None を返す"""
    try:
        return fn()
    except Exception as e:
        print(f"  [WARN] {section_name} failed: {e}")
        return None


def _read_parquet(name: str) -> pd.DataFrame | None:
    path = PARQUET_DIR / name
    if not path.exists():
        print(f"  [WARN] {name} not found")
        return None
    return pd.read_parquet(path)


def _to_date(d: Any) -> str:
    """日付をYYYY-MM-DD文字列に変換"""
    if isinstance(d, str):
        return d[:10]
    if hasattr(d, "strftime"):
        return d.strftime("%Y-%m-%d")
    return str(d)


def _f(v: Any, decimals: int = 4) -> Any:
    """NaN/Inf を None に変換し、float を丸める（JSON安全化）"""
    if v is None:
        return None
    if isinstance(v, str):
        try:
            v = float(v)
        except (ValueError, TypeError):
            return v
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        return round(float(v), decimals)
    return v


def _get_latest_trading_date(df: pd.DataFrame) -> str | None:
    """DataFrame の date カラムから最新日付を取得"""
    if df is None or df.empty or "date" not in df.columns:
        return None
    return _to_date(pd.to_datetime(df["date"]).max())


def _detect_target_date() -> str:
    """parquet の最新日付から営業日を自動検出"""
    df = _read_parquet("index_prices_max_1d.parquet")
    if df is not None:
        d = _get_latest_trading_date(df)
        if d:
            return d
    # フォールバック: JST の今日
    jst = timezone(timedelta(hours=9))
    return datetime.now(jst).strftime("%Y-%m-%d")


def _prev_close_for(df: pd.DataFrame, ticker: str, target_date: str) -> float | None:
    """指定 ticker の前営業日終値を取得（NaN行は除外）"""
    close_col = "Close" if "Close" in df.columns else "close"
    sub = df[df["ticker"] == ticker].copy() if "ticker" in df.columns else df.copy()
    sub["date"] = pd.to_datetime(sub["date"])
    sub = sub.sort_values("date")
    sub = sub.dropna(subset=[close_col])
    td = pd.Timestamp(target_date)
    prev = sub[sub["date"] < td]
    if prev.empty:
        return None
    return float(prev.iloc[-1][close_col])


def _row_for(df: pd.DataFrame, ticker: str, target_date: str) -> pd.Series | None:
    """指定 ticker・日付の行を取得（NaN行は除外）"""
    close_col = "Close" if "Close" in df.columns else "close"
    sub = df[df["ticker"] == ticker].copy() if "ticker" in df.columns else df.copy()
    sub["date"] = pd.to_datetime(sub["date"]).dt.strftime("%Y-%m-%d")
    row = sub[sub["date"] == target_date]
    if row.empty:
        return None
    r = row.iloc[0]
    # Close が NaN なら無効行
    if pd.isna(r[close_col]):
        return None
    return r


def _build_ohlc_entry(df: pd.DataFrame, ticker: str, target_date: str,
                       include_hl: bool = False) -> dict[str, Any]:
    """OHLC + 前日比のエントリを構築。parquetになければyfinanceフォールバック"""
    row = _row_for(df, ticker, target_date)
    if row is None:
        # parquetにデータがない → yfinanceで取得
        yf_result = _yfinance_ohlc(ticker, target_date, include_hl)
        if yf_result:
            return yf_result
        return {"error": "no_data"}
    close_col = "Close" if "Close" in row.index else "close"
    high_col = "High" if "High" in row.index else "high"
    low_col = "Low" if "Low" in row.index else "low"

    close = float(row[close_col])
    prev_close = _prev_close_for(df, ticker, target_date)
    change = close - prev_close if prev_close else None
    change_pct = (change / prev_close * 100) if prev_close else None

    result: dict[str, Any] = {"close": _f(close), "prev_close": _f(prev_close),
                               "change": _f(change), "change_pct": _f(change_pct)}
    if include_hl:
        result["high"] = _f(float(row[high_col])) if high_col in row.index else None
        result["low"] = _f(float(row[low_col])) if low_col in row.index else None
    return result


def _yfinance_ohlc(ticker: str, target_date: str, include_hl: bool = False) -> dict[str, Any] | None:
    """yfinanceからOHLC+前日比を取得（_build_ohlc_entryのフォールバック）"""
    try:
        import yfinance as yf
        end = pd.Timestamp(target_date) + pd.Timedelta(days=3)
        start = pd.Timestamp(target_date) - pd.Timedelta(days=7)
        data = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                           end=end.strftime("%Y-%m-%d"), progress=False)
        if data.empty:
            return None
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        data = data.sort_index()
        td = pd.Timestamp(target_date)
        target_rows = data[data.index == td]
        if target_rows.empty:
            # 対象日以前の最新行を使用
            target_rows = data[data.index <= td].tail(1)
        if target_rows.empty:
            return None
        current = target_rows.iloc[0]
        close = float(current["Close"])
        if pd.isna(close):
            return None
        # 前日終値
        prev_rows = data[data.index < td].dropna(subset=["Close"])
        prev_close = float(prev_rows.iloc[-1]["Close"]) if len(prev_rows) >= 1 else None
        change = close - prev_close if prev_close else None
        change_pct = (change / prev_close * 100) if prev_close else None
        result: dict[str, Any] = {"close": _f(close), "prev_close": _f(prev_close),
                                   "change": _f(change), "change_pct": _f(change_pct)}
        if include_hl:
            result["high"] = _f(float(current["High"])) if not pd.isna(current.get("High")) else None
            result["low"] = _f(float(current["Low"])) if not pd.isna(current.get("Low")) else None
        return result
    except Exception:
        return None


def _yfinance_latest(ticker: str, target_date: str) -> dict[str, Any]:
    """yfinance で直近2日分取得し、前日比を計算"""
    try:
        import yfinance as yf
        end = pd.Timestamp(target_date) + pd.Timedelta(days=3)
        start = pd.Timestamp(target_date) - pd.Timedelta(days=7)
        data = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                           end=end.strftime("%Y-%m-%d"), progress=False)
        if data.empty:
            return {"error": "no_data"}
        # yfinance returns MultiIndex columns when single ticker
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        data = data.sort_index()
        td = pd.Timestamp(target_date)
        # 対象日以前の最新2行を取得
        recent = data[data.index <= td].tail(2)
        if len(recent) < 1:
            return {"error": "no_data"}
        current = recent.iloc[-1]
        close = float(current["Close"])
        prev_close = float(recent.iloc[-2]["Close"]) if len(recent) >= 2 else None
        change_pct = ((close - prev_close) / prev_close * 100) if prev_close else None
        return {"close": _f(close), "change_pct": _f(change_pct)}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# セクション builders
# ---------------------------------------------------------------------------

def build_market_summary(date: str) -> dict[str, Any]:
    """国内主要指数 + 為替 + VI"""
    idx = _read_parquet("index_prices_max_1d.parquet")
    topix = _read_parquet("topix_prices_max_1d.parquet")
    curr = _read_parquet("currency_prices_max_1d.parquet")
    vi = _read_parquet("nikkei_vi_max_1d.parquet")

    result: dict[str, Any] = {}

    # N225
    if idx is not None:
        result["n225"] = _build_ohlc_entry(idx, "^N225", date, include_hl=True)

    # TOPIX 系
    if topix is not None:
        for ticker, key in [("0000", "topix"), ("0500", "topix_prime"),
                            ("0501", "topix_standard"), ("0502", "topix_growth")]:
            row = _row_for(topix, ticker, date)
            if row is not None:
                close = float(row["close"])
                prev = _prev_close_for(topix, ticker, date)
                pct = ((close - prev) / prev * 100) if prev else None
                result[key] = {"close": _f(close), "prev_close": _f(prev), "change_pct": _f(pct)}

    # USDJPY — yfinance直接取得（parquetは16:45時点でNaNになるため）
    result["usdjpy"] = _yfinance_latest("JPY=X", date)

    # Nikkei VI (no ticker column)
    if vi is not None:
        vi_copy = vi.copy()
        vi_copy["ticker"] = "VI"
        result["vi"] = _build_ohlc_entry(vi_copy, "VI", date, include_hl=True)

    return result


def build_divergence(date: str) -> dict[str, Any]:
    """N225 vs TOPIX の乖離（5日分）"""
    idx = _read_parquet("index_prices_max_1d.parquet")
    topix = _read_parquet("topix_prices_max_1d.parquet")
    if idx is None or topix is None:
        return {"error": "data_missing"}

    # N225
    n225 = idx[idx["ticker"] == "^N225"].copy()
    n225["date"] = pd.to_datetime(n225["date"]).dt.strftime("%Y-%m-%d")
    close_col = "Close" if "Close" in n225.columns else "close"
    n225 = n225.sort_values("date")
    n225["pct"] = n225[close_col].pct_change(fill_method=None) * 100

    # TOPIX (0000)
    tp = topix[topix["ticker"] == "0000"].copy()
    tp["date"] = pd.to_datetime(tp["date"]).dt.strftime("%Y-%m-%d")
    tp = tp.sort_values("date")
    tp["pct"] = tp["close"].pct_change(fill_method=None) * 100

    # 5日分（対象日以前）
    n225_recent = n225[n225["date"] <= date].tail(5)
    tp_recent = tp[tp["date"] <= date].tail(5)

    merged = n225_recent[["date", "pct"]].merge(
        tp_recent[["date", "pct"]], on="date", suffixes=("_n225", "_topix")
    )
    merged["gap"] = merged["pct_n225"] - merged["pct_topix"]

    history = []
    for _, r in merged.iterrows():
        history.append({
            "date": r["date"],
            "n225_pct": _f(r["pct_n225"]),
            "topix_pct": _f(r["pct_topix"]),
            "gap": _f(r["gap"]),
        })

    today_row = merged[merged["date"] == date]
    today = {}
    if not today_row.empty:
        t = today_row.iloc[0]
        today = {"n225_pct": _f(t["pct_n225"]), "topix_pct": _f(t["pct_topix"]), "gap": _f(t["gap"])}

    return {"today": today, "history_5d": history}


def build_sectors(date: str) -> dict[str, Any]:
    """33業種別指数"""
    df = _read_parquet("sectors_prices_max_1d.parquet")
    if df is None:
        return {"error": "data_missing"}

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    today = df[df["date"] == date].copy()
    if today.empty:
        return {"error": "no_data_for_date", "date": date}

    sectors = []
    for _, row in today.iterrows():
        close = float(row["close"])
        prev = _prev_close_for(df, row["ticker"], date)
        pct = ((close - prev) / prev * 100) if prev else None
        sectors.append({
            "name": row.get("name", ""),
            "code": row.get("ticker", row.get("code", "")),
            "close": _f(close),
            "prev_close": _f(prev),
            "change_pct": _f(pct),
        })

    # 変動率降順
    sectors.sort(key=lambda x: x["change_pct"] if x["change_pct"] is not None else -999, reverse=True)

    up = sum(1 for s in sectors if s["change_pct"] is not None and s["change_pct"] > 0)
    down = sum(1 for s in sectors if s["change_pct"] is not None and s["change_pct"] < 0)
    unchanged = len(sectors) - up - down

    return {"date": date, "all": sectors, "up_count": up, "down_count": down, "unchanged_count": unchanged}


def build_foreign_markets(date: str) -> dict[str, Any]:
    """海外市場指数"""
    idx = _read_parquet("index_prices_max_1d.parquet")
    fut = _read_parquet("futures_prices_max_1d.parquet")

    result: dict[str, Any] = {"us": {}, "asia": {}, "futures": {}}

    # 米株は前営業日データ（米国時間ずれ）→ yfinance で取得
    us_map = {"sp500": "^GSPC", "nasdaq": "^IXIC", "dow": "^DJI"}
    for key, ticker in us_map.items():
        result["us"][key] = _yfinance_latest(ticker, date)

    # アジア（ローカルparquet）
    asia_map = {"kospi": "^KS11", "shanghai": "000001.SS", "hang_seng": "^HSI"}
    for key, ticker in asia_map.items():
        if idx is not None:
            result["asia"][key] = _build_ohlc_entry(idx, ticker, date)
        else:
            result["asia"][key] = {"error": "data_missing"}

    # 先物
    if fut is not None:
        result["futures"]["nkd"] = _build_ohlc_entry(fut, "NKD=F", date)
    # VIX (yfinance)
    result["futures"]["vix"] = _yfinance_latest("^VIX", date)

    return result


def build_commodities(date: str) -> dict[str, Any]:
    """商品先物"""
    fut = _read_parquet("futures_prices_max_1d.parquet")
    if fut is None:
        return {"error": "data_missing"}

    result: dict[str, Any] = {}
    for key, ticker in [("wti", "CL=F"), ("gold", "GC=F"), ("copper", "HG=F")]:
        entry = _build_ohlc_entry(fut, ticker, date)
        result[key] = entry
    return result


def build_rates(date: str) -> dict[str, Any]:
    """金利・為替公定レート"""
    result: dict[str, Any] = {}

    # US10Y — yfinance直接取得（parquetは16:45時点でNaNになるため）
    result["us10y"] = _yfinance_latest("^TNX", date)

    # JGB10Y (MOF jgbcm.csv)
    result["jgb10y"] = _fetch_mof_jgb10y()

    # 日米金利差
    us10y_val = result.get("us10y", {}).get("close")
    jgb_val = result.get("jgb10y", {}).get("value")
    if us10y_val is not None and jgb_val is not None:
        result["rate_diff_us_jp"] = _f(us10y_val - jgb_val)
    else:
        result["rate_diff_us_jp"] = None

    # 無担保コールO/N (BOJ FM01)
    result["overnight_call"] = _fetch_boj_fm01()

    # 公表仲値・17時スポット (BOJ FM08)
    result["boj_fx"] = _fetch_boj_fm08()

    return result


def _fetch_mof_jgb10y() -> dict[str, Any]:
    """財務省 国債金利情報CSV から10年金利を取得"""
    try:
        url = "https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return {"error": "fetch_failed", "detail": "MOF CSV not found"}
        # Shift-JIS でエンコードされている場合がある
        content = resp.content.decode("shift_jis", errors="replace")
        df = pd.read_csv(io.StringIO(content), skiprows=1)
        # カラム名: 基準日, 1年, 2年, ... 10年, ...
        df.columns = [c.strip() for c in df.columns]
        date_col = df.columns[0]
        col_10y = [c for c in df.columns if "10年" in c or c == "10年"]
        if not col_10y:
            # カラム番号で推定（基準日, 1年, 2年, 3年, 4年, 5年, 6年, 7年, 8年, 9年, 10年, ...）
            col_10y = [df.columns[10]] if len(df.columns) > 10 else []
        if not col_10y:
            return {"error": "column_not_found"}
        latest = df.dropna(subset=col_10y).iloc[-1]
        return {
            "value": _f(float(latest[col_10y[0]])),
            "date": str(latest[date_col]).strip(),
            "source": "mof_jgbcm",
        }
    except Exception as e:
        return {"error": "fetch_failed", "detail": str(e)}


def _fetch_boj_fm01() -> dict[str, Any]:
    """日本銀行 FM01（無担保コールO/N金利）— BOJ時系列統計API"""
    try:
        from datetime import datetime
        now = datetime.now()
        ym = now.strftime("%Y%m")
        url = "https://www.stat-search.boj.or.jp/api/v1/getDataCode"
        resp = requests.get(url, params={
            "format": "json", "lang": "jp", "db": "fm01",
            "code": "STRDCLUCON", "startDate": ym, "endDate": ym,
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        rs = data.get("RESULTSET", [])
        if not rs:
            return {"error": "no_resultset"}
        vals = rs[0].get("VALUES", {})
        dates = vals.get("SURVEY_DATES", [])
        values = vals.get("VALUES", [])
        # 末尾から非null値を探す
        for i in range(len(values) - 1, -1, -1):
            if values[i] is not None:
                d = str(dates[i])
                date_str = f"{d[:4]}/{d[4:6]}/{d[6:]}"
                return {"rate": _f(values[i]), "date": date_str, "source": "boj_api_fm01"}
        return {"error": "no_data", "detail": "all values null"}
    except Exception as e:
        return {"error": "fetch_failed", "detail": str(e)}


def _fetch_boj_fm08() -> dict[str, Any]:
    """日本銀行 FM08（外国為替公表相場）"""
    try:
        url = "https://www.boj.or.jp/about/services/tame/tame_rate/kijun/index.htm"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        content = resp.content.decode("utf-8", errors="replace")
        tables = pd.read_html(io.StringIO(content))
        if not tables:
            return {"error": "no_table"}
        # USD/JPY の公表仲値を探す
        for tbl in tables:
            tbl_str = tbl.to_string()
            if "米ドル" in tbl_str or "USD" in tbl_str:
                for _, row in tbl.iterrows():
                    row_str = " ".join(str(v) for v in row.values)
                    if "米ドル" in row_str or "USD" in row_str:
                        nums = []
                        for v in row.values:
                            try:
                                val = float(str(v).replace(",", ""))
                                if 50 < val < 300:  # USD/JPY の妥当な範囲
                                    nums.append(val)
                            except (ValueError, TypeError):
                                continue
                        if nums:
                            return {"center": _f(nums[0]), "source": "boj_fm08"}
        return {"error": "parse_failed"}
    except Exception as e:
        return {"error": "fetch_failed", "detail": str(e)}


def _short_category(row: pd.Series | dict) -> str:
    """空売り区分を判定: 制度/いちにち/いちにち残0/NG"""
    shortable = bool(row.get("shortable", False) or row.get("is_shortable", False))
    if shortable:
        return "制度"
    day_trade = row.get("day_trade", False)
    if day_trade:
        dt_shares = row.get("day_trade_available_shares", 0)
        if pd.isna(dt_shares):
            dt_shares = 0
        return "いちにち" if float(dt_shares) > 0 else "いちにち残0"
    return "NG"


def _build_grok_from_archive(arc_for: pd.DataFrame, date: str) -> dict[str, Any]:
    """archive データから grok セクションを構築（grade カラムがない場合あり）"""
    details = []
    for _, row in arc_for.iterrows():
        p2_long = _f(row.get("profit_per_100_shares_phase2"))
        short_result = -p2_long if p2_long is not None else None
        label = ("WIN" if short_result > 0 else "LOSE" if short_result < 0 else "DRAW") if short_result is not None else None
        details.append({
            "ticker": row.get("ticker", ""),
            "stock_name": row.get("stock_name", ""),
            "grade": row.get("grade", ""),
            "buy_price": _f(row.get("buy_price")),
            "shortable": bool(row.get("shortable", False)),
            "short_category": _short_category(row),
            "p2_long": p2_long,
            "short_result": _f(short_result),
            "short_result_label": label,
        })

    # grade 分布（archive に grade がある場合のみ）
    grades = [d["grade"] for d in details if d["grade"]]
    grade_dist = {}
    for g in ["G1", "G2", "G3", "G4"]:
        grade_dist[g] = grades.count(g)

    g1g2 = [d for d in details if d["grade"] in ("G1", "G2")]
    g1g2_short_total = sum(d["short_result"] or 0 for d in g1g2)
    g1g2_tradeable = [d for d in g1g2 if d.get("short_category") in ("制度", "いちにち")]
    g1g2_tradeable_total = sum(d["short_result"] or 0 for d in g1g2_tradeable)

    return {
        "selection_date": date,
        "grade_distribution": grade_dist,
        "total": len(details),
        "details": details,
        "summary": {
            "g1g2_short_total": _f(g1g2_short_total),
            "g1g2_tradeable_total": _f(g1g2_tradeable_total),
            "g1g2_count": len(g1g2),
            "g1g2_win": sum(1 for d in g1g2 if d["short_result_label"] == "WIN"),
            "g1g2_lose": sum(1 for d in g1g2 if d["short_result_label"] == "LOSE"),
            "g1g2_draw": sum(1 for d in g1g2 if d["short_result_label"] == "DRAW"),
        },
        "source": "archive",
    }


def build_grok(date: str) -> dict[str, Any] | None:
    """Grok銘柄選定データ"""
    grok = _read_parquet("grok_trending.parquet")
    if grok is None:
        return None

    grok_date = grok["date"].iloc[0]
    grok_date_str = _to_date(grok_date)

    # grok_trending は最新選定のみ保持。対象日と不一致なら archive から取得
    if grok_date_str != date:
        print(f"  [INFO] grok_trending date={grok_date_str} != target={date}, trying archive")
        arc = _read_parquet("backtest/grok_trending_archive.parquet")
        if arc is not None and "selection_date" in arc.columns:
            arc_for = arc[arc["selection_date"] == date]
            if not arc_for.empty:
                # archive からの簡易構築（grade がない場合あり）
                return _build_grok_from_archive(arc_for, date)
        print(f"  [INFO] No grok data for {date}")
        return None

    # Grade 分布
    grade_dist = grok["grade"].value_counts().to_dict()
    for g in ["G1", "G2", "G3", "G4"]:
        if g not in grade_dist:
            grade_dist[g] = 0

    # Archive から P2 結合
    arc = _read_parquet("backtest/grok_trending_archive.parquet")
    p2_map: dict[str, float] = {}
    if arc is not None:
        arc_for = arc[arc["selection_date"] == grok_date_str]
        for _, r in arc_for.iterrows():
            p2_map[r["ticker"]] = r.get("profit_per_100_shares_phase2", None)

    # 銘柄詳細
    details = []
    for _, row in grok.iterrows():
        ticker = row["ticker"]
        p2_long = p2_map.get(ticker)
        # ショート結果: LONG利益の符号反転
        short_result = -p2_long if p2_long is not None else None
        if short_result is not None:
            if short_result > 0:
                label = "WIN"
            elif short_result < 0:
                label = "LOSE"
            else:
                label = "DRAW"
        else:
            label = None

        details.append({
            "ticker": ticker,
            "stock_name": row.get("stock_name", ""),
            "grade": row.get("grade", ""),
            "buy_price": _f(row.get("Close", row.get("close"))),
            "shortable": bool(row.get("shortable", False)),
            "short_category": _short_category(row),
            "p2_long": _f(p2_long),
            "short_result": _f(short_result),
            "short_result_label": label,
        })

    # G1+G2 サマリー
    g1g2 = [d for d in details if d["grade"] in ("G1", "G2")]
    g1g2_short_total = sum(d["short_result"] or 0 for d in g1g2)
    g1g2_tradeable = [d for d in g1g2 if d["short_category"] in ("制度", "いちにち")]
    g1g2_tradeable_total = sum(d["short_result"] or 0 for d in g1g2_tradeable)
    g1g2_win = sum(1 for d in g1g2 if d["short_result_label"] == "WIN")
    g1g2_lose = sum(1 for d in g1g2 if d["short_result_label"] == "LOSE")
    g1g2_draw = sum(1 for d in g1g2 if d["short_result_label"] == "DRAW")

    return {
        "selection_date": grok_date_str,
        "grade_distribution": grade_dist,
        "total": len(grok),
        "details": details,
        "summary": {
            "g1g2_short_total": _f(g1g2_short_total),
            "g1g2_tradeable_total": _f(g1g2_tradeable_total),
            "g1g2_count": len(g1g2),
            "g1g2_win": g1g2_win,
            "g1g2_lose": g1g2_lose,
            "g1g2_draw": g1g2_draw,
        },
    }


def build_anomaly(date: str) -> dict[str, Any] | None:
    """カレンダーアノマリー — 翌営業日のアノマリーを返す（大引け後に読むため）"""
    ma = _read_parquet("market_anomaly.parquet")
    if ma is None:
        return None

    td = pd.Timestamp(date)
    # 翌営業日を算出（土日スキップ）
    next_bd = td + pd.offsets.BDay(1)
    weekday_idx = next_bd.weekday()  # 0=月, 4=金
    weekday_name = WEEKDAY_JP.get(weekday_idx, str(weekday_idx))
    week_of_year = next_bd.isocalendar()[1]
    month = next_bd.month

    # N225_dow テーブルから翌営業日の曜日を取得
    dow = ma[ma["table_name"] == "N225_dow"]
    result: dict[str, Any] = {
        "target_date": next_bd.strftime("%Y-%m-%d"),
        "weekday": weekday_name,
        "week_of_year": week_of_year,
        "month": month,
    }

    dow_row = dow[dow["index"] == str(weekday_idx)]
    if not dow_row.empty:
        r = dow_row.iloc[0]
        result["n225_dow"] = {
            "avg_all": _f(r.get("avg_all")),
            "win_rate_all": _f(r.get("win_rate_all")),
            "count_all": _f(r.get("count_all")),
            "avg_5y": _f(r.get("avg_5y")),
            "win_rate_5y": _f(r.get("win_rate_5y")),
        }

    return result


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main(target_date: str | None = None) -> int:
    print("=" * 60)
    print("Generate Market Report Data")
    print("=" * 60)

    date = target_date or _detect_target_date()
    print(f"  Target date: {date}")

    result: dict[str, Any] = {
        "date": date,
        "generated_at": datetime.now(timezone(timedelta(hours=9))).isoformat(),
    }

    result["market_summary"] = _safe(lambda: build_market_summary(date), "market_summary")
    result["n225_topix_divergence"] = _safe(lambda: build_divergence(date), "divergence")
    result["sectors"] = _safe(lambda: build_sectors(date), "sectors")
    result["foreign_markets"] = _safe(lambda: build_foreign_markets(date), "foreign_markets")
    result["commodities"] = _safe(lambda: build_commodities(date), "commodities")
    result["rates"] = _safe(lambda: build_rates(date), "rates")
    result["grok"] = _safe(lambda: build_grok(date), "grok")
    result["calendar_anomaly"] = _safe(lambda: build_anomaly(date), "calendar_anomaly")
    # 外部CSV取得失敗分を web_search_required に追加
    web_search = ["騰落銘柄数", "売買代金"]
    rates = result.get("rates") or {}
    if isinstance(rates.get("overnight_call"), dict) and "error" in rates["overnight_call"]:
        web_search.append("無担保コールO/N金利")
    if isinstance(rates.get("boj_fx"), dict) and "error" in rates["boj_fx"]:
        web_search.append("公表仲値（USD/JPY）")
    result["web_search_required"] = web_search

    # 出力
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"report_data_{date}.json"
    output_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"\n  [OK] Saved: {output_path}")

    # セクション別ステータス
    for key in ["market_summary", "n225_topix_divergence", "sectors", "foreign_markets",
                 "commodities", "rates", "grok", "calendar_anomaly"]:
        val = result.get(key)
        if val is None:
            status = "NULL"
        elif isinstance(val, dict) and "error" in val:
            status = f"ERROR: {val['error']}"
        else:
            status = "OK"
        print(f"  {key}: {status}")

    print("=" * 60)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate market report data JSON")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    args = parser.parse_args()
    raise SystemExit(main(args.date))
