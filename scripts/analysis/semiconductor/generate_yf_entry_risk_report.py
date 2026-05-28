from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.jquants_client import JQuantsClient  # noqa: E402
from scripts.lib.jquants_fetcher import JQuantsFetcher  # noqa: E402


OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_FILE = OUT_DIR / "ai_semiconductor_yf_entry_risk_report.html"


@dataclass(frozen=True)
class Stock:
    code: str
    name: str
    label: str
    segment: str


UNIVERSE = [
    Stock("6857", "アドバンテスト", "A", "テスタ/検査"),
    Stock("6146", "ディスコ", "A", "後工程装置"),
    Stock("8035", "東京エレクトロン", "A", "エッチング/成膜/塗布"),
    Stock("6920", "レーザーテック", "A", "EUVマスク検査"),
    Stock("7735", "SCREEN", "A", "洗浄装置"),
    Stock("6525", "KOKUSAI ELECTRIC", "A", "成膜装置"),
    Stock("4062", "イビデン", "A", "パッケージ基板"),
    Stock("4186", "東京応化工業", "A", "フォトレジスト"),
    Stock("4004", "レゾナック", "A", "パッケージ/CMP材料"),
    Stock("3436", "SUMCO", "A", "シリコンウエハ"),
    Stock("4063", "信越化学工業", "A", "シリコンウエハ"),
    Stock("6723", "ルネサス", "A", "マイコン/アナログ"),
    Stock("285A", "キオクシア", "A", "NAND"),
    Stock("6503", "三菱電機", "A", "パワー半導体/重電"),
    Stock("6504", "富士電機", "A", "パワー半導体"),
    Stock("6501", "日立製作所", "A/B", "検査装置/電力"),
    Stock("5801", "古河電工", "B", "光通信/電力ケーブル/冷却"),
    Stock("5803", "フジクラ", "B", "光通信/高密度配線"),
    Stock("6981", "村田製作所", "B", "MLCC/電源部品/EMI"),
    Stock("6976", "太陽誘電", "B", "MLCC/受動部品"),
    Stock("6367", "ダイキン工業", "B", "冷却/空調"),
    Stock("5802", "住友電工", "C", "光通信/電線"),
    Stock("5805", "SWCC", "C", "電線/電力ケーブル"),
    Stock("6645", "オムロン", "C", "電源/制御"),
    Stock("1963", "日揮HD", "C", "設備/EPC"),
    Stock("1979", "大気社", "C", "空調/クリーンルーム"),
    Stock("1802", "大林組", "D", "建設"),
    Stock("1925", "大和ハウス", "D", "建設/不動産"),
    Stock("8801", "三井不動産", "D", "不動産/DC"),
    Stock("8802", "三菱地所", "D", "不動産/DC"),
]

OVERSEAS = ["NVDA", "^SOX", "AVGO", "MU", "TSM", "^IXIC", "NQ=F", "NKD=F", "JPY=X"]
LABEL_SCORE = {"A": 3.0, "A/B": 2.5, "B": 2.0, "C": 0.0, "D": -1.0}


def pct(a: float, b: float) -> float:
    if pd.isna(a) or pd.isna(b) or b == 0:
        return np.nan
    return (a / b - 1.0) * 100.0


PRICE_KEYS = {"close", "prev_high", "m5_last", "vwap", "sales_bil", "op_bil", "fop_bil"}
SIGNED_PERCENT_KEYS = {
    "ret1",
    "ret5",
    "ret20",
    "vs25",
    "vs75",
    "slope25_5d",
    "dist20hi",
    "day_oc",
    "close_vs_vwap",
    "h1_3bar",
    "max_dd_1y",
    "max_dd_60d",
    "current_dd_1y",
    "q01",
    "q05",
    "cvar05",
    "worst_1d",
    "margin_long_wow",
    "margin_short_wow",
    "op_margin",
    "op_yoy",
    "sector_short_ratio",
}
UNSIGNED_PERCENT_KEYS = {
    "day_range",
    "close_pos",
    "downside_vol_ann",
    "median_abs_ret",
    "short_to_so_max",
    "short_prev_ratio",
    "short_peak_1y",
}
INTEGER_KEYS = {
    "m5_bars",
    "long_vol",
    "short_vol",
    "short_pos_shares",
    "fund_count",
    "doc_count",
    "announcement_count",
    "earnings_count",
}
NEUTRAL_NUMERIC_KEYS = {"score", "integrated_score", "vol_ratio20", "margin_ratio", "sl_ratio"}


def fmt_price(x: object) -> str:
    if x is None or pd.isna(x):
        return "-"
    value = float(x)
    if value.is_integer():
        return f"{value:,.0f}"
    return f"{value:,.1f}"


def fmt_signed_percent(x: object, digits: int = 2) -> str:
    if x is None or pd.isna(x):
        return "-"
    value = float(x)
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.{digits}f}%"


def fmt_unsigned_percent(x: object, digits: int = 2) -> str:
    if x is None or pd.isna(x):
        return "-"
    return f"{float(x):.{digits}f}%"


def fmt_number(x: object, digits: int = 2) -> str:
    if x is None or pd.isna(x):
        return "-"
    if isinstance(x, (float, np.floating)):
        return f"{x:.{digits}f}"
    if isinstance(x, (int, np.integer)):
        return f"{int(x):,}"
    return escape(str(x))


def fmt_cell(key: str, value: object, digits: int = 2) -> str:
    if key in PRICE_KEYS:
        return fmt_price(value)
    if key in SIGNED_PERCENT_KEYS:
        return fmt_signed_percent(value, digits)
    if key in UNSIGNED_PERCENT_KEYS:
        return fmt_unsigned_percent(value, digits)
    if key in INTEGER_KEYS:
        return "-" if value is None or pd.isna(value) else f"{int(value):,}"
    return fmt_number(value, digits)


def panel(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        if ticker in df.columns.get_level_values(0):
            return df[ticker].dropna(how="all").copy()
        return pd.DataFrame()
    return df.dropna(how="all").copy()


def badge(label: str) -> str:
    classes = {
        "A": "badge-up",
        "A/B": "badge-blue",
        "B": "badge-amber",
        "C": "badge-purple",
        "D": "badge-neutral",
    }
    return f"<span class='badge {classes.get(label, 'badge-neutral')}'>{escape(label)}</span>"


def numeric_class(key: str, value: object) -> str:
    if not isinstance(value, (float, int, np.floating, np.integer)) or pd.isna(value):
        return "r num-neutral"
    if key in PRICE_KEYS or key in INTEGER_KEYS or key in NEUTRAL_NUMERIC_KEYS or key in UNSIGNED_PERCENT_KEYS:
        return "r num-neutral"
    if value > 0:
        return "r num-pos"
    if value < 0:
        return "r num-neg"
    return "r num-neutral"


def max_drawdown(close: pd.Series) -> float:
    close = close.dropna()
    if close.empty:
        return np.nan
    return float((close / close.cummax() - 1.0).min() * 100.0)


def current_drawdown(close: pd.Series) -> float:
    close = close.dropna()
    if close.empty:
        return np.nan
    return float((close.iloc[-1] / close.cummax().iloc[-1] - 1.0) * 100.0)


def cvar(returns: pd.Series, q: float = 0.05) -> float:
    returns = returns.dropna()
    if returns.empty:
        return np.nan
    cutoff = returns.quantile(q)
    tail = returns[returns <= cutoff]
    return float(tail.mean() * 100.0) if not tail.empty else np.nan


def fetch_jquants_daily(codes: list[str]) -> pd.DataFrame:
    client = JQuantsClient()
    fetcher = JQuantsFetcher(client)
    to_date = date.today()
    from_date = to_date - timedelta(days=430)
    frames = []
    for i, code in enumerate(codes, 1):
        print(f"[JQUANTS] daily {i}/{len(codes)} {code}", flush=True)
        df = fetcher.get_prices_daily(code=code, from_date=from_date, to_date=to_date)
        if not df.empty:
            df["Code"] = code
            frames.append(df)
        time.sleep(0.15)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out.dropna(subset=["Date", "Close"])


def code5(code: str) -> str:
    return f"{code}0" if len(code) == 4 else code


def latest_trading_date(prices: pd.DataFrame) -> str:
    if prices.empty:
        return date.today().isoformat()
    return str(pd.to_datetime(prices["Date"]).max().date())


def safe_float(value: Any) -> float:
    try:
        if value is None or value == "":
            return np.nan
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def safe_pct_change(new: Any, old: Any) -> float:
    new_f = safe_float(new)
    old_f = safe_float(old)
    if pd.isna(new_f) or pd.isna(old_f) or old_f == 0:
        return np.nan
    return (new_f / old_f - 1.0) * 100.0


def latest_per_code(df: pd.DataFrame, code_col: str, date_col: str) -> pd.DataFrame:
    if df.empty or code_col not in df.columns or date_col not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out.dropna(subset=[date_col])
    out["_code4"] = out[code_col].astype(str).str[:4]
    return out.sort_values(date_col).groupby("_code4", as_index=False).tail(1)


def fetch_margin_interest(codes: list[str]) -> pd.DataFrame:
    client = JQuantsClient()
    fetcher = JQuantsFetcher(client)
    frames = []
    from_date = date.today() - timedelta(days=75)
    for i, code in enumerate(codes, 1):
        print(f"[JQUANTS] margin-interest {i}/{len(codes)} {code}", flush=True)
        df = fetcher.get_margin_interest(code=code5(code), from_date=from_date, to_date=date.today())
        if not df.empty:
            df["_code4"] = df["Code"].astype(str).str[:4]
            frames.append(df)
        time.sleep(0.12)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    for col in ["ShrtVol", "LongVol", "ShrtNegVol", "LongNegVol", "ShrtStdVol", "LongStdVol"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def margin_rows(margin: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if margin.empty:
        return pd.DataFrame()
    for stock in UNIVERSE:
        d = margin[margin["_code4"] == stock.code].sort_values("Date")
        if d.empty:
            continue
        last = d.iloc[-1]
        prev = d.iloc[-2] if len(d) >= 2 else None
        long_vol = safe_float(last.get("LongVol"))
        short_vol = safe_float(last.get("ShrtVol"))
        rows.append(
            {
                "code": stock.code,
                "name": stock.name,
                "label": stock.label,
                "margin_date": str(last["Date"].date()),
                "long_vol": long_vol,
                "short_vol": short_vol,
                "margin_ratio": long_vol / short_vol if short_vol and not pd.isna(short_vol) else np.nan,
                "margin_long_wow": safe_pct_change(long_vol, prev.get("LongVol") if prev is not None else np.nan),
                "margin_short_wow": safe_pct_change(short_vol, prev.get("ShrtVol") if prev is not None else np.nan),
            }
        )
    return pd.DataFrame(rows)


def fetch_short_sale_report(codes: list[str]) -> pd.DataFrame:
    client = JQuantsClient()
    fetcher = JQuantsFetcher(client)
    frames = []
    from_date = date.today() - timedelta(days=430)
    for i, code in enumerate(codes, 1):
        print(f"[JQUANTS] short-sale-report {i}/{len(codes)} {code}", flush=True)
        df = fetcher.get_short_sale_report(code=code5(code), from_date=from_date, to_date=date.today())
        if not df.empty:
            df["_code4"] = df["Code"].astype(str).str[:4]
            frames.append(df)
        time.sleep(0.12)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    for col in ["DiscDate", "CalcDate", "PrevRptDate"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")
    for col in ["ShrtPosToSO", "ShrtPosShares", "ShrtPosUnits", "PrevRptRatio"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def short_sale_rows(shorts: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if shorts.empty:
        return pd.DataFrame()
    for stock in UNIVERSE:
        d = shorts[shorts["_code4"] == stock.code].dropna(subset=["CalcDate"]).copy()
        if d.empty:
            continue
        latest_calc = d["CalcDate"].max()
        latest = d[d["CalcDate"] == latest_calc]
        current_max = float(latest["ShrtPosToSO"].max() * 100.0)
        prev_max = float(latest["PrevRptRatio"].max() * 100.0) if "PrevRptRatio" in latest.columns else np.nan
        peak = float(d["ShrtPosToSO"].max() * 100.0)
        rows.append(
            {
                "code": stock.code,
                "name": stock.name,
                "label": stock.label,
                "short_calc_date": str(latest_calc.date()),
                "fund_count": int(latest["SSName"].nunique()) if "SSName" in latest.columns else len(latest),
                "short_pos_shares": float(latest["ShrtPosShares"].sum()),
                "short_to_so_max": current_max,
                "short_prev_ratio": prev_max,
                "short_peak_1y": peak,
                "short_status": "解消/低下" if current_max == 0 and prev_max > 0 else "残あり",
            }
        )
    return pd.DataFrame(rows)


def fetch_margin_alert_and_sector_ratio(base_date: str, codes: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    client = JQuantsClient()
    fetcher = JQuantsFetcher(client)
    master = pd.DataFrame(client.request("/equities/master").get("data", []))
    alerts = pd.DataFrame()
    ratios = pd.DataFrame()
    for offset in range(0, 8):
        d = (pd.Timestamp(base_date) - pd.Timedelta(days=offset)).date()
        try:
            alerts = fetcher.get_margin_alert(date_val=d)
            ratios = fetcher.get_short_ratio(date_val=d)
        except Exception:
            continue
        if not alerts.empty or not ratios.empty:
            break
    if not alerts.empty:
        alerts["_code4"] = alerts["Code"].astype(str).str[:4]
        alerts = alerts[alerts["_code4"].isin(codes)].copy()
        for col in ["ShrtOut", "LongOut", "SLRatio"]:
            if col in alerts.columns:
                alerts[col] = pd.to_numeric(alerts[col], errors="coerce")
    if not ratios.empty:
        ratios["Date"] = pd.to_datetime(ratios["Date"], errors="coerce")
        for col in ["SellExShortVa", "ShrtWithResVa", "ShrtNoResVa"]:
            ratios[col] = pd.to_numeric(ratios[col], errors="coerce")
        denom = ratios["SellExShortVa"] + ratios["ShrtWithResVa"] + ratios["ShrtNoResVa"]
        ratios["sector_short_ratio"] = (ratios["ShrtWithResVa"] + ratios["ShrtNoResVa"]) / denom * 100.0
    return alerts, ratios, master


def sector_ratio_rows(master: pd.DataFrame, ratios: pd.DataFrame) -> pd.DataFrame:
    if master.empty or ratios.empty:
        return pd.DataFrame()
    m = master[master["Code"].astype(str).str[:4].isin([s.code for s in UNIVERSE])].copy()
    m["_code4"] = m["Code"].astype(str).str[:4]
    rows = []
    for stock in UNIVERSE:
        mr = m[m["_code4"] == stock.code].tail(1)
        if mr.empty:
            continue
        s33 = str(mr.iloc[0].get("S33", ""))
        rr = ratios[ratios["S33"].astype(str) == s33].tail(1)
        rows.append(
            {
                "code": stock.code,
                "name": stock.name,
                "label": stock.label,
                "sector33": s33,
                "sector33_name": str(mr.iloc[0].get("S33Nm", "")),
                "ratio_date": str(rr.iloc[0]["Date"].date()) if not rr.empty else "-",
                "sector_short_ratio": float(rr.iloc[0].get("sector_short_ratio")) if not rr.empty else np.nan,
            }
        )
    return pd.DataFrame(rows)


def margin_alert_rows(alerts: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if alerts.empty:
        return pd.DataFrame()
    for _, r in latest_per_code(alerts, "_code4", "PubDate").iterrows():
        stock = next((s for s in UNIVERSE if s.code == r["_code4"]), None)
        if stock is None:
            continue
        rows.append(
            {
                "code": stock.code,
                "name": stock.name,
                "label": stock.label,
                "alert_pub_date": str(pd.to_datetime(r["PubDate"]).date()),
                "long_vol": safe_float(r.get("LongOut")),
                "short_vol": safe_float(r.get("ShrtOut")),
                "sl_ratio": safe_float(r.get("SLRatio")),
                "alert_class": str(r.get("TSEMrgnRegCls", "")),
            }
        )
    return pd.DataFrame(rows)


def fetch_financial_summary(codes: list[str]) -> pd.DataFrame:
    client = JQuantsClient()
    frames = []
    for i, code in enumerate(codes, 1):
        print(f"[JQUANTS] fins/summary {i}/{len(codes)} {code}", flush=True)
        try:
            rows = client.request("/fins/summary", params={"code": code5(code)}).get("data", [])
        except Exception as exc:
            print(f"[WARN] fins/summary {code} failed: {exc}", flush=True)
            rows = []
        if rows:
            frames.append(pd.DataFrame(rows))
        time.sleep(0.12)
    if frames:
        return pd.concat(frames, ignore_index=True)
    path = ROOT / "data" / "parquet" / "fins_summary.parquet"
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def financial_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df["_code4"] = df["Code"].astype(str).str[:4]
    df = df[df["_code4"].isin([s.code for s in UNIVERSE])].copy()
    if df.empty:
        return pd.DataFrame()
    df["DiscDate"] = pd.to_datetime(df["DiscDate"], errors="coerce")
    rows = []
    for stock in UNIVERSE:
        d = df[df["_code4"] == stock.code].sort_values("DiscDate")
        if d.empty:
            continue
        latest = d.iloc[-1]
        same_type = d[
            (d["CurPerType"].astype(str) == str(latest.get("CurPerType")))
            & (d["DiscDate"] < latest["DiscDate"])
        ]
        prev = same_type.iloc[-1] if not same_type.empty else None
        sales = safe_float(latest.get("Sales")) / 1e9
        op = safe_float(latest.get("OP")) / 1e9
        rows.append(
            {
                "code": stock.code,
                "name": stock.name,
                "label": stock.label,
                "disc_date": str(latest["DiscDate"].date()) if pd.notna(latest["DiscDate"]) else "-",
                "period": str(latest.get("CurPerType", "")),
                "sales_bil": sales,
                "op_bil": op,
                "op_margin": op / sales * 100.0 if sales and not pd.isna(sales) else np.nan,
                "op_yoy": safe_pct_change(latest.get("OP"), prev.get("OP") if prev is not None else np.nan),
                "fop_bil": safe_float(latest.get("FOP")) / 1e9,
            }
        )
    return pd.DataFrame(rows)


def disclosure_rows() -> pd.DataFrame:
    rows = []
    edinet_path = ROOT / "data" / "parquet" / "edinet_documents.parquet"
    ann_path = ROOT / "data" / "parquet" / "announcements.parquet"
    earn_path = ROOT / "data" / "parquet" / "earnings_disclosure.parquet"
    edinet = pd.read_parquet(edinet_path) if edinet_path.exists() else pd.DataFrame()
    ann = pd.read_parquet(ann_path) if ann_path.exists() else pd.DataFrame()
    earn = pd.read_parquet(earn_path) if earn_path.exists() else pd.DataFrame()
    for stock in UNIVERSE:
        e = edinet[edinet.get("sec_code", pd.Series(dtype=str)).astype(str).str[:4] == stock.code].copy() if not edinet.empty else pd.DataFrame()
        a = ann[ann.get("code", pd.Series(dtype=str)).astype(str).str[:4] == stock.code].copy() if not ann.empty else pd.DataFrame()
        q = earn[earn.get("code", pd.Series(dtype=str)).astype(str).str[:4] == stock.code].copy() if not earn.empty else pd.DataFrame()
        rows.append(
            {
                "code": stock.code,
                "name": stock.name,
                "label": stock.label,
                "edinet_latest": str(e["date"].max())[:10] if not e.empty and "date" in e.columns else "-",
                "doc_count": len(e),
                "announcement_latest": str(a["announcementDate"].max())[:10] if not a.empty and "announcementDate" in a.columns else "-",
                "announcement_count": len(a),
                "earnings_latest": str(q["disc_date"].max())[:10] if not q.empty and "disc_date" in q.columns else "-",
                "earnings_count": len(q),
            }
        )
    return pd.DataFrame(rows)


def macro_rows() -> pd.DataFrame:
    path = ROOT / "data" / "parquet" / "macro" / "estat_ci_index.parquet"
    if not path.exists():
        return pd.DataFrame([{"source": "e-Stat CI", "latest": "-", "value": "local file missing"}])
    df = pd.read_parquet(path)
    latest = df.tail(1)
    if latest.empty:
        return pd.DataFrame([{"source": "e-Stat CI", "latest": "-", "value": "empty"}])
    date_col = "date" if "date" in latest.columns else latest.columns[0]
    value_cols = [c for c in latest.columns if c != date_col]
    value = ", ".join(f"{c}={latest.iloc[0][c]}" for c in value_cols[:4])
    return pd.DataFrame([{"source": "e-Stat CI", "latest": str(latest.iloc[0][date_col])[:10], "value": value}])


def daily_metrics(stock: Stock, prices: pd.DataFrame) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    d = prices[prices["Code"].astype(str) == stock.code].sort_values("Date").copy()
    if len(d) < 80:
        return None, None

    close = float(d["Close"].iloc[-1])
    ma25 = d["Close"].rolling(25).mean()
    ma75 = d["Close"].rolling(75).mean()
    hi20 = float(d["High"].tail(20).max())
    vol20 = float(d["Volume"].tail(20).mean())
    vol = float(d["Volume"].iloc[-1])
    row = {
        "code": stock.code,
        "name": stock.name,
        "label": stock.label,
        "segment": stock.segment,
        "daily_date": str(d["Date"].iloc[-1].date()),
        "close": close,
        "prev_high": float(d["High"].iloc[-1]),
        "ret1": pct(close, float(d["Close"].iloc[-2])),
        "ret5": pct(close, float(d["Close"].iloc[-6])),
        "ret20": pct(close, float(d["Close"].iloc[-21])),
        "vs25": pct(close, float(ma25.iloc[-1])),
        "vs75": pct(close, float(ma75.iloc[-1])),
        "slope25_5d": pct(float(ma25.iloc[-1]), float(ma25.iloc[-6])),
        "dist20hi": pct(close, hi20),
        "vol_ratio20": vol / vol20 if vol20 else np.nan,
        "turnover_bil": close * vol / 1e9,
    }

    score = LABEL_SCORE.get(stock.label, 0.0)
    if row["vs25"] > 0:
        score += 2
    if row["slope25_5d"] > 0:
        score += 2
    if row["ret5"] > 0:
        score += 1
    if row["ret20"] > 0:
        score += 1
    if -5 <= row["dist20hi"] <= 0:
        score += 2
    elif row["dist20hi"] < -15:
        score -= 2
    if row["vol_ratio20"] >= 1.2:
        score += 1
    if row["turnover_bil"] >= 10:
        score += 1
    if row["vs25"] > 20:
        score -= 2
    elif row["vs25"] > 12:
        score -= 1

    if stock.label in {"C", "D"}:
        action = "監視のみ" if score >= 9 and row["turnover_bil"] >= 5 else "見送り"
    elif score >= 12 and row["vs25"] <= 18 and row["ret5"] >= 0:
        action = "買い候補"
    elif score >= 9:
        action = "寄り後条件付き"
    elif score >= 7:
        action = "押し目/回復待ち"
    else:
        action = "見送り"

    if action == "買い候補":
        rule = f"公式日足の前日高値{row['prev_high']:.0f}超え。寄り高は追わない"
    elif action == "寄り後条件付き":
        rule = f"公式日足の前日高値{row['prev_high']:.0f}超え確認。分足はVWAP上維持のみ参考"
    elif action == "押し目/回復待ち":
        rule = f"公式日足終値{close:.0f}奪回、または25日線上維持まで待ち"
    else:
        rule = "翌日エントリー対象外"

    returns = d["Close"].pct_change()
    neg = returns[returns < 0]
    risk = {
        "code": stock.code,
        "name": stock.name,
        "label": stock.label,
        "segment": stock.segment,
        "max_dd_1y": max_drawdown(d["Close"]),
        "max_dd_60d": max_drawdown(d["Close"].tail(60)),
        "current_dd_1y": current_drawdown(d["Close"]),
        "q01": float(returns.quantile(0.01) * 100.0),
        "q05": float(returns.quantile(0.05) * 100.0),
        "cvar05": cvar(returns, 0.05),
        "downside_vol_ann": float(neg.std() * np.sqrt(245) * 100.0) if not neg.empty else np.nan,
        "worst_1d": float(returns.min() * 100.0),
        "median_abs_ret": float(returns.abs().median() * 100.0),
    }
    row["score"] = score
    row["action"] = action
    row["tail_risk"] = "高" if risk["cvar05"] <= -6 or risk["max_dd_60d"] <= -25 else ("中" if risk["cvar05"] <= -4 or risk["max_dd_60d"] <= -15 else "低")
    row["rule"] = rule
    return row, risk


def intraday_rows() -> pd.DataFrame:
    tickers = [f"{s.code}.T" for s in UNIVERSE]
    min5 = yf.download(tickers, period="5d", interval="5m", group_by="ticker", auto_adjust=False, progress=False, threads=True)
    rows = []
    for stock in UNIVERSE:
        ticker = f"{stock.code}.T"
        m5 = panel(min5, ticker).dropna(subset=["Close"])
        if m5.empty:
            rows.append({"code": stock.code, "name": stock.name, "label": stock.label, "m5_date": "取得不足"})
            continue
        idx = pd.Series(m5.index)
        dates = idx.dt.tz_convert("Asia/Tokyo").dt.date if getattr(m5.index, "tz", None) is not None else idx.dt.date
        last_day = dates.max()
        day = m5[dates.values == last_day].copy()
        op = float(day["Open"].dropna().iloc[0])
        cl = float(day["Close"].dropna().iloc[-1])
        high = float(day["High"].max())
        low = float(day["Low"].min())
        vwap = float((day["Close"] * day["Volume"]).sum() / day["Volume"].sum()) if day["Volume"].sum() else np.nan
        rows.append(
            {
                "code": stock.code,
                "name": stock.name,
                "label": stock.label,
                "m5_date": str(last_day),
                "m5_bars": len(day),
                "m5_last": cl,
                "day_oc": pct(cl, op),
                "day_range": pct(high, low),
                "close_pos": (cl - low) / (high - low) * 100.0 if high != low else np.nan,
                "close_vs_vwap": pct(cl, vwap),
                "vwap": vwap,
            }
        )
    return pd.DataFrame(rows)


def overseas_rows() -> pd.DataFrame:
    daily = yf.download(OVERSEAS, period="3mo", interval="1d", group_by="ticker", auto_adjust=False, progress=False, threads=True)
    hour1 = yf.download(OVERSEAS, period="5d", interval="1h", group_by="ticker", auto_adjust=False, progress=False, threads=True)
    rows = []
    for ticker in OVERSEAS:
        d = panel(daily, ticker).dropna(subset=["Close"])
        h = panel(hour1, ticker).dropna(subset=["Close"])
        if len(d) < 21:
            continue
        rows.append(
            {
                "ticker": ticker,
                "date": str(d.index[-1].date()),
                "close": float(d["Close"].iloc[-1]),
                "ret1": pct(float(d["Close"].iloc[-1]), float(d["Close"].iloc[-2])),
                "ret5": pct(float(d["Close"].iloc[-1]), float(d["Close"].iloc[-6])),
                "ret20": pct(float(d["Close"].iloc[-1]), float(d["Close"].iloc[-21])),
                "h1_3bar": pct(float(h["Close"].iloc[-1]), float(h["Close"].iloc[-4])) if len(h) > 4 else np.nan,
            }
        )
    return pd.DataFrame(rows)


def table(df: pd.DataFrame, columns: list[tuple[str, str, int | None]], min_width: int = 1100) -> str:
    if df.empty:
        return "<p class='note'>該当なし</p>"
    head = "".join(f"<th class='{'r' if digits is not None else ''}'>{escape(label)}</th>" for _, label, digits in columns)
    body = []
    for _, row in df.iterrows():
        cells = []
        for key, _, digits in columns:
            value = row.get(key)
            if key == "label":
                cells.append(f"<td>{badge(str(value))}</td>")
            elif digits is None:
                cls = " class='wrap'" if key in {"rule", "segment"} else ""
                cells.append(f"<td{cls}>{escape(str(value))}</td>")
            else:
                cells.append(f"<td class='{numeric_class(key, value)}'>{fmt_cell(key, value, digits)}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table style='min-width:{min_width}px'><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def picks_text(daily: pd.DataFrame) -> tuple[str, str, str]:
    if daily.empty:
        return "-", "-", "-"
    conditional = daily[daily["action"].isin(["買い候補", "寄り後条件付き"])].head(6)
    watch = daily[daily["action"] == "押し目/回復待ち"].head(4)
    avoid = daily[daily["action"] == "見送り"].head(6)

    def join_codes(df: pd.DataFrame) -> str:
        if df.empty:
            return "-"
        return " / ".join(f"{r.code} {r.name}" for r in df.itertuples())

    return join_codes(conditional), join_codes(watch), join_codes(avoid)


def integrated_rows(
    daily: pd.DataFrame,
    risks: pd.DataFrame,
    margin: pd.DataFrame,
    shorts: pd.DataFrame,
    finance: pd.DataFrame,
) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame()
    out = daily.copy()
    for df in [risks, margin, shorts, finance]:
        if df.empty:
            continue
        keep = [c for c in df.columns if c not in {"name", "label", "segment"}]
        out = out.merge(df[keep], on="code", how="left")
    out["integrated_score"] = out["score"].astype(float)
    out.loc[out.get("margin_long_wow", pd.Series(np.nan, index=out.index)) > 0, "integrated_score"] += 0.5
    out.loc[out.get("margin_short_wow", pd.Series(np.nan, index=out.index)) > 20, "integrated_score"] -= 0.5
    out.loc[out.get("op_yoy", pd.Series(np.nan, index=out.index)) > 0, "integrated_score"] += 0.5
    out.loc[out.get("cvar05", pd.Series(np.nan, index=out.index)) <= -6, "integrated_score"] -= 1.0
    return out.sort_values(["integrated_score", "turnover_bil"], ascending=[False, False])


def candidate_cards(integrated: pd.DataFrame) -> str:
    if integrated.empty:
        return "<p class='note'>候補なし</p>"
    cards = []
    for row in integrated[integrated["action"].isin(["買い候補", "寄り後条件付き"])].head(6).itertuples():
        cards.append(
            "<div class='candidate-card'>"
            f"<div class='candidate-head'><b>{escape(row.code)} {escape(row.name)}</b>{badge(row.label)}</div>"
            f"<div class='candidate-meta'>{escape(row.segment)} / {escape(row.action)}</div>"
            "<div class='candidate-grid'>"
            f"<div><span>公式終値</span><b>{fmt_price(row.close)}</b></div>"
            f"<div><span>5日</span><b class='{numeric_class('ret5', row.ret5).replace('r ', '')}'>{fmt_signed_percent(row.ret5)}</b></div>"
            f"<div><span>25日線比</span><b class='{numeric_class('vs25', row.vs25).replace('r ', '')}'>{fmt_signed_percent(row.vs25)}</b></div>"
            f"<div><span>CVaR5%</span><b class='{numeric_class('cvar05', getattr(row, 'cvar05', np.nan)).replace('r ', '')}'>{fmt_signed_percent(getattr(row, 'cvar05', np.nan))}</b></div>"
            f"<div><span>信用買残WoW</span><b class='{numeric_class('margin_long_wow', getattr(row, 'margin_long_wow', np.nan)).replace('r ', '')}'>{fmt_signed_percent(getattr(row, 'margin_long_wow', np.nan))}</b></div>"
            f"<div><span>空売り 現/前</span><b>{fmt_unsigned_percent(getattr(row, 'short_to_so_max', np.nan))} / {fmt_unsigned_percent(getattr(row, 'short_prev_ratio', np.nan))}</b></div>"
            f"<div><span>営業益YoY</span><b class='{numeric_class('op_yoy', getattr(row, 'op_yoy', np.nan)).replace('r ', '')}'>{fmt_signed_percent(getattr(row, 'op_yoy', np.nan))}</b></div>"
            f"<div><span>統合点</span><b>{fmt_number(getattr(row, 'integrated_score', np.nan), 1)}</b></div>"
            "</div>"
            f"<div class='candidate-rule'>{escape(row.rule)}</div>"
            "</div>"
        )
    return "<div class='candidate-cards'>" + "".join(cards) + "</div>"


def coverage_rows(
    daily: pd.DataFrame,
    intraday: pd.DataFrame,
    overseas: pd.DataFrame,
    risks: pd.DataFrame,
    margin: pd.DataFrame,
    shorts: pd.DataFrame,
    alerts: pd.DataFrame,
    sector: pd.DataFrame,
    finance: pd.DataFrame,
    disclosure: pd.DataFrame,
    macro: pd.DataFrame,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"dataset": "J-Quants 公式日足", "coverage": f"{len(daily)}/29", "latest": daily["daily_date"].max() if not daily.empty else "-", "usage": "日足OHLCV・移動平均・DD・左尾"},
            {"dataset": "yfinance 5分足", "coverage": f"{len(intraday)}/29", "latest": intraday["m5_date"].max() if not intraday.empty else "-", "usage": "翌日寄り後のVWAP/寄り天確認。日足代替には使わない"},
            {"dataset": "yfinance 海外/先物/為替", "coverage": f"{len(overseas)}/{len(OVERSEAS)}", "latest": overseas["date"].max() if not overseas.empty else "-", "usage": "NVDA/SOX/AVGO/MU/TSM/NASDAQ/先物/為替の地合い"},
            {"dataset": "J-Quants 信用残", "coverage": f"{len(margin)}/29", "latest": margin["margin_date"].max() if not margin.empty else "-", "usage": "信用買残・売残・需給悪化確認"},
            {"dataset": "J-Quants 空売り残高報告", "coverage": f"{len(shorts)}/29", "latest": shorts["short_calc_date"].max() if not shorts.empty else "-", "usage": "大口空売り残の確認"},
            {"dataset": "J-Quants 日々公表", "coverage": f"{len(alerts)}/29", "latest": alerts["alert_pub_date"].max() if not alerts.empty else "-", "usage": "該当銘柄だけ表示"},
            {"dataset": "J-Quants 業種別空売り比率", "coverage": f"{len(sector)}/29", "latest": sector["ratio_date"].max() if not sector.empty else "-", "usage": "33業種単位の空売り圧力"},
            {"dataset": "J-Quants 財務サマリ parquet", "coverage": f"{len(finance)}/29", "latest": finance["disc_date"].max() if not finance.empty else "-", "usage": "売上・営業益・営業益率・会社予想"},
            {"dataset": "EDINET/適時開示ローカル", "coverage": f"{int((disclosure['doc_count'] > 0).sum())}/29" if not disclosure.empty else "0/29", "latest": disclosure["edinet_latest"].max() if not disclosure.empty else "-", "usage": "ローカル保有分の開示確認"},
            {"dataset": "e-Stat CI ローカル", "coverage": f"{len(macro)}/1", "latest": macro["latest"].max() if not macro.empty else "-", "usage": "マクロ景気一致指数の状態確認"},
        ]
    )


def build_html(
    daily: pd.DataFrame,
    integrated: pd.DataFrame,
    risks: pd.DataFrame,
    intraday: pd.DataFrame,
    overseas: pd.DataFrame,
    margin: pd.DataFrame,
    shorts: pd.DataFrame,
    alerts: pd.DataFrame,
    sector: pd.DataFrame,
    finance: pd.DataFrame,
    disclosure: pd.DataFrame,
    macro: pd.DataFrame,
    missing: list[str],
) -> str:
    conditional, watch, avoid = picks_text(integrated if not integrated.empty else daily)
    cards_html = candidate_cards(integrated)
    coverage = coverage_rows(daily, intraday, overseas, risks, margin, shorts, alerts, sector, finance, disclosure, macro)
    return f"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AIインフラ順張り 翌日エントリー・リスク測定</title>
<style>
:root {{
  --bg: #09090b;
  --card: #18181b;
  --card-border: #27272a;
  --text: #fafafa;
  --text-muted: #a1a1aa;
  --up: #34d399;
  --up-bg: rgba(52,211,153,0.1);
  --down: #fb7185;
  --down-bg: rgba(251,113,133,0.1);
  --amber: #fbbf24;
  --amber-bg: rgba(251,191,36,0.15);
  --blue: #60a5fa;
  --blue-bg: rgba(96,165,250,0.1);
  --purple: #a78bfa;
  --purple-bg: rgba(167,139,250,0.1);
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
  background: var(--bg);
  color: var(--text);
  font-family: "Helvetica Neue", -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans JP", sans-serif;
  font-feature-settings: "tnum" 1, "lnum" 1;
  -webkit-font-smoothing: antialiased;
  line-height: 1.6;
  padding: 24px;
  max-width: 1760px;
  margin: 0 auto;
}}
header {{ margin-bottom: 20px; }}
h1 {{ font-size: 1.5rem; font-weight: 700; margin-bottom: 4px; }}
h2 {{ font-size: 1.1rem; font-weight: 700; margin-bottom: 12px; }}
.subtitle {{ color: var(--text-muted); font-size: 0.875rem; }}
.section {{ background: var(--card); border: 1px solid var(--card-border); border-radius: 12px; padding: 24px; margin-bottom: 20px; overflow-x: auto; }}
.note {{ color: var(--text-muted); font-size: 0.875rem; line-height: 1.7; }}
.summary-list {{ display: grid; gap: 10px; font-size: 1rem; }}
.summary-list b {{ color: var(--text); }}
.grid-4 {{ display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 16px; margin-bottom: 20px; }}
.stat-card {{ background: rgba(255,255,255,0.02); border: 1px solid var(--card-border); border-radius: 8px; padding: 16px; text-align: center; }}
.stat-card .label {{ color: var(--text-muted); font-size: 0.75rem; margin-bottom: 4px; }}
.stat-card .value {{ font-size: 1.5rem; font-weight: 700; font-variant-numeric: tabular-nums; }}
.candidate-cards {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; margin-top: 14px; }}
.candidate-card {{ background: rgba(255,255,255,0.02); border: 1px solid var(--card-border); border-radius: 8px; padding: 16px; }}
.candidate-head {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; font-size: 1.05rem; }}
.candidate-meta {{ color: var(--text-muted); font-size: 0.85rem; margin-top: 4px; }}
.candidate-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 14px 0; }}
.candidate-grid div {{ text-align: right; }}
.candidate-grid span {{ display: block; color: var(--text-muted); font-size: 0.78rem; margin-bottom: 2px; text-align: right; }}
.candidate-grid b {{ display: block; text-align: right; font-size: 1.05rem; font-variant-numeric: tabular-nums; }}
.candidate-rule {{ color: var(--text-muted); font-size: 0.9rem; line-height: 1.6; }}
table {{ width: max-content; border-collapse: collapse; font-size: 0.95rem; margin: 14px 0; table-layout: auto; }}
th {{ text-align: left; padding: 10px 14px; background: rgba(255,255,255,0.03); color: var(--text-muted); font-weight: 600; border-bottom: 1px solid var(--card-border); white-space: nowrap; }}
td {{ padding: 10px 14px; border-bottom: 1px solid rgba(255,255,255,0.05); vertical-align: top; white-space: nowrap; }}
td.wrap {{ white-space: normal; min-width: 260px; max-width: 420px; }}
td.r, th.r {{ text-align: right; font-variant-numeric: tabular-nums; }}
tr:hover td {{ background: rgba(255,255,255,0.02); }}
.num-pos {{ color: var(--up); font-weight: 600; }}
.num-neg {{ color: var(--down); font-weight: 600; }}
.num-neutral {{ color: var(--text-muted); }}
.badge {{ display: inline-block; padding: 2px 10px; border-radius: 9999px; font-size: 0.75rem; font-weight: 600; white-space: nowrap; }}
.badge-up {{ background: var(--up-bg); color: var(--up); border: 1px solid rgba(52,211,153,0.3); }}
.badge-down {{ background: var(--down-bg); color: var(--down); border: 1px solid rgba(251,113,133,0.3); }}
.badge-amber {{ background: var(--amber-bg); color: var(--amber); border: 1px solid rgba(251,191,36,0.3); }}
.badge-blue {{ background: var(--blue-bg); color: var(--blue); border: 1px solid rgba(96,165,250,0.3); }}
.badge-purple {{ background: var(--purple-bg); color: var(--purple); border: 1px solid rgba(167,139,250,0.3); }}
.badge-neutral {{ background: rgba(255,255,255,0.05); color: var(--text-muted); border: 1px solid var(--card-border); }}
.alert-box {{ border-radius: 8px; padding: 16px; margin: 16px 0; font-size: 0.875rem; line-height: 1.7; }}
.alert-warning {{ background: var(--amber-bg); border: 1px solid rgba(251,191,36,0.3); color: var(--amber); }}
footer {{ text-align: center; color: var(--text-muted); font-size: 0.7rem; margin-top: 40px; padding: 16px 0; border-top: 1px solid var(--card-border); }}
@media (max-width: 1200px) {{ .candidate-cards {{ grid-template-columns: 1fr 1fr; }} }}
@media (max-width: 768px) {{ body {{ padding: 16px; }} .grid-4,.candidate-cards {{ grid-template-columns: 1fr; }} .section {{ padding: 16px; }} }}
</style>
</head>
<body>
<header>
<h1>AIインフラ順張り 翌日エントリー・リスク測定</h1>
<div class="subtitle">生成: {datetime.now().strftime("%Y-%m-%d %H:%M")} / 公式日足: J-Quants / 分足・海外: yfinance / 対象: 29銘柄</div>
</header>
<div class="grid-4">
  <div class="stat-card"><div class="label">日足取得</div><div class="value">{len(daily)}/29</div></div>
  <div class="stat-card"><div class="label">需給取得</div><div class="value">{len(margin)}/29</div></div>
  <div class="stat-card"><div class="label">買い候補</div><div class="value">{int((daily["action"] == "買い候補").sum()) if not daily.empty else 0}</div></div>
  <div class="stat-card"><div class="label">財務取得</div><div class="value">{len(finance)}/29</div></div>
</div>
<main>
<section class="section">
<h2>結論</h2>
  <div class="summary-list">
  <div><b>優先:</b> {escape(conditional)}</div>
  <div><b>押し目待ち:</b> {escape(watch)}</div>
  <div><b>見送り上位:</b> {escape(avoid)}</div>
  <div class="note">これは「取得可能データ統合版」。月曜は成行買いではなく、公式日足の前日高値ブレイク、寄り後VWAP上維持、過大ギャップ回避を同時に見る。</div>
</div>
{cards_html}
</section>
<section class="section">
<h2>データ取得範囲</h2>
{table(coverage, [("dataset","データ",None),("coverage","対象カバー",None),("latest","最新日",None),("usage","用途",None)], 1350)}
</section>
<section class="section">
<h2>海外地合い</h2>
{table(overseas, [("ticker","指標",None),("date","日付",None),("close","終値",2),("ret1","1日%",2),("ret5","5日%",2),("ret20","20日%",2),("h1_3bar","1h 3本%",2)], 900)}
</section>
<section class="section">
<h2>統合ランキング</h2>
{table(integrated, [("code","コード",None),("name","銘柄",None),("label","根拠",None),("segment","分類",None),("daily_date","日足基準日",None),("close","公式終値",0),("ret5","5日",2),("ret20","20日",2),("vs25","25日線比",2),("dist20hi","20日高値比",2),("vol_ratio20","出来高20日比",2),("cvar05","CVaR5",2),("margin_long_wow","信用買残WoW",2),("margin_short_wow","信用売残WoW",2),("short_to_so_max","空売り現",2),("short_prev_ratio","空売り前回",2),("short_peak_1y","空売り1年ピーク",2),("op_yoy","営業益YoY",2),("integrated_score","統合点",1),("action","判定",None),("tail_risk","左尾",None),("rule","翌日条件",None)], 2500)}
</section>
<section class="section">
<h2>J-Quants日足ランキング</h2>
{table(daily, [("code","コード",None),("name","銘柄",None),("label","根拠",None),("segment","分類",None),("daily_date","日足基準日",None),("close","公式終値",0),("ret5","5日",2),("ret20","20日",2),("vs25","25日線比",2),("dist20hi","20日高値比",2),("vol_ratio20","出来高20日比",2),("score","日足点",1),("action","判定",None),("tail_risk","左尾",None),("rule","翌日条件",None)], 1900)}
</section>
<section class="section">
<h2>J-Quants需給</h2>
{table(margin, [("code","コード",None),("name","銘柄",None),("label","根拠",None),("margin_date","信用基準日",None),("long_vol","信用買残",0),("short_vol","信用売残",0),("margin_ratio","買残/売残",2),("margin_long_wow","買残WoW",2),("margin_short_wow","売残WoW",2)], 1250)}
{table(shorts, [("code","コード",None),("name","銘柄",None),("label","根拠",None),("short_calc_date","空売り計算日",None),("fund_count","報告者数",0),("short_pos_shares","空売り株数",0),("short_to_so_max","現対発行比",2),("short_prev_ratio","前回対発行比",2),("short_peak_1y","1年ピーク",2),("short_status","状態",None)], 1300)}
{table(alerts, [("code","コード",None),("name","銘柄",None),("label","根拠",None),("alert_pub_date","公表日",None),("long_vol","買残",0),("short_vol","売残",0),("sl_ratio","売買倍率",2),("alert_class","規制区分",None)], 1050)}
{table(sector, [("code","コード",None),("name","銘柄",None),("label","根拠",None),("sector33","33業種",None),("sector33_name","業種名",None),("ratio_date","基準日",None),("sector_short_ratio","業種空売り比率",2)], 1100)}
</section>
<section class="section">
<h2>財務・開示・マクロ</h2>
{table(finance, [("code","コード",None),("name","銘柄",None),("label","根拠",None),("disc_date","開示日",None),("period","期",None),("sales_bil","売上 十億円",1),("op_bil","営業益 十億円",1),("op_margin","営業益率",2),("op_yoy","営業益YoY",2),("fop_bil","会社予想営業益 十億円",1)], 1450)}
{table(disclosure, [("code","コード",None),("name","銘柄",None),("label","根拠",None),("edinet_latest","EDINET最新",None),("doc_count","EDINET件数",0),("announcement_latest","決算予定",None),("announcement_count","予定件数",0),("earnings_latest","適時開示最新",None),("earnings_count","適時開示件数",0)], 1300)}
{table(macro, [("source","データ",None),("latest","最新",None),("value","値",None)], 900)}
</section>
<section class="section">
<h2>yfinance 5分足 日中挙動</h2>
{table(intraday, [("code","コード",None),("name","銘柄",None),("label","根拠",None),("m5_date","分足基準日",None),("m5_bars","本数",0),("m5_last","最終5分足価格",0),("day_oc","始値比",2),("day_range","日中レンジ",2),("close_pos","レンジ位置",2),("close_vs_vwap","VWAP比",2),("vwap","VWAP",0)], 1300)}
</section>
<section class="section">
<h2>DD・左尾リスク</h2>
<p class="note">q01/q05は1%・5%分位、CVaR5%は悪い5%だけの平均損失。</p>
{table(risks, [("code","コード",None),("name","銘柄",None),("label","根拠",None),("segment","分類",None),("max_dd_1y","最大DD1年",2),("max_dd_60d","最大DD60日",2),("current_dd_1y","現在DD",2),("q01","1%分位",2),("q05","5%分位",2),("cvar05","CVaR5",2),("downside_vol_ann","下方ボラ年率",2),("worst_1d","最悪1日",2),("median_abs_ret","日次中央値幅",2)], 1600)}
</section>
<section class="section">
<h2>取得不足</h2>
<p class="note">{escape(", ".join(missing) if missing else "なし")}</p>
</section>
</main>
<footer>Generated by generate_yf_entry_risk_report.py</footer>
</body>
</html>
"""


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    codes = [s.code for s in UNIVERSE]
    jq_daily = fetch_jquants_daily(codes)
    rows = []
    risks = []
    missing = []
    for stock in UNIVERSE:
        row, risk = daily_metrics(stock, jq_daily)
        if row is None or risk is None:
            missing.append(f"{stock.code} {stock.name}")
            continue
        rows.append(row)
        risks.append(risk)
    daily = pd.DataFrame(rows)
    if not daily.empty:
        daily = daily.sort_values(["score", "turnover_bil"], ascending=[False, False])
    risk_df = pd.DataFrame(risks)
    if not risk_df.empty:
        risk_df = risk_df.sort_values("cvar05")
    base_date = latest_trading_date(jq_daily)
    margin_raw = fetch_margin_interest(codes)
    margin = margin_rows(margin_raw)
    shorts_raw = fetch_short_sale_report(codes)
    shorts = short_sale_rows(shorts_raw)
    alerts_raw, sector_ratio_raw, master = fetch_margin_alert_and_sector_ratio(base_date, codes)
    alerts = margin_alert_rows(alerts_raw)
    sector = sector_ratio_rows(master, sector_ratio_raw)
    finance_raw = fetch_financial_summary(codes)
    finance = financial_rows(finance_raw)
    disclosure = disclosure_rows()
    macro = macro_rows()
    integrated = integrated_rows(daily, risk_df, margin, shorts, finance)
    intraday = intraday_rows()
    overseas = overseas_rows()
    html = build_html(
        daily=daily,
        integrated=integrated,
        risks=risk_df,
        intraday=intraday,
        overseas=overseas,
        margin=margin,
        shorts=shorts,
        alerts=alerts,
        sector=sector,
        finance=finance,
        disclosure=disclosure,
        macro=macro,
        missing=missing,
    )
    OUT_FILE.write_text(html, encoding="utf-8")
    print(f"WROTE {OUT_FILE}")
    print(
        " ".join(
            [
                f"jquants_daily={len(daily)}",
                f"missing={len(missing)}",
                f"margin={len(margin)}",
                f"shorts={len(shorts)}",
                f"alerts={len(alerts)}",
                f"sector={len(sector)}",
                f"finance={len(finance)}",
                f"disclosure={len(disclosure)}",
                f"intraday={len(intraday)}",
                f"overseas={len(overseas)}",
                f"risks={len(risk_df)}",
            ]
        )
    )
    if missing:
        print("MISSING " + ", ".join(missing))


if __name__ == "__main__":
    main()
