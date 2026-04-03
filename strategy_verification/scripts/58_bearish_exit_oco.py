#!/usr/bin/env python3
"""
58_bearish_exit_oco.py
大陰線(-5%) Exit改善: SMA20指値OCO vs 現行（翌寄付）比較

比較パターン:
  現行:  Close>SMA20 → 翌Open / Day3 -3% → Day4Open
  A:     High≥前日SMA20 → 前日SMA20指値 / SLなし
  B-3:   High≥前日SMA20 → 前日SMA20指値 / -3%逆指値
  B-5:   同上 / -5%逆指値
  B-7:   同上 / -7%逆指値
  C:     High≥前日SMA20 → 前日SMA20指値 / Day3 -3%→Day4Open（現行SL）

データ: Core+Large ②株価≤15,000（VIフィルタなし、前回PF 5.69の条件）
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from common_cfg.paths import PARQUET_DIR

SCREENING_DIR = PARQUET_DIR / "screening"
VI_CSV_1 = ROOT.parent / "dash_plotly" / "data" / "csv" / "日経平均ボラティリティー・インデックス 過去データ.csv"
VI_CSV_2 = ROOT.parent / "dash_plotly" / "data" / "csv" / "日経平均ボラティリティー・インデックス 過去データ (1).csv"
UNIVERSE_PATH = PARQUET_DIR / "universe.parquet"
OUTPUT_HTML = ROOT / "docs" / "bearish_exit_oco.html"

LOOKBACK_YEARS = 10
MAX_HOLD = 30


def load_data() -> pd.DataFrame:
    ps = pd.read_parquet(SCREENING_DIR / "prices_max_1d_core_large.parquet")
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)
    cutoff = ps["date"].max() - pd.DateOffset(years=LOOKBACK_YEARS)
    ps = ps[ps["date"] >= cutoff - pd.Timedelta(days=200)].copy()

    g = ps.groupby("ticker")
    ps["body_pct"] = (ps["Close"] - ps["Open"]) / ps["Open"] * 100
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    # 前日SMA20（指値注文で使う値: 当日寄付前に計算可能）
    ps["prev_sma20"] = g["sma20"].shift(1)

    frames = []
    for p in [VI_CSV_1, VI_CSV_2]:
        if p.exists():
            df = pd.read_csv(p)
            df.columns = [c.strip() for c in df.columns]
            df = df.rename(columns={"日付": "date", "終値": "vi_close"})
            df["date"] = pd.to_datetime(df["date"])
            df["vi_close"] = pd.to_numeric(df["vi_close"], errors="coerce")
            frames.append(df[["date", "vi_close"]].dropna())
    vi = pd.concat(frames).drop_duplicates(subset="date").sort_values("date")
    ps = ps.merge(vi[["date", "vi_close"]], on="date", how="left")
    ps["vi_close"] = ps.groupby("ticker")["vi_close"].ffill()
    ps = ps.dropna(subset=["sma20", "vi_close"]).copy()
    ps = ps[ps["date"] >= cutoff].copy()
    return ps


def get_signals(ps: pd.DataFrame) -> pd.DataFrame:
    """②条件: 大陰線(-5%) + 株価≤15,000"""
    mask = (ps["body_pct"] <= -5) & (ps["Close"] <= 15000)
    sigs = ps[mask].sort_values(["ticker", "date"]).copy()

    # 連続除外（5日）
    filtered = []
    last: dict[str, pd.Timestamp] = {}
    for _, r in sigs.iterrows():
        tk = r["ticker"]
        if tk in last and (r["date"] - last[tk]).days < 5:
            continue
        last[tk] = r["date"]
        filtered.append(r)
    return pd.DataFrame(filtered) if filtered else pd.DataFrame()


def backtest_baseline(
    ps: pd.DataFrame, signals: pd.DataFrame,
) -> pd.DataFrame:
    """現行: Close>SMA20→翌Open / Day3 -3%→Day4Open"""
    ticker_data = {tk: g.sort_values("date").reset_index(drop=True)
                   for tk, g in ps.groupby("ticker")}
    trades = []
    for _, sig in signals.iterrows():
        tk_df = ticker_data.get(sig["ticker"])
        if tk_df is None:
            continue
        future = tk_df[tk_df["date"] > sig["date"]]
        if len(future) < 2:
            continue
        ep = float(future.iloc[0]["Open"])
        if pd.isna(ep) or ep <= 0:
            continue
        e_date = future.iloc[0]["date"]
        hl = min(len(future) - 1, MAX_HOLD)

        xp, xd, xt, xday = None, None, None, 0
        for i in range(hl):
            row = future.iloc[i]
            cc = float(row["Close"])
            if i > 0:
                ret_i = (cc / ep - 1) * 100
                if i == 2 and ret_i < -3:
                    if i + 1 < len(future):
                        nr = future.iloc[i + 1]
                        xp, xd, xt, xday = float(nr["Open"]), nr["date"], "SL_D3", i + 2
                        break
                if cc > float(row["sma20"]) and i + 1 < len(future):
                    nr = future.iloc[i + 1]
                    xp, xd, xt, xday = float(nr["Open"]), nr["date"], "SMA20_翌Open", i + 2
                    break
            if i >= hl - 1:
                if i + 1 < len(future):
                    xp, xd = float(future.iloc[i + 1]["Open"]), future.iloc[i + 1]["date"]
                else:
                    xp, xd = cc, row["date"]
                xt, xday = "MAX_HOLD", i + 2
                break
        if xp is None:
            continue
        ret = round((xp / ep - 1) * 100, 3)
        trades.append(_trade_row(sig, e_date, ep, xp, xd, xt, xday, ret))
    return pd.DataFrame(trades)


def backtest_oco(
    ps: pd.DataFrame, signals: pd.DataFrame,
    sl_pct: float | None = None,
    day3_cut: bool = False,
) -> pd.DataFrame:
    """OCO: High≥前日SMA20→指値決済 / 逆指値SL or Day3SL"""
    ticker_data = {tk: g.sort_values("date").reset_index(drop=True)
                   for tk, g in ps.groupby("ticker")}
    trades = []
    for _, sig in signals.iterrows():
        tk_df = ticker_data.get(sig["ticker"])
        if tk_df is None:
            continue
        future = tk_df[tk_df["date"] > sig["date"]]
        if len(future) < 2:
            continue
        ep = float(future.iloc[0]["Open"])
        if pd.isna(ep) or ep <= 0:
            continue
        e_date = future.iloc[0]["date"]
        hl = min(len(future) - 1, MAX_HOLD)

        sl_price = ep * (1 + sl_pct / 100) if sl_pct is not None else None

        xp, xd, xt, xday = None, None, None, 0
        for i in range(hl):
            row = future.iloc[i]
            hi = float(row["High"])
            lo = float(row["Low"])
            cc = float(row["Close"])
            prev_sma = float(row["prev_sma20"]) if not pd.isna(row["prev_sma20"]) else None

            if i > 0:
                # Day3損切り（現行SLロジック）
                if day3_cut and i == 2:
                    ret_i = (cc / ep - 1) * 100
                    if ret_i < -3:
                        if i + 1 < len(future):
                            nr = future.iloc[i + 1]
                            xp, xd, xt, xday = float(nr["Open"]), nr["date"], "SL_D3", i + 2
                            break

                # 逆指値SL: Low ≤ SL価格
                if sl_price is not None and lo <= sl_price:
                    # SMA20指値と逆指値が同日ヒットの場合:
                    # 寄付がSL以下なら寄付でSL約定（GD）
                    # そうでなければザラ場中にSLヒット
                    op = float(row["Open"])
                    if op <= sl_price:
                        xp, xd, xt, xday = op, row["date"], f"SL_{abs(sl_pct):.0f}%", i + 1
                    else:
                        # 同日にSMA20指値もヒットする可能性
                        if prev_sma is not None and hi >= prev_sma and prev_sma > sl_price:
                            # どちらが先にヒットしたか不明 → 保守的にSLとする
                            # ただしSMA20が先の可能性もある。Lowが先かHighが先か不明
                            # → SMA20指値の方が価格が高いので、利確優先でSMA20とする
                            xp, xd, xt, xday = prev_sma, row["date"], "SMA20_指値", i + 1
                        else:
                            xp, xd, xt, xday = sl_price, row["date"], f"SL_{abs(sl_pct):.0f}%", i + 1
                    break

                # SMA20指値ヒット: High ≥ 前日SMA20
                if prev_sma is not None and hi >= prev_sma:
                    # 寄付がSMA20以上なら寄付で約定
                    op = float(row["Open"])
                    if op >= prev_sma:
                        xp = op  # 指値以上で寄り付いたら寄付値で約定
                    else:
                        xp = prev_sma  # ザラ場中にタッチ
                    xd, xt, xday = row["date"], "SMA20_指値", i + 1
                    break

            if i >= hl - 1:
                if i + 1 < len(future):
                    xp, xd = float(future.iloc[i + 1]["Open"]), future.iloc[i + 1]["date"]
                else:
                    xp, xd = cc, row["date"]
                xt, xday = "MAX_HOLD", i + 2
                break
        if xp is None:
            continue
        ret = round((xp / ep - 1) * 100, 3)
        trades.append(_trade_row(sig, e_date, ep, xp, xd, xt, xday, ret))
    return pd.DataFrame(trades)


def _trade_row(sig, e_date, ep, xp, xd, xt, xday, ret):
    return {
        "ticker": sig["ticker"],
        "signal_date": sig["date"],
        "entry_date": e_date,
        "exit_date": xd,
        "entry_price": round(ep, 1),
        "exit_price": round(xp, 1),
        "ret_pct": ret,
        "pnl_yen": int((xp - ep) * 100),
        "exit_type": xt,
        "hold_days": int(xday),
        "body_pct": round(float(sig["body_pct"]), 2),
    }


def calc_stats(df: pd.DataFrame) -> dict:
    n = len(df)
    if n == 0:
        return {"n": 0}
    wins = int((df["ret_pct"] > 0).sum())
    wr = round(wins / n * 100, 1)
    avg = round(df["ret_pct"].mean(), 3)
    med = round(df["ret_pct"].median(), 3)
    gw = df[df["ret_pct"] > 0]["ret_pct"].sum()
    gl = abs(df[df["ret_pct"] < 0]["ret_pct"].sum())
    pf = round(min(gw / gl if gl > 0 else 99, 99), 2)
    h = round(df["hold_days"].mean(), 1)
    eff = round(avg / h, 4) if h > 0 else 0
    pnl = int(df["pnl_yen"].sum())
    return {
        "n": n, "wins": wins, "wr": wr, "avg": avg, "med": med,
        "pf": pf, "hold": h, "eff": eff, "pnl": pnl,
    }


def print_stats(label: str, s: dict) -> None:
    if s["n"] == 0:
        print(f"  {label:45s}  n=0")
        return
    print(
        f"  {label:45s}  n={s['n']:4d}  WR={s['wr']:5.1f}%  "
        f"avg={s['avg']:+7.3f}%  med={s['med']:+7.3f}%  PF={s['pf']:5.2f}  "
        f"hold={s['hold']:4.1f}d  eff={s['eff']:+.4f}%/d  PnL=¥{s['pnl']:+,}"
    )


def gd_analysis(baseline: pd.DataFrame, oco_a: pd.DataFrame) -> pd.DataFrame:
    """SMA20 exitトレードのGD影響を個別比較"""
    bl_sma = baseline[baseline["exit_type"] == "SMA20_翌Open"].copy()
    oco_sma = oco_a[oco_a["exit_type"] == "SMA20_指値"].copy()

    bl_sma = bl_sma.rename(columns={"exit_price": "xp_bl", "ret_pct": "ret_bl", "hold_days": "hold_bl"})
    oco_sma = oco_sma.rename(columns={"exit_price": "xp_oco", "ret_pct": "ret_oco", "hold_days": "hold_oco"})
    merged = bl_sma.merge(
        oco_sma[["ticker", "signal_date", "xp_oco", "ret_oco", "hold_oco"]],
        on=["ticker", "signal_date"],
        how="inner",
    )
    merged["gd_impact"] = merged["ret_oco"] - merged["ret_bl"]
    return merged.sort_values("gd_impact", ascending=False)


def build_html(all_stats: list[dict], gd_df: pd.DataFrame) -> str:
    stats_json = json.dumps(all_stats, ensure_ascii=False)

    # GDワースト/ベスト
    gd_records = []
    if not gd_df.empty:
        for _, r in gd_df.head(20).iterrows():
            gd_records.append({
                "ticker": r["ticker"],
                "signal": r["signal_date"].strftime("%Y-%m-%d"),
                "ret_bl": round(float(r["ret_bl"]), 2),
                "ret_oco": round(float(r["ret_oco"]), 2),
                "impact": round(float(r["gd_impact"]), 2),
                "hold_bl": int(r["hold_bl"]),
                "hold_oco": int(r["hold_oco"]),
            })
        for _, r in gd_df.tail(20).iterrows():
            gd_records.append({
                "ticker": r["ticker"],
                "signal": r["signal_date"].strftime("%Y-%m-%d"),
                "ret_bl": round(float(r["ret_bl"]), 2),
                "ret_oco": round(float(r["ret_oco"]), 2),
                "impact": round(float(r["gd_impact"]), 2),
                "hold_bl": int(r["hold_bl"]),
                "hold_oco": int(r["hold_oco"]),
            })
    gd_json = json.dumps(gd_records, ensure_ascii=False)
    gd_summary = {}
    if not gd_df.empty:
        gd_summary = {
            "n": len(gd_df),
            "avg_impact": round(float(gd_df["gd_impact"].mean()), 3),
            "med_impact": round(float(gd_df["gd_impact"].median()), 3),
            "positive": int((gd_df["gd_impact"] > 0).sum()),
            "negative": int((gd_df["gd_impact"] < 0).sum()),
            "max_gain": round(float(gd_df["gd_impact"].max()), 2),
            "max_loss": round(float(gd_df["gd_impact"].min()), 2),
        }
    gd_summary_json = json.dumps(gd_summary, ensure_ascii=False)

    css = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: #0f0f23; color: #e2e8f0; font-family: 'Helvetica Neue', Arial, sans-serif; }
.header { background: linear-gradient(135deg, #1e1b4b, #312e81); padding: 24px 32px; border-bottom: 2px solid #4338ca; }
.header h1 { font-size: 20px; }
.header p { color: #a5b4fc; font-size: 13px; margin-top: 4px; }
.container { max-width: 1400px; margin: 0 auto; padding: 20px 24px 60px; }
h2 { color: #a5b4fc; font-size: 16px; margin: 28px 0 12px; }
table { width: 100%; border-collapse: collapse; font-size: 14px; margin-bottom: 20px; }
th { padding: 10px 12px; text-align: left; color: #9ca3af; font-size: 12px; font-weight: 600;
     border-bottom: 2px solid #4338ca; white-space: nowrap; background: #1a1a2e; }
td { padding: 9px 12px; border-bottom: 1px solid #1e1e3a; white-space: nowrap; }
tr:hover { background: #1e1b4b33; }
.win { color: #4ade80; } .lose { color: #f87171; }
.best-row { background: #14532d33; }
.num { font-variant-numeric: tabular-nums; text-align: right; }
.pf { font-weight: bold; font-size: 16px; }
.section { background: #1a1a2e; border: 1px solid #2d2d44; border-radius: 10px; padding: 20px; margin-bottom: 16px; }
.gd-summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin: 16px 0; }
.gd-card { background: #1e1b4b; border: 1px solid #4338ca; border-radius: 8px; padding: 12px; text-align: center; }
.gd-label { color: #a5b4fc; font-size: 12px; }
.gd-value { font-size: 22px; font-weight: bold; margin-top: 4px; }
"""

    js = """
const STATS = __STATS__;
const GD_RECORDS = __GD__;
const GD_SUMMARY = __GD_SUMMARY__;

// --- 比較テーブル ---
const tbody = document.getElementById("compare-body");
let bestPf = 0, bestIdx = -1;
STATS.forEach((s, i) => { if (s.pf > bestPf) { bestPf = s.pf; bestIdx = i; } });

STATS.forEach((s, i) => {
  const tr = document.createElement("tr");
  if (i === bestIdx) tr.className = "best-row";
  const pfClass = s.pf >= 5 ? "win" : s.pf >= 3 ? "" : "lose";
  const wrClass = s.wr >= 70 ? "win" : s.wr >= 50 ? "" : "lose";
  tr.innerHTML =
    "<td><b>" + s.label + "</b></td>" +
    '<td class="num">' + s.n + "</td>" +
    '<td class="num ' + wrClass + '">' + s.wr + "%</td>" +
    '<td class="num">' + (s.avg > 0 ? "+" : "") + s.avg.toFixed(3) + "%</td>" +
    '<td class="num">' + (s.med > 0 ? "+" : "") + s.med.toFixed(3) + "%</td>" +
    '<td class="num pf ' + pfClass + '">' + s.pf + "</td>" +
    '<td class="num">' + s.hold + "d</td>" +
    '<td class="num">' + (s.eff > 0 ? "+" : "") + s.eff.toFixed(4) + "</td>" +
    '<td class="num">&yen;' + s.pnl.toLocaleString() + "</td>";
  tbody.appendChild(tr);
});

// --- GDサマリー ---
if (GD_SUMMARY.n) {
  const impClass = GD_SUMMARY.avg_impact > 0 ? "win" : "lose";
  document.getElementById("gd-n").textContent = GD_SUMMARY.n;
  const el = document.getElementById("gd-avg");
  el.textContent = (GD_SUMMARY.avg_impact > 0 ? "+" : "") + GD_SUMMARY.avg_impact.toFixed(3) + "%";
  el.className = "gd-value " + impClass;
  document.getElementById("gd-pos").textContent = GD_SUMMARY.positive + " (" + (GD_SUMMARY.positive/GD_SUMMARY.n*100).toFixed(0) + "%)";
  document.getElementById("gd-neg").textContent = GD_SUMMARY.negative + " (" + (GD_SUMMARY.negative/GD_SUMMARY.n*100).toFixed(0) + "%)";
}

// --- GD個別テーブル ---
const gdBody = document.getElementById("gd-body");
GD_RECORDS.forEach(r => {
  const tr = document.createElement("tr");
  const impClass = r.impact > 0 ? "win" : "lose";
  tr.innerHTML =
    "<td>" + r.ticker + "</td>" +
    "<td>" + r.signal + "</td>" +
    '<td class="num">' + (r.ret_bl > 0 ? "+" : "") + r.ret_bl.toFixed(2) + "%</td>" +
    '<td class="num">' + (r.ret_oco > 0 ? "+" : "") + r.ret_oco.toFixed(2) + "%</td>" +
    '<td class="num ' + impClass + '"><b>' + (r.impact > 0 ? "+" : "") + r.impact.toFixed(2) + "%</b></td>" +
    '<td class="num">' + r.hold_bl + "d → " + r.hold_oco + "d</td>";
  gdBody.appendChild(tr);
});
"""
    js = js.replace("__STATS__", stats_json)
    js = js.replace("__GD__", gd_json)
    js = js.replace("__GD_SUMMARY__", gd_summary_json)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>大陰線 Exit OCO比較</title>
<style>{css}</style>
</head>
<body>
<div class="header">
  <h1>大陰線(-5%) Exit改善: SMA20指値OCO vs 現行</h1>
  <p>②株価≤15,000 | Core+Large | {LOOKBACK_YEARS}年 | MAX_HOLD {MAX_HOLD}日</p>
</div>
<div class="container">

<h2>Exit方式 比較</h2>
<div class="section">
<table>
<thead><tr>
  <th>パターン</th><th>n</th><th>WR</th><th>avg</th><th>med</th>
  <th>PF</th><th>hold</th><th>eff(%/d)</th><th>PnL</th>
</tr></thead>
<tbody id="compare-body"></tbody>
</table>
</div>

<h2>GD影響分析（SMA20 exitトレードのみ: 指値 vs 翌Open）</h2>
<div class="section">
  <div class="gd-summary">
    <div class="gd-card"><div class="gd-label">対象トレード</div><div class="gd-value" id="gd-n">-</div></div>
    <div class="gd-card"><div class="gd-label">平均影響</div><div class="gd-value" id="gd-avg">-</div></div>
    <div class="gd-card"><div class="gd-label">指値有利</div><div class="gd-value win" id="gd-pos">-</div></div>
    <div class="gd-card"><div class="gd-label">翌Open有利</div><div class="gd-value lose" id="gd-neg">-</div></div>
  </div>
  <table>
  <thead><tr>
    <th>銘柄</th><th>シグナル日</th><th>翌Open ret</th><th>指値 ret</th><th>影響</th><th>保有日数</th>
  </tr></thead>
  <tbody id="gd-body"></tbody>
  </table>
</div>

</div>
<script>{js}</script>
</body>
</html>"""
    return html


def main() -> int:
    print("=" * 120)
    print("大陰線(-5%) Exit改善: OCO指値 vs 現行")
    print("=" * 120)

    ps = load_data()
    print(f"  Prices: {len(ps):,} rows, {ps['ticker'].nunique()} tickers")
    signals = get_signals(ps)
    print(f"  Signals: {len(signals)}")

    # --- 各パターン実行 ---
    configs = [
        ("現行: SMA20翌Open + Day3SL", "baseline", {}),
        ("A: SMA20指値 / SLなし", "oco", {"sl_pct": None, "day3_cut": False}),
        ("B-3: SMA20指値 / -3%逆指値", "oco", {"sl_pct": -3, "day3_cut": False}),
        ("B-5: SMA20指値 / -5%逆指値", "oco", {"sl_pct": -5, "day3_cut": False}),
        ("B-7: SMA20指値 / -7%逆指値", "oco", {"sl_pct": -7, "day3_cut": False}),
        ("B-10: SMA20指値 / -10%逆指値", "oco", {"sl_pct": -10, "day3_cut": False}),
        ("C: SMA20指値 / Day3SL", "oco", {"sl_pct": None, "day3_cut": True}),
    ]

    all_stats = []
    results = {}
    print(f"\n{'':2s}{'パターン':45s}  {'n':>4s}  {'WR':>6s}  {'avg':>8s}  {'med':>8s}  {'PF':>5s}  {'hold':>5s}  {'eff':>8s}  {'PnL':>10s}")
    print("-" * 120)
    for label, mode, kwargs in configs:
        if mode == "baseline":
            df = backtest_baseline(ps, signals)
        else:
            df = backtest_oco(ps, signals, **kwargs)
        s = calc_stats(df)
        s["label"] = label
        all_stats.append(s)
        results[label] = df
        print_stats(label, s)

    # --- GD分析 ---
    print("\n" + "=" * 120)
    print("■ GD影響分析（SMA20 exit: 指値 vs 翌Open）")
    print("-" * 120)
    baseline = results["現行: SMA20翌Open + Day3SL"]
    oco_a = results["A: SMA20指値 / SLなし"]
    gd_df = gd_analysis(baseline, oco_a)
    if not gd_df.empty:
        imp = gd_df["gd_impact"]
        print(f"  対象: {len(gd_df)}件")
        print(f"  平均影響: {imp.mean():+.3f}%")
        print(f"  中央値:   {imp.median():+.3f}%")
        print(f"  指値有利: {(imp > 0).sum()}件 ({(imp > 0).mean()*100:.0f}%)")
        print(f"  翌Open有利: {(imp < 0).sum()}件 ({(imp < 0).mean()*100:.0f}%)")
        print(f"  最大改善: {imp.max():+.2f}%")
        print(f"  最大悪化: {imp.min():+.2f}%")

        print("\n  ■ GD損失ワースト10（指値の方が有利だったケース）")
        for _, r in gd_df.head(10).iterrows():
            print(f"    {r['ticker']}  {r['signal_date'].strftime('%Y-%m-%d')}  "
                  f"翌Open={r['ret_bl']:+.2f}%  指値={r['ret_oco']:+.2f}%  "
                  f"差={r['gd_impact']:+.2f}%  hold {r['hold_bl']}→{r['hold_oco']}d")

    # --- HTML出力 ---
    html = build_html(all_stats, gd_df)
    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"\n  [OK] {OUTPUT_HTML}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
