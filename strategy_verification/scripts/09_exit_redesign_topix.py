#!/usr/bin/env python3
"""
09_exit_redesign_topix.py
==========================
TOPIX 1,660銘柄版 Exit Rule Redesign

Changes from 09_exit_redesign.py:
- Data: trades_cleaned_topix_v2 + prices_cleaned_topix_v3
- SL: 全ルール999.0（Ch02でSL不要と確認済み）
- B4: 全カテゴリで検証（168版はTC13dのみ）
- simulate_all: iterrows → numpy配列直接アクセス（高速化）
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SV_DIR = ROOT / "strategy_verification"
PROCESSED = SV_DIR / "data" / "processed"
REPORT_DIR = SV_DIR / "chapters" / "05_exit_redesign"

RULES = ["B1", "B2", "B3", "B4"]
MAX_HOLD = 60


# ---- Price lookup ----

def build_price_lookup(prices: pd.DataFrame) -> dict:
    lookup = {}
    for ticker, grp in prices.groupby("ticker"):
        grp = grp.sort_values("date").dropna(subset=["Close"])
        if len(grp) == 0:
            continue
        lookup[ticker] = {
            "dates": grp["date"].values,
            "opens": grp["Open"].values.astype(np.float64),
            "highs": grp["High"].values.astype(np.float64),
            "lows": grp["Low"].values.astype(np.float64),
            "closes": grp["Close"].values.astype(np.float64),
        }
    return lookup


# ---- Simulation ----

def simulate_trade(
    pl: dict, entry_date: np.datetime64, entry_price: float,
    mode: str, param: float, sl_pct: float = 999.0,
) -> tuple[float, float, int] | None:
    """Returns (ret_pct, pnl, hold_days) or None."""
    dates = pl["dates"]
    opens = pl["opens"]
    highs = pl["highs"]
    lows = pl["lows"]
    closes = pl["closes"]

    entry_mask = dates == entry_date
    if not entry_mask.any():
        return None
    entry_idx = int(np.where(entry_mask)[0][0])

    sl_price = entry_price * (1 - sl_pct / 100) if sl_pct < 900 else 0.0
    max_high = entry_price
    exit_price = None
    exit_day = 0

    for d in range(MAX_HOLD):
        ci = entry_idx + d
        if ci >= len(dates):
            break

        c_high = highs[ci]
        c_low = lows[ci]
        c_close = closes[ci]

        if d == 0:
            if sl_pct < 900 and c_low <= sl_price:
                exit_price = sl_price
                exit_day = d
                break
            max_high = max(max_high, c_high)
            continue

        if sl_pct < 900 and c_low <= sl_price:
            exit_price = sl_price
            exit_day = d
            break

        max_high = max(max_high, c_high)

        if mode == "fixed_N":
            n = int(param)
            if d == n:
                exit_price = opens[ci + 1] if ci + 1 < len(dates) else c_close
                exit_day = d + 1 if ci + 1 < len(dates) else d
                break

        elif mode == "min_hold_N":
            n = int(param)
            if d >= n and c_close < entry_price:
                exit_price = opens[ci + 1] if ci + 1 < len(dates) else c_close
                exit_day = d + 1 if ci + 1 < len(dates) else d
                break

        elif mode == "trailing_X":
            x = param
            trail_trigger = max_high * (1 - x / 100)
            if c_close <= trail_trigger and d >= 1:
                exit_price = opens[ci + 1] if ci + 1 < len(dates) else c_close
                exit_day = d + 1 if ci + 1 < len(dates) else d
                break

    if exit_price is None:
        ci = min(entry_idx + MAX_HOLD - 1, len(dates) - 1)
        exit_price = opens[ci + 1] if ci + 1 < len(dates) else closes[ci]
        exit_day = MAX_HOLD if ci + 1 < len(dates) else MAX_HOLD - 1

    ret_pct = (exit_price / entry_price - 1) * 100
    pnl = entry_price * 100 * ret_pct / 100
    return (round(ret_pct, 3), round(pnl, 2), exit_day)


def simulate_all_fast(
    tickers: np.ndarray, entry_dates: np.ndarray, entry_prices: np.ndarray,
    price_lookup: dict, mode: str, param: float, sl_pct: float = 999.0,
) -> dict:
    """iterrows不使用の高速版。"""
    rets = []
    pnls = []
    holds = []

    for i in range(len(tickers)):
        t = tickers[i]
        if t not in price_lookup:
            continue
        r = simulate_trade(price_lookup[t], entry_dates[i], entry_prices[i],
                           mode, param, sl_pct)
        if r is not None:
            rets.append(r[0])
            pnls.append(r[1])
            holds.append(r[2])

    if not rets:
        return {"n": 0, "wr": 0, "pf": 0, "pnl_m": 0, "avg_ret": 0, "avg_hold": 0}

    rets_a = np.array(rets)
    pnls_a = np.array(pnls)
    holds_a = np.array(holds)
    wins = rets_a > 0
    gw = rets_a[wins].sum()
    gl = abs(rets_a[~wins].sum())

    return {
        "n": len(rets),
        "wr": round(wins.mean() * 100, 1),
        "pf": round(gw / gl if gl > 0 else 999, 2),
        "pnl_m": round(pnls_a.sum() / 10000, 1),
        "avg_ret": round(rets_a.mean(), 3),
        "avg_hold": round(holds_a.mean(), 1),
    }


# ---- HTML helpers ----

def _stat_card(label: str, value: str, sub: str = "", tone: str = "") -> str:
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    cls = {"pos": "card-pos", "neg": "card-neg", "warn": "card-warn"}.get(tone, "")
    return (f'<div class="stat-card {cls}"><div class="label">{label}</div>'
            f'<div class="value">{value}</div>{sub_html}</div>')


def _table_html(headers: list[str], rows: list[list], highlight_col: int | None = None) -> str:
    ths = "".join(f"<th>{h}</th>" for h in headers)
    best_idx = -1
    if highlight_col is not None:
        vals = []
        for r in rows:
            try:
                raw = (str(r[highlight_col]).replace("万", "").replace(",", "")
                       .replace("+", "").replace("<b>", "").replace("</b>", ""))
                vals.append(float(raw))
            except (ValueError, IndexError):
                vals.append(-9999)
        if vals:
            best_idx = vals.index(max(vals))
    trs = []
    for i, row in enumerate(rows):
        cls = ' class="best-row"' if i == best_idx else ""
        tds = "".join(f"<td>{c}</td>" for c in row)
        trs.append(f"<tr{cls}>{tds}</tr>")
    return f'<table><thead><tr>{ths}</tr></thead><tbody>{"".join(trs)}</tbody></table>'


def _insight_box(text: str) -> str:
    return f'<div class="insight-box">{text}</div>'


def _section(title: str, content: str) -> str:
    return f'<section><h2>{title}</h2>{content}</section>'


def _plotly_bar(div_id: str, traces: list[dict], title: str = "",
                xaxis_title: str = "", yaxis_title: str = "", height: int = 350) -> str:
    data = json.dumps(traces)
    layout = json.dumps({
        "template": "plotly_dark",
        "paper_bgcolor": "transparent", "plot_bgcolor": "transparent",
        "margin": {"t": 40, "b": 50, "l": 60, "r": 20},
        "font": {"size": 11, "color": "#e2e8f0"},
        "title": {"text": title, "font": {"size": 13}},
        "barmode": "group",
        "xaxis": {"title": xaxis_title}, "yaxis": {"title": yaxis_title},
    })
    return f'<div id="{div_id}" style="height:{height}px"></div>\n<script>Plotly.newPlot("{div_id}",{data},{layout},{{responsive:true}})</script>'


# ---- Main ----

def main():
    t0 = time.time()

    # ===== Load =====
    print("[1/6] Loading TOPIX data...")
    trades = pd.read_parquet(PROCESSED / "trades_cleaned_topix_v2.parquet")
    prices = pd.read_parquet(PROCESSED / "prices_cleaned_topix_v3.parquet")
    long = trades[trades["direction"] == "LONG"].copy()
    print(f"  LONG trades: {len(long):,}")
    print(f"  Price records: {len(prices):,}")
    print(f"  Tickers: {prices['ticker'].nunique()}")

    print("[2/6] Building price lookup...")
    t1 = time.time()
    price_lookup = build_price_lookup(prices)
    print(f"  Done: {len(price_lookup)} tickers ({time.time()-t1:.1f}s)")

    # Pre-extract arrays per rule
    rule_data = {}
    for rule in RULES:
        sub = long[long["rule"] == rule]
        rule_data[rule] = {
            "n": len(sub),
            "tickers": sub["ticker"].values,
            "dates": sub["entry_date"].values.astype("datetime64[ns]"),
            "prices": sub["entry_price"].values.astype(np.float64),
            "ret_pcts": sub["ret_pct"].values,
            "entry_prices_raw": sub["entry_price"].values,
            "hold_days": sub["hold_days"].values,
        }
    for rule in RULES:
        print(f"  {rule}: {rule_data[rule]['n']:,} trades")

    sections_html = []

    # ===== Section 1: Baseline =====
    print("[3/6] Section 1: Baseline...")
    baseline_results = {}
    cards = []
    for rule in RULES:
        rd = rule_data[rule]
        ret = rd["ret_pcts"]
        pnl_total = (rd["entry_prices_raw"] * 100 * ret / 100).sum() / 10000
        wins = ret > 0
        gw = ret[wins].sum()
        gl = abs(ret[~wins].sum())
        pf = gw / gl if gl > 0 else 999
        wr = wins.mean() * 100
        avg_hold = rd["hold_days"].mean()

        baseline_results[rule] = {
            "n": rd["n"], "wr": round(wr, 1), "pf": round(pf, 2),
            "pnl_m": round(pnl_total, 1), "avg_hold": round(avg_hold, 1),
        }
        cards.append(_stat_card(
            rule, f'PnL {pnl_total:+,.0f}万',
            f'N={rd["n"]:,} / WR={wr:.1f}% / PF={pf:.2f} / AvgHold={avg_hold:.0f}d',
            "pos" if pnl_total > 500 else ("warn" if pnl_total > 0 else "neg"),
        ))

    s1 = f'<div class="card-grid">{" ".join(cards)}</div>'
    s1 += _insight_box(
        "現行のシグナルexit（SMA20タッチ/デッドクロス）による結果。SL=なし（全ルール）。"
        "<br>1,660 TOPIX銘柄でexit ruleを変更し改善を狙う。"
    )
    sections_html.append(_section("1. ベースライン（現行exit rule）", s1))

    # ===== Section 2: Fixed Hold Period =====
    print("[4/6] Section 2: Fixed hold period...")
    fixed_ns = [3, 5, 7, 10, 14, 20, 30, 45, 60]
    fixed_results: dict[str, list] = {rule: [] for rule in RULES}

    for rule in RULES:
        rd = rule_data[rule]
        t1 = time.time()
        for n in fixed_ns:
            s = simulate_all_fast(rd["tickers"], rd["dates"], rd["prices"],
                                  price_lookup, "fixed_N", n, 999.0)
            fixed_results[rule].append(s)
        print(f"  {rule}: fixed hold done ({time.time()-t1:.1f}s)")

    s2 = _insight_box(
        "<b>固定保有期間</b>: シグナルexitを全て無視し、N日目に強制exit。SLなし。"
    )
    for rule in RULES:
        rows = []
        for i, n in enumerate(fixed_ns):
            s = fixed_results[rule][i]
            delta = s["pnl_m"] - baseline_results[rule]["pnl_m"]
            rows.append([
                f'{n}日', f'{s["n"]:,}', f'{s["wr"]}%', f'{s["pf"]:.2f}',
                f'{s["pnl_m"]:+,.0f}万', f'{s["avg_hold"]:.0f}d', f'{delta:+,.0f}万',
            ])
        s2 += f"<h3>{rule}</h3>"
        s2 += _table_html(["保有日数", "N", "WR", "PF", "PnL", "AvgHold", "vs現行"], rows, 4)

        labels = [f"{n}d" for n in fixed_ns]
        pnls = [r["pnl_m"] for r in fixed_results[rule]]
        traces = [{"x": labels, "y": pnls, "type": "bar", "name": "PnL(万)",
                   "marker": {"color": ["rgba(52,211,153,0.7)" if v > 0 else "rgba(248,113,113,0.7)" for v in pnls]}}]
        s2 += _plotly_bar(f"fixed_{rule}", traces, f"{rule}: 固定保有期間別PnL", "保有日数", "PnL(万)")

    sections_html.append(_section("2. 固定保有期間（全ルール）", s2))

    # ===== Section 3: Minimum Hold Period =====
    print("[5/6] Section 3: Minimum hold period...")
    min_ns = [3, 5, 7, 10, 14, 20, 30]
    minhold_results: dict[str, list] = {rule: [] for rule in RULES}

    for rule in RULES:
        rd = rule_data[rule]
        t1 = time.time()
        for n in min_ns:
            s = simulate_all_fast(rd["tickers"], rd["dates"], rd["prices"],
                                  price_lookup, "min_hold_N", n, 999.0)
            minhold_results[rule].append(s)
        print(f"  {rule}: min hold done ({time.time()-t1:.1f}s)")

    s3 = _insight_box(
        "<b>最低保有期間</b>: 最初のN日間はexit無視。N日後、Close < entryで翌日exit。SLなし。"
    )
    for rule in RULES:
        rows = []
        for i, n in enumerate(min_ns):
            s = minhold_results[rule][i]
            delta = s["pnl_m"] - baseline_results[rule]["pnl_m"]
            rows.append([
                f'{n}日', f'{s["n"]:,}', f'{s["wr"]}%', f'{s["pf"]:.2f}',
                f'{s["pnl_m"]:+,.0f}万', f'{s["avg_hold"]:.0f}d', f'{delta:+,.0f}万',
            ])
        s3 += f"<h3>{rule}</h3>"
        s3 += _table_html(["最低保有", "N", "WR", "PF", "PnL", "AvgHold", "vs現行"], rows, 4)

        labels = [f"{n}d" for n in min_ns]
        pnls = [r["pnl_m"] for r in minhold_results[rule]]
        traces = [{"x": labels, "y": pnls, "type": "bar", "name": "PnL(万)",
                   "marker": {"color": ["rgba(52,211,153,0.7)" if v > 0 else "rgba(248,113,113,0.7)" for v in pnls]}}]
        s3 += _plotly_bar(f"minhold_{rule}", traces, f"{rule}: 最低保有期間別PnL", "最低保有日数", "PnL(万)")

    sections_html.append(_section("3. 最低保有期間（全ルール）", s3))

    # ===== Section 4: Trailing Stop =====
    print("[6/6] Section 4: Trailing stop...")
    trail_xs = [2.0, 3.0, 5.0, 7.0, 10.0, 15.0, 20.0]
    trail_results: dict[str, list] = {rule: [] for rule in RULES}

    for rule in RULES:
        rd = rule_data[rule]
        t1 = time.time()
        for x in trail_xs:
            s = simulate_all_fast(rd["tickers"], rd["dates"], rd["prices"],
                                  price_lookup, "trailing_X", x, 999.0)
            trail_results[rule].append(s)
        print(f"  {rule}: trailing done ({time.time()-t1:.1f}s)")

    s4 = _insight_box(
        "<b>トレーリングストップ</b>: 保有中の最高値からX%下落でexit。SLなし。"
    )
    for rule in RULES:
        rows = []
        for i, x in enumerate(trail_xs):
            s = trail_results[rule][i]
            delta = s["pnl_m"] - baseline_results[rule]["pnl_m"]
            rows.append([
                f'{x}%', f'{s["n"]:,}', f'{s["wr"]}%', f'{s["pf"]:.2f}',
                f'{s["pnl_m"]:+,.0f}万', f'{s["avg_hold"]:.0f}d', f'{delta:+,.0f}万',
            ])
        s4 += f"<h3>{rule}</h3>"
        s4 += _table_html(["Trail幅", "N", "WR", "PF", "PnL", "AvgHold", "vs現行"], rows, 4)

        labels = [f"{x}%" for x in trail_xs]
        pnls = [r["pnl_m"] for r in trail_results[rule]]
        traces = [{"x": labels, "y": pnls, "type": "bar", "name": "PnL(万)",
                   "marker": {"color": ["rgba(52,211,153,0.7)" if v > 0 else "rgba(248,113,113,0.7)" for v in pnls]}}]
        s4 += _plotly_bar(f"trail_{rule}", traces, f"{rule}: トレーリングストップ幅別PnL", "Trail幅", "PnL(万)")

    sections_html.append(_section("4. トレーリングストップ（全ルール）", s4))

    # ===== Section 5: Best Strategy Comparison =====
    s5 = ""
    grand_total_best = 0.0
    total_base = sum(baseline_results[r]["pnl_m"] for r in RULES)
    grand_detail = []

    for rule in RULES:
        base = baseline_results[rule]

        best_fixed_idx = max(range(len(fixed_results[rule])),
                             key=lambda i: fixed_results[rule][i]["pnl_m"])
        bf = fixed_results[rule][best_fixed_idx]
        bf_n = fixed_ns[best_fixed_idx]

        best_mh_idx = max(range(len(minhold_results[rule])),
                          key=lambda i: minhold_results[rule][i]["pnl_m"])
        bmh = minhold_results[rule][best_mh_idx]
        bmh_n = min_ns[best_mh_idx]

        best_tr_idx = max(range(len(trail_results[rule])),
                          key=lambda i: trail_results[rule][i]["pnl_m"])
        btr = trail_results[rule][best_tr_idx]
        btr_x = trail_xs[best_tr_idx]

        rows = [
            ["現行（signal exit）", "-", f'{base["wr"]}%', f'{base["pf"]:.2f}',
             f'{base["pnl_m"]:+,.0f}万', f'{base["avg_hold"]}d', "-"],
            [f"固定保有 {bf_n}日", f"fixed_{bf_n}d",
             f'{bf["wr"]}%', f'{bf["pf"]:.2f}',
             f'{bf["pnl_m"]:+,.0f}万', f'{bf["avg_hold"]}d',
             f'{bf["pnl_m"] - base["pnl_m"]:+,.0f}万'],
            [f"最低保有 {bmh_n}日", f"min_hold_{bmh_n}d",
             f'{bmh["wr"]}%', f'{bmh["pf"]:.2f}',
             f'{bmh["pnl_m"]:+,.0f}万', f'{bmh["avg_hold"]}d',
             f'{bmh["pnl_m"] - base["pnl_m"]:+,.0f}万'],
            [f"Trail {btr_x}%", f"trail_{btr_x}%",
             f'{btr["wr"]}%', f'{btr["pf"]:.2f}',
             f'{btr["pnl_m"]:+,.0f}万', f'{btr["avg_hold"]}d',
             f'{btr["pnl_m"] - base["pnl_m"]:+,.0f}万'],
        ]
        s5 += f"<h3>{rule}</h3>"
        s5 += _table_html(["戦略", "パラメータ", "WR", "PF", "PnL", "AvgHold", "vs現行"], rows, 4)

        # Find overall best for this rule
        candidates = [
            ("現行", base["pnl_m"]),
            (f"固定{bf_n}d", bf["pnl_m"]),
            (f"最低保有{bmh_n}d", bmh["pnl_m"]),
            (f"Trail{btr_x}%", btr["pnl_m"]),
        ]
        best_name, best_pnl = max(candidates, key=lambda x: x[1])
        grand_total_best += best_pnl
        grand_detail.append((rule, best_name, best_pnl, best_pnl - base["pnl_m"]))

    s5 += "<h3>総合: 各ルールのベスト戦略</h3>"
    grand_rows = []
    for rule, name, pnl_val, delta in grand_detail:
        grand_rows.append([rule, name, f'{pnl_val:+,.0f}万', f'{delta:+,.0f}万'])
    grand_delta = grand_total_best - total_base
    grand_rows.append([
        "<b>合計</b>", "",
        f'<b>{grand_total_best:+,.0f}万</b>',
        f'<b>{grand_delta:+,.0f}万</b>',
    ])
    s5 += _table_html(["Rule", "最適戦略", "PnL", "vs現行"], grand_rows)
    s5 += _insight_box(
        f"<b>Exit Rule Redesignにより、総PnLが{total_base:+,.0f}万 → {grand_total_best:+,.0f}万"
        f"（{grand_delta:+,.0f}万改善）</b><br>"
        f"全ルールSLなし。TOPIX 1,660銘柄ベース。"
    )
    sections_html.append(_section("5. 最適戦略比較", s5))

    # ===== Generate HTML =====
    body = "\n".join(sections_html)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    elapsed = time.time() - t0

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ch5 Exit Rule Redesign — TOPIX 1,660銘柄</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
:root {{
  --bg: #0f1117; --card: #1a1d27; --border: #2a2d3a;
  --text: #e2e8f0; --muted: #8892a8; --primary: #60a5fa;
  --pos: #34d399; --neg: #f87171; --warn: #fbbf24;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: var(--bg); color: var(--text);
  line-height: 1.6; padding: 20px; max-width: 1400px; margin: 0 auto;
}}
h1 {{ font-size: 1.4rem; margin-bottom: 4px; }}
h2 {{ font-size: 1.1rem; color: var(--primary); margin: 24px 0 12px; border-bottom: 1px solid var(--border); padding-bottom: 6px; }}
h3 {{ font-size: 0.95rem; color: var(--muted); margin: 16px 0 8px; }}
section {{ margin-bottom: 24px; }}
.meta {{ font-size: 0.75rem; color: var(--muted); margin-bottom: 16px; }}
.card-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 12px 0; }}
.stat-card {{
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; padding: 12px; text-align: center;
}}
.stat-card .label {{ font-size: 0.75rem; color: var(--muted); }}
.stat-card .value {{ font-size: 1.3rem; font-weight: 700; margin: 4px 0; }}
.stat-card .sub {{ font-size: 0.7rem; color: var(--muted); }}
.card-pos .value {{ color: var(--pos); }}
.card-neg .value {{ color: var(--neg); }}
.card-warn .value {{ color: var(--warn); }}
table {{
  width: 100%; border-collapse: collapse; font-size: 0.8rem;
  margin: 10px 0; background: var(--card);
}}
th, td {{ padding: 6px 10px; border: 1px solid var(--border); text-align: right; }}
th {{ background: #1e2130; color: var(--primary); font-weight: 600; text-align: center; }}
td:first-child {{ text-align: left; font-weight: 500; }}
.best-row {{ background: rgba(96, 165, 250, 0.12); }}
.insight-box {{
  background: rgba(96, 165, 250, 0.08); border-left: 3px solid var(--primary);
  padding: 10px 14px; margin: 12px 0; font-size: 0.82rem;
  border-radius: 0 6px 6px 0; line-height: 1.7;
}}
@media (max-width: 768px) {{
  .card-grid {{ grid-template-columns: repeat(2, 1fr); }}
  table {{ font-size: 0.7rem; }}
  th, td {{ padding: 4px 6px; }}
}}
</style>
</head>
<body>
<h1>Chapter 5: Exit Rule Redesign — TOPIX 1,660銘柄</h1>
<div class="meta">Generated: {now} | Data: {len(long):,} LONG trades | SL: 全ルールなし | Runtime: {elapsed:.0f}s</div>
{body}
</body>
</html>"""

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / "report_topix_all.html"
    out.write_text(html, encoding="utf-8")
    print(f"\n[OK] Report saved: {out}")
    print(f"  Size: {out.stat().st_size / 1024:.0f} KB")
    print(f"  Done in {time.time() - t0:.1f}s ({(time.time()-t0)/60:.1f}min)")


if __name__ == "__main__":
    main()
