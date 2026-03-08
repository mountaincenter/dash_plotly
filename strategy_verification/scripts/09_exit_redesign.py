#!/usr/bin/env python3
"""
09_exit_redesign.py
====================
Chapter 5: Exit Rule Redesign — B1-B3の捕捉率改善

現行exit rule（SMA20タッチ/デッドクロス）を撤廃し、
代替exit strategyで再シミュレーションして最適解を探す。

入力:
  - strategy_verification/data/processed/trades_cleaned.parquet
  - strategy_verification/data/processed/prices_cleaned.parquet

出力:
  - strategy_verification/chapters/05_exit_redesign/report.html

検証する Exit Mode:
  1. 現行（signal exit）— ベースライン
  2. 固定保有期間（N日で強制exit）
  3. 最低保有期間（N日間はsignal exitを無視）
  4. トレーリングストップ（MFEからX%下落でexit）
  5. 最適組み合わせ
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
MAX_HOLD = 60  # 最大保有日数


# ---- Price lookup builder ----


def build_price_lookup(prices: pd.DataFrame) -> dict:
    """ticker → {dates, opens, highs, lows, closes} numpy配列"""
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


# ---- Simulation engine ----


def simulate_trade(
    pl: dict,
    entry_date: np.datetime64,
    entry_price: float,
    mode: str,
    param: float,
    sl_pct: float = 999.0,
) -> dict | None:
    """
    単一トレードを再シミュレーション。

    mode:
      "current" — 使わない（既存ret_pctをそのまま使う）
      "fixed_N" — N日目のOpen（翌営業日寄付）で強制exit
      "min_hold_N" — N日間はexit不可、N日後にClose < entry で初めてexit
      "trailing_X" — 保有中のMFEからX%下落したら翌日Openでexit
    param:
      fixed_N: N(日数), min_hold_N: N(日数), trailing_X: X(%)
    sl_pct:
      SL幅（%）。ザラ場の安値 ≤ entry*(1-sl/100) で即exit。999=SLなし。
    """
    dates = pl["dates"]
    opens = pl["opens"]
    highs = pl["highs"]
    lows = pl["lows"]
    closes = pl["closes"]

    # entry_dateのインデックスを探す
    entry_mask = dates == entry_date
    if not entry_mask.any():
        return None
    entry_idx = np.where(entry_mask)[0][0]

    if sl_pct < 900:
        sl_price = entry_price * (1 - sl_pct / 100)
    else:
        sl_price = 0.0

    max_high = entry_price  # MFE tracking
    exit_price = None
    exit_day = 0
    exit_reason = ""

    for d in range(MAX_HOLD):
        ci = entry_idx + d
        if ci >= len(dates):
            break

        current_low = lows[ci]
        current_high = highs[ci]
        current_close = closes[ci]
        current_open = opens[ci]

        # d=0: エントリー日。SLのみチェック（現行ロジック準拠）
        if d == 0:
            if sl_pct < 900 and current_low <= sl_price:
                exit_price = sl_price
                exit_day = d
                exit_reason = "SL"
                break
            max_high = max(max_high, current_high)
            continue

        # SLチェック（全モード共通）
        if sl_pct < 900 and current_low <= sl_price:
            exit_price = sl_price
            exit_day = d
            exit_reason = "SL"
            break

        max_high = max(max_high, current_high)

        if mode == "fixed_N":
            # N日目に翌営業日Openでexit
            n = int(param)
            if d == n:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                    exit_reason = f"fixed_{n}d"
                else:
                    exit_price = current_close
                    exit_day = d
                    exit_reason = f"fixed_{n}d"
                break

        elif mode == "min_hold_N":
            # N日間はexit不可。N日後、Close < entry で翌日Openでexit
            n = int(param)
            if d >= n and current_close < entry_price:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                    exit_reason = f"min_hold_{n}d_exit"
                else:
                    exit_price = current_close
                    exit_day = d
                    exit_reason = f"min_hold_{n}d_exit"
                break

        elif mode == "trailing_X":
            # MFEからX%下落で翌日Openでexit
            x = param
            mfe_price = max_high
            trail_trigger = mfe_price * (1 - x / 100)
            if current_close <= trail_trigger and d >= 1:
                if ci + 1 < len(dates):
                    exit_price = opens[ci + 1]
                    exit_day = d + 1
                    exit_reason = f"trail_{x}pct"
                else:
                    exit_price = current_close
                    exit_day = d
                    exit_reason = f"trail_{x}pct"
                break

    # expire: MAX_HOLD到達
    if exit_price is None:
        ci = min(entry_idx + MAX_HOLD - 1, len(dates) - 1)
        if ci + 1 < len(dates):
            exit_price = opens[ci + 1]
            exit_day = MAX_HOLD
        else:
            exit_price = closes[ci]
            exit_day = MAX_HOLD - 1
        exit_reason = "expire"

    ret_pct = (exit_price / entry_price - 1) * 100
    pnl = entry_price * 100 * ret_pct / 100  # 100株

    return {
        "ret_pct": round(ret_pct, 3),
        "pnl": round(pnl, 2),
        "hold_days": exit_day,
        "exit_reason": exit_reason,
    }


def simulate_all(
    trades: pd.DataFrame,
    price_lookup: dict,
    mode: str,
    param: float,
    sl_pct: float = 999.0,
) -> dict:
    """全トレードをシミュレーションし統計量を返す"""
    results = []
    for _, row in trades.iterrows():
        ticker = row["ticker"]
        if ticker not in price_lookup:
            continue
        r = simulate_trade(
            price_lookup[ticker],
            row["entry_date"].to_numpy().astype("datetime64[ns]")
            if hasattr(row["entry_date"], "to_numpy")
            else np.datetime64(row["entry_date"]),
            float(row["entry_price"]),
            mode, param, sl_pct,
        )
        if r is not None:
            results.append(r)

    if not results:
        return {"n": 0, "wr": 0, "pf": 0, "pnl_m": 0, "avg_ret": 0, "avg_hold": 0}

    rets = np.array([r["ret_pct"] for r in results])
    pnls = np.array([r["pnl"] for r in results])
    holds = np.array([r["hold_days"] for r in results])

    wins = rets > 0
    gw = rets[wins].sum()
    gl = abs(rets[~wins].sum())
    pf = gw / gl if gl > 0 else 999

    return {
        "n": len(results),
        "wr": round(wins.mean() * 100, 1),
        "pf": round(pf, 2),
        "pnl_m": round(pnls.sum() / 10000, 1),
        "avg_ret": round(rets.mean(), 3),
        "avg_hold": round(holds.mean(), 1),
    }


# ---- HTML helpers ----


def _stat_card(label: str, value: str, sub: str = "", tone: str = "") -> str:
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    cls = {"pos": "card-pos", "neg": "card-neg", "warn": "card-warn"}.get(tone, "")
    return (
        f'<div class="stat-card {cls}">'
        f'<div class="label">{label}</div>'
        f'<div class="value">{value}</div>{sub_html}</div>'
    )


def _table_html(headers: list[str], rows: list[list], highlight_col: int | None = None) -> str:
    ths = "".join(f"<th>{h}</th>" for h in headers)
    trs = []
    best_idx = -1
    if highlight_col is not None:
        vals = []
        for r in rows:
            try:
                raw = str(r[highlight_col]).replace("万", "").replace(",", "").replace("+", "").replace("<b>", "").replace("</b>", "")
                v = float(raw)
            except (ValueError, IndexError):
                v = -9999
            vals.append(v)
        if vals:
            best_idx = vals.index(max(vals))
    for i, row in enumerate(rows):
        cls = ' class="best-row"' if i == best_idx else ""
        tds = "".join(f"<td>{c}</td>" for c in row)
        trs.append(f"<tr{cls}>{tds}</tr>")
    return f'<table><thead><tr>{ths}</tr></thead><tbody>{"".join(trs)}</tbody></table>'


def _insight_box(text: str) -> str:
    return f'<div class="insight-box">{text}</div>'


def _section(title: str, content: str) -> str:
    return f'<section><h2>{title}</h2>{content}</section>'


def _plotly_grouped_bar(div_id: str, traces: list[dict], title: str = "",
                         xaxis_title: str = "", yaxis_title: str = "",
                         height: int = 350) -> str:
    data = json.dumps(traces)
    layout = json.dumps({
        "template": "plotly_dark",
        "paper_bgcolor": "transparent", "plot_bgcolor": "transparent",
        "margin": {"t": 40, "b": 50, "l": 60, "r": 20},
        "font": {"size": 11, "color": "#e2e8f0"},
        "title": {"text": title, "font": {"size": 13}},
        "barmode": "group",
        "xaxis": {"title": xaxis_title},
        "yaxis": {"title": yaxis_title},
    })
    return f"""<div id="{div_id}" style="height:{height}px"></div>
<script>Plotly.newPlot("{div_id}",{data},{layout},{{responsive:true}})</script>"""


# ---- Main ----


def main():
    t0 = time.time()
    print("[1/7] Loading data...")
    trades = pd.read_parquet(PROCESSED / "trades_cleaned.parquet")
    prices = pd.read_parquet(PROCESSED / "prices_cleaned.parquet")

    long = trades[trades["direction"] == "LONG"].copy()
    print(f"  LONG trades: {len(long):,}")
    print(f"  Price records: {len(prices):,}")

    print("[2/7] Building price lookup...")
    price_lookup = build_price_lookup(prices)
    print(f"  Tickers with prices: {len(price_lookup)}")

    sections_html = []

    # SL settings (Ch4-2 optimal: B1/B2=-3%, B3=-2.5%, B4=none)
    PROPOSED_SLS = {"B1": 3.0, "B2": 3.0, "B3": 2.5, "B4": 999.0}

    # ==================== Section 1: Baseline (current exits) ====================
    print("[3/7] Section 1: Baseline...")
    baseline_html = ""
    baseline_results = {}
    cards = []
    for rule in RULES:
        sub = long[long["rule"] == rule]
        # 現行exitのret_pctそのまま使う
        ret = sub["ret_pct"].values
        pnl_total = (sub["entry_price"] * 100 * ret / 100).sum() / 10000
        wins = ret > 0
        gw = ret[wins].sum()
        gl = abs(ret[~wins].sum())
        pf = gw / gl if gl > 0 else 999
        wr = wins.mean() * 100
        avg_hold = sub["hold_days"].mean()

        baseline_results[rule] = {
            "n": len(sub), "wr": round(wr, 1), "pf": round(pf, 2),
            "pnl_m": round(pnl_total, 1), "avg_hold": round(avg_hold, 1),
        }
        cards.append(_stat_card(
            rule, f'PnL {pnl_total:+,.0f}万',
            f'N={len(sub):,} / WR={wr:.1f}% / PF={pf:.2f} / AvgHold={avg_hold:.0f}d',
            "pos" if pnl_total > 500 else ("warn" if pnl_total > 0 else "neg"),
        ))

    baseline_html += f'<div class="card-grid">{" ".join(cards)}</div>'
    baseline_html += _insight_box(
        "現行のシグナルexit（SMA20タッチ/デッドクロス）による結果。"
        "B1-B3の捕捉率は9-13%。ここからexit ruleを変更して改善を狙う。"
    )
    sections_html.append(_section("1. ベースライン（現行exit rule）", baseline_html))

    # ==================== Section 2: Fixed Hold Period ====================
    print("[4/7] Section 2: Fixed hold period simulation...")
    fixed_html = ""
    fixed_html += _insight_box(
        "<b>固定保有期間</b>: シグナルexitを全て無視し、N日目に強制exit。"
        "「出るな、持て」が本当に正しいかを検証する。"
    )

    fixed_ns = [3, 5, 7, 10, 14, 20, 30, 45, 60]
    fixed_results: dict[str, list] = {rule: [] for rule in RULES}

    for rule in ["B1", "B2", "B3"]:  # B4は別途
        sub = long[long["rule"] == rule]
        sl = PROPOSED_SLS[rule]
        rule_rows = []

        for n in fixed_ns:
            s = simulate_all(sub, price_lookup, "fixed_N", n, sl)
            fixed_results[rule].append(s)
            delta = s["pnl_m"] - baseline_results[rule]["pnl_m"]
            rule_rows.append([
                f'{n}日',
                f'{s["n"]:,}',
                f'{s["wr"]}%',
                f'{s["pf"]:.2f}',
                f'{s["pnl_m"]:+,.0f}万',
                f'{s["avg_hold"]:.0f}d',
                f'{delta:+,.0f}万',
            ])

        fixed_html += f"<h3>{rule}（SL=-{sl}%）</h3>"
        fixed_html += _table_html(
            ["保有日数", "N", "WR", "PF", "PnL", "AvgHold", "vs現行"],
            rule_rows, highlight_col=4,
        )

        # Chart
        labels = [f"{n}d" for n in fixed_ns]
        pnls = [r["pnl_m"] for r in fixed_results[rule]]
        traces = [{"x": labels, "y": pnls, "type": "bar", "name": "PnL(万)",
                    "marker": {"color": ["rgba(52,211,153,0.7)" if v > 0 else "rgba(248,113,113,0.7)" for v in pnls]}}]
        fixed_html += _plotly_grouped_bar(
            f"fixed_{rule}", traces, title=f"{rule}: 固定保有期間別PnL",
            xaxis_title="保有日数", yaxis_title="PnL(万)",
        )

    sections_html.append(_section("2. 固定保有期間（B1-B3）", fixed_html))

    # ==================== Section 3: Minimum Hold Period ====================
    print("[5/7] Section 3: Minimum hold period simulation...")
    minhold_html = ""
    minhold_html += _insight_box(
        "<b>最低保有期間</b>: 最初のN日間はsignal exitを無視。"
        "N日経過後、Close < entry_price で翌日exit。MFEの蒸発前に脱出しつつ、早すぎるexitを防ぐ。"
    )

    min_ns = [3, 5, 7, 10, 14, 20, 30]
    minhold_results: dict[str, list] = {rule: [] for rule in RULES}

    for rule in ["B1", "B2", "B3"]:
        sub = long[long["rule"] == rule]
        sl = PROPOSED_SLS[rule]
        rule_rows = []

        for n in min_ns:
            s = simulate_all(sub, price_lookup, "min_hold_N", n, sl)
            minhold_results[rule].append(s)
            delta = s["pnl_m"] - baseline_results[rule]["pnl_m"]
            rule_rows.append([
                f'{n}日',
                f'{s["n"]:,}',
                f'{s["wr"]}%',
                f'{s["pf"]:.2f}',
                f'{s["pnl_m"]:+,.0f}万',
                f'{s["avg_hold"]:.0f}d',
                f'{delta:+,.0f}万',
            ])

        minhold_html += f"<h3>{rule}（SL=-{sl}%）</h3>"
        minhold_html += _table_html(
            ["最低保有", "N", "WR", "PF", "PnL", "AvgHold", "vs現行"],
            rule_rows, highlight_col=4,
        )

        labels = [f"{n}d" for n in min_ns]
        pnls = [r["pnl_m"] for r in minhold_results[rule]]
        traces = [{"x": labels, "y": pnls, "type": "bar", "name": "PnL(万)",
                    "marker": {"color": ["rgba(52,211,153,0.7)" if v > 0 else "rgba(248,113,113,0.7)" for v in pnls]}}]
        minhold_html += _plotly_grouped_bar(
            f"minhold_{rule}", traces, title=f"{rule}: 最低保有期間別PnL",
            xaxis_title="最低保有日数", yaxis_title="PnL(万)",
        )

    sections_html.append(_section("3. 最低保有期間（B1-B3）", minhold_html))

    # ==================== Section 4: Trailing Stop ====================
    print("[6/7] Section 4: Trailing stop simulation...")
    trail_html = ""
    trail_html += _insight_box(
        "<b>トレーリングストップ</b>: 保有中の最高値からX%下落したらexit。"
        "利益が伸びれば持ち続け、反転したら脱出。MFE捕捉率を直接改善する手法。"
    )

    trail_xs = [2.0, 3.0, 5.0, 7.0, 10.0, 15.0, 20.0]
    trail_results: dict[str, list] = {rule: [] for rule in RULES}

    for rule in ["B1", "B2", "B3"]:
        sub = long[long["rule"] == rule]
        sl = PROPOSED_SLS[rule]
        rule_rows = []

        for x in trail_xs:
            s = simulate_all(sub, price_lookup, "trailing_X", x, sl)
            trail_results[rule].append(s)
            delta = s["pnl_m"] - baseline_results[rule]["pnl_m"]
            rule_rows.append([
                f'{x}%',
                f'{s["n"]:,}',
                f'{s["wr"]}%',
                f'{s["pf"]:.2f}',
                f'{s["pnl_m"]:+,.0f}万',
                f'{s["avg_hold"]:.0f}d',
                f'{delta:+,.0f}万',
            ])

        trail_html += f"<h3>{rule}（SL=-{sl}%）</h3>"
        trail_html += _table_html(
            ["Trail幅", "N", "WR", "PF", "PnL", "AvgHold", "vs現行"],
            rule_rows, highlight_col=4,
        )

        labels = [f"{x}%" for x in trail_xs]
        pnls = [r["pnl_m"] for r in trail_results[rule]]
        traces = [{"x": labels, "y": pnls, "type": "bar", "name": "PnL(万)",
                    "marker": {"color": ["rgba(52,211,153,0.7)" if v > 0 else "rgba(248,113,113,0.7)" for v in pnls]}}]
        trail_html += _plotly_grouped_bar(
            f"trail_{rule}", traces, title=f"{rule}: トレーリングストップ幅別PnL",
            xaxis_title="Trail幅", yaxis_title="PnL(万)",
        )

    sections_html.append(_section("4. トレーリングストップ（B1-B3）", trail_html))

    # ==================== Section 5: Best Strategy Comparison ====================
    print("[7/7] Section 5: Best strategy comparison...")
    best_html = ""

    for rule in ["B1", "B2", "B3"]:
        # Find best from each category
        base = baseline_results[rule]

        best_fixed_idx = max(range(len(fixed_results[rule])),
                             key=lambda i: fixed_results[rule][i]["pnl_m"])
        best_fixed = fixed_results[rule][best_fixed_idx]
        best_fixed_n = fixed_ns[best_fixed_idx]

        best_minhold_idx = max(range(len(minhold_results[rule])),
                               key=lambda i: minhold_results[rule][i]["pnl_m"])
        best_minhold = minhold_results[rule][best_minhold_idx]
        best_minhold_n = min_ns[best_minhold_idx]

        best_trail_idx = max(range(len(trail_results[rule])),
                             key=lambda i: trail_results[rule][i]["pnl_m"])
        best_trail = trail_results[rule][best_trail_idx]
        best_trail_x = trail_xs[best_trail_idx]

        rows = [
            ["現行（signal exit）", "-",
             f'{base["wr"]}%', f'{base["pf"]:.2f}',
             f'{base["pnl_m"]:+,.0f}万', f'{base["avg_hold"]}d', "-"],
            [f"固定保有 {best_fixed_n}日", f"fixed_{best_fixed_n}d",
             f'{best_fixed["wr"]}%', f'{best_fixed["pf"]:.2f}',
             f'{best_fixed["pnl_m"]:+,.0f}万', f'{best_fixed["avg_hold"]}d',
             f'{best_fixed["pnl_m"] - base["pnl_m"]:+,.0f}万'],
            [f"最低保有 {best_minhold_n}日", f"min_hold_{best_minhold_n}d",
             f'{best_minhold["wr"]}%', f'{best_minhold["pf"]:.2f}',
             f'{best_minhold["pnl_m"]:+,.0f}万', f'{best_minhold["avg_hold"]}d',
             f'{best_minhold["pnl_m"] - base["pnl_m"]:+,.0f}万'],
            [f"Trail {best_trail_x}%", f"trail_{best_trail_x}%",
             f'{best_trail["wr"]}%', f'{best_trail["pf"]:.2f}',
             f'{best_trail["pnl_m"]:+,.0f}万', f'{best_trail["avg_hold"]}d',
             f'{best_trail["pnl_m"] - base["pnl_m"]:+,.0f}万'],
        ]

        best_html += f"<h3>{rule}</h3>"
        best_html += _table_html(
            ["戦略", "パラメータ", "WR", "PF", "PnL", "AvgHold", "vs現行"],
            rows, highlight_col=4,
        )

    # B4 reference (TimeCut 13d from Ch4)
    b4_sub = long[long["rule"] == "B4"]
    b4_base = baseline_results["B4"]
    b4_tc13 = simulate_all(b4_sub, price_lookup, "fixed_N", 13, 999.0)
    best_html += "<h3>B4（参考: Ch4 TimeCut 13d）</h3>"
    best_html += _table_html(
        ["戦略", "パラメータ", "WR", "PF", "PnL", "AvgHold", "vs現行"],
        [
            ["現行", "-", f'{b4_base["wr"]}%', f'{b4_base["pf"]:.2f}',
             f'{b4_base["pnl_m"]:+,.0f}万', f'{b4_base["avg_hold"]}d', "-"],
            ["固定13日", "fixed_13d", f'{b4_tc13["wr"]}%', f'{b4_tc13["pf"]:.2f}',
             f'{b4_tc13["pnl_m"]:+,.0f}万', f'{b4_tc13["avg_hold"]}d',
             f'{b4_tc13["pnl_m"] - b4_base["pnl_m"]:+,.0f}万'],
        ], highlight_col=4,
    )

    # Grand total
    total_base = sum(baseline_results[r]["pnl_m"] for r in RULES)

    # Find each rule's best overall strategy
    grand_total_best = 0.0
    grand_detail = []
    for rule in ["B1", "B2", "B3"]:
        base_pnl = baseline_results[rule]["pnl_m"]
        candidates = [
            ("現行", base_pnl),
            (f"固定{fixed_ns[max(range(len(fixed_results[rule])), key=lambda i: fixed_results[rule][i]['pnl_m'])]}d",
             max(r["pnl_m"] for r in fixed_results[rule])),
            (f"最低保有{min_ns[max(range(len(minhold_results[rule])), key=lambda i: minhold_results[rule][i]['pnl_m'])]}d",
             max(r["pnl_m"] for r in minhold_results[rule])),
            (f"Trail{trail_xs[max(range(len(trail_results[rule])), key=lambda i: trail_results[rule][i]['pnl_m'])]}%",
             max(r["pnl_m"] for r in trail_results[rule])),
        ]
        best_name, best_pnl = max(candidates, key=lambda x: x[1])
        grand_total_best += best_pnl
        grand_detail.append((rule, best_name, best_pnl, best_pnl - base_pnl))

    # B4: TimeCut 13d
    grand_total_best += b4_tc13["pnl_m"]
    grand_detail.append(("B4", "固定13d", b4_tc13["pnl_m"], b4_tc13["pnl_m"] - b4_base["pnl_m"]))

    grand_delta = grand_total_best - total_base

    best_html += "<h3>総合: 各ルールのベスト戦略</h3>"
    grand_rows = []
    for rule, name, pnl_val, delta in grand_detail:
        grand_rows.append([rule, name, f'{pnl_val:+,.0f}万', f'{delta:+,.0f}万'])
    grand_rows.append([
        "<b>合計</b>", "",
        f'<b>{grand_total_best:+,.0f}万</b>',
        f'<b>{grand_delta:+,.0f}万</b>',
    ])
    best_html += _table_html(["Rule", "最適戦略", "PnL", "vs現行"], grand_rows)
    best_html += _insight_box(
        f"<b>Exit Rule Redesignにより、12年間の総PnLが{total_base:+,.0f}万 → {grand_total_best:+,.0f}万"
        f"（{grand_delta:+,.0f}万改善）</b>"
    )

    sections_html.append(_section("5. 最適戦略比較", best_html))

    # ==================== Generate HTML ====================
    body = "\n".join(sections_html)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ch5 Exit Rule Redesign — Granville Strategy Verification</title>
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
  line-height: 1.6; padding: 20px;
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
<h1>Chapter 5: Exit Rule Redesign</h1>
<div class="meta">Generated: {now} | Data: {len(long):,} LONG trades | Re-simulated with prices_cleaned.parquet</div>
{body}
</body>
</html>"""

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / "report.html"
    out.write_text(html, encoding="utf-8")
    print(f"\n[OK] Report saved: {out}")
    print(f"  Size: {out.stat().st_size / 1024:.0f} KB")
    print(f"  Done in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
