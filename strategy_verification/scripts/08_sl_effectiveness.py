#!/usr/bin/env python3
"""
08_sl_effectiveness.py
=======================
Chapter 4-2: SL（ストップロス）有用性の包括的検証
全ルール（B1-B4）でSLあり vs なし、SL幅スイープ、SL後回復率、
最適SL（PnLベース）を分析する。

入力:
  - strategy_verification/data/processed/trades_with_mae_mfe.parquet

出力:
  - strategy_verification/chapters/04-2_sl_effectiveness/report.html

分析内容:
  1. 現行SL設定のレビュー
  2. SL幅スイープ（PF/PnL/WR/最大DD）
  3. SL後回復分析（SLに切られたが実際はプラスで終わったトレード）
  4. SLの「コスト」と「ベネフィット」定量化
  5. ルール別最適SL（PnLベース）
  6. B4: SL + TimeCut 13d の相互作用
  7. PnLインパクト: 現行SL vs 提案SL
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
REPORT_DIR = SV_DIR / "chapters" / "04-2_sl_effectiveness"

RULES = ["B1", "B2", "B3", "B4"]

# Chapter 3 optimal SLs (PF最大化)
CURRENT_SLS: dict[str, float] = {"B1": 1.5, "B2": 1.5, "B3": 2.0, "B4": 999.0}

# SL candidates to sweep
SL_CANDIDATES = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 7.0, 10.0, 999.0]


# ---- Simulation helpers ----


def simulate_sl(trades: pd.DataFrame, sl_pct: float) -> dict:
    """SL適用後の統計量を計算"""
    if len(trades) == 0:
        return {"n": 0, "wr": 0, "pf": 0, "avg_ret": 0, "pnl_m": 0, "max_loss": 0, "sl_rate": 0}

    ret = trades["ret_pct"].copy()
    if sl_pct < 900:
        sl_hit = trades["mae_pct"] < -sl_pct
        ret[sl_hit] = -sl_pct
        sl_rate = sl_hit.mean() * 100
    else:
        sl_rate = 0.0

    wins = ret > 0
    gross_w = ret[wins].sum()
    gross_l = abs(ret[~wins].sum())
    pf = gross_w / gross_l if gross_l > 0 else 999
    pnl = (trades["entry_price"] * 100 * ret / 100).sum() / 10000
    max_loss = ret.min()

    # Max drawdown (cumulative PnL series)
    pnl_series = trades["entry_price"] * 100 * ret / 100
    cum = pnl_series.cumsum()
    peak = cum.cummax()
    dd = (cum - peak).min() / 10000  # 万円

    return {
        "n": len(trades),
        "wr": round(wins.mean() * 100, 1),
        "pf": round(pf, 2),
        "avg_ret": round(ret.mean(), 3),
        "pnl_m": round(pnl, 1),
        "max_loss": round(max_loss, 2),
        "sl_rate": round(sl_rate, 1),
        "max_dd_m": round(dd, 1),
    }


def sl_recovery_analysis(trades: pd.DataFrame, sl_pct: float) -> dict:
    """SLに切られたトレードのうち、実際はプラスで終わったものを分析"""
    if sl_pct >= 900 or len(trades) == 0:
        return {"sl_hit_n": 0, "recovered_n": 0, "recovered_pct": 0,
                "lost_profit_m": 0, "sl_saved_m": 0}

    sl_hit = trades["mae_pct"] < -sl_pct
    sl_trades = trades[sl_hit]
    n_sl = len(sl_trades)

    if n_sl == 0:
        return {"sl_hit_n": 0, "recovered_n": 0, "recovered_pct": 0,
                "lost_profit_m": 0, "sl_saved_m": 0}

    # SLに切られたが実際のret_pctはプラス（=SLなしなら利益だった）
    recovered = sl_trades[sl_trades["ret_pct"] > 0]
    n_recovered = len(recovered)

    # SLで失った利益（回復したトレードの実際のret_pct合計）
    lost_profit = (recovered["entry_price"] * 100 * recovered["ret_pct"] / 100).sum() / 10000

    # SLで救われた損失（実際のret_pctが-sl_pctより悪いトレード）
    worse = sl_trades[sl_trades["ret_pct"] < -sl_pct]
    saved = (worse["entry_price"] * 100 * (worse["ret_pct"] - (-sl_pct)) / 100).sum() / 10000
    # savedは負の値（さらに悪い損失を防いだ）→ 絶対値にする
    saved_abs = abs(saved)

    return {
        "sl_hit_n": n_sl,
        "recovered_n": n_recovered,
        "recovered_pct": round(n_recovered / n_sl * 100, 1) if n_sl > 0 else 0,
        "lost_profit_m": round(lost_profit, 1),
        "sl_saved_m": round(saved_abs, 1),
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
                v = float(str(r[highlight_col]).replace("万", "").replace(",", "").replace("+", "").replace("<b>", "").replace("</b>", ""))
            except (ValueError, IndexError):
                v = -999
            vals.append(v)
        if vals:
            best_idx = vals.index(max(vals))

    for i, row in enumerate(rows):
        cls = ' class="best-row"' if i == best_idx else ""
        tds = "".join(f"<td>{c}</td>" for c in row)
        trs.append(f"<tr{cls}>{tds}</tr>")
    return f'<table><thead><tr>{ths}</tr></thead><tbody>{"".join(trs)}</tbody></table>'


def _plotly_line(div_id: str, traces: list[dict], title: str = "",
                 xaxis_title: str = "", yaxis_title: str = "",
                 yaxis2: bool = False, height: int = 350) -> str:
    data = json.dumps(traces)
    layout_dict = {
        "template": "plotly_dark",
        "paper_bgcolor": "transparent",
        "plot_bgcolor": "transparent",
        "margin": {"t": 40, "b": 50, "l": 60, "r": 60 if yaxis2 else 20},
        "font": {"size": 11, "color": "#e2e8f0"},
        "title": {"text": title, "font": {"size": 13}},
        "xaxis": {"title": xaxis_title},
        "yaxis": {"title": yaxis_title},
        "legend": {"x": 0.02, "y": 0.98, "bgcolor": "rgba(0,0,0,0)"},
    }
    if yaxis2:
        layout_dict["yaxis2"] = {
            "title": "PnL(万)", "overlaying": "y", "side": "right",
            "gridcolor": "transparent", "color": "#fbbf24",
        }
    layout = json.dumps(layout_dict)
    return f"""<div id="{div_id}" style="height:{height}px"></div>
<script>Plotly.newPlot("{div_id}",{data},{layout},{{responsive:true}})</script>"""


def _plotly_bar(div_id: str, x: list, y: list, name: str, color: str = "#60a5fa",
                height: int = 280) -> str:
    data = json.dumps([{"x": x, "y": y, "type": "bar", "name": name,
                        "marker": {"color": color}}])
    layout = json.dumps({
        "template": "plotly_dark",
        "paper_bgcolor": "transparent",
        "plot_bgcolor": "transparent",
        "margin": {"t": 30, "b": 40, "l": 50, "r": 20},
        "font": {"size": 11, "color": "#e2e8f0"},
    })
    return f"""<div id="{div_id}" style="height:{height}px"></div>
<script>Plotly.newPlot("{div_id}",{data},{layout},{{responsive:true}})</script>"""


def _insight_box(text: str) -> str:
    return f'<div class="insight-box">{text}</div>'


def _section(title: str, content: str) -> str:
    return f'<section><h2>{title}</h2>{content}</section>'


# ---- Main ----


def main():
    t0 = time.time()
    print("[1/8] Loading data...")
    df = pd.read_parquet(PROCESSED / "trades_with_mae_mfe.parquet")
    long = df[df["direction"] == "LONG"].copy()
    print(f"  LONG trades: {len(long):,}")

    sections_html = []
    colors = {"B1": "#60a5fa", "B2": "#34d399", "B3": "#fbbf24", "B4": "#f87171"}

    # ==================== Section 1: Current SL Overview ====================
    print("[2/8] Section 1: Current SL settings...")
    overview_html = ""
    cards = []
    for rule in RULES:
        sub = long[long["rule"] == rule]
        sl = CURRENT_SLS[rule]
        s = simulate_sl(sub, sl)
        sl_label = f"-{sl}%" if sl < 900 else "なし"
        cards.append(_stat_card(
            f'{rule} (SL={sl_label})',
            f'PF {s["pf"]:.2f}',
            f'N={s["n"]:,} / WR={s["wr"]}% / SL率={s["sl_rate"]}% / PnL={s["pnl_m"]:+,.0f}万',
            "pos" if s["pf"] > 1.5 else ("warn" if s["pf"] > 1.0 else "neg"),
        ))
    overview_html += f'<div class="card-grid">{" ".join(cards)}</div>'
    overview_html += _insight_box(
        "Ch3で最適化されたSL設定。<b>PF最大化</b>で決定されたが、<b>PnL最大化</b>とは一致しない可能性がある。"
        "<br>本章ではSLの有用性をPnL・回復率・最大DDの観点から包括的に検証する。"
    )
    sections_html.append(_section("1. 現行SL設定（Ch3最適値）", overview_html))

    # ==================== Section 2: SL Sweep ====================
    print("[3/8] Section 2: SL Sweep...")
    sweep_html = ""

    # Store best SL by PnL for later use
    best_sl_by_pnl: dict[str, float] = {}

    for rule in RULES:
        sub = long[long["rule"] == rule]
        sweep_rows = []
        pnl_values = []
        pf_values = []
        wr_values = []
        sl_labels_chart = []

        for sl in SL_CANDIDATES:
            s = simulate_sl(sub, sl)
            sl_label = f"-{sl}%" if sl < 900 else "なし"
            pnl_values.append(s["pnl_m"])
            pf_values.append(s["pf"])
            wr_values.append(s["wr"])
            sl_labels_chart.append(sl_label)

            is_current = (sl == CURRENT_SLS[rule])
            marker = " *" if is_current else ""
            sweep_rows.append([
                f'{sl_label}{marker}',
                f'{s["sl_rate"]}%',
                f'{s["wr"]}%',
                f'{s["pf"]:.2f}',
                f'{s["avg_ret"]:+.3f}%',
                f'{s["pnl_m"]:+,.0f}万',
                f'{s["max_loss"]:+.2f}%',
                f'{s["max_dd_m"]:+,.0f}万',
            ])

        # Find best SL by PnL
        best_idx = pnl_values.index(max(pnl_values))
        best_sl_by_pnl[rule] = SL_CANDIDATES[best_idx]

        sweep_html += f"<h3>{rule}（現行SL: {'-' + str(CURRENT_SLS[rule]) + '%' if CURRENT_SLS[rule] < 900 else 'なし'}）</h3>"
        sweep_html += _table_html(
            ["SL幅", "SL率", "WR", "PF", "Avg Ret", "PnL", "最大損失", "最大DD"],
            sweep_rows, highlight_col=5,  # PnL列でハイライト
        )

        # Chart: PF and PnL by SL
        traces = [
            {"x": sl_labels_chart, "y": pf_values, "type": "scatter",
             "mode": "lines+markers", "name": "PF",
             "line": {"color": colors[rule]}, "marker": {"size": 8}},
            {"x": sl_labels_chart, "y": pnl_values, "type": "bar",
             "name": "PnL(万)", "yaxis": "y2",
             "marker": {"color": [
                 "rgba(251,191,36,0.7)" if v == max(pnl_values) else "rgba(96,165,250,0.3)"
                 for v in pnl_values
             ]}},
        ]
        sweep_html += _plotly_line(
            f"sweep_{rule}", traces, title=f"{rule}: SL幅別 PF & PnL",
            xaxis_title="SL幅", yaxis_title="PF", yaxis2=True,
        )

    sweep_html += _insight_box(
        "<b>* = 現行SL（Ch3 PF最大化）</b>。黄色ハイライト行 = PnL最大のSL幅。"
        "<br>PF最大化とPnL最大化が一致しないケースに注目。"
    )
    sections_html.append(_section("2. SL幅スイープ: PF vs PnL", sweep_html))

    # ==================== Section 3: Recovery Analysis ====================
    print("[4/8] Section 3: SL Recovery Analysis...")
    recovery_html = ""
    recovery_html += _insight_box(
        "<b>SL後回復分析</b>: SLに切られたトレードのうち、SLなしなら利益で終わったもの（=SLのコスト）と、"
        "SLで-SL%以上の損失を防いだもの（=SLのベネフィット）を定量化する。"
    )

    for rule in RULES:
        sub = long[long["rule"] == rule]
        recovery_html += f"<h3>{rule}</h3>"

        recovery_rows = []
        for sl in [1.0, 1.5, 2.0, 3.0, 5.0, 7.0]:
            r = sl_recovery_analysis(sub, sl)
            if r["sl_hit_n"] == 0:
                continue
            net = r["sl_saved_m"] - r["lost_profit_m"]
            net_cls = "pos" if net > 0 else "neg"
            recovery_rows.append([
                f'-{sl}%',
                f'{r["sl_hit_n"]:,}',
                f'{r["recovered_n"]:,}',
                f'{r["recovered_pct"]}%',
                f'{r["lost_profit_m"]:+,.0f}万',
                f'{r["sl_saved_m"]:+,.0f}万',
                f'<span style="color:var(--{"pos" if net > 0 else "neg"})">{net:+,.0f}万</span>',
            ])

        recovery_html += _table_html(
            ["SL幅", "SL発動数", "回復した数", "回復率", "失った利益", "防いだ損失", "SL純効果"],
            recovery_rows,
        )

    recovery_html += _insight_box(
        "<b>SL純効果 = 防いだ損失 - 失った利益</b>"
        "<br>正 → SLは利益を守っている（有効）"
        "<br>負 → SLは利益を削っている（有害）"
    )
    sections_html.append(_section("3. SL後回復分析: コスト vs ベネフィット", recovery_html))

    # ==================== Section 4: PF-max vs PnL-max SL ====================
    print("[5/8] Section 4: PF-max vs PnL-max comparison...")
    compare_html = ""
    compare_rows = []

    for rule in RULES:
        sub = long[long["rule"] == rule]
        current_sl = CURRENT_SLS[rule]
        best_pnl_sl = best_sl_by_pnl[rule]

        s_current = simulate_sl(sub, current_sl)
        s_best = simulate_sl(sub, best_pnl_sl)
        s_none = simulate_sl(sub, 999.0)

        curr_label = f'-{current_sl}%' if current_sl < 900 else 'なし'
        best_label = f'-{best_pnl_sl}%' if best_pnl_sl < 900 else 'なし'
        delta = s_best["pnl_m"] - s_current["pnl_m"]

        compare_rows.append([
            rule,
            curr_label,
            f'{s_current["pf"]:.2f}',
            f'{s_current["pnl_m"]:+,.0f}万',
            best_label,
            f'{s_best["pf"]:.2f}',
            f'{s_best["pnl_m"]:+,.0f}万',
            f'{delta:+,.0f}万',
            f'{s_none["pf"]:.2f}',
            f'{s_none["pnl_m"]:+,.0f}万',
        ])

    compare_html += _table_html(
        ["Rule", "現行SL", "PF", "PnL", "PnL最大SL", "PF", "PnL", "差分",
         "SLなしPF", "SLなしPnL"],
        compare_rows,
    )
    compare_html += _insight_box(
        "<b>PF最大SL ≠ PnL最大SL</b>の場合、Ch3の「PF最大化」基準でのSL選定が"
        "PnLを犠牲にしている可能性がある。"
    )
    sections_html.append(_section("4. PF最大SL vs PnL最大SL", compare_html))

    # ==================== Section 5: Year-by-Year SL Impact ====================
    print("[6/8] Section 5: Year-by-year SL impact...")
    long["year"] = long["entry_date"].dt.year
    yearly_html = ""

    for rule in RULES:
        sub = long[long["rule"] == rule]
        current_sl = CURRENT_SLS[rule]
        best_pnl_sl = best_sl_by_pnl[rule]

        yearly_rows = []
        years = sorted(sub["year"].unique())
        chart_years = []
        chart_current = []
        chart_best = []
        chart_none = []

        for year in years:
            ysub = sub[sub["year"] == year]
            s_curr = simulate_sl(ysub, current_sl)
            s_best = simulate_sl(ysub, best_pnl_sl)
            s_none = simulate_sl(ysub, 999.0)

            yearly_rows.append([
                str(year),
                f'{s_curr["pnl_m"]:+,.0f}万',
                f'{s_best["pnl_m"]:+,.0f}万',
                f'{s_none["pnl_m"]:+,.0f}万',
                f'{s_best["pnl_m"] - s_curr["pnl_m"]:+,.0f}万',
            ])
            chart_years.append(str(year))
            chart_current.append(s_curr["pnl_m"])
            chart_best.append(s_best["pnl_m"])
            chart_none.append(s_none["pnl_m"])

        curr_label = f'-{current_sl}%' if current_sl < 900 else 'なし'
        best_label = f'-{best_pnl_sl}%' if best_pnl_sl < 900 else 'なし'

        yearly_html += f"<h3>{rule}</h3>"
        yearly_html += _table_html(
            ["年", f'現行({curr_label})', f'PnL最大({best_label})', 'SLなし', '差分'],
            yearly_rows,
        )

        traces = [
            {"x": chart_years, "y": chart_current, "type": "bar",
             "name": f"現行({curr_label})", "marker": {"color": "rgba(96,165,250,0.7)"}},
            {"x": chart_years, "y": chart_best, "type": "bar",
             "name": f"PnL最大({best_label})", "marker": {"color": "rgba(251,191,36,0.7)"}},
            {"x": chart_years, "y": chart_none, "type": "bar",
             "name": "SLなし", "marker": {"color": "rgba(248,113,113,0.4)"}},
        ]
        layout_extra = json.dumps({
            "template": "plotly_dark",
            "paper_bgcolor": "transparent", "plot_bgcolor": "transparent",
            "margin": {"t": 40, "b": 50, "l": 60, "r": 20},
            "font": {"size": 11, "color": "#e2e8f0"},
            "title": {"text": f"{rule}: 年別PnL比較", "font": {"size": 13}},
            "barmode": "group",
            "xaxis": {"title": "年"},
            "yaxis": {"title": "PnL(万)"},
        })
        yearly_html += f"""<div id="yearly_{rule}" style="height:300px"></div>
<script>Plotly.newPlot("yearly_{rule}",{json.dumps(traces)},{layout_extra},{{responsive:true}})</script>"""

    sections_html.append(_section("5. 年別SL効果: 安定性の検証", yearly_html))

    # ==================== Section 6: B4 SL + TimeCut Interaction ====================
    print("[7/8] Section 6: B4 SL + TimeCut interaction...")
    b4 = long[long["rule"] == "B4"]
    b4_html = ""
    b4_html += _insight_box(
        "B4は現在SLなし。TimeCut 13dとSLを組み合わせた場合の効果を検証する。"
    )

    b4_combo_rows = []
    for sl in [999.0, 3.0, 5.0, 7.0, 10.0]:
        for tc in [999, 13, 20, 30]:
            if tc < 999:
                sub = b4[b4["hold_days"] <= tc]
                tc_label = f"≤{tc}d"
            else:
                sub = b4
                tc_label = "なし"

            s = simulate_sl(sub, sl)
            sl_label = f"-{sl}%" if sl < 900 else "なし"

            b4_combo_rows.append([
                sl_label, tc_label,
                f'{s["n"]:,}',
                f'{s["wr"]}%',
                f'{s["pf"]:.2f}',
                f'{s["pnl_m"]:+,.0f}万',
                f'{s["max_loss"]:+.2f}%',
            ])

    b4_html += _table_html(
        ["SL", "TimeCut", "N", "WR", "PF", "PnL", "最大損失"],
        b4_combo_rows, highlight_col=5,
    )

    b4_html += _insight_box(
        "<b>B4の最適組み合わせ</b>: SL × TimeCut のマトリックスからPnL最大の組み合わせを特定する。"
    )
    sections_html.append(_section("6. B4: SL × TimeCut マトリックス", b4_html))

    # ==================== Section 7: PnL Impact Assessment ====================
    print("[8/8] Section 7: PnL Impact Assessment...")
    impact_html = ""

    # Proposed SL: PnL最大のSL
    total_current = 0.0
    total_proposed = 0.0
    impact_rows = []

    for rule in RULES:
        sub = long[long["rule"] == rule]
        current_sl = CURRENT_SLS[rule]
        proposed_sl = best_sl_by_pnl[rule]

        s_curr = simulate_sl(sub, current_sl)
        s_prop = simulate_sl(sub, proposed_sl)

        delta = s_prop["pnl_m"] - s_curr["pnl_m"]
        total_current += s_curr["pnl_m"]
        total_proposed += s_prop["pnl_m"]

        curr_label = f'-{current_sl}%' if current_sl < 900 else 'なし'
        prop_label = f'-{proposed_sl}%' if proposed_sl < 900 else 'なし'
        change = "変更なし" if current_sl == proposed_sl else f'{curr_label} → {prop_label}'

        impact_rows.append([
            rule, change,
            f'{s_curr["pf"]:.2f}', f'{s_curr["pnl_m"]:+,.0f}万',
            f'{s_prop["pf"]:.2f}', f'{s_prop["pnl_m"]:+,.0f}万',
            f'{delta:+,.0f}万',
        ])

    total_delta = total_proposed - total_current
    impact_rows.append([
        "<b>合計</b>", "", "",
        f'<b>{total_current:+,.0f}万</b>', "",
        f'<b>{total_proposed:+,.0f}万</b>',
        f'<b>{total_delta:+,.0f}万</b>',
    ])

    impact_html += _table_html(
        ["Rule", "SL変更", "現行PF", "現行PnL", "提案PF", "提案PnL", "差分"],
        impact_rows,
    )

    # Verdict
    if total_delta > 0:
        verdict = (
            f"<b>SL最適化により総PnLが{total_delta:+,.0f}万円改善。</b>"
        )
        verdict_cls = "pos"
    elif total_delta == 0:
        verdict = "<b>現行SLが既にPnL最大。変更不要。</b>"
        verdict_cls = "warn"
    else:
        verdict = (
            f"<b>現行SLの方がPnLが高い（差分{total_delta:,.0f}万）。SL変更は不採用。</b>"
        )
        verdict_cls = "neg"

    impact_html += _insight_box(verdict)

    # Ch4 + Ch4-2 combined
    impact_html += "<h3>Ch4（TimeCut） + Ch4-2（SL最適化）統合効果</h3>"
    combined_rows = []
    total_base = 0.0
    total_combined = 0.0

    for rule in RULES:
        sub = long[long["rule"] == rule]
        current_sl = CURRENT_SLS[rule]
        proposed_sl = best_sl_by_pnl[rule]

        # Baseline: current SL, no TimeCut
        s_base = simulate_sl(sub, current_sl)

        # Combined: proposed SL + B4 TimeCut 13d
        if rule == "B4":
            combined_sub = sub[sub["hold_days"] <= 13]
        else:
            combined_sub = sub
        s_comb = simulate_sl(combined_sub, proposed_sl)

        delta = s_comb["pnl_m"] - s_base["pnl_m"]
        total_base += s_base["pnl_m"]
        total_combined += s_comb["pnl_m"]

        combined_rows.append([
            rule,
            f'{s_base["n"]:,} → {s_comb["n"]:,}',
            f'{s_base["pf"]:.2f} → {s_comb["pf"]:.2f}',
            f'{s_base["pnl_m"]:+,.0f}万 → {s_comb["pnl_m"]:+,.0f}万',
            f'{delta:+,.0f}万',
        ])

    total_comb_delta = total_combined - total_base
    combined_rows.append([
        "<b>合計</b>", "", "", "", f'<b>{total_comb_delta:+,.0f}万</b>',
    ])

    impact_html += _table_html(
        ["Rule", "N変化", "PF変化", "PnL変化", "差分"],
        combined_rows,
    )
    impact_html += _insight_box(
        f"<b>Ch4（B4 TimeCut 13d） + Ch4-2（SL PnL最適化）統合: 総PnL差分 = {total_comb_delta:+,.0f}万</b>"
    )

    sections_html.append(_section("7. PnLインパクト: 現行SL vs 提案SL", impact_html))

    # ==================== Generate HTML ====================
    body = "\n".join(sections_html)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ch4-2 SL Effectiveness — Granville Strategy Verification</title>
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
<h1>Ch4-2: SL（ストップロス）有用性の包括的検証</h1>
<div class="meta">Generated: {now} | Data: trades_with_mae_mfe.parquet ({len(long):,} LONG trades)</div>
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
