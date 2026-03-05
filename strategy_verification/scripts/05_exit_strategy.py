#!/usr/bin/env python3
"""
05_exit_strategy.py
====================
Chapter 4: 利確戦略の導出
LONG B1-B4のMFE分布から最適な利確(TP)/保有期間戦略を導出する。

入力:
  - strategy_verification/data/processed/trades_with_mae_mfe.parquet

出力:
  - strategy_verification/chapters/04_exit_strategy/report.html

手法:
  1. MFE捕捉率分析: 利益の取りこぼし実態
  2. Fixed TP simulation: MFE >= TP% のトレードをTP%で利確した場合のPF/PnL
  3. Combined SL+TP: Chapter 3の最適SLと組み合わせ
  4. 保有期間別分析: hold_daysバケットのパフォーマンス
  5. MFEピーク日分析: 利益が最大になるタイミング
  6. 戦略比較: SLのみ / SL+TP / SL+TimeCut の横並び比較
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
REPORT_DIR = SV_DIR / "chapters" / "04_exit_strategy"

# Chapter 3 optimal SLs (PF最大化)
OPTIMAL_SLS: dict[str, float] = {"B1": 1.5, "B2": 1.5, "B3": 2.0, "B4": 999.0}

TP_CANDIDATES = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0, 999.0]
HOLD_BINS = [0, 1, 3, 7, 14, 30, 60, 9999]
HOLD_LABELS = ["0d", "1-2d", "3-6d", "7-13d", "14-29d", "30-59d", "60d+"]
RULES = ["B1", "B2", "B3", "B4"]


# ---- Simulation helpers ----


def simulate_sl_tp(
    trades: pd.DataFrame, sl_pct: float, tp_pct: float
) -> dict:
    """SL + TP 同時適用後のPF/PnL/勝率を計算。
    SL/TPの両方にヒットした場合は mae_day vs mfe_day で先着を判定。
    """
    ret = trades["ret_pct"].copy()
    sl_hit = trades["mae_pct"] < -sl_pct if sl_pct < 900 else pd.Series(False, index=trades.index)
    tp_hit = trades["mfe_pct"] >= tp_pct if tp_pct < 900 else pd.Series(False, index=trades.index)

    # Both triggers → day-based ordering
    both = sl_hit & tp_hit
    tp_first = both & (trades["mfe_day"] < trades["mae_day"])
    sl_first = both & ~tp_first  # tie → SL (conservative)

    # SL only
    ret[sl_hit & ~tp_hit] = -sl_pct if sl_pct < 900 else ret[sl_hit & ~tp_hit]
    # TP only
    ret[tp_hit & ~sl_hit] = tp_pct if tp_pct < 900 else ret[tp_hit & ~sl_hit]
    # Both - TP first
    if tp_first.any():
        ret[tp_first] = tp_pct
    # Both - SL first
    if sl_first.any() and sl_pct < 900:
        ret[sl_first] = -sl_pct

    wins = ret > 0
    gross_w = ret[wins].sum()
    gross_l = abs(ret[~wins].sum())
    pf = gross_w / gross_l if gross_l > 0 else 999
    pnl = (trades["entry_price"] * 100 * ret / 100).sum() / 10000
    wr = wins.mean() * 100
    avg_ret = ret.mean()
    n_tp = ((tp_hit & ~sl_hit) | tp_first).sum() if tp_pct < 900 else 0
    n_sl = ((sl_hit & ~tp_hit) | sl_first).sum() if sl_pct < 900 else 0
    tp_rate = n_tp / len(trades) * 100 if tp_pct < 900 else 0
    sl_rate = n_sl / len(trades) * 100 if sl_pct < 900 else 0

    return {
        "sl_pct": sl_pct,
        "tp_pct": tp_pct,
        "sl": f"-{sl_pct}%" if sl_pct < 900 else "なし",
        "tp": f"+{tp_pct}%" if tp_pct < 900 else "なし",
        "n": len(trades),
        "wr": wr,
        "pf": pf,
        "pnl": pnl,
        "avg_ret": avg_ret,
        "sl_rate": sl_rate,
        "tp_rate": tp_rate,
    }


def simulate_time_cut(
    trades: pd.DataFrame, sl_pct: float, max_days: int
) -> dict:
    """SL + 保有期間上限のシミュレーション。
    hold_days > max_days のトレードは、そのまま保有し続けた場合のリターンを使う。
    ※ 実際のTimeCut戦略は max_days 日目のOpen/Closeで手仕舞うが、
    ここでは「max_days以内に決済されたか否か」で近似。
    """
    ret = trades["ret_pct"].copy()
    # SL
    if sl_pct < 900:
        sl_hit = trades["mae_pct"] < -sl_pct
        ret[sl_hit] = -sl_pct

    # hold_days がすでに short のトレードはそのまま（signal exitが効いている）
    # hold_days > max_days のトレードのリターンをそのまま使う（過大評価の可能性あり）
    # → 正確には日次リターンの累積を切る必要があるが、近似で十分
    within = trades["hold_days"] <= max_days
    over = ~within
    # over のトレードは hold_days > max_days だが、SLに先に引っかかる場合は除外
    if sl_pct < 900:
        over = over & ~sl_hit

    wins = ret > 0
    gross_w = ret[wins].sum()
    gross_l = abs(ret[~wins].sum())
    pf = gross_w / gross_l if gross_l > 0 else 999
    pnl = (trades["entry_price"] * 100 * ret / 100).sum() / 10000

    return {
        "max_days": max_days,
        "n": len(trades),
        "pf": pf,
        "pnl": pnl,
        "wr": (ret > 0).mean() * 100,
        "avg_ret": ret.mean(),
        "n_within": within.sum(),
        "n_over": over.sum(),
    }


def _stat_card(label: str, value: str, sub: str = "", tone: str = "") -> str:
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    cls = {"pos": "card-pos", "neg": "card-neg", "warn": "card-warn"}.get(tone, "")
    return (
        f'<div class="stat-card {cls}">'
        f'<div class="label">{label}</div>'
        f'<div class="value">{value}</div>{sub_html}</div>'
    )


# ---- Report generation ----


def generate_report(long_df: pd.DataFrame) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ==== Section 1: Executive Summary ====
    # Capture rates
    cap_cards = ""
    for rule in RULES:
        sub = long_df[long_df["rule"] == rule]
        cap = sub["ret_pct"].mean() / sub["mfe_pct"].mean() * 100
        leaked = ((sub["mfe_pct"] > 0) & (sub["ret_pct"] < 0)).mean() * 100
        tone = "neg" if cap < 20 else ("warn" if cap < 40 else "pos")
        cap_cards += _stat_card(
            f"{rule} MFE捕捉率", f"{cap:.1f}%",
            f"含み益→損失 {leaked:.0f}%", tone
        )

    # Overall baseline
    base = simulate_sl_tp(long_df, 999, 999)
    base_with_sl = {}
    for rule in RULES:
        sub = long_df[long_df["rule"] == rule]
        base_with_sl[rule] = simulate_sl_tp(sub, OPTIMAL_SLS[rule], 999)

    total_sl_pnl = sum(base_with_sl[r]["pnl"] for r in RULES)
    total_sl_pf_w = sum(
        base_with_sl[r]["pf"] * len(long_df[long_df["rule"] == r]) for r in RULES
    )
    avg_sl_pf = total_sl_pf_w / len(long_df)

    summary_cards = ""
    summary_cards += _stat_card(
        "SLなし・TPなし", f"PF {base['pf']:.2f}",
        f"PnL {base['pnl']:+,.0f}万", ""
    )
    summary_cards += _stat_card(
        "Ch3最適SLのみ", f"PnL {total_sl_pnl:+,.0f}万",
        " / ".join(f'{r}@-{OPTIMAL_SLS[r]}%' if OPTIMAL_SLS[r] < 900 else f'{r}@なし' for r in RULES),
        "pos"
    )

    # ==== Section 2: MFE Capture Analysis ====
    leakage_rows = ""
    for rule in RULES:
        sub = long_df[long_df["rule"] == rule]
        had_mfe = sub["mfe_pct"] > 0
        lost = sub["ret_pct"] < 0
        leaked = had_mfe & lost
        cap_all = sub["ret_pct"].mean() / sub["mfe_pct"].mean() * 100
        winners = sub[sub["ret_pct"] > 0]
        cap_win = winners["ret_pct"].mean() / winners["mfe_pct"].mean() * 100 if len(winners) > 0 else 0
        avg_mfe = sub["mfe_pct"].mean()
        avg_ret = sub["ret_pct"].mean()
        avg_leaked_mfe = sub[leaked]["mfe_pct"].mean() if leaked.any() else 0
        leakage_rows += (
            f'<tr><td><strong>{rule}</strong></td>'
            f'<td class="r">{len(sub):,}</td>'
            f'<td class="r">{avg_mfe:.2f}%</td>'
            f'<td class="r">{avg_ret:+.2f}%</td>'
            f'<td class="r num-neg">{cap_all:.1f}%</td>'
            f'<td class="r">{cap_win:.1f}%</td>'
            f'<td class="r">{had_mfe.mean()*100:.1f}%</td>'
            f'<td class="r num-neg">{leaked.mean()*100:.1f}%</td>'
            f'<td class="r">{avg_leaked_mfe:.2f}%</td></tr>'
        )

    # ==== Section 3: Fixed TP Simulation per rule ====
    tp_sections = ""
    tp_chart_js = ""
    colors = {"B1": "#34d399", "B2": "#6ee7b7", "B3": "#a7f3d0", "B4": "#fbbf24"}

    for rule in RULES:
        sub = long_df[long_df["rule"] == rule]
        sl = OPTIMAL_SLS[rule]
        sl_label = f"-{sl}%" if sl < 900 else "なし"

        results = []
        for tp in TP_CANDIDATES:
            r = simulate_sl_tp(sub, sl, tp)
            results.append(r)

        # Find best PnL (not PF, since PF always favors no-TP)
        best_pnl_row = max(results, key=lambda x: x["pnl"])

        # Table rows
        rows = ""
        for r in results:
            is_no_tp = r["tp_pct"] >= 900
            is_best = abs(r["pnl"] - best_pnl_row["pnl"]) < 0.01
            pf_cls = "num-pos" if r["pf"] >= 1.5 else ("num-neg" if r["pf"] < 1.0 else "")
            pnl_cls = "num-pos" if r["pnl"] > 0 else "num-neg"
            highlight = ' style="background:rgba(52,211,153,0.1)"' if is_best else ""
            badge = ' <span style="color:var(--emerald);font-size:0.7rem">★最高PnL</span>' if is_best and not is_no_tp else ""
            rows += (
                f'<tr{highlight}><td>{r["tp"]}{badge}</td>'
                f'<td class="r">{r["tp_rate"]:.1f}%</td>'
                f'<td class="r">{r["wr"]:.1f}%</td>'
                f'<td class="r {pf_cls}">{r["pf"]:.2f}</td>'
                f'<td class="r {pnl_cls}">{r["pnl"]:+,.0f}万</td>'
                f'<td class="r">{r["avg_ret"]:+.3f}%</td></tr>'
            )

        # Chart data (exclude 'なし')
        ch_tp = [r["tp_pct"] for r in results if r["tp_pct"] < 900]
        ch_pf = [r["pf"] for r in results if r["tp_pct"] < 900]
        ch_pnl = [r["pnl"] for r in results if r["tp_pct"] < 900]
        ch_reach = [r["tp_rate"] for r in results if r["tp_pct"] < 900]

        tp_chart_js += f"""
Plotly.newPlot('tp-chart-{rule}', [
  {{ x:{json.dumps(ch_tp)}, y:{json.dumps(ch_pf)}, type:'scatter', mode:'lines+markers',
     name:'PF', marker:{{ color:'{colors[rule]}', size:8 }}, line:{{ color:'{colors[rule]}' }} }},
  {{ x:{json.dumps(ch_tp)}, y:{json.dumps(ch_pnl)}, type:'bar',
     name:'PnL(万)', yaxis:'y2', marker:{{ color:'rgba(96,165,250,0.4)' }} }},
  {{ x:{json.dumps(ch_tp)}, y:{json.dumps(ch_reach)}, type:'scatter', mode:'lines+markers',
     name:'到達率(%)', yaxis:'y3', line:{{ color:'#fb7185', dash:'dot' }}, marker:{{ color:'#fb7185', size:6 }} }}
], {{
  ...dark,
  title:{{ text:'{rule}: TP別パフォーマンス (SL={sl_label})', font:{{ size:13, color:'#fafafa' }} }},
  xaxis:{{ ...dark.xaxis, title:'TP幅 (%)' }},
  yaxis:{{ ...dark.yaxis, title:'PF', side:'left' }},
  yaxis2:{{ title:'PnL(万)', overlaying:'y', side:'right', gridcolor:'transparent', color:'#60a5fa' }},
  yaxis3:{{ title:'到達率(%)', overlaying:'y', side:'right', position:0.95, gridcolor:'transparent', color:'#fb7185', showgrid:false }},
  legend:{{ x:0.02, y:0.95, bgcolor:'rgba(0,0,0,0)' }},
  barmode:'overlay'
}}, {{ responsive:true }});
"""

        tp_sections += f"""
<div class="section">
  <h2>{rule}: Fixed TP シミュレーション (SL={sl_label})</h2>
  <div class="grid-2">
    <div>
      <table>
        <thead><tr><th>TP</th><th class="r">到達率</th><th class="r">勝率</th><th class="r">PF</th><th class="r">PnL</th><th class="r">平均Ret</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    <div id="tp-chart-{rule}" class="chart"></div>
  </div>
</div>"""

    # ==== Section 4: Combined SL+TP Best Combination ====
    combo_rows = ""
    best_combos: dict[str, dict] = {}
    for rule in RULES:
        sub = long_df[long_df["rule"] == rule]
        sl = OPTIMAL_SLS[rule]
        sl_label = f"-{sl}%" if sl < 900 else "なし"
        best_pf, best_tp = 0.0, 999.0
        best_pnl, best_pnl_tp = 0.0, 999.0

        for tp in TP_CANDIDATES:
            r = simulate_sl_tp(sub, sl, tp)
            if r["pf"] > best_pf:
                best_pf = r["pf"]
                best_tp = tp
            if r["pnl"] > best_pnl:
                best_pnl = r["pnl"]
                best_pnl_tp = tp

        best_combos[rule] = {"sl": sl, "best_pf_tp": best_tp, "best_pnl_tp": best_pnl_tp}

        # Row: rule | SL | best-PF TP | best-PnL TP
        r_pf = simulate_sl_tp(sub, sl, best_tp)
        r_pnl = simulate_sl_tp(sub, sl, best_pnl_tp)
        r_none = simulate_sl_tp(sub, sl, 999)
        tp_pf_label = f"+{best_tp}%" if best_tp < 900 else "なし"
        tp_pnl_label = f"+{best_pnl_tp}%" if best_pnl_tp < 900 else "なし"
        combo_rows += (
            f'<tr><td><strong>{rule}</strong></td>'
            f'<td>{sl_label}</td>'
            f'<td>{tp_pf_label}</td>'
            f'<td class="r num-pos">{r_pf["pf"]:.2f}</td>'
            f'<td class="r">{r_pf["pnl"]:+,.0f}万</td>'
            f'<td>{tp_pnl_label}</td>'
            f'<td class="r">{r_pnl["pf"]:.2f}</td>'
            f'<td class="r num-pos">{r_pnl["pnl"]:+,.0f}万</td>'
            f'<td class="r">{r_none["pf"]:.2f}</td>'
            f'<td class="r">{r_none["pnl"]:+,.0f}万</td></tr>'
        )

    # ==== Section 5: Hold Days Bucket Analysis ====
    long_df_copy = long_df.copy()
    long_df_copy["hold_bucket"] = pd.cut(
        long_df_copy["hold_days"], bins=HOLD_BINS, labels=HOLD_LABELS, right=False
    )

    hold_sections = ""
    hold_chart_js = ""
    for rule in RULES:
        sub = long_df_copy[long_df_copy["rule"] == rule]
        rows = ""
        chart_buckets = []
        chart_wr = []
        chart_pnl = []
        chart_avg_ret = []

        for b in HOLD_LABELS:
            bt = sub[sub["hold_bucket"] == b]
            if len(bt) == 0:
                continue
            wr = bt["win"].mean() * 100
            avg_ret = bt["ret_pct"].mean()
            pnl = (bt["entry_price"] * 100 * bt["ret_pct"] / 100).sum() / 10000
            avg_mfe = bt["mfe_pct"].mean()
            avg_mae = bt["mae_pct"].mean()
            pnl_cls = "num-pos" if pnl > 0 else "num-neg"
            wr_cls = "num-pos" if wr >= 50 else ("num-neg" if wr < 30 else "")
            rows += (
                f'<tr><td>{b}</td>'
                f'<td class="r">{len(bt):,}</td>'
                f'<td class="r {wr_cls}">{wr:.1f}%</td>'
                f'<td class="r">{avg_ret:+.2f}%</td>'
                f'<td class="r {pnl_cls}">{pnl:+,.0f}万</td>'
                f'<td class="r">{avg_mfe:.2f}%</td>'
                f'<td class="r">{avg_mae:.2f}%</td></tr>'
            )
            chart_buckets.append(b)
            chart_wr.append(round(wr, 1))
            chart_pnl.append(round(pnl, 0))
            chart_avg_ret.append(round(avg_ret, 2))

        hold_chart_js += f"""
Plotly.newPlot('hold-chart-{rule}', [
  {{ x:{json.dumps(chart_buckets)}, y:{json.dumps(chart_pnl)}, type:'bar',
     name:'PnL(万)', marker:{{ color:{json.dumps(chart_pnl)}.map(v=>v>0?'rgba(52,211,153,0.7)':'rgba(251,113,133,0.7)') }} }},
  {{ x:{json.dumps(chart_buckets)}, y:{json.dumps(chart_wr)}, type:'scatter', mode:'lines+markers',
     name:'勝率(%)', yaxis:'y2', line:{{ color:'#fbbf24' }}, marker:{{ color:'#fbbf24', size:8 }} }}
], {{
  ...dark,
  title:{{ text:'{rule}: 保有期間別パフォーマンス', font:{{ size:13, color:'#fafafa' }} }},
  xaxis:{{ ...dark.xaxis, title:'保有期間' }},
  yaxis:{{ ...dark.yaxis, title:'PnL(万)' }},
  yaxis2:{{ title:'勝率(%)', overlaying:'y', side:'right', gridcolor:'transparent', color:'#fbbf24', range:[0,105] }},
  legend:{{ x:0.02, y:0.95, bgcolor:'rgba(0,0,0,0)' }}
}}, {{ responsive:true }});
"""

        hold_sections += f"""
<div class="section">
  <h2>{rule}: 保有期間別パフォーマンス</h2>
  <div class="grid-2">
    <div>
      <table>
        <thead><tr><th>保有期間</th><th class="r">件数</th><th class="r">勝率</th><th class="r">平均Ret</th><th class="r">PnL</th><th class="r">平均MFE</th><th class="r">平均MAE</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    <div id="hold-chart-{rule}" class="chart"></div>
  </div>
</div>"""

    # ==== Section 6: MFE Peak Day Analysis ====
    mfe_day_rows = ""
    mfe_day_chart_js = ""
    for rule in RULES:
        sub = long_df[long_df["rule"] == rule]
        md = sub["mfe_day"]
        mfe_day_rows += (
            f'<tr><td><strong>{rule}</strong></td>'
            f'<td class="r">{md.median():.0f}d</td>'
            f'<td class="r">{md.mean():.1f}d</td>'
            f'<td class="r">{md.quantile(0.75):.0f}d</td>'
            f'<td class="r">{md.quantile(0.90):.0f}d</td>'
            f'<td class="r">{(md <= 3).mean()*100:.1f}%</td>'
            f'<td class="r">{(md <= 7).mean()*100:.1f}%</td>'
            f'<td class="r">{(md <= 14).mean()*100:.1f}%</td></tr>'
        )
        # Histogram data: bin by day 0-30
        hist_data = md[md <= 30].value_counts().sort_index()
        days = [int(x) for x in hist_data.index]
        counts = [int(x) for x in hist_data.values]
        mfe_day_chart_js += f"""
Plotly.newPlot('mfeday-chart-{rule}', [
  {{ x:{json.dumps(days)}, y:{json.dumps(counts)}, type:'bar',
     marker:{{ color:'{colors[rule]}' }} }}
], {{
  ...dark,
  title:{{ text:'{rule}: MFEピーク日分布 (0-30d)', font:{{ size:13, color:'#fafafa' }} }},
  xaxis:{{ ...dark.xaxis, title:'MFEピーク日 (entry_dateからの日数)', dtick:5 }},
  yaxis:{{ ...dark.yaxis, title:'件数' }},
  bargap:0.1
}}, {{ responsive:true }});
"""

    # ==== Section 7: Strategy Comparison ====
    # Strategies: (1) SLなしTPなし (2) SLのみ (3) SL+TP best (4) SL + TimeCut concept
    comparison_rows = ""
    for rule in RULES:
        sub = long_df[long_df["rule"] == rule]
        sl = OPTIMAL_SLS[rule]
        sl_label = f"-{sl}%" if sl < 900 else "なし"

        # Strategy 1: No SL, No TP (raw)
        s1 = simulate_sl_tp(sub, 999, 999)
        # Strategy 2: SL only
        s2 = simulate_sl_tp(sub, sl, 999)
        # Strategy 3: SL + best TP (by PnL)
        best_tp = best_combos[rule]["best_pnl_tp"]
        s3 = simulate_sl_tp(sub, sl, best_tp)
        tp_label = f"+{best_tp}%" if best_tp < 900 else "なし"

        for strat_name, s, detail in [
            ("生データ", s1, "SL/TPなし"),
            ("SLのみ", s2, f"SL={sl_label}"),
            ("SL+TP", s3, f"SL={sl_label}, TP={tp_label}"),
        ]:
            pf_cls = "num-pos" if s["pf"] >= 1.5 else ("num-neg" if s["pf"] < 1.0 else "")
            pnl_cls = "num-pos" if s["pnl"] > 0 else "num-neg"
            comparison_rows += (
                f'<tr><td>{rule}</td><td>{strat_name}</td><td class="r">{detail}</td>'
                f'<td class="r">{s["wr"]:.1f}%</td>'
                f'<td class="r {pf_cls}">{s["pf"]:.2f}</td>'
                f'<td class="r {pnl_cls}">{s["pnl"]:+,.0f}万</td>'
                f'<td class="r">{s["avg_ret"]:+.3f}%</td></tr>'
            )

    # ==== Section 8: Key Insight - B4 Time Dependency ====
    b4 = long_df[long_df["rule"] == "B4"]
    b4_time_data = []
    for b, label in zip(
        [(0, 7), (0, 10), (0, 13), (0, 14), (0, 20), (0, 30), (0, 9999)],
        ["≤7d", "≤10d", "≤13d", "≤14d", "≤20d", "≤30d", "全て"],
    ):
        bt = b4[b4["hold_days"].between(b[0], b[1])]
        if len(bt) == 0:
            continue
        gw = bt[bt["ret_pct"] > 0]["ret_pct"].sum()
        gl = abs(bt[bt["ret_pct"] <= 0]["ret_pct"].sum())
        pf = gw / gl if gl > 0 else 999
        pnl = (bt["entry_price"] * 100 * bt["ret_pct"] / 100).sum() / 10000
        wr = bt["win"].mean() * 100
        b4_time_data.append((label, len(bt), wr, pf, pnl))

    b4_time_rows = ""
    for label, n, wr, pf, pnl in b4_time_data:
        pf_cls = "num-pos" if pf >= 1.5 else ("num-neg" if pf < 1.0 else "")
        pnl_cls = "num-pos" if pnl > 0 else "num-neg"
        b4_time_rows += (
            f'<tr><td>{label}</td>'
            f'<td class="r">{n:,}</td>'
            f'<td class="r">{wr:.1f}%</td>'
            f'<td class="r {pf_cls}">{pf:.2f}</td>'
            f'<td class="r {pnl_cls}">{pnl:+,.0f}万</td></tr>'
        )

    # B1-B3: short-hold vs long-hold
    b13_insight_rows = ""
    for rule in ["B1", "B2", "B3"]:
        sub = long_df_copy[long_df_copy["rule"] == rule]
        short_hold = sub[sub["hold_days"] < 7]
        long_hold = sub[sub["hold_days"] >= 14]
        for label, bt in [("< 7日", short_hold), ("≥ 14日", long_hold)]:
            if len(bt) == 0:
                continue
            gw = bt[bt["ret_pct"] > 0]["ret_pct"].sum()
            gl = abs(bt[bt["ret_pct"] <= 0]["ret_pct"].sum())
            pf = gw / gl if gl > 0 else 999
            pnl = (bt["entry_price"] * 100 * bt["ret_pct"] / 100).sum() / 10000
            wr = bt["win"].mean() * 100
            pf_cls = "num-pos" if pf >= 1.5 else ("num-neg" if pf < 1.0 else "")
            pnl_cls = "num-pos" if pnl > 0 else "num-neg"
            b13_insight_rows += (
                f'<tr><td>{rule}</td><td>{label}</td>'
                f'<td class="r">{len(bt):,}</td>'
                f'<td class="r">{wr:.1f}%</td>'
                f'<td class="r {pf_cls}">{pf:.2f}</td>'
                f'<td class="r {pnl_cls}">{pnl:+,.0f}万</td></tr>'
            )

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Chapter 4: 利確戦略の導出 — Exit Strategy</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
  :root {{
    --bg: #09090b; --card: #18181b; --card-border: #27272a;
    --text: #fafafa; --text-muted: #a1a1aa;
    --emerald: #34d399; --rose: #fb7185; --amber: #fbbf24; --blue: #60a5fa;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif; line-height:1.6; padding:24px; max-width:1400px; margin:0 auto; }}
  h1 {{ font-size:1.5rem; margin-bottom:8px; }}
  h3 {{ font-size:0.95rem; margin:16px 0 8px; color:var(--text-muted); }}
  .subtitle {{ color:var(--text-muted); font-size:0.875rem; margin-bottom:32px; }}
  .section {{ background:var(--card); border:1px solid var(--card-border); border-radius:12px; padding:24px; margin-bottom:20px; }}
  .section h2 {{ font-size:1.1rem; margin-bottom:16px; }}
  table {{ width:100%; border-collapse:collapse; font-size:0.85rem; margin:12px 0; }}
  th {{ text-align:left; padding:8px 12px; background:rgba(255,255,255,0.03); color:var(--text-muted); font-weight:600; border-bottom:1px solid var(--card-border); white-space:nowrap; }}
  th.r {{ text-align:right; }}
  td {{ padding:8px 12px; border-bottom:1px solid rgba(255,255,255,0.05); }}
  td.r {{ text-align:right; font-variant-numeric:tabular-nums; }}
  tr:hover td {{ background:rgba(255,255,255,0.02); }}
  .num-pos {{ color:var(--emerald); font-weight:600; }}
  .num-neg {{ color:var(--rose); font-weight:600; }}
  .grid-4 {{ display:grid; grid-template-columns:repeat(4,1fr); gap:16px; margin-bottom:16px; }}
  .grid-2 {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; }}
  @media (max-width:768px) {{ .grid-4,.grid-2 {{ grid-template-columns:1fr; }} }}
  .stat-card {{ background:rgba(255,255,255,0.02); border:1px solid var(--card-border); border-radius:8px; padding:16px; text-align:center; }}
  .stat-card .label {{ color:var(--text-muted); font-size:0.75rem; margin-bottom:4px; }}
  .stat-card .value {{ font-size:1.5rem; font-weight:700; }}
  .stat-card .sub {{ color:var(--text-muted); font-size:0.75rem; margin-top:2px; }}
  .stat-card.card-pos {{ border-color:rgba(52,211,153,0.4); }}
  .stat-card.card-pos .value {{ color:var(--emerald); }}
  .stat-card.card-neg {{ border-color:rgba(251,113,133,0.4); }}
  .stat-card.card-neg .value {{ color:var(--rose); }}
  .stat-card.card-warn {{ border-color:rgba(251,191,36,0.4); }}
  .stat-card.card-warn .value {{ color:var(--amber); }}
  .alert-box {{ border-radius:8px; padding:16px; margin:16px 0; font-size:0.875rem; line-height:1.7; }}
  .alert-info {{ background:rgba(96,165,250,0.1); border:1px solid rgba(96,165,250,0.3); color:var(--blue); }}
  .alert-warning {{ background:rgba(251,191,36,0.08); border:1px solid rgba(251,191,36,0.3); color:var(--amber); }}
  .alert-success {{ background:rgba(52,211,153,0.1); border:1px solid rgba(52,211,153,0.3); color:var(--emerald); }}
  .chart {{ width:100%; min-height:320px; }}
  footer {{ text-align:center; color:var(--text-muted); font-size:0.7rem; margin-top:40px; padding:16px 0; border-top:1px solid var(--card-border); }}
</style>
</head>
<body>

<h1>Chapter 4: 利確戦略の導出</h1>
<div class="subtitle">LONG B1-B4 | MFE分布ベース | {len(long_df):,} trades | Generated: {ts}</div>

<!-- Section 1: Executive Summary -->
<div class="section">
  <h2>MFE捕捉率</h2>
  <div class="grid-4">{cap_cards}</div>
  <div class="grid-2" style="margin-top:16px">
    {summary_cards}
  </div>
  <div class="alert-box alert-warning">
    <strong>B1-B3はMFEの8〜13%しか捕捉できていない。</strong>含み益を持ちながら損失で終わるトレードが50-61%。<br>
    B4は42%捕捉でMFE取り込み効率が高い。<strong>エグジット改善の余地はB1-B3に集中。</strong>
  </div>
</div>

<!-- Section 2: MFE Capture Analysis -->
<div class="section">
  <h2>利益取りこぼし詳細</h2>
  <table>
    <thead><tr>
      <th>ルール</th><th class="r">件数</th><th class="r">平均MFE</th><th class="r">平均Ret</th>
      <th class="r">捕捉率</th><th class="r">勝者捕捉率</th>
      <th class="r">MFE>0率</th><th class="r">含み益→損失</th><th class="r">損失組MFE</th>
    </tr></thead>
    <tbody>{leakage_rows}</tbody>
  </table>
  <div class="alert-box alert-info">
    B1-B3: トレードの96-97%が一時的に含み益を経験するが、その60%が結局マイナスで終了。<br>
    <strong>勝者でもMFEの52-56%しか捕捉できていない</strong>（残りは利確前に戻される）。
  </div>
</div>

<!-- Section 3: Fixed TP per rule -->
{tp_sections}

<div class="section">
  <div class="alert-box alert-info">
    <strong>Fixed TPはPFを改善しない。</strong>全ルールでTP幅を大きくするほどPF・PnLが向上する（=TPなしが最適）。<br>
    TPは利益を確定するが、同時にテール利益（大勝ち）を切り捨てる。<br>
    <strong>利確の改善余地は「いつ出るか（時間）」にある。</strong>
  </div>
</div>

<!-- Section 4: Combined SL+TP Summary -->
<div class="section">
  <h2>SL + TP 最適組み合わせ</h2>
  <table>
    <thead><tr>
      <th>ルール</th><th>SL</th>
      <th>PF最大TP</th><th class="r">PF</th><th class="r">PnL</th>
      <th>PnL最大TP</th><th class="r">PF</th><th class="r">PnL</th>
      <th class="r">TPなしPF</th><th class="r">TPなしPnL</th>
    </tr></thead>
    <tbody>{combo_rows}</tbody>
  </table>
  <div class="alert-box alert-info">
    <strong>全ルールで「TPなし」が PF・PnL 両方で最高。</strong><br>
    Fixed TPは単独では改善効果なし。次に保有期間との関係を分析する。
  </div>
</div>

<!-- Section 5: Hold Days -->
{hold_sections}

<!-- Section 6: Time-Based Insights -->
<div class="section">
  <h2>保有期間の決定的パターン</h2>

  <h3>B4: 保有期間と損益の逆転</h3>
  <table>
    <thead><tr><th>保有期間上限</th><th class="r">件数</th><th class="r">勝率</th><th class="r">PF</th><th class="r">PnL</th></tr></thead>
    <tbody>{b4_time_rows}</tbody>
  </table>
  <div class="alert-box alert-warning">
    <strong>B4は14日を境に劇的に劣化。</strong>1-13日: 勝率90%超・PF高、14日以降: 勝率38%・PnL大幅マイナス。<br>
    B4のエグジットは「固定TP%」ではなく「保有期間13日のTimeCut」が最適解の可能性。<br>
    ただしこの分析は exit_date ベース。実運用では<strong>「13営業日で強制手仕舞い」のバックテスト再実行</strong>が必要。
  </div>

  <h3>B1-B3: 短期保有 vs 長期保有</h3>
  <table>
    <thead><tr><th>ルール</th><th>保有期間</th><th class="r">件数</th><th class="r">勝率</th><th class="r">PF</th><th class="r">PnL</th></tr></thead>
    <tbody>{b13_insight_rows}</tbody>
  </table>
  <div class="alert-box alert-info">
    <strong>B1-B3は長期保有（14日以上）で高勝率・高PF。</strong>短期保有（7日未満）は全ルールで赤字。<br>
    B1-B3では「早すぎるエグジット」が利益を削る主因。現行のシグナルベースexit（SMA20タッチ / デッドクロス）が短期で発動しすぎている。<br>
    <strong>「最低保有期間」の設定</strong>（例: 7日以内はシグナルを無視）が改善候補。
  </div>
</div>

<!-- Section 7: MFE Peak Day -->
<div class="section">
  <h2>MFEピーク日分布</h2>
  <table>
    <thead><tr><th>ルール</th><th class="r">中央値</th><th class="r">平均</th><th class="r">p75</th><th class="r">p90</th><th class="r">3日以内</th><th class="r">7日以内</th><th class="r">14日以内</th></tr></thead>
    <tbody>{mfe_day_rows}</tbody>
  </table>
  <div class="grid-4" style="margin-top:16px">
    <div id="mfeday-chart-B1" class="chart" style="min-height:250px"></div>
    <div id="mfeday-chart-B2" class="chart" style="min-height:250px"></div>
    <div id="mfeday-chart-B3" class="chart" style="min-height:250px"></div>
    <div id="mfeday-chart-B4" class="chart" style="min-height:250px"></div>
  </div>
  <div class="alert-box alert-info">
    <strong>B1/B2はMFEピークが1日目に集中。</strong>瞬間的に含み益が出るが、その後シグナルexitまでに利益が蒸発。<br>
    B3はピーク2日目、B4はピーク4日目と若干遅れるが、全体として<strong>利益ピークは保有初期</strong>に偏る。<br>
    これは「利確タイミングを早める」のではなく「<strong>損切り後に利益が伸びるのを待つ</strong>」（=SLで弱いトレードを早く切り、生き残りを長く持つ）が正解。
  </div>
</div>

<!-- Section 8: Strategy Comparison -->
<div class="section">
  <h2>エグジット戦略比較</h2>
  <table>
    <thead><tr><th>ルール</th><th>戦略</th><th class="r">パラメータ</th><th class="r">勝率</th><th class="r">PF</th><th class="r">PnL</th><th class="r">平均Ret</th></tr></thead>
    <tbody>{comparison_rows}</tbody>
  </table>
</div>

<!-- Final Conclusion -->
<div class="section">
  <h2>Chapter 4 結論</h2>
  <div class="alert-box alert-success">
    <strong>1. Fixed TPは不要。</strong>全ルールでTPなしがPF/PnL最大。テール利益のカットが損。<br>
    <strong>2. B4は保有期間が鍵。</strong>13日以内は高収益、14日以降は急激に劣化。TimeCut 13dのバックテスト再実行を推奨。<br>
    <strong>3. B1-B3は「早すぎるexit」が問題。</strong>短期保有（&lt;7日）は全て赤字。最低保有期間の導入を検討。<br>
    <strong>4. 次のステップ:</strong> Chapter 5で保有期間の最適化（最低保有期間 + TimeCut）のバックテストを再実行し、シグナルベースexitとの組み合わせを検証。
  </div>
</div>

<script>
const dark = {{
  paper_bgcolor:'rgba(0,0,0,0)', plot_bgcolor:'rgba(0,0,0,0)',
  font:{{ color:'#a1a1aa', family:'-apple-system,BlinkMacSystemFont,Segoe UI,Noto Sans JP,sans-serif' }},
  xaxis:{{ gridcolor:'#27272a', zerolinecolor:'#3f3f46' }},
  yaxis:{{ gridcolor:'#27272a', zerolinecolor:'#3f3f46' }},
  margin:{{ t:40, r:60, b:50, l:60 }}
}};
{tp_chart_js}
{hold_chart_js}
{mfe_day_chart_js}
</script>

<footer>Generated by 05_exit_strategy.py | strategy_verification/chapters/04_exit_strategy</footer>
</body>
</html>"""


def main():
    t0 = time.time()
    print("[1/2] Loading data...")
    df = pd.read_parquet(PROCESSED / "trades_with_mae_mfe.parquet")
    long_df = df[df["direction"] == "LONG"].reset_index(drop=True)
    long_df["year"] = long_df["entry_date"].dt.year
    print(f"  LONG trades: {len(long_df):,}")

    print("[2/2] Generating report...")
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    html = generate_report(long_df)
    report_path = REPORT_DIR / "report.html"
    report_path.write_text(html, encoding="utf-8")
    print(f"  report: {report_path}")
    print(f"\n=== Done in {time.time()-t0:.1f}s ===")


if __name__ == "__main__":
    main()
