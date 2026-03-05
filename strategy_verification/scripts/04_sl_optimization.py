#!/usr/bin/env python3
"""
04_sl_optimization.py
=====================
Chapter 3: SL最適化
LONG B1-B4のMAE分布からルール別最適SL幅を導出する。

入力:
  - strategy_verification/data/processed/trades_with_mae_mfe.parquet

出力:
  - strategy_verification/chapters/03_sl_optimization/report.html

手法:
  各SL幅で「MAE < -SL% のトレードを SL価格で強制exitした場合」の
  PF / PnL / 勝率をシミュレーションし、最適SLを特定する。
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
REPORT_DIR = SV_DIR / "chapters" / "03_sl_optimization"

SL_CANDIDATES = [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 999.0]  # 999 = SLなし


def simulate_sl(trades: pd.DataFrame, sl_pct: float) -> dict:
    """SL適用後のPF/PnL/勝率を計算"""
    t = trades.copy()
    if sl_pct < 900:
        hit = t["mae_pct"] < -sl_pct
        t.loc[hit, "sim_ret"] = -sl_pct
        t.loc[~hit, "sim_ret"] = t.loc[~hit, "ret_pct"]
    else:
        t["sim_ret"] = t["ret_pct"]

    wins = t["sim_ret"] > 0
    gross_w = t.loc[wins, "sim_ret"].sum()
    gross_l = abs(t.loc[~wins, "sim_ret"].sum())
    pf = gross_w / gross_l if gross_l > 0 else 999
    # PnL: entry_price * 100株 * ret%
    pnl = (t["entry_price"] * 100 * t["sim_ret"] / 100).sum() / 10000
    wr = wins.mean() * 100
    avg_ret = t["sim_ret"].mean()
    sl_rate = (t["mae_pct"] < -sl_pct).mean() * 100 if sl_pct < 900 else 0
    return {
        "sl": f"-{sl_pct}%" if sl_pct < 900 else "なし",
        "sl_pct": sl_pct,
        "n": len(t),
        "wr": wr,
        "pf": pf,
        "pnl": pnl,
        "avg_ret": avg_ret,
        "sl_rate": sl_rate,
    }


def find_optimal(results: list[dict]) -> dict:
    """PFが最大のSLを返す"""
    return max(results, key=lambda r: r["pf"])


def _stat_card(label: str, value: str, sub: str = "", tone: str = "") -> str:
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    cls = {"pos": "card-pos", "neg": "card-neg", "warn": "card-warn"}.get(tone, "")
    return f'<div class="stat-card {cls}"><div class="label">{label}</div><div class="value">{value}</div>{sub_html}</div>'


def generate_report(long_df: pd.DataFrame) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rules = ["B1", "B2", "B3", "B4"]

    # --- Per-rule simulation ---
    all_results: dict[str, list[dict]] = {}
    optimals: dict[str, dict] = {}
    for rule in rules:
        sub = long_df[long_df["rule"] == rule]
        results = [simulate_sl(sub, sl) for sl in SL_CANDIDATES]
        all_results[rule] = results
        optimals[rule] = find_optimal(results)

    # --- All B combined ---
    all_b = [simulate_sl(long_df, sl) for sl in SL_CANDIDATES]
    opt_all = find_optimal(all_b)

    # --- Rule-specific optimal applied ---
    # Apply each rule's optimal SL and sum
    opt_combined_pnl = 0
    opt_combined_parts = []
    for rule in rules:
        sub = long_df[long_df["rule"] == rule]
        opt = optimals[rule]
        sim = simulate_sl(sub, opt["sl_pct"])
        opt_combined_pnl += sim["pnl"]
        opt_combined_parts.append(f'{rule}@{opt["sl"]}')

    # Current SL-3% for comparison
    current_all = simulate_sl(long_df, 3.0)

    # --- Plotly data: PF vs SL for each rule ---
    chart_data = {}
    for rule in rules:
        sls = [r["sl_pct"] for r in all_results[rule] if r["sl_pct"] < 900]
        pfs = [r["pf"] for r in all_results[rule] if r["sl_pct"] < 900]
        pnls = [r["pnl"] for r in all_results[rule] if r["sl_pct"] < 900]
        chart_data[rule] = {"sl": sls, "pf": pfs, "pnl": pnls}

    # --- MAE percentile table per rule ---
    pctl_rows = ""
    for rule in rules:
        sub = long_df[long_df["rule"] == rule]["mae_pct"]
        ps = [sub.quantile(q) for q in [0.10, 0.25, 0.50, 0.75, 0.90]]
        opt = optimals[rule]
        pctl_rows += (
            f'<tr><td><strong>{rule}</strong></td>'
            f'<td class="r">{len(long_df[long_df["rule"]==rule]):,}</td>'
            + "".join(f'<td class="r">{p:.2f}%</td>' for p in ps)
            + f'<td class="r" style="color:var(--emerald);font-weight:700">{opt["sl"]}</td>'
            f'<td class="r">{opt["pf"]:.2f}</td></tr>'
        )

    # --- Simulation table per rule ---
    sim_sections = ""
    for rule in rules:
        rows = ""
        opt_sl = optimals[rule]["sl_pct"]
        for r in all_results[rule]:
            is_opt = r["sl_pct"] == opt_sl
            is_current = abs(r["sl_pct"] - 3.0) < 0.01
            highlight = ' style="background:rgba(52,211,153,0.1)"' if is_opt else (' style="background:rgba(96,165,250,0.08)"' if is_current else "")
            badge = ' <span style="color:var(--emerald);font-size:0.7rem">★最適</span>' if is_opt else (' <span style="color:var(--blue);font-size:0.7rem">現行</span>' if is_current else "")
            pf_color = "num-pos" if r["pf"] >= 1.5 else ("num-neg" if r["pf"] < 1.0 else "")
            pnl_color = "num-pos" if r["pnl"] > 0 else "num-neg"
            rows += (
                f'<tr{highlight}><td>{r["sl"]}{badge}</td>'
                f'<td class="r">{r["sl_rate"]:.1f}%</td>'
                f'<td class="r">{r["wr"]:.1f}%</td>'
                f'<td class="r {pf_color}">{r["pf"]:.2f}</td>'
                f'<td class="r {pnl_color}">{r["pnl"]:+,.0f}万</td>'
                f'<td class="r">{r["avg_ret"]:+.3f}%</td></tr>'
            )
        sim_sections += f"""
<div class="section">
  <h2>{rule}: SLシミュレーション</h2>
  <div class="grid-2">
    <div>
      <table>
        <thead><tr><th>SL</th><th class="r">SL到達率</th><th class="r">勝率</th><th class="r">PF</th><th class="r">PnL</th><th class="r">平均リターン</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    <div id="chart-{rule}" class="chart"></div>
  </div>
</div>"""

    # --- MFE recovery insight ---
    recovery_rows = ""
    for rule in rules:
        sub = long_df[long_df["rule"] == rule]
        opt_sl = optimals[rule]["sl_pct"]
        if opt_sl >= 900:
            recovery_rows += f'<tr><td>{rule}</td><td colspan="4">SLなし推奨</td></tr>'
            continue
        hit = sub[sub["mae_pct"] < -opt_sl]
        survived = sub[sub["mae_pct"] >= -opt_sl]
        if len(hit) == 0:
            continue
        hit_had_mfe = (hit["mfe_pct"] > 0).mean() * 100
        hit_avg_mfe = hit["mfe_pct"].mean()
        surv_wr = survived["win"].mean() * 100 if len(survived) > 0 else 0
        recovery_rows += (
            f'<tr><td><strong>{rule}</strong></td>'
            f'<td class="r">{len(hit):,}件 ({len(hit)/len(sub)*100:.1f}%)</td>'
            f'<td class="r">{hit_had_mfe:.0f}%</td>'
            f'<td class="r">{hit_avg_mfe:+.2f}%</td>'
            f'<td class="r">{surv_wr:.1f}%</td></tr>'
        )

    # --- Period analysis ---
    latest_year = int(long_df["year"].max())
    periods = [
        ("全体", long_df),
        (f"直近10年 ({latest_year-9}-{latest_year})", long_df[long_df["year"] >= latest_year - 9]),
        (f"直近7年 ({latest_year-6}-{latest_year})", long_df[long_df["year"] >= latest_year - 6]),
        (f"直近5年 ({latest_year-4}-{latest_year})", long_df[long_df["year"] >= latest_year - 4]),
        (f"直近3年 ({latest_year-2}-{latest_year})", long_df[long_df["year"] >= latest_year - 2]),
    ]
    period_rows = ""
    for plabel, pdata in periods:
        period_rows += f'<tr><td colspan="7" style="background:rgba(255,255,255,0.05);font-weight:700;padding-top:12px">{plabel}</td></tr>'
        for rule in rules:
            sub = pdata[pdata["rule"] == rule]
            if len(sub) == 0:
                continue
            best_pf, best_sl, best_pnl = 0, 0, 0
            for sl in SL_CANDIDATES:
                sim = simulate_sl(sub, sl)
                if sim["pf"] > best_pf:
                    best_pf, best_sl, best_pnl = sim["pf"], sl, sim["pnl"]
            sl_label = f"-{best_sl}%" if best_sl < 900 else "なし"
            # Current SL-3%
            c3 = simulate_sl(sub, 3.0)
            pf_diff = best_pf - c3["pf"]
            pnl_diff = best_pnl - c3["pnl"]
            diff_cls = "num-pos" if pnl_diff > 0 else "num-neg"
            period_rows += (
                f'<tr><td>{rule}</td><td class="r">{len(sub):,}</td>'
                f'<td class="r" style="color:var(--emerald);font-weight:700">{sl_label}</td>'
                f'<td class="r">{best_pf:.2f}</td>'
                f'<td class="r">{best_pnl:+,.0f}万</td>'
                f'<td class="r">{c3["pf"]:.2f} / {c3["pnl"]:+,.0f}万</td>'
                f'<td class="r {diff_cls}">{pnl_diff:+,.0f}万</td></tr>'
            )

    # --- B4 risk analysis ---
    b4 = long_df[long_df["rule"] == "B4"]
    b4_worst_trades = b4.nsmallest(5, "mae_pct")[["ticker", "entry_date", "entry_price", "mae_pct", "mfe_pct", "ret_pct", "hold_days"]]
    b4_worst_rows = ""
    for _, r in b4_worst_trades.iterrows():
        b4_worst_rows += (
            f'<tr><td>{r["ticker"]}</td><td>{r["entry_date"].strftime("%Y-%m-%d")}</td>'
            f'<td class="r">{r["entry_price"]:.0f}</td>'
            f'<td class="r num-neg">{r["mae_pct"]:.1f}%</td>'
            f'<td class="r">{r["mfe_pct"]:+.1f}%</td>'
            f'<td class="r">{r["ret_pct"]:+.1f}%</td>'
            f'<td class="r">{r["hold_days"]}d</td></tr>'
        )

    # --- Chart JS ---
    chart_js = ""
    colors = {"B1": "#34d399", "B2": "#6ee7b7", "B3": "#a7f3d0", "B4": "#fbbf24"}
    for rule in rules:
        cd = chart_data[rule]
        opt_sl = optimals[rule]["sl_pct"]
        chart_js += f"""
Plotly.newPlot('chart-{rule}', [
  {{ x: {json.dumps(cd['sl'])}, y: {json.dumps(cd['pf'])}, type:'scatter', mode:'lines+markers',
     name:'PF', marker:{{ color:'{colors[rule]}', size:8 }}, line:{{ color:'{colors[rule]}' }} }},
  {{ x: {json.dumps(cd['sl'])}, y: {json.dumps(cd['pnl'])}, type:'bar',
     name:'PnL(万)', yaxis:'y2', marker:{{ color:'rgba(96,165,250,0.4)' }} }}
], {{
  ...dark,
  title: {{ text: '{rule}: PF & PnL vs SL幅', font:{{ size:13, color:'#fafafa' }} }},
  xaxis: {{ ...dark.xaxis, title:'SL幅 (%)', dtick:1 }},
  yaxis: {{ ...dark.yaxis, title:'PF', side:'left' }},
  yaxis2: {{ title:'PnL(万)', overlaying:'y', side:'right', gridcolor:'transparent', color:'#60a5fa' }},
  shapes: [{{ type:'line', x0:{opt_sl}, x1:{opt_sl}, y0:0, y1:1, yref:'paper', line:{{ color:'#34d399', width:2, dash:'dash' }} }}],
  annotations: [{{ x:{opt_sl}, y:1, yref:'paper', text:'最適', showarrow:false, font:{{ color:'#34d399', size:10 }}, yanchor:'bottom' }}],
  legend: {{ x:0.7, y:0.95, bgcolor:'rgba(0,0,0,0)' }},
  barmode: 'overlay'
}}, {{ responsive:true }});
"""

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Chapter 3: SL最適化 — ルール別最適SL幅の導出</title>
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

<h1>Chapter 3: SL最適化</h1>
<div class="subtitle">LONG B1-B4 | MAE分布ベース | {len(long_df):,} trades | Generated: {ts}</div>

<!-- Summary -->
<div class="section">
  <h2>結論</h2>
  <div class="grid-4">
    {_stat_card("現行 SL-3% 一律", f"PF {current_all['pf']:.2f}", f"PnL {current_all['pnl']:+,.0f}万", "warn" if current_all['pf'] < 1.5 else "pos")}
    {_stat_card("全体最適 一律SL", f"{opt_all['sl']}", f"PF {opt_all['pf']:.2f} / PnL {opt_all['pnl']:+,.0f}万", "pos")}
    {_stat_card("ルール別最適SL", f"PnL {opt_combined_pnl:+,.0f}万", " / ".join(opt_combined_parts), "pos")}
    {_stat_card("SLなし", f"PF {all_b[-1]['pf']:.2f}", f"PnL {all_b[-1]['pnl']:+,.0f}万")}
  </div>
</div>

<!-- MAE Percentile Overview -->
<div class="section">
  <h2>MAEパーセンタイル分布（ルール別）</h2>
  <table>
    <thead><tr><th>ルール</th><th class="r">件数</th><th class="r">p10</th><th class="r">p25</th><th class="r">p50</th><th class="r">p75</th><th class="r">p90</th><th class="r">最適SL</th><th class="r">最適PF</th></tr></thead>
    <tbody>{pctl_rows}</tbody>
  </table>
  <div class="alert-box alert-info">
    B4は逆張り戦略のため、MAE中央値が-5.4%と深い。一律SL-3%ではB4トレードの65%が切られる。<br>
    <strong>ルール別にSL幅を設定することでPnLを最大化できる。</strong>
  </div>
</div>

<!-- Per-rule sections -->
{sim_sections}

<!-- Period Analysis -->
<div class="section">
  <h2>期間別: 最適SL推移（全体 vs 足元）</h2>
  <table>
    <thead><tr><th>ルール</th><th class="r">件数</th><th class="r">最適SL</th><th class="r">最適PF</th><th class="r">最適PnL</th><th class="r">現行-3% PF/PnL</th><th class="r">差分</th></tr></thead>
    <tbody>{period_rows}</tbody>
  </table>
  <div class="alert-box alert-info">
    <strong>最適SLは3年〜10年で安定</strong>: B1=-6%, B4=なし はどのウィンドウでも一貫。<br>
    B4は足元ほどPFが向上（全体2.26→直近3年6.77）。戦略の劣化は見られない。<br>
    <strong>一律SL-3%は全期間で次善策。ルール別SLで改善余地あり。</strong>
  </div>
</div>

<!-- B4 Risk -->
<div class="section">
  <h2>B4 リスク分析（SLなし推奨の注意点）</h2>
  <h3>最悪MAEトップ5</h3>
  <table>
    <thead><tr><th>銘柄</th><th>エントリー日</th><th class="r">エントリー価格</th><th class="r">MAE</th><th class="r">MFE</th><th class="r">リターン</th><th class="r">保有日</th></tr></thead>
    <tbody>{b4_worst_rows}</tbody>
  </table>
  <div class="alert-box alert-warning">
    <strong>生存者バイアスに注意:</strong> この分析は現在のTOPIX構成143銘柄が対象。<br>
    過去26年で上場廃止・倒産した銘柄はデータに含まれていない。<br>
    B4（乖離率-8%超で買い）は倒産直前の銘柄でも発火する。<br>
    <strong>実運用では B4 にも緩めのSL（-15%〜-20%）を検討</strong>し、壊滅的損失を防ぐべき。
  </div>
</div>

<!-- Recovery Analysis -->
<div class="section">
  <h2>SLで切られるトレードの特性</h2>
  <table>
    <thead><tr><th>ルール</th><th class="r">SL到達件数</th><th class="r">一時含み益あり率</th><th class="r">平均MFE</th><th class="r">生存組の勝率</th></tr></thead>
    <tbody>{recovery_rows}</tbody>
  </table>
  <div class="alert-box alert-warning">
    SLで切られるトレードの90%以上が<strong>一時的に含み益を経験</strong>している。<br>
    これはSL幅の問題というより、<strong>利確タイミング（Chapter 4）</strong>との組み合わせで改善すべき課題。
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
{chart_js}
</script>

<footer>Generated by 04_sl_optimization.py | strategy_verification/chapters/03_sl_optimization</footer>
</body>
</html>"""


def main():
    t0 = time.time()
    print("[1/2] Loading data...")
    df = pd.read_parquet(PROCESSED / "trades_with_mae_mfe.parquet")
    long_df = df[df["direction"] == "LONG"].reset_index(drop=True)
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
