#!/usr/bin/env python3
"""
07_feature_segmentation.py
===========================
Chapter 4-1: エントリー特徴量によるセグメント分析
LONG B1-B4の各ルールに対し、SMA20乖離率・ATR・vol_ratioでセグメントし、
PF/勝率/平均リターン/最適保有日数の変化を分析する。

入力:
  - strategy_verification/data/processed/trades_with_features.parquet

出力:
  - strategy_verification/chapters/04-1_feature_segmentation/report.html
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
REPORT_DIR = SV_DIR / "chapters" / "04-1_feature_segmentation"

RULES = ["B1", "B2", "B3", "B4"]

# Chapter 3 optimal SLs
OPTIMAL_SLS: dict[str, float] = {"B1": 1.5, "B2": 1.5, "B3": 2.0, "B4": 999.0}

HOLD_BINS = [0, 1, 3, 7, 14, 30, 60, 9999]
HOLD_LABELS = ["0d", "1-2d", "3-6d", "7-13d", "14-29d", "30-59d", "60d+"]


# ---- Segmentation definitions ----

SMA20_BINS = {
    "B1": [(-999, -5), (-5, 0), (0, 5), (5, 15), (15, 999)],
    "B2": [(-999, -5), (-5, 0), (0, 5), (5, 15), (15, 999)],
    "B3": [(-999, -5), (-5, 0), (0, 5), (5, 15), (15, 999)],
    "B4": [(-999, -20), (-20, -15), (-15, -10), (-10, -5), (-5, 999)],
}

ATR_BINS = [(0, 1.5), (1.5, 2.5), (2.5, 4.0), (4.0, 6.0), (6.0, 999)]
ATR_LABELS = ["<1.5%", "1.5-2.5%", "2.5-4%", "4-6%", "6%+"]

VOL_BINS = [(0, 0.5), (0.5, 0.8), (0.8, 1.2), (1.2, 2.0), (2.0, 999)]
VOL_LABELS = ["<0.5x", "0.5-0.8x", "0.8-1.2x", "1.2-2x", "2x+"]


def calc_stats(df: pd.DataFrame, sl_pct: float) -> dict:
    """SL適用後の統計量を計算"""
    if len(df) == 0:
        return {"n": 0, "wr": 0, "pf": 0, "avg_ret": 0, "pnl_m": 0}
    ret = df["ret_pct"].copy()
    if sl_pct < 900:
        sl_hit = df["mae_pct"] < -sl_pct
        ret[sl_hit] = -sl_pct

    wins = ret > 0
    gross_w = ret[wins].sum()
    gross_l = abs(ret[~wins].sum())
    pf = gross_w / gross_l if gross_l > 0 else 999
    pnl = (df["entry_price"] * 100 * ret / 100).sum() / 10000
    return {
        "n": len(df),
        "wr": round(wins.mean() * 100, 1),
        "pf": round(pf, 2),
        "avg_ret": round(ret.mean(), 2),
        "pnl_m": round(pnl, 1),
    }


def hold_day_analysis(df: pd.DataFrame, sl_pct: float) -> list[dict]:
    """保有日数バケット別のPF/勝率"""
    df = df.copy()
    df["hold_bin"] = pd.cut(df["hold_days"], bins=HOLD_BINS, labels=HOLD_LABELS, right=False)
    rows = []
    for label in HOLD_LABELS:
        sub = df[df["hold_bin"] == label]
        if len(sub) == 0:
            continue
        s = calc_stats(sub, sl_pct)
        s["hold_bin"] = label
        rows.append(s)
    return rows


def segment_analysis(df: pd.DataFrame, col: str, bins: list, labels: list, sl_pct: float) -> list[dict]:
    """指定カラムでセグメントし統計量を計算"""
    rows = []
    for (lo, hi), label in zip(bins, labels):
        sub = df[(df[col] >= lo) & (df[col] < hi)]
        s = calc_stats(sub, sl_pct)
        s["segment"] = label
        # 最適保有日数（PF最大のバケット）
        hd = hold_day_analysis(sub, sl_pct)
        if hd:
            best = max(hd, key=lambda x: x["pf"] if x["n"] >= 10 else -1)
            s["best_hold"] = best["hold_bin"]
            s["best_hold_pf"] = best["pf"]
        else:
            s["best_hold"] = "-"
            s["best_hold_pf"] = 0
        rows.append(s)
    return rows


def make_sma20_labels(bins: list) -> list[str]:
    labels = []
    for lo, hi in bins:
        if lo <= -900:
            labels.append(f"<{hi}%")
        elif hi >= 900:
            labels.append(f"{lo}%+")
        else:
            labels.append(f"{lo}~{hi}%")
    return labels


# ---- HTML generation ----


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
    # PF最大行を探す
    best_idx = -1
    if highlight_col is not None:
        vals = []
        for r in rows:
            try:
                v = float(r[highlight_col]) if r[highlight_col] != "-" else -1
            except (ValueError, IndexError):
                v = -1
            vals.append(v)
        if vals:
            best_idx = vals.index(max(vals))

    for i, row in enumerate(rows):
        cls = ' class="best-row"' if i == best_idx else ""
        tds = "".join(f"<td>{c}</td>" for c in row)
        trs.append(f"<tr{cls}>{tds}</tr>")
    return f'<table><thead><tr>{ths}</tr></thead><tbody>{"".join(trs)}</tbody></table>'


def _plotly_bar(div_id: str, x: list, y: list, name: str, color: str = "#60a5fa") -> str:
    data = json.dumps([{"x": x, "y": y, "type": "bar", "name": name,
                        "marker": {"color": color}}])
    layout = json.dumps({
        "template": "plotly_dark",
        "paper_bgcolor": "transparent",
        "plot_bgcolor": "transparent",
        "margin": {"t": 30, "b": 40, "l": 50, "r": 20},
        "font": {"size": 11, "color": "#e2e8f0"},
    })
    return f"""<div id="{div_id}" style="height:280px"></div>
<script>Plotly.newPlot("{div_id}",{data},{layout},{{responsive:true}})</script>"""


def _plotly_grouped_bar(div_id: str, categories: list, traces: list[dict], height: int = 300) -> str:
    data = json.dumps(traces)
    layout = json.dumps({
        "template": "plotly_dark",
        "paper_bgcolor": "transparent",
        "plot_bgcolor": "transparent",
        "margin": {"t": 30, "b": 50, "l": 50, "r": 20},
        "font": {"size": 11, "color": "#e2e8f0"},
        "barmode": "group",
        "xaxis": {"tickangle": -30},
    })
    return f"""<div id="{div_id}" style="height:{height}px"></div>
<script>Plotly.newPlot("{div_id}",{data},{layout},{{responsive:true}})</script>"""


def _plotly_heatmap(div_id: str, x: list, y: list, z: list[list],
                     colorscale: str = "RdYlGn", title: str = "", height: int = 350) -> str:
    """Plotly heatmapを生成"""
    # z内のNaNをNone（JSON null）に変換
    z_clean = []
    for row in z:
        z_clean.append([None if (v is None or (isinstance(v, float) and np.isnan(v))) else v for v in row])
    data = json.dumps([{
        "x": x, "y": y, "z": z_clean, "type": "heatmap",
        "colorscale": colorscale,
        "hoverongaps": False,
        "showscale": True,
        "colorbar": {"tickfont": {"color": "#e2e8f0"}},
    }])
    layout = json.dumps({
        "template": "plotly_dark",
        "paper_bgcolor": "transparent",
        "plot_bgcolor": "transparent",
        "margin": {"t": 40, "b": 60, "l": 80, "r": 20},
        "font": {"size": 11, "color": "#e2e8f0"},
        "title": {"text": title, "font": {"size": 13}},
        "xaxis": {"tickangle": -30},
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
    print("[1/5] Loading data...")
    df = pd.read_parquet(PROCESSED / "trades_with_features.parquet")
    long = df[df["direction"] == "LONG"].copy()
    print(f"  LONG trades: {len(long):,}")

    sections_html = []

    # ==================== Section 1: Overview ====================
    print("[2/5] Section 1: Overview...")
    cards = []
    for rule in RULES:
        sub = long[long["rule"] == rule]
        s = calc_stats(sub, OPTIMAL_SLS[rule])
        cards.append(_stat_card(
            rule, f'PF {s["pf"]:.2f}',
            f'N={s["n"]:,} / WR={s["wr"]:.1f}% / PnL={s["pnl_m"]:+,.0f}万',
            "pos" if s["pf"] > 1.5 else ("warn" if s["pf"] > 1.0 else "neg"),
        ))
    overview_html = f'<div class="card-grid">{" ".join(cards)}</div>'
    overview_html += _insight_box(
        "Ch3最適SL適用後のベースライン。ここから各変数でセグメントし、PF/保有日数に差が出るかを検証する。"
        "<br>変数: SMA20乖離率 / ATR(14) / vol_ratio"
    )
    sections_html.append(_section("1. ベースライン（Ch3 SL適用後）", overview_html))

    # ==================== Section 2: SMA20 Deviation ====================
    print("[3/5] Section 2-3: SMA20 Deviation + ATR...")

    for var_name, var_col, bins_dict_or_list, labels_fn, sec_num, sec_title, interpretation in [
        ("SMA20乖離率", "sma20_dev", SMA20_BINS, make_sma20_labels, 2,
         "SMA20乖離率セグメント",
         "B4（乖離反発系）: SMA20から大きく乖離するほどリターンが高く、最適保有は短期(3-6d)。"
         "<br>B1-B3（GC/トレンド系）: 乖離率はPF/最適保有にほぼ影響しない。"),
        ("ATR(14)", "atr14_pct", ATR_BINS, lambda _: ATR_LABELS, 3,
         "ATR（ボラティリティ）セグメント",
         "B1-B3: 低ボラ（ATR<2.5%）でPFが高い。高ボラ銘柄はSL率が上がりPF低下。"
         "<br>B4: 高ボラ（ATR>4%）で平均リターンが高いが、SLなし前提。"),
        ("vol_ratio", "vol_ratio", VOL_BINS, lambda _: VOL_LABELS, 4,
         "出来高比率セグメント",
         "全ルールで出来高比率のPFへの影響は弱い。有意な差は確認されず、フィルター変数としては不採用。"),
    ]:
        var_html = ""
        # Per-rule segment table + heatmap
        for rule in RULES:
            sub = long[long["rule"] == rule]
            sl = OPTIMAL_SLS[rule]

            if isinstance(bins_dict_or_list, dict):
                bins = bins_dict_or_list[rule]
                labels = labels_fn(bins)
            else:
                bins = bins_dict_or_list
                labels = labels_fn(bins)

            seg_rows = segment_analysis(sub, var_col, bins, labels, sl)

            # Table
            table_rows = []
            for r in seg_rows:
                table_rows.append([
                    r["segment"], r["n"], f'{r["wr"]:.1f}%',
                    f'{r["pf"]:.2f}', f'{r["avg_ret"]:+.2f}%',
                    f'{r["pnl_m"]:+,.0f}万', r["best_hold"], f'{r["best_hold_pf"]:.2f}',
                ])
            var_html += f"<h3>{rule}（SL={OPTIMAL_SLS[rule] if OPTIMAL_SLS[rule] < 900 else 'なし'}）</h3>"
            var_html += _table_html(
                [var_name, "N", "WR", "PF", "Avg Ret", "PnL", "最適Hold", "Hold PF"],
                table_rows, highlight_col=3,
            )

            # Heatmap: segment × hold_bin → PF
            heatmap_x = HOLD_LABELS
            heatmap_y = [r["segment"] for r in seg_rows]
            heatmap_z = []
            for r in seg_rows:
                lo_hi = bins[seg_rows.index(r)]
                row_sub = sub[(sub[var_col] >= lo_hi[0]) & (sub[var_col] < lo_hi[1])]
                hd = hold_day_analysis(row_sub, sl)
                hd_map = {h["hold_bin"]: h["pf"] for h in hd}
                heatmap_z.append([hd_map.get(lb, None) for lb in HOLD_LABELS])

            var_html += _plotly_heatmap(
                f"hm_{var_col}_{rule}", heatmap_x, heatmap_y, heatmap_z,
                title=f"{rule}: {var_name} × 保有日数 → PF",
            )

        var_html += _insight_box(interpretation)
        sections_html.append(_section(f"{sec_num}. {sec_title}", var_html))

    # ==================== Section 5: B4 Deep Dive ====================
    print("[4/5] Section 5: B4 Deep Dive...")
    b4 = long[long["rule"] == "B4"].copy()
    b4_html = ""

    # SMA20 deviation × ATR cross-segment for B4
    sma_bins_b4 = [(-999, -15), (-15, -10), (-10, -5), (-5, 999)]
    sma_labels_b4 = ["<-15%", "-15~-10%", "-10~-5%", "-5%+"]
    atr_bins_b4 = [(0, 2.5), (2.5, 4.0), (4.0, 999)]
    atr_labels_b4 = ["ATR<2.5%", "ATR 2.5-4%", "ATR 4%+"]

    cross_rows = []
    for (slo, shi), slabel in zip(sma_bins_b4, sma_labels_b4):
        for (alo, ahi), alabel in zip(atr_bins_b4, atr_labels_b4):
            sub = b4[(b4["sma20_dev"] >= slo) & (b4["sma20_dev"] < shi) &
                     (b4["atr14_pct"] >= alo) & (b4["atr14_pct"] < ahi)]
            s = calc_stats(sub, 999.0)
            hd = hold_day_analysis(sub, 999.0)
            best_hold = max(hd, key=lambda x: x["pf"] if x["n"] >= 5 else -1)["hold_bin"] if hd else "-"
            cross_rows.append([
                slabel, alabel, s["n"],
                f'{s["wr"]:.1f}%', f'{s["pf"]:.2f}', f'{s["avg_ret"]:+.2f}%',
                f'{s["pnl_m"]:+,.0f}万', best_hold,
            ])

    b4_html += "<h3>B4: SMA20乖離率 × ATR クロスセグメント</h3>"
    b4_html += _table_html(
        ["SMA20乖離", "ATR", "N", "WR", "PF", "Avg Ret", "PnL", "最適Hold"],
        cross_rows, highlight_col=4,
    )

    # B4 hold_day heatmap by SMA20 deviation
    hm_x = HOLD_LABELS
    hm_y = sma_labels_b4
    hm_z = []
    for (slo, shi) in sma_bins_b4:
        sub = b4[(b4["sma20_dev"] >= slo) & (b4["sma20_dev"] < shi)]
        hd = hold_day_analysis(sub, 999.0)
        hd_map = {h["hold_bin"]: round(h["avg_ret"], 2) for h in hd}
        hm_z.append([hd_map.get(lb, None) for lb in HOLD_LABELS])

    b4_html += _plotly_heatmap(
        "hm_b4_sma_hold", hm_x, hm_y, hm_z,
        title="B4: SMA20乖離 × 保有日数 → 平均リターン(%)",
        colorscale="RdYlGn",
    )

    b4_html += _insight_box(
        "<b>B4の最適戦略</b>: SMA20から-15%以上乖離 + ATR高 → 3-6日保有が最も効率的。"
        "<br>浅い乖離（-5%+）はシグナル自体が弱く、PFも低い。"
    )
    sections_html.append(_section("5. B4 Deep Dive: SMA20 × ATR クロス分析", b4_html))

    # ==================== Section 6: Summary ====================
    print("[5/6] Section 6: Summary...")
    summary_rows = []
    for rule in RULES:
        sub = long[long["rule"] == rule]
        sl = OPTIMAL_SLS[rule]
        base = calc_stats(sub, sl)

        # SMA20でのベスト/ワーストセグメント
        if isinstance(SMA20_BINS, dict):
            sma_bins = SMA20_BINS[rule]
        else:
            sma_bins = SMA20_BINS
        sma_labels = make_sma20_labels(sma_bins)
        sma_seg = segment_analysis(sub, "sma20_dev", sma_bins, sma_labels, sl)
        sma_valid = [s for s in sma_seg if s["n"] >= 20]
        sma_best = max(sma_valid, key=lambda x: x["pf"])["segment"] if sma_valid else "-"

        # ATRでのベスト
        atr_seg = segment_analysis(sub, "atr14_pct", ATR_BINS, ATR_LABELS, sl)
        atr_valid = [s for s in atr_seg if s["n"] >= 20]
        atr_best = max(atr_valid, key=lambda x: x["pf"])["segment"] if atr_valid else "-"

        summary_rows.append([
            rule, f'{base["pf"]:.2f}', f'{base["wr"]:.1f}%',
            f'{base["pnl_m"]:+,.0f}万', sma_best, atr_best,
        ])

    summary_html = _table_html(
        ["Rule", "Base PF", "WR", "PnL", "Best SMA20帯", "Best ATR帯"],
        summary_rows,
    )
    summary_html += _insight_box(
        "<b>結論</b>:"
        "<br>・SMA20乖離率: B4のみ有効（深い乖離=高PF）。B1-B3は差が小さい。"
        "<br>・ATR: B1-B3は低ボラが有利。B4は高ボラで高リターン。"
        "<br>・vol_ratio: 全ルールで効果なし → フィルターとして不採用。"
        "<br>・<b>次のステップ</b>: B4はSMA20乖離+ATRで銘柄フィルタリング、B1-B3はATRフィルタリングの導入を検討。"
    )
    sections_html.append(_section("6. まとめ: 変数の有効性", summary_html))

    # ==================== Section 7: PnL Impact Assessment ====================
    print("[6/6] Section 7: PnL Impact Assessment...")

    # Define filters per rule based on segmentation findings
    FILTERS: dict[str, list[tuple[str, str, float, float]]] = {
        # (col, label, min, max) — min <= col < max
        "B1": [("atr14_pct", "ATR<2.5%", 0, 2.5)],
        "B2": [("atr14_pct", "ATR<2.5%", 0, 2.5)],
        "B3": [("atr14_pct", "ATR<2.5%", 0, 2.5)],
        "B4": [("sma20_dev", "SMA20乖離<-10%", -999, -10)],
    }

    impact_html = ""
    impact_html += _insight_box(
        "<b>検証</b>: Section 2-5のセグメント分析で高PF帯を特定した。"
        "「その帯のトレードだけに絞ったら、総PnLは増えるか？」を検証する。"
        "<br><b>B1-B3</b>: ATR<2.5%（低ボラ）のみ / <b>B4</b>: SMA20乖離<-10%（深い乖離）のみ"
    )

    impact_rows = []
    total_base_pnl = 0.0
    total_filt_pnl = 0.0

    for rule in RULES:
        sub = long[long["rule"] == rule]
        sl = OPTIMAL_SLS[rule]

        # Baseline
        base = calc_stats(sub, sl)

        # Filtered
        filtered = sub.copy()
        filter_descs = []
        for col, label, lo, hi in FILTERS[rule]:
            filtered = filtered[(filtered[col] >= lo) & (filtered[col] < hi)]
            filter_descs.append(label)
        filt = calc_stats(filtered, sl)

        n_removed = base["n"] - filt["n"]
        delta = filt["pnl_m"] - base["pnl_m"]
        total_base_pnl += base["pnl_m"]
        total_filt_pnl += filt["pnl_m"]

        impact_rows.append([
            rule,
            " + ".join(filter_descs),
            f'{base["n"]:,}',
            f'{filt["n"]:,}',
            f'{n_removed:,} ({n_removed/base["n"]*100:.0f}%)',
            f'{base["pf"]:.2f}',
            f'{filt["pf"]:.2f}',
            f'{base["pnl_m"]:+,.0f}万',
            f'{filt["pnl_m"]:+,.0f}万',
            f'{delta:+,.0f}万',
        ])

    # Total row
    total_delta = total_filt_pnl - total_base_pnl
    impact_rows.append([
        "<b>合計</b>", "", "", "", "",
        "", "",
        f'<b>{total_base_pnl:+,.0f}万</b>',
        f'<b>{total_filt_pnl:+,.0f}万</b>',
        f'<b>{total_delta:+,.0f}万</b>',
    ])

    impact_table = _table_html(
        ["Rule", "フィルター", "Base N", "Filt N", "除外", "Base PF", "Filt PF",
         "Base PnL", "Filt PnL", "差分"],
        impact_rows,
    )

    # Verdict
    if total_delta > 0:
        verdict = (
            f"<b>フィルタリングにより総PnLが{total_delta:+,.0f}万円改善。</b>"
            f"トレード数は減少するが、低品質トレードの除外で利益が増加する。"
        )
    elif total_delta == 0:
        verdict = "<b>フィルタリングによるPnL変化なし。</b>PFは改善するが金額は同等。"
    else:
        verdict = (
            f"<b>フィルタリングにより総PnLが{total_delta:,.0f}万円減少。</b>"
            f"PFは改善するが、除外されたトレードの利益が大きく、金額ベースでは悪化する。"
            f"<br>→ フィルタリングは不採用。高PF帯の情報は「銘柄選定の参考」に留める。"
        )

    impact_html += impact_table
    impact_html += _insight_box(verdict)

    # Ch4 + Ch4-1 combined impact
    # Ch4: B4 TimeCut 13d → need to calculate
    b4_sub = long[long["rule"] == "B4"]
    b4_within13 = b4_sub[b4_sub["hold_days"] <= 13]
    b4_filt_13 = b4_within13[(b4_within13["sma20_dev"] >= -999) & (b4_within13["sma20_dev"] < -10)]

    combined_pnl = 0.0
    combined_html = "<h3>Ch4 + Ch4-1 統合効果</h3>"
    combined_rows = []
    for rule in RULES:
        sub = long[long["rule"] == rule]
        sl = OPTIMAL_SLS[rule]
        base = calc_stats(sub, sl)

        if rule == "B4":
            # Ch4: TimeCut 13d + Ch4-1: SMA20 < -10%
            combined_sub = sub[sub["hold_days"] <= 13].copy()
            for col, label, lo, hi in FILTERS[rule]:
                combined_sub = combined_sub[(combined_sub[col] >= lo) & (combined_sub[col] < hi)]
            comb = calc_stats(combined_sub, sl)
        else:
            # B1-B3: Ch4 no change + Ch4-1 filter
            combined_sub = sub.copy()
            for col, label, lo, hi in FILTERS[rule]:
                combined_sub = combined_sub[(combined_sub[col] >= lo) & (combined_sub[col] < hi)]
            comb = calc_stats(combined_sub, sl)

        delta = comb["pnl_m"] - base["pnl_m"]
        combined_pnl += delta
        combined_rows.append([
            rule,
            f'{base["n"]:,} → {comb["n"]:,}',
            f'{base["pf"]:.2f} → {comb["pf"]:.2f}',
            f'{base["pnl_m"]:+,.0f}万 → {comb["pnl_m"]:+,.0f}万',
            f'{delta:+,.0f}万',
        ])

    combined_rows.append([
        "<b>合計</b>", "", "", "", f'<b>{combined_pnl:+,.0f}万</b>',
    ])
    combined_html += _table_html(
        ["Rule", "N変化", "PF変化", "PnL変化", "差分"],
        combined_rows,
    )
    combined_html += _insight_box(
        f"<b>Ch4（TimeCut 13d） + Ch4-1（特徴量フィルター）統合: 総PnL差分 = {combined_pnl:+,.0f}万</b>"
    )

    impact_html += combined_html
    sections_html.append(_section("7. PnLインパクト: フィルタリングで利益は増えるか？", impact_html))

    # ==================== Generate HTML ====================
    body = "\n".join(sections_html)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ch4-1 Feature Segmentation — Granville Strategy Verification</title>
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
<h1>Ch4-1: エントリー特徴量セグメント分析</h1>
<div class="meta">Generated: {now} | Data: trades_with_features.parquet ({len(long):,} LONG trades)</div>
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
