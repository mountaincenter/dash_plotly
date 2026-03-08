"""
Granville Step1 検証結果 HTML生成スクリプト
3つのparquet（SLなし / SL-3% / SL-5%）を読み込み、
包括的な検証HTMLを出力する。
"""

import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path(__file__).parent
OUT = BASE / "validation.html"

# ── データ読み込み ──
dfs = {
    "なし": pd.read_parquet(BASE / "trades_no_sl.parquet"),
    "-3%": pd.read_parquet(BASE / "trades_sl3.parquet"),
    "-5%": pd.read_parquet(BASE / "trades_sl5.parquet"),
}

SL_LABELS = ["なし", "-3%", "-5%"]
LONG_RULES = ["B1", "B2", "B3", "B4"]
SHORT_RULES = ["S1", "S2", "S3", "S4"]
ALL_RULES = LONG_RULES + SHORT_RULES
REGIMES = ["Uptrend", "Downtrend"]


def calc_stats(df: pd.DataFrame) -> dict:
    """トレード群の統計を計算"""
    n = len(df)
    if n == 0:
        return {"n": 0, "win_rate": 0, "pf": 0, "avg_pnl": 0, "total_pnl_man": 0, "avg_hold": 0}
    wins = df[df["win"]]
    losses = df[~df["win"]]
    gross_profit = wins["pnl"].sum() if len(wins) > 0 else 0
    gross_loss = abs(losses["pnl"].sum()) if len(losses) > 0 else 0
    pf = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
    return {
        "n": n,
        "win_rate": len(wins) / n * 100,
        "pf": round(pf, 2),
        "avg_pnl": int(df["pnl"].mean()),
        "total_pnl_man": round(df["pnl"].sum() / 10000, 1),
        "avg_hold": round(df["hold_days"].mean(), 1),
    }


def pf_color(pf: float) -> str:
    """PFに応じた文字色"""
    if pf < 1.0:
        return "#ff4444"
    elif pf < 1.3:
        return "#ffcc00"
    elif pf < 1.5:
        return "#00cccc"
    elif pf <= 2.0:
        return "#44cc44"
    else:
        return "#00ff88"


def pnl_color(val: float) -> str:
    """PnLに応じた背景色"""
    if val > 0:
        intensity = min(abs(val) / 500, 1.0)
        g = int(40 + 60 * intensity)
        return f"rgba(0,{g},0,0.3)"
    elif val < 0:
        intensity = min(abs(val) / 500, 1.0)
        r = int(40 + 60 * intensity)
        return f"rgba({r},0,0,0.3)"
    return "transparent"


def fmt_num(v, is_pnl=False):
    """数値フォーマット"""
    if isinstance(v, float):
        if abs(v) >= 100:
            return f"{v:,.0f}"
        return f"{v:,.1f}"
    if isinstance(v, (int, np.integer)):
        return f"{v:,}"
    return str(v)


def stats_cells_html(stats: dict) -> str:
    """1つのSLバリアント分のセル群"""
    pf_c = pf_color(stats["pf"])
    pnl_c = "#44cc44" if stats["total_pnl_man"] > 0 else "#ff4444"
    return (
        f'<td class="num">{stats["n"]:,}</td>'
        f'<td class="num">{stats["win_rate"]:.1f}%</td>'
        f'<td class="num" style="color:{pf_c};font-weight:bold">{stats["pf"]:.2f}</td>'
        f'<td class="num">{stats["avg_pnl"]:,}</td>'
        f'<td class="num" style="color:{pnl_c}">{stats["total_pnl_man"]:,.1f}</td>'
        f'<td class="num">{stats["avg_hold"]:.1f}</td>'
    )


# ── Section 1: Executive Summary ──
def build_section1() -> str:
    df3 = dfs["-3%"]
    long_df = df3[df3["direction"] == "LONG"]
    short_df = df3[df3["direction"] == "SHORT"]
    b4_df = df3[df3["rule"] == "B4"]

    long_stats = calc_stats(long_df)
    short_stats = calc_stats(short_df)
    b4_stats = calc_stats(b4_df)

    def card(title, stats, accent):
        pnl_sign = "+" if stats["total_pnl_man"] > 0 else ""
        return f"""
        <div class="card" style="border-left: 4px solid {accent}">
            <div class="card-title" style="color:{accent}">{title}</div>
            <div class="card-value">{pnl_sign}{stats["total_pnl_man"]:,.1f}万円</div>
            <div class="card-detail">
                トレード数: {stats["n"]:,} | 勝率: {stats["win_rate"]:.1f}% | PF: {stats["pf"]:.2f}
            </div>
        </div>"""

    conclusion = """
    <div class="conclusion-box">
        <div class="conclusion-title">結論</div>
        <ul>
            <li><strong>SHORT（S1-S4）は全て不採用</strong>: PF &lt; 1.0、全期間で累計マイナス</li>
            <li><strong>LONG（B1-B4）が有効</strong>: 特にB4（乖離反発）が最も安定</li>
            <li>SL-3%が最適なリスク管理水準（後述のSL比較で確認）</li>
        </ul>
    </div>"""

    return f"""
    <section>
        <h2>1. Executive Summary（SL-3%基準）</h2>
        <div class="cards-row">
            {card("LONG全体（B1-B4）", long_stats, "#44cc44")}
            {card("SHORT全体（S1-S4）", short_stats, "#ff4444")}
            {card("Best Signal: B4", b4_stats, "#00ccff")}
        </div>
        {conclusion}
    </section>"""


# ── Section 2: SL Comparison ──
def build_section2() -> str:
    header = """
    <tr>
        <th rowspan="2">Signal</th>
        <th colspan="6" class="group-header" style="border-bottom:2px solid #444">SLなし</th>
        <th colspan="6" class="group-header" style="border-bottom:2px solid #444">SL -3%</th>
        <th colspan="6" class="group-header" style="border-bottom:2px solid #444">SL -5%</th>
    </tr>
    <tr>""" + (
        '<th>件数</th><th>勝率</th><th>PF</th><th>平均PnL</th><th>合計(万)</th><th>平均日数</th>' * 3
    ) + "</tr>"

    rows = []
    for rule in ALL_RULES:
        row_class = "long-row" if rule.startswith("B") else "short-row"
        cells = f'<td class="rule-cell {row_class}">{rule}</td>'
        for sl in SL_LABELS:
            sub = dfs[sl][dfs[sl]["rule"] == rule]
            stats = calc_stats(sub)
            cells += stats_cells_html(stats)
        rows.append(f"<tr>{cells}</tr>")

    # LONG/SHORT合計行
    for label, rules, cls in [("LONG合計", LONG_RULES, "long-row"), ("SHORT合計", SHORT_RULES, "short-row")]:
        cells = f'<td class="rule-cell {cls}" style="font-weight:bold">{label}</td>'
        for sl in SL_LABELS:
            sub = dfs[sl][dfs[sl]["rule"].isin(rules)]
            stats = calc_stats(sub)
            cells += stats_cells_html(stats)
        rows.append(f'<tr class="total-row">{cells}</tr>')

    return f"""
    <section>
        <h2>2. ストップロス比較（8シグナル x 3バリアント）</h2>
        <div class="table-container">
            <table>
                <thead>{header}</thead>
                <tbody>{"".join(rows)}</tbody>
            </table>
        </div>
    </section>"""


# ── Section 3: Regime Split ──
def build_section3() -> str:
    sections = []
    for sl in SL_LABELS:
        df = dfs[sl]
        header = """
        <tr>
            <th>Signal</th><th>Regime</th>
            <th>件数</th><th>勝率</th><th>PF</th><th>平均PnL</th><th>合計(万)</th><th>平均日数</th>
        </tr>"""
        rows = []
        for rule in LONG_RULES:
            for regime in REGIMES:
                sub = df[(df["rule"] == rule) & (df["regime"] == regime) & (df["direction"] == "LONG")]
                stats = calc_stats(sub)
                is_key = (rule == "B1" and regime == "Uptrend") or (rule == "B4" and regime == "Downtrend")
                bg = "rgba(0,80,0,0.15)" if regime == "Uptrend" else "rgba(80,0,0,0.15)"
                highlight = "border-left: 3px solid #00ff88;" if is_key else ""
                regime_ja = "上昇トレンド" if regime == "Uptrend" else "下降トレンド"
                row = f'<tr style="background:{bg};{highlight}">'
                row += f'<td class="rule-cell long-row">{rule}</td>'
                row += f'<td>{regime_ja}</td>'
                row += stats_cells_html(stats)
                row += "</tr>"
                rows.append(row)

        sections.append(f"""
        <h3>SL {sl}</h3>
        <div class="table-container">
            <table>
                <thead>{header}</thead>
                <tbody>{"".join(rows)}</tbody>
            </table>
        </div>""")

    return f"""
    <section>
        <h2>3. レジーム別分析（LONGシグナルのみ）</h2>
        <p class="note">緑背景 = 上昇トレンド、赤背景 = 下降トレンド。左に緑線 = 重点コンボ（B1×上昇、B4×下降）</p>
        {"".join(sections)}
    </section>"""


# ── Section 4 & 5: Year-by-Year Heatmap ──
def build_yearly_heatmap(direction: str, rules: list, label: str, section_num: int, purpose: str) -> str:
    df = dfs["-3%"]
    sub = df[(df["direction"] == direction) & (df["rule"].isin(rules))]
    years = sorted(sub["year"].unique())

    header = "<tr><th>年</th>"
    for r in rules:
        header += f"<th>{r}</th>"
    header += f"<th style='font-weight:bold'>{label}合計</th></tr>"

    rows = []
    for y in years:
        row = f"<td>{y}</td>"
        year_total = 0
        for r in rules:
            val = sub[(sub["year"] == y) & (sub["rule"] == r)]["pnl"].sum() / 10000
            year_total += val
            bg = pnl_color(val)
            sign = "+" if val > 0 else ""
            row += f'<td class="num" style="background:{bg}">{sign}{val:,.1f}</td>'
        bg = pnl_color(year_total)
        sign = "+" if year_total > 0 else ""
        row += f'<td class="num" style="background:{bg};font-weight:bold">{sign}{year_total:,.1f}</td>'
        rows.append(f"<tr>{row}</tr>")

    # 合計行
    total_row = '<td style="font-weight:bold">全期間</td>'
    grand_total = 0
    for r in rules:
        val = sub[sub["rule"] == r]["pnl"].sum() / 10000
        grand_total += val
        sign = "+" if val > 0 else ""
        total_row += f'<td class="num" style="font-weight:bold">{sign}{val:,.1f}</td>'
    sign = "+" if grand_total > 0 else ""
    total_row += f'<td class="num" style="font-weight:bold">{sign}{grand_total:,.1f}</td>'

    return f"""
    <section>
        <h2>{section_num}. 年別PnLヒートマップ（{label}、SL-3%）</h2>
        <p class="note">{purpose}</p>
        <div class="table-container">
            <table>
                <thead>{header}</thead>
                <tbody>{"".join(rows)}<tr class="total-row">{total_row}</tr></tbody>
            </table>
        </div>
    </section>"""


# ── Section 6: Structural Change Analysis ──
def build_section6() -> str:
    df = dfs["-3%"]
    years = sorted(df["year"].unique())

    combos = [
        ("B4 x 下降トレンド", "B4", "Downtrend"),
        ("B1 x 上昇トレンド", "B1", "Uptrend"),
    ]

    header = "<tr><th>期間</th>"
    for name, _, _ in combos:
        header += f"<th>{name} PF</th><th>トレード数</th>"
    header += "</tr>"

    rows = []
    for i in range(len(years) - 2):
        y_start, y_end = years[i], years[i + 2]
        row = f"<td>{y_start}-{y_end}</td>"
        for _, rule, regime in combos:
            sub = df[
                (df["rule"] == rule)
                & (df["regime"] == regime)
                & (df["direction"] == "LONG")
                & (df["year"] >= y_start)
                & (df["year"] <= y_end)
            ]
            stats = calc_stats(sub)
            pf_c = pf_color(stats["pf"])
            row += f'<td class="num" style="color:{pf_c};font-weight:bold">{stats["pf"]:.2f}</td>'
            row += f'<td class="num">{stats["n"]:,}</td>'
        rows.append(f"<tr>{row}</tr>")

    return f"""
    <section>
        <h2>6. 構造変化分析（ローリング3年PF、SL-3%）</h2>
        <p class="note">エッジが安定しているか、それとも最近の現象かを確認する</p>
        <div class="table-container">
            <table>
                <thead>{header}</thead>
                <tbody>{"".join(rows)}</tbody>
            </table>
        </div>
    </section>"""


# ── Section 7: Conclusions for Step 2 ──
def build_section7() -> str:
    df = dfs["-3%"]
    years = sorted(df["year"].unique())
    recent_5y = [y for y in years if y >= years[-1] - 4]

    results = []
    for rule in ALL_RULES:
        direction = "LONG" if rule.startswith("B") else "SHORT"
        sub_full = df[(df["rule"] == rule) & (df["direction"] == direction)]
        sub_recent = sub_full[sub_full["year"].isin(recent_5y)]
        stats_full = calc_stats(sub_full)
        stats_recent = calc_stats(sub_recent)
        results.append({
            "rule": rule,
            "direction": direction,
            "pf_full": stats_full["pf"],
            "pf_recent": stats_recent["pf"],
            "total_pnl_man": stats_full["total_pnl_man"],
            "survive": stats_full["pf"] > 1.2 and stats_recent["pf"] > 1.2,
        })

    survivors = [r for r in results if r["survive"]]
    dead = [r for r in results if not r["survive"]]

    survivor_items = ""
    for r in survivors:
        survivor_items += f'<li><strong>{r["rule"]}</strong>: 全期間PF={r["pf_full"]:.2f}, 直近5年PF={r["pf_recent"]:.2f}, 累計{r["total_pnl_man"]:+,.1f}万円</li>'

    dead_items = ""
    for r in dead:
        dead_items += f'<li><span style="color:#ff4444">{r["rule"]}</span>: 全期間PF={r["pf_full"]:.2f}, 直近5年PF={r["pf_recent"]:.2f}</li>'

    # レジーム効果の重点コンボ
    regime_findings = []
    for rule, regime, regime_ja in [("B1", "Uptrend", "上昇トレンド"), ("B4", "Downtrend", "下降トレンド")]:
        sub = df[(df["rule"] == rule) & (df["regime"] == regime) & (df["direction"] == "LONG")]
        stats = calc_stats(sub)
        regime_findings.append(f'<li><strong>{rule} x {regime_ja}</strong>: PF={stats["pf"]:.2f}, 累計{stats["total_pnl_man"]:+,.1f}万円</li>')

    return f"""
    <section>
        <h2>7. Step 2への結論</h2>
        <div class="conclusion-box">
            <div class="conclusion-title">生存シグナル（全期間 AND 直近5年 PF &gt; 1.2）</div>
            <ul>{survivor_items}</ul>
        </div>
        <div class="conclusion-box" style="border-left-color:#ff4444">
            <div class="conclusion-title" style="color:#ff4444">不採用シグナル</div>
            <ul>{dead_items}</ul>
        </div>
        <div class="conclusion-box" style="border-left-color:#00ccff">
            <div class="conclusion-title" style="color:#00ccff">レジーム効果 - 重点コンボ</div>
            <ul>{"".join(regime_findings)}</ul>
            <p style="margin-top:12px">レジームフィルターにより、LONGシグナルの精度を大幅に向上可能。
            Step 2ではレジーム条件付きエントリーを検証する。</p>
        </div>
        <div class="conclusion-box" style="border-left-color:#ffcc00">
            <div class="conclusion-title" style="color:#ffcc00">Step 2 検証項目</div>
            <ul>
                <li>生存シグナルのレジーム条件付きエントリー精度</li>
                <li>B1 x 上昇トレンド + B4 x 下降トレンドのポートフォリオ効果</li>
                <li>SL-3%でのドローダウン管理</li>
                <li>セクター別フィルターの有効性</li>
            </ul>
        </div>
    </section>"""


# ── HTML組み立て ──
def build_html() -> str:
    css = """
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
        background: #0a0a0f;
        color: #e0e0e0;
        font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
        font-size: 13px;
        line-height: 1.6;
        padding: 24px 32px;
    }
    h1 {
        font-size: 22px;
        color: #ffffff;
        border-bottom: 2px solid #333;
        padding-bottom: 12px;
        margin-bottom: 8px;
    }
    .subtitle {
        color: #888;
        font-size: 12px;
        margin-bottom: 32px;
    }
    h2 {
        font-size: 16px;
        color: #cccccc;
        margin-top: 40px;
        margin-bottom: 16px;
        border-left: 3px solid #555;
        padding-left: 12px;
    }
    h3 {
        font-size: 14px;
        color: #aaa;
        margin-top: 20px;
        margin-bottom: 8px;
    }
    section {
        margin-bottom: 40px;
    }
    .cards-row {
        display: flex;
        gap: 16px;
        flex-wrap: wrap;
        margin-bottom: 20px;
    }
    .card {
        background: #111118;
        border: 1px solid #2a2a35;
        border-radius: 6px;
        padding: 16px 20px;
        flex: 1;
        min-width: 260px;
    }
    .card-title {
        font-size: 13px;
        font-weight: bold;
        margin-bottom: 8px;
    }
    .card-value {
        font-size: 28px;
        font-weight: bold;
        color: #ffffff;
        margin-bottom: 6px;
    }
    .card-detail {
        font-size: 11px;
        color: #999;
    }
    .conclusion-box {
        background: #111118;
        border: 1px solid #2a2a35;
        border-left: 4px solid #44cc44;
        border-radius: 4px;
        padding: 16px 20px;
        margin-bottom: 16px;
    }
    .conclusion-title {
        font-size: 14px;
        font-weight: bold;
        color: #44cc44;
        margin-bottom: 8px;
    }
    .conclusion-box ul {
        margin-left: 20px;
    }
    .conclusion-box li {
        margin-bottom: 4px;
    }
    .table-container {
        overflow-x: auto;
        margin-bottom: 16px;
    }
    table {
        border-collapse: collapse;
        width: 100%;
        font-size: 12px;
    }
    th {
        background: #16161e;
        color: #aaa;
        padding: 8px 10px;
        text-align: center;
        border: 1px solid #2a2a35;
        font-weight: 600;
        white-space: nowrap;
    }
    .group-header {
        background: #1a1a25;
        color: #ccc;
        font-size: 13px;
    }
    td {
        padding: 6px 10px;
        border: 1px solid #2a2a35;
        white-space: nowrap;
    }
    .num {
        text-align: right;
    }
    .rule-cell {
        font-weight: bold;
        text-align: center;
    }
    .long-row { color: #44cc44; }
    .short-row { color: #ff6666; }
    .total-row {
        background: #16161e;
        border-top: 2px solid #444;
    }
    tr:hover {
        background: rgba(255,255,255,0.03);
    }
    .note {
        color: #777;
        font-size: 11px;
        margin-bottom: 12px;
    }
    """

    body = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Granville Step1 検証結果</title>
<style>{css}</style>
</head>
<body>
<h1>Granville 8法則 Step1 検証結果</h1>
<div class="subtitle">
    データ: TOPIX100構成銘柄 | 期間: 1999-2026 |
    SLバリアント: なし / -3% / -5% |
    トレード数: {len(dfs["なし"]):,}（各バリアント同数）|
    生成日: 2026-03-02
</div>
{build_section1()}
{build_section2()}
{build_section3()}
{build_yearly_heatmap("LONG", LONG_RULES, "LONG", 4, "構造変化点の特定: 戦略が機能し始めた/しなくなった時期を確認")}
{build_yearly_heatmap("SHORT", SHORT_RULES, "SHORT", 5, "SHORTがどの時代でも機能しないことを確認")}
{build_section6()}
{build_section7()}
</body>
</html>"""
    return body


if __name__ == "__main__":
    html = build_html()
    OUT.write_text(html, encoding="utf-8")
    print(f"Generated: {OUT}")
    print(f"Size: {OUT.stat().st_size:,} bytes")
