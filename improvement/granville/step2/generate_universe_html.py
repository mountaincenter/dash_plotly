"""Step2 ユニバース比較 HTML生成スクリプト."""

import pandas as pd
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).parent
PARQUET = BASE_DIR / "universe_summary.parquet"
OUTPUT = BASE_DIR / "universe.html"


def pf_color(pf: float) -> str:
    """PF値に応じた色を返す."""
    if pd.isna(pf):
        return "#666"
    if pf < 1.0:
        return "#ff4444"
    if pf < 1.3:
        return "#cccc44"
    if pf < 1.5:
        return "#44cccc"
    if pf < 2.0:
        return "#44cc44"
    return "#00ff66"


def pf_bg_color(pf: float) -> str:
    """ヒートマップ用の背景色を返す."""
    if pd.isna(pf):
        return "#1a1a25"
    if pf < 0.8:
        return "#3a1111"
    if pf < 1.0:
        return "#2a1a11"
    if pf < 1.3:
        return "#2a2a11"
    if pf < 1.5:
        return "#112a2a"
    if pf < 2.0:
        return "#113a11"
    return "#115511"


def fmt_pf(v: float) -> str:
    if pd.isna(v):
        return "-"
    return f"{v:.2f}"


def fmt_pct(v: float) -> str:
    if pd.isna(v):
        return "-"
    return f"{v:.1f}%"


def fmt_int(v) -> str:
    if pd.isna(v):
        return "-"
    return f"{int(v):,}"


def fmt_man(v: float) -> str:
    if pd.isna(v):
        return "-"
    return f"{v:+,.0f}"


def fmt_yen(v: float) -> str:
    if pd.isna(v):
        return "-"
    return f"¥{v:+,.0f}"


def fmt_days(v: float) -> str:
    if pd.isna(v):
        return "-"
    return f"{v:.1f}日"


def get_row(df: pd.DataFrame, **filters) -> pd.Series | None:
    """条件に合致する行を1行返す."""
    mask = pd.Series(True, index=df.index)
    for col, val in filters.items():
        mask &= df[col] == val
    subset = df[mask]
    if len(subset) == 0:
        return None
    return subset.iloc[0]


def generate_html(df: pd.DataFrame) -> str:
    universes = ["Core30", "TOPIX100", "政策銘柄", "Core30+政策銘柄", "全銘柄"]
    price_ranges = ["<5000", "<10000", "<20000", "制限なし"]
    rules = ["B1", "B2", "B3", "B4", "LONG合計"]
    rules_no_total = ["B1", "B2", "B3", "B4"]

    lines: list[str] = []
    a = lines.append

    # --- Head ---
    a("<!DOCTYPE html>")
    a('<html lang="ja">')
    a("<head>")
    a('<meta charset="UTF-8">')
    a('<meta name="viewport" content="width=device-width, initial-scale=1.0">')
    a("<title>Granville Step2 ユニバース比較</title>")
    a("<style>")
    a("* { margin: 0; padding: 0; box-sizing: border-box; }")
    a("body {")
    a("    background: #0a0a0f;")
    a("    color: #e0e0e0;")
    a("    font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;")
    a("    font-size: 13px;")
    a("    line-height: 1.6;")
    a("    padding: 24px 32px;")
    a("}")
    a("h1 { font-size: 22px; color: #fff; border-bottom: 2px solid #333; padding-bottom: 12px; margin-bottom: 8px; }")
    a(".subtitle { color: #888; font-size: 12px; margin-bottom: 32px; }")
    a("h2 { font-size: 16px; color: #ccc; margin-top: 40px; margin-bottom: 16px; border-left: 3px solid #555; padding-left: 12px; }")
    a("h3 { font-size: 14px; color: #aaa; margin-top: 20px; margin-bottom: 8px; }")
    a("section { margin-bottom: 40px; }")
    a(".note { color: #777; font-size: 11px; margin-bottom: 12px; }")
    a(".table-container { overflow-x: auto; margin-bottom: 16px; }")
    a("table { border-collapse: collapse; width: 100%; font-size: 12px; }")
    a("th { background: #16161e; color: #aaa; padding: 8px 10px; text-align: center; border: 1px solid #2a2a35; font-weight: 600; white-space: nowrap; }")
    a("td { padding: 6px 10px; border: 1px solid #2a2a35; white-space: nowrap; }")
    a(".num { text-align: right; }")
    a(".center { text-align: center; }")
    a("tr:hover { background: rgba(255,255,255,0.03); }")
    a(".group-header { background: #1a1a25; color: #ccc; font-size: 13px; }")
    a(".total-row { background: #16161e; border-top: 2px solid #444; font-weight: bold; }")
    a(".conclusion-box { background: #111118; border: 1px solid #2a2a35; border-left: 4px solid #44cc44; border-radius: 4px; padding: 16px 20px; margin-bottom: 16px; }")
    a(".conclusion-title { font-size: 14px; font-weight: bold; color: #44cc44; margin-bottom: 8px; }")
    a(".conclusion-box ul { margin-left: 20px; }")
    a(".conclusion-box li { margin-bottom: 4px; }")
    a(".cards-row { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }")
    a(".card { background: #111118; border: 1px solid #2a2a35; border-radius: 6px; padding: 16px 20px; flex: 1; min-width: 260px; }")
    a(".card-title { font-size: 13px; font-weight: bold; margin-bottom: 8px; }")
    a(".card-value { font-size: 28px; font-weight: bold; color: #fff; margin-bottom: 6px; }")
    a(".card-detail { font-size: 11px; color: #999; }")
    a("</style>")
    a("</head>")
    a("<body>")

    # --- Title ---
    total_trades = len(df)
    a("<h1>Granville 8法則 Step2 ユニバース比較</h1>")
    a(f'<div class="subtitle">')
    a(f"    分析対象: 5ユニバース × 4株価帯 × 3SLバリアント × 5ルール × 3レジーム |")
    a(f"    メインSL: SL-3% | 生成日: {date.today()}")
    a("</div>")

    # ===============================================
    # Section 1: ユニバース比較サマリー
    # ===============================================
    a("<section>")
    a('<h2>1. ユニバース比較サマリー（SL-3%, 株価帯: 制限なし, レジーム: 全体）</h2>')
    a('<p class="note">各ユニバースのルール別パフォーマンス。セル: トレード数 / PF / 平均損益</p>')
    a('<div class="table-container"><table>')
    a("<tr><th>ユニバース</th>")
    for r in rules:
        a(f"<th>{r}</th>")
    a("</tr>")

    for univ in universes:
        is_total = univ == "全銘柄"
        cls = ' class="total-row"' if is_total else ""
        a(f"<tr{cls}>")
        a(f"<td><b>{univ}</b></td>")
        for rule in rules:
            row = get_row(df, sl="SL-3%", universe=univ, price_range="制限なし", rule=rule, regime="全体")
            if row is not None:
                pf = row["pf"]
                color = pf_color(pf)
                a(f'<td class="num">{fmt_int(row["trades"])} / <span style="color:{color}">{fmt_pf(pf)}</span> / {fmt_yen(row["avg_pnl"])}</td>')
            else:
                a('<td class="num">-</td>')
        a("</tr>")
    a("</table></div>")
    a("</section>")

    # ===============================================
    # Section 2: 株価帯の影響
    # ===============================================
    a("<section>")
    a('<h2>2. 株価帯の影響（SL-3%, ユニバース: 全銘柄, レジーム: 全体）</h2>')
    a('<p class="note">株価帯を制限した場合のパフォーマンス変化。高価格株の影響を確認</p>')
    a('<div class="table-container"><table>')
    a("<tr><th>株価帯</th>")
    for r in rules:
        a(f"<th>{r}</th>")
    a("</tr>")

    for pr in price_ranges:
        is_total = pr == "制限なし"
        cls = ' class="total-row"' if is_total else ""
        a(f"<tr{cls}>")
        a(f"<td><b>{pr}</b></td>")
        for rule in rules:
            row = get_row(df, sl="SL-3%", universe="全銘柄", price_range=pr, rule=rule, regime="全体")
            if row is not None:
                pf = row["pf"]
                color = pf_color(pf)
                a(f'<td class="num">{fmt_int(row["trades"])} / <span style="color:{color}">{fmt_pf(pf)}</span> / {fmt_yen(row["avg_pnl"])}</td>')
            else:
                a('<td class="num">-</td>')
        a("</tr>")
    a("</table></div>")
    a("</section>")

    # ===============================================
    # Section 3: ユニバース × 株価帯 クロス集計 (B1 Uptrend)
    # ===============================================
    def render_heatmap_section(section_num: int, title: str, note: str, rule: str, regime: str) -> None:
        a("<section>")
        a(f'<h2>{section_num}. {title}</h2>')
        a(f'<p class="note">{note}</p>')
        a('<div class="table-container"><table>')
        a("<tr><th>ユニバース \\ 株価帯</th>")
        for pr in price_ranges:
            a(f"<th>{pr}</th>")
        a("</tr>")

        for univ in universes:
            a("<tr>")
            a(f"<td><b>{univ}</b></td>")
            for pr in price_ranges:
                row = get_row(df, sl="SL-3%", universe=univ, price_range=pr, rule=rule, regime=regime)
                if row is not None:
                    pf = row["pf"]
                    color = pf_color(pf)
                    bg = pf_bg_color(pf)
                    trades = int(row["trades"]) if not pd.isna(row["trades"]) else 0
                    a(f'<td class="center" style="background:{bg}; color:{color}; font-weight:bold">'
                      f'{fmt_pf(pf)}<br><span style="font-size:10px;color:#888">{trades:,}件</span></td>')
                else:
                    a('<td class="center" style="color:#666">-</td>')
            a("</tr>")
        a("</table></div>")
        a("</section>")

    render_heatmap_section(
        3,
        "ユニバース × 株価帯 クロス集計（B1, Uptrend, SL-3%）",
        "B1（GC系）のUptrend環境でのPFヒートマップ。低価格帯で効率的な組み合わせを探索",
        "B1", "Uptrend"
    )

    # ===============================================
    # Section 4: ユニバース × 株価帯 クロス集計 (B4 Downtrend)
    # ===============================================
    render_heatmap_section(
        4,
        "ユニバース × 株価帯 クロス集計（B4, Downtrend, SL-3%）",
        "B4（乖離反発系）のDowntrend環境でのPFヒートマップ。逆張り戦略の最適組み合わせ",
        "B4", "Downtrend"
    )

    # ===============================================
    # Section 5: ユニバース × 株価帯 クロス集計 (LONG合計)
    # ===============================================
    render_heatmap_section(
        5,
        "ユニバース × 株価帯 クロス集計（LONG合計, 全体, SL-3%）",
        "全ルール合計の総合パフォーマンス。運用全体の最適ユニバース・株価帯",
        "LONG合計", "全体"
    )

    # ===============================================
    # Section 6: レジーム別詳細
    # ===============================================
    a("<section>")
    a('<h2>6. レジーム別詳細（SL-3%, 株価帯: 制限なし）</h2>')
    a('<p class="note">各ユニバースでレジーム効果が維持されるか確認。B1-B4 × Uptrend/Downtrend</p>')

    regimes = ["Uptrend", "Downtrend"]

    for univ in universes:
        a(f"<h3>{univ}</h3>")
        a('<div class="table-container"><table>')
        a("<tr><th>ルール</th><th>レジーム</th><th>トレード数</th><th>勝率</th><th>PF</th><th>平均損益</th><th>合計損益(万)</th><th>平均保有</th></tr>")

        for rule in rules_no_total:
            for i, regime in enumerate(regimes):
                row = get_row(df, sl="SL-3%", universe=univ, price_range="制限なし", rule=rule, regime=regime)
                if row is not None:
                    pf = row["pf"]
                    color = pf_color(pf)
                    rule_label = rule if i == 0 else ""
                    a("<tr>")
                    a(f'<td class="center"><b>{rule_label}</b></td>')
                    a(f"<td>{regime}</td>")
                    a(f'<td class="num">{fmt_int(row["trades"])}</td>')
                    a(f'<td class="num">{fmt_pct(row["win_rate"])}</td>')
                    a(f'<td class="num" style="color:{color}">{fmt_pf(pf)}</td>')
                    a(f'<td class="num">{fmt_yen(row["avg_pnl"])}</td>')
                    a(f'<td class="num">{fmt_man(row["total_pnl_man"])}</td>')
                    a(f'<td class="num">{fmt_days(row["avg_hold"])}</td>')
                    a("</tr>")
                else:
                    rule_label = rule if i == 0 else ""
                    a(f"<tr><td class='center'><b>{rule_label}</b></td><td>{regime}</td>" + "<td class='num'>-</td>" * 6 + "</tr>")

        # LONG合計の行
        a('<tr class="total-row">')
        for regime in regimes:
            row = get_row(df, sl="SL-3%", universe=univ, price_range="制限なし", rule="LONG合計", regime=regime)
            if row is not None:
                pf = row["pf"]
                color = pf_color(pf)
                a(f"<tr class='total-row'><td class='center'><b>LONG合計</b></td><td>{regime}</td>")
                a(f'<td class="num">{fmt_int(row["trades"])}</td>')
                a(f'<td class="num">{fmt_pct(row["win_rate"])}</td>')
                a(f'<td class="num" style="color:{color}">{fmt_pf(pf)}</td>')
                a(f'<td class="num">{fmt_yen(row["avg_pnl"])}</td>')
                a(f'<td class="num">{fmt_man(row["total_pnl_man"])}</td>')
                a(f'<td class="num">{fmt_days(row["avg_hold"])}</td>')
                a("</tr>")

        a("</table></div>")

    a("</section>")

    # ===============================================
    # Section 7: SL比較 (TOPIX100)
    # ===============================================
    a("<section>")
    a('<h2>7. SL比較（TOPIX100, 株価帯: 制限なし, レジーム: 全体）</h2>')
    a('<p class="note">ストップロス設定の比較。SLなし / SL-3% / SL-5% のパフォーマンス差</p>')

    sl_variants = ["SLなし", "SL-3%", "SL-5%"]

    a('<div class="table-container"><table>')
    a("<tr><th>SL</th><th>ルール</th><th>トレード数</th><th>勝率</th><th>PF</th><th>平均損益</th><th>合計損益(万)</th><th>平均保有</th><th>SL発動率</th></tr>")

    for sl in sl_variants:
        for i, rule in enumerate(rules):
            row = get_row(df, sl=sl, universe="TOPIX100", price_range="制限なし", rule=rule, regime="全体")
            is_total = rule == "LONG合計"
            cls = ' class="total-row"' if is_total else ""
            sl_label = sl if i == 0 else ""
            if row is not None:
                pf = row["pf"]
                color = pf_color(pf)
                a(f"<tr{cls}>")
                a(f"<td><b>{sl_label}</b></td>")
                a(f"<td><b>{rule}</b></td>")
                a(f'<td class="num">{fmt_int(row["trades"])}</td>')
                a(f'<td class="num">{fmt_pct(row["win_rate"])}</td>')
                a(f'<td class="num" style="color:{color}">{fmt_pf(pf)}</td>')
                a(f'<td class="num">{fmt_yen(row["avg_pnl"])}</td>')
                a(f'<td class="num">{fmt_man(row["total_pnl_man"])}</td>')
                a(f'<td class="num">{fmt_days(row["avg_hold"])}</td>')
                a(f'<td class="num">{fmt_pct(row["sl_rate"])}</td>')
                a("</tr>")
            else:
                a(f"<tr{cls}><td><b>{sl_label}</b></td><td><b>{rule}</b></td>" + '<td class="num">-</td>' * 7 + "</tr>")

        # セパレータ
        if sl != sl_variants[-1]:
            a(f'<tr><td colspan="9" style="background:#0a0a0f; height:4px; border:none;"></td></tr>')

    a("</table></div>")
    a("</section>")

    # ===============================================
    # Section 8: 結論
    # ===============================================
    a("<section>")
    a('<h2>8. Step 2 結論</h2>')

    # 各ユニバースのLONG合計 PFを集める
    univ_results: list[dict] = []
    for univ in universes:
        row = get_row(df, sl="SL-3%", universe=univ, price_range="制限なし", rule="LONG合計", regime="全体")
        if row is not None:
            univ_results.append({
                "universe": univ,
                "pf": row["pf"],
                "total_pnl_man": row["total_pnl_man"],
                "trades": row["trades"],
                "avg_pnl": row["avg_pnl"],
            })

    # PF最高ユニバース
    best_pf_univ = max(univ_results, key=lambda x: x["pf"]) if univ_results else None
    # 合計損益最大ユニバース
    best_pnl_univ = max(univ_results, key=lambda x: x["total_pnl_man"]) if univ_results else None

    # 株価帯の影響 - 全銘柄LONG合計で比較
    price_results: list[dict] = []
    for pr in price_ranges:
        row = get_row(df, sl="SL-3%", universe="全銘柄", price_range=pr, rule="LONG合計", regime="全体")
        if row is not None:
            price_results.append({
                "price_range": pr,
                "pf": row["pf"],
                "avg_pnl": row["avg_pnl"],
                "total_pnl_man": row["total_pnl_man"],
            })

    a('<div class="conclusion-box">')
    a('<div class="conclusion-title">資金制約下の最適ユニバース</div>')
    a("<ul>")
    if best_pf_univ:
        a(f'<li>PF最高: <b>{best_pf_univ["universe"]}</b>（PF {fmt_pf(best_pf_univ["pf"])}, 平均損益 {fmt_yen(best_pf_univ["avg_pnl"])}）</li>')
    # 小型ユニバース（Core30）の結果
    core30 = next((x for x in univ_results if x["universe"] == "Core30"), None)
    if core30:
        a(f'<li>Core30: PF {fmt_pf(core30["pf"])}, {fmt_int(core30["trades"])}トレード, 合計 {fmt_man(core30["total_pnl_man"])}万</li>')
    a("</ul>")
    a("</div>")

    a('<div class="conclusion-box">')
    a('<div class="conclusion-title">最大PnLのユニバース</div>')
    a("<ul>")
    if best_pnl_univ:
        a(f'<li>合計損益最大: <b>{best_pnl_univ["universe"]}</b>（合計 {fmt_man(best_pnl_univ["total_pnl_man"])}万, PF {fmt_pf(best_pnl_univ["pf"])}）</li>')
    topix100 = next((x for x in univ_results if x["universe"] == "TOPIX100"), None)
    if topix100:
        a(f'<li>TOPIX100: 合計 {fmt_man(topix100["total_pnl_man"])}万, PF {fmt_pf(topix100["pf"])}, {fmt_int(topix100["trades"])}トレード</li>')
    a("</ul>")
    a("</div>")

    a('<div class="conclusion-box">')
    a('<div class="conclusion-title">株価帯の影響</div>')
    a("<ul>")
    for pr_data in price_results:
        a(f'<li>{pr_data["price_range"]}: PF {fmt_pf(pr_data["pf"])}, 平均損益 {fmt_yen(pr_data["avg_pnl"])}, 合計 {fmt_man(pr_data["total_pnl_man"])}万</li>')
    a("</ul>")
    a("</div>")

    a('<div class="conclusion-box">')
    a('<div class="conclusion-title">Step 2.5 への提言</div>')
    a("<ul>")
    a("<li>PF効率とトレード数のバランスが取れたユニバースを選定基盤とする</li>")
    a("<li>株価帯フィルタは過学習リスクがあるため、制限なしをデフォルトとして検証継続</li>")
    a("<li>SL-3%をベースラインとして固定し、エントリーフィルタの追加検証に進む</li>")
    a("<li>レジーム別のルール適用（Uptrend→B1, Downtrend→B4）の有効性を全ユニバースで確認</li>")
    a("</ul>")
    a("</div>")

    a("</section>")

    # --- Footer ---
    a("</body>")
    a("</html>")

    return "\n".join(lines)


def main() -> None:
    df = pd.read_parquet(PARQUET)
    print(f"読み込み: {len(df)} 行")
    print(f"カラム: {list(df.columns)}")
    print(f"ユニバース: {sorted(df['universe'].unique())}")
    print(f"株価帯: {sorted(df['price_range'].unique())}")
    print(f"SL: {sorted(df['sl'].unique())}")
    print(f"ルール: {sorted(df['rule'].unique())}")
    print(f"レジーム: {sorted(df['regime'].unique())}")

    html = generate_html(df)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"出力: {OUTPUT} ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
