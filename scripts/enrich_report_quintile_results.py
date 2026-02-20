#!/usr/bin/env python3
"""
レポート HTML に Q1-Q5 検証結果（P&L 実績）を追加する。

Usage:
    python scripts/enrich_report_quintile_results.py 20260220
    python scripts/enrich_report_quintile_results.py 20260220 --weekly
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import REPORTS_DIR

REPORT_DIR = ROOT / "improvement" / "output"
ARCHIVE = ROOT / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
TRENDING = ROOT / "data" / "parquet" / "grok_trending.parquet"


def _credit(r: pd.Series) -> str:
    if r.get("ng") or (not r.get("is_shortable") and not r.get("day_trade_available_shares")):
        return '<span class="tag tag-neutral">除0</span>'
    if r.get("day_trade") and r.get("day_trade_available_shares", 0) > 0:
        return f"いちにち {int(r['day_trade_available_shares']):,}"
    return "制度" if r.get("is_shortable") else "—"


def _is_excluded(r: pd.Series) -> bool:
    return bool(r.get("ng") or (not r.get("is_shortable") and not r.get("day_trade_available_shares")))


def _pnl_td(val: float, invert: bool = False) -> str:
    if pd.isna(val):
        return '<td class="r num-neutral">—</td>'
    v = int(val)
    if v == 0:
        return '<td class="r num-neutral">0</td>'
    if invert:
        cls = "num-pos" if v < 0 else ""
        sty = ' style="color:var(--amber);"' if v > 0 else ""
    else:
        cls = "num-pos" if v > 0 else "num-neg"
        sty = ""
    return f'<td class="r {cls}"{sty}>{v:+,}</td>'


def _tk(ticker: str) -> str:
    return ticker.replace(".T", "")


def _row_q1(r: pd.Series) -> str:
    p2 = r["profit_per_100_shares_phase2"]
    win = r.get("phase2_win")
    rc, rt = ("num-pos", "勝") if win else ("num-neg", "負")
    if not pd.isna(p2) and int(p2) == 0:
        rc, rt = "", "引分"
    hl = ""
    if not pd.isna(p2) and abs(p2) >= 5000:
        hl = ' class="highlight-row-green"' if p2 > 0 else ' class="highlight-row"'
    bp = int(r["buy_price"]) if not pd.isna(r["buy_price"]) else 0
    return (
        f'      <tr{hl}><td>{r["stock_name"]} ({_tk(r["ticker"])})</td>'
        f'<td class="r">{bp:,}</td><td>{_credit(r)}</td>'
        f'{_pnl_td(r["profit_per_100_shares_phase1"])}{_pnl_td(p2)}'
        f'<td class="{rc}">{rt}</td></tr>'
    )


def _row_q24(r: pd.Series) -> str:
    p2 = r["profit_per_100_shares_phase2"]
    win = r.get("phase2_win")
    rc, rt = ("num-pos", "勝") if win else ("num-neg", "負")
    if not pd.isna(p2) and int(p2) == 0:
        rc, rt = "", "引分"
    hl = ' class="highlight-row-green"' if (not pd.isna(p2) and p2 > 0) else ""
    bp = int(r["buy_price"]) if not pd.isna(r["buy_price"]) else 0
    return (
        f'      <tr{hl}><td>{r["stock_name"]} ({_tk(r["ticker"])})</td>'
        f'<td>{r["quintile"]}</td><td class="r">{bp:,}</td><td>{_credit(r)}</td>'
        f'{_pnl_td(r["profit_per_100_shares_phase1"])}{_pnl_td(p2)}'
        f'<td class="{rc}">{rt}</td></tr>'
    )


def _row_q5(r: pd.Series) -> str:
    p2 = r["profit_per_100_shares_phase2"]
    exc = _is_excluded(r)
    if exc:
        jc, jt, js = "num-neutral", "取引不可", ""
    elif not pd.isna(p2) and p2 <= 0:
        jc, jt, js = "num-pos", "回避正解", ""
    else:
        jc, jt, js = "", "機会損失", ' style="color:var(--amber);"'
    hl = ' class="highlight-row"' if (not pd.isna(p2) and abs(p2) >= 5000) else ""
    bp = int(r["buy_price"]) if not pd.isna(r["buy_price"]) else 0
    return (
        f'      <tr{hl}><td>{r["stock_name"]} ({_tk(r["ticker"])})</td>'
        f'<td class="r">{bp:,}</td><td>{_credit(r)}</td>'
        f'{_pnl_td(r["profit_per_100_shares_phase1"], invert=True)}'
        f'{_pnl_td(p2, invert=True)}'
        f'<td class="{jc}"{js}>{jt}</td></tr>'
    )


def build_results_html(day: pd.DataFrame) -> str:
    q1 = day[day["quintile"] == "Q1"]
    q24 = day[day["quintile"].isin(["Q2", "Q3", "Q4"])]
    q5 = day[day["quintile"] == "Q5"]
    parts: list[str] = []

    # Q1
    q1_p2 = int(q1["profit_per_100_shares_phase2"].sum())
    badge = "badge-emerald" if q1_p2 >= 0 else "badge-red"
    parts.append(
        f'  <h3>Q1 ショート実績（{len(q1)}件） <span class="badge {badge}">P2: {q1_p2:+,}円</span></h3>\n'
        f'  <table>\n'
        f'    <thead><tr><th>銘柄</th><th class="r">買値</th><th>信用</th>'
        f'<th class="r">P1(寄→前引)</th><th class="r">P2(寄→大引)</th><th>結果</th></tr></thead>\n'
        f'    <tbody>\n' + "\n".join(_row_q1(r) for _, r in q1.iterrows()) +
        '\n    </tbody>\n  </table>'
    )

    # Q2-Q4
    if not q24.empty:
        q24_p2 = int(q24["profit_per_100_shares_phase2"].sum())
        parts.append(
            f'\n\n  <h3>Q2-Q4（{len(q24)}件） <span class="badge badge-amber">P2: {q24_p2:+,}円</span></h3>\n'
            f'  <table>\n'
            f'    <thead><tr><th>銘柄</th><th>Q</th><th class="r">買値</th><th>信用</th>'
            f'<th class="r">P1</th><th class="r">P2</th><th>結果</th></tr></thead>\n'
            f'    <tbody>\n' + "\n".join(_row_q24(r) for _, r in q24.iterrows()) +
            '\n    </tbody>\n  </table>'
        )

    # Q5
    parts.append(
        f'\n\n  <h3>Q5 回避した利益（{len(q5)}件）— ショートしなかった銘柄</h3>\n'
        f'  <p style="font-size:0.75rem; color:var(--text-muted); margin-bottom:4px;">'
        f'マイナス＝株価上昇→回避正解（<span class="num-pos">緑</span>）｜'
        f'プラス＝株価下落→機会損失（<span style="color:var(--amber);">黄</span>）｜'
        f'除0＝そもそも取引不可</p>\n'
        f'  <table>\n'
        f'    <thead><tr><th>銘柄</th><th class="r">買値</th><th>信用</th>'
        f'<th class="r">P1(寄→前引)</th><th class="r">P2(寄→大引)</th><th>判定</th></tr></thead>\n'
        f'    <tbody>\n' + "\n".join(_row_q5(r) for _, r in q5.iterrows()) +
        '\n    </tbody>\n  </table>'
    )

    # Summary
    parts.append('\n\n  <h3 style="margin-top:24px;">サマリー</h3>')
    parts.append(_build_summary_html(day))

    return "".join(parts)


def _prepare_day(arch: pd.DataFrame, date_iso: str, gt: pd.DataFrame | None) -> pd.DataFrame | None:
    """archive から 1日分を取得し、ショート視点に反転して返す。"""
    day = arch[arch["selection_date"] == date_iso].copy()
    if day.empty:
        return None

    # quintile: archive にあればそれを使う、なければ gt から取得
    if "quintile" not in day.columns or day["quintile"].isna().all():
        if gt is not None:
            day = day.merge(gt[["ticker", "quintile"]], on="ticker", how="left")
    # archive はロング視点 → ショート視点に反転
    day["profit_per_100_shares_phase1"] = -day["profit_per_100_shares_phase1"]
    day["profit_per_100_shares_phase2"] = -day["profit_per_100_shares_phase2"]
    day["phase2_win"] = ~day["phase2_win"].fillna(False).astype(bool)
    return day


def build_weekly_results_html(
    arch: pd.DataFrame, dates: list[str], gt: pd.DataFrame | None
) -> str:
    """週次レポート用: 日別Q1-Q5テーブル + 週間サマリー。"""
    parts: list[str] = []
    all_days: list[pd.DataFrame] = []

    # quintile が archive にない場合は gt の日付を注記用に取得
    gt_date = str(gt["date"].iloc[0])[:10] if gt is not None else None
    has_native_quintile = "quintile" in arch.columns

    for date_iso in dates:
        day = _prepare_day(arch, date_iso, gt)
        if day is None:
            continue
        if "quintile" not in day.columns or day["quintile"].isna().all():
            parts.append(
                f'\n  <h3>{date_iso} — quintileデータなし</h3>\n'
                f'  <p style="color:var(--text-muted);">この日のquintile割り当てが'
                f'アーカイブに保存されていないためスキップ</p>'
            )
            continue
        all_days.append(day)
        note = ""
        if not has_native_quintile and gt_date and date_iso != gt_date:
            note = f' <span style="font-size:0.75rem; color:var(--text-muted);">（※quintileは{gt_date}時点の割当を適用）</span>'
        parts.append(f'\n  <h3 style="margin-top:20px;">{date_iso}{note}</h3>')
        parts.append(build_results_html(day))

    if not all_days:
        parts.append(
            '\n  <p style="color:var(--text-muted);">該当週にquintileデータのある日がありません</p>'
        )
        return "\n".join(parts)

    # 週間合計サマリー
    week = pd.concat(all_days, ignore_index=True)
    parts.append(f'\n\n  <h3 style="margin-top:28px; border-top:1px solid var(--card-border); padding-top:16px;">'
                 f'週間合計（{len(all_days)}日分）</h3>')
    parts.append(_build_summary_html(week))

    return "\n".join(parts)


def _build_summary_html(day: pd.DataFrame) -> str:
    """全体/除0除外のサマリー部分のみ生成（日次・週次共通）。"""
    q1 = day[day["quintile"] == "Q1"]
    q24 = day[day["quintile"].isin(["Q2", "Q3", "Q4"])]
    q5 = day[day["quintile"] == "Q5"]

    all_p2 = int(day["profit_per_100_shares_phase2"].sum())
    tradable = day[~day.apply(_is_excluded, axis=1)]
    trad_p2 = int(tradable["profit_per_100_shares_phase2"].sum()) if len(tradable) > 0 else 0
    draws = int((day["profit_per_100_shares_phase2"].fillna(0) == 0).sum())
    wins = int(day["phase2_win"].sum())
    losses = len(day) - wins - draws
    wld = f"{wins}勝{f'{draws}分' if draws else ''}{losses}敗"

    all_cls = "num-pos" if all_p2 >= 0 else "num-neg"
    trad_cls = "num-pos" if trad_p2 >= 0 else "num-neg"

    parts: list[str] = []
    parts.append(f'''
  <div class="grid-2" style="margin-top:8px;">
    <div><div class="stat-card" style="border-left:3px solid var(--emerald);">
      <div class="label">見かけ上のP2（全銘柄）</div>
      <div class="value {all_cls}" style="font-size:1.3rem;">{all_p2:+,}円</div>
      <div class="sub">{len(day)}件（{wld}）</div>
    </div></div>
    <div><div class="stat-card" style="border-left:3px solid var(--amber);">
      <div class="label">実際に取引可能なP2（除0除外）</div>
      <div class="value {trad_cls}" style="font-size:1.3rem;">{trad_p2:+,}円</div>
      <div class="sub">除0を除く{len(tradable)}件のみで集計</div>
    </div></div>
  </div>''')

    rows_detail: list[str] = []
    for label, grp, is_q5 in [
        ("<strong>Q1 ショート</strong>", q1, False),
        ("Q2-Q4", q24, False),
        ("Q5 機会損失", q5, True),
    ]:
        if grp.empty:
            continue
        g_all_p2 = int(grp["profit_per_100_shares_phase2"].sum())
        g_trad = grp[~grp.apply(_is_excluded, axis=1)]
        g_trad_p2 = int(g_trad["profit_per_100_shares_phase2"].sum()) if len(g_trad) > 0 else 0
        g_excl = grp[grp.apply(_is_excluded, axis=1)]
        g_excl_p2 = int(g_excl["profit_per_100_shares_phase2"].sum()) if len(g_excl) > 0 else 0

        if is_q5:
            a_cls, a_sty = "", ' style="color:var(--amber);"'
            t_cls, t_sty = "", ' style="color:var(--amber);"'
        else:
            a_cls = "num-pos" if g_all_p2 >= 0 else "num-neg"
            a_sty = ""
            t_cls = "num-pos" if g_trad_p2 >= 0 else "num-neg"
            t_sty = ""

        if len(g_excl) == 0:
            diff_text = "除0なし"
        else:
            diff_text = f"除0の{len(g_excl)}件が{g_excl_p2:+,}を占めていた"

        rows_detail.append(
            f'      <tr><td>{label}</td>'
            f'<td class="r {a_cls}"{a_sty}>{g_all_p2:+,}（{len(grp)}件）</td>'
            f'<td class="r {t_cls}"{t_sty}>{g_trad_p2:+,}（{len(g_trad)}件）</td>'
            f'<td>{diff_text}</td></tr>'
        )

    parts.append(
        f'\n  <table style="margin-top:16px;">\n'
        f'    <thead><tr><th></th><th class="r">全体</th><th class="r">除0除外</th>'
        f'<th>差分の内訳</th></tr></thead>\n'
        f'    <tbody>\n' + "\n".join(rows_detail) + '\n    </tbody>\n  </table>'
    )
    return "".join(parts)


def _inject_daily(html: str, results_html: str) -> str | None:
    """日次レポートにQ1-Q5セクションを注入。"""
    marker = "<h3>Q1 ショート"
    idx_start = html.find(marker)
    if idx_start == -1:
        # Q1 候補（未enriched）を探す
        marker2 = "<h3>Q1 候補"
        idx_start = html.find(marker2)
        if idx_start == -1:
            return None

    section_end = re.search(r"</div>\s*\n\s*<!-- =====", html[idx_start:])
    if not section_end:
        return None

    idx_end = idx_start + section_end.start()
    return html[:idx_start] + results_html + "\n" + html[idx_end:]


def _inject_weekly(html: str, results_html: str) -> str | None:
    """週次レポートに Q1-Q5 検証セクションを挿入（結論の直前）。"""
    # 既存のQ1-Q5セクションがあれば置換
    existing = html.find("<!-- ===== Q1-Q5 検証 =====")
    if existing != -1:
        section_end = re.search(r"</div>\s*\n\s*<!-- =====", html[existing:])
        if section_end:
            idx_end = existing + section_end.start()
            section_html = (
                f'<!-- ===== Q1-Q5 検証 ===== -->\n'
                f'<div class="section">\n'
                f'  <h2>Q1-Q5 検証結果（P&amp;L実績）</h2>\n'
                f'{results_html}\n'
                f'</div>\n\n'
            )
            return html[:existing] + section_html + html[idx_end:]

    # 新規挿入: 結論セクションの直前
    marker = "<!-- ===== 9. 結論 ====="
    idx = html.find(marker)
    if idx == -1:
        # セクション番号なしも試す
        marker = "<!-- ===== 結論 ====="
        idx = html.find(marker)
    if idx == -1:
        return None

    section_html = (
        f'<!-- ===== Q1-Q5 検証 ===== -->\n'
        f'<div class="section">\n'
        f'  <h2>Q1-Q5 検証結果（P&amp;L実績）</h2>\n'
        f'{results_html}\n'
        f'</div>\n\n'
    )
    return html[:idx] + section_html + html[idx:]


def _get_week_dates(date_iso: str) -> list[str]:
    """指定日を含む月〜金の日付リストを返す。"""
    d = pd.Timestamp(date_iso)
    monday = d - pd.Timedelta(days=d.weekday())
    return [(monday + pd.Timedelta(days=i)).strftime("%Y-%m-%d") for i in range(5)]


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <YYYYMMDD> [--weekly]", file=sys.stderr)
        sys.exit(1)

    date_str = sys.argv[1]
    weekly = "--weekly" in sys.argv
    date_iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    arch = pd.read_parquet(ARCHIVE)
    gt = pd.read_parquet(TRENDING)

    if weekly:
        report_path = REPORT_DIR / f"market_analysis_weekly_{date_str}.html"
        if not report_path.exists():
            print(f"[ERROR] {report_path} が見つかりません", file=sys.stderr)
            sys.exit(1)

        week_dates = _get_week_dates(date_iso)
        results_html = build_weekly_results_html(arch, week_dates, gt)

        html = report_path.read_text(encoding="utf-8")
        new_html = _inject_weekly(html, results_html)
        if new_html is None:
            print("[ERROR] 挿入位置が見つかりません", file=sys.stderr)
            sys.exit(1)
    else:
        report_path = REPORT_DIR / f"market_analysis_{date_str}.html"
        if not report_path.exists():
            print(f"[ERROR] {report_path} が見つかりません", file=sys.stderr)
            sys.exit(1)

        gt_date = str(gt["date"].iloc[0])[:10]
        if gt_date != date_iso:
            print(f"[ERROR] grok_trending の日付 ({gt_date}) が {date_iso} と一致しません", file=sys.stderr)
            sys.exit(1)

        day = _prepare_day(arch, date_iso, gt)
        if day is None:
            print(f"[ERROR] {date_iso} のデータが archive にありません", file=sys.stderr)
            sys.exit(1)

        results_html = build_results_html(day)

        html = report_path.read_text(encoding="utf-8")
        new_html = _inject_daily(html, results_html)
        if new_html is None:
            print("[ERROR] 挿入位置が見つかりません", file=sys.stderr)
            sys.exit(1)

    report_path.write_text(new_html, encoding="utf-8")
    print(f"[OK] {report_path}")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    dest = REPORTS_DIR / report_path.name
    dest.write_text(new_html, encoding="utf-8")
    print(f"[OK] {dest}")


if __name__ == "__main__":
    main()
