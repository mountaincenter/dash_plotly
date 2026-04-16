#!/usr/bin/env python3
"""
report_data JSON → HTMLレポート自動生成

16:45パイプラインで実行。データ転記セクションを自動生成し、
推論セクション（タイムライン・要因分析・判断材料・結論）は空枠で生成。
後からClaude Codeで推論部分を洗い替え。
"""
from __future__ import annotations

import html
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
REPORT_DATA_DIR = ROOT / "data" / "parquet" / "market_summary" / "structured"
REPORTS_DIR = ROOT / "data" / "reports"

WEEKDAY_JA = ["月", "火", "水", "木", "金", "土", "日"]


def _e(v: Any) -> str:
    """HTMLエスケープ。None/非文字列は空文字に"""
    if v is None:
        return ""
    return html.escape(str(v), quote=True)


def _f(v: Any, decimals: int = 2) -> str:
    """数値フォーマット。None/NaN/inf/非数値は'--'"""
    if v is None:
        return "--"
    try:
        val = float(v)
    except (ValueError, TypeError):
        return "--"
    if not math.isfinite(val):
        return "--"
    return f"{val:,.{decimals}f}"


def _sign(v: Any) -> str:
    """符号付き文字列"""
    if v is None:
        return "--"
    try:
        val = float(v)
    except (ValueError, TypeError):
        return "--"
    if not math.isfinite(val):
        return "--"
    return f"+{val:,.2f}" if val >= 0 else f"{val:,.2f}"


def _sign_pct(v: Any) -> str:
    """符号付きパーセント"""
    if v is None:
        return "--"
    try:
        val = float(v)
    except (ValueError, TypeError):
        return "--"
    if not math.isfinite(val):
        return "--"
    return f"+{val:.2f}%" if val >= 0 else f"{val:.2f}%"


def _css_class(v: Any) -> str:
    """正負でCSSクラスを返す"""
    if v is None:
        return "num-neutral"
    try:
        val = float(v)
        if val > 0:
            return "num-pos"
        elif val < 0:
            return "num-neg"
        return "num-neutral"
    except (ValueError, TypeError):
        return "num-neutral"


def _row_class(v: Any) -> str:
    """行ハイライトクラス"""
    if v is None:
        return ""
    try:
        val = float(v)
        if val > 0:
            return ' class="highlight-row-green"'
        elif val < 0:
            return ' class="highlight-row"'
        return ""
    except (ValueError, TypeError):
        return ""


def _safe_get(d: dict, *keys, default=None):
    """ネストされたdict安全取得"""
    current = d
    for k in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(k, default)
    return current


def _build_missing_section(section_num: str, title: str, reason: str = "データ未取得") -> str:
    """データ欠損時のセクション空枠（構造を維持するため）"""
    lines = [f'<!-- ===== {section_num}. {title} ===== -->']
    lines.append('<div class="section">')
    lines.append(f'  <h2>{_e(title)} <span class="evidence-label evidence-unverified">{_e(reason)}</span></h2>')
    lines.append(f'  <div class="alert-box alert-warning">{_e(reason)}。ソースが取得できなかった可能性あり。</div>')
    lines.append('</div>')
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CSS (4/15レポートと完全一致)
# ---------------------------------------------------------------------------
CSS = """:root {
    --bg: #09090b; --card: #18181b; --card-border: #27272a;
    --text: #fafafa; --text-muted: #a1a1aa;
    --emerald: #34d399; --emerald-bg: rgba(52,211,153,0.1);
    --rose: #fb7185; --rose-bg: rgba(251,113,133,0.1);
    --amber: #fbbf24; --amber-bg: rgba(251,191,36,0.3);
    --blue: #60a5fa; --blue-bg: rgba(96,165,250,0.1);
    --purple: #a78bfa; --purple-bg: rgba(167,139,250,0.1);
    --cyan: #22d3ee; --cyan-bg: rgba(34,211,238,0.1);
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif; line-height:1.6; padding:24px; max-width:1200px; margin:0 auto; }
  h1 { font-size:1.5rem; margin-bottom:8px; display:flex; align-items:center; gap:12px; flex-wrap:wrap; }
  .subtitle { color:var(--text-muted); font-size:0.875rem; margin-bottom:32px; }
  .badge { display:inline-block; padding:2px 10px; border-radius:9999px; font-size:0.75rem; font-weight:600; }
  .badge-rose { background:var(--rose-bg); color:var(--rose); border:1px solid rgba(251,113,133,0.3); }
  .badge-emerald { background:var(--emerald-bg); color:var(--emerald); border:1px solid rgba(52,211,153,0.3); }
  .badge-amber { background:rgba(251,191,36,0.15); color:var(--amber); border:1px solid rgba(251,191,36,0.3); }
  .badge-blue { background:var(--blue-bg); color:var(--blue); border:1px solid rgba(96,165,250,0.3); }
  .badge-purple { background:var(--purple-bg); color:var(--purple); border:1px solid rgba(167,139,250,0.3); }
  .section { background:var(--card); border:1px solid var(--card-border); border-radius:12px; padding:24px; margin-bottom:20px; }
  .section h2 { font-size:1.1rem; margin-bottom:16px; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .section h3 { font-size:0.95rem; color:var(--text-muted); margin:16px 0 8px 0; }
  table { width:100%; border-collapse:collapse; font-size:0.85rem; margin:12px 0; }
  th { text-align:left; padding:8px 12px; background:rgba(255,255,255,0.03); color:var(--text-muted); font-weight:600; border-bottom:1px solid var(--card-border); white-space:nowrap; }
  th.r { text-align:right; }
  td { padding:8px 12px; border-bottom:1px solid rgba(255,255,255,0.05); }
  td.r { text-align:right; font-variant-numeric:tabular-nums; }
  tr:hover td { background:rgba(255,255,255,0.02); }
  .num-pos { color:var(--emerald); font-weight:600; }
  .num-neg { color:var(--rose); font-weight:600; }
  .num-neutral { color:var(--text-muted); }
  .highlight-row td { background:rgba(251,113,133,0.05); }
  .highlight-row-green td { background:rgba(52,211,153,0.05); }
  .highlight-row-amber td { background:rgba(251,191,36,0.05); }
  .alert-box { border-radius:8px; padding:16px; margin:16px 0; font-size:0.875rem; line-height:1.7; }
  .alert-danger { background:var(--rose-bg); border:1px solid rgba(251,113,133,0.3); color:var(--rose); }
  .alert-warning { background:rgba(251,191,36,0.08); border:1px solid rgba(251,191,36,0.3); color:var(--amber); }
  .alert-info { background:var(--blue-bg); border:1px solid rgba(96,165,250,0.3); color:var(--blue); }
  .alert-success { background:var(--emerald-bg); border:1px solid rgba(52,211,153,0.3); color:var(--emerald); }
  .grid-2 { display:grid; grid-template-columns:1fr 1fr; gap:20px; }
  .grid-3 { display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px; }
  .grid-4 { display:grid; grid-template-columns:1fr 1fr 1fr 1fr; gap:16px; }
  @media (max-width:768px) { .grid-2,.grid-3,.grid-4 { grid-template-columns:1fr; } }
  .stat-card { background:rgba(255,255,255,0.02); border:1px solid var(--card-border); border-radius:8px; padding:16px; text-align:center; }
  .stat-card .label { color:var(--text-muted); font-size:0.75rem; margin-bottom:4px; }
  .stat-card .value { font-size:1.5rem; font-weight:700; }
  .stat-card .sub { color:var(--text-muted); font-size:0.75rem; margin-top:2px; }
  .timeline { position:relative; padding-left:28px; margin:16px 0; }
  .timeline::before { content:''; position:absolute; left:8px; top:4px; bottom:4px; width:2px; background:var(--card-border); }
  .timeline-item { position:relative; margin-bottom:16px; font-size:0.875rem; }
  .timeline-item::before { content:''; position:absolute; left:-24px; top:6px; width:10px; height:10px; border-radius:50%; background:var(--card-border); border:2px solid var(--bg); }
  .timeline-item.peak::before { background:var(--emerald); }
  .timeline-item.drop::before { background:var(--rose); }
  .timeline-item.neutral::before { background:var(--amber); }
  .factor-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin:12px 0; }
  .factor-card { background:rgba(255,255,255,0.02); border:1px solid var(--card-border); border-radius:8px; padding:14px; }
  .factor-card .factor-title { font-size:0.8rem; font-weight:600; margin-bottom:6px; display:flex; align-items:center; gap:6px; }
  .factor-card .factor-detail { font-size:0.8rem; color:var(--text-muted); line-height:1.5; }
  .source-list { margin-top:12px; padding-left:20px; }
  .source-list li { font-size:0.75rem; color:var(--text-muted); margin-bottom:4px; }
  .source-list a { color:var(--blue); text-decoration:none; }
  .source-list a:hover { text-decoration:underline; }
  .tag { display:inline-block; padding:1px 8px; border-radius:4px; font-size:0.7rem; font-weight:600; margin-right:4px; }
  .tag-bull { background:rgba(52,211,153,0.15); color:var(--emerald); }
  .tag-bear { background:rgba(251,113,133,0.15); color:var(--rose); }
  .tag-neutral { background:rgba(251,191,36,0.12); color:var(--amber); }
  .comparison-bar { display:flex; align-items:center; gap:8px; margin:4px 0; font-size:0.8rem; }
  .comparison-bar .bar-label { min-width:120px; color:var(--text-muted); }
  .comparison-bar .bar-track { flex:1; height:20px; background:rgba(255,255,255,0.03); border-radius:4px; overflow:hidden; }
  .comparison-bar .bar-fill { height:100%; border-radius:4px; display:flex; align-items:center; padding:0 8px; font-size:0.7rem; font-weight:600; color:#fff; }
  .bar-fill-green { background:var(--emerald); }
  .bar-fill-red { background:var(--rose); }
  .quote-box { border-left:3px solid var(--purple); padding:12px 16px; margin:12px 0; background:rgba(167,139,250,0.05); border-radius:0 8px 8px 0; font-size:0.85rem; color:var(--text-muted); font-style:italic; }
  .quote-box .quote-source { margin-top:6px; font-style:normal; font-size:0.75rem; color:var(--purple); }
  .evidence-label { display:inline-block; padding:1px 8px; border-radius:4px; font-size:0.65rem; font-weight:700; letter-spacing:0.5px; vertical-align:middle; margin-left:8px; }
  .evidence-fact { background:rgba(52,211,153,0.15); color:var(--emerald); border:1px solid rgba(52,211,153,0.3); }
  .evidence-inference { background:rgba(251,191,36,0.12); color:var(--amber); border:1px solid rgba(251,191,36,0.3); }
  .evidence-unverified { background:rgba(251,113,133,0.12); color:var(--rose); border:1px solid rgba(251,113,133,0.3); }
  .evidence-legend { display:flex; gap:16px; margin-bottom:24px; padding:12px 16px; background:rgba(255,255,255,0.02); border:1px solid var(--card-border); border-radius:8px; font-size:0.8rem; color:var(--text-muted); }
  .evidence-legend-item { display:flex; align-items:center; gap:6px; }
  .footnote { font-size:0.75rem; color:var(--text-muted); margin-top:8px; font-style:italic; }
  .placeholder-section { border:2px dashed var(--amber); background:rgba(251,191,36,0.03); }
  .placeholder-note { color:var(--amber); font-size:0.85rem; padding:24px; text-align:center; }
  footer { text-align:center; color:var(--text-muted); font-size:0.7rem; margin-top:40px; padding:16px 0; border-top:1px solid var(--card-border); }"""


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _build_header(data: dict) -> str:
    """h1 + subtitle + evidence legend"""
    ms = data.get("market_summary", {})
    n225 = ms.get("n225", {})
    close = n225.get("close")
    pct = n225.get("change_pct")
    change = n225.get("change")
    vi = ms.get("vi", {})
    breadth = ms.get("market_breadth", {})
    adv = breadth.get("advancing", "--")
    dec = breadth.get("declining", "--")
    sectors = data.get("sectors", {})
    up_count = sectors.get("up_count", "--")
    down_count = sectors.get("down_count", "--")

    date_str = data.get("date", "")
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        wd = WEEKDAY_JA[dt.weekday()]
        date_disp = f"{dt.year}/{dt.month:02d}/{dt.day:02d}（{wd}）"
    except ValueError:
        date_disp = date_str

    direction = "上昇" if (pct and pct > 0) else "下落" if (pct and pct < 0) else "横ばい"
    badge_class = "badge-emerald" if (pct and pct > 0) else "badge-rose" if (pct and pct < 0) else "badge-amber"

    vi_close = vi.get("close", "--")
    vi_pct = vi.get("change_pct")
    vi_str = f"VI {vi_close}({_sign_pct(vi_pct)})" if vi_pct is not None else f"VI {vi_close}"

    badge_text = f"{direction} N225 {_f(close, 0)}({_sign_pct(pct)}) {vi_str} {up_count}上昇/{down_count}下落"

    lines = []
    lines.append(f'<h1>マーケット振り返り {date_disp} <span class="badge {badge_class}">{badge_text}</span></h1>')
    lines.append(f'<div class="subtitle">自動生成レポート（データ転記）。推論セクションはClaude Codeで追記予定。</div>')
    lines.append("")
    lines.append('<div class="evidence-legend">')
    lines.append('  <div class="evidence-legend-item"><span class="evidence-label evidence-fact">事実</span> データソースで確認済み</div>')
    lines.append('  <div class="evidence-legend-item"><span class="evidence-label evidence-inference">推論</span> 筆者の解釈（後から追記）</div>')
    lines.append('</div>')
    return "\n".join(lines)


def _build_market_summary(data: dict) -> str:
    """セクション1: マーケットサマリー"""
    ms = data.get("market_summary", {})
    n225 = ms.get("n225", {})
    topix = ms.get("topix", {})
    usdjpy = ms.get("usdjpy", {})
    vi = ms.get("vi", {})
    breadth = ms.get("market_breadth", {})
    jq_breadth = data.get("jquants_breadth", {})

    lines = ['<!-- ===== 1. マーケットサマリー ===== -->']
    lines.append('<div class="section">')
    lines.append('  <h2>本日のマーケットサマリー <span class="evidence-label evidence-fact">事実</span></h2>')

    # 4カード: N225, TOPIX, USD/JPY, VI
    lines.append('  <div class="grid-4">')

    # N225
    n_close = n225.get("close")
    n_pct = n225.get("change_pct")
    n_change = n225.get("change")
    lines.append(f'    <div class="stat-card"><div class="label">日経平均</div>')
    lines.append(f'      <div class="value {_css_class(n_pct)}">{_f(n_close, 0)}</div>')
    lines.append(f'      <div class="sub">{_sign(n_change)} ({_sign_pct(n_pct)}) <span class="evidence-label evidence-fact">parquet</span></div></div>')

    # TOPIX
    t_close = topix.get("close")
    t_pct = topix.get("change_pct")
    lines.append(f'    <div class="stat-card"><div class="label">TOPIX</div>')
    lines.append(f'      <div class="value {_css_class(t_pct)}">{_f(t_close)}</div>')
    lines.append(f'      <div class="sub">{_sign_pct(t_pct)} <span class="evidence-label evidence-fact">S3</span></div></div>')

    # USD/JPY
    usd_close = usdjpy.get("close")
    usd_pct = usdjpy.get("change_pct")
    usd_src = usdjpy.get("source", "yfinance")
    if "error" in usdjpy:
        lines.append(f'    <div class="stat-card"><div class="label">USD/JPY</div>')
        lines.append(f'      <div class="value num-neutral">--</div>')
        lines.append(f'      <div class="sub">データ取得失敗</div></div>')
    else:
        lines.append(f'    <div class="stat-card"><div class="label">USD/JPY</div>')
        lines.append(f'      <div class="value {_css_class(usd_pct)}">{_f(usd_close)}円</div>')
        lines.append(f'      <div class="sub">{_sign_pct(usd_pct)} <span class="evidence-label evidence-fact">{_e(usd_src)}</span></div></div>')

    # VI
    vi_close = vi.get("close")
    vi_pct = vi.get("change_pct")
    vi_change = vi.get("change")
    vi_src = vi.get("source", "parquet")
    lines.append(f'    <div class="stat-card"><div class="label">日経VI</div>')
    lines.append(f'      <div class="value {_css_class(vi_pct)}">{_f(vi_close)}</div>')
    lines.append(f'      <div class="sub">前日比{_sign(vi_change)}（{_sign_pct(vi_pct)}） <span class="evidence-label evidence-fact">{_e(vi_src)}</span></div></div>')

    lines.append('  </div>')  # grid-4

    # N225 高値・安値・VI日中レンジ
    n_high = n225.get("high")
    n_low = n225.get("low")
    vi_high = vi.get("high")
    vi_low = vi.get("low")
    if n_high is not None:
        n_prev = n225.get("prev_close", 0)
        lines.append('  <div style="margin-top:16px;"><div class="grid-3">')
        lines.append(f'    <div class="stat-card"><div class="label">日経225 高値</div>')
        lines.append(f'      <div class="value num-pos" style="font-size:1.2rem;">{_f(n_high, 0)}</div>')
        lines.append(f'      <div class="sub">{_sign(n_high - n_prev if n_prev else None)}円（vs 前日終値{_f(n_prev, 0)}）</div></div>')
        lines.append(f'    <div class="stat-card"><div class="label">日経225 安値</div>')
        lines.append(f'      <div class="value" style="font-size:1.2rem;">{_f(n_low, 0)}</div>')
        lines.append(f'      <div class="sub">{_sign(n_low - n_prev if n_prev else None)}円（vs 前日終値{_f(n_prev, 0)}）</div></div>')
        if vi_high is not None:
            lines.append(f'    <div class="stat-card"><div class="label">日経VI 日中レンジ</div>')
            lines.append(f'      <div class="value" style="font-size:1.2rem;">{_f(vi_high)} → {_f(vi_close)}</div>')
            lines.append(f'      <div class="sub">高値{_f(vi_high)} 安値{_f(vi_low)} 終値{_f(vi_close)} <span class="evidence-label evidence-fact">{vi_src}</span></div></div>')
        lines.append('  </div></div>')

    # 騰落・売買代金・日中値幅
    adv = breadth.get("advancing")
    dec = breadth.get("declining")
    tv = breadth.get("trading_value_million")
    if adv is not None:
        lines.append('  <div style="margin-top:16px;"><div class="grid-3">')
        lines.append(f'    <div class="stat-card"><div class="label">騰落銘柄数（プライム）</div>')
        lines.append(f'      <div class="value num-pos" style="font-size:1.2rem;">{adv:,} / {dec:,}</div>')
        lines.append(f'      <div class="sub">値上がり{adv:,} 値下がり{dec:,} <span class="evidence-label evidence-fact">日経電子版</span></div></div>')
        if tv is not None:
            tv_oku = tv / 100  # 百万円→億円
            lines.append(f'    <div class="stat-card"><div class="label">売買代金</div>')
            lines.append(f'      <div class="value" style="font-size:1.2rem;">{tv_oku:,.0f}億円</div>')
            lines.append(f'      <div class="sub">{tv:,}百万円 <span class="evidence-label evidence-fact">日経電子版</span></div></div>')
        if n_high is not None and n_low is not None:
            intraday = n_high - n_low
            lines.append(f'    <div class="stat-card"><div class="label">日中値幅</div>')
            lines.append(f'      <div class="value num-neutral" style="font-size:1.2rem;">{intraday:,.0f}円</div>')
            lines.append(f'      <div class="sub">高値{_f(n_high, 0)} - 安値{_f(n_low, 0)} <span class="evidence-label evidence-fact">parquet</span></div></div>')
        lines.append('  </div></div>')

    # TOPIX サブ指数
    lines.append('  <h3>TOPIX サブ指数 <span class="evidence-label evidence-fact">S3</span></h3>')
    lines.append('  <table><thead><tr><th>指数</th><th class="r">終値</th><th class="r">変化率</th></tr></thead><tbody>')
    for key, name in [("topix", "TOPIX"), ("topix_prime", "TOPIX-Prime"), ("topix_standard", "TOPIX-Standard"), ("topix_growth", "TOPIX-Growth")]:
        d = ms.get(key, {})
        c = d.get("close")
        p = d.get("change_pct")
        rc = _row_class(p)
        lines.append(f'    <tr{rc}><td>{name}</td><td class="r">{_f(c)}</td><td class="r {_css_class(p)}">{_sign_pct(p)}</td></tr>')
    lines.append('  </tbody></table>')

    # 市場別騰落数 (J-Quants)
    if jq_breadth and jq_breadth.get("total_adv") is not None:
        lines.append('  <h3>市場別騰落数（全市場） <span class="evidence-label evidence-fact">J-Quants API</span></h3>')
        lines.append('  <table><thead><tr><th>市場</th><th class="r">値上がり</th><th class="r">値下がり</th><th class="r">比率</th></tr></thead><tbody>')
        for label, adv_key, dec_key in [("全体", "total_adv", "total_dec"), ("プライム", "prime_adv", "prime_dec"), ("スタンダード", "standard_adv", "standard_dec"), ("グロース", "growth_adv", "growth_dec")]:
            a = jq_breadth.get(adv_key, 0)
            d = jq_breadth.get(dec_key, 0)
            ratio = f"{a / d:.2f}:1" if d > 0 else "--"
            bold = " style='font-weight:600'" if label == "全体" else ""
            rc = ' class="highlight-row-green"' if label == "全体" else ""
            ratio_cls = _css_class(a - d)
            lines.append(f'    <tr{rc}><td><strong>{label}</strong></td><td class="r {_css_class(1)}">{a:,}</td><td class="r {_css_class(-1)}">{d:,}</td><td class="r {ratio_cls}">{ratio}</td></tr>')
        lines.append('  </tbody></table>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_placeholder(section_num: str, title: str) -> str:
    """推論セクション用の空枠"""
    lines = [f'<!-- ===== {section_num}. {title} ===== -->']
    lines.append('<div class="section placeholder-section">')
    lines.append(f'  <h2>{title} <span class="evidence-label evidence-inference">推論</span></h2>')
    lines.append(f'  <div class="placeholder-note">このセクションはClaude Codeの推論で追記されます</div>')
    lines.append('</div>')
    return "\n".join(lines)


def _build_volume_leaders(data: dict) -> str:
    """セクション3.5: 売買代金上位・値上がり率・値下がり率"""
    vl = data.get("jquants_volume_leaders", {})
    if not vl or not vl.get("volume_leaders"):
        return _build_missing_section("3.5", "売買代金上位・値上がり率・値下がり率")

    lines = ['<!-- ===== 3.5 売買代金・値上がり率・値下がり率 ===== -->']
    lines.append('<div class="section">')
    lines.append('  <h2>売買代金上位・値上がり率・値下がり率 <span class="evidence-label evidence-fact">J-Quants API</span></h2>')

    # 売買代金TOP10
    lines.append('  <h3>売買代金TOP10</h3>')
    lines.append('  <table><thead><tr><th>銘柄</th><th>市場</th><th>セクター</th><th class="r">終値</th><th class="r">変化率</th><th class="r">売買代金</th></tr></thead><tbody>')
    for item in vl.get("volume_leaders", [])[:10]:
        name = item.get("name", "")
        code = item.get("code", "")[:4]
        market = item.get("market", "")
        sector = item.get("sector", "")
        close = item.get("close")
        pct = item.get("day_change_pct")
        tv = item.get("trading_value_billion")
        tv_str = f'{tv:,.0f}億' if tv is not None else "--"
        rc = _row_class(pct)
        lines.append(f'    <tr{rc}><td>{_e(name)} ({_e(code)})</td><td>{_e(market)}</td><td>{_e(sector)}</td><td class="r">{_f(close, 0)}</td><td class="r {_css_class(pct)}">{_sign_pct(pct)}</td><td class="r">{tv_str}</td></tr>')
    lines.append('  </tbody></table>')

    # 値上がり率・値下がり率 TOP5
    gainers = vl.get("top_gainers", [])[:5]
    losers = vl.get("top_losers", [])[:5]
    if gainers or losers:
        lines.append('  <div class="grid-2" style="margin-top:16px;">')
        if gainers:
            lines.append('    <div><h3>値上がり率TOP5</h3>')
            lines.append('    <table><thead><tr><th>銘柄</th><th>市場</th><th class="r">変化率</th></tr></thead><tbody>')
            for item in gainers:
                name = item.get("name", "")
                code = item.get("code", "")[:4]
                market = item.get("market", "")
                pct = item.get("change_pct")
                lines.append(f'      <tr class="highlight-row-green"><td>{_e(name)} ({_e(code)})</td><td>{_e(market)}</td><td class="r num-pos">{_sign_pct(pct)}</td></tr>')
            lines.append('    </tbody></table></div>')
        if losers:
            lines.append('    <div><h3>値下がり率TOP5</h3>')
            lines.append('    <table><thead><tr><th>銘柄</th><th>市場</th><th class="r">変化率</th></tr></thead><tbody>')
            for item in losers:
                name = item.get("name", "")
                code = item.get("code", "")[:4]
                market = item.get("market", "")
                pct = item.get("change_pct")
                lines.append(f'      <tr class="highlight-row"><td>{_e(name)} ({_e(code)})</td><td>{_e(market)}</td><td class="r num-neg">{_sign_pct(pct)}</td></tr>')
            lines.append('    </tbody></table></div>')
        lines.append('  </div>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_sectors(data: dict) -> str:
    """セクション4: セクター動向"""
    sectors = data.get("sectors", {})
    all_sectors = sectors.get("all", [])
    if not all_sectors:
        return _build_missing_section("4", "セクター動向")
    ms = data.get("market_summary", {})

    up_count = sectors.get("up_count", 0)
    down_count = sectors.get("down_count", 0)
    top = all_sectors[0] if all_sectors else {}
    bottom = all_sectors[-1] if all_sectors else {}

    lines = ['<!-- ===== 4. セクター動向 ===== -->']
    lines.append('<div class="section">')
    badge_text = f'上昇{up_count}/下落{down_count} {_e(top.get("name", ""))}+{_f(_safe_get(top, "change_pct"))}%首位 {_e(bottom.get("name", ""))}{_f(_safe_get(bottom, "change_pct"))}%最下位'
    lines.append(f'  <h2>セクター動向 <span class="badge badge-emerald">{badge_text}</span> <span class="evidence-label evidence-fact">S3</span></h2>')

    # 上位5 / 下位5
    top5 = [s for s in all_sectors if (s.get("change_pct") or 0) > 0][:5]
    bottom5 = [s for s in all_sectors if (s.get("change_pct") or 0) < 0]
    bottom5 = bottom5[-5:] if len(bottom5) >= 5 else bottom5
    bottom5 = list(reversed(bottom5))

    lines.append('  <div class="grid-2">')
    lines.append('    <div><h3>上昇上位5業種</h3>')
    lines.append('    <table><thead><tr><th>セクター</th><th class="r">終値</th><th class="r">変化率</th></tr></thead><tbody>')
    for s in top5:
        lines.append(f'      <tr class="highlight-row-green"><td>{_e(s.get("name", ""))}</td><td class="r">{_f(s.get("close"))}</td><td class="r num-pos">{_sign_pct(s.get("change_pct"))}</td></tr>')
    lines.append('    </tbody></table></div>')
    lines.append('    <div><h3>下落下位5業種</h3>')
    lines.append('    <table><thead><tr><th>セクター</th><th class="r">終値</th><th class="r">変化率</th></tr></thead><tbody>')
    for s in bottom5:
        lines.append(f'      <tr class="highlight-row"><td>{_e(s.get("name", ""))}</td><td class="r">{_f(s.get("close"))}</td><td class="r num-neg">{_sign_pct(s.get("change_pct"))}</td></tr>')
    lines.append('    </tbody></table></div>')
    lines.append('  </div>')

    # 全33業種
    lines.append('  <h3>全33業種 一覧 <span class="evidence-label evidence-fact">S3</span></h3>')
    lines.append('  <table><thead><tr><th>セクター</th><th class="r">終値</th><th class="r">変化率</th></tr></thead><tbody>')
    for s in all_sectors:
        pct = s.get("change_pct")
        rc = _row_class(pct)
        lines.append(f'    <tr{rc}><td>{_e(s.get("name", ""))}</td><td class="r">{_f(s.get("close"))}</td><td class="r {_css_class(pct)}">{_sign_pct(pct)}</td></tr>')
    lines.append('  </tbody></table>')

    # 大型 vs 中小型
    lines.append('  <h3>大型 vs 中小型</h3>')
    lines.append('  <table><thead><tr><th>区分</th><th class="r">変化率</th></tr></thead><tbody>')
    for key, name in [("topix_growth", "TOPIX-Growth"), ("n225", "日経225"), ("topix", "TOPIX"), ("topix_prime", "TOPIX-Prime"), ("topix_standard", "TOPIX-Standard")]:
        d = ms.get(key, {})
        p = d.get("change_pct")
        lines.append(f'    <tr{_row_class(p)}><td>{name}</td><td class="r {_css_class(p)}">{_sign_pct(p)}</td></tr>')
    lines.append('  </tbody></table>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_divergence(data: dict) -> str:
    """セクション5: N225 vs TOPIX乖離"""
    div = data.get("n225_topix_divergence", {})
    today = div.get("today", {})
    history = div.get("history_5d", [])
    if not today:
        return _build_missing_section("5", "日経平均 vs TOPIX 乖離")

    n_pct = today.get("n225_pct")
    t_pct = today.get("topix_pct")
    gap = today.get("gap")

    lines = ['<!-- ===== 5. N225 vs TOPIX 乖離 ===== -->']
    lines.append('<div class="section">')
    lines.append(f'  <h2>日経平均 vs TOPIX 乖離 <span class="badge badge-emerald">N225 {_sign_pct(n_pct)} vs TOPIX {_sign_pct(t_pct)} 乖離{_sign_pct(gap)}</span> <span class="evidence-label evidence-fact">parquet+S3</span></h2>')

    # バー
    for label, val in [("日経225", n_pct), ("TOPIX", t_pct), ("乖離", gap)]:
        w = min(abs(float(val or 0)) * 8, 100)
        fill_cls = "bar-fill-green" if (val or 0) >= 0 else "bar-fill-red"
        lines.append(f'  <div class="comparison-bar"><div class="bar-label">{label}</div><div class="bar-track"><div class="bar-fill {fill_cls}" style="width:{w:.0f}%;">{_sign_pct(val)}</div></div></div>')

    # 履歴テーブル
    if history:
        lines.append('  <div style="margin-top:16px;"><h3>直近の推移</h3>')
        lines.append('  <table><thead><tr><th>日付</th><th class="r">日経225</th><th class="r">TOPIX</th><th class="r">乖離</th></tr></thead><tbody>')
        for h in history:
            d = h.get("date", "")
            np = h.get("n225_pct")
            tp = h.get("topix_pct")
            g = h.get("gap")
            lines.append(f'    <tr><td>{d}</td><td class="r {_css_class(np)}">{_sign_pct(np)}</td><td class="r {_css_class(tp)}">{_sign_pct(tp)}</td><td class="r {_css_class(g)}">{_sign_pct(g)}</td></tr>')
        lines.append('  </tbody></table></div>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_foreign_markets(data: dict) -> str:
    """セクション6: 米株・海外要因"""
    fm = data.get("foreign_markets", {})
    us = fm.get("us", {})
    asia = fm.get("asia", {})
    futures = fm.get("futures", {})

    lines = ['<!-- ===== 6. 米株・海外要因 ===== -->']
    lines.append('<div class="section">')
    lines.append('  <h2>米株・海外要因 <span class="evidence-label evidence-fact">事実(yfinance)</span></h2>')

    lines.append('  <div class="grid-2">')

    # NY市場
    lines.append('    <div><h3>前日NY市場</h3>')
    lines.append('    <div class="alert-box alert-success" style="margin-top:0;">')
    for key, name in [("sp500", "S&P 500"), ("nasdaq", "NASDAQ"), ("dow", "DOW")]:
        d = us.get(key, {})
        lines.append(f'      &bull; {_e(name)}: {_f(d.get("close"))} ({_sign_pct(d.get("change_pct"))}) <span class="evidence-label evidence-fact">yfinance</span><br>')
    vix = futures.get("vix", {})
    if vix:
        lines.append(f'      &bull; VIX: {_f(vix.get("close"))} ({_sign_pct(vix.get("change_pct"))}) <span class="evidence-label evidence-fact">yfinance</span>')
    lines.append('    </div></div>')

    # CME NKD
    nkd = futures.get("nkd", {})
    lines.append('    <div><h3>CME NKD先物 <span class="evidence-label evidence-fact">parquet</span></h3>')
    lines.append('    <table><thead><tr><th>指標</th><th class="r">値</th></tr></thead><tbody>')
    lines.append(f'      <tr><td>CME NKD 終値</td><td class="r">{_f(nkd.get("close"), 0)}</td></tr>')
    lines.append(f'      <tr><td>前日比</td><td class="r {_css_class(nkd.get("change"))}">{_sign(nkd.get("change"))} ({_sign_pct(nkd.get("change_pct"))})</td></tr>')
    lines.append('    </tbody></table></div>')

    lines.append('  </div>')

    # アジア
    lines.append('  <h3>アジア市場 <span class="evidence-label evidence-fact">parquet</span></h3>')
    lines.append('  <table><thead><tr><th>指数</th><th class="r">終値</th><th class="r">前日比</th><th class="r">変化率</th></tr></thead><tbody>')
    for key, name in [("kospi", "KOSPI"), ("hang_seng", "ハンセン"), ("shanghai", "上海総合")]:
        d = asia.get(key, {})
        rc = _row_class(d.get("change_pct"))
        lines.append(f'    <tr{rc}><td>{name}</td><td class="r">{_f(d.get("close"))}</td><td class="r {_css_class(d.get("change"))}">{_sign(d.get("change"))}</td><td class="r {_css_class(d.get("change_pct"))}">{_sign_pct(d.get("change_pct"))}</td></tr>')
    lines.append('  </tbody></table>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_investor_types(data: dict) -> str:
    """セクション6.5: 投資部門別売買動向"""
    inv = data.get("jquants_investor_types", {})
    if not inv or inv.get("error"):
        return _build_missing_section("6.5", "投資部門別売買動向")

    foreign_net = inv.get("foreign_net", 0)
    individual_net = inv.get("individual_net", 0)
    trust_net = inv.get("trust_bank_net", 0)
    period = inv.get("period", "")
    badge_text = f'外国人 {_sign(foreign_net)}百万円 {"買い越し" if foreign_net > 0 else "売り越し"}'

    lines = ['<!-- ===== 6.5 投資部門別 ===== -->']
    lines.append('<div class="section">')
    lines.append(f'  <h2>投資部門別売買動向 <span class="badge {"badge-emerald" if foreign_net > 0 else "badge-rose"}">{badge_text}</span> <span class="evidence-label evidence-fact">J-Quants API</span></h2>')

    lines.append('  <table><thead><tr><th>投資部門</th><th class="r">売買差額（百万円）</th><th>備考</th></tr></thead><tbody>')
    lines.append(f'    <tr{_row_class(foreign_net)}><td><strong>外国人</strong></td><td class="r {_css_class(foreign_net)}"><strong>{_sign(foreign_net)}</strong></td><td>{_e(period)}週。買{_f(inv.get("foreign_buy"), 0)} / 売{_f(inv.get("foreign_sell"), 0)}</td></tr>')
    lines.append(f'    <tr{_row_class(individual_net)}><td>個人</td><td class="r {_css_class(individual_net)}">{_sign(individual_net)}</td><td></td></tr>')
    lines.append(f'    <tr{_row_class(trust_net)}><td>信託銀行</td><td class="r {_css_class(trust_net)}">{_sign(trust_net)}</td><td></td></tr>')
    lines.append('  </tbody></table>')
    lines.append(f'  <p class="footnote"><span class="evidence-label evidence-fact">事実</span> 集計期間: {_e(period)}（公表日{_e(inv.get("pub_date", ""))}）</p>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_fx(data: dict) -> str:
    """セクション7: 為替"""
    ms = data.get("market_summary", {})
    usdjpy = ms.get("usdjpy", {})

    lines = ['<!-- ===== 7. 為替 ===== -->']
    lines.append('<div class="section">')

    if "error" in usdjpy:
        # エラー時もセクションを出す（欠落より明示の方が良い）
        lines.append('  <h2>為替 <span class="evidence-label evidence-unverified">データ取得失敗</span></h2>')
        lines.append('  <div class="alert-box alert-warning">USD/JPYデータ取得失敗。yfinance経由の取得に失敗した可能性あり。</div>')
        lines.append('</div>')
        return "\n".join(lines)

    src = _e(usdjpy.get("source", "yfinance"))
    lines.append(f'  <h2>為替: {_f(usdjpy.get("close"))}円 <span class="evidence-label evidence-fact">{src}</span></h2>')

    lines.append('  <table><thead><tr><th>タイミング</th><th class="r">USD/JPY</th><th>備考</th></tr></thead><tbody>')
    lines.append(f'    <tr class="highlight-row-amber"><td><strong>{_e(data.get("date", ""))}</strong></td><td class="r"><strong>{_f(usdjpy.get("close"))}</strong></td><td><strong>{_sign_pct(usdjpy.get("change_pct"))}</strong> <span class="evidence-label evidence-fact">{src}</span></td></tr>')
    lines.append('  </tbody></table>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_commodities(data: dict) -> str:
    """セクション7.5: コモディティ"""
    comm = data.get("commodities", {})
    if not comm:
        return _build_missing_section("7.5", "コモディティ")

    wti = comm.get("wti", {})
    gold = comm.get("gold", {})
    copper = comm.get("copper", {})

    lines = ['<!-- ===== 7.5 コモディティ ===== -->']
    lines.append('<div class="section">')
    lines.append(f'  <h2>コモディティ <span class="badge badge-amber">WTI ${_f(wti.get("close"))}({_sign_pct(wti.get("change_pct"))}) Gold ${_f(gold.get("close"))}({_sign_pct(gold.get("change_pct"))})</span> <span class="evidence-label evidence-fact">事実</span></h2>')

    lines.append('  <table><thead><tr><th>商品</th><th class="r">最新</th><th class="r">変化率</th><th>備考</th></tr></thead><tbody>')
    for name, d in [("WTI原油 (CL=F)", wti), ("金先物 (GC=F)", gold), ("銅先物 (HG=F)", copper)]:
        prev = d.get("prev_close")
        prev_str = f'前日${_f(prev)}→${_f(d.get("close"))}' if prev else ""
        lines.append(f'    <tr><td>{name}</td><td class="r">${_f(d.get("close"))}</td><td class="r {_css_class(d.get("change_pct"))}">{_sign_pct(d.get("change_pct"))}</td><td>{prev_str}</td></tr>')
    lines.append('  </tbody></table>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_rates(data: dict) -> str:
    """セクション7.7: 金利動向"""
    rates = data.get("rates", {})
    if not rates:
        return _build_missing_section("7.7", "金利動向")

    jgb = rates.get("jgb10y", {})
    us10y = rates.get("us10y", {})
    diff = rates.get("rate_diff_us_jp")
    call_rate = rates.get("overnight_call", {})

    lines = ['<!-- ===== 7.7 金利動向 ===== -->']
    lines.append('<div class="section">')
    lines.append('  <h2>金利動向 <span class="evidence-label evidence-fact">事実</span></h2>')

    # JGB
    if jgb and not jgb.get("error"):
        lines.append('  <h3>JGB 10年国債利回り <span class="evidence-label evidence-fact">財務省CSV</span></h3>')
        lines.append('  <table><thead><tr><th>日付</th><th class="r">利回り (%)</th></tr></thead><tbody>')
        lines.append(f'    <tr class="highlight-row-amber"><td><strong>{_e(jgb.get("date", ""))}</strong></td><td class="r"><strong>{_e(jgb.get("value", "--"))}</strong></td></tr>')
        lines.append('  </tbody></table>')

    # US10Y
    if us10y and not us10y.get("error"):
        lines.append('  <h3>米国10年債利回り <span class="evidence-label evidence-fact">yfinance</span></h3>')
        lines.append('  <table><thead><tr><th>日付</th><th class="r">利回り (%)</th><th class="r">変化率</th></tr></thead><tbody>')
        lines.append(f'    <tr class="highlight-row-amber"><td><strong>{_e(data.get("date", ""))}</strong></td><td class="r"><strong>{_f(us10y.get("close"), 3)}</strong></td><td class="r {_css_class(us10y.get("change_pct"))}"><strong>{_sign_pct(us10y.get("change_pct"))}</strong></td></tr>')
        lines.append('  </tbody></table>')

    # 金利差 + コール
    lines.append('  <div class="grid-2" style="margin-top:12px;">')
    if diff is not None:
        lines.append(f'    <div class="stat-card"><div class="label">日米10年金利差</div><div class="value num-neutral">{_f(diff, 3)}%</div>')
        lines.append(f'      <div class="sub">US10Y {_f(us10y.get("close"), 3)}% - JGB10Y {_e(jgb.get("value", "--"))}% <span class="evidence-label evidence-fact">事実</span></div></div>')
    if call_rate and not call_rate.get("error"):
        lines.append(f'    <div class="stat-card"><div class="label">短期金利</div><div class="value num-neutral">{_e(call_rate.get("rate", "--"))}%</div>')
        lines.append(f'      <div class="sub">無担保コールO/N ({_e(call_rate.get("date", ""))}) <span class="evidence-label evidence-fact">BOJ API fm01</span></div></div>')
    lines.append('  </div>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_margin(data: dict) -> str:
    """セクション7.8: 信用残"""
    margin = data.get("jquants_margin", {})
    if not margin or margin.get("error"):
        return _build_missing_section("7.8", "信用残・需給環境")

    lines = ['<!-- ===== 7.8 信用残 ===== -->']
    lines.append('<div class="section">')
    lines.append('  <h2>信用残・需給環境 <span class="evidence-label evidence-fact">J-Quants API</span></h2>')

    sl_ratio = margin.get("aggregate_sl_ratio", "--")
    alert_count = margin.get("alert_count", "--")

    lines.append('  <div class="grid-3">')
    lines.append(f'    <div class="stat-card"><div class="label">日々公表銘柄数</div><div class="value num-neutral">{_e(alert_count)}</div>')
    lines.append(f'      <div class="sub">日々公表対象銘柄 <span class="evidence-label evidence-fact">jquants</span></div></div>')
    lines.append(f'    <div class="stat-card"><div class="label">信用倍率（日々公表分）</div><div class="value" style="color:var(--amber);">{_e(sl_ratio)}</div>')
    lines.append(f'      <div class="sub">買残 / 売残 <span class="evidence-label evidence-fact">jquants</span></div></div>')
    balance = "買い優勢" if (isinstance(sl_ratio, (int, float)) and sl_ratio > 1) else "売り優勢" if (isinstance(sl_ratio, (int, float)) and sl_ratio < 1) else "均衡"
    lines.append(f'    <div class="stat-card"><div class="label">需給バランス</div><div class="value num-neutral" style="font-size:1.2rem;">{balance}</div>')
    lines.append(f'      <div class="sub">信用倍率{_e(sl_ratio)}倍</div></div>')
    lines.append('  </div>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_grok(data: dict) -> str:
    """セクション8: Grok選定結果"""
    grok = data.get("grok", {})
    if not grok or not grok.get("details"):
        return _build_missing_section("8", "本日のGrok選定")

    details = grok.get("details", [])
    summary = grok.get("summary", {})
    bucket_dist = grok.get("bucket_distribution", {})
    total = grok.get("total", 0)

    short_total = summary.get("short_bucket_total", 0)
    short_win = summary.get("short_win", 0)
    short_lose = summary.get("short_lose", 0)
    short_count = summary.get("short_count", 0)

    lines = ['<!-- ===== 8. Grok選定結果 ===== -->']
    lines.append('<div class="section">')
    short_wr = f'{short_win / short_count * 100:.0f}%' if short_count > 0 else "--"
    lines.append(f'  <h2>本日のGrok選定 {total}銘柄 バケット評価 <span class="badge badge-emerald">SHORT勝率{short_wr} {_sign(short_total)}円</span> <span class="evidence-label evidence-fact">事実</span></h2>')

    # バケット分布カード
    # 各バケット集計
    buckets: dict[str, dict] = {}
    for d in details:
        b = d.get("bucket", "")
        if b not in buckets:
            buckets[b] = {"total": 0, "win": 0, "lose": 0, "draw": 0, "pl": 0}
        buckets[b]["total"] += 1
        sr = d.get("short_result", 0) or 0
        if b == "LONG":
            # LONGはロング損益 = -(short_result) (short_resultは空売り損益なのでロングは反転)
            pl = -sr
        else:
            pl = sr
        buckets[b]["pl"] += pl
        label = d.get("short_result_label", "")
        if b == "LONG":
            # LONGバケットはロング視点で判定
            if pl > 0:
                buckets[b]["win"] += 1
            elif pl < 0:
                buckets[b]["lose"] += 1
            else:
                buckets[b]["draw"] += 1
        else:
            if label == "WIN":
                buckets[b]["win"] += 1
            elif label == "LOSE":
                buckets[b]["lose"] += 1
            else:
                buckets[b]["draw"] += 1

    lines.append('  <h3>バケット別 分布</h3>')
    lines.append('  <div class="grid-3">')
    for bname, blabel, eval_type in [("SHORT", "SHORTバケット（ショート評価）", "short"), ("DISC", "DISCバケット（参考・ショート）", "short"), ("LONG", "LONGバケット（ロング評価）", "long")]:
        b = buckets.get(bname, {"total": 0, "win": 0, "lose": 0, "draw": 0, "pl": 0})
        pl_cls = _css_class(b["pl"])
        record = f'{b["total"]}銘柄 {b["win"]}勝{b["lose"]}敗'
        if b["draw"]:
            record += f'{b["draw"]}分'
        wr = f'（勝率{b["win"] / b["total"] * 100:.0f}%）' if b["total"] > 0 else ""
        lines.append(f'    <div class="stat-card"><div class="label">{blabel}</div>')
        lines.append(f'      <div class="value {pl_cls}">{_sign(b["pl"])}円</div>')
        lines.append(f'      <div class="sub">{record}{wr}</div></div>')
    lines.append('  </div>')

    # 全銘柄テーブル (prob昇順)
    sorted_details = sorted(details, key=lambda x: x.get("prob", 0))
    lines.append('  <h3>全銘柄 prob昇順 <span class="evidence-label evidence-fact">事実</span></h3>')
    lines.append('  <table><thead><tr><th>銘柄</th><th>Bucket</th><th class="r">prob</th><th>空売り区分</th><th class="r">買値</th><th class="r">損益</th><th>結果</th></tr></thead><tbody>')

    current_bucket = None
    for d in sorted_details:
        bucket = d.get("bucket", "")
        if bucket != current_bucket:
            current_bucket = bucket
            b = buckets.get(bucket, {})
            if bucket == "SHORT":
                color = "var(--rose)"
                bg_rgb = "251,113,133"
            elif bucket == "DISC":
                color = "var(--amber)"
                bg_rgb = "251,191,36"
            else:
                color = "var(--blue)"
                bg_rgb = "96,165,250"
            bucket_header = f'{_e(bucket)} {b.get("total", 0)}銘柄 {b.get("win", 0)}勝{b.get("lose", 0)}敗 = {_sign(b.get("pl", 0))}円'
            if bucket == "LONG":
                bucket_header += "（ロング損益）"
            lines.append(f'    <tr style="background:rgba({bg_rgb},0.03);"><td colspan="7" style="font-size:0.75rem;color:{color};font-weight:600;padding:4px 12px;border:none;">{bucket_header}</td></tr>')

        ticker = d.get("ticker", "").replace(".T", "")
        name = d.get("stock_name", "")
        prob = d.get("prob")
        buy = d.get("buy_price")
        sr = d.get("short_result", 0) or 0
        label = d.get("short_result_label", "")
        cat = d.get("short_category", "")

        if bucket == "LONG":
            pl = -sr
            result_label = "WIN" if pl > 0 else "LOSE" if pl < 0 else "DRAW"
        else:
            pl = sr
            result_label = label

        pl_cls = _css_class(pl)
        res_cls = "num-pos" if result_label == "WIN" else "num-neg" if result_label == "LOSE" else "num-neutral"
        lines.append(f'    <tr><td>{_e(name)} ({_e(ticker)})</td><td>{_e(bucket)}</td><td class="r">{_f(prob, 3)}</td><td>{_e(cat)}</td><td class="r">{_f(buy, 0)}</td><td class="r {pl_cls}">{_sign(pl)}</td><td class="{res_cls}">{_e(result_label)}</td></tr>')

    lines.append('  </tbody></table>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_calendar(data: dict) -> str:
    """セクション8.5: カレンダーアノマリー"""
    cal = data.get("calendar_anomaly", {})
    if not cal:
        return _build_missing_section("8.5", "カレンダーアノマリー")
    n225_dow = cal.get("n225_dow", {})
    if not n225_dow:
        return _build_missing_section("8.5", "カレンダーアノマリー")

    td = _e(cal.get("target_date", ""))
    wd = _e(cal.get("weekday", ""))
    woy = _e(cal.get("week_of_year", ""))
    mon = _e(cal.get("month", ""))

    lines = ['<!-- ===== 8.5 カレンダーアノマリー ===== -->']
    lines.append('<div class="section">')
    lines.append(f'  <h2>カレンダーアノマリー（翌営業日: {td} {wd}・Week{woy}) <span class="evidence-label evidence-fact">事実</span></h2>')

    lines.append('  <table><tr><th>条件</th><th class="r">平均リターン</th><th class="r">勝率</th><th class="r">サンプル数</th></tr>')
    lines.append(f'    <tr><td>{wd}・Week{woy}・{mon}月（全期間）</td><td class="r {_css_class(n225_dow.get("avg_all"))}">{_sign_pct(n225_dow.get("avg_all"))}</td><td class="r">{_f(n225_dow.get("win_rate_all"), 1)}%</td><td class="r">{_f(n225_dow.get("count_all"), 0)}</td></tr>')
    lines.append(f'    <tr class="highlight-row-amber"><td>{wd}・Week{woy}・{mon}月（直近5年）</td><td class="r {_css_class(n225_dow.get("avg_5y"))}">{_sign_pct(n225_dow.get("avg_5y"))}</td><td class="r">{_f(n225_dow.get("win_rate_5y"), 1)}%</td><td class="r">--</td></tr>')
    lines.append('  </table>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_sources(data: dict) -> str:
    """セクション10: 参照ソース"""
    date = data.get("date", "")
    lines = ['<!-- ===== 10. 参照ソース ===== -->']
    lines.append('<div class="section">')
    lines.append('  <h2>参照ソース</h2>')

    lines.append('  <h3>マーケットデータ <span class="evidence-label evidence-fact">事実</span></h3>')
    lines.append('  <ul class="source-list">')
    lines.append(f'    <li>日経平均・日経VI: report_data_{date}.json（parquet / investing.com経由）</li>')
    lines.append(f'    <li>TOPIX サブ指数: S3 topix_prices_max_1d.parquet</li>')
    lines.append(f'    <li>セクター: S3 sectors_prices_max_1d.parquet（33業種）</li>')
    lines.append(f'    <li>アジア市場: report_data_{date}.json（parquet経由）</li>')
    lines.append(f'    <li>コモディティ・NKD先物: report_data_{date}.json（yfinance経由）</li>')
    lines.append(f'    <li>Grok選定: report_data_{date}.json + S3 grok_trending.parquet + grok_trending_archive</li>')
    lines.append(f'    <li>カレンダーアノマリー: report_data_{date}.json（market_anomaly.parquet経由）</li>')
    lines.append('  </ul>')

    lines.append('  <h3>J-Quants API <span class="evidence-label evidence-fact">事実</span></h3>')
    lines.append('  <ul class="source-list">')
    lines.append(f'    <li>売買代金上位・値上がり率/値下がり率: jquants_volume_leaders</li>')
    lines.append(f'    <li>市場別騰落数: jquants_breadth</li>')
    lines.append(f'    <li>投資部門別: jquants_investor_types</li>')
    lines.append(f'    <li>信用残: jquants_margin</li>')
    lines.append('  </ul>')

    lines.append('  <h3>yfinance・外部 <span class="evidence-label evidence-fact">事実</span></h3>')
    lines.append('  <ul class="source-list">')
    lines.append(f'    <li>USD/JPY: yfinance / currency_prices_max_1d.parquet</li>')
    lines.append(f'    <li>米国10年債: yfinance ^TNX</li>')
    lines.append(f'    <li>騰落銘柄数・売買代金: <a href="https://www.nikkei.com/markets/kabu/japanidx/" target="_blank">日経電子版</a></li>')
    lines.append(f'    <li>日経VI: <a href="https://www.investing.com/indices/nikkei-volatility" target="_blank">investing.com JNIVE</a></li>')
    lines.append('  </ul>')

    lines.append('  <h3>財務省・日銀 <span class="evidence-label evidence-fact">事実</span></h3>')
    lines.append('  <ul class="source-list">')
    lines.append(f'    <li>国債金利情報: <a href="https://www.mof.go.jp/jgbs/reference/interest_rate/" target="_blank">財務省 jgbcm.csv</a></li>')
    lines.append(f'    <li>無担保コールO/N: BOJ API fm01</li>')
    lines.append('  </ul>')

    lines.append('</div>')
    return "\n".join(lines)


def _build_footer(data: dict) -> str:
    """フッター"""
    date = data.get("date", "")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"""<footer>
  Auto-generated by generate_report_html.py / report_data_{date}.json / {now}<br>
  Evidence labels: <span class="evidence-label evidence-fact">事実</span> = データソースで確認済 / <span class="evidence-label evidence-inference">推論</span> = Claude Codeで追記
</footer>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate_report(date: str) -> str:
    """report_data JSONからHTMLレポートを生成"""
    json_path = REPORT_DATA_DIR / f"report_data_{date}.json"
    if not json_path.exists():
        print(f"ERROR: {json_path} not found", file=sys.stderr)
        sys.exit(1)

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    sections = [
        f'<!DOCTYPE html>\n<html lang="ja">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width, initial-scale=1.0">',
        f'<title>マーケット振り返り {date}</title>',
        f'<style>\n{CSS}\n</style>',
        '</head>\n<body>\n',
        _build_header(data),
        _build_market_summary(data),
        _build_placeholder("2", "日中タイムライン"),
        _build_placeholder("3", "要因分析"),
        _build_volume_leaders(data),
        _build_sectors(data),
        _build_divergence(data),
        _build_foreign_markets(data),
        _build_investor_types(data),
        _build_fx(data),
        _build_commodities(data),
        _build_rates(data),
        _build_margin(data),
        _build_grok(data),
        _build_calendar(data),
        _build_placeholder("9", "今後の判断材料"),
        _build_sources(data),
        _build_placeholder("11", "結論"),
        _build_footer(data),
        '\n</body>\n</html>',
    ]

    return "\n\n".join(s for s in sections if s)


def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_report_html.py YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    date = sys.argv[1]
    html = generate_report(date)

    out_path = REPORTS_DIR / f"market_analysis_{date.replace('-', '')}.html"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"Report generated: {out_path}")
    print(f"  Size: {out_path.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
