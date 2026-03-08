#!/usr/bin/env python3
"""
Grok SHORT戦略分析: grok_rankベースのTop N別ショート成績
Phase2（大引け売り）の符号を反転してSHORT利益を算出
除0 = 在庫0株（shortable=False）を除外するトグル
"""
import pandas as pd
from pathlib import Path
import json

ARCHIVE = Path(__file__).resolve().parent.parent / "data/parquet/backtest/grok_trending_archive.parquet"
OUTPUT = Path(__file__).resolve().parent.parent / "data/reports/grok_short_analysis.html"


def calc_tier(sub: pd.DataFrame, label: str) -> dict:
    total = len(sub)
    if total == 0:
        return {"tier": label, "total": 0, "wins": 0, "losses": 0, "wr": 0,
                "total_profit": 0, "avg_profit": 0, "median_profit": 0, "best": 0, "worst": 0}
    return {
        "tier": label,
        "total": total,
        "wins": int(sub["short_win"].sum()),
        "losses": total - int(sub["short_win"].sum()),
        "wr": sub["short_win"].mean() * 100,
        "total_profit": sub["short_profit"].sum(),
        "avg_profit": sub["short_profit"].mean(),
        "median_profit": sub["short_profit"].median(),
        "best": sub["short_profit"].max(),
        "worst": sub["short_profit"].min(),
    }


def analyze():
    df = pd.read_parquet(ARCHIVE)
    # df = df[df["backtest_date"] >= "2026-01-01"].copy()
    df = df.copy()  # 全期間
    df["short_profit"] = -df["profit_per_100_shares_phase2"]
    df["short_return"] = -df["phase2_return"]
    df["short_win"] = df["short_profit"] > 0
    # shortable列: 在庫ありかどうか
    # 12/22以前は在庫データなし("-")→全て在庫ありとして扱う
    if "shortable" not in df.columns:
        df["shortable"] = True
    else:
        df.loc[df["backtest_date"] < "2025-12-22", "shortable"] = True

    tiers = [3, 5, 7, 10]

    # 全銘柄 & 在庫ありのみ、両方計算
    results_all = []
    results_shortable = []
    for n in tiers:
        sub = df[df["grok_rank"] <= n]
        results_all.append(calc_tier(sub, f"Top {n}"))
        results_shortable.append(calc_tier(sub[sub["shortable"]], f"Top {n}"))
    results_all.append(calc_tier(df, "ALL"))
    results_shortable.append(calc_tier(df[df["shortable"]], "ALL"))

    # ランク別（個別 rank 1, 2, 3, ... 10, 11+）
    rank_all = []
    rank_shortable = []
    max_rank = int(df["grok_rank"].max()) if len(df) > 0 else 10
    for r in range(1, min(max_rank, 15) + 1):
        sub = df[df["grok_rank"] == r]
        rank_all.append(calc_tier(sub, f"Rank {r}"))
        rank_shortable.append(calc_tier(sub[sub["shortable"]], f"Rank {r}"))
    if max_rank > 15:
        sub = df[df["grok_rank"] > 15]
        rank_all.append(calc_tier(sub, "Rank 16+"))
        rank_shortable.append(calc_tier(sub[sub["shortable"]], "Rank 16+"))

    # 日別累計 (各tier) — 全銘柄 & shortableのみ
    def make_daily_cum(src: pd.DataFrame):
        cum = {}
        for n in tiers:
            sub = src[src["grok_rank"] <= n].groupby("backtest_date")["short_profit"].sum().sort_index()
            cum[f"top{n}"] = sub.cumsum().reset_index().rename(columns={"backtest_date": "date", "short_profit": "cum"}).to_dict("records")
        sub_all = src.groupby("backtest_date")["short_profit"].sum().sort_index()
        cum["all"] = sub_all.cumsum().reset_index().rename(columns={"backtest_date": "date", "short_profit": "cum"}).to_dict("records")
        return cum

    daily_cum_all = make_daily_cum(df)
    daily_cum_shortable = make_daily_cum(df[df["shortable"]])

    # 日別勝敗 (Top5)
    def make_daily_detail(src: pd.DataFrame):
        detail = []
        for date, grp in src[src["grok_rank"] <= 5].groupby("backtest_date"):
            detail.append({
                "date": date,
                "profit": grp["short_profit"].sum(),
                "wins": int(grp["short_win"].sum()),
                "total": len(grp),
                "wr": grp["short_win"].mean() * 100,
            })
        return detail

    daily_all = make_daily_detail(df)
    daily_shortable = make_daily_detail(df[df["shortable"]])

    # Best/Worst trades
    top_trades = df.nlargest(10, "short_profit")[
        ["backtest_date", "ticker", "stock_name", "grok_rank", "buy_price", "daily_close", "short_profit", "short_return", "shortable"]
    ].to_dict("records")
    worst_trades = df.nsmallest(10, "short_profit")[
        ["backtest_date", "ticker", "stock_name", "grok_rank", "buy_price", "daily_close", "short_profit", "short_return", "shortable"]
    ].to_dict("records")

    shortable_count = int(df["shortable"].sum())
    total_count = len(df)

    return (results_all, results_shortable, daily_cum_all, daily_cum_shortable,
            daily_all, daily_shortable, top_trades, worst_trades, total_count, shortable_count,
            rank_all, rank_shortable)


def build_html(results_all, results_shortable, daily_cum_all, daily_cum_shortable,
               daily_all, daily_shortable, top_trades, worst_trades, total_count, shortable_count,
               rank_all, rank_shortable):
    def s(v):
        return f"+{v:,.0f}" if v >= 0 else f"{v:,.0f}"

    def gc(v):
        """gain/loss color class name"""
        return "gain" if v >= 0 else "loss"

    def tier_rows_html(results):
        rows = ""
        for t in results:
            rows += f"""
            <tr>
                <td class="font-bold">{t['tier']}</td>
                <td class="num">{t['total']}</td>
                <td class="num">{t['wins']}</td>
                <td class="num">{t['losses']}</td>
                <td class="num {gc(t['wr']-50)}">{t['wr']:.1f}%</td>
                <td class="num font-bold {gc(t['total_profit'])}">{s(t['total_profit'])}円</td>
                <td class="num {gc(t['avg_profit'])}">{s(t['avg_profit'])}円</td>
                <td class="num dim">{s(t['median_profit'])}円</td>
                <td class="num gain">{s(t['best'])}円</td>
                <td class="num loss">{s(t['worst'])}円</td>
            </tr>"""
        return rows

    def daily_rows_html(detail):
        rows = ""
        for d in sorted(detail, key=lambda x: x["date"], reverse=True):
            rows += f"""
            <tr>
                <td class="num">{d['date']}</td>
                <td class="num font-bold {gc(d['profit'])}">{s(d['profit'])}円</td>
                <td class="num">{d['wins']}/{d['total']}</td>
                <td class="num {gc(d['wr']-50)}">{d['wr']:.0f}%</td>
            </tr>"""
        return rows

    def trade_rows_html(trades):
        rows = ""
        for t in trades:
            ret_pct = t["short_return"] * 100
            shortable_mark = "" if t.get("shortable") else '<span class="badge-no">在庫無</span>'
            rows += f"""
            <tr>
                <td class="num">{t['backtest_date']}</td>
                <td>{t['stock_name']}<span class="dim ml">{t['ticker']}</span> {shortable_mark}</td>
                <td class="num">#{t['grok_rank']}</td>
                <td class="num">{t['buy_price']:,.0f}</td>
                <td class="num">{t['daily_close']:,.0f}</td>
                <td class="num font-bold {gc(t['short_profit'])}">{s(t['short_profit'])}円</td>
                <td class="num {gc(ret_pct)}">{ret_pct:+.2f}%</td>
            </tr>"""
        return rows

    # Pre-render both modes
    tier_all = tier_rows_html(results_all)
    tier_shortable = tier_rows_html(results_shortable)
    rank_all_html = tier_rows_html(rank_all)
    rank_shortable_html = tier_rows_html(rank_shortable)
    daily_all_html = daily_rows_html(daily_all)
    daily_shortable_html = daily_rows_html(daily_shortable)

    # KPI for both
    def kpi_data(results):
        t5 = results[1]  # Top5
        al = results[-1]  # ALL
        return t5, al

    t5_all, al_all = kpi_data(results_all)
    t5_short, al_short = kpi_data(results_shortable)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GROK SHORT戦略分析 2026</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  :root {{
    --bg: oklch(0.145 0 0);
    --card: oklch(0.205 0 0);
    --border: oklch(1 0 0 / 10%);
    --text: oklch(0.985 0 0);
    --sub: oklch(0.708 0 0);
    --gain-color: #34d399;
    --loss-color: #fb7185;
    --accent: oklch(0.488 0.243 264.376);
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: "Helvetica Neue", "SF Pro Text", "Inter", "Roboto", ui-sans-serif,
      "Yu Gothic", "Hiragino Kaku Gothic ProN", "Hiragino Sans", "Meiryo",
      system-ui, -apple-system, "Segoe UI", Arial, "Noto Sans", sans-serif;
    background: var(--bg); color: var(--text); font-size: 13px; line-height: 1.6;
    -webkit-font-smoothing: antialiased;
  }}
  .container {{ max-width: 1100px; margin: 0 auto; padding: 24px 20px; }}

  /* Header */
  .header {{ border-bottom: 1px solid var(--border); padding-bottom: 16px; margin-bottom: 20px; }}
  .header h1 {{ font-size: 20px; font-weight: 700; letter-spacing: -0.3px; }}
  .header .sub {{ color: var(--sub); font-size: 12px; margin-top: 4px; }}

  /* Toggle */
  .toggle-bar {{
    display: flex; align-items: center; gap: 16px; margin-bottom: 20px;
    padding: 10px 14px; background: var(--card); border: 1px solid var(--border);
    border-radius: 8px;
  }}
  .toggle-bar label {{
    font-size: 12px; color: var(--sub); display: flex; align-items: center;
    gap: 6px; cursor: pointer; user-select: none;
  }}
  .toggle-bar label.active {{ color: var(--text); font-weight: 600; }}
  .toggle-bar input[type="checkbox"] {{ accent-color: var(--accent); width: 16px; height: 16px; }}
  .toggle-info {{ font-size: 11px; color: var(--sub); }}

  /* Tables */
  table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
  th {{
    background: oklch(0.269 0 0); color: var(--sub); font-size: 10px;
    letter-spacing: 0.05em; text-transform: uppercase;
    padding: 8px; text-align: left; border-bottom: 1px solid var(--border);
    white-space: nowrap; position: sticky; top: 0; z-index: 1;
  }}
  td {{ padding: 6px 8px; border-bottom: 1px solid var(--border); white-space: nowrap; }}
  tbody tr:hover {{ background: oklch(0.269 0 0); }}
  .num {{
    text-align: right; font-variant-numeric: tabular-nums;
    font-feature-settings: "tnum" 1, "lnum" 1;
  }}
  .font-bold {{ font-weight: 700; }}
  .dim {{ color: var(--sub); }}
  .ml {{ margin-left: 4px; }}
  .gain {{ color: var(--gain-color); }}
  .loss {{ color: var(--loss-color); }}

  /* Badge */
  .badge-no {{
    display: inline-block; font-size: 9px; padding: 1px 4px;
    background: oklch(0.577 0.245 27.325 / 20%); color: var(--loss-color);
    border-radius: 3px; margin-left: 4px; vertical-align: middle;
  }}

  /* Section */
  .section {{ margin-bottom: 24px; }}
  .section-title {{
    font-size: 14px; font-weight: 700; margin-bottom: 10px;
    padding-bottom: 6px; border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 8px;
  }}
  .section-title .count {{ font-size: 11px; color: var(--sub); font-weight: 400; }}

  /* Chart */
  .chart-wrap {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 16px; margin-bottom: 16px;
  }}
  .chart-wrap canvas {{ max-height: 300px; }}

  /* Grid */
  .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
  @media (max-width: 768px) {{ .grid-2 {{ grid-template-columns: 1fr; }} }}

  /* KPI strip */
  .kpi-strip {{
    display: grid; grid-template-columns: repeat(5, 1fr); gap: 8px; margin-bottom: 20px;
  }}
  .kpi {{
    padding: 14px 12px; text-align: center;
    background: var(--card); border: 1px solid var(--border); border-radius: 10px;
  }}
  .kpi .label {{ font-size: 10px; color: var(--sub); letter-spacing: 0.05em; margin-bottom: 6px; }}
  .kpi .value {{
    font-size: 20px; font-weight: 700;
    font-variant-numeric: tabular-nums; font-feature-settings: "tnum" 1;
  }}

  /* Table container */
  .table-wrap {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; overflow: hidden;
  }}
  .table-scroll {{ overflow-y: auto; }}

  /* Footer */
  .footer {{
    margin-top: 32px; padding-top: 12px; border-top: 1px solid var(--border);
    font-size: 10px; color: var(--sub); text-align: center; letter-spacing: 0.1em;
  }}

  @media (max-width: 640px) {{
    .kpi-strip {{ grid-template-columns: repeat(3, 1fr); }}
  }}
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <h1>GROK SHORT Analysis — Full Period</h1>
    <div class="sub">Phase2（大引け売り） / grok_rank別 Top N / SHORT利益 = -(phase2 profit) / 全{total_count}件（在庫あり{shortable_count}件）</div>
  </div>

  <!-- Toggle: 除0 = 在庫なし除外 -->
  <div class="toggle-bar">
    <label id="toggleLabel">
      <input type="checkbox" id="excludeZero" onchange="toggleMode()">
      除0（在庫0株を除外 / 12/22以降のみ対象）
    </label>
    <span class="toggle-info" id="toggleInfo">全{total_count}銘柄を表示中</span>
  </div>

  <!-- KPI: Top5 summary -->
  <div class="kpi-strip" id="kpiStrip">
    <!-- filled by JS -->
  </div>

  <!-- Tier Summary -->
  <div class="section">
    <div class="section-title">Tier別サマリー</div>
    <div class="table-wrap">
      <div style="overflow-x:auto;">
      <table>
        <thead>
          <tr>
            <th>Tier</th><th>件数</th><th>勝</th><th>負</th>
            <th>勝率</th><th>累計損益</th><th>平均</th><th>中央値</th><th>Best</th><th>Worst</th>
          </tr>
        </thead>
        <tbody id="tierBody">{tier_all}</tbody>
      </table>
      </div>
    </div>
  </div>

  <!-- Rank別サマリー -->
  <div class="section">
    <div class="section-title">ランク別サマリー</div>
    <div class="table-wrap">
      <div style="overflow-x:auto;">
      <table>
        <thead>
          <tr>
            <th>Rank</th><th>件数</th><th>勝</th><th>負</th>
            <th>勝率</th><th>累計損益</th><th>平均</th><th>中央値</th><th>Best</th><th>Worst</th>
          </tr>
        </thead>
        <tbody id="rankBody">{rank_all_html}</tbody>
      </table>
      </div>
    </div>
  </div>

  <!-- Cumulative Chart -->
  <div class="section">
    <div class="section-title">累計損益推移</div>
    <div class="chart-wrap">
      <canvas id="cumChart"></canvas>
    </div>
  </div>

  <!-- Daily + Trades -->
  <div class="grid-2">
    <div class="section">
      <div class="section-title">日別損益（Top5） <span class="count" id="dailyCount"></span></div>
      <div class="table-wrap">
        <div class="table-scroll" style="max-height:400px;">
        <table>
          <thead><tr><th>Date</th><th>損益</th><th>勝敗</th><th>勝率</th></tr></thead>
          <tbody id="dailyBody">{daily_all_html}</tbody>
        </table>
        </div>
      </div>
    </div>

    <div>
      <div class="section">
        <div class="section-title">Best SHORT Trades</div>
        <div class="table-wrap">
          <div style="overflow-x:auto;">
          <table>
            <thead><tr><th>Date</th><th>銘柄</th><th>#</th><th>始値</th><th>終値</th><th>損益</th><th>Ret</th></tr></thead>
            <tbody>{trade_rows_html(top_trades)}</tbody>
          </table>
          </div>
        </div>
      </div>
      <div class="section">
        <div class="section-title">Worst SHORT Trades</div>
        <div class="table-wrap">
          <div style="overflow-x:auto;">
          <table>
            <thead><tr><th>Date</th><th>銘柄</th><th>#</th><th>始値</th><th>終値</th><th>損益</th><th>Ret</th></tr></thead>
            <tbody>{trade_rows_html(worst_trades)}</tbody>
          </table>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div class="footer">GROK SHORT STRATEGY ANALYSIS — Full Period — PHASE2</div>
</div>

<script>
// Data for both modes
const MODE = {{
  all: {{
    tierHtml: `{tier_all}`,
    rankHtml: `{rank_all_html}`,
    dailyHtml: `{daily_all_html}`,
    cumData: {json.dumps(daily_cum_all, default=str)},
    kpi: {{
      t5_profit: {t5_all['total_profit']},
      t5_wr: {t5_all['wr']:.1f},
      t5_avg: {t5_all['avg_profit']:.0f},
      all_profit: {al_all['total_profit']},
      all_wr: {al_all['wr']:.1f},
    }},
    count: {total_count},
    label: '全{total_count}銘柄を表示中',
    dailyCount: '{len(daily_all)}日',
  }},
  shortable: {{
    tierHtml: `{tier_shortable}`,
    rankHtml: `{rank_shortable_html}`,
    dailyHtml: `{daily_shortable_html}`,
    cumData: {json.dumps(daily_cum_shortable, default=str)},
    kpi: {{
      t5_profit: {t5_short['total_profit']},
      t5_wr: {t5_short['wr']:.1f},
      t5_avg: {t5_short['avg_profit']:.0f},
      all_profit: {al_short['total_profit']},
      all_wr: {al_short['wr']:.1f},
    }},
    count: {shortable_count},
    label: '在庫あり{shortable_count}銘柄のみ表示',
    dailyCount: '{len(daily_shortable)}日',
  }},
}};

let chart = null;
const chartColors = {{
  top3: '#6366f1', top5: '#f59e0b', top7: '#8b5cf6', top10: '#06b6d4', all: '#6b7280',
}};

function sign(v) {{ return v >= 0 ? '+' + v.toLocaleString() : v.toLocaleString(); }}
function colorClass(v) {{ return v >= 0 ? 'gain' : 'loss'; }}

function renderKPI(kpi) {{
  const strip = document.getElementById('kpiStrip');
  const items = [
    ['TOP5 累計', kpi.t5_profit, '円'],
    ['TOP5 勝率', kpi.t5_wr, '%', true],
    ['TOP5 平均', kpi.t5_avg, '円'],
    ['ALL 累計', kpi.all_profit, '円'],
    ['ALL 勝率', kpi.all_wr, '%', true],
  ];
  strip.innerHTML = items.map(([label, val, unit, isWr]) => {{
    const cls = isWr ? colorClass(val - 50) : colorClass(val);
    const display = isWr ? val.toFixed(1) + unit : sign(Math.round(val)) + unit;
    return `<div class="kpi"><div class="label">${{label}}</div><div class="value ${{cls}}">${{display}}</div></div>`;
  }}).join('');
}}

function renderChart(cumData) {{
  const ctx = document.getElementById('cumChart').getContext('2d');
  if (chart) chart.destroy();

  const datasets = Object.entries(cumData).map(([key, data]) => ({{
    label: key.toUpperCase().replace('TOP', 'Top '),
    data: data.map(d => ({{ x: d.date, y: d.cum }})),
    borderColor: chartColors[key],
    backgroundColor: 'transparent',
    borderWidth: key === 'top5' ? 2.5 : 1.5,
    pointRadius: 0,
    tension: 0.3,
  }}));

  chart = new Chart(ctx, {{
    type: 'line',
    data: {{ datasets }},
    options: {{
      responsive: true,
      interaction: {{ intersect: false, mode: 'index' }},
      plugins: {{
        legend: {{
          position: 'top',
          labels: {{ color: '#a1a1aa', font: {{ size: 11 }}, boxWidth: 20, padding: 12 }},
        }},
        tooltip: {{
          backgroundColor: 'oklch(0.205 0 0)',
          borderColor: 'oklch(1 0 0 / 10%)',
          borderWidth: 1,
          titleColor: '#a1a1aa',
          bodyColor: '#e4e4e7',
          callbacks: {{ label: (c) => c.dataset.label + ': ¥' + c.parsed.y.toLocaleString() }},
        }},
      }},
      scales: {{
        x: {{
          type: 'category',
          ticks: {{ color: '#71717a', font: {{ size: 10 }}, maxRotation: 45, autoSkip: true, maxTicksLimit: 20 }},
          grid: {{ color: 'oklch(1 0 0 / 5%)' }},
        }},
        y: {{
          ticks: {{ color: '#71717a', font: {{ size: 10 }}, callback: (v) => '¥' + (v / 1000).toFixed(0) + 'k' }},
          grid: {{ color: 'oklch(1 0 0 / 5%)' }},
        }},
      }},
    }},
  }});
}}

function setMode(mode) {{
  const m = MODE[mode];
  document.getElementById('tierBody').innerHTML = m.tierHtml;
  document.getElementById('rankBody').innerHTML = m.rankHtml;
  document.getElementById('dailyBody').innerHTML = m.dailyHtml;
  document.getElementById('toggleInfo').textContent = m.label;
  document.getElementById('dailyCount').textContent = m.dailyCount;
  renderKPI(m.kpi);
  renderChart(m.cumData);
  const label = document.getElementById('toggleLabel');
  label.classList.toggle('active', mode === 'shortable');
}}

function toggleMode() {{
  const checked = document.getElementById('excludeZero').checked;
  setMode(checked ? 'shortable' : 'all');
}}

// Initial render
setMode('all');
</script>
</body>
</html>"""

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"Written: {OUTPUT}")


if __name__ == "__main__":
    results = analyze()
    build_html(*results)
