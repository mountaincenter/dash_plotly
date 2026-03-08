#!/usr/bin/env python3
"""
MAE/MFE分析HTMLレポート生成

granville_ifd_archive.parquet を読み込み、
SL幅と利確タイミングの妥当性を検証するHTMLレポートを生成する。
Plotly.js CDNでインタラクティブチャートを埋め込み。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent
ARCHIVE_PATH = BASE_DIR / "data" / "parquet" / "backtest" / "granville_ifd_archive.parquet"
OUTPUT_PATH = BASE_DIR / "data" / "reports" / "mae_mfe_analysis.html"

PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.0.min.js"


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def load_archive() -> pd.DataFrame:
    df = pd.read_parquet(str(ARCHIVE_PATH), engine="pyarrow")
    return df


def pf(wins: pd.Series, losses: pd.Series) -> float:
    """Profit Factor"""
    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())
    if gross_loss == 0:
        return float("inf") if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def fmt(v: float, decimals: int = 2) -> str:
    if pd.isna(v):
        return "—"
    return f"{v:,.{decimals}f}"


def fmt_pct(v: float, decimals: int = 2) -> str:
    if pd.isna(v):
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.{decimals}f}%"


def num_cls(v: float) -> str:
    if pd.isna(v):
        return "num-neutral"
    return "num-pos" if v >= 0 else "num-neg"


def to_json(obj: object) -> str:
    """Python→JS用JSON"""
    return json.dumps(obj, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def section_executive_summary(df: pd.DataFrame) -> str:
    n = len(df)
    wins = df[df["pnl_yen"] > 0]
    losses = df[df["pnl_yen"] <= 0]
    win_rate = len(wins) / n * 100
    profit_factor = pf(wins["pnl_yen"], losses["pnl_yen"])
    total_pnl = df["pnl_yen"].sum()
    sl_count = (df["exit_type"] == "SL").sum()
    sl_rate = sl_count / n * 100
    avg_mae = df["mae_pct"].mean()
    avg_mfe = df["mfe_pct"].mean()
    # MFE捕捉率（マクロ）: 平均実現リターン / 平均MFE
    avg_ret = df["ret_pct"].mean()
    capture_rate = avg_ret / avg_mfe * 100 if avg_mfe != 0 else 0.0
    # MFE>0から反転して損失のトレード数
    mfe_pos = df[df["mfe_pct"] > 0]
    mfe_pos_but_lost = mfe_pos[mfe_pos["pnl_yen"] <= 0]
    reversal_rate = len(mfe_pos_but_lost) / len(mfe_pos) * 100 if len(mfe_pos) > 0 else 0.0

    # ファインディング
    sl_trades_with_mfe = df[(df["exit_type"] == "SL") & (df["mfe_pct"] > 0)]
    sl_mfe_ratio = len(sl_trades_with_mfe) / sl_count * 100 if sl_count > 0 else 0

    return f"""
<div class="section">
  <h2>1. Executive Summary</h2>
  <div class="grid-4">
    <div class="stat-card">
      <div class="label">総トレード数</div>
      <div class="value">{n:,}</div>
    </div>
    <div class="stat-card">
      <div class="label">勝率</div>
      <div class="value {num_cls(win_rate - 50)}">{win_rate:.1f}%</div>
      <div class="sub">{len(wins):,}勝 / {len(losses):,}敗</div>
    </div>
    <div class="stat-card">
      <div class="label">Profit Factor</div>
      <div class="value {num_cls(profit_factor - 1)}">{fmt(profit_factor)}</div>
    </div>
    <div class="stat-card">
      <div class="label">総PnL</div>
      <div class="value {num_cls(total_pnl)}">¥{total_pnl:,.0f}</div>
      <div class="sub">100株基準</div>
    </div>
  </div>
  <div class="grid-4" style="margin-top:12px;">
    <div class="stat-card">
      <div class="label">SL率</div>
      <div class="value num-neg">{sl_rate:.1f}%</div>
      <div class="sub">{sl_count:,} / {n:,}</div>
    </div>
    <div class="stat-card">
      <div class="label">平均MAE</div>
      <div class="value num-neg">{fmt_pct(avg_mae)}</div>
    </div>
    <div class="stat-card">
      <div class="label">平均MFE</div>
      <div class="value num-pos">{fmt_pct(avg_mfe)}</div>
    </div>
    <div class="stat-card">
      <div class="label">MFE捕捉率</div>
      <div class="value">{capture_rate:.1f}%</div>
      <div class="sub">平均ret / 平均MFE</div>
    </div>
  </div>
  <div class="alert-box alert-info" style="margin-top:16px;">
    含み益を経験したトレード {len(mfe_pos):,}件のうち <strong>{reversal_rate:.1f}%</strong>（{len(mfe_pos_but_lost):,}件）が反転して損失で終了。利確ルールの改善余地あり。
  </div>
</div>
"""


def section_mae_distribution(df: pd.DataFrame) -> str:
    """Section 2: MAE分布分析"""
    # ヒストグラムデータ
    mae_vals = df["mae_pct"].dropna().tolist()

    # MAE帯別テーブル
    bins = [(-100, -5), (-5, -4), (-4, -3), (-3, -2), (-2, -1), (-1, 0.01)]
    labels = ["-5%以下", "-5〜-4%", "-4〜-3%", "-3〜-2%", "-2〜-1%", "-1〜0%"]

    rows_html = ""
    for (lo, hi), label in zip(bins, labels):
        mask = (df["mae_pct"] >= lo) & (df["mae_pct"] < hi)
        sub = df[mask]
        cnt = len(sub)
        if cnt == 0:
            rows_html += f"<tr><td>{label}</td><td class='r'>{cnt:,}</td><td class='r'>—</td><td class='r'>—</td><td class='r'>—</td></tr>"
            continue
        wr = (sub["pnl_yen"] > 0).sum() / cnt * 100
        avg_pnl = sub["pnl_yen"].mean()
        pct_of_total = cnt / len(df) * 100
        cls = num_cls(avg_pnl)
        rows_html += f"""<tr><td>{label}</td><td class='r'>{cnt:,}</td><td class='r'>{pct_of_total:.1f}%</td><td class='r'>{wr:.1f}%</td><td class='r {cls}'>¥{avg_pnl:,.0f}</td></tr>"""

    # insight: SL構造の説明
    sl_trades = df[df["exit_type"] == "SL"]
    sl_count = len(sl_trades)
    non_sl = df[df["exit_type"] != "SL"]
    # SL未発動トレード（MAE > -3%）の勝率
    non_sl_wr = (non_sl["pnl_yen"] > 0).sum() / len(non_sl) * 100 if len(non_sl) > 0 else 0
    # MAE -2〜-3%帯（SLギリギリ回避）の勝率
    near_sl = df[(df["mae_pct"] >= -3) & (df["mae_pct"] < -2)]
    near_sl_wr = (near_sl["pnl_yen"] > 0).sum() / len(near_sl) * 100 if len(near_sl) > 0 else 0
    insight = (
        f"MAE &lt; -3%の {sl_count:,}件は全てSL決済済み（勝率0%は当然）。"
        f"SL幅拡大の効果はSLなしバックテストで別途検証が必要。"
        f"参考: SL未発動トレード勝率 <strong>{non_sl_wr:.1f}%</strong>、"
        f"MAE -2〜-3%帯（SLギリギリ回避）勝率 <strong>{near_sl_wr:.1f}%</strong>。"
    )

    return f"""
<div class="section">
  <h2>2. MAE分布分析（SL幅の妥当性）</h2>
  <div id="chart-mae-hist"></div>
  <script>
  (function() {{
    var data = [{to_json(mae_vals)}];
    Plotly.newPlot('chart-mae-hist', [{{
      x: data[0], type: 'histogram',
      xbins: {{ start: -15, end: 1, size: 0.5 }},
      marker: {{ color: 'rgba(251,113,133,0.7)', line: {{ color: 'rgba(251,113,133,1)', width: 1 }} }},
      name: 'MAE分布'
    }}], {{
      template: 'plotly_dark',
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: {{ title: 'MAE (%)', gridcolor: '#27272a' }},
      yaxis: {{ title: '件数', gridcolor: '#27272a' }},
      margin: {{ t: 30, b: 50, l: 60, r: 20 }},
      shapes: [{{ type: 'line', x0: -3, x1: -3, y0: 0, y1: 1, yref: 'paper',
                  line: {{ color: '#fbbf24', width: 2, dash: 'dash' }} }}],
      annotations: [{{ x: -3, y: 1, yref: 'paper', text: 'SL -3%', showarrow: false,
                       font: {{ color: '#fbbf24', size: 12 }}, yanchor: 'bottom' }}]
    }}, {{ responsive: true }});
  }})();
  </script>
  <h3>MAE帯別統計</h3>
  <table>
    <thead><tr><th>MAE帯</th><th class="r">件数</th><th class="r">構成比</th><th class="r">勝率</th><th class="r">平均PnL</th></tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
  <div class="alert-box alert-warning" style="margin-top:12px;">{insight}</div>
</div>
"""


def section_mfe_distribution(df: pd.DataFrame) -> str:
    """Section 3: MFE分布分析"""
    mfe_vals = df["mfe_pct"].dropna().tolist()

    bins = [(0, 1), (1, 2), (2, 3), (3, 5), (5, 10), (10, 100)]
    labels = ["0〜1%", "1〜2%", "2〜3%", "3〜5%", "5〜10%", "10%以上"]

    rows_html = ""
    for (lo, hi), label in zip(bins, labels):
        mask = (df["mfe_pct"] >= lo) & (df["mfe_pct"] < hi)
        sub = df[mask]
        cnt = len(sub)
        if cnt == 0:
            rows_html += f"<tr><td>{label}</td><td class='r'>{cnt:,}</td><td class='r'>—</td><td class='r'>—</td></tr>"
            continue
        avg_ret = sub["ret_pct"].mean()
        avg_mfe_band = sub["mfe_pct"].mean()
        cap = avg_ret / avg_mfe_band * 100 if avg_mfe_band > 0 else 0.0
        cls = num_cls(avg_ret)
        rows_html += f"""<tr><td>{label}</td><td class='r'>{cnt:,}</td><td class='r {cls}'>{fmt_pct(avg_ret)}</td><td class='r'>{cap:.1f}%</td></tr>"""

    # insight: 利益の取りこぼし
    mfe_pos = df[df["mfe_pct"] > 0]
    if len(mfe_pos) > 0:
        avg_mfe_pos = mfe_pos["mfe_pct"].mean()
        avg_ret_pos = mfe_pos["ret_pct"].mean()
        lost = avg_mfe_pos - avg_ret_pos
        insight = f"MFE&gt;0のトレード平均: 最大含み益 {fmt_pct(avg_mfe_pos)} に対し実現 {fmt_pct(avg_ret_pos)}。平均 <strong>{fmt_pct(lost)}</strong> の利益を取りこぼし。"
    else:
        insight = "MFE>0のトレードなし。"

    return f"""
<div class="section">
  <h2>3. MFE分布分析（利確タイミング）</h2>
  <div id="chart-mfe-hist"></div>
  <script>
  (function() {{
    var data = [{to_json(mfe_vals)}];
    Plotly.newPlot('chart-mfe-hist', [{{
      x: data[0], type: 'histogram',
      xbins: {{ start: 0, end: 30, size: 0.5 }},
      marker: {{ color: 'rgba(52,211,153,0.7)', line: {{ color: 'rgba(52,211,153,1)', width: 1 }} }},
      name: 'MFE分布'
    }}], {{
      template: 'plotly_dark',
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: {{ title: 'MFE (%)', gridcolor: '#27272a' }},
      yaxis: {{ title: '件数', gridcolor: '#27272a' }},
      margin: {{ t: 30, b: 50, l: 60, r: 20 }}
    }}, {{ responsive: true }});
  }})();
  </script>
  <h3>MFE帯別統計</h3>
  <table>
    <thead><tr><th>MFE帯</th><th class="r">件数</th><th class="r">平均実現リターン</th><th class="r">MFE捕捉率</th></tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
  <div class="alert-box alert-info" style="margin-top:12px;">{insight}</div>
</div>
"""


def section_mae_vs_mfe_scatter(df: pd.DataFrame) -> str:
    """Section 4: MAE vs MFE 散布図"""
    # exit_type別に分割
    exit_types = df["exit_type"].unique().tolist()
    colors = {
        "SL": "rgba(251,113,133,0.6)",
        "SMA20_touch": "rgba(52,211,153,0.6)",
        "dead_cross": "rgba(96,165,250,0.6)",
        "time_cut": "rgba(251,191,36,0.6)",
        "expire": "rgba(167,139,250,0.6)",
    }

    traces_js = []
    for et in exit_types:
        sub = df[df["exit_type"] == et]
        color = colors.get(et, "rgba(200,200,200,0.5)")
        traces_js.append(f"""{{
      x: {to_json(sub['mae_pct'].tolist())},
      y: {to_json(sub['mfe_pct'].tolist())},
      mode: 'markers',
      type: 'scatter',
      name: '{et}',
      marker: {{ color: '{color}', size: 3 }},
      text: {to_json(sub['ticker'].tolist())}
    }}""")

    traces_str = ",\n    ".join(traces_js)

    # SLトレードでMFE>0の割合
    sl_trades = df[df["exit_type"] == "SL"]
    sl_mfe_pos = sl_trades[sl_trades["mfe_pct"] > 0]
    sl_mfe_pct = len(sl_mfe_pos) / len(sl_trades) * 100 if len(sl_trades) > 0 else 0

    return f"""
<div class="section">
  <h2>4. MAE vs MFE 散布図</h2>
  <div id="chart-scatter"></div>
  <script>
  (function() {{
    Plotly.newPlot('chart-scatter', [
    {traces_str}
    ], {{
      template: 'plotly_dark',
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: {{ title: 'MAE (%)', gridcolor: '#27272a' }},
      yaxis: {{ title: 'MFE (%)', gridcolor: '#27272a' }},
      margin: {{ t: 30, b: 50, l: 60, r: 20 }},
      legend: {{ font: {{ size: 11 }} }},
      shapes: [
        {{ type: 'line', x0: -3, x1: -3, y0: 0, y1: 1, yref: 'paper',
           line: {{ color: '#fbbf24', width: 1.5, dash: 'dash' }} }}
      ]
    }}, {{ responsive: true }});
  }})();
  </script>
  <div class="alert-box alert-warning" style="margin-top:12px;">
    SLトレード {len(sl_trades):,}件のうち <strong>{sl_mfe_pct:.1f}%</strong>（{len(sl_mfe_pos):,}件）はSL前にMFE&gt;0を記録。含み益からSLへ反転したケースが多い。
  </div>
</div>
"""


def section_time_analysis(df: pd.DataFrame) -> str:
    """Section 5: 時間軸分析"""
    # exit_day別統計
    ed_stats = df.groupby("exit_day").agg(
        count=("pnl_yen", "size"),
        avg_pnl=("pnl_yen", "mean"),
        total_pnl=("pnl_yen", "sum"),
    ).reset_index()
    ed_stats = ed_stats[ed_stats["exit_day"] <= 60]

    exit_days = ed_stats["exit_day"].tolist()
    exit_counts = ed_stats["count"].tolist()
    exit_avg_pnl = [round(v, 0) for v in ed_stats["avg_pnl"].tolist()]

    # バケット統計
    buckets = [(0, 0), (1, 2), (3, 6), (7, 13), (14, 29), (30, 60)]
    bucket_labels = ["0日（当日）", "1-2日", "3-6日", "7-13日", "14-29日", "30-60日"]

    bucket_rows = ""
    for (lo, hi), label in zip(buckets, bucket_labels):
        mask = (df["exit_day"] >= lo) & (df["exit_day"] <= hi)
        sub = df[mask]
        cnt = len(sub)
        if cnt == 0:
            bucket_rows += f"<tr><td>{label}</td><td class='r'>—</td><td class='r'>—</td><td class='r'>—</td><td class='r'>—</td></tr>"
            continue
        wr = (sub["pnl_yen"] > 0).sum() / cnt * 100
        avg_pnl = sub["pnl_yen"].mean()
        avg_ret = sub["ret_pct"].mean()
        cls = num_cls(avg_pnl)
        bucket_rows += f"""<tr><td>{label}</td><td class='r'>{cnt:,}</td><td class='r'>{wr:.1f}%</td><td class='r {cls}'>{fmt_pct(avg_ret)}</td><td class='r {cls}'>¥{avg_pnl:,.0f}</td></tr>"""

    # mfe_day分布
    mfe_day_stats = df.groupby("mfe_day").agg(count=("pnl_yen", "size")).reset_index()
    mfe_day_stats = mfe_day_stats[mfe_day_stats["mfe_day"] <= 60]
    mfe_days = mfe_day_stats["mfe_day"].tolist()
    mfe_day_counts = mfe_day_stats["count"].tolist()

    return f"""
<div class="section">
  <h2>5. 時間軸分析（保有期間）</h2>
  <div class="grid-2">
    <div>
      <h3>Exit Day別 件数・平均PnL</h3>
      <div id="chart-exit-day"></div>
    </div>
    <div>
      <h3>MFE Day分布（利益ピーク日）</h3>
      <div id="chart-mfe-day"></div>
    </div>
  </div>
  <script>
  (function() {{
    Plotly.newPlot('chart-exit-day', [
      {{ x: {to_json(exit_days)}, y: {to_json(exit_counts)}, type: 'bar', name: '件数',
         marker: {{ color: 'rgba(96,165,250,0.7)' }}, yaxis: 'y' }},
      {{ x: {to_json(exit_days)}, y: {to_json(exit_avg_pnl)}, type: 'scatter', mode: 'lines+markers', name: '平均PnL',
         marker: {{ color: '#fbbf24', size: 4 }}, line: {{ color: '#fbbf24', width: 1.5 }}, yaxis: 'y2' }}
    ], {{
      template: 'plotly_dark',
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: {{ title: '保有日数', gridcolor: '#27272a' }},
      yaxis: {{ title: '件数', gridcolor: '#27272a', side: 'left' }},
      yaxis2: {{ title: '平均PnL (¥)', overlaying: 'y', side: 'right', gridcolor: 'rgba(0,0,0,0)' }},
      margin: {{ t: 10, b: 50, l: 60, r: 60 }},
      legend: {{ x: 0.5, y: 1.05, orientation: 'h', xanchor: 'center' }},
      barmode: 'overlay'
    }}, {{ responsive: true }});

    Plotly.newPlot('chart-mfe-day', [{{
      x: {to_json(mfe_days)}, y: {to_json(mfe_day_counts)}, type: 'bar',
      marker: {{ color: 'rgba(52,211,153,0.7)' }}, name: 'MFE日'
    }}], {{
      template: 'plotly_dark',
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: {{ title: 'MFE発生日（営業日）', gridcolor: '#27272a' }},
      yaxis: {{ title: '件数', gridcolor: '#27272a' }},
      margin: {{ t: 10, b: 50, l: 60, r: 20 }}
    }}, {{ responsive: true }});
  }})();
  </script>
  <h3>保有期間バケット別統計</h3>
  <table>
    <thead><tr><th>保有期間</th><th class="r">件数</th><th class="r">勝率</th><th class="r">平均リターン</th><th class="r">平均PnL</th></tr></thead>
    <tbody>{bucket_rows}</tbody>
  </table>
</div>
"""


def section_exit_type(df: pd.DataFrame) -> str:
    """Section 6: Exit Type別分析"""
    exit_order = ["SL", "SMA20_touch", "dead_cross", "time_cut", "expire"]
    rows_html = ""
    chart_labels = []
    chart_pnl = []
    chart_colors = []
    color_map = {
        "SL": "rgba(251,113,133,0.8)",
        "SMA20_touch": "rgba(52,211,153,0.8)",
        "dead_cross": "rgba(96,165,250,0.8)",
        "time_cut": "rgba(251,191,36,0.8)",
        "expire": "rgba(167,139,250,0.8)",
    }

    for et in exit_order:
        sub = df[df["exit_type"] == et]
        cnt = len(sub)
        if cnt == 0:
            continue
        wins = sub[sub["pnl_yen"] > 0]
        losses_sub = sub[sub["pnl_yen"] <= 0]
        wr = len(wins) / cnt * 100
        pf_val = pf(wins["pnl_yen"], losses_sub["pnl_yen"])
        avg_mae = sub["mae_pct"].mean()
        avg_mfe = sub["mfe_pct"].mean()
        total_pnl = sub["pnl_yen"].sum()
        avg_pnl = sub["pnl_yen"].mean()
        pf_str = f"{pf_val:.2f}" if pf_val != float("inf") else "∞"
        pnl_cls = num_cls(total_pnl)

        chart_labels.append(et)
        chart_pnl.append(int(total_pnl))
        chart_colors.append(color_map.get(et, "rgba(200,200,200,0.8)"))

        rows_html += f"""<tr>
          <td>{et}</td><td class='r'>{cnt:,}</td><td class='r'>{wr:.1f}%</td>
          <td class='r'>{pf_str}</td><td class='r num-neg'>{fmt_pct(avg_mae)}</td>
          <td class='r num-pos'>{fmt_pct(avg_mfe)}</td>
          <td class='r {pnl_cls}'>¥{total_pnl:,.0f}</td><td class='r {pnl_cls}'>¥{avg_pnl:,.0f}</td>
        </tr>"""

    # 未知のexit_type
    known = set(exit_order)
    other = df[~df["exit_type"].isin(known)]
    if len(other) > 0:
        for et in other["exit_type"].unique():
            sub = other[other["exit_type"] == et]
            cnt = len(sub)
            wins = sub[sub["pnl_yen"] > 0]
            losses_sub = sub[sub["pnl_yen"] <= 0]
            wr = len(wins) / cnt * 100
            pf_val = pf(wins["pnl_yen"], losses_sub["pnl_yen"])
            total_pnl = sub["pnl_yen"].sum()
            avg_pnl = sub["pnl_yen"].mean()
            pf_str = f"{pf_val:.2f}" if pf_val != float("inf") else "∞"
            pnl_cls = num_cls(total_pnl)
            rows_html += f"""<tr>
              <td>{et}</td><td class='r'>{cnt:,}</td><td class='r'>{wr:.1f}%</td>
              <td class='r'>{pf_str}</td><td class='r'>—</td><td class='r'>—</td>
              <td class='r {pnl_cls}'>¥{total_pnl:,.0f}</td><td class='r {pnl_cls}'>¥{avg_pnl:,.0f}</td>
            </tr>"""
            chart_labels.append(et)
            chart_pnl.append(int(total_pnl))
            chart_colors.append("rgba(200,200,200,0.8)")

    return f"""
<div class="section">
  <h2>6. Exit Type別分析</h2>
  <table>
    <thead><tr>
      <th>Exit Type</th><th class="r">件数</th><th class="r">勝率</th><th class="r">PF</th>
      <th class="r">平均MAE</th><th class="r">平均MFE</th><th class="r">総PnL</th><th class="r">平均PnL</th>
    </tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
  <h3>Exit Type別 PnL貢献</h3>
  <div id="chart-exit-pnl"></div>
  <script>
  (function() {{
    Plotly.newPlot('chart-exit-pnl', [{{
      x: {to_json(chart_labels)},
      y: {to_json(chart_pnl)},
      type: 'bar',
      marker: {{ color: {to_json(chart_colors)} }}
    }}], {{
      template: 'plotly_dark',
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: {{ gridcolor: '#27272a' }},
      yaxis: {{ title: 'PnL (¥)', gridcolor: '#27272a' }},
      margin: {{ t: 10, b: 50, l: 80, r: 20 }}
    }}, {{ responsive: true }});
  }})();
  </script>
</div>
"""


def section_overnight_gap(df: pd.DataFrame) -> str:
    """Section 7: オーバーナイトギャップ分析"""
    gap_vals = df["overnight_gap_pct"].dropna().tolist()

    bins = [(-100, -2), (-2, -1), (-1, -0.5), (-0.5, 0), (0, 0.5), (0.5, 1), (1, 2), (2, 100)]
    labels = ["-2%以下", "-2〜-1%", "-1〜-0.5%", "-0.5〜0%", "0〜+0.5%", "+0.5〜+1%", "+1〜+2%", "+2%以上"]

    rows_html = ""
    for (lo, hi), label in zip(bins, labels):
        mask = (df["overnight_gap_pct"] >= lo) & (df["overnight_gap_pct"] < hi)
        sub = df[mask]
        cnt = len(sub)
        if cnt == 0:
            rows_html += f"<tr><td>{label}</td><td class='r'>{cnt:,}</td><td class='r'>—</td><td class='r'>—</td></tr>"
            continue
        sl_rate = (sub["exit_type"] == "SL").sum() / cnt * 100
        avg_ret = sub["ret_pct"].mean()
        cls = num_cls(avg_ret)
        rows_html += f"""<tr><td>{label}</td><td class='r'>{cnt:,}</td><td class='r'>{sl_rate:.1f}%</td><td class='r {cls}'>{fmt_pct(avg_ret)}</td></tr>"""

    # GDリスク定量化
    gd_trades = df[df["overnight_gap_pct"] < -1]
    gd_sl_rate = (gd_trades["exit_type"] == "SL").sum() / len(gd_trades) * 100 if len(gd_trades) > 0 else 0
    gu_trades = df[df["overnight_gap_pct"] > 1]
    gu_avg_ret = gu_trades["ret_pct"].mean() if len(gu_trades) > 0 else 0

    return f"""
<div class="section">
  <h2>7. オーバーナイトギャップ分析</h2>
  <div id="chart-gap-hist"></div>
  <script>
  (function() {{
    Plotly.newPlot('chart-gap-hist', [{{
      x: {to_json(gap_vals)}, type: 'histogram',
      xbins: {{ start: -5, end: 5, size: 0.25 }},
      marker: {{ color: 'rgba(167,139,250,0.7)', line: {{ color: 'rgba(167,139,250,1)', width: 1 }} }},
      name: 'Gap分布'
    }}], {{
      template: 'plotly_dark',
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: {{ title: 'オーバーナイトギャップ (%)', gridcolor: '#27272a' }},
      yaxis: {{ title: '件数', gridcolor: '#27272a' }},
      margin: {{ t: 30, b: 50, l: 60, r: 20 }}
    }}, {{ responsive: true }});
  }})();
  </script>
  <h3>ギャップ帯別統計</h3>
  <table>
    <thead><tr><th>ギャップ帯</th><th class="r">件数</th><th class="r">SL率</th><th class="r">平均リターン</th></tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
  <div class="alert-box alert-danger" style="margin-top:12px;">
    GD -1%以上のトレード {len(gd_trades):,}件: SL率 <strong>{gd_sl_rate:.1f}%</strong>。
    GU +1%以上のトレード {len(gu_trades):,}件: 平均リターン <strong>{fmt_pct(gu_avg_ret)}</strong>。
  </div>
</div>
"""


def section_signal_type(df: pd.DataFrame) -> str:
    """Section 8: Signal Type別比較"""
    signal_types = sorted(df["signal_type"].unique().tolist())

    rows_html = ""
    for st in signal_types:
        sub = df[df["signal_type"] == st]
        cnt = len(sub)
        if cnt == 0:
            continue
        wins = sub[sub["pnl_yen"] > 0]
        losses_sub = sub[sub["pnl_yen"] <= 0]
        wr = len(wins) / cnt * 100
        pf_val = pf(wins["pnl_yen"], losses_sub["pnl_yen"])
        avg_ret = sub["ret_pct"].mean()
        total_pnl = sub["pnl_yen"].sum()
        avg_mae = sub["mae_pct"].mean()
        avg_mfe = sub["mfe_pct"].mean()
        sl_rate = (sub["exit_type"] == "SL").sum() / cnt * 100
        avg_exit_day = sub["exit_day"].mean()
        pf_str = f"{pf_val:.2f}" if pf_val != float("inf") else "∞"
        pnl_cls = num_cls(total_pnl)

        rows_html += f"""<tr>
          <td><strong>{st}</strong></td><td class='r'>{cnt:,}</td><td class='r'>{wr:.1f}%</td>
          <td class='r'>{pf_str}</td><td class='r {pnl_cls}'>¥{total_pnl:,.0f}</td>
          <td class='r {pnl_cls}'>{fmt_pct(avg_ret)}</td>
          <td class='r'>{sl_rate:.1f}%</td>
          <td class='r num-neg'>{fmt_pct(avg_mae)}</td><td class='r num-pos'>{fmt_pct(avg_mfe)}</td>
          <td class='r'>{avg_exit_day:.1f}</td>
        </tr>"""

    return f"""
<div class="section">
  <h2>8. Signal Type別比較</h2>
  <table>
    <thead><tr>
      <th>Signal</th><th class="r">件数</th><th class="r">勝率</th><th class="r">PF</th>
      <th class="r">総PnL</th><th class="r">平均リターン</th><th class="r">SL率</th>
      <th class="r">平均MAE</th><th class="r">平均MFE</th><th class="r">平均保有日数</th>
    </tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>
"""


# ---------------------------------------------------------------------------
# HTML assembly
# ---------------------------------------------------------------------------

def generate_html(df: pd.DataFrame) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_range = ""
    if "signal_date" in df.columns:
        dates = pd.to_datetime(df["signal_date"])
        date_range = f"{dates.min().strftime('%Y/%m/%d')} ~ {dates.max().strftime('%Y/%m/%d')}"

    sections = [
        section_executive_summary(df),
        section_mae_distribution(df),
        section_mfe_distribution(df),
        section_mae_vs_mfe_scatter(df),
        section_time_analysis(df),
        section_exit_type(df),
        section_overnight_gap(df),
        section_signal_type(df),
    ]

    body = "\n".join(sections)

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MAE/MFE分析レポート — グランビルIFD</title>
<script src="{PLOTLY_CDN}"></script>
<style>
  :root {{
    --bg: #09090b; --card: #18181b; --card-border: #27272a;
    --text: #fafafa; --text-muted: #a1a1aa;
    --emerald: #34d399; --emerald-bg: rgba(52,211,153,0.1);
    --rose: #fb7185; --rose-bg: rgba(251,113,133,0.1);
    --amber: #fbbf24; --amber-bg: rgba(251,191,36,0.3);
    --blue: #60a5fa; --blue-bg: rgba(96,165,250,0.1);
    --purple: #a78bfa; --purple-bg: rgba(167,139,250,0.1);
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif; line-height:1.6; padding:24px; max-width:1200px; margin:0 auto; }}
  h1 {{ font-size:1.5rem; margin-bottom:8px; }}
  .subtitle {{ color:var(--text-muted); font-size:0.875rem; margin-bottom:32px; }}
  .section {{ background:var(--card); border:1px solid var(--card-border); border-radius:12px; padding:24px; margin-bottom:20px; }}
  .section h2 {{ font-size:1.1rem; margin-bottom:16px; display:flex; align-items:center; gap:8px; }}
  .section h3 {{ font-size:0.95rem; color:var(--text-muted); margin:16px 0 8px 0; }}
  table {{ width:100%; border-collapse:collapse; font-size:0.85rem; margin:12px 0; }}
  th {{ text-align:left; padding:8px 12px; background:rgba(255,255,255,0.03); color:var(--text-muted); font-weight:600; border-bottom:1px solid var(--card-border); white-space:nowrap; }}
  th.r {{ text-align:right; }}
  td {{ padding:8px 12px; border-bottom:1px solid rgba(255,255,255,0.05); }}
  td.r {{ text-align:right; font-variant-numeric:tabular-nums; }}
  tr:hover td {{ background:rgba(255,255,255,0.02); }}
  .num-pos {{ color:var(--emerald); font-weight:600; }}
  .num-neg {{ color:var(--rose); font-weight:600; }}
  .num-neutral {{ color:var(--text-muted); }}
  .alert-box {{ border-radius:8px; padding:16px; margin:16px 0; font-size:0.875rem; line-height:1.7; }}
  .alert-danger {{ background:var(--rose-bg); border:1px solid rgba(251,113,133,0.3); color:var(--rose); }}
  .alert-warning {{ background:rgba(251,191,36,0.08); border:1px solid rgba(251,191,36,0.3); color:var(--amber); }}
  .alert-info {{ background:var(--blue-bg); border:1px solid rgba(96,165,250,0.3); color:var(--blue); }}
  .alert-success {{ background:var(--emerald-bg); border:1px solid rgba(52,211,153,0.3); color:var(--emerald); }}
  .grid-2 {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; }}
  .grid-3 {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px; }}
  .grid-4 {{ display:grid; grid-template-columns:1fr 1fr 1fr 1fr; gap:16px; }}
  @media (max-width:768px) {{ .grid-2,.grid-3,.grid-4 {{ grid-template-columns:1fr; }} }}
  .stat-card {{ background:rgba(255,255,255,0.02); border:1px solid var(--card-border); border-radius:8px; padding:16px; text-align:center; }}
  .stat-card .label {{ color:var(--text-muted); font-size:0.75rem; margin-bottom:4px; }}
  .stat-card .value {{ font-size:1.5rem; font-weight:700; }}
  .stat-card .sub {{ color:var(--text-muted); font-size:0.75rem; margin-top:2px; }}
  footer {{ text-align:center; color:var(--text-muted); font-size:0.7rem; margin-top:40px; padding:16px 0; border-top:1px solid var(--card-border); }}
</style>
</head>
<body>

<h1>MAE/MFE分析レポート — グランビルIFD</h1>
<div class="subtitle">データ期間: {date_range} | {len(df):,} trades | Generated: {now}</div>

{body}

<footer>Generated by generate_mae_mfe_report.py | Source: granville_ifd_archive.parquet</footer>

</body>
</html>
"""


def main() -> None:
    print("Loading archive...")
    df = load_archive()
    print(f"  {len(df):,} trades loaded ({df.columns.tolist()})")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    print("Generating HTML...")
    html = generate_html(df)

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Generated: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
