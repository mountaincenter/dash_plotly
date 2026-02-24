"""
モンテカルロ・シミュレーション: バックテスト結果のリスク定量化

対応データ:
  - grok_trending_archive.parquet（日次×複数銘柄、phase2損益）
  - granville_ifd_archive.parquet（トレード単位、pnl_yen）

Usage:
  python monte_carlo_backtest.py                  # grok（デフォルト）
  python monte_carlo_backtest.py --source granville
  python monte_carlo_backtest.py --source both     # 両方比較
"""
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import json

BASE_DIR = Path(__file__).resolve().parents[2]
BACKTEST_DIR = BASE_DIR / "data" / "parquet" / "backtest"
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
N_SIMULATIONS = 10_000


# --- Data Loading ---

def load_grok_daily_pnl(mode: str = "short") -> pd.Series:
    """grok: 日次損益を集計（phase2 = 大引けまでの損益）"""
    path = BACKTEST_DIR / "grok_trending_archive.parquet"
    df = pd.read_parquet(path)
    if mode == "short":
        df["pnl"] = -df["profit_per_100_shares_phase2"]
    else:
        df["pnl"] = df["profit_per_100_shares_phase2"]
    daily = df.groupby("backtest_date")["pnl"].sum()
    return daily


def load_granville_daily_pnl() -> pd.Series:
    """granville: トレード単位のpnl_yenを日次集計"""
    path = BACKTEST_DIR / "granville_ifd_archive.parquet"
    df = pd.read_parquet(path)
    daily = df.groupby("entry_date")["pnl_yen"].sum()
    return daily


# --- Monte Carlo Engine ---

def run_monte_carlo(daily_pnl: np.ndarray, n_sims: int = N_SIMULATIONS) -> dict:
    """日次損益を並べ替え × n_sims 回で最大ドローダウン分布を求める"""
    n_days = len(daily_pnl)
    max_drawdowns = np.zeros(n_sims)
    final_pnls = np.zeros(n_sims)

    for i in range(n_sims):
        shuffled = np.random.permutation(daily_pnl)
        cumulative = np.cumsum(shuffled)
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = running_max - cumulative
        max_drawdowns[i] = np.max(drawdowns)
        final_pnls[i] = cumulative[-1]

    max_drawdowns.sort()
    final_pnls.sort()

    return {
        "n_sims": n_sims,
        "n_days": n_days,
        "daily_mean": float(np.mean(daily_pnl)),
        "daily_std": float(np.std(daily_pnl)),
        "total_actual": float(np.sum(daily_pnl)),
        "drawdown": {
            "median": float(np.percentile(max_drawdowns, 50)),
            "p75": float(np.percentile(max_drawdowns, 75)),
            "p90": float(np.percentile(max_drawdowns, 90)),
            "p95": float(np.percentile(max_drawdowns, 95)),
            "p99": float(np.percentile(max_drawdowns, 99)),
            "max": float(np.max(max_drawdowns)),
            "values": max_drawdowns.tolist(),
        },
        "final_pnl": {
            "median": float(np.percentile(final_pnls, 50)),
            "p5": float(np.percentile(final_pnls, 5)),
            "p95": float(np.percentile(final_pnls, 95)),
            "min": float(np.min(final_pnls)),
            "max": float(np.max(final_pnls)),
            "values": final_pnls.tolist(),
        },
    }


# --- HTML Generation ---

def generate_html(strategies: dict[str, dict], title: str, subtitle: str) -> str:
    """複数戦略の結果をタブ切替 HTML に変換"""
    tab_keys = list(strategies.keys())
    tab_labels = {k: k for k in tab_keys}
    data_json = json.dumps(strategies, ensure_ascii=False)
    first_key = tab_keys[0]

    tabs_html = "\n".join(
        f'  <div class="tab{" active" if k == first_key else ""}" onclick="switchMode(\'{k}\')">{k}</div>'
        for k in tab_keys
    )

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family:-apple-system,BlinkMacSystemFont,'Hiragino Sans',sans-serif; background:#0a0a0a; color:#e0e0e0; line-height:1.6; }}
  .container {{ max-width:960px; margin:0 auto; padding:32px 24px 80px; }}
  h1 {{ font-size:24px; color:#fff; margin-bottom:4px; }}
  h2 {{ font-size:18px; color:#fff; margin:36px 0 12px; padding-top:24px; border-top:1px solid #222; }}
  p {{ margin-bottom:12px; color:#bbb; font-size:14px; }}
  .subtitle {{ color:#888; font-size:13px; margin-bottom:32px; }}
  .tabs {{ display:flex; gap:8px; margin-bottom:24px; flex-wrap:wrap; }}
  .tab {{ padding:8px 20px; border-radius:6px; font-size:14px; font-weight:600; cursor:pointer; border:1px solid #333; background:#141414; color:#888; transition:all 0.2s; }}
  .tab.active {{ background:#10b981; color:#000; border-color:#10b981; }}
  .tab:hover:not(.active) {{ border-color:#555; color:#ccc; }}
  .cards {{ display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin:16px 0; }}
  .card {{ background:#141414; border:1px solid #252525; border-radius:10px; padding:16px; text-align:center; }}
  .card .label {{ font-size:11px; color:#888; }}
  .card .value {{ font-size:22px; font-weight:700; margin-top:4px; }}
  .card .note {{ font-size:11px; color:#666; margin-top:4px; }}
  .green {{ color:#10b981; }}
  .amber {{ color:#f59e0b; }}
  .red {{ color:#ef4444; }}
  .box {{ background:#141414; border:1px solid #252525; border-radius:10px; padding:20px; margin:16px 0; }}
  canvas {{ width:100% !important; border-radius:8px; margin:8px 0; }}
  .insight {{ background:linear-gradient(135deg,#0d2818,#0d1b2a); border:1px solid #1a3d2e; border-radius:10px; padding:16px; margin:20px 0; font-size:14px; }}
  .insight::before {{ content:"💡"; margin-right:8px; }}
  .meta {{ font-size:12px; color:#666; margin:8px 0; }}
  table {{ width:100%; border-collapse:collapse; margin:12px 0; }}
  th,td {{ padding:8px 12px; text-align:left; border-bottom:1px solid #1a1a1a; font-size:13px; }}
  th {{ color:#888; font-weight:400; }}
</style>
</head>
<body>
<div class="container">

<h1>{title}</h1>
<p class="subtitle">{subtitle}</p>

<div class="tabs">
{tabs_html}
</div>

<div id="content"></div>

</div>

<script>
const DATA = {data_json};
let currentMode = '{first_key}';

function fmt(n) {{
  if (Math.abs(n) >= 10000) return (n/10000).toFixed(1) + '万';
  return Math.round(n).toLocaleString();
}}
function fmtYen(n) {{
  const sign = n >= 0 ? '+' : '';
  if (Math.abs(n) >= 10000) return sign + (n/10000).toFixed(1) + '万円';
  return sign + Math.round(n).toLocaleString() + '円';
}}
function colorClass(n) {{ return n >= 0 ? 'green' : 'red'; }}

function switchMode(mode) {{
  currentMode = mode;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => {{
    if (t.textContent === mode) t.classList.add('active');
  }});
  render();
}}

function buildHistogram(values, nBins) {{
  const min = values[0], max = values[values.length-1];
  const range = max - min || 1, binWidth = range / nBins;
  const bins = Array(nBins).fill(0);
  for (const v of values) {{ let idx = Math.floor((v-min)/binWidth); if(idx>=nBins) idx=nBins-1; bins[idx]++; }}
  return {{ bins, min, max, range }};
}}

function render() {{
  const d = DATA[currentMode];
  const dd = d.drawdown, pnl = d.final_pnl;
  document.getElementById('content').innerHTML = `
    <div class="meta">${{d.n_days}}日間の日次損益 × ${{d.n_sims.toLocaleString()}}回 | 日次平均: ${{fmtYen(d.daily_mean)}} | 標準偏差: ${{fmt(d.daily_std)}}円 | 実績合計: ${{fmtYen(d.total_actual)}}</div>

    <h2>最大ドローダウンの分布</h2>
    <p>「この戦略で最悪どれくらい凹むか」の確率分布</p>
    <div class="cards">
      <div class="card"><div class="label">中央値 (50%)</div><div class="value red">-${{fmt(dd.median)}}円</div><div class="note">半分のケースでこれ以下</div></div>
      <div class="card"><div class="label">95%タイル</div><div class="value amber">-${{fmt(dd.p95)}}円</div><div class="note">20回に1回の最悪ケース</div></div>
      <div class="card"><div class="label">99%タイル</div><div class="value red">-${{fmt(dd.p99)}}円</div><div class="note">100回に1回の最悪ケース</div></div>
    </div>
    <div class="box"><canvas id="ddCanvas" height="200"></canvas></div>
    <div class="insight">
      モンテカルロで ${{d.n_sims.toLocaleString()}} 回並べ替えると、<span class="amber">95%の確率で -${{fmt(dd.p95)}}円 以内</span>、<span class="red">最悪時は -${{fmt(dd.max)}}円</span> まであり得る。
    </div>

    <h2>最終損益の分布</h2>
    <p>合計損益は同じだが、途中経過の「体験」が全く違う</p>
    <div class="cards">
      <div class="card"><div class="label">最悪ケース (5%)</div><div class="value ${{colorClass(pnl.p5)}}">${{fmtYen(pnl.p5)}}</div></div>
      <div class="card"><div class="label">中央値</div><div class="value ${{colorClass(pnl.median)}}">${{fmtYen(pnl.median)}}</div></div>
      <div class="card"><div class="label">最良ケース (95%)</div><div class="value ${{colorClass(pnl.p95)}}">${{fmtYen(pnl.p95)}}</div></div>
    </div>
    <div class="box"><canvas id="pnlCanvas" height="200"></canvas></div>

    <h2>ポジションサイジングへの示唆</h2>
    <table>
      <tr><th>許容最大損失</th><th>95%DD基準の倍率</th><th>意味</th></tr>
      <tr><td>${{fmt(dd.p95)}}円</td><td>1.0倍</td><td>20回に1回、許容上限に到達</td></tr>
      <tr><td>${{fmt(dd.p95*0.5)}}円</td><td>0.5倍</td><td>余裕を持った運用</td></tr>
      <tr><td>${{fmt(dd.p95*2)}}円</td><td>2.0倍</td><td>攻めた運用（DD2倍リスク）</td></tr>
    </table>
    <div class="insight">
      許容損失額が決まっていれば、95%DD（-${{fmt(dd.p95)}}円）で割って最大ポジションサイズが決まる。<br>
      例: 許容損失50万円 → 50万 ÷ ${{fmt(dd.p95)}} ≈ ${{(500000/dd.p95).toFixed(1)}}倍 が上限。
    </div>`;
  drawHistogram('ddCanvas', dd.values, '#ef4444', dd.p95, 'DD 95%');
  drawHistogram('pnlCanvas', pnl.values, '#10b981', pnl.p5, 'PnL 5%');
}}

function drawHistogram(canvasId, values, color, threshold, thresholdLabel) {{
  const canvas = document.getElementById(canvasId);
  const ctx = canvas.getContext('2d');
  const W = canvas.offsetWidth, H = 200;
  canvas.width = W*2; canvas.height = H*2; ctx.scale(2,2);
  const nBins = 50;
  const hist = buildHistogram(values, nBins);
  const maxCount = Math.max(...hist.bins);
  const pad = {{l:50,r:16,t:8,b:24}};
  const plotW = W-pad.l-pad.r, plotH = H-pad.t-pad.b;
  const barW = plotW/nBins;
  for (let i=0;i<nBins;i++) {{
    const h = maxCount>0?(hist.bins[i]/maxCount)*plotH:0;
    const x = pad.l+i*barW, y = pad.t+plotH-h;
    const binCenter = hist.min+(i+0.5)*(hist.range/nBins);
    const isThreshold = canvasId==='ddCanvas'?binCenter>=threshold:binCenter<=threshold;
    ctx.fillStyle = isThreshold?'#f59e0b':color;
    ctx.globalAlpha = 0.7;
    ctx.fillRect(x,y,barW-1,h);
  }}
  ctx.globalAlpha=1;
  const tx = pad.l+((threshold-hist.min)/hist.range)*plotW;
  ctx.strokeStyle='#f59e0b'; ctx.lineWidth=1.5; ctx.setLineDash([4,3]);
  ctx.beginPath(); ctx.moveTo(tx,pad.t); ctx.lineTo(tx,pad.t+plotH); ctx.stroke(); ctx.setLineDash([]);
  ctx.fillStyle='#f59e0b'; ctx.font='11px sans-serif';
  ctx.fillText(thresholdLabel+': '+fmt(threshold), tx+4, pad.t+14);
  ctx.strokeStyle='#333'; ctx.lineWidth=1;
  ctx.beginPath(); ctx.moveTo(pad.l,pad.t+plotH); ctx.lineTo(pad.l+plotW,pad.t+plotH); ctx.stroke();
  ctx.fillStyle='#666'; ctx.font='10px sans-serif';
  for(let i=0;i<=4;i++) {{ const v=hist.min+(hist.range*i/4); ctx.fillText(fmt(v),pad.l+(i/4)*plotW,pad.t+plotH+14); }}
}}

render();
</script>
</body>
</html>"""


# --- Main ---

def run_source_grok() -> dict[str, dict]:
    daily_short = load_grok_daily_pnl("short")
    daily_long = load_grok_daily_pnl("long")
    print(f"  Grok ショート: {len(daily_short)}日, 合計 {daily_short.sum():+,.0f}円")
    print(f"  Grok ロング:   {len(daily_long)}日, 合計 {daily_long.sum():+,.0f}円")
    return {
        "Grok ショート": run_monte_carlo(daily_short.values),
        "Grok ロング": run_monte_carlo(daily_long.values),
    }


def run_source_granville() -> dict[str, dict]:
    daily = load_granville_daily_pnl()
    print(f"  Granville: {len(daily)}日, 合計 {daily.sum():+,.0f}円")
    return {
        "Granville IFD": run_monte_carlo(daily.values),
    }


def print_summary(strategies: dict[str, dict]):
    for name, result in strategies.items():
        dd = result["drawdown"]
        print(f"\n=== {name} ===")
        print(f"  実績合計:      {result['total_actual']:+,.0f}円")
        print(f"  最大DD 中央値: -{dd['median']:,.0f}円")
        print(f"  最大DD 95%:    -{dd['p95']:,.0f}円")
        print(f"  最大DD 99%:    -{dd['p99']:,.0f}円")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["grok", "granville", "both"], default="grok")
    args = parser.parse_args()

    np.random.seed(42)
    print(f"Running Monte Carlo ({N_SIMULATIONS:,} simulations)...")

    strategies: dict[str, dict] = {}

    if args.source in ("grok", "both"):
        strategies.update(run_source_grok())
    if args.source in ("granville", "both"):
        strategies.update(run_source_granville())

    print_summary(strategies)

    # HTML output
    source_label = args.source if args.source != "both" else "grok_granville"
    output_path = OUTPUT_DIR / f"monte_carlo_{source_label}.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    title = "モンテカルロ・シミュレーション結果"
    subtitle = f"バックテスト損益を {N_SIMULATIONS:,} 回並べ替え、リスクを定量化"
    html = generate_html(strategies, title, subtitle)
    output_path.write_text(html, encoding="utf-8")
    print(f"\nHTML output: {output_path}")


if __name__ == "__main__":
    main()
