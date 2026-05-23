#!/usr/bin/env python3
"""Pair universe health check.

This report is for stopping degraded pairs before they keep costing money.
It uses the existing pair backtest rows as the launch point and summarizes
each pair by full history, 2026, recent trades, rolling PF, drawdown and tail.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.pipeline.generate_pairs_signals import EXCLUDE_PAIRS, EXCLUDE_SECTORS, PF_MIN, V2_PAIRS  # noqa: E402

OUT_DIR = ROOT / "data" / "analysis"
RAW_PATH = OUT_DIR / "pair_mece_leg_momentum_raw_20260523.parquet"

RECENT_N = 20
ROLL_N = 20


def stats(pnls: pd.Series) -> dict[str, float | int | None]:
    p = pd.Series(pnls).dropna()
    if p.empty:
        return {"n": 0, "pf": None, "wr": 0.0, "pnl": 0, "avg": 0.0, "max_dd": 0, "max_loss": 0, "p05": 0}
    wins = p[p > 0]
    losses = p[p < 0]
    gp = float(wins.sum())
    gl = float(-losses.sum())
    eq = p.cumsum()
    dd = eq - eq.cummax()
    return {
        "n": int(len(p)),
        "pf": round(gp / gl, 3) if gl > 0 else None,
        "wr": round(float((p > 0).mean() * 100), 1),
        "pnl": round(float(p.sum())),
        "avg": round(float(p.mean()), 1),
        "max_dd": round(float(dd.min())),
        "max_loss": round(float(p.min())),
        "p05": round(float(p.quantile(0.05))),
    }


def rolling_pf(values: pd.Series, n: int = ROLL_N) -> pd.Series:
    def calc(x: np.ndarray) -> float:
        s = pd.Series(x)
        gp = s[s > 0].sum()
        gl = -s[s < 0].sum()
        if gl <= 0:
            return np.nan
        return float(gp / gl)

    return values.rolling(n, min_periods=max(8, n // 2)).apply(calc, raw=True)


def pair_meta() -> pd.DataFrame:
    rows = []
    for tk1, tk2, lb, pf, n, r1d in V2_PAIRS:
        n1, n2 = int(tk1[:4]), int(tk2[:4])
        sector_excluded = any(lo <= n1 <= hi and lo <= n2 <= hi for lo, hi in EXCLUDE_SECTORS)
        pair_excluded = (tk1, tk2) in EXCLUDE_PAIRS
        rows.append(
            {
                "pair": f"{tk1}/{tk2}",
                "tk1": tk1,
                "tk2": tk2,
                "lookback": lb,
                "defined_pf": pf,
                "defined_n": n,
                "revert_1d": r1d,
                "運用状態": "除外" if sector_excluded or pair_excluded or pf < PF_MIN else "運用中",
                "除外理由": "sector" if sector_excluded else ("pair" if pair_excluded else ("pf_min" if pf < PF_MIN else "")),
            }
        )
    return pd.DataFrame(rows)


def label(row: pd.Series) -> str:
    if row["運用状態"] == "除外":
        return "除外済"
    deterioration = []
    risk = []
    if row["n_2026"] >= 3 and row["pnl_2026"] <= -30000:
        deterioration.append("2026損益悪化")
    if pd.notna(row["pf_2026"]) and row["n_2026"] >= 5 and row["pf_2026"] < 1.0:
        deterioration.append("2026PF<1")
    if row["recent_n"] >= 8 and row["recent_pnl"] <= -30000:
        deterioration.append("直近損益悪化")
    if pd.notna(row["rolling_pf_last"]) and row["rolling_pf_last"] < 1.0 and row["rolling_pf_n"] >= 10:
        deterioration.append("rollingPF<1")
    if row["max_loss_2026"] <= -25000:
        risk.append("2026大損失")
    if row["same_sector"] and row["short_strong_loss_n"] >= 3 and row["short_strong_loss_pnl"] < 0:
        risk.append("同業種short強い悪化")

    if len(deterioration) >= 2:
        return "停止候補"
    if deterioration:
        return "警告"
    if risk:
        return "ロット注意"
    return "継続"


def fmt(v: object) -> str:
    if pd.isna(v):
        return "-"
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, float):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        return f"{v:.3f}".rstrip("0").rstrip(".")
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def table(df: pd.DataFrame, limit: int | None = None) -> str:
    if limit is not None:
        df = df.head(limit)
    if df.empty:
        return '<p class="muted">データなし</p>'
    head = "".join(f"<th>{c}</th>" for c in df.columns)
    body = []
    for _, row in df.iterrows():
        cells = []
        for c in df.columns:
            val = row[c]
            cls = "num" if isinstance(val, (int, float)) and not isinstance(val, bool) else ""
            cells.append(f'<td class="{cls}">{fmt(val)}</td>')
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def build_summary() -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_parquet(RAW_PATH)
    raw["signal_date"] = pd.to_datetime(raw["signal_date"])
    raw["trade_date"] = pd.to_datetime(raw["trade_date"])
    raw["pair"] = raw["tk1"] + "/" + raw["tk2"]
    raw["same_sector"] = raw["long_sector"].eq(raw["short_sector"])

    meta = pair_meta()
    rows = []
    details = []
    for pair, g in raw.sort_values("trade_date").groupby("pair"):
        g = g.copy()
        full = stats(g["pnl"])
        y2026 = g[g["trade_date"].dt.year == 2026]
        s2026 = stats(y2026["pnl"])
        recent = g.tail(RECENT_N)
        sr = stats(recent["pnl"])
        rpf = rolling_pf(g["pnl"])
        worst = g.sort_values("pnl").head(3)
        short_strong = g[(g["same_sector"]) & (g["short_state"] == "strong")]
        short_strong_loss = short_strong[short_strong["pnl"] < 0]

        row = {
            "pair": pair,
            "long/short例": f"{g.iloc[-1]['long_tk']} / {g.iloc[-1]['short_tk']}",
            "業種": f"{g.iloc[-1]['long_sector']} / {g.iloc[-1]['short_sector']}",
            "same_sector": bool(g["same_sector"].iloc[-1]),
            "full_n": full["n"],
            "full_pf": full["pf"],
            "full_pnl": full["pnl"],
            "n_2026": s2026["n"],
            "pf_2026": s2026["pf"],
            "pnl_2026": s2026["pnl"],
            "max_loss_2026": s2026["max_loss"],
            "recent_n": sr["n"],
            "recent_pf": sr["pf"],
            "recent_pnl": sr["pnl"],
            "rolling_pf_last": round(float(rpf.dropna().iloc[-1]), 3) if not rpf.dropna().empty else np.nan,
            "rolling_pf_min": round(float(rpf.min()), 3) if pd.notna(rpf.min()) else np.nan,
            "rolling_pf_n": int(rpf.dropna().shape[0]),
            "max_loss": full["max_loss"],
            "p05": full["p05"],
            "short_strong_loss_n": int(len(short_strong_loss)),
            "short_strong_loss_pnl": round(float(short_strong_loss["pnl"].sum())) if len(short_strong_loss) else 0,
            "worst3": " / ".join(f"{r.trade_date:%Y-%m-%d}:{r.pnl:,.0f}" for r in worst.itertuples()),
        }
        rows.append(row)
        details.append(g.assign(health_pair=pair))

    summary = pd.DataFrame(rows).merge(meta, on="pair", how="left")
    summary["判定"] = summary.apply(label, axis=1)
    order = {"停止候補": 0, "警告": 1, "ロット注意": 2, "継続": 3, "除外済": 4}
    summary = summary.sort_values(
        by=["判定", "pnl_2026", "recent_pnl", "max_loss_2026"],
        key=lambda s: s.map(order) if s.name == "判定" else s,
        ascending=[True, True, True, True],
    )
    return summary, pd.concat(details, ignore_index=True)


def display(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "判定",
        "運用状態",
        "pair",
        "long/short例",
        "業種",
        "full_n",
        "full_pf",
        "full_pnl",
        "n_2026",
        "pf_2026",
        "pnl_2026",
        "max_loss_2026",
        "recent_pf",
        "recent_pnl",
        "rolling_pf_last",
        "rolling_pf_min",
        "max_loss",
        "p05",
        "short_strong_loss_n",
        "short_strong_loss_pnl",
        "worst3",
        "除外理由",
    ]
    labels = {
        "pair": "ペア",
        "long/short例": "直近L/S",
        "full_n": "全件数",
        "full_pf": "全PF",
        "full_pnl": "全損益",
        "n_2026": "2026件数",
        "pf_2026": "2026PF",
        "pnl_2026": "2026損益",
        "max_loss_2026": "2026最大損失",
        "recent_pf": "直近PF",
        "recent_pnl": "直近損益",
        "rolling_pf_last": "rollingPF直近",
        "rolling_pf_min": "rollingPF最低",
        "max_loss": "最大損失",
        "p05": "左尾5%",
        "short_strong_loss_n": "同業種short強い負け数",
        "short_strong_loss_pnl": "同業種short強い負け額",
        "worst3": "Worst3",
    }
    return df[[c for c in cols if c in df.columns]].rename(columns=labels)


def write_html(path: Path, summary: pd.DataFrame) -> None:
    stop = summary[summary["判定"] == "停止候補"]
    warn = summary[summary["判定"] == "警告"]
    size_warn = summary[summary["判定"] == "ロット注意"]
    excluded = summary[summary["運用状態"] == "除外"]
    html = f"""<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pair Health Check</title>
  <style>
    :root {{ color-scheme: dark; --bg:#0b0f14; --panel:#121821; --panel2:#17202b; --line:#2b3746; --text:#e8edf3; --muted:#9aa8b8; --bad:#ff7777; --warn:#f4c95d; --good:#54d18a; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; font-size:15px; }}
    main {{ width:min(1600px,calc(100vw - 40px)); margin:0 auto; padding:28px 0 48px; }}
    h1 {{ margin:0 0 8px; font-size:30px; }}
    h2 {{ margin:28px 0 12px; font-size:20px; }}
    p {{ color:var(--muted); line-height:1.6; }}
    .cards {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin:20px 0; }}
    .card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px 16px; }}
    .label {{ color:var(--muted); font-size:13px; }}
    .value {{ font-size:26px; font-weight:700; margin-top:6px; }}
    .callout {{ background:#241d0c; border:1px solid #67501a; border-radius:8px; padding:14px 16px; color:var(--muted); }}
    .callout strong {{ color:var(--warn); }}
    .table-wrap {{ overflow-x:auto; border:1px solid var(--line); border-radius:8px; background:var(--panel); }}
    table {{ width:100%; border-collapse:collapse; min-width:1500px; }}
    th,td {{ padding:9px 10px; border-bottom:1px solid var(--line); white-space:nowrap; }}
    th {{ text-align:left; color:var(--muted); background:var(--panel2); position:sticky; top:0; }}
    td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
    tr:last-child td {{ border-bottom:0; }}
    @media (max-width:980px) {{ main {{ width:calc(100vw - 24px); }} .cards {{ grid-template-columns:1fr 1fr; }} }}
  </style>
</head>
<body>
<main>
  <h1>Pair Health Check</h1>
  <p>pair universeを同じ物差しで見て、停止候補・警告・継続を分けます。目的は「負け始めたpairを止める」ことです。</p>
  <div class="cards">
    <section class="card"><div class="label">停止候補</div><div class="value">{len(stop)}</div></section>
    <section class="card"><div class="label">警告</div><div class="value">{len(warn)}</div></section>
    <section class="card"><div class="label">ロット注意</div><div class="value">{len(size_warn)}</div></section>
    <section class="card"><div class="label">除外済</div><div class="value">{len(excluded)}</div></section>
  </div>
  <div class="callout"><strong>判定ロジック:</strong> 停止候補は「2026損益悪化、2026PF&lt;1、直近損益悪化、rollingPF&lt;1」の劣化条件を複数満たすもの。大損失や同業種short強い悪化は、損益がまだ良い場合は停止ではなくロット注意に分けます。</div>

  <h2>1. 停止候補</h2>
  <div class="table-wrap">{table(display(stop), limit=80)}</div>

  <h2>2. 警告</h2>
  <div class="table-wrap">{table(display(warn), limit=120)}</div>

  <h2>3. ロット注意</h2>
  <div class="table-wrap">{table(display(size_warn), limit=120)}</div>

  <h2>4. 全pair</h2>
  <div class="table-wrap">{table(display(summary), limit=250)}</div>
</main>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    summary, details = build_summary()
    summary_path = OUT_DIR / "pair_health_check_summary_20260523.csv"
    details_path = OUT_DIR / "pair_health_check_details_20260523.parquet"
    html_path = OUT_DIR / "pair_health_check_20260523.html"
    summary.to_csv(summary_path, index=False)
    details.to_parquet(details_path, index=False)
    write_html(html_path, summary)
    print("STOP CANDIDATES")
    print(display(summary[summary["判定"] == "停止候補"]).to_string(index=False))
    print("WROTE", summary_path)
    print("WROTE", details_path)
    print("WROTE", html_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
