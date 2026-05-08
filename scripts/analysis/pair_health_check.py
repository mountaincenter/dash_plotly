"""
pair_health_check.py
現行 V2_PAIRS 161 ペアの劣化状況を 3 ウィンドウで可視化。

ウィンドウ:
  full: 2020-01-01 ~ 今日
  2y:   直近24ヶ月
  6m:   直近6ヶ月

出力:
  data/analysis/pair_health_YYYY-MM-DD.csv
  docs/pair_health_YYYY-MM-DD.html

運用ロジックへの反映は行わない (可視化専用)。
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "pipeline"))
from generate_pairs_signals import V2_PAIRS, EXCLUDE_PAIRS, Z_ENTRY  # type: ignore

EXCLUDE_SECTORS = [(9000, 9099)]
PRICES = ROOT / "data" / "parquet" / "granville" / "prices_topix.parquet"
OUT_CSV_DIR = ROOT / "data" / "analysis"
OUT_HTML_DIR = ROOT / "docs"
MIN_DATE = "2020-01-01"


def load_prices(tickers: set[str]) -> pd.DataFrame:
    df = pd.read_parquet(PRICES)
    df = df[df["ticker"].isin(tickers)].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= MIN_DATE].dropna(subset=["Open", "Close"])
    return df.sort_values(["ticker", "date"])


def pair_signals(ps: pd.DataFrame, tk1: str, tk2: str, lookback: int) -> pd.DataFrame:
    d1 = ps[ps["ticker"] == tk1].set_index("date")[["Open", "Close"]].sort_index()
    d2 = ps[ps["ticker"] == tk2].set_index("date")[["Open", "Close"]].sort_index()
    common = d1.index.intersection(d2.index)
    if len(common) < lookback + 5:
        return pd.DataFrame()
    d1 = d1.loc[common]
    d2 = d2.loc[common]

    spread = np.log(d1["Close"] / d2["Close"])
    mu = spread.rolling(lookback).mean()
    sigma = spread.rolling(lookback).std()
    z = (spread - mu) / sigma

    o1_next = d1["Open"].shift(-1)
    c1_next = d1["Close"].shift(-1)
    o2_next = d2["Open"].shift(-1)
    c2_next = d2["Close"].shift(-1)

    ret_long_tk1 = (c1_next - o1_next) / o1_next
    ret_short_tk1 = (o1_next - c1_next) / o1_next
    ret_long_tk2 = (c2_next - o2_next) / o2_next
    ret_short_tk2 = (o2_next - c2_next) / o2_next

    ret_pos = np.where(z > 0, ret_short_tk1 + ret_long_tk2, ret_long_tk1 + ret_short_tk2)

    out = pd.DataFrame({
        "date": d1.index,
        "z": z.values,
        "ret_pair": ret_pos,
    })
    out = out[(out["z"].abs() >= Z_ENTRY)].dropna(subset=["ret_pair"])
    return out


def pf(returns: np.ndarray) -> float:
    g = returns[returns > 0].sum()
    l = -returns[returns < 0].sum()
    return g / l if l > 0 else np.inf


def window_stats(sigs: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> dict:
    window = sigs[(sigs["date"] >= start) & (sigs["date"] < end)]
    if window.empty:
        return {"n": 0, "pf": np.nan, "avg_bp": np.nan, "win_rate": np.nan}
    r = window["ret_pair"].values
    return {
        "n": len(window),
        "pf": pf(r),
        "avg_bp": r.mean() * 10000,
        "win_rate": (r > 0).mean(),
    }


def main():
    today = pd.Timestamp.now().normalize()
    start_full = pd.Timestamp(MIN_DATE)
    start_2y = today - pd.DateOffset(years=2)
    start_6m = today - pd.DateOffset(months=6)

    tickers = set()
    for a, b, *_ in V2_PAIRS:
        tickers.update([a, b])
    print(f"[Load] prices for {len(tickers)} tickers")
    ps = load_prices(tickers)
    print(f"  rows={len(ps):,}  {ps['date'].min().date()} ~ {ps['date'].max().date()}")

    rows = []
    for i, (tk1, tk2, lb, full_pf_orig, full_n_orig, revert_1d_orig) in enumerate(V2_PAIRS):
        n1, n2 = int(tk1[:4]), int(tk2[:4])
        sector_excluded = any(lo <= n1 <= hi and lo <= n2 <= hi for lo, hi in EXCLUDE_SECTORS)
        pair_excluded = (tk1, tk2) in EXCLUDE_PAIRS

        sig = pair_signals(ps, tk1, tk2, lb)
        if sig.empty:
            rows.append({
                "tk1": tk1, "tk2": tk2, "lookback": lb,
                "orig_pf": full_pf_orig, "orig_n": full_n_orig,
                "sector_excluded": sector_excluded, "pair_excluded": pair_excluded,
                "full_n": 0, "full_pf": np.nan, "full_avg_bp": np.nan, "full_wr": np.nan,
                "pf_2y": np.nan, "n_2y": 0, "avg_bp_2y": np.nan, "wr_2y": np.nan,
                "pf_6m": np.nan, "n_6m": 0, "avg_bp_6m": np.nan, "wr_6m": np.nan,
                "ratio_2y_over_full": np.nan, "ratio_6m_over_full": np.nan,
            })
            continue

        full = window_stats(sig, start_full, today + pd.Timedelta(days=1))
        two_y = window_stats(sig, start_2y, today + pd.Timedelta(days=1))
        six_m = window_stats(sig, start_6m, today + pd.Timedelta(days=1))

        ratio_2y = (two_y["pf"] / full["pf"]) if full["pf"] and full["pf"] > 0 and np.isfinite(full["pf"]) else np.nan
        ratio_6m = (six_m["pf"] / full["pf"]) if full["pf"] and full["pf"] > 0 and np.isfinite(full["pf"]) else np.nan

        rows.append({
            "tk1": tk1, "tk2": tk2, "lookback": lb,
            "orig_pf": full_pf_orig, "orig_n": full_n_orig,
            "sector_excluded": sector_excluded, "pair_excluded": pair_excluded,
            "full_n": full["n"], "full_pf": full["pf"], "full_avg_bp": full["avg_bp"], "full_wr": full["win_rate"],
            "pf_2y": two_y["pf"], "n_2y": two_y["n"], "avg_bp_2y": two_y["avg_bp"], "wr_2y": two_y["win_rate"],
            "pf_6m": six_m["pf"], "n_6m": six_m["n"], "avg_bp_6m": six_m["avg_bp"], "wr_6m": six_m["win_rate"],
            "ratio_2y_over_full": ratio_2y, "ratio_6m_over_full": ratio_6m,
        })
        if (i + 1) % 30 == 0:
            print(f"  processed {i+1}/{len(V2_PAIRS)}")

    df = pd.DataFrame(rows)

    OUT_CSV_DIR.mkdir(parents=True, exist_ok=True)
    OUT_HTML_DIR.mkdir(parents=True, exist_ok=True)
    date_str = today.strftime("%Y-%m-%d")
    csv_path = OUT_CSV_DIR / f"pair_health_{date_str}.csv"
    df.to_csv(csv_path, index=False)
    print(f"\n[saved] {csv_path}")

    print("\n=== サマリ ===")
    active = df[(~df["sector_excluded"]) & (~df["pair_excluded"])]
    print(f"active pairs: {len(active)}")
    print(f"  full_pf >= 1.5: {(active['full_pf'] >= 1.5).sum()}")
    print(f"  pf_2y  >= 1.5: {(active['pf_2y']  >= 1.5).sum()}")
    print(f"  pf_6m  >= 1.5: {(active['pf_6m']  >= 1.5).sum()}")
    print(f"  6m劣化 (ratio_6m < 0.7): {(active['ratio_6m_over_full'] < 0.7).sum()}")
    print(f"  6m改善 (ratio_6m > 1.3): {(active['ratio_6m_over_full'] > 1.3).sum()}")

    print("\n=== 直近6ヶ月で大きく劣化したペア (PF≥1.5 前提, ratio_6m < 0.7, N>=3) ===")
    degraded = active[
        (active["full_pf"] >= 1.5)
        & (active["ratio_6m_over_full"] < 0.7)
        & (active["n_6m"] >= 3)
    ].sort_values("ratio_6m_over_full")
    print(degraded[["tk1", "tk2", "full_pf", "pf_2y", "pf_6m", "n_6m", "ratio_6m_over_full"]].to_string(index=False))

    print("\n=== 直近6ヶ月で大きく改善したペア (full_pf<1.5 だが pf_6m>=2.0, N>=3) ===")
    improved = active[
        (active["full_pf"] < 1.5)
        & (active["pf_6m"] >= 2.0)
        & (active["n_6m"] >= 3)
    ].sort_values("pf_6m", ascending=False)
    print(improved[["tk1", "tk2", "full_pf", "pf_2y", "pf_6m", "n_6m"]].to_string(index=False))

    temp_excluded = df[df["pair_excluded"]].copy()
    if not temp_excluded.empty:
        print("\n=== TEMP除外ペアの復帰候補 (pf_6m>=1.5 & n_6m>=3) ===")
        candidates = temp_excluded[
            (temp_excluded["pf_6m"] >= 1.5) & (temp_excluded["n_6m"] >= 3)
        ]
        if candidates.empty:
            print("  復帰候補なし")
        else:
            print(candidates[["tk1", "tk2", "full_pf", "pf_2y", "pf_6m", "n_6m", "ratio_6m_over_full"]].to_string(index=False))
        print("\n=== TEMP除外ペアの現状 ===")
        print(temp_excluded[["tk1", "tk2", "full_pf", "pf_2y", "pf_6m", "n_6m", "ratio_6m_over_full"]].to_string(index=False))

    # Simple HTML table
    html_path = OUT_HTML_DIR / f"pair_health_{date_str}.html"
    df_sorted = df.sort_values("ratio_6m_over_full", na_position="last")
    html = _render_html(df_sorted, date_str, len(active))
    html_path.write_text(html, encoding="utf-8")
    print(f"\n[saved] {html_path}")


def _render_html(df: pd.DataFrame, date_str: str, active_count: int) -> str:
    def fmt_pf(v):
        if pd.isna(v): return "-"
        if np.isinf(v): return "∞"
        return f"{v:.2f}"
    def fmt_pct(v):
        if pd.isna(v): return "-"
        return f"{v*100:.0f}%"
    def fmt_bp(v):
        if pd.isna(v): return "-"
        return f"{v:+.1f}"
    def fmt_ratio(v):
        if pd.isna(v): return "-"
        return f"{v:.2f}"

    rows_html = []
    for _, r in df.iterrows():
        exc = []
        if r["sector_excluded"]: exc.append("sector")
        if r["pair_excluded"]: exc.append("pair")
        exc_str = ",".join(exc) if exc else "-"
        ratio_6m = r["ratio_6m_over_full"]
        cls = ""
        if pd.notna(ratio_6m):
            if ratio_6m < 0.7: cls = "degraded"
            elif ratio_6m > 1.3: cls = "improved"
        rows_html.append(
            f'<tr class="{cls}">'
            f'<td>{r["tk1"]}</td><td>{r["tk2"]}</td><td>{r["lookback"]}</td>'
            f'<td>{exc_str}</td>'
            f'<td class="num">{fmt_pf(r["orig_pf"])}</td>'
            f'<td class="num">{int(r["full_n"])}</td>'
            f'<td class="num">{fmt_pf(r["full_pf"])}</td>'
            f'<td class="num">{fmt_bp(r["full_avg_bp"])}</td>'
            f'<td class="num">{fmt_pct(r["full_wr"])}</td>'
            f'<td class="num">{int(r["n_2y"])}</td>'
            f'<td class="num">{fmt_pf(r["pf_2y"])}</td>'
            f'<td class="num">{fmt_bp(r["avg_bp_2y"])}</td>'
            f'<td class="num">{int(r["n_6m"])}</td>'
            f'<td class="num">{fmt_pf(r["pf_6m"])}</td>'
            f'<td class="num">{fmt_bp(r["avg_bp_6m"])}</td>'
            f'<td class="num emph">{fmt_ratio(ratio_6m)}</td>'
            f'</tr>'
        )

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<title>Pair Health Check {date_str}</title>
<style>
body {{ font-family: -apple-system, sans-serif; background: #0f1419; color: #e6e6e6; padding: 20px; }}
h1 {{ font-size: 1.4em; margin-bottom: 4px; }}
.meta {{ color: #888; font-size: 0.85em; margin-bottom: 16px; }}
table {{ border-collapse: collapse; font-size: 0.82em; }}
th, td {{ border: 1px solid #333; padding: 4px 8px; text-align: left; }}
th {{ background: #1a2030; position: sticky; top: 0; }}
td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
td.emph {{ font-weight: 600; }}
tr.degraded {{ background: rgba(239, 68, 68, 0.12); }}
tr.degraded td.emph {{ color: #f87171; }}
tr.improved {{ background: rgba(34, 197, 94, 0.10); }}
tr.improved td.emph {{ color: #4ade80; }}
.legend {{ margin: 8px 0 16px 0; font-size: 0.82em; color: #aaa; }}
.legend span {{ padding: 2px 6px; margin-right: 8px; border-radius: 3px; }}
.legend .d {{ background: rgba(239, 68, 68, 0.2); color: #f87171; }}
.legend .i {{ background: rgba(34, 197, 94, 0.15); color: #4ade80; }}
</style>
</head>
<body>
<h1>Pair Health Check — {date_str}</h1>
<div class="meta">全 {len(df)} ペア (active {active_count}) / ratio_6m = pf_6m / full_pf</div>
<div class="legend">
  <span class="d">劣化 (ratio_6m &lt; 0.7)</span>
  <span class="i">改善 (ratio_6m &gt; 1.3)</span>
  ソート: ratio_6m 昇順 (劣化順)
</div>
<table>
<thead>
<tr>
<th>tk1</th><th>tk2</th><th>lb</th><th>excl</th>
<th>orig_pf</th>
<th>full_n</th><th>full_pf</th><th>full_bp</th><th>full_wr</th>
<th>n_2y</th><th>pf_2y</th><th>bp_2y</th>
<th>n_6m</th><th>pf_6m</th><th>bp_6m</th>
<th>ratio_6m</th>
</tr>
</thead>
<tbody>
{"".join(rows_html)}
</tbody>
</table>
</body>
</html>
"""


if __name__ == "__main__":
    main()
