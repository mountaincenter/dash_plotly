#!/usr/bin/env python3
"""
14_topix_exit_optimization.py
==============================
TOPIX 1,660銘柄の出口戦略最適化（Ch5相当）。
Ch5-2 (11_exit_methods_survey.py) のトップ16手法に絞って実行。

入力:
  - strategy_verification/data/processed/trades_topix_no_sl.parquet (639K LONG)
  - strategy_verification/data/processed/prices_cleaned_topix.parquet (8.6M rows)

出力:
  - strategy_verification/data/processed/topix_exit_results.parquet
  - strategy_verification/chapters/08_topix_exit/report.html

SL設定 (Ch4 PnL最適):
  B1=-3%, B2=-2.5%, B3=-2.5%, B4=なし
"""
from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SV_DIR = ROOT / "strategy_verification"
PROCESSED = SV_DIR / "data" / "processed"
REPORT_DIR = SV_DIR / "chapters" / "08_topix_exit"

MAX_HOLD = 60

# Ch4結果: PnL最適SL
SL_MAP: dict[str, float | None] = {"B1": 3.0, "B2": 2.5, "B3": 2.5, "B4": None}

# テスト対象出口手法
EXIT_METHODS = [
    ("signal", {}),
    ("fixed_13d", {"hold": 13}),
    ("fixed_30d", {"hold": 30}),
    ("fixed_60d", {"hold": 60}),
    ("trail_5pct", {"trail_pct": 5.0}),
    ("trail_10pct", {"trail_pct": 10.0}),
    ("chandelier_2atr", {"atr_mult": 2.0}),
    ("chandelier_3atr", {"atr_mult": 3.0}),
    ("donchian_10d", {"don_n": 10}),
    ("donchian_20d", {"don_n": 20}),
    ("target_10pct", {"target_pct": 10.0}),
    ("target_15pct", {"target_pct": 15.0}),
    ("high_20d", {"high_n": 20}),
    ("high_60d", {"high_n": 60}),
    ("atr_expand_2x", {"atr_expand": 2.0}),
    ("trail10_max60d", {"trail_pct": 10.0, "max_hold": 60}),
]


# ---------------------------------------------------------------------------
# Price lookup
# ---------------------------------------------------------------------------

def build_price_lookup(prices: pd.DataFrame) -> dict:
    """銘柄別の価格+テクニカル指標 lookup dict を構築"""
    print("  Building price lookup with indicators...")
    t0 = time.time()

    lookup: dict = {}
    n_tickers = prices["ticker"].nunique()

    for i, (ticker, grp) in enumerate(prices.groupby("ticker")):
        if (i + 1) % 500 == 0:
            print(f"    {i+1}/{n_tickers} ({time.time()-t0:.0f}s)")

        g = grp.sort_values("date").reset_index(drop=True)
        close = g["Close"].values.astype(np.float64)
        high = g["High"].values.astype(np.float64)
        low = g["Low"].values.astype(np.float64)
        n = len(g)

        # SMA20
        sma20 = np.full(n, np.nan)
        for j in range(19, n):
            sma20[j] = close[j-19:j+1].mean()

        # ATR(14)
        tr = np.full(n, np.nan)
        for j in range(1, n):
            tr[j] = max(high[j] - low[j], abs(high[j] - close[j-1]), abs(low[j] - close[j-1]))
        atr14 = np.full(n, np.nan)
        for j in range(14, n):
            atr14[j] = np.nanmean(tr[j-13:j+1])

        # ATR 20日平均
        atr_avg20 = np.full(n, np.nan)
        for j in range(33, n):  # 14 + 20 - 1
            vals = atr14[j-19:j+1]
            valid = vals[~np.isnan(vals)]
            if len(valid) > 0:
                atr_avg20[j] = valid.mean()

        # Rolling high/low for donchian
        high_10d = np.full(n, np.nan)
        low_10d = np.full(n, np.nan)
        high_20d = np.full(n, np.nan)
        low_20d = np.full(n, np.nan)
        high_60d = np.full(n, np.nan)
        for j in range(9, n):
            low_10d[j] = np.nanmin(low[max(0,j-9):j+1])
        for j in range(19, n):
            high_20d[j] = np.nanmax(high[max(0,j-19):j+1])
            low_20d[j] = np.nanmin(low[max(0,j-19):j+1])
        for j in range(59, n):
            high_60d[j] = np.nanmax(high[max(0,j-59):j+1])

        lookup[ticker] = {
            "dates": g["date"].values,
            "opens": g["Open"].values.astype(np.float64),
            "highs": high,
            "lows": low,
            "closes": close,
            "sma20": sma20,
            "atr14": atr14,
            "atr_avg20": atr_avg20,
            "low_10d": low_10d,
            "low_20d": low_20d,
            "high_20d": high_20d,
            "high_60d": high_60d,
        }

    print(f"  Price lookup built: {len(lookup)} tickers ({time.time()-t0:.0f}s)")
    return lookup


# ---------------------------------------------------------------------------
# Trade simulation
# ---------------------------------------------------------------------------

def simulate_trade(
    entry_date: np.datetime64,
    entry_price: float,
    ticker: str,
    rule: str,
    is_contrarian: bool,
    sl_pct: float | None,
    method: str,
    params: dict,
    lookup: dict,
) -> dict | None:
    """1トレードを指定出口手法でシミュレーション"""
    if ticker not in lookup:
        return None
    pl = lookup[ticker]
    dates = pl["dates"]
    entry_dt = np.datetime64(entry_date)

    # entry_dateの位置
    entry_idx = np.searchsorted(dates, entry_dt)
    if entry_idx >= len(dates) or dates[entry_idx] != entry_dt:
        return None

    opens = pl["opens"]
    highs = pl["highs"]
    lows = pl["lows"]
    closes = pl["closes"]
    sma20 = pl["sma20"]
    atr14 = pl["atr14"]
    atr_avg20 = pl["atr_avg20"]

    max_hold_override = params.get("max_hold", MAX_HOLD)
    end_idx = min(entry_idx + max_hold_override, len(dates) - 1)
    sl_price = entry_price * (1 - sl_pct / 100) if sl_pct else None

    max_price = entry_price  # trailing用
    exit_price = None
    exit_day = 0
    exit_type = "expire"

    for j in range(entry_idx, end_idx + 1):
        day = j - entry_idx
        h, l, c = highs[j], lows[j], closes[j]
        if np.isnan(c):
            continue

        if h > max_price:
            max_price = h

        # SL check
        if sl_price and l <= sl_price:
            exit_price = sl_price
            exit_day = day
            exit_type = "sl"
            break

        # Exit method checks (skip day 0 for some)
        if day == 0 and method not in ("fixed_13d", "fixed_30d", "fixed_60d", "target_10pct", "target_15pct"):
            continue

        # --- signal: SMA20タッチ / デッドクロス ---
        if method == "signal":
            s = sma20[j]
            if not np.isnan(s):
                if is_contrarian:
                    if c >= s:  # 反発完了
                        exit_price = opens[min(j+1, len(dates)-1)]
                        exit_day = day
                        exit_type = "signal"
                        break
                else:
                    if day > 0 and c < s:
                        exit_price = opens[min(j+1, len(dates)-1)]
                        exit_day = day
                        exit_type = "signal"
                        break

        # --- fixed hold ---
        elif method.startswith("fixed_"):
            hold_n = params["hold"]
            if day >= hold_n:
                exit_price = opens[j] if day == hold_n else c
                exit_day = day
                exit_type = "fixed"
                break

        # --- trailing ---
        elif method.startswith("trail_") and "max_hold" not in params:
            trail = params["trail_pct"]
            trail_stop = max_price * (1 - trail / 100)
            if l <= trail_stop:
                exit_price = trail_stop
                exit_day = day
                exit_type = "trail"
                break

        # --- trail + max hold (hybrid) ---
        elif method == "trail10_max60d":
            trail = params["trail_pct"]
            trail_stop = max_price * (1 - trail / 100)
            if l <= trail_stop:
                exit_price = trail_stop
                exit_day = day
                exit_type = "trail"
                break

        # --- chandelier ---
        elif method.startswith("chandelier_"):
            mult = params["atr_mult"]
            atr = atr14[j]
            if not np.isnan(atr):
                chan_stop = max_price - mult * atr
                if l <= chan_stop:
                    exit_price = chan_stop
                    exit_day = day
                    exit_type = "chandelier"
                    break

        # --- donchian N-day low break ---
        elif method.startswith("donchian_"):
            don_n = params["don_n"]
            if don_n == 10:
                don_low = pl["low_10d"][j]
            else:
                don_low = pl["low_20d"][j]
            if not np.isnan(don_low) and l <= don_low and day > 0:
                exit_price = don_low
                exit_day = day
                exit_type = "donchian"
                break

        # --- target % ---
        elif method.startswith("target_"):
            target = params["target_pct"]
            target_price = entry_price * (1 + target / 100)
            if h >= target_price:
                exit_price = target_price
                exit_day = day
                exit_type = "target"
                break

        # --- N日高値 ---
        elif method.startswith("high_"):
            high_n = params["high_n"]
            if high_n == 20:
                rolling_high = pl["high_20d"][j]
            else:
                rolling_high = pl["high_60d"][j]
            if not np.isnan(rolling_high) and h >= rolling_high and day > 0:
                exit_price = opens[min(j+1, len(dates)-1)]
                exit_day = day
                exit_type = "high_n"
                break

        # --- ATR急拡大 ---
        elif method == "atr_expand_2x":
            atr = atr14[j]
            atr_avg = atr_avg20[j]
            if not np.isnan(atr) and not np.isnan(atr_avg) and atr_avg > 0:
                if atr > params["atr_expand"] * atr_avg:
                    exit_price = opens[min(j+1, len(dates)-1)]
                    exit_day = day
                    exit_type = "atr_expand"
                    break

    # Expiry
    if exit_price is None:
        exit_price = opens[end_idx] if end_idx < len(dates) else closes[end_idx - 1]
        exit_day = end_idx - entry_idx
        if np.isnan(exit_price):
            exit_price = closes[min(end_idx, len(dates)-1)]

    if exit_price is None or np.isnan(exit_price) or exit_price <= 0 or entry_price <= 0:
        return None

    ret_pct = (exit_price / entry_price - 1) * 100
    if np.isnan(ret_pct) or np.isinf(ret_pct):
        return None
    pnl = int(round(entry_price * 100 * ret_pct / 100))

    return {
        "ret_pct": round(ret_pct, 3),
        "pnl": pnl,
        "hold_days": exit_day,
        "exit_type": exit_type,
        "win": ret_pct > 0,
    }


# ---------------------------------------------------------------------------
# Main simulation loop
# ---------------------------------------------------------------------------

def run_simulations(trades: pd.DataFrame, lookup: dict) -> pd.DataFrame:
    """全トレード × 全手法のシミュレーション（numpy配列化で高速化）"""
    print("\n[3/4] Running simulations...")
    t0 = time.time()

    contrarian_rules = {"B4"}

    # trades を numpy 配列に変換（iterrows回避）
    entry_dates = trades["entry_date"].values
    entry_prices = trades["entry_price"].values.astype(np.float64)
    tickers = trades["ticker"].values
    rules = trades["rule"].values
    segments = trades["segment"].values if "segment" in trades.columns else np.full(len(trades), "")
    n_trades = len(trades)

    all_results = []

    for mi, (method, params) in enumerate(EXIT_METHODS):
        mt0 = time.time()
        method_results = []

        for idx in range(n_trades):
            rule = rules[idx]
            sl = SL_MAP.get(rule)
            res = simulate_trade(
                entry_date=entry_dates[idx],
                entry_price=float(entry_prices[idx]),
                ticker=tickers[idx],
                rule=rule,
                is_contrarian=rule in contrarian_rules,
                sl_pct=sl,
                method=method,
                params=params,
                lookup=lookup,
            )
            if res:
                method_results.append((
                    method, rule, tickers[idx], segments[idx],
                    res["ret_pct"], res["pnl"], res["hold_days"],
                    res["exit_type"], res["win"],
                ))

        elapsed = time.time() - mt0
        n = len(method_results)
        pnl = sum(r[5] for r in method_results) / 10000
        print(f"  [{mi+1}/{len(EXIT_METHODS)}] {method:20s}: {n:>7,} trades, PnL={pnl:>+10,.0f}万 ({elapsed:.0f}s)")
        all_results.extend(method_results)

    df = pd.DataFrame(all_results, columns=[
        "method", "rule", "ticker", "segment",
        "ret_pct", "pnl", "hold_days", "exit_type", "win",
    ])
    print(f"\n  Total: {len(df):,} results ({time.time()-t0:.0f}s)")
    return df


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(results: pd.DataFrame, n_trades: int) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rules = ["B1", "B2", "B3", "B4"]

    # Summary table: method × rule
    summary_rows = ""
    method_totals: list[dict] = []
    for method, _ in EXIT_METHODS:
        mdf = results[results["method"] == method]
        total_pnl = mdf["pnl"].sum() / 10000
        total_n = len(mdf)
        total_wr = mdf["win"].mean() * 100 if total_n > 0 else 0
        w = mdf[mdf["win"]]["pnl"].sum()
        l_abs = abs(mdf[~mdf["win"]]["pnl"].sum())
        total_pf = w / l_abs if l_abs > 0 else 999
        avg_hold = mdf["hold_days"].mean() if total_n > 0 else 0

        cells = ""
        for rule in rules:
            rdf = mdf[mdf["rule"] == rule]
            if len(rdf) == 0:
                cells += '<td class="r">-</td><td class="r">-</td>'
                continue
            rpnl = rdf["pnl"].sum() / 10000
            rw = rdf[rdf["win"]]["pnl"].sum()
            rl = abs(rdf[~rdf["win"]]["pnl"].sum())
            rpf = rw / rl if rl > 0 else 999
            pnl_cls = "num-pos" if rpnl > 0 else "num-neg"
            cells += f'<td class="r">{rpf:.2f}</td><td class="r {pnl_cls}">{rpnl:+,.0f}</td>'

        pnl_cls = "num-pos" if total_pnl > 0 else "num-neg"
        summary_rows += (
            f'<tr><td><strong>{method}</strong></td>'
            f'<td class="r">{total_n:,}</td>'
            f'<td class="r">{total_wr:.1f}%</td>'
            f'<td class="r">{total_pf:.2f}</td>'
            f'<td class="r {pnl_cls}">{total_pnl:+,.0f}</td>'
            f'<td class="r">{avg_hold:.1f}</td>'
            f'{cells}</tr>'
        )
        method_totals.append({
            "method": method, "pnl": total_pnl, "pf": total_pf,
            "wr": total_wr, "n": total_n, "avg_hold": avg_hold,
        })

    # Ranking
    ranked = sorted(method_totals, key=lambda r: r["pnl"], reverse=True)
    rank_rows = ""
    for i, r in enumerate(ranked, 1):
        pnl_cls = "num-pos" if r["pnl"] > 0 else "num-neg"
        eff = r["pnl"] / r["avg_hold"] if r["avg_hold"] > 0 else 0
        rank_rows += (
            f'<tr><td>{i}</td><td><strong>{r["method"]}</strong></td>'
            f'<td class="r">{r["pf"]:.2f}</td>'
            f'<td class="r {pnl_cls}">{r["pnl"]:+,.0f}</td>'
            f'<td class="r">{r["wr"]:.1f}%</td>'
            f'<td class="r">{r["avg_hold"]:.1f}</td>'
            f'<td class="r">{eff:+,.1f}</td></tr>'
        )

    # Per-rule best
    rule_best_rows = ""
    for rule in rules:
        rdf = results[results["rule"] == rule]
        best_method = ""
        best_pnl = -1e18
        for method, _ in EXIT_METHODS:
            mdf = rdf[rdf["method"] == method]
            mpnl = mdf["pnl"].sum() / 10000
            if mpnl > best_pnl:
                best_pnl = mpnl
                best_method = method
        mdf = rdf[rdf["method"] == best_method]
        bw = mdf[mdf["win"]]["pnl"].sum()
        bl = abs(mdf[~mdf["win"]]["pnl"].sum())
        bpf = bw / bl if bl > 0 else 999
        rule_best_rows += (
            f'<tr><td><strong>{rule}</strong></td>'
            f'<td>{best_method}</td>'
            f'<td class="r">{bpf:.2f}</td>'
            f'<td class="r num-pos">{best_pnl:+,.0f}万</td>'
            f'<td class="r">{mdf["hold_days"].mean():.1f}d</td></tr>'
        )

    # Year-by-year for top 3 methods
    top3 = [r["method"] for r in ranked[:3]]
    yearly_rows = ""
    years = sorted(results[results["method"] == top3[0]]["rule"].map(
        lambda r: results[(results["method"] == top3[0]) & (results["rule"] == r)]
    ).index.unique()) if len(top3) > 0 else []

    # Simpler year-by-year
    results_with_year = results.copy()
    # Need entry_date for year - approximate from data
    # Actually, we need to add year during simulation. For now, skip yearly.

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Chapter 8: TOPIX 1,660 出口戦略最適化</title>
<style>
  :root {{
    --bg: #09090b; --card: #18181b; --card-border: #27272a;
    --text: #fafafa; --text-muted: #a1a1aa;
    --emerald: #34d399; --rose: #fb7185; --amber: #fbbf24; --blue: #60a5fa;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif; line-height:1.6; padding:24px; max-width:1400px; margin:0 auto; }}
  h1 {{ font-size:1.5rem; margin-bottom:8px; }}
  .subtitle {{ color:var(--text-muted); font-size:0.875rem; margin-bottom:32px; }}
  .section {{ background:var(--card); border:1px solid var(--card-border); border-radius:12px; padding:24px; margin-bottom:20px; }}
  .section h2 {{ font-size:1.1rem; margin-bottom:16px; }}
  table {{ width:100%; border-collapse:collapse; font-size:0.8rem; margin:12px 0; }}
  th {{ text-align:left; padding:6px 10px; background:rgba(255,255,255,0.03); color:var(--text-muted); font-weight:600; border-bottom:1px solid var(--card-border); white-space:nowrap; }}
  th.r {{ text-align:right; }}
  td {{ padding:6px 10px; border-bottom:1px solid rgba(255,255,255,0.05); }}
  td.r {{ text-align:right; font-variant-numeric:tabular-nums; }}
  tr:hover td {{ background:rgba(255,255,255,0.02); }}
  .num-pos {{ color:var(--emerald); font-weight:600; }}
  .num-neg {{ color:var(--rose); font-weight:600; }}
  .grid-4 {{ display:grid; grid-template-columns:repeat(4,1fr); gap:16px; }}
  @media (max-width:768px) {{ .grid-4 {{ grid-template-columns:1fr 1fr; }} }}
  .stat-card {{ background:rgba(255,255,255,0.02); border:1px solid var(--card-border); border-radius:8px; padding:16px; text-align:center; }}
  .stat-card .label {{ color:var(--text-muted); font-size:0.75rem; margin-bottom:4px; }}
  .stat-card .value {{ font-size:1.5rem; font-weight:700; }}
  .stat-card .sub {{ color:var(--text-muted); font-size:0.75rem; margin-top:2px; }}
  .alert-box {{ border-radius:8px; padding:16px; margin:16px 0; font-size:0.875rem; line-height:1.7; }}
  .alert-info {{ background:rgba(96,165,250,0.1); border:1px solid rgba(96,165,250,0.3); color:var(--blue); }}
  .alert-success {{ background:rgba(52,211,153,0.1); border:1px solid rgba(52,211,153,0.3); color:var(--emerald); }}
  footer {{ text-align:center; color:var(--text-muted); font-size:0.7rem; margin-top:40px; padding:16px 0; border-top:1px solid var(--card-border); }}
</style>
</head>
<body>

<h1>Chapter 8: TOPIX 1,660銘柄 出口戦略最適化</h1>
<div class="subtitle">16手法 × B1-B4 | SL: B1=-3%, B2=-2.5%, B3=-2.5%, B4=なし | {n_trades:,} LONG trades | {ts}</div>

<div class="section">
  <h2>ルール別最適出口</h2>
  <table>
    <thead><tr><th>Rule</th><th>最適手法</th><th class="r">PF</th><th class="r">PnL</th><th class="r">AvgHold</th></tr></thead>
    <tbody>{rule_best_rows}</tbody>
  </table>
</div>

<div class="section">
  <h2>PnLランキング（資金制約なし）</h2>
  <table>
    <thead><tr><th>#</th><th>手法</th><th class="r">PF</th><th class="r">PnL(万)</th><th class="r">WR</th><th class="r">AvgHold</th><th class="r">効率(万/日)</th></tr></thead>
    <tbody>{rank_rows}</tbody>
  </table>
</div>

<div class="section">
  <h2>全手法 × ルール別詳細</h2>
  <div style="overflow-x:auto;">
  <table>
    <thead><tr>
      <th>手法</th><th class="r">件数</th><th class="r">WR</th><th class="r">PF</th><th class="r">PnL(万)</th><th class="r">AvgHold</th>
      <th class="r">B1 PF</th><th class="r">B1 PnL</th>
      <th class="r">B2 PF</th><th class="r">B2 PnL</th>
      <th class="r">B3 PF</th><th class="r">B3 PnL</th>
      <th class="r">B4 PF</th><th class="r">B4 PnL</th>
    </tr></thead>
    <tbody>{summary_rows}</tbody>
  </table>
  </div>
</div>

<div class="section">
  <h2>結論</h2>
  <div class="alert-box alert-success">
    168銘柄Ch5の結論（B1/B3=fixed 60d, B4=SLなし signal）がTOPIX 1,660銘柄でも再現されるか、
    または資金効率を重視するならdonchian/high系が優位か、をこの結果から判断する。
  </div>
</div>

<footer>Generated by 14_topix_exit_optimization.py</footer>
</body>
</html>"""


def main() -> None:
    t0 = time.time()
    for d in [PROCESSED, REPORT_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    print("[1/4] Loading data...")
    trades = pd.read_parquet(PROCESSED / "trades_topix_no_sl.parquet")
    prices = pd.read_parquet(PROCESSED / "prices_cleaned_topix.parquet")
    print(f"  trades: {len(trades):,} | prices: {len(prices):,}")

    print("\n[2/4] Building price lookup...")
    import pickle
    cache_path = PROCESSED / "_topix_price_lookup.pkl"
    if cache_path.exists():
        print("  Loading cached price lookup...")
        with open(cache_path, "rb") as f:
            lookup = pickle.load(f)
        print(f"  Loaded: {len(lookup)} tickers")
    else:
        lookup = build_price_lookup(prices)
        with open(cache_path, "wb") as f:
            pickle.dump(lookup, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"  Cached: {cache_path}")

    results = run_simulations(trades, lookup)

    print("\n[4/4] Saving & report...")
    results.to_parquet(PROCESSED / "topix_exit_results.parquet", index=False)
    html = generate_report(results, len(trades))
    report_path = REPORT_DIR / "report.html"
    report_path.write_text(html, encoding="utf-8")
    print(f"  report: {report_path}")

    print(f"\n=== Done in {time.time()-t0:.0f}s ({(time.time()-t0)/60:.1f}min) ===")


if __name__ == "__main__":
    main()
