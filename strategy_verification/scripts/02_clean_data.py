#!/usr/bin/env python3
"""
02_clean_data.py
================
生トレードデータ + 価格データをクリーニングし、分析可能な状態にする。

入力:
  - improvement/granville/step1/trades_no_sl.parquet (117,564 trades)
  - data/parquet/prices_max_1d.parquet (価格データ)

出力:
  - strategy_verification/data/processed/trades_cleaned.parquet
  - strategy_verification/data/processed/prices_cleaned.parquet
  - strategy_verification/chapters/01_data_quality/cleaning_log.html

クリーニングルール:
  1. 8766.T 2005-09-29以前 → 価格データ破損（yfinance分割調整エラー）
  2. Volume=0 日にエントリーしたトレード → 寄付価格が信頼不可
  3. hold_days == 0 → データ終端アーティファクト（2026-02-27）
  4. 価格データから Volume=0 行を除外 → MAE/MFE計算時のHigh/Low信頼性
"""
from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]  # dash_plotly/
TRADES_RAW = ROOT / "improvement" / "granville" / "step1" / "trades_no_sl.parquet"
PRICES_RAW = ROOT / "data" / "parquet" / "prices_max_1d.parquet"

OUT_DIR = ROOT / "strategy_verification" / "data" / "processed"
REPORT_DIR = ROOT / "strategy_verification" / "chapters" / "01_data_quality"


def load_raw() -> tuple[pd.DataFrame, pd.DataFrame]:
    print("[1/4] Loading raw data...")
    trades = pd.read_parquet(TRADES_RAW)
    prices = pd.read_parquet(PRICES_RAW)
    prices["date"] = pd.to_datetime(prices["date"])
    trades["entry_date"] = pd.to_datetime(trades["entry_date"])
    trades["exit_date"] = pd.to_datetime(trades["exit_date"])
    trades["signal_date"] = pd.to_datetime(trades["signal_date"])
    print(f"  trades: {len(trades):,} rows, {trades['ticker'].nunique()} tickers")
    print(f"  prices: {len(prices):,} rows, {prices['ticker'].nunique()} tickers")
    return trades, prices


def clean_prices(prices: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """価格データクリーニング"""
    print("[2/4] Cleaning prices...")
    log: list[dict] = []
    n0 = len(prices)

    # Rule 1: 8766.T before 2005-09-29 (corrupt adjusted prices)
    mask_8766 = (prices["ticker"] == "8766.T") & (prices["date"] < "2005-09-29")
    n_8766 = mask_8766.sum()
    prices = prices[~mask_8766].copy()
    log.append({
        "rule": "P1",
        "description": "8766.T 2005-09-29以前の価格データ（yfinance分割調整破損）",
        "removed": n_8766,
        "severity": "致命的",
    })

    # Rule 2: Volume=0 rows (holiday dummy / no real OHLC)
    mask_vol0 = prices["Volume"] == 0
    n_vol0 = mask_vol0.sum()
    prices = prices[~mask_vol0].copy()
    log.append({
        "rule": "P2",
        "description": "Volume=0 行（祝日ダミー / OHLC信頼不可）",
        "removed": n_vol0,
        "severity": "中程度",
    })

    print(f"  removed: {n0 - len(prices):,} rows ({n0:,} → {len(prices):,})")
    return prices, log


def clean_trades(
    trades: pd.DataFrame, prices_clean: pd.DataFrame
) -> tuple[pd.DataFrame, list[dict]]:
    """トレードデータクリーニング"""
    print("[3/4] Cleaning trades...")
    log: list[dict] = []
    n0 = len(trades)

    # Rule 1: 8766.T trades before 2005-09-29 (entry on corrupt data)
    mask_8766 = (trades["ticker"] == "8766.T") & (trades["entry_date"] < "2005-09-29")
    n_8766 = mask_8766.sum()
    trades = trades[~mask_8766].copy()
    log.append({
        "rule": "T1",
        "description": "8766.T 2005-09-29以前のトレード（破損価格データ上のシグナル）",
        "removed": n_8766,
        "severity": "致命的",
    })

    # Rule 2: Entry on Volume=0 day (entry_price unreliable)
    # Build set of (ticker, date) with valid volume
    valid_dates = set(
        zip(prices_clean["ticker"], prices_clean["date"].dt.strftime("%Y-%m-%d"))
    )
    trades["_entry_str"] = trades["entry_date"].dt.strftime("%Y-%m-%d")
    mask_vol0_entry = ~trades.apply(
        lambda r: (r["ticker"], r["_entry_str"]) in valid_dates, axis=1
    )
    n_vol0 = mask_vol0_entry.sum()
    trades = trades[~mask_vol0_entry].copy()
    trades = trades.drop(columns=["_entry_str"])
    log.append({
        "rule": "T2",
        "description": "Volume=0 日にエントリーしたトレード（寄付価格が信頼不可）",
        "removed": n_vol0,
        "severity": "中程度",
    })

    # Rule 3: hold_days == 0 (data edge artifact)
    mask_hold0 = trades["hold_days"] == 0
    n_hold0 = mask_hold0.sum()
    trades = trades[~mask_hold0].copy()
    log.append({
        "rule": "T3",
        "description": "hold_days=0（データ終端アーティファクト 2026-02-27）",
        "removed": n_hold0,
        "severity": "軽微",
    })

    # Rule 4: entry_price < 1 (residual corrupt data)
    mask_low = trades["entry_price"] < 1.0
    n_low = mask_low.sum()
    trades = trades[~mask_low].copy()
    log.append({
        "rule": "T4",
        "description": "entry_price < 1.0（残存する破損データ）",
        "removed": n_low,
        "severity": "致命的",
    })

    print(f"  removed: {n0 - len(trades):,} trades ({n0:,} → {len(trades):,})")
    return trades, log


def generate_cleaning_report(
    price_log: list[dict],
    trade_log: list[dict],
    n_prices_before: int,
    n_prices_after: int,
    n_trades_before: int,
    n_trades_after: int,
) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def row(entry: dict) -> str:
        sev_cls = {
            "致命的": "sev-1",
            "中程度": "sev-2",
            "軽微": "sev-3",
        }.get(entry["severity"], "")
        return (
            f'<tr><td>{entry["rule"]}</td>'
            f'<td class="{sev_cls}">{entry["severity"]}</td>'
            f'<td>{entry["description"]}</td>'
            f'<td class="r">{entry["removed"]:,}</td></tr>'
        )

    price_rows = "\n".join(row(e) for e in price_log)
    trade_rows = "\n".join(row(e) for e in trade_log)
    total_price_removed = sum(e["removed"] for e in price_log)
    total_trade_removed = sum(e["removed"] for e in trade_log)

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cleaning Log — データクリーニング結果</title>
<style>
  :root {{
    --bg: #09090b; --card: #18181b; --card-border: #27272a;
    --text: #fafafa; --text-muted: #a1a1aa;
    --emerald: #34d399; --rose: #fb7185; --amber: #fbbf24; --blue: #60a5fa;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif; line-height:1.6; padding:24px; max-width:1200px; margin:0 auto; }}
  h1 {{ font-size:1.5rem; margin-bottom:8px; }}
  .subtitle {{ color:var(--text-muted); font-size:0.875rem; margin-bottom:32px; }}
  .section {{ background:var(--card); border:1px solid var(--card-border); border-radius:12px; padding:24px; margin-bottom:20px; }}
  .section h2 {{ font-size:1.1rem; margin-bottom:16px; }}
  table {{ width:100%; border-collapse:collapse; font-size:0.85rem; margin:12px 0; }}
  th {{ text-align:left; padding:8px 12px; background:rgba(255,255,255,0.03); color:var(--text-muted); font-weight:600; border-bottom:1px solid var(--card-border); white-space:nowrap; }}
  th.r {{ text-align:right; }}
  td {{ padding:8px 12px; border-bottom:1px solid rgba(255,255,255,0.05); }}
  td.r {{ text-align:right; font-variant-numeric:tabular-nums; }}
  .grid-4 {{ display:grid; grid-template-columns:1fr 1fr 1fr 1fr; gap:16px; }}
  @media (max-width:768px) {{ .grid-4 {{ grid-template-columns:1fr 1fr; }} }}
  .stat-card {{ background:rgba(255,255,255,0.02); border:1px solid var(--card-border); border-radius:8px; padding:16px; text-align:center; }}
  .stat-card .label {{ color:var(--text-muted); font-size:0.75rem; margin-bottom:4px; }}
  .stat-card .value {{ font-size:1.5rem; font-weight:700; }}
  .stat-card .sub {{ color:var(--text-muted); font-size:0.75rem; margin-top:2px; }}
  .sev-1 {{ color: var(--rose); font-weight: 700; }}
  .sev-2 {{ color: var(--amber); font-weight: 700; }}
  .sev-3 {{ color: var(--blue); }}
  .alert-box {{ border-radius:8px; padding:16px; margin:16px 0; font-size:0.875rem; line-height:1.7; }}
  .alert-success {{ background:rgba(52,211,153,0.1); border:1px solid rgba(52,211,153,0.3); color:var(--emerald); }}
  footer {{ text-align:center; color:var(--text-muted); font-size:0.7rem; margin-top:40px; padding:16px 0; border-top:1px solid var(--card-border); }}
</style>
</head>
<body>

<h1>Cleaning Log: データクリーニング結果</h1>
<div class="subtitle">Generated: {ts}</div>

<div class="section">
  <h2>サマリー</h2>
  <div class="grid-4">
    <div class="stat-card">
      <div class="label">価格データ（前）</div>
      <div class="value">{n_prices_before:,}</div>
    </div>
    <div class="stat-card">
      <div class="label">価格データ（後）</div>
      <div class="value">{n_prices_after:,}</div>
      <div class="sub">-{total_price_removed:,} ({total_price_removed/n_prices_before*100:.1f}%)</div>
    </div>
    <div class="stat-card">
      <div class="label">トレード（前）</div>
      <div class="value">{n_trades_before:,}</div>
    </div>
    <div class="stat-card">
      <div class="label">トレード（後）</div>
      <div class="value">{n_trades_after:,}</div>
      <div class="sub">-{total_trade_removed:,} ({total_trade_removed/n_trades_before*100:.1f}%)</div>
    </div>
  </div>
</div>

<div class="section">
  <h2>価格データ クリーニング</h2>
  <table>
    <thead><tr><th>Rule</th><th>重要度</th><th>説明</th><th class="r">除外行数</th></tr></thead>
    <tbody>{price_rows}</tbody>
  </table>
</div>

<div class="section">
  <h2>トレードデータ クリーニング</h2>
  <table>
    <thead><tr><th>Rule</th><th>重要度</th><th>説明</th><th class="r">除外件数</th></tr></thead>
    <tbody>{trade_rows}</tbody>
  </table>
</div>

<div class="section">
  <div class="alert-box alert-success">
    クリーニング完了。除外率はトレード {total_trade_removed/n_trades_before*100:.2f}%、価格 {total_price_removed/n_prices_before*100:.2f}%。<br>
    出力: <code>data/processed/trades_cleaned.parquet</code>, <code>data/processed/prices_cleaned.parquet</code>
  </div>
</div>

<footer>Generated by 02_clean_data.py | strategy_verification/chapters/01_data_quality</footer>
</body>
</html>"""


def main():
    t0 = time.time()
    trades_raw, prices_raw = load_raw()
    n_prices_before = len(prices_raw)
    n_trades_before = len(trades_raw)

    prices_clean, price_log = clean_prices(prices_raw)
    trades_clean, trade_log = clean_trades(trades_raw, prices_clean)

    # Save
    print("[4/4] Saving...")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    trades_clean.to_parquet(OUT_DIR / "trades_cleaned.parquet", index=False)
    prices_clean.to_parquet(OUT_DIR / "prices_cleaned.parquet", index=False)
    print(f"  trades: {OUT_DIR / 'trades_cleaned.parquet'}")
    print(f"  prices: {OUT_DIR / 'prices_cleaned.parquet'}")

    # Report
    html = generate_cleaning_report(
        price_log, trade_log,
        n_prices_before, len(prices_clean),
        n_trades_before, len(trades_clean),
    )
    report_path = REPORT_DIR / "cleaning_log.html"
    report_path.write_text(html, encoding="utf-8")
    print(f"  report: {report_path}")

    print(f"\n=== Done in {time.time()-t0:.1f}s ===")
    print(f"  prices: {n_prices_before:,} → {len(prices_clean):,} (-{n_prices_before-len(prices_clean):,})")
    print(f"  trades: {n_trades_before:,} → {len(trades_clean):,} (-{n_trades_before-len(trades_clean):,})")


if __name__ == "__main__":
    main()
