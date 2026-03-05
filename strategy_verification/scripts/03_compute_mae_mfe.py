#!/usr/bin/env python3
"""
03_compute_mae_mfe.py
=====================
クリーニング済みデータにMAE/MFEカラムを付与し、Chapter 2レポートを生成する。

入力:
  - strategy_verification/data/processed/trades_cleaned.parquet
  - strategy_verification/data/processed/prices_cleaned.parquet

出力:
  - strategy_verification/data/processed/trades_with_mae_mfe.parquet
  - strategy_verification/chapters/02_mae_mfe_raw/report.html

MAE/MFE計算仕様:
  - 保有期間: entry_date <= date < exit_date（exit_dateはOpen約定なので含まない）
  - LONG: MAE = (min_low / entry - 1)*100,  MFE = (max_high / entry - 1)*100
  - SHORT: MAE = (1 - max_high / entry)*100, MFE = (1 - min_low / entry)*100
  - overnight_gap_pct: signal_date Close → entry_date Open のギャップ
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]  # dash_plotly/
SV_DIR = ROOT / "strategy_verification"
PROCESSED = SV_DIR / "data" / "processed"
REPORT_DIR = SV_DIR / "chapters" / "02_mae_mfe_raw"


# ---------------------------------------------------------------------------
# 1. MAE/MFE 計算
# ---------------------------------------------------------------------------

def compute_mae_mfe(trades: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """全トレードにMAE/MFEカラムを付与"""
    print("[2/3] Computing MAE/MFE for {:,} trades...".format(len(trades)))
    t0 = time.time()

    # 価格を ticker → numpy arrays に変換（高速ルックアップ）
    price_lookup: dict[str, dict] = {}
    for ticker, grp in prices.groupby("ticker"):
        grp = grp.sort_values("date")
        price_lookup[ticker] = {
            "dates": grp["date"].values,
            "opens": grp["Open"].values.astype(np.float64),
            "highs": grp["High"].values.astype(np.float64),
            "lows": grp["Low"].values.astype(np.float64),
            "closes": grp["Close"].values.astype(np.float64),
        }

    mae_pcts = np.full(len(trades), np.nan)
    mfe_pcts = np.full(len(trades), np.nan)
    mae_days = np.full(len(trades), -1, dtype=np.int32)
    mfe_days = np.full(len(trades), -1, dtype=np.int32)
    overnight_gaps = np.full(len(trades), np.nan)

    trades_reset = trades.reset_index(drop=True)
    skip_count = 0

    for idx in range(len(trades_reset)):
        row = trades_reset.iloc[idx]
        ticker = row["ticker"]
        is_long = row["direction"] == "LONG"
        entry_price = float(row["entry_price"])
        entry_date = row["entry_date"]
        exit_date = row["exit_date"]
        signal_date = row["signal_date"]

        if ticker not in price_lookup:
            skip_count += 1
            continue

        pl = price_lookup[ticker]
        dates = pl["dates"]
        opens = pl["opens"]
        highs = pl["highs"]
        lows = pl["lows"]
        closes = pl["closes"]

        entry_dt = np.datetime64(entry_date)
        exit_dt = np.datetime64(exit_date)
        signal_dt = np.datetime64(signal_date)

        # 保有期間: entry_date <= date < exit_date
        mask = (dates >= entry_dt) & (dates < exit_dt)
        hold_indices = np.where(mask)[0]

        if len(hold_indices) == 0:
            skip_count += 1
            continue

        hold_highs = highs[hold_indices]
        hold_lows = lows[hold_indices]

        if is_long:
            min_low = np.nanmin(hold_lows)
            max_high = np.nanmax(hold_highs)
            mae_pcts[idx] = (min_low / entry_price - 1) * 100
            mfe_pcts[idx] = (max_high / entry_price - 1) * 100
            mae_days[idx] = int(np.nanargmin(hold_lows))
            mfe_days[idx] = int(np.nanargmax(hold_highs))
        else:
            max_high = np.nanmax(hold_highs)
            min_low = np.nanmin(hold_lows)
            mae_pcts[idx] = (1 - max_high / entry_price) * 100  # negative when price goes up
            mfe_pcts[idx] = (1 - min_low / entry_price) * 100   # positive when price goes down
            mae_days[idx] = int(np.nanargmax(hold_highs))
            mfe_days[idx] = int(np.nanargmin(hold_lows))

        # overnight gap: signal_date Close → entry_date Open
        sig_idx = np.searchsorted(dates, signal_dt)
        if sig_idx < len(dates) and dates[sig_idx] == signal_dt:
            prev_close = closes[sig_idx]
            entry_idx = np.searchsorted(dates, entry_dt)
            if entry_idx < len(dates) and dates[entry_idx] == entry_dt:
                entry_open = opens[entry_idx]
                if prev_close > 0:
                    overnight_gaps[idx] = (entry_open / prev_close - 1) * 100

    trades_out = trades_reset.copy()
    trades_out["mae_pct"] = np.round(mae_pcts, 3)
    trades_out["mfe_pct"] = np.round(mfe_pcts, 3)
    trades_out["mae_day"] = mae_days
    trades_out["mfe_day"] = mfe_days
    trades_out["overnight_gap_pct"] = np.round(overnight_gaps, 3)

    valid = trades_out["mae_pct"].notna()
    print(f"  computed: {valid.sum():,} / {len(trades_out):,} (skipped: {skip_count})")
    print(f"  Done in {time.time()-t0:.1f}s")

    return trades_out[valid].reset_index(drop=True)


# ---------------------------------------------------------------------------
# 2. レポート生成
# ---------------------------------------------------------------------------

def _stat_card(label: str, value: str, sub: str = "", tone: str = "") -> str:
    """tone: 'pos' = green border/value, 'neg' = red, 'warn' = amber, '' = neutral"""
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    cls = {"pos": "card-pos", "neg": "card-neg", "warn": "card-warn"}.get(tone, "")
    return f'<div class="stat-card {cls}"><div class="label">{label}</div><div class="value">{value}</div>{sub_html}</div>'


def _color(v: float, fmt: str = "+.1f") -> str:
    cls = "num-pos" if v > 0 else "num-neg" if v < 0 else ""
    return f'<span class="{cls}">{v:{fmt}}</span>'


def _pf(wins: float, losses: float) -> str:
    if losses == 0:
        return "∞"
    return f"{wins / losses:.2f}"


def generate_report(df: pd.DataFrame) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    n = len(df)
    directions = df["direction"].unique().tolist()
    is_long_only = directions == ["LONG"]
    long_df = df[df["direction"] == "LONG"]
    short_df = df[df["direction"] == "SHORT"]
    filter_label = "LONGのみ" if is_long_only else "全方向（LONG+SHORT）"

    # --- Executive Summary ---
    wr = df["win"].mean() * 100
    gross_w = df.loc[df["ret_pct"] > 0, "ret_pct"].sum()
    gross_l = abs(df.loc[df["ret_pct"] <= 0, "ret_pct"].sum())
    pf = gross_w / gross_l if gross_l > 0 else 999
    total_pnl = df["pnl"].sum() / 10000
    avg_mae = df["mae_pct"].mean()
    avg_mfe = df["mfe_pct"].mean()
    median_mae = df["mae_pct"].median()
    median_mfe = df["mfe_pct"].median()
    avg_ret = df["ret_pct"].mean()
    mfe_capture = (avg_ret / avg_mfe * 100) if avg_mfe != 0 else 0
    avg_hold = df["hold_days"].mean()

    # LONG stats
    l_wr = long_df["win"].mean() * 100
    l_gw = long_df.loc[long_df["ret_pct"] > 0, "ret_pct"].sum()
    l_gl = abs(long_df.loc[long_df["ret_pct"] <= 0, "ret_pct"].sum())
    l_pf = l_gw / l_gl if l_gl > 0 else 999
    l_pnl = long_df["pnl"].sum() / 10000

    s_wr = short_df["win"].mean() * 100
    s_gw = short_df.loc[short_df["ret_pct"] > 0, "ret_pct"].sum()
    s_gl = abs(short_df.loc[short_df["ret_pct"] <= 0, "ret_pct"].sum())
    s_pf = s_gw / s_gl if s_gl > 0 else 999
    s_pnl = short_df["pnl"].sum() / 10000

    # --- MAE band analysis ---
    mae_bands = [
        ("0% ~", df["mae_pct"] >= 0),
        ("-1% ~ 0%", (df["mae_pct"] >= -1) & (df["mae_pct"] < 0)),
        ("-2% ~ -1%", (df["mae_pct"] >= -2) & (df["mae_pct"] < -1)),
        ("-3% ~ -2%", (df["mae_pct"] >= -3) & (df["mae_pct"] < -2)),
        ("-5% ~ -3%", (df["mae_pct"] >= -5) & (df["mae_pct"] < -3)),
        ("-10% ~ -5%", (df["mae_pct"] >= -10) & (df["mae_pct"] < -5)),
        ("< -10%", df["mae_pct"] < -10),
    ]
    mae_rows = ""
    for label, mask in mae_bands:
        sub = df[mask]
        if len(sub) == 0:
            mae_rows += f"<tr><td>{label}</td><td class='r'>0</td><td class='r'>-</td><td class='r'>-</td><td class='r'>-</td></tr>"
            continue
        cnt = len(sub)
        pct = cnt / n * 100
        wr_b = sub["win"].mean() * 100
        avg_r = sub["ret_pct"].mean()
        mae_rows += f"<tr><td>{label}</td><td class='r'>{cnt:,} ({pct:.1f}%)</td><td class='r'>{wr_b:.1f}%</td><td class='r'>{_color(avg_r)}</td><td class='r'>{_color(sub['mfe_pct'].mean())}</td></tr>"

    # --- MFE band analysis ---
    mfe_bands = [
        ("< 0%", df["mfe_pct"] < 0),
        ("0% ~ 1%", (df["mfe_pct"] >= 0) & (df["mfe_pct"] < 1)),
        ("1% ~ 2%", (df["mfe_pct"] >= 1) & (df["mfe_pct"] < 2)),
        ("2% ~ 3%", (df["mfe_pct"] >= 2) & (df["mfe_pct"] < 3)),
        ("3% ~ 5%", (df["mfe_pct"] >= 3) & (df["mfe_pct"] < 5)),
        ("5% ~ 10%", (df["mfe_pct"] >= 5) & (df["mfe_pct"] < 10)),
        ("10% ~ 20%", (df["mfe_pct"] >= 10) & (df["mfe_pct"] < 20)),
        (">= 20%", df["mfe_pct"] >= 20),
    ]
    mfe_rows = ""
    for label, mask in mfe_bands:
        sub = df[mask]
        if len(sub) == 0:
            mfe_rows += f"<tr><td>{label}</td><td class='r'>0</td><td class='r'>-</td><td class='r'>-</td><td class='r'>-</td></tr>"
            continue
        cnt = len(sub)
        pct = cnt / n * 100
        avg_r = sub["ret_pct"].mean()
        capture = (avg_r / sub["mfe_pct"].mean() * 100) if sub["mfe_pct"].mean() != 0 else 0
        mfe_rows += f"<tr><td>{label}</td><td class='r'>{cnt:,} ({pct:.1f}%)</td><td class='r'>{_color(avg_r)}</td><td class='r'>{_color(sub['mae_pct'].mean())}</td><td class='r'>{capture:.1f}%</td></tr>"

    # --- Time analysis ---
    hold_buckets = [
        ("1日", (df["hold_days"] >= 1) & (df["hold_days"] <= 1)),
        ("2-3日", (df["hold_days"] >= 2) & (df["hold_days"] <= 3)),
        ("4-6日", (df["hold_days"] >= 4) & (df["hold_days"] <= 6)),
        ("7-13日", (df["hold_days"] >= 7) & (df["hold_days"] <= 13)),
        ("14-29日", (df["hold_days"] >= 14) & (df["hold_days"] <= 29)),
        ("30-60日", (df["hold_days"] >= 30) & (df["hold_days"] <= 60)),
        ("60日超", df["hold_days"] > 60),
    ]
    time_rows = ""
    for label, mask in hold_buckets:
        sub = df[mask]
        if len(sub) == 0:
            time_rows += f"<tr><td>{label}</td><td class='r'>0</td><td class='r'>-</td><td class='r'>-</td><td class='r'>-</td><td class='r'>-</td></tr>"
            continue
        cnt = len(sub)
        pct = cnt / n * 100
        wr_b = sub["win"].mean() * 100
        avg_r = sub["ret_pct"].mean()
        avg_mfe_b = sub["mfe_pct"].mean()
        avg_mae_b = sub["mae_pct"].mean()
        time_rows += f"<tr><td>{label}</td><td class='r'>{cnt:,} ({pct:.1f}%)</td><td class='r'>{wr_b:.1f}%</td><td class='r'>{_color(avg_r)}</td><td class='r'>{_color(avg_mfe_b)}</td><td class='r'>{_color(avg_mae_b)}</td></tr>"

    # --- MFE peak day distribution ---
    mfe_day_buckets = [
        ("0日目（当日）", df["mfe_day"] == 0),
        ("1日目", df["mfe_day"] == 1),
        ("2-3日目", (df["mfe_day"] >= 2) & (df["mfe_day"] <= 3)),
        ("4-6日目", (df["mfe_day"] >= 4) & (df["mfe_day"] <= 6)),
        ("7-13日目", (df["mfe_day"] >= 7) & (df["mfe_day"] <= 13)),
        ("14日目以降", df["mfe_day"] >= 14),
    ]
    mfe_day_rows = ""
    for label, mask in mfe_day_buckets:
        sub = df[mask]
        if len(sub) == 0:
            mfe_day_rows += f"<tr><td>{label}</td><td class='r'>0</td><td class='r'>-</td><td class='r'>-</td></tr>"
            continue
        cnt = len(sub)
        pct = cnt / n * 100
        avg_mfe_b = sub["mfe_pct"].mean()
        avg_r = sub["ret_pct"].mean()
        mfe_day_rows += f"<tr><td>{label}</td><td class='r'>{cnt:,} ({pct:.1f}%)</td><td class='r'>{_color(avg_mfe_b)}</td><td class='r'>{_color(avg_r)}</td></tr>"

    # --- Rule comparison ---
    rules_order = ["B1", "B2", "B3", "B4", "S1", "S2", "S3", "S4"]
    rule_rows = ""
    for rule in rules_order:
        sub = df[df["rule"] == rule]
        if len(sub) == 0:
            continue
        cnt = len(sub)
        direction = sub["direction"].iloc[0]
        wr_b = sub["win"].mean() * 100
        gw = sub.loc[sub["ret_pct"] > 0, "ret_pct"].sum()
        gl = abs(sub.loc[sub["ret_pct"] <= 0, "ret_pct"].sum())
        pf_b = _pf(gw, gl)
        avg_r = sub["ret_pct"].mean()
        avg_mae_b = sub["mae_pct"].mean()
        avg_mfe_b = sub["mfe_pct"].mean()
        med_mae_b = sub["mae_pct"].median()
        med_mfe_b = sub["mfe_pct"].median()
        avg_hold_b = sub["hold_days"].mean()
        total_pnl_b = sub["pnl"].sum() / 10000
        rule_rows += (
            f"<tr><td><strong>{rule}</strong></td><td>{direction}</td>"
            f"<td class='r'>{cnt:,}</td>"
            f"<td class='r'>{wr_b:.1f}%</td>"
            f"<td class='r'>{pf_b}</td>"
            f"<td class='r'>{_color(avg_r)}</td>"
            f"<td class='r'>{_color(avg_mae_b)}</td>"
            f"<td class='r'>{_color(avg_mfe_b)}</td>"
            f"<td class='r'>{med_mae_b:.2f}%</td>"
            f"<td class='r'>{med_mfe_b:.2f}%</td>"
            f"<td class='r'>{avg_hold_b:.1f}d</td>"
            f"<td class='r'>{_color(total_pnl_b, '+,.0f')}万</td>"
            f"</tr>"
        )

    # --- Overnight gap analysis ---
    gap_df = df[df["overnight_gap_pct"].notna()]
    gap_bands = [
        ("< -2%", gap_df["overnight_gap_pct"] < -2),
        ("-2% ~ -1%", (gap_df["overnight_gap_pct"] >= -2) & (gap_df["overnight_gap_pct"] < -1)),
        ("-1% ~ 0%", (gap_df["overnight_gap_pct"] >= -1) & (gap_df["overnight_gap_pct"] < 0)),
        ("0% ~ 1%", (gap_df["overnight_gap_pct"] >= 0) & (gap_df["overnight_gap_pct"] < 1)),
        ("1% ~ 2%", (gap_df["overnight_gap_pct"] >= 1) & (gap_df["overnight_gap_pct"] < 2)),
        (">= 2%", gap_df["overnight_gap_pct"] >= 2),
    ]
    gap_rows = ""
    for label, mask in gap_bands:
        sub = gap_df[mask]
        if len(sub) == 0:
            gap_rows += f"<tr><td>{label}</td><td class='r'>0</td><td class='r'>-</td><td class='r'>-</td></tr>"
            continue
        cnt = len(sub)
        pct = cnt / len(gap_df) * 100
        wr_b = sub["win"].mean() * 100
        avg_r = sub["ret_pct"].mean()
        gap_rows += f"<tr><td>{label}</td><td class='r'>{cnt:,} ({pct:.1f}%)</td><td class='r'>{wr_b:.1f}%</td><td class='r'>{_color(avg_r)}</td></tr>"

    # --- Plotly chart data ---
    # MAE histogram
    mae_hist_data = df["mae_pct"].clip(-30, 5).tolist()
    # MFE histogram
    mfe_hist_data = df["mfe_pct"].clip(-5, 50).tolist()
    # Scatter: MAE vs MFE (sample 5000 for performance)
    sample = df.sample(min(5000, len(df)), random_state=42)
    scatter_mae = sample["mae_pct"].clip(-30, 5).tolist()
    scatter_mfe = sample["mfe_pct"].clip(-5, 50).tolist()
    scatter_rule = sample["rule"].tolist()
    scatter_ret = sample["ret_pct"].tolist()
    # MFE day histogram
    mfe_day_hist = df["mfe_day"].clip(0, 60).tolist()
    # Hold days histogram
    hold_day_hist = df["hold_days"].clip(0, 68).tolist()

    # LONG-only MAE for SL insight
    long_mae = long_df["mae_pct"]
    sl_insight_lines = []
    for sl in [2, 3, 5, 8]:
        hit = (long_mae < -sl).sum()
        hit_pct = hit / len(long_mae) * 100
        # Of those hit, how many had MFE > 0 (would have been profitable)?
        hit_trades = long_df[long_mae < -sl]
        recovery = (hit_trades["mfe_pct"] > 0).mean() * 100 if len(hit_trades) > 0 else 0
        sl_insight_lines.append(
            f"SL -{sl}%: LONG {hit:,}件 ({hit_pct:.1f}%) が到達 — うち{recovery:.0f}%はMFE>0（一時含み益あり）"
        )
    sl_insight = "<br>".join(sl_insight_lines)

    # MFE capture insight
    # Of trades with MFE >= 3%, what % ended with ret < 1%?
    big_mfe = df[df["mfe_pct"] >= 3]
    if len(big_mfe) > 0:
        wasted = (big_mfe["ret_pct"] < 1).mean() * 100
        mfe_insight = (
            f"MFE≥3%のトレード {len(big_mfe):,}件のうち、<strong>{wasted:.1f}%</strong>が最終リターン1%未満で終了。"
            f"<br>利益の取りこぼしが大きい。利確ルールの余地あり。"
        )
    else:
        mfe_insight = "MFE≥3%のトレードなし"

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Chapter 2: MAE/MFE生分布分析（SLなし・全8原則）</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
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
  .section h3 {{ font-size:0.95rem; color:var(--text-muted); margin:16px 0 8px 0; }}
  table {{ width:100%; border-collapse:collapse; font-size:0.85rem; margin:12px 0; }}
  th {{ text-align:left; padding:8px 12px; background:rgba(255,255,255,0.03); color:var(--text-muted); font-weight:600; border-bottom:1px solid var(--card-border); white-space:nowrap; }}
  th.r {{ text-align:right; }}
  td {{ padding:8px 12px; border-bottom:1px solid rgba(255,255,255,0.05); }}
  td.r {{ text-align:right; font-variant-numeric:tabular-nums; }}
  tr:hover td {{ background:rgba(255,255,255,0.02); }}
  .num-pos {{ color:var(--emerald); font-weight:600; }}
  .num-neg {{ color:var(--rose); font-weight:600; }}
  .grid-4 {{ display:grid; grid-template-columns:repeat(4, 1fr); gap:16px; margin-bottom:16px; }}
  .grid-3 {{ display:grid; grid-template-columns:repeat(3, 1fr); gap:16px; margin-bottom:16px; }}
  .grid-2 {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; }}
  @media (max-width:768px) {{ .grid-4,.grid-3,.grid-2 {{ grid-template-columns:1fr; }} }}
  .stat-card {{ background:rgba(255,255,255,0.02); border:1px solid var(--card-border); border-radius:8px; padding:16px; text-align:center; }}
  .stat-card .label {{ color:var(--text-muted); font-size:0.75rem; margin-bottom:4px; }}
  .stat-card .value {{ font-size:1.5rem; font-weight:700; }}
  .stat-card .sub {{ color:var(--text-muted); font-size:0.75rem; margin-top:2px; }}
  .stat-card.card-pos {{ border-color: rgba(52,211,153,0.4); }}
  .stat-card.card-pos .value {{ color: var(--emerald); }}
  .stat-card.card-neg {{ border-color: rgba(251,113,133,0.4); }}
  .stat-card.card-neg .value {{ color: var(--rose); }}
  .stat-card.card-warn {{ border-color: rgba(251,191,36,0.4); }}
  .stat-card.card-warn .value {{ color: var(--amber); }}
  .alert-box {{ border-radius:8px; padding:16px; margin:16px 0; font-size:0.875rem; line-height:1.7; }}
  .alert-info {{ background:rgba(96,165,250,0.1); border:1px solid rgba(96,165,250,0.3); color:var(--blue); }}
  .alert-warning {{ background:rgba(251,191,36,0.08); border:1px solid rgba(251,191,36,0.3); color:var(--amber); }}
  .alert-success {{ background:rgba(52,211,153,0.1); border:1px solid rgba(52,211,153,0.3); color:var(--emerald); }}
  .chart {{ width:100%; min-height:350px; }}
  footer {{ text-align:center; color:var(--text-muted); font-size:0.7rem; margin-top:40px; padding:16px 0; border-top:1px solid var(--card-border); }}
</style>
</head>
<body>

<h1>Chapter 2{".1 LONG特化" if is_long_only else ""}: MAE/MFE 生分布分析</h1>
<div class="subtitle">SLなし・{filter_label}・クリーニング済み | {n:,} trades ({df["rule"].nunique()}原則: {", ".join(sorted(df["rule"].unique()))}) | Generated: {ts}</div>

<!-- Section 1: Executive Summary -->
<div class="section">
  <h2>1. Executive Summary</h2>
  <div class="grid-4">
    {_stat_card("総トレード数", f"{n:,}", f"B1-B4 LONGのみ" if is_long_only else f"LONG {len(long_df):,} / SHORT {len(short_df):,}")}
    {_stat_card("勝率", f"{wr:.1f}%", f"" if is_long_only else f"L:{l_wr:.1f}% / S:{s_wr:.1f}%", "pos" if wr >= 50 else "warn" if wr >= 40 else "neg")}
    {_stat_card("PF", f"{pf:.2f}", f"" if is_long_only else f"L:{l_pf:.2f} / S:{s_pf:.2f}", "pos" if pf >= 1.5 else "warn" if pf >= 1.0 else "neg")}
    {_stat_card("総PnL", f"{total_pnl:+,.0f}万", f"" if is_long_only else f"L:{l_pnl:+,.0f}万 / S:{s_pnl:+,.0f}万", "pos" if total_pnl > 0 else "neg")}
  </div>
  <div class="grid-4">
    {_stat_card("平均MAE", f"{avg_mae:.2f}%", f"中央値: {median_mae:.2f}%", "neg")}
    {_stat_card("平均MFE", f"{avg_mfe:+.2f}%", f"中央値: {median_mfe:+.2f}%", "pos" if avg_mfe > 0 else "neg")}
    {_stat_card("MFE捕捉率", f"{mfe_capture:.1f}%", f"平均リターン{avg_ret:+.2f}% / 平均MFE{avg_mfe:+.2f}%", "pos" if mfe_capture >= 30 else "warn" if mfe_capture >= 15 else "neg")}
    {_stat_card("平均保有日数", f"{avg_hold:.1f}日", f"最大 {int(df['hold_days'].max())}日")}
  </div>
</div>

<!-- Section 2: MAE Distribution -->
<div class="section">
  <h2>2. MAE分布分析（SL幅の手がかり）</h2>
  <div id="mae-hist" class="chart"></div>
  <h3>MAE帯別統計</h3>
  <table>
    <thead><tr><th>MAE帯</th><th class="r">件数 (構成比)</th><th class="r">勝率</th><th class="r">平均リターン</th><th class="r">平均MFE</th></tr></thead>
    <tbody>{mae_rows}</tbody>
  </table>
  <div class="alert-box alert-warning">
    <strong>SL到達シミュレーション{"" if is_long_only else "（LONGのみ）"}:</strong><br>
    {sl_insight}
  </div>
</div>

<!-- Section 3: MFE Distribution -->
<div class="section">
  <h2>3. MFE分布分析（利確タイミングの手がかり）</h2>
  <div id="mfe-hist" class="chart"></div>
  <h3>MFE帯別統計</h3>
  <table>
    <thead><tr><th>MFE帯</th><th class="r">件数 (構成比)</th><th class="r">平均リターン</th><th class="r">平均MAE</th><th class="r">MFE捕捉率</th></tr></thead>
    <tbody>{mfe_rows}</tbody>
  </table>
  <div class="alert-box alert-info">
    {mfe_insight}
  </div>
</div>

<!-- Section 4: MAE vs MFE Scatter -->
<div class="section">
  <h2>4. MAE vs MFE 散布図</h2>
  <div id="scatter" class="chart" style="min-height:500px;"></div>
</div>

<!-- Section 5: Time Analysis -->
<div class="section">
  <h2>5. 時間軸分析</h2>
  <div class="grid-2">
    <div>
      <h3>保有期間別統計</h3>
      <table>
        <thead><tr><th>保有期間</th><th class="r">件数</th><th class="r">勝率</th><th class="r">平均リターン</th><th class="r">平均MFE</th><th class="r">平均MAE</th></tr></thead>
        <tbody>{time_rows}</tbody>
      </table>
    </div>
    <div>
      <h3>MFEピーク日（利益最大の日）</h3>
      <table>
        <thead><tr><th>MFEピーク日</th><th class="r">件数</th><th class="r">平均MFE</th><th class="r">平均リターン</th></tr></thead>
        <tbody>{mfe_day_rows}</tbody>
      </table>
    </div>
  </div>
  <div id="hold-hist" class="chart"></div>
  <div id="mfe-day-hist" class="chart"></div>
</div>

<!-- Section 6: Rule Comparison -->
<div class="section">
  <h2>6. グランビル8原則別比較</h2>
  <table>
    <thead><tr>
      <th>原則</th><th>方向</th><th class="r">件数</th><th class="r">勝率</th><th class="r">PF</th>
      <th class="r">平均リターン</th><th class="r">平均MAE</th><th class="r">平均MFE</th>
      <th class="r">中央MAE</th><th class="r">中央MFE</th><th class="r">平均保有</th><th class="r">総PnL</th>
    </tr></thead>
    <tbody>{rule_rows}</tbody>
  </table>
</div>

<!-- Section 7: Overnight Gap -->
<div class="section">
  <h2>7. オーバーナイトギャップ分析</h2>
  <h3>ギャップ帯別統計</h3>
  <table>
    <thead><tr><th>ギャップ帯</th><th class="r">件数</th><th class="r">勝率</th><th class="r">平均リターン</th></tr></thead>
    <tbody>{gap_rows}</tbody>
  </table>
</div>

<!-- Plotly Charts -->
<script>
const dark = {{
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: {{ color: '#a1a1aa', family: '-apple-system,BlinkMacSystemFont,Segoe UI,Noto Sans JP,sans-serif' }},
  xaxis: {{ gridcolor: '#27272a', zerolinecolor: '#3f3f46' }},
  yaxis: {{ gridcolor: '#27272a', zerolinecolor: '#3f3f46' }},
  margin: {{ t: 40, r: 20, b: 50, l: 60 }}
}};

// MAE histogram
Plotly.newPlot('mae-hist', [{{
  x: {json.dumps(mae_hist_data)},
  type: 'histogram',
  nbinsx: 80,
  marker: {{ color: 'rgba(251,113,133,0.7)', line: {{ color: 'rgba(251,113,133,1)', width: 0.5 }} }},
  name: 'MAE'
}}], {{
  ...dark,
  title: {{ text: 'MAE分布（全トレード）', font: {{ size: 14, color: '#fafafa' }} }},
  xaxis: {{ ...dark.xaxis, title: 'MAE (%)' }},
  yaxis: {{ ...dark.yaxis, title: '件数' }},
  shapes: [
    {{ type: 'line', x0: -3, x1: -3, y0: 0, y1: 1, yref: 'paper', line: {{ color: '#fbbf24', width: 2, dash: 'dash' }} }},
    {{ type: 'line', x0: -5, x1: -5, y0: 0, y1: 1, yref: 'paper', line: {{ color: '#fb7185', width: 2, dash: 'dash' }} }}
  ],
  annotations: [
    {{ x: -3, y: 1, yref: 'paper', text: 'SL -3%', showarrow: false, font: {{ color: '#fbbf24', size: 11 }}, yanchor: 'bottom' }},
    {{ x: -5, y: 1, yref: 'paper', text: 'SL -5%', showarrow: false, font: {{ color: '#fb7185', size: 11 }}, yanchor: 'bottom' }}
  ]
}}, {{ responsive: true }});

// MFE histogram
Plotly.newPlot('mfe-hist', [{{
  x: {json.dumps(mfe_hist_data)},
  type: 'histogram',
  nbinsx: 80,
  marker: {{ color: 'rgba(52,211,153,0.7)', line: {{ color: 'rgba(52,211,153,1)', width: 0.5 }} }},
  name: 'MFE'
}}], {{
  ...dark,
  title: {{ text: 'MFE分布（全トレード）', font: {{ size: 14, color: '#fafafa' }} }},
  xaxis: {{ ...dark.xaxis, title: 'MFE (%)' }},
  yaxis: {{ ...dark.yaxis, title: '件数' }}
}}, {{ responsive: true }});

// Scatter
const rules = {json.dumps(scatter_rule)};
const uniqueRules = [...new Set(rules)].sort();
const colors = {{ B1:'#34d399', B2:'#6ee7b7', B3:'#a7f3d0', B4:'#d1fae5', S1:'#fb7185', S2:'#fda4af', S3:'#fecdd3', S4:'#ffe4e6' }};
const scatterTraces = uniqueRules.map(r => {{
  const idx = rules.map((v,i) => v===r ? i : -1).filter(i => i>=0);
  return {{
    x: idx.map(i => {json.dumps(scatter_mae)}[i]),
    y: idx.map(i => {json.dumps(scatter_mfe)}[i]),
    mode: 'markers',
    type: 'scatter',
    name: r,
    marker: {{ color: colors[r] || '#60a5fa', size: 4, opacity: 0.6 }},
    text: idx.map(i => `${{r}} ret:${{{json.dumps(scatter_ret)}[i].toFixed(1)}}%`),
    hoverinfo: 'text'
  }};
}});
Plotly.newPlot('scatter', scatterTraces, {{
  ...dark,
  title: {{ text: 'MAE vs MFE（サンプル5,000件）', font: {{ size: 14, color: '#fafafa' }} }},
  xaxis: {{ ...dark.xaxis, title: 'MAE (%)', range: [-30, 5] }},
  yaxis: {{ ...dark.yaxis, title: 'MFE (%)', range: [-5, 50] }},
  legend: {{ font: {{ size: 10 }}, bgcolor: 'rgba(0,0,0,0)' }},
  shapes: [
    {{ type: 'line', x0: -30, x1: 5, y0: 0, y1: 0, line: {{ color: '#3f3f46', width: 1 }} }},
    {{ type: 'line', x0: 0, x1: 0, y0: -5, y1: 50, line: {{ color: '#3f3f46', width: 1 }} }}
  ]
}}, {{ responsive: true }});

// Hold days histogram
Plotly.newPlot('hold-hist', [{{
  x: {json.dumps(hold_day_hist)},
  type: 'histogram',
  nbinsx: 40,
  marker: {{ color: 'rgba(96,165,250,0.7)', line: {{ color: 'rgba(96,165,250,1)', width: 0.5 }} }}
}}], {{
  ...dark,
  title: {{ text: '保有日数分布', font: {{ size: 14, color: '#fafafa' }} }},
  xaxis: {{ ...dark.xaxis, title: '保有日数' }},
  yaxis: {{ ...dark.yaxis, title: '件数' }}
}}, {{ responsive: true }});

// MFE day histogram
Plotly.newPlot('mfe-day-hist', [{{
  x: {json.dumps(mfe_day_hist)},
  type: 'histogram',
  nbinsx: 40,
  marker: {{ color: 'rgba(251,191,36,0.7)', line: {{ color: 'rgba(251,191,36,1)', width: 0.5 }} }}
}}], {{
  ...dark,
  title: {{ text: 'MFEピーク日分布（利益最大は何日目か）', font: {{ size: 14, color: '#fafafa' }} }},
  xaxis: {{ ...dark.xaxis, title: 'MFEピーク日（エントリーからの日数）' }},
  yaxis: {{ ...dark.yaxis, title: '件数' }}
}}, {{ responsive: true }});
</script>

<footer>Generated by 03_compute_mae_mfe.py | strategy_verification/chapters/02_mae_mfe_raw | Data: trades_cleaned.parquet (SLなし)</footer>
</body>
</html>"""


# ---------------------------------------------------------------------------
# 3. Main
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--long", action="store_true", help="LONGのみに絞る")
    args = parser.parse_args()

    t0 = time.time()

    # MAE/MFE計算済みがあればそれを使う（再計算スキップ）
    out_path = PROCESSED / "trades_with_mae_mfe.parquet"
    if out_path.exists():
        print("[1/3] Loading pre-computed MAE/MFE data...")
        trades_enriched = pd.read_parquet(out_path)
        print(f"  loaded: {len(trades_enriched):,} rows")
    else:
        print("[1/3] Loading cleaned data...")
        trades = pd.read_parquet(PROCESSED / "trades_cleaned.parquet")
        prices = pd.read_parquet(PROCESSED / "prices_cleaned.parquet")
        print(f"  trades: {len(trades):,}, prices: {len(prices):,}")
        trades_enriched = compute_mae_mfe(trades, prices)
        PROCESSED.mkdir(parents=True, exist_ok=True)
        trades_enriched.to_parquet(out_path, index=False)
        print(f"  saved: {out_path} ({len(trades_enriched):,} rows)")

    # フィルター適用
    if args.long:
        trades_enriched = trades_enriched[trades_enriched["direction"] == "LONG"].reset_index(drop=True)
        suffix = "_long"
        print(f"  [LONG filter] {len(trades_enriched):,} trades")
    else:
        suffix = ""

    # レポート生成
    print("[3/3] Generating report...")
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    html = generate_report(trades_enriched)
    report_path = REPORT_DIR / f"report{suffix}.html"
    report_path.write_text(html, encoding="utf-8")
    print(f"  report: {report_path}")

    print(f"\n=== Done in {time.time()-t0:.1f}s ===")


if __name__ == "__main__":
    main()
