#!/usr/bin/env python3
"""
10_stock_selection.py
======================
Chapter 5-1: Stock Selection under New Exit Rules

Ch5で確定したexit rule（B1/B3: fixed 60d, B2: min_hold 30d, B4: fixed 13d）の下で、
各ファクターによるセグメント別PnLを検証し、B1/B3の銘柄選別基準を探る。

入力:
  - strategy_verification/data/processed/trades_with_features.parquet
  - strategy_verification/data/processed/prices_cleaned.parquet
  - data/parquet/index_prices_max_1d.parquet (^N225, 1306.T)
  - strategy_verification/data/processed/nikkei_vi.parquet

出力:
  - strategy_verification/chapters/05-1_stock_selection/report.html

ファクター:
  既存: atr14_pct, sma20_dev, vol_ratio, regime, consec_down, sector
  新規: price_band, sma20_slope, momentum_20d, n225_trend, topix_trend, vi_level
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SV_DIR = ROOT / "strategy_verification"
PROCESSED = SV_DIR / "data" / "processed"
REPORT_DIR = SV_DIR / "chapters" / "05-1_stock_selection"

RULES = ["B1", "B2", "B3", "B4"]
MAX_HOLD = 60

# Ch5 optimal exit configs: (mode, param, sl_pct)
EXIT_CONFIGS: dict[str, tuple[str, float, float]] = {
    "B1": ("fixed_N", 60, 3.0),
    "B2": ("min_hold_N", 30, 3.0),
    "B3": ("fixed_N", 60, 2.5),
    "B4": ("fixed_N", 13, 999.0),
}


# ---- Price lookup & simulation (from 09_exit_redesign.py) ----


def build_price_lookup(prices: pd.DataFrame) -> dict:
    lookup = {}
    for ticker, grp in prices.groupby("ticker"):
        grp = grp.sort_values("date").dropna(subset=["Close"])
        if len(grp) == 0:
            continue
        lookup[ticker] = {
            "dates": grp["date"].values,
            "opens": grp["Open"].values.astype(np.float64),
            "highs": grp["High"].values.astype(np.float64),
            "lows": grp["Low"].values.astype(np.float64),
            "closes": grp["Close"].values.astype(np.float64),
        }
    return lookup


def simulate_trade(
    pl: dict, entry_date: np.datetime64, entry_price: float,
    mode: str, param: float, sl_pct: float = 999.0,
) -> dict | None:
    dates, opens, highs, lows, closes = pl["dates"], pl["opens"], pl["highs"], pl["lows"], pl["closes"]
    entry_mask = dates == entry_date
    if not entry_mask.any():
        return None
    entry_idx = np.where(entry_mask)[0][0]
    sl_price = entry_price * (1 - sl_pct / 100) if sl_pct < 900 else 0.0
    max_high = entry_price
    exit_price = None
    exit_day = 0

    for d in range(MAX_HOLD):
        ci = entry_idx + d
        if ci >= len(dates):
            break
        c_low, c_high, c_close, c_open = lows[ci], highs[ci], closes[ci], opens[ci]

        if d == 0:
            if sl_pct < 900 and c_low <= sl_price:
                exit_price, exit_day = sl_price, d
                break
            max_high = max(max_high, c_high)
            continue

        if sl_pct < 900 and c_low <= sl_price:
            exit_price, exit_day = sl_price, d
            break
        max_high = max(max_high, c_high)

        if mode == "fixed_N":
            n = int(param)
            if d == n:
                exit_price = opens[ci + 1] if ci + 1 < len(dates) else c_close
                exit_day = d + 1 if ci + 1 < len(dates) else d
                break
        elif mode == "min_hold_N":
            n = int(param)
            if d >= n and c_close < entry_price:
                exit_price = opens[ci + 1] if ci + 1 < len(dates) else c_close
                exit_day = d + 1 if ci + 1 < len(dates) else d
                break

    if exit_price is None:
        ci = min(entry_idx + MAX_HOLD - 1, len(dates) - 1)
        exit_price = opens[ci + 1] if ci + 1 < len(dates) else closes[ci]
        exit_day = MAX_HOLD if ci + 1 < len(dates) else MAX_HOLD - 1

    ret_pct = (exit_price / entry_price - 1) * 100
    pnl = entry_price * 100 * ret_pct / 100
    return {"sim_ret": round(ret_pct, 3), "sim_pnl": round(pnl, 2), "sim_hold": exit_day}


def simulate_per_trade(trades: pd.DataFrame, price_lookup: dict) -> pd.DataFrame:
    """全トレードを各ルールのCh5 exit configで再シミュレーション。per-trade結果を返す。"""
    results = []
    for idx, row in trades.iterrows():
        ticker = row["ticker"]
        rule = row["rule"]
        if ticker not in price_lookup or rule not in EXIT_CONFIGS:
            results.append({"idx": idx, "sim_ret": np.nan, "sim_pnl": np.nan, "sim_hold": np.nan})
            continue
        mode, param, sl_pct = EXIT_CONFIGS[rule]
        ed = row["entry_date"]
        ed_np = ed.to_numpy().astype("datetime64[ns]") if hasattr(ed, "to_numpy") else np.datetime64(ed)
        r = simulate_trade(price_lookup[ticker], ed_np, float(row["entry_price"]), mode, param, sl_pct)
        if r is None:
            results.append({"idx": idx, "sim_ret": np.nan, "sim_pnl": np.nan, "sim_hold": np.nan})
        else:
            results.append({"idx": idx, **r})
    return pd.DataFrame(results).set_index("idx")


# ---- Factor enrichment ----


def enrich_price_factors(trades: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """SMA20傾き、20日モメンタムを計算してtradesに結合"""
    factor_parts = []
    for ticker, grp in prices.groupby("ticker"):
        grp = grp.sort_values("date").copy()
        sma20 = grp["Close"].rolling(20, min_periods=20).mean()
        grp["sma20_slope"] = (sma20 / sma20.shift(5) - 1) * 100
        grp["momentum_20d"] = (grp["Close"] / grp["Close"].shift(20) - 1) * 100
        factor_parts.append(grp[["ticker", "date", "sma20_slope", "momentum_20d"]])
    factor_df = pd.concat(factor_parts, ignore_index=True)
    trades = trades.merge(factor_df, left_on=["ticker", "entry_date"], right_on=["ticker", "date"], how="left")
    trades.drop(columns=["date"], inplace=True, errors="ignore")
    return trades


def enrich_market_context(trades: pd.DataFrame, index_prices: pd.DataFrame, vi: pd.DataFrame) -> pd.DataFrame:
    """N225トレンド、TOPIXトレンド、VI水準をtradesに結合"""
    # N225
    n225 = index_prices[index_prices["ticker"] == "^N225"].sort_values("date").copy()
    n225["n225_sma25"] = n225["Close"].rolling(25, min_periods=25).mean()
    n225["n225_trend"] = np.where(n225["Close"] > n225["n225_sma25"], "Bull", "Bear")
    n225_ctx = n225[["date", "n225_trend"]].copy()

    # TOPIX (1306.T)
    topix = index_prices[index_prices["ticker"] == "1306.T"].sort_values("date").copy()
    topix["topix_sma25"] = topix["Close"].rolling(25, min_periods=25).mean()
    topix["topix_trend"] = np.where(topix["Close"] > topix["topix_sma25"], "Bull", "Bear")
    topix_ctx = topix[["date", "topix_trend"]].copy()

    # VI
    vi = vi.copy()
    vi_ctx = vi[["date", "close"]].rename(columns={"close": "vi_close"})

    # merge_asof で最近の営業日にマッチ
    trades = trades.sort_values("entry_date")
    n225_ctx = n225_ctx.sort_values("date")
    topix_ctx = topix_ctx.sort_values("date")
    vi_ctx = vi_ctx.sort_values("date")

    trades = pd.merge_asof(trades, n225_ctx, left_on="entry_date", right_on="date", direction="backward")
    trades.drop(columns=["date"], inplace=True, errors="ignore")
    trades = pd.merge_asof(trades, topix_ctx, left_on="entry_date", right_on="date", direction="backward")
    trades.drop(columns=["date"], inplace=True, errors="ignore")
    trades = pd.merge_asof(trades, vi_ctx, left_on="entry_date", right_on="date", direction="backward")
    trades.drop(columns=["date"], inplace=True, errors="ignore")

    # VI level bins
    trades["vi_level"] = pd.cut(
        trades["vi_close"], bins=[0, 20, 25, 30, 999],
        labels=["Low(<20)", "Normal(20-25)", "High(25-30)", "VHigh(30+)"],
    )

    return trades


def add_price_band(trades: pd.DataFrame) -> pd.DataFrame:
    trades["price_band"] = pd.cut(
        trades["entry_price"],
        bins=[0, 500, 1000, 2000, 5000, 999999],
        labels=["<500", "500-1K", "1K-2K", "2K-5K", "5K+"],
    )
    return trades


# ---- Segment statistics ----


def segment_stats(df: pd.DataFrame, col: str) -> list[dict]:
    """ファクター列でグループ化し、sim_ret/sim_pnlの統計量を返す"""
    rows = []
    for seg, grp in df.groupby(col, observed=True):
        valid = grp.dropna(subset=["sim_ret"])
        if len(valid) == 0:
            continue
        rets = valid["sim_ret"].values
        pnls = valid["sim_pnl"].values
        wins = rets > 0
        gw = rets[wins].sum()
        gl = abs(rets[~wins].sum())
        pf = gw / gl if gl > 0 else 999
        rows.append({
            "segment": str(seg),
            "n": len(valid),
            "wr": round(wins.mean() * 100, 1),
            "pf": round(pf, 2),
            "avg_ret": round(rets.mean(), 3),
            "pnl_m": round(pnls.sum() / 10000, 1),
        })
    return rows


# ---- Factor definitions ----

# (display_name, column, bin_edges, bin_labels) — bin-based factors
# For categorical factors: (display_name, column, None, None)

FACTOR_DEFS: list[tuple[str, str, list | None, list | None]] = [
    ("株価帯", "price_band", None, None),
    ("ATR(14)", "atr14_pct_bin", [0, 1.5, 2.5, 4.0, 6.0, 999], ["<1.5%", "1.5-2.5%", "2.5-4%", "4-6%", "6%+"]),
    ("SMA20乖離率", "sma20_dev_bin", [-999, -10, -5, 0, 5, 15, 999], ["<-10%", "-10~-5%", "-5~0%", "0~5%", "5~15%", "15%+"]),
    ("出来高比率", "vol_ratio_bin", [0, 0.5, 0.8, 1.2, 2.0, 999], ["<0.5x", "0.5-0.8x", "0.8-1.2x", "1.2-2x", "2x+"]),
    ("レジーム", "regime", None, None),
    ("連続陰線", "consec_bin", [0, 1, 3, 5, 99], ["0日", "1-2日", "3-4日", "5日+"]),
    ("セクター", "sector_group", None, None),
    ("SMA20傾き", "sma20_slope_bin", [-999, -2, 0, 2, 5, 999], ["急落(<-2%)", "下降(-2~0%)", "横ばい(0~2%)", "上昇(2~5%)", "急騰(5%+)"]),
    ("20日モメンタム", "momentum_bin", [-999, -10, -5, 0, 5, 10, 999], ["<-10%", "-10~-5%", "-5~0%", "0~5%", "5~10%", "10%+"]),
    ("N225トレンド", "n225_trend", None, None),
    ("TOPIXトレンド", "topix_trend", None, None),
    ("日経VI水準", "vi_level", None, None),
]


def prepare_factor_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ビン化カラムを作成"""
    for name, col, edges, labels in FACTOR_DEFS:
        if edges is None:
            continue  # categorical
        src_col = col.replace("_bin", "")
        if src_col not in df.columns:
            continue
        df[col] = pd.cut(df[src_col], bins=edges, labels=labels, right=False)

    # consec_down → consec_bin（右端含む）
    df["consec_bin"] = pd.cut(df["consec_down"], bins=[0, 1, 3, 5, 99], labels=["0日", "1-2日", "3-4日", "5日+"], right=False)

    # sector grouping: top 8 + その他
    top_sectors = df["sector"].value_counts().head(8).index.tolist()
    df["sector_group"] = df["sector"].where(df["sector"].isin(top_sectors), "その他")

    return df


# ---- HTML helpers ----


def _stat_card(label: str, value: str, sub: str = "", tone: str = "") -> str:
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    cls = {"pos": "card-pos", "neg": "card-neg", "warn": "card-warn"}.get(tone, "")
    return (
        f'<div class="stat-card {cls}">'
        f'<div class="label">{label}</div>'
        f'<div class="value">{value}</div>{sub_html}</div>'
    )


def _table_html(headers: list[str], rows: list[list], highlight_col: int | None = None) -> str:
    ths = "".join(f"<th>{h}</th>" for h in headers)
    trs = []
    best_idx = -1
    if highlight_col is not None:
        vals = []
        for r in rows:
            try:
                raw = str(r[highlight_col]).replace("万", "").replace(",", "").replace("+", "").replace("<b>", "").replace("</b>", "")
                v = float(raw)
            except (ValueError, IndexError):
                v = -9999
            vals.append(v)
        if vals:
            best_idx = vals.index(max(vals))
    for i, row in enumerate(rows):
        cls = ' class="best-row"' if i == best_idx else ""
        tds = "".join(f"<td>{c}</td>" for c in row)
        trs.append(f"<tr{cls}>{tds}</tr>")
    return f'<table><thead><tr>{ths}</tr></thead><tbody>{"".join(trs)}</tbody></table>'


def _insight_box(text: str) -> str:
    return f'<div class="insight-box">{text}</div>'


def _section(title: str, content: str) -> str:
    return f'<section><h2>{title}</h2>{content}</section>'


def _plotly_bar(div_id: str, x: list, y: list, title: str = "",
                yaxis_title: str = "", height: int = 300) -> str:
    colors = ["rgba(52,211,153,0.7)" if v > 0 else "rgba(248,113,113,0.7)" for v in y]
    data = json.dumps([{"x": x, "y": y, "type": "bar", "marker": {"color": colors}}])
    layout = json.dumps({
        "template": "plotly_dark",
        "paper_bgcolor": "transparent", "plot_bgcolor": "transparent",
        "margin": {"t": 35, "b": 50, "l": 55, "r": 15},
        "font": {"size": 11, "color": "#e2e8f0"},
        "title": {"text": title, "font": {"size": 12}},
        "yaxis": {"title": yaxis_title},
    })
    return f"""<div id="{div_id}" style="height:{height}px"></div>
<script>Plotly.newPlot("{div_id}",{data},{layout},{{responsive:true}})</script>"""


# ---- Main ----


def main():
    t0 = time.time()

    # ==== Load data ====
    print("[1/6] Loading data...")
    trades = pd.read_parquet(PROCESSED / "trades_with_features.parquet")
    prices = pd.read_parquet(PROCESSED / "prices_cleaned.parquet")
    index_prices = pd.read_parquet(ROOT / "data" / "parquet" / "index_prices_max_1d.parquet")
    vi = pd.read_parquet(PROCESSED / "nikkei_vi.parquet")

    long = trades[trades["direction"] == "LONG"].copy()
    print(f"  LONG trades: {len(long):,}")

    # ==== Build price lookup & simulate ====
    print("[2/6] Building price lookup & simulating all trades...")
    price_lookup = build_price_lookup(prices)
    sim_results = simulate_per_trade(long, price_lookup)
    long = long.join(sim_results)
    valid = long.dropna(subset=["sim_ret"])
    print(f"  Simulated: {len(valid):,} / {len(long):,}")

    # ==== Enrich with new factors ====
    print("[3/6] Enriching with new factors...")
    valid = enrich_price_factors(valid, prices)
    valid = enrich_market_context(valid, index_prices, vi)
    valid = add_price_band(valid)
    valid = prepare_factor_columns(valid)

    # NaN report
    for col in ["sma20_slope", "momentum_20d", "n225_trend", "topix_trend", "vi_close"]:
        n_na = valid[col].isna().sum() if col in valid.columns else -1
        print(f"  {col}: {n_na:,} NaN ({n_na/len(valid)*100:.1f}%)")

    sections_html = []

    # ==== Section 1: Baseline ====
    print("[4/6] Section 1: Baseline...")
    cards = []
    rule_baseline: dict[str, dict] = {}
    for rule in RULES:
        sub = valid[valid["rule"] == rule]
        rets = sub["sim_ret"].values
        pnls = sub["sim_pnl"].values
        wins = rets > 0
        gw, gl = rets[wins].sum(), abs(rets[~wins].sum())
        pf = gw / gl if gl > 0 else 999
        pnl_m = pnls.sum() / 10000
        mode, param, sl = EXIT_CONFIGS[rule]
        mode_label = f"fixed_{int(param)}d" if mode == "fixed_N" else f"min_hold_{int(param)}d"
        sl_label = f"SL-{sl}%" if sl < 900 else "SLなし"
        rule_baseline[rule] = {"n": len(sub), "wr": round(wins.mean() * 100, 1), "pf": round(pf, 2), "pnl_m": round(pnl_m, 1)}
        cards.append(_stat_card(
            rule, f'{pnl_m:+,.0f}万',
            f'{mode_label} / {sl_label} / N={len(sub):,} / WR={wins.mean()*100:.1f}% / PF={pf:.2f}',
            "pos" if pnl_m > 500 else ("warn" if pnl_m > 0 else "neg"),
        ))
    baseline_html = f'<div class="card-grid">{" ".join(cards)}</div>'
    baseline_html += _insight_box(
        "Ch5で確定したexit rule適用後のベースライン。"
        "ここから各ファクターでセグメントし、B1/B3の銘柄選別基準を探る。"
    )
    sections_html.append(_section("1. ベースライン（Ch5 Exit Rules適用）", baseline_html))

    # ==== Section 2-3: Factor sweep for B1 and B3 ====
    print("[5/6] Section 2-3: Factor sweep for B1/B3...")

    factor_rankings: dict[str, list] = {"B1": [], "B3": []}  # (factor_name, pnl_spread)

    for rule in ["B1", "B3"]:
        sec_num = 2 if rule == "B1" else 3
        sub = valid[valid["rule"] == rule]
        factor_html = ""

        for fname, fcol, _, _ in FACTOR_DEFS:
            if fcol not in sub.columns:
                continue
            stats = segment_stats(sub, fcol)
            if len(stats) < 2:
                continue

            # テーブル
            rows = []
            for s in stats:
                pnl_val = s["pnl_m"]
                rows.append([
                    s["segment"], f'{s["n"]:,}', f'{s["wr"]:.1f}%',
                    f'{s["pf"]:.2f}', f'{s["avg_ret"]:+.3f}%', f'{pnl_val:+,.1f}万',
                ])
            factor_html += f"<h3>{fname}</h3>"
            factor_html += _table_html(
                ["セグメント", "N", "WR", "PF", "Avg Ret", "PnL(万)"],
                rows, highlight_col=5,
            )

            # PnLバーチャート
            segs = [s["segment"] for s in stats]
            pnls = [s["pnl_m"] for s in stats]
            factor_html += _plotly_bar(
                f"bar_{rule}_{fcol}", segs, pnls,
                title=f"{rule}: {fname}別PnL", yaxis_title="PnL(万)", height=250,
            )

            # Factor ranking: PnL spread (best - worst)
            if pnls:
                spread = max(pnls) - min(pnls)
                factor_rankings[rule].append((fname, spread, stats))

        sections_html.append(_section(f"{sec_num}. {rule} ファクター別PnL分析", factor_html))

    # ==== Section 4: Factor Effectiveness Ranking ====
    print("[6/6] Section 4-5: Ranking & Impact...")
    rank_html = ""
    for rule in ["B1", "B3"]:
        sorted_factors = sorted(factor_rankings[rule], key=lambda x: x[1], reverse=True)
        rows = []
        for i, (fname, spread, stats) in enumerate(sorted_factors, 1):
            best = max(stats, key=lambda s: s["pnl_m"])
            worst = min(stats, key=lambda s: s["pnl_m"])
            rows.append([
                f'{i}', fname,
                f'{best["segment"]} ({best["pnl_m"]:+,.1f}万)',
                f'{worst["segment"]} ({worst["pnl_m"]:+,.1f}万)',
                f'{spread:,.1f}万',
            ])
        rank_html += f"<h3>{rule}: PnL識別力ランキング</h3>"
        rank_html += _table_html(
            ["#", "ファクター", "Best Segment", "Worst Segment", "PnL Spread"],
            rows, highlight_col=4,
        )

    rank_html += _insight_box(
        "PnL Spread = ベストセグメントのPnL - ワーストセグメントのPnL。"
        "Spreadが大きいファクターほど銘柄選別に有効。"
    )
    sections_html.append(_section("4. ファクター識別力ランキング", rank_html))

    # ==== Section 5: PnL Impact (filter test) ====
    impact_html = ""
    impact_html += _insight_box(
        "<b>検証</b>: ランキング上位ファクターの「ベストセグメント」に絞った場合、総PnLは増えるか？"
        "<br>PFが高くてもトレード数が減れば総PnLは下がる可能性がある（Ch4-1の教訓）。"
    )

    for rule in ["B1", "B3"]:
        sub = valid[valid["rule"] == rule]
        base = rule_baseline[rule]
        sorted_factors = sorted(factor_rankings[rule], key=lambda x: x[1], reverse=True)

        impact_rows = []
        for fname, spread, stats in sorted_factors[:5]:  # top 5のみ
            best_seg = max(stats, key=lambda s: s["pnl_m"])
            # このセグメントだけに絞った場合
            delta = best_seg["pnl_m"] - base["pnl_m"]
            impact_rows.append([
                fname,
                best_seg["segment"],
                f'{best_seg["n"]:,}',
                f'{best_seg["wr"]:.1f}%',
                f'{best_seg["pf"]:.2f}',
                f'{best_seg["pnl_m"]:+,.1f}万',
                f'{delta:+,.1f}万',
            ])

        impact_html += f"<h3>{rule}（ベースライン: {base['pnl_m']:+,.1f}万 / N={base['n']:,}）</h3>"
        impact_html += _table_html(
            ["ファクター", "Best Segment", "N", "WR", "PF", "PnL", "vs全体"],
            impact_rows, highlight_col=5,
        )

    # 複合フィルター: top2ファクターのAND条件
    for rule in ["B1", "B3"]:
        sub = valid[valid["rule"] == rule]
        base = rule_baseline[rule]
        sorted_factors = sorted(factor_rankings[rule], key=lambda x: x[1], reverse=True)

        if len(sorted_factors) >= 2:
            f1_name, _, f1_stats = sorted_factors[0]
            f2_name, _, f2_stats = sorted_factors[1]
            f1_best = max(f1_stats, key=lambda s: s["pnl_m"])["segment"]
            f2_best = max(f2_stats, key=lambda s: s["pnl_m"])["segment"]

            # Find corresponding columns
            f1_col = next((col for name, col, _, _ in FACTOR_DEFS if name == f1_name), None)
            f2_col = next((col for name, col, _, _ in FACTOR_DEFS if name == f2_name), None)

            if f1_col and f2_col and f1_col in sub.columns and f2_col in sub.columns:
                filtered = sub[
                    (sub[f1_col].astype(str) == f1_best) &
                    (sub[f2_col].astype(str) == f2_best)
                ]
                filt_valid = filtered.dropna(subset=["sim_ret"])
                if len(filt_valid) > 0:
                    rets = filt_valid["sim_ret"].values
                    pnls_arr = filt_valid["sim_pnl"].values
                    wins = rets > 0
                    gw, gl = rets[wins].sum(), abs(rets[~wins].sum())
                    pf = gw / gl if gl > 0 else 999
                    pnl_m = pnls_arr.sum() / 10000
                    delta = pnl_m - base["pnl_m"]

                    impact_html += f"<h3>{rule}: 複合フィルター（{f1_name}={f1_best} AND {f2_name}={f2_best}）</h3>"
                    impact_html += _table_html(
                        ["条件", "N", "WR", "PF", "PnL", "vs全体"],
                        [[
                            f'{f1_name}={f1_best} & {f2_name}={f2_best}',
                            f'{len(filt_valid):,}',
                            f'{wins.mean()*100:.1f}%',
                            f'{pf:.2f}',
                            f'{pnl_m:+,.1f}万',
                            f'{delta:+,.1f}万',
                        ]],
                    )

    sections_html.append(_section("5. PnLインパクト: フィルタリング効果", impact_html))

    # ==== Section 6: B2/B4 Reference ====
    ref_html = ""
    for rule in ["B2", "B4"]:
        sub = valid[valid["rule"] == rule]
        ref_html += f"<h3>{rule}（参考）</h3>"
        # top 3 factors only
        rule_factors = []
        for fname, fcol, _, _ in FACTOR_DEFS:
            if fcol not in sub.columns:
                continue
            stats = segment_stats(sub, fcol)
            if len(stats) < 2:
                continue
            pnls = [s["pnl_m"] for s in stats]
            spread = max(pnls) - min(pnls)
            rule_factors.append((fname, spread, stats))

        rule_factors.sort(key=lambda x: x[1], reverse=True)
        for fname, spread, stats in rule_factors[:3]:
            best = max(stats, key=lambda s: s["pnl_m"])
            worst = min(stats, key=lambda s: s["pnl_m"])
            rows = [[s["segment"], f'{s["n"]:,}', f'{s["wr"]:.1f}%',
                     f'{s["pf"]:.2f}', f'{s["pnl_m"]:+,.1f}万'] for s in stats]
            ref_html += f"<h4>{fname}（Spread: {spread:,.1f}万）</h4>"
            ref_html += _table_html(["セグメント", "N", "WR", "PF", "PnL(万)"], rows, highlight_col=4)

    sections_html.append(_section("6. B2/B4 参考", ref_html))

    # ==== Generate HTML ====
    body = "\n".join(sections_html)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ch5-1 Stock Selection — Granville Strategy Verification</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
:root {{
  --bg: #0f1117; --card: #1a1d27; --border: #2a2d3a;
  --text: #e2e8f0; --muted: #8892a8; --primary: #60a5fa;
  --pos: #34d399; --neg: #f87171; --warn: #fbbf24;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: var(--bg); color: var(--text);
  line-height: 1.6; padding: 20px;
}}
h1 {{ font-size: 1.4rem; margin-bottom: 4px; }}
h2 {{ font-size: 1.1rem; color: var(--primary); margin: 24px 0 12px; border-bottom: 1px solid var(--border); padding-bottom: 6px; }}
h3 {{ font-size: 0.95rem; color: var(--muted); margin: 16px 0 8px; }}
h4 {{ font-size: 0.85rem; color: var(--muted); margin: 12px 0 6px; }}
section {{ margin-bottom: 24px; }}
.meta {{ font-size: 0.75rem; color: var(--muted); margin-bottom: 16px; }}
.card-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 12px 0; }}
.stat-card {{
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; padding: 12px; text-align: center;
}}
.stat-card .label {{ font-size: 0.75rem; color: var(--muted); }}
.stat-card .value {{ font-size: 1.3rem; font-weight: 700; margin: 4px 0; }}
.stat-card .sub {{ font-size: 0.65rem; color: var(--muted); }}
.card-pos .value {{ color: var(--pos); }}
.card-neg .value {{ color: var(--neg); }}
.card-warn .value {{ color: var(--warn); }}
table {{
  width: 100%; border-collapse: collapse; font-size: 0.8rem;
  margin: 10px 0; background: var(--card);
}}
th, td {{ padding: 6px 10px; border: 1px solid var(--border); text-align: right; }}
th {{ background: #1e2130; color: var(--primary); font-weight: 600; text-align: center; }}
td:first-child {{ text-align: left; font-weight: 500; }}
.best-row {{ background: rgba(96, 165, 250, 0.12); }}
.insight-box {{
  background: rgba(96, 165, 250, 0.08); border-left: 3px solid var(--primary);
  padding: 10px 14px; margin: 12px 0; font-size: 0.82rem;
  border-radius: 0 6px 6px 0; line-height: 1.7;
}}
@media (max-width: 768px) {{
  .card-grid {{ grid-template-columns: repeat(2, 1fr); }}
  table {{ font-size: 0.7rem; }}
  th, td {{ padding: 4px 6px; }}
}}
</style>
</head>
<body>
<h1>Chapter 5-1: Stock Selection under New Exit Rules</h1>
<div class="meta">Generated: {now} | Data: {len(valid):,} LONG trades (simulated with Ch5 exit rules) | 12 factors analyzed</div>
{body}
</body>
</html>"""

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / "report.html"
    out.write_text(html, encoding="utf-8")
    print(f"\n[OK] Report saved: {out}")
    print(f"  Size: {out.stat().st_size / 1024:.0f} KB")
    print(f"  Done in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
