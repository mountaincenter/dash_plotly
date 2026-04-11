#!/usr/bin/env python3
"""
granville_b1b3_focused.py
B1-B3 仮説検証 — 多重比較排除 + 下落期検証

事前定義フィルター（3つのみ）:
  H1: VI ≥ 30 + B1 hold4d/9d（恐怖局面リバウンド）
  H2: N225>SMA20 + CME flat + B3 hold4d/9d（上昇トレンド+静かな夜）
  H3: N225 ret20 < -5% + B1 hold4d（急落後の反発）

期間分割:
  全期間:   2020-01-01 ~ 2026-04-07
  下落期:   2022-01-01 ~ 2022-12-31（N225: 29000→26000、年間-9%）
  横ばい期: 2023-01-01 ~ 2023-06-30（N225: 26000→33000、回復初動）
  上昇期:   2023-07-01 ~ 2024-12-31
  OOS:      2025-01-01 ~ 2026-04-07
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

PRICES_FILE = ROOT / "data" / "parquet" / "granville" / "prices_topix.parquet"
INDEX_FILE = ROOT / "data" / "parquet" / "index_prices_max_1d.parquet"
FUTURES_FILE = ROOT / "data" / "parquet" / "futures_prices_max_1d.parquet"
VI_FILE = ROOT / "data" / "parquet" / "nikkei_vi_history.parquet"

PERIODS = [
    ("下落期2022", "2022-01-01", "2022-12-31"),
    ("回復期2023H1", "2023-01-01", "2023-06-30"),
    ("上昇期2023H2-2024", "2023-07-01", "2024-12-31"),
    ("IS全体", "2020-01-01", "2024-12-31"),
    ("OOS", "2025-01-01", "2026-04-07"),
]


def load_prices() -> pd.DataFrame:
    print("[1] Loading prices...")
    ps = pd.read_parquet(PRICES_FILE)
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)
    print(f"  {len(ps):,} rows, {ps['ticker'].nunique()} tickers")

    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["prev_close"] = g["Close"].shift(1)
    ps["prev_sma20"] = g["sma20"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)

    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["above"] = ps["Close"] > ps["sma20"]
    ps["below"] = ps["Close"] < ps["sma20"]
    ps["prev_below"] = ps["prev_close"] < ps["prev_sma20"]
    ps["up_day"] = ps["Close"] > ps["prev_close"]

    # エントリー=翌日Open、イグジット=N日後Open
    for d in [1, 5, 10]:
        ps[f"open_{d}d_later"] = g["Open"].shift(-d)

    ps = ps.dropna(subset=["sma20"])
    return ps


def load_regime() -> pd.DataFrame:
    idx = pd.read_parquet(INDEX_FILE)
    idx["date"] = pd.to_datetime(idx["date"])
    n225 = idx[idx["ticker"] == "^N225"][["date", "Close"]].sort_values("date")
    n225["n225_sma20"] = n225["Close"].rolling(20).mean()
    n225["n225_above_sma20"] = n225["Close"] > n225["n225_sma20"]
    n225["n225_ret20"] = n225["Close"].pct_change(20) * 100
    n225["n225_prev_close"] = n225["Close"].shift(1)

    # CME gap
    fut = pd.read_parquet(FUTURES_FILE)
    fut["date"] = pd.to_datetime(fut["date"])
    nkd = fut[fut["ticker"] == "NKD=F"][["date", "Close"]].sort_values("date")
    nkd = nkd.rename(columns={"Close": "nkd_close"})
    n225 = n225.merge(nkd, on="date", how="left")
    n225["cme_gap"] = (n225["nkd_close"] / n225["n225_prev_close"] - 1) * 100

    # VI
    if VI_FILE.exists():
        vi = pd.read_parquet(VI_FILE)
        vi["date"] = pd.to_datetime(vi["date"])
        col = "close" if "close" in vi.columns else "Close"
        vi = vi[["date", col]].rename(columns={col: "vi"})
        n225 = n225.merge(vi, on="date", how="left")
    else:
        n225["vi"] = np.nan

    return n225[["date", "n225_above_sma20", "n225_ret20", "cme_gap", "vi"]].dropna(subset=["n225_above_sma20"])


def detect_signals(ps: pd.DataFrame) -> pd.DataFrame:
    df = ps.copy()
    dev = df["dev_from_sma20"]
    sma_up = df["sma20_up"]

    df["B1"] = df["prev_below"] & df["above"] & sma_up
    df["B2"] = sma_up & dev.between(-5, 0) & df["up_day"] & df["below"]
    df["B3"] = sma_up & df["above"] & dev.between(0, 3) & (df["prev_dev"] > dev) & df["up_day"]

    sigs = []
    for rule in ["B1", "B3", "B2"]:
        s = df[df[rule]].copy()
        s["rule"] = rule
        sigs.append(s)

    out = pd.concat(sigs)
    out = out.drop_duplicates(subset=["ticker", "date"], keep="first")
    return out


def calc_return(signals: pd.DataFrame, hold_days: int) -> pd.Series:
    """entry=翌日Open, exit=hold_days後Open"""
    entry_col = "open_1d_later"
    exit_day = hold_days + 1
    available = [1, 5, 10]
    closest = min(available, key=lambda x: abs(x - exit_day))
    exit_col = f"open_{closest}d_later"
    valid = signals.dropna(subset=[entry_col, exit_col])
    if valid.empty:
        return pd.Series(dtype=float)
    return (valid[exit_col] - valid[entry_col]) / valid[entry_col] * 100


def eval_hypothesis(signals: pd.DataFrame, mask: pd.Series, hold: int, label: str) -> dict:
    s = signals[mask]
    ret = calc_return(s, hold)
    n = len(ret)
    if n < 10:
        return {"label": label, "hold": hold, "n": n, "skip": True}

    wins = ret[ret > 0]
    losses = ret[ret <= 0]
    pf = wins.sum() / abs(losses.sum()) if losses.sum() != 0 else float("inf")

    return {
        "label": label,
        "hold": hold,
        "n": n,
        "ret_mean": round(ret.mean(), 3),
        "ret_median": round(ret.median(), 3),
        "win_rate": round(len(wins) / len(ret) * 100, 1),
        "pf": round(pf, 2),
        "skip": False,
    }


def run_period(signals: pd.DataFrame, period_name: str, start: str, end: str) -> list[dict]:
    sub = signals[(signals["date"] >= start) & (signals["date"] <= end)]
    n_signals = len(sub)
    n_tickers = sub["ticker"].nunique() if not sub.empty else 0
    print(f"\n  [{period_name}] {start} ~ {end}  シグナル: {n_signals:,}  銘柄: {n_tickers}")

    if n_signals < 30:
        print("    → サンプル不足")
        return []

    rows = []

    # ベースライン
    for hold in [4, 9]:
        r = eval_hypothesis(sub, sub.index.isin(sub.index), hold, "BASE_ALL")
        r["period"] = period_name
        rows.append(r)

        for rule in ["B1", "B3"]:
            r = eval_hypothesis(sub, sub["rule"] == rule, hold, f"BASE_{rule}")
            r["period"] = period_name
            rows.append(r)

    # H1: VI ≥ 30 + B1
    for hold in [4, 9]:
        mask = (sub["vi"] >= 30) & (sub["rule"] == "B1")
        r = eval_hypothesis(sub, mask, hold, f"H1_VI30_B1")
        r["period"] = period_name
        rows.append(r)

    # H1b: VI ≥ 25 + B1（閾値緩和）
    for hold in [4, 9]:
        mask = (sub["vi"] >= 25) & (sub["rule"] == "B1")
        r = eval_hypothesis(sub, mask, hold, f"H1b_VI25_B1")
        r["period"] = period_name
        rows.append(r)

    # H2: N225>SMA20 + CME flat + B3
    for hold in [4, 9]:
        mask = (sub["n225_above_sma20"]) & (sub["cme_gap"].between(-0.5, 0.5)) & (sub["rule"] == "B3")
        r = eval_hypothesis(sub, mask, hold, f"H2_N225up_CMEflat_B3")
        r["period"] = period_name
        rows.append(r)

    # H2b: N225>SMA20 + CME flat + ALL（ルール制限なし）
    for hold in [4, 9]:
        mask = (sub["n225_above_sma20"]) & (sub["cme_gap"].between(-0.5, 0.5))
        r = eval_hypothesis(sub, mask, hold, f"H2b_N225up_CMEflat_ALL")
        r["period"] = period_name
        rows.append(r)

    # H3: N225 ret20 < -5% + B1
    for hold in [4, 9]:
        mask = (sub["n225_ret20"] < -5) & (sub["rule"] == "B1")
        r = eval_hypothesis(sub, mask, hold, f"H3_N225drop5_B1")
        r["period"] = period_name
        rows.append(r)

    # H3b: N225 ret20 < -5% + ALL
    for hold in [4, 9]:
        mask = (sub["n225_ret20"] < -5)
        r = eval_hypothesis(sub, mask, hold, f"H3b_N225drop5_ALL")
        r["period"] = period_name
        rows.append(r)

    return rows


def print_results(all_rows: list[dict]) -> None:
    df = pd.DataFrame(all_rows)
    df = df[~df.get("skip", False)].copy()

    # 全期間横並び表示
    labels = df["label"].unique()
    holds = sorted(df["hold"].unique())
    periods = [p[0] for p in PERIODS]

    for hold in holds:
        print(f"\n{'=' * 140}")
        print(f"  保有{hold}日 — 全期間横並び")
        print(f"{'=' * 140}")

        header = f"  {'仮説':<30}"
        for p in periods:
            header += f" | {p:>16} n {'PF':>5} {'勝率':>5} {'ret':>7}"
        print(header)
        print(f"  {'─' * 135}")

        for label in labels:
            line = f"  {label:<30}"
            for p in periods:
                row = df[(df["label"] == label) & (df["hold"] == hold) & (df["period"] == p)]
                if row.empty or row.iloc[0].get("skip"):
                    line += f" | {'':>16}   {'':>5} {'':>5} {'':>7}"
                else:
                    r = row.iloc[0]
                    n = r["n"]
                    pf = r.get("pf", 0)
                    wr = r.get("win_rate", 0)
                    ret = r.get("ret_mean", 0)
                    # PFハイライト
                    pf_str = f"{pf:>5.2f}"
                    line += f" | {n:>16} {pf_str:>5} {wr:>4.0f}% {ret:>6.2f}%"
            print(line)

    # 仮説ごとの期間安定性スコア
    print(f"\n{'=' * 140}")
    print("  仮説安定性チェック（全期間でPF>1.0の数 / 全期間数）")
    print(f"{'=' * 140}")

    for hold in holds:
        print(f"\n  --- hold {hold}d ---")
        for label in labels:
            if "BASE" in label:
                continue
            pfs = []
            for p in periods:
                row = df[(df["label"] == label) & (df["hold"] == hold) & (df["period"] == p)]
                if not row.empty and not row.iloc[0].get("skip"):
                    pfs.append((p, row.iloc[0].get("pf", 0)))

            if not pfs:
                continue

            n_above = sum(1 for _, pf in pfs if pf > 1.0)
            total = len(pfs)
            min_pf = min(pf for _, pf in pfs)
            max_pf = max(pf for _, pf in pfs)
            stability = f"{n_above}/{total}"

            verdict = "✅" if n_above == total and min_pf >= 1.1 else "△" if n_above >= total - 1 else "❌"
            detail = " ".join(f"{p[:4]}={pf:.2f}" for p, pf in pfs)
            print(f"  {verdict} {label:<30} 安定性={stability} min={min_pf:.2f} max={max_pf:.2f}  [{detail}]")


def main() -> int:
    print("=" * 70)
    print("Granville B1-B3 仮説検証（フォーカス版）")
    print("  事前定義3仮説 × 5期間")
    print("=" * 70)

    ps = load_prices()
    regime = load_regime()
    signals = detect_signals(ps)
    signals = signals.merge(regime, on="date", how="left")

    print(f"\n  Total signals: {len(signals):,}")

    all_rows = []
    for period_name, start, end in PERIODS:
        rows = run_period(signals, period_name, start, end)
        all_rows.extend(rows)

    print_results(all_rows)

    out = ROOT / "data" / "analysis" / "granville_b1b3_focused.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(all_rows).to_csv(out, index=False)
    print(f"\n📁 詳細: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
