#!/usr/bin/env python3
"""
37_b4_vi_n225_exclusion.py
Codex指摘2: VI/N225除外ルールの効果検証

C案（前5日高値含む）ベースで、除外ルール有無を資金制約付きで比較:
- ベースライン: 除外なし
- VI30-40膠着除外
- N225<-3%除外
- VI30-40GU除外
- 全除外ルール適用
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

PRICES_PATH = PARQUET_DIR / "granville" / "prices_topix.parquet"
MAX_HOLD = 15
INITIAL_CAPITAL = 4_650_000
MARGIN_RATE = 0.30
PRE_ENTRY_DAYS = 5  # C案


def load_vi() -> pd.DataFrame:
    """3つのCSVからVI全期間データを結合"""
    base = Path("/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/data/csv")

    vi2 = pd.read_csv(base / "日経平均ボラティリティー・インデックス 過去データ.csv")
    vi2 = vi2.rename(columns={"日付": "date", "終値": "vi_close"})

    vi3 = pd.read_csv(base / "日経平均ボラティリティー・インデックス 過去データ (1).csv")
    vi3 = vi3.rename(columns={"日付": "date", "終値": "vi_close"})

    vi1 = pd.read_csv(base / "nikkeivi.csv")
    vi1 = vi1.rename(columns={"日付": "date", "終値": "vi_close"})

    combined = pd.concat([vi2[["date", "vi_close"]], vi3[["date", "vi_close"]], vi1[["date", "vi_close"]]])
    combined["date"] = pd.to_datetime(combined["date"], format="mixed")
    combined["vi_close"] = pd.to_numeric(combined["vi_close"].astype(str).str.replace(",", ""), errors="coerce")
    combined = combined.dropna(subset=["vi_close"])
    combined = combined.drop_duplicates(subset=["date"], keep="last")
    combined = combined.sort_values("date").reset_index(drop=True)
    combined["vi_prev"] = combined["vi_close"].shift(1)
    combined["vi_chg"] = (combined["vi_close"] - combined["vi_prev"]) / combined["vi_prev"] * 100
    return combined


def load_n225() -> pd.DataFrame:
    """N225データ"""
    idx = pd.read_parquet(PARQUET_DIR / "index_prices_max_1d.parquet")
    n225 = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    n225["date"] = pd.to_datetime(n225["date"])
    n225 = n225.dropna(subset=["Close"]).sort_values("date").reset_index(drop=True)
    n225["n225_sma20"] = n225["Close"].rolling(20, min_periods=20).mean()
    n225["n225_dev"] = (n225["Close"] - n225["n225_sma20"]) / n225["n225_sma20"] * 100
    n225 = n225.rename(columns={"Close": "n225_close"})
    return n225[["date", "n225_close", "n225_dev"]]


def load_prices_with_indicators():
    ps = pd.read_parquet(PRICES_PATH)
    ps["date"] = pd.to_datetime(ps["date"])
    cutoff = ps["date"].max() - pd.DateOffset(years=2)
    buffer = cutoff - pd.Timedelta(days=120)
    ps = ps[ps["date"] >= buffer].sort_values(["ticker", "date"]).reset_index(drop=True)

    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ps["prev_close"] = g["Close"].shift(1)
    ps["dev"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["up_day"] = ps["Close"] > ps["prev_close"]
    ps["sma60"] = g["Close"].transform(lambda x: x.rolling(60, min_periods=60).mean())
    ps["sma100"] = g["Close"].transform(lambda x: x.rolling(100, min_periods=100).mean())
    ps["dev60"] = (ps["Close"] - ps["sma60"]) / ps["sma60"] * 100
    ps["dev100"] = (ps["Close"] - ps["sma100"]) / ps["sma100"] * 100
    ps["max_up20"] = g["dev"].transform(lambda x: x.rolling(60, min_periods=1).max())
    ps["max_up60"] = g["dev60"].transform(lambda x: x.rolling(60, min_periods=1).max())
    ps["max_up100"] = g["dev100"].transform(lambda x: x.rolling(60, min_periods=1).max())
    ps = ps.dropna(subset=["sma20"])
    ps = ps[ps["date"] >= cutoff].copy()

    # VI/N225結合
    vi = load_vi()
    n225 = load_n225()
    ps = ps.merge(vi[["date", "vi_close", "vi_chg"]], on="date", how="left")
    ps = ps.merge(n225[["date", "n225_dev"]], on="date", how="left")

    return ps


def get_b4_signals(ps, exclude_rules=None):
    """B4シグナル検出（除外ルール適用）"""
    b4 = (ps["dev"] < -15) & ps["up_day"]
    surge = (ps["max_up20"] >= 15) | (ps["max_up60"] >= 20) | (ps["max_up100"] >= 30)
    b4 = b4 & ~surge

    if exclude_rules and "vi_stag" in exclude_rules:
        # VI30-40膠着: VI>=30 AND VI<=40 AND |変化|<1%
        vi_stag = (ps["vi_close"] >= 30) & (ps["vi_close"] <= 40) & (ps["vi_chg"].abs() < 1)
        b4 = b4 & ~vi_stag

    if exclude_rules and "n225_drop" in exclude_rules:
        # N225 SMA20乖離 < -3%
        n225_drop = ps["n225_dev"] < -3
        b4 = b4 & ~n225_drop

    if exclude_rules and "vi_gu" in exclude_rules:
        # VI30-40 AND 前日比>0（ギャップアップ）
        vi_gu = (ps["vi_close"] >= 30) & (ps["vi_close"] <= 40) & (ps["vi_chg"] > 0)
        b4 = b4 & ~vi_gu

    signals = ps[b4].copy()
    return signals.sort_values(["date", "dev"])


def sim_portfolio(signals, all_data_cache):
    """資金制約付きポートフォリオシミュレーション（C案: 前5日含む）"""
    capital = INITIAL_CAPITAL
    peak = capital
    max_dd = 0.0
    realized_pnl = 0
    open_positions = []
    trades_taken = 0
    trades_skipped = 0
    total_pnl = 0
    wins = 0
    gross_win = 0.0
    gross_loss = 0.0

    for sig_date, day_sigs in signals.groupby("date"):
        # close expired
        new_open = []
        for pos in open_positions:
            if pos["exit_date"] <= sig_date:
                capital += pos["margin"]
                realized_pnl += pos["pnl"]
                total_pnl += pos["pnl"]
                if pos["ret"] > 0:
                    wins += 1
                    gross_win += pos["ret"]
                else:
                    gross_loss += abs(pos["ret"])
            else:
                new_open.append(pos)
        open_positions = new_open

        equity = INITIAL_CAPITAL + realized_pnl
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

        for _, sig in day_sigs.iterrows():
            tk = sig["ticker"]
            if tk not in all_data_cache:
                continue
            tk_df = all_data_cache[tk]
            sig_indices = tk_df.index[tk_df["date"] == sig_date]
            if len(sig_indices) == 0:
                continue
            sig_iloc = tk_df.index.get_loc(sig_indices[0])
            if sig_iloc + 1 >= len(tk_df):
                continue

            ep = float(tk_df.iloc[sig_iloc + 1]["Open"])
            if pd.isna(ep) or ep <= 0:
                continue
            entry_iloc = sig_iloc + 1

            margin = ep * 100 * MARGIN_RATE
            if margin > capital:
                trades_skipped += 1
                continue

            # C案 exit計算
            exit_price = 0.0
            exit_date = None
            exit_day = 0
            hold_end = min(entry_iloc + MAX_HOLD, len(tk_df) - 1)

            for i in range(min(hold_end - entry_iloc, MAX_HOLD)):
                cur_iloc = entry_iloc + i
                if cur_iloc >= len(tk_df):
                    break
                cur_high = float(tk_df.iloc[cur_iloc]["High"])

                if i > 0:
                    start = max(entry_iloc - PRE_ENTRY_DAYS, cur_iloc - 19)
                    start = max(0, start)
                    window = tk_df.iloc[start:cur_iloc + 1]["High"]
                    if cur_high >= float(window.max()):
                        next_iloc = cur_iloc + 1
                        if next_iloc < len(tk_df):
                            exit_price = float(tk_df.iloc[next_iloc]["Open"])
                            exit_date = tk_df.iloc[next_iloc]["date"]
                            exit_day = i + 2
                            break

                if i >= min(hold_end - entry_iloc, MAX_HOLD) - 1:
                    next_iloc = cur_iloc + 1
                    if next_iloc < len(tk_df):
                        exit_price = float(tk_df.iloc[next_iloc]["Open"])
                        exit_date = tk_df.iloc[next_iloc]["date"]
                        exit_day = i + 2
                        break

            if exit_date is None or exit_price <= 0:
                continue

            ret = (exit_price / ep - 1) * 100
            pnl = int((exit_price - ep) * 100)

            capital -= margin
            open_positions.append({"exit_date": exit_date, "margin": margin, "pnl": pnl, "ret": ret})
            trades_taken += 1

    for pos in open_positions:
        total_pnl += pos["pnl"]
        if pos["ret"] > 0:
            wins += 1
            gross_win += pos["ret"]
        else:
            gross_loss += abs(pos["ret"])

    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    wr = wins / trades_taken * 100 if trades_taken > 0 else 0

    return {
        "taken": trades_taken,
        "skipped": trades_skipped,
        "pnl": total_pnl,
        "wr": round(wr, 1),
        "pf": round(pf, 2),
        "max_dd": round(max_dd, 1),
    }


def main():
    print("=" * 70)
    print("B4 VI/N225除外ルール検証（C案ベース、資金制約あり）")
    print("=" * 70)

    print("\nLoading data...")
    ps = load_prices_with_indicators()
    print(f"  {ps['ticker'].nunique()} tickers, {ps['date'].min().date()} ~ {ps['date'].max().date()}")
    print(f"  VI coverage: {ps['vi_close'].notna().sum() / len(ps) * 100:.0f}%")
    print(f"  N225 coverage: {ps['n225_dev'].notna().sum() / len(ps) * 100:.0f}%")

    # ticker別データキャッシュ
    all_data = pd.read_parquet(PRICES_PATH)
    all_data["date"] = pd.to_datetime(all_data["date"])
    cutoff = ps["date"].min() - pd.Timedelta(days=60)
    all_data = all_data[all_data["date"] >= cutoff].sort_values(["ticker", "date"]).reset_index(drop=True)
    cache = {tk: gdf.sort_values("date").reset_index(drop=True) for tk, gdf in all_data.groupby("ticker")}

    scenarios = [
        ("ベースライン（除外なし）", None),
        ("VI30-40膠着除外", ["vi_stag"]),
        ("N225<-3%除外", ["n225_drop"]),
        ("VI30-40GU除外", ["vi_gu"]),
        ("全除外ルール適用", ["vi_stag", "n225_drop", "vi_gu"]),
    ]

    print(f"\n{'シナリオ':<25} {'シグナル':>6} {'件数':>5} {'見送':>5} {'WR%':>6} {'PF':>6} {'PnL':>12} {'DD%':>6}")
    print("-" * 80)

    for label, rules in scenarios:
        sigs = get_b4_signals(ps, rules)
        r = sim_portfolio(sigs, cache)
        print(f"{label:<25} {len(sigs):>6} {r['taken']:>5} {r['skipped']:>5} {r['wr']:>6} {r['pf']:>6} {r['pnl']:>12,} {r['max_dd']:>6}")


if __name__ == "__main__":
    main()
