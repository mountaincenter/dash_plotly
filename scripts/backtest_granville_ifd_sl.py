#!/usr/bin/env python3
"""
グランビル IFD損切りのみ検証
============================
TP設定なし（利確は保有期間満了で引け決済）
SLのみ: 2%, 3%, 4%, 5%, 7%, 10%, なし
保有日数: 5, 7, 10
フィルター: uptrend + CI拡大 / 悪セクター除外
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MACRO_DIR = ROOT / "improvement" / "data" / "macro"
BAD_SECTORS = ["医薬品", "輸送用機器", "小売業", "その他製品", "陸運業", "サービス業"]

SL_RANGE = [2.0, 3.0, 4.0, 5.0, 7.0, 10.0, None]  # None = SLなし
HOLD_RANGE = [5, 7, 10]


def load_data():
    m = pd.read_parquet(ROOT / "data" / "parquet" / "meta.parquet")
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)
    tickers = m["ticker"].tolist()

    p = pd.read_parquet(ROOT / "data" / "parquet" / "prices_max_1d.parquet")
    ps = p[p["ticker"].isin(tickers)].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    idx = pd.read_parquet(ROOT / "data" / "parquet" / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").rename(columns={"Close": "nk225_close"})
    nk["nk225_sma20"] = nk["nk225_close"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225_close"] > nk["nk225_sma20"]

    ci = pd.read_parquet(MACRO_DIR / "estat_ci_index.parquet")
    ci = ci[["date", "leading"]].rename(columns={"leading": "ci_leading"})
    ci["ci_leading_chg3m"] = ci["ci_leading"].diff(3)

    daily = pd.DataFrame({"date": ps["date"].drop_duplicates().sort_values()})
    daily = daily.merge(ci, on="date", how="left").sort_values("date").ffill()

    g = ps.groupby("ticker")
    ps["sma5"] = g["Close"].transform(lambda x: x.rolling(5).mean())
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["sma5_above_sma20"] = ps["sma5"] > ps["sma20"]
    ps["prev_sma5_above"] = g["sma5_above_sma20"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)
    ps["prev_close"] = g["Close"].shift(1)
    ps = ps.dropna(subset=["sma20"])

    ps = ps.merge(nk[["date", "market_uptrend"]], on="date", how="left")
    ps = ps.merge(daily, on="date", how="left")
    ps["macro_ci_expand"] = ps["ci_leading_chg3m"] > 0
    ps = ps.merge(m[["ticker", "sectors"]], on="ticker", how="left")

    return ps


def detect_signals(df):
    df = df.copy()
    dev = df["dev_from_sma20"]
    sma_up = df["sma20_up"]
    above = df["Close"] > df["sma20"]

    df["sig_A"] = (dev.between(-8, -3)) & (df["Close"] > df["prev_close"])
    df["sig_B"] = (
        sma_up & above &
        (dev.between(0, 2)) & (df["prev_dev"] <= 0.5) &
        (df["Close"] > df["prev_close"])
    )
    df["sig_C"] = (
        df["sma5_above_sma20"] &
        ~df["prev_sma5_above"].fillna(False).astype(bool)
    )
    return df


def simulate_ifd_sl(prices_df, signal_rows, sl_pct, hold_days):
    """IFD損切りのみシミュレーション（TPなし）"""
    rets = []
    exit_types = []

    for ticker in signal_rows["ticker"].unique():
        tk = prices_df[prices_df["ticker"] == ticker].sort_values("date")
        dates = tk["date"].values
        opens = tk["Open"].values
        highs = tk["High"].values
        lows = tk["Low"].values
        closes = tk["Close"].values
        date_idx = {d: i for i, d in enumerate(dates)}

        sig_dates = signal_rows[signal_rows["ticker"] == ticker]["date"].values
        for sd in sig_dates:
            if sd not in date_idx:
                continue
            idx = date_idx[sd]
            if idx + 1 >= len(dates):
                continue

            entry_idx = idx + 1
            entry_price = opens[entry_idx]
            if np.isnan(entry_price) or entry_price <= 0:
                continue

            sl_price = entry_price * (1 - sl_pct / 100) if sl_pct else 0

            hit_sl = False
            for d in range(hold_days):
                ci = entry_idx + d
                if ci >= len(dates):
                    break
                if sl_pct and lows[ci] <= sl_price:
                    ret = -sl_pct
                    hit_sl = True
                    break

            if not hit_sl:
                last_idx = min(entry_idx + hold_days - 1, len(dates) - 1)
                exit_price = closes[last_idx]
                ret = (exit_price / entry_price - 1) * 100

            rets.append(ret)
            exit_types.append("SL" if hit_sl else "expire")

    if not rets:
        return None

    rets = np.array(rets)
    n = len(rets)
    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    pf = round(wins.sum() / abs(losses.sum()), 2) if len(losses) > 0 and losses.sum() != 0 else 999
    sl_hit = sum(1 for t in exit_types if t == "SL") / n * 100
    max_loss = rets.min()
    max_win = rets.max()

    return {
        "n": n,
        "mean": round(rets.mean(), 3),
        "median": round(float(np.median(rets)), 3),
        "win%": round((rets > 0).mean() * 100, 1),
        "pf": pf,
        "sl_hit%": round(sl_hit, 1),
        "max_loss": round(max_loss, 1),
        "max_win": round(max_win, 1),
        "sharpe": round(rets.mean() / rets.std(), 3) if rets.std() > 0 else 0,
    }


def main():
    print("=" * 80)
    print("グランビル IFD損切りのみ検証（TPなし・引け決済）")
    print("フィルター: uptrend + CI拡大 / 悪セクター除外")
    print("=" * 80)

    prices = load_data()
    prices = detect_signals(prices)

    filtered = prices[
        (prices["market_uptrend"] == True) &
        (prices["macro_ci_expand"] == True) &
        (~prices["sectors"].isin(BAD_SECTORS))
    ].copy()

    signal_names = {
        "sig_A": "A_dip_buy",
        "sig_B": "B_sma_support",
        "sig_C": "C_mini_gc",
    }

    for col, name in signal_names.items():
        sig_rows = filtered[filtered[col]].copy()
        print(f"\n{'='*80}")
        print(f"{name}（{len(sig_rows):,}件）")
        print(f"{'='*80}")

        for hd in HOLD_RANGE:
            print(f"\n  --- 保有{hd}日 ---")
            print(f"  {'SL':>6s} {'件数':>6s} {'平均%':>7s} {'中央%':>7s} {'勝率':>6s} "
                  f"{'PF':>6s} {'SL発動%':>6s} {'最大損':>6s} {'最大益':>6s} {'Sharpe':>7s}")
            print(f"  {'-'*75}")

            for sl in SL_RANGE:
                sl_label = f"{sl:.1f}%" if sl else "なし"
                st = simulate_ifd_sl(prices, sig_rows, sl, hd)
                if st is None:
                    continue

                marker = ""
                if st["pf"] >= 1.5:
                    marker = " ★★"
                elif st["pf"] >= 1.4:
                    marker = " ★"
                elif st["pf"] >= 1.3:
                    marker = " ◎"
                elif st["pf"] >= 1.2:
                    marker = " ○"

                print(
                    f"  {sl_label:>6s} {st['n']:>6,d} {st['mean']:>+7.3f} {st['median']:>+7.3f} "
                    f"{st['win%']:>5.1f}% {st['pf']:>5.02f} {st['sl_hit%']:>5.1f}% "
                    f"{st['max_loss']:>+5.1f}% {st['max_win']:>+5.1f}% "
                    f"{st['sharpe']:>7.3f}{marker}"
                )

    # ============================================================
    # ベスト設定 × セクター別
    # ============================================================
    print(f"\n{'='*80}")
    print("ベスト設定（SL -5% / 7日引け）× セクター別")
    print(f"{'='*80}")

    meta = pd.read_parquet(ROOT / "data" / "parquet" / "meta.parquet")
    meta["sectors"] = meta["sectors"].str.replace("･", "・", regex=False)

    for col, name in signal_names.items():
        sig_rows = filtered[filtered[col]].copy()
        if len(sig_rows) == 0:
            continue

        # セクター別にIFD SL-5%/7日を実行
        print(f"\n  --- {name} ---")
        print(f"  {'セクター':<16s} {'件数':>6s} {'平均%':>7s} {'勝率':>6s} {'PF':>6s} {'SL発動%':>6s}")
        print(f"  {'-'*55}")

        sector_res = []
        for sector in sorted(sig_rows["sectors"].dropna().unique()):
            sec_sig = sig_rows[sig_rows["sectors"] == sector]
            st = simulate_ifd_sl(prices, sec_sig, 5.0, 7)
            if st is None or st["n"] < 10:
                continue
            sector_res.append({"sector": sector, **st})

        sector_res = sorted(sector_res, key=lambda x: x["pf"], reverse=True)
        for r in sector_res:
            marker = " ★" if r["pf"] >= 1.5 else " ◎" if r["pf"] >= 1.3 else ""
            print(
                f"  {r['sector']:<16s} {r['n']:>6,d} {r['mean']:>+7.3f} "
                f"{r['win%']:>5.1f}% {r['pf']:>5.02f} {r['sl_hit%']:>5.1f}%{marker}"
            )

    print(f"\n{'='*80}")
    print("完了")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
