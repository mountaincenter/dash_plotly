#!/usr/bin/env python3
"""
グランビル全4 buyシグナル × IFOバックテスト × セクター分析
===================================================
分析1: A/B/C/D 全シグナルを同一IFO条件(SL-2.5%/TP+3.0%/5日)で比較
分析2: セクター別パフォーマンス（東証33業種ベース）
フィルター: uptrend(日経>SMA20) + CI先行指数拡大
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

SL_PCT = 2.5   # 損切 %
TP_PCT = 3.0   # 利確 %
HOLD_DAYS = 5  # 最大保有日数


# ========== データロード ==========

def load_prices():
    m = pd.read_parquet(ROOT / "data" / "parquet" / "meta.parquet")
    # セクター名正規化（半角・全角中点統一）
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)
    tickers = m["ticker"].tolist()

    p = pd.read_parquet(ROOT / "data" / "parquet" / "prices_max_1d.parquet")
    ps = p[p["ticker"].isin(tickers)].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    # 日経225
    idx = pd.read_parquet(ROOT / "data" / "parquet" / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").rename(columns={"Close": "nk225_close"})
    nk["nk225_sma20"] = nk["nk225_close"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225_close"] > nk["nk225_sma20"]

    return ps, nk, m


def load_macro() -> pd.DataFrame:
    frames = []
    ci = pd.read_parquet(MACRO_DIR / "estat_ci_index.parquet")
    ci = ci[["date", "leading"]].rename(columns={"leading": "ci_leading"})
    ci["ci_leading_chg3m"] = ci["ci_leading"].diff(3)
    frames.append(ci)

    merged = frames[0]
    merged = merged.sort_values("date").reset_index(drop=True)
    return merged


def macro_to_daily(macro: pd.DataFrame, daily_dates: pd.Series) -> pd.DataFrame:
    daily = pd.DataFrame({"date": daily_dates.drop_duplicates().sort_values()})
    daily = daily.merge(macro, on="date", how="left")
    daily = daily.sort_values("date").ffill()
    return daily


# ========== テクニカル指標 ==========

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    g = df.groupby("ticker")

    df["sma5"] = g["Close"].transform(lambda x: x.rolling(5).mean())
    df["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    df["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    df["sma20_up"] = df["sma20_slope"] > 0

    df["sma5_above_sma20"] = df["sma5"] > df["sma20"]
    df["prev_sma5_above"] = g["sma5_above_sma20"].shift(1)

    df["dev_from_sma20"] = (df["Close"] - df["sma20"]) / df["sma20"] * 100
    df["prev_dev"] = g["dev_from_sma20"].shift(1)
    df["prev_close"] = g["Close"].shift(1)

    return df


def detect_signals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # 複数シグナル対応（1行に複数のシグナルがあり得る）
    df["sig_A"] = False
    df["sig_B"] = False
    df["sig_C"] = False
    df["sig_D"] = False

    dev = df["dev_from_sma20"]
    prev_dev = df["prev_dev"]
    sma_up = df["sma20_up"]
    above = df["Close"] > df["sma20"]

    # A: 押し目買い（乖離-3~-8% + 反発）
    df["sig_A"] = (dev.between(-8, -3)) & (df["Close"] > df["prev_close"])

    # B: SMA支持反発
    df["sig_B"] = (
        sma_up & above &
        (dev.between(0, 2)) & (prev_dev <= 0.5) &
        (df["Close"] > df["prev_close"])
    )

    # C: ミニGC（SMA5 > SMA20 クロス）
    df["sig_C"] = (
        df["sma5_above_sma20"] &
        ~df["prev_sma5_above"].fillna(False).astype(bool)
    )

    # D: 深押し買い（-5%以上乖離 + 反発）
    df["sig_D"] = (dev <= -5) & (df["Close"] > df["prev_close"])

    return df


# ========== IFOシミュレーション ==========

def simulate_ifo(prices_df: pd.DataFrame, signal_rows: pd.DataFrame,
                 sl_pct: float = SL_PCT, tp_pct: float = TP_PCT,
                 hold_days: int = HOLD_DAYS) -> pd.DataFrame:
    """
    IFOシミュレーション
    エントリー: シグナル翌日の始値
    SL/TP: 保有中の各日High/Lowでチェック
    期限: hold_days営業日目の引けで強制決済
    """
    results = []

    for ticker in signal_rows["ticker"].unique():
        tk_prices = prices_df[prices_df["ticker"] == ticker].sort_values("date")
        tk_signals = signal_rows[signal_rows["ticker"] == ticker]
        dates = tk_prices["date"].values
        date_idx = {d: i for i, d in enumerate(dates)}

        for _, sig_row in tk_signals.iterrows():
            sig_date = sig_row["date"]
            if sig_date not in date_idx:
                continue
            idx = date_idx[sig_date]

            # 翌営業日のOpen でエントリー
            if idx + 1 >= len(dates):
                continue
            entry_idx = idx + 1
            entry_price = tk_prices.iloc[entry_idx]["Open"]
            if pd.isna(entry_price) or entry_price <= 0:
                continue

            sl_price = entry_price * (1 - sl_pct / 100)
            tp_price = entry_price * (1 + tp_pct / 100)

            exit_type = "expire"
            exit_price = np.nan
            exit_day = 0

            # 保有期間中の各日チェック（entry_idx = day 1）
            for d in range(hold_days):
                check_idx = entry_idx + d
                if check_idx >= len(dates):
                    exit_type = "expire_early"
                    exit_price = tk_prices.iloc[min(check_idx - 1, len(dates) - 1)]["Close"]
                    exit_day = d
                    break

                row = tk_prices.iloc[check_idx]
                day_low = row["Low"]
                day_high = row["High"]

                # SL/TP判定（同日両方ヒットの場合SL優先 = 保守的）
                if day_low <= sl_price:
                    exit_type = "SL"
                    exit_price = sl_price
                    exit_day = d + 1
                    break
                elif day_high >= tp_price:
                    exit_type = "TP"
                    exit_price = tp_price
                    exit_day = d + 1
                    break

            # 期限満了（5日目引け）
            if exit_type == "expire":
                last_idx = min(entry_idx + hold_days - 1, len(dates) - 1)
                exit_price = tk_prices.iloc[last_idx]["Close"]
                exit_day = hold_days

            ret_pct = (exit_price / entry_price - 1) * 100

            results.append({
                "ticker": ticker,
                "signal_date": sig_date,
                "entry_date": dates[entry_idx],
                "entry_price": entry_price,
                "exit_price": exit_price,
                "exit_type": exit_type,
                "exit_day": exit_day,
                "ret_pct": round(ret_pct, 4),
            })

    return pd.DataFrame(results)


def ifo_stats(ifo_df: pd.DataFrame) -> dict:
    """IFO結果の統計量"""
    if ifo_df.empty or len(ifo_df) < 5:
        return None
    rets = ifo_df["ret_pct"]
    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    pf = round(wins.sum() / abs(losses.sum()), 2) if len(losses) > 0 and losses.sum() != 0 else 999
    return {
        "n": len(rets),
        "mean": round(rets.mean(), 3),
        "median": round(rets.median(), 3),
        "win%": round((rets > 0).mean() * 100, 1),
        "pf": pf,
        "tp_hit%": round((ifo_df["exit_type"] == "TP").mean() * 100, 1),
        "sl_hit%": round((ifo_df["exit_type"] == "SL").mean() * 100, 1),
        "expire%": round((ifo_df["exit_type"] == "expire").mean() * 100, 1),
        "avg_days": round(ifo_df["exit_day"].mean(), 1),
    }


# ========== メイン ==========

def main():
    print("=" * 72)
    print("グランビル全4 buyシグナル × IFO(SL-2.5%/TP+3.0%/5日) × セクター分析")
    print("フィルター: uptrend(日経>SMA20) + CI先行指数拡大")
    print("=" * 72)

    # --- データロード ---
    print("\n[1] データロード...")
    prices, nk, meta = load_prices()
    macro = load_macro()

    # テクニカル指標
    prices = add_features(prices)
    prices = prices.dropna(subset=["sma20"])

    # 日経マージ
    prices = prices.merge(nk[["date", "market_uptrend"]], on="date", how="left")

    # マクロマージ
    daily_macro = macro_to_daily(macro, prices["date"])
    prices = prices.merge(daily_macro, on="date", how="left")
    prices["macro_ci_expand"] = prices["ci_leading_chg3m"] > 0

    # セクターマージ
    prices = prices.merge(meta[["ticker", "sectors"]], on="ticker", how="left")

    print(f"  銘柄数: {prices['ticker'].nunique()}")
    print(f"  期間: {prices['date'].min().date()} ~ {prices['date'].max().date()}")

    # --- シグナル検出 ---
    print("\n[2] シグナル検出...")
    prices = detect_signals(prices)

    signal_names = {
        "sig_A": "A_dip_buy（押し目買い）",
        "sig_B": "B_sma_support（SMA支持反発）",
        "sig_C": "C_mini_gc（ミニGC）",
        "sig_D": "D_deep_dip（深押し買い）",
    }

    for col, name in signal_names.items():
        cnt = prices[col].sum()
        print(f"  {name}: {cnt:,}")

    # --- フィルター適用 ---
    print("\n[3] フィルター適用: uptrend + CI拡大...")
    filtered = prices[
        (prices["market_uptrend"] == True) &
        (prices["macro_ci_expand"] == True)
    ].copy()
    print(f"  フィルター後レコード: {len(filtered):,} / {len(prices):,}")

    # ============================================================
    # 分析1: 全4シグナル × IFO比較
    # ============================================================
    print(f"\n{'='*72}")
    print("分析1: 全4 buyシグナル × IFO(SL-2.5%/TP+3.0%/5日)")
    print(f"{'='*72}")

    # フィルターなし vs uptrend+CI の両方で比較
    for label, dataset in [("ベースライン（フィルターなし）", prices),
                           ("uptrend + CI拡大", filtered)]:
        print(f"\n--- {label} ---")
        print(f"  {'シグナル':<28s} {'件数':>6s} {'平均%':>7s} {'勝率':>6s} {'PF':>6s} "
              f"{'TP%':>5s} {'SL%':>5s} {'期限%':>5s} {'平均日':>5s}")
        print(f"  {'-'*78}")

        for col, name in signal_names.items():
            sig_rows = dataset[dataset[col]].copy()
            if len(sig_rows) == 0:
                print(f"  {name:<28s}      0")
                continue
            ifo = simulate_ifo(prices, sig_rows)
            st = ifo_stats(ifo)
            if st is None:
                print(f"  {name:<28s}  データ不足")
                continue
            print(
                f"  {name:<28s} {st['n']:>6,d} {st['mean']:>+7.3f} "
                f"{st['win%']:>5.1f}% {st['pf']:>5.2f} "
                f"{st['tp_hit%']:>4.1f}% {st['sl_hit%']:>4.1f}% "
                f"{st['expire%']:>4.1f}% {st['avg_days']:>4.1f}"
            )

    # 年間シグナル数の推定
    print(f"\n--- 年間シグナル数（uptrend+CI、直近5年平均） ---")
    recent = filtered[filtered["date"] >= "2021-01-01"]
    years = (recent["date"].max() - recent["date"].min()).days / 365.25
    if years > 0:
        for col, name in signal_names.items():
            cnt = recent[col].sum()
            per_year = cnt / years
            print(f"  {name:<28s} {cnt:>5} 回 / {years:.1f}年 = {per_year:.0f} 回/年")

    # ============================================================
    # 分析2: セクター別パフォーマンス
    # ============================================================
    print(f"\n{'='*72}")
    print("分析2: セクター別パフォーマンス（uptrend+CI、全シグナル合算）")
    print(f"{'='*72}")

    # 全シグナルを1つにまとめる
    filtered_any = filtered[
        filtered["sig_A"] | filtered["sig_B"] | filtered["sig_C"] | filtered["sig_D"]
    ].copy()
    filtered_any["signal_label"] = ""
    filtered_any.loc[filtered_any["sig_A"], "signal_label"] = "A"
    filtered_any.loc[filtered_any["sig_B"], "signal_label"] = "B"
    filtered_any.loc[filtered_any["sig_C"], "signal_label"] = "C"
    filtered_any.loc[filtered_any["sig_D"], "signal_label"] = "D"

    ifo_all = simulate_ifo(prices, filtered_any)
    ifo_all = ifo_all.merge(
        meta[["ticker", "sectors"]].drop_duplicates(),
        on="ticker", how="left"
    )

    # セクター別集計
    print(f"\n  {'セクター':<16s} {'件数':>6s} {'平均%':>7s} {'勝率':>6s} {'PF':>6s} {'銘柄数':>5s}")
    print(f"  {'-'*50}")

    sector_results = []
    for sector in sorted(ifo_all["sectors"].dropna().unique()):
        sec_df = ifo_all[ifo_all["sectors"] == sector]
        st = ifo_stats(sec_df)
        if st is None:
            continue
        n_tickers = sec_df["ticker"].nunique()
        sector_results.append({
            "sector": sector,
            **st,
            "n_tickers": n_tickers,
        })

    sector_results = sorted(sector_results, key=lambda x: x["mean"], reverse=True)
    for r in sector_results:
        print(
            f"  {r['sector']:<16s} {r['n']:>6,d} {r['mean']:>+7.3f} "
            f"{r['win%']:>5.1f}% {r['pf']:>5.02f} {r['n_tickers']:>5d}"
        )

    # PF >= 1.3 のセクター
    good_sectors = [r["sector"] for r in sector_results if r["pf"] >= 1.3]
    bad_sectors = [r["sector"] for r in sector_results if r["pf"] < 1.0]
    print(f"\n  PF >= 1.3 のセクター: {good_sectors}")
    print(f"  PF < 1.0 のセクター（除外候補）: {bad_sectors}")

    # ============================================================
    # 分析3: セクター別 × シグナル別クロス集計
    # ============================================================
    print(f"\n{'='*72}")
    print("分析3: セクター × シグナル別クロス集計（uptrend+CI）")
    print(f"{'='*72}")

    for col, name in signal_names.items():
        sig_rows = filtered[filtered[col]].copy()
        if len(sig_rows) == 0:
            continue
        ifo = simulate_ifo(prices, sig_rows)
        ifo = ifo.merge(meta[["ticker", "sectors"]].drop_duplicates(), on="ticker", how="left")

        print(f"\n  --- {name} ---")
        print(f"  {'セクター':<16s} {'件数':>6s} {'平均%':>7s} {'勝率':>6s} {'PF':>6s}")
        print(f"  {'-'*42}")

        cross = []
        for sector in sorted(ifo["sectors"].dropna().unique()):
            sec_df = ifo[ifo["sectors"] == sector]
            st = ifo_stats(sec_df)
            if st is None:
                continue
            cross.append({"sector": sector, **st})
        cross = sorted(cross, key=lambda x: x["mean"], reverse=True)
        for r in cross:
            print(
                f"  {r['sector']:<16s} {r['n']:>6,d} {r['mean']:>+7.3f} "
                f"{r['win%']:>5.1f}% {r['pf']:>5.02f}"
            )

    # ============================================================
    # 分析4: 良セクター限定のIFO再検証
    # ============================================================
    if good_sectors:
        print(f"\n{'='*72}")
        print(f"分析4: 良セクター限定（PF>=1.3）× 全シグナルIFO")
        print(f"対象: {good_sectors}")
        print(f"{'='*72}")

        good_filtered = filtered[filtered["sectors"].isin(good_sectors)]

        print(f"\n  {'シグナル':<28s} {'件数':>6s} {'平均%':>7s} {'勝率':>6s} {'PF':>6s} "
              f"{'TP%':>5s} {'SL%':>5s}")
        print(f"  {'-'*65}")

        for col, name in signal_names.items():
            sig_rows = good_filtered[good_filtered[col]].copy()
            if len(sig_rows) == 0:
                continue
            ifo = simulate_ifo(prices, sig_rows)
            st = ifo_stats(ifo)
            if st is None:
                continue
            print(
                f"  {name:<28s} {st['n']:>6,d} {st['mean']:>+7.3f} "
                f"{st['win%']:>5.1f}% {st['pf']:>5.02f} "
                f"{st['tp_hit%']:>4.1f}% {st['sl_hit%']:>4.1f}%"
            )

        # 全シグナル合算
        good_any = good_filtered[
            good_filtered["sig_A"] | good_filtered["sig_B"] |
            good_filtered["sig_C"] | good_filtered["sig_D"]
        ]
        ifo_good_all = simulate_ifo(prices, good_any)
        st_all = ifo_stats(ifo_good_all)
        if st_all:
            print(f"\n  {'全シグナル合算':<28s} {st_all['n']:>6,d} {st_all['mean']:>+7.3f} "
                  f"{st_all['win%']:>5.1f}% {st_all['pf']:>5.02f} "
                  f"{st_all['tp_hit%']:>4.1f}% {st_all['sl_hit%']:>4.1f}%")

    # ============================================================
    # 分析5: 悪セクター除外のIFO再検証
    # ============================================================
    if bad_sectors:
        print(f"\n{'='*72}")
        print(f"分析5: 悪セクター除外（PF<1.0）× 全シグナルIFO")
        print(f"除外: {bad_sectors}")
        print(f"{'='*72}")

        excl_filtered = filtered[~filtered["sectors"].isin(bad_sectors)]

        print(f"\n  {'シグナル':<28s} {'件数':>6s} {'平均%':>7s} {'勝率':>6s} {'PF':>6s} "
              f"{'TP%':>5s} {'SL%':>5s}")
        print(f"  {'-'*65}")

        for col, name in signal_names.items():
            sig_rows = excl_filtered[excl_filtered[col]].copy()
            if len(sig_rows) == 0:
                continue
            ifo = simulate_ifo(prices, sig_rows)
            st = ifo_stats(ifo)
            if st is None:
                continue
            print(
                f"  {name:<28s} {st['n']:>6,d} {st['mean']:>+7.3f} "
                f"{st['win%']:>5.1f}% {st['pf']:>5.02f} "
                f"{st['tp_hit%']:>4.1f}% {st['sl_hit%']:>4.1f}%"
            )

        excl_any = excl_filtered[
            excl_filtered["sig_A"] | excl_filtered["sig_B"] |
            excl_filtered["sig_C"] | excl_filtered["sig_D"]
        ]
        ifo_excl_all = simulate_ifo(prices, excl_any)
        st_all = ifo_stats(ifo_excl_all)
        if st_all:
            print(f"\n  {'全シグナル合算':<28s} {st_all['n']:>6,d} {st_all['mean']:>+7.3f} "
                  f"{st_all['win%']:>5.1f}% {st_all['pf']:>5.02f} "
                  f"{st_all['tp_hit%']:>4.1f}% {st_all['sl_hit%']:>4.1f}%")

    print(f"\n{'='*72}")
    print("完了")
    print(f"{'='*72}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
