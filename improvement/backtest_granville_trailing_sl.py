#!/usr/bin/env python3
"""
グランビルIFD トレイリングSL検証
================================
1時間足データ（3年分）を使い、動的SL戦略をバックテスト

比較戦略:
A) Baseline: 固定SL -3.5%, 7日引け決済
B) Day1 early cut: Day1で-1.5%下げたら即撤退
C) Trailing breakeven: +1.5%到達でSLを建値に引き上げ
D) Progressive trailing:
   - デフォルトSL: -3.5%
   - max gain >= +1% → SL = -1%
   - max gain >= +2% → SL = 0%（建値）
   - max gain >= +3% → SL = +1%
E) Day1確認 + Progressive:
   - Day1引けで-1%以下 → 翌日寄りで撤退
   - それ以外はDと同じ
"""
from __future__ import annotations

import sys
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MACRO_DIR = ROOT / "improvement" / "data" / "macro"
BAD_SECTORS = ["医薬品", "輸送用機器", "小売業", "その他製品", "陸運業", "サービス業"]

HOLD_DAYS = 7


def notify_slack(message: str):
    try:
        from dotenv import dotenv_values
        env = dotenv_values(ROOT / ".env.slack")
        url = env.get("SLACK_WEBHOOK_CLAUDE", "")
        if not url:
            return
        import urllib.request
        data = json.dumps({"text": message}).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req)
    except Exception as e:
        print(f"[WARN] Slack failed: {e}")


def _rsi(series, period):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def load_signals():
    """日足からグランビルシグナルを生成（1時間足の期間に限定）"""
    print("[1/3] Loading data and generating signals...")
    t0 = time.time()

    m = pd.read_parquet(ROOT / "data/parquet/meta.parquet")
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)
    tickers = m["ticker"].tolist()

    p = pd.read_parquet(ROOT / "data/parquet/prices_max_1d.parquet")
    ps = p[p["ticker"].isin(tickers)].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    # 1時間足の期間に限定
    h1 = pd.read_parquet(ROOT / "data/parquet/prices_730d_1h.parquet")
    h1["date"] = pd.to_datetime(h1["date"])
    h1_min_date = h1["date"].min().normalize()
    print(f"  1h data starts: {h1_min_date.date()}")

    # Index
    idx = pd.read_parquet(ROOT / "data/parquet/index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").rename(columns={"Close": "nk225_close"})
    nk["nk225_sma20"] = nk["nk225_close"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225_close"] > nk["nk225_sma20"]

    # CI
    ci = pd.read_parquet(MACRO_DIR / "estat_ci_index.parquet")
    ci = ci[["date", "leading"]].rename(columns={"leading": "ci_leading"})
    ci["ci_leading_chg3m"] = ci["ci_leading"].diff(3)
    daily = pd.DataFrame({"date": ps["date"].drop_duplicates().sort_values()})
    daily = daily.merge(ci, on="date", how="left").sort_values("date").ffill()

    # SMA
    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)
    ps["prev_close"] = g["Close"].shift(1)
    ps = ps.dropna(subset=["sma20"])

    ps = ps.merge(nk[["date", "market_uptrend"]], on="date", how="left")
    ps = ps.merge(daily, on="date", how="left")
    ps["macro_ci_expand"] = ps["ci_leading_chg3m"] > 0
    ps = ps.merge(m[["ticker", "sectors"]], on="ticker", how="left")

    # シグナル検出
    dev = ps["dev_from_sma20"]
    ps["sig_A"] = (dev.between(-8, -3)) & (ps["Close"] > ps["prev_close"])
    ps["sig_B"] = (
        ps["sma20_up"] & (ps["Close"] > ps["sma20"]) &
        (dev.between(0, 2)) & (ps["prev_dev"] <= 0.5) &
        (ps["Close"] > ps["prev_close"])
    )

    mask = (
        (ps["market_uptrend"] == True) &
        (ps["macro_ci_expand"] == True) &
        (~ps["sectors"].isin(BAD_SECTORS))
    )
    sig = ps[mask & (ps["sig_A"] | ps["sig_B"])].copy()
    sig["signal_type"] = "A"
    sig.loc[sig["sig_B"], "signal_type"] = "B"
    sig.loc[sig["sig_A"] & sig["sig_B"], "signal_type"] = "A+B"

    # 1時間足期間に限定（SMA計算に20日必要なので少し余裕を持たせて）
    sig = sig[sig["date"] >= h1_min_date + pd.Timedelta(days=30)]

    print(f"  Signals in 1h period: {len(sig):,}")
    print(f"  Date range: {sig['date'].min().date()} ~ {sig['date'].max().date()}")
    print(f"  Done in {time.time()-t0:.1f}s")
    return sig, ps, h1


def simulate_strategies(signals, daily_prices, hourly_prices):
    """各戦略をシミュレーション"""
    print("[2/3] Simulating strategies with 1h data...")
    t0 = time.time()

    # 1時間足を日付でインデックス化
    h1 = hourly_prices.copy()
    h1["trade_date"] = h1["date"].dt.normalize()

    strategies = {
        "A_baseline_sl35": {"sl_pct": 3.5, "trailing": None, "day1_cut": None},
        "B_day1_cut_15": {"sl_pct": 3.5, "trailing": None, "day1_cut": -1.5},
        "C_trail_breakeven": {"sl_pct": 3.5, "trailing": [(1.5, 0.0)], "day1_cut": None},
        "D_progressive": {"sl_pct": 3.5, "trailing": [(1.0, -1.0), (2.0, 0.0), (3.0, 1.0)], "day1_cut": None},
        "E_day1_confirm_progressive": {"sl_pct": 3.5, "trailing": [(1.0, -1.0), (2.0, 0.0), (3.0, 1.0)], "day1_cut": -1.0},
        "F_tight_sl25": {"sl_pct": 2.5, "trailing": None, "day1_cut": None},
        "G_tight_progressive": {"sl_pct": 2.5, "trailing": [(0.5, -0.5), (1.5, 0.0), (2.5, 1.0)], "day1_cut": None},
    }

    all_results = {}

    for strat_name, params in strategies.items():
        results = []

        for ticker in signals["ticker"].unique():
            # 日足データ
            tk_daily = daily_prices[daily_prices["ticker"] == ticker].sort_values("date")
            daily_dates = tk_daily["date"].values
            daily_opens = tk_daily["Open"].values
            daily_closes = tk_daily["Close"].values
            daily_date_idx = {d: i for i, d in enumerate(daily_dates)}

            # 1時間足データ
            tk_h1 = h1[h1["ticker"] == ticker].sort_values("date")

            sig_rows = signals[signals["ticker"] == ticker]
            for _, row in sig_rows.iterrows():
                sd = row["date"]
                if sd not in daily_date_idx:
                    continue
                idx = daily_date_idx[sd]
                if idx + 1 >= len(daily_dates):
                    continue

                entry_idx = idx + 1
                entry_price = daily_opens[entry_idx]
                if np.isnan(entry_price) or entry_price <= 0:
                    continue

                entry_date = daily_dates[entry_idx]

                # 保有期間の日足日付リスト
                hold_dates = []
                for d in range(HOLD_DAYS):
                    di = entry_idx + d
                    if di < len(daily_dates):
                        hold_dates.append(daily_dates[di])

                if not hold_dates:
                    continue

                # 1時間足でシミュレーション
                sl_pct = params["sl_pct"]
                current_sl = entry_price * (1 - sl_pct / 100)
                max_gain_pct = 0.0
                exit_price = None
                exit_type = None
                exit_day = 0

                for day_num, trade_date in enumerate(hold_dates):
                    # この日の1時間足
                    day_h1 = tk_h1[tk_h1["trade_date"] == trade_date].sort_values("date")

                    if len(day_h1) == 0:
                        # 1時間足がない場合は日足で代替
                        di = entry_idx + day_num
                        if di < len(daily_dates):
                            low = tk_daily.iloc[di]["Low"]
                            high = tk_daily.iloc[di]["High"]
                            close = tk_daily.iloc[di]["Close"]

                            if low <= current_sl:
                                exit_price = current_sl
                                exit_type = "SL"
                                exit_day = day_num
                                break

                            gain_pct = (high / entry_price - 1) * 100
                            if gain_pct > max_gain_pct:
                                max_gain_pct = gain_pct

                            # トレイリング更新
                            if params["trailing"]:
                                for threshold, new_sl_pct in sorted(params["trailing"], reverse=True):
                                    if max_gain_pct >= threshold:
                                        new_sl = entry_price * (1 + new_sl_pct / 100)
                                        if new_sl > current_sl:
                                            current_sl = new_sl
                                        break
                        continue

                    for _, bar in day_h1.iterrows():
                        # SL判定（安値で判定）
                        if bar["Low"] <= current_sl:
                            exit_price = current_sl
                            exit_type = "SL"
                            exit_day = day_num
                            break

                        # 最大利益更新
                        gain_pct = (bar["High"] / entry_price - 1) * 100
                        if gain_pct > max_gain_pct:
                            max_gain_pct = gain_pct

                        # トレイリング更新
                        if params["trailing"]:
                            for threshold, new_sl_pct in sorted(params["trailing"], reverse=True):
                                if max_gain_pct >= threshold:
                                    new_sl = entry_price * (1 + new_sl_pct / 100)
                                    if new_sl > current_sl:
                                        current_sl = new_sl
                                    break

                    if exit_price is not None:
                        break

                    # Day1確認ルール
                    if day_num == 0 and params["day1_cut"] is not None:
                        di = entry_idx + day_num
                        if di < len(daily_dates):
                            day1_close = tk_daily.iloc[di]["Close"]
                            day1_ret = (day1_close / entry_price - 1) * 100
                            if day1_ret <= params["day1_cut"]:
                                # 翌日寄りで撤退
                                if entry_idx + 1 < len(daily_dates):
                                    exit_price = daily_opens[entry_idx + 1]
                                    exit_type = "day1_cut"
                                    exit_day = 1
                                    break

                # 保有期間満了
                if exit_price is None:
                    last_di = entry_idx + HOLD_DAYS - 1
                    if last_di < len(daily_dates):
                        exit_price = daily_closes[last_di]
                    else:
                        exit_price = daily_closes[-1]
                    exit_type = "expire"
                    exit_day = HOLD_DAYS - 1

                ret_pct = (exit_price / entry_price - 1) * 100
                pnl = int(round(entry_price * 100 * ret_pct / 100))

                results.append({
                    "date": sd,
                    "ticker": row["ticker"],
                    "signal_type": row["signal_type"],
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "ret_pct": ret_pct,
                    "pnl": pnl,
                    "exit_type": exit_type,
                    "exit_day": exit_day,
                    "max_gain_pct": max_gain_pct,
                    "win": ret_pct > 0,
                })

        df = pd.DataFrame(results)
        all_results[strat_name] = df

    print(f"  Done in {time.time()-t0:.1f}s")
    return all_results


def analyze_results(all_results):
    """結果を比較分析"""
    print("\n[3/3] Results comparison")
    print("=" * 100)

    header = f"{'戦略':<35s} {'件数':>5s} {'勝率':>6s} {'平均%':>7s} {'PF':>6s} {'PnL(万)':>8s} {'SL%':>5s} {'day1cut%':>8s} {'trail%':>6s} {'expire%':>7s}"
    print(header)
    print("-" * 100)

    summaries = []
    for name, df in all_results.items():
        if len(df) == 0:
            continue
        n = len(df)
        wr = df["win"].mean() * 100
        mean_ret = df["ret_pct"].mean()
        wins = df[df["ret_pct"] > 0]["ret_pct"].sum()
        losses = abs(df[df["ret_pct"] <= 0]["ret_pct"].sum())
        pf = round(wins / losses, 2) if losses > 0 else 999
        total_pnl = df["pnl"].sum()
        sl_rate = (df["exit_type"] == "SL").mean() * 100
        d1_rate = (df["exit_type"] == "day1_cut").mean() * 100
        trail_rate = (df["exit_type"].isin(["trail_SL"])).mean() * 100
        expire_rate = (df["exit_type"] == "expire").mean() * 100

        marker = ""
        if pf >= 2.0:
            marker = " ★★"
        elif pf >= 1.5:
            marker = " ★"

        print(f"{name:<35s} {n:>5d} {wr:>5.1f}% {mean_ret:>+6.2f}% {pf:>5.2f} {total_pnl/10000:>+7.1f} "
              f"{sl_rate:>4.1f}% {d1_rate:>7.1f}% {trail_rate:>5.1f}% {expire_rate:>6.1f}%{marker}")

        summaries.append({"name": name, "n": n, "wr": wr, "pf": pf, "pnl": total_pnl, "mean_ret": mean_ret})

    # 月別比較（ベースラインvs最良戦略）
    baseline = all_results.get("A_baseline_sl35")
    if baseline is not None and len(baseline) > 0:
        print(f"\n{'='*80}")
        print("月別比較: Baseline vs Progressive Trailing")
        print(f"{'='*80}")

        best_name = max(summaries, key=lambda x: x["pf"])["name"]
        best = all_results[best_name]

        baseline["month"] = pd.to_datetime(baseline["date"]).dt.to_period("M")
        best["month"] = pd.to_datetime(best["date"]).dt.to_period("M")

        months = sorted(set(baseline["month"].unique()) | set(best["month"].unique()))

        print(f"{'月':>10s} {'Base件数':>6s} {'BasePnL':>8s} {'BasePF':>6s} "
              f"{'Best件数':>6s} {'BestPnL':>8s} {'BestPF':>6s} {'diff':>8s}")
        print("-" * 70)

        for month in months:
            b = baseline[baseline["month"] == month]
            t = best[best["month"] == month]

            b_pnl = b["pnl"].sum() if len(b) > 0 else 0
            t_pnl = t["pnl"].sum() if len(t) > 0 else 0

            b_wins = b[b["ret_pct"] > 0]["ret_pct"].sum() if len(b) > 0 else 0
            b_losses = abs(b[b["ret_pct"] <= 0]["ret_pct"].sum()) if len(b) > 0 else 1
            b_pf = round(b_wins / b_losses, 2) if b_losses > 0 else 999

            t_wins = t[t["ret_pct"] > 0]["ret_pct"].sum() if len(t) > 0 else 0
            t_losses = abs(t[t["ret_pct"] <= 0]["ret_pct"].sum()) if len(t) > 0 else 1
            t_pf = round(t_wins / t_losses, 2) if t_losses > 0 else 999

            diff = t_pnl - b_pnl
            print(f"{str(month):>10s} {len(b):>6d} {b_pnl:>+8,d} {b_pf:>5.2f} "
                  f"{len(t):>6d} {t_pnl:>+8,d} {t_pf:>5.02f} {diff:>+8,d}")

    # exit_type別の損益分布
    print(f"\n{'='*80}")
    print("Exit Type別 損益分布（全戦略）")
    print(f"{'='*80}")
    for name, df in all_results.items():
        if len(df) == 0:
            continue
        print(f"\n  --- {name} ---")
        for et in df["exit_type"].unique():
            sub = df[df["exit_type"] == et]
            print(f"  {et:<12s}: {len(sub):>4d}件 avg={sub['ret_pct'].mean():>+5.2f}% "
                  f"median={sub['ret_pct'].median():>+5.2f}% pnl=¥{sub['pnl'].sum():>+,}")

    return summaries


def main():
    print("=" * 80)
    print("Granville IFD Trailing SL Backtest (1h data)")
    print("=" * 80)
    t_start = time.time()

    try:
        signals, daily, hourly = load_signals()
        all_results = simulate_strategies(signals, daily, hourly)
        summaries = analyze_results(all_results)

        elapsed = time.time() - t_start

        base = next((s for s in summaries if s["name"] == "A_baseline_sl35"), None)
        best = max(summaries, key=lambda x: x["pf"])

        msg = (
            f"Granville Trailing SL 検証完了\n\n"
            f"Baseline (固定SL-3.5%):\n"
            f"  {base['n']}件, 勝率{base['wr']:.1f}%, PF{base['pf']:.2f}, PnL¥{base['pnl']:+,}\n\n"
            f"Best ({best['name']}):\n"
            f"  {best['n']}件, 勝率{best['wr']:.1f}%, PF{best['pf']:.2f}, PnL¥{best['pnl']:+,}\n\n"
            f"処理時間: {elapsed:.0f}秒"
        )
        print(f"\n{'='*80}")
        print(msg)
        print(f"{'='*80}")
        notify_slack(msg)

    except Exception as e:
        import traceback
        msg = f"Granville Trailing SL 検証失敗\n```\n{traceback.format_exc()}\n```"
        print(msg)
        notify_slack(msg)
        raise


if __name__ == "__main__":
    main()
