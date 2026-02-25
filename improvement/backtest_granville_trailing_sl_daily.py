#!/usr/bin/env python3
"""
グランビルIFD トレイリングSL検証（日足・グランビル出口ベース）
=============================================================
本番パイプライン（backtest_granville_ifd.py）と同じグランビル出口ルールを使用。

出口ルール（原理原則）:
  TP: 高値≥エントリー+10% → その日にTP（ザラ場自動執行）
  A: 終値≥SMA20 → 翌日寄付売り（SMA20回帰）
  B: SMA5がSMA20を下抜け → 翌日寄付売り（デッドクロス）
  7日マイナス: 7日目終値 < エントリー → 翌日寄付売り
  SL: Low ≤ SL価格 → 逆指値（ザラ場自動執行）
  最大60営業日

トレイリングSLの追加効果:
  引け後に終値ベースで含み益を確認し、SLを翌日から引き上げる
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

MAX_HOLD_DAYS = 60
TP_PCT = 10.0


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


def load_signals():
    """日足からグランビルシグナル生成（26年分）"""
    print("[1/3] Loading data...")
    t0 = time.time()

    m = pd.read_parquet(ROOT / "data/parquet/meta.parquet")
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)
    tickers = m["ticker"].tolist()

    p = pd.read_parquet(ROOT / "data/parquet/prices_max_1d.parquet")
    ps = p[p["ticker"].isin(tickers)].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    idx = pd.read_parquet(ROOT / "data/parquet/index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").dropna(subset=["Close"]).rename(columns={"Close": "nk225_close"})
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
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)
    ps["prev_close"] = g["Close"].shift(1)
    ps = ps.dropna(subset=["sma20"])

    ps = ps.merge(nk[["date", "market_uptrend"]], on="date", how="left")
    ps = ps.merge(daily, on="date", how="left")
    ps["macro_ci_expand"] = ps["ci_leading_chg3m"] > 0
    ps = ps.merge(m[["ticker", "sectors"]], on="ticker", how="left")

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

    print(f"  Signals: {len(sig):,}")
    print(f"  Date range: {sig['date'].min().date()} ~ {sig['date'].max().date()}")
    print(f"  Done in {time.time()-t0:.1f}s")
    return sig, ps


def simulate_granville_trailing(daily_prices, signals, strat_name, sl_pct,
                                trailing_rules=None, use_tp=True):
    """
    グランビル出口ルール + トレイリングSL

    出口の優先順位（本番パイプラインと同じ）:
    1. SL: Low <= SL価格（ザラ場逆指値）
    2. TP: High >= エントリー+10%（ザラ場利確）※use_tp=Trueの場合
    3. A: 終値 >= SMA20（SMA20回帰）→ 翌日寄付売り
    4. B: SMA5がSMA20を下抜け（デッドクロス）→ 翌日寄付売り
    5. 7日マイナス: d==6で終値 < エントリー → 翌日寄付売り
    6. 最大60営業日 → 翌日寄付売り

    trailing_rules: [(gain_threshold_pct, new_sl_pct), ...]
        終値ベースで含み益がgain_threshold以上になったら、
        翌日からSLをentry_price * (1 + new_sl_pct/100)に引き上げ
    """
    results = []

    for ticker in signals["ticker"].unique():
        tk = daily_prices[daily_prices["ticker"] == ticker].sort_values("date")
        dates = tk["date"].values
        opens = tk["Open"].values
        highs = tk["High"].values
        lows = tk["Low"].values
        closes = tk["Close"].values
        sma5s = tk["sma5"].values
        sma20s = tk["sma20"].values
        date_idx = {d: i for i, d in enumerate(dates)}

        sig_rows = signals[signals["ticker"] == ticker]
        for _, row in sig_rows.iterrows():
            sd = row["date"]
            sig_type = row["signal_type"]
            if sd not in date_idx:
                continue
            idx = date_idx[sd]
            if idx + 1 >= len(dates):
                continue

            entry_idx = idx + 1
            entry_price = float(opens[entry_idx])
            if np.isnan(entry_price) or entry_price <= 0:
                continue

            current_sl = entry_price * (1 - sl_pct / 100)
            tp_price = entry_price * (1 + TP_PCT / 100) if use_tp else None
            max_close_gain_pct = 0.0
            exit_price = None
            exit_type = None
            exit_day = 0
            pending_sl = None

            for d in range(MAX_HOLD_DAYS):
                ci = entry_idx + d
                if ci >= len(dates):
                    break

                # 翌日適用のSLがあれば反映
                if pending_sl is not None:
                    current_sl = pending_sl
                    pending_sl = None

                # 1. SL判定（Lowベース = ザラ場逆指値）
                if float(lows[ci]) <= current_sl:
                    exit_price = current_sl
                    exit_type = "SL"
                    exit_day = d
                    break

                # 2. TP判定（Highベース = ザラ場利確）
                if tp_price and float(highs[ci]) >= tp_price:
                    exit_price = tp_price
                    exit_type = "TP"
                    exit_day = d
                    break

                # エントリー日はグランビル出口チェックしない
                if d == 0:
                    # トレイリング更新だけ行う
                    close_gain = (float(closes[ci]) / entry_price - 1) * 100
                    if close_gain > max_close_gain_pct:
                        max_close_gain_pct = close_gain
                    if trailing_rules:
                        for thr, new_sl_pct in sorted(trailing_rules, reverse=True):
                            if max_close_gain_pct >= thr:
                                new_sl = entry_price * (1 + new_sl_pct / 100)
                                if new_sl > current_sl and (pending_sl is None or new_sl > pending_sl):
                                    pending_sl = new_sl
                                break
                    continue

                close_val = float(closes[ci])
                sma5_val = float(sma5s[ci])
                sma20_val = float(sma20s[ci])

                # 3. A: 終値 >= SMA20 → 翌日寄付売り
                if sig_type in ("A", "A+B") and close_val >= sma20_val:
                    if ci + 1 >= len(dates):
                        break
                    exit_price = float(opens[ci + 1])
                    exit_type = "SMA20_touch"
                    exit_day = d + 1
                    break

                # 4. デッドクロス（SMA5がSMA20を上→下）→ 翌日寄付売り
                prev_sma5 = float(sma5s[ci - 1])
                prev_sma20 = float(sma20s[ci - 1])
                if prev_sma5 >= prev_sma20 and sma5_val < sma20_val:
                    if ci + 1 >= len(dates):
                        break
                    exit_price = float(opens[ci + 1])
                    exit_type = "dead_cross"
                    exit_day = d + 1
                    break

                # 5. 7日マイナス損切り（d==6で終値 < エントリー）
                if d == 6 and close_val < entry_price:
                    if ci + 1 >= len(dates):
                        break
                    exit_price = float(opens[ci + 1])
                    exit_type = "time_cut"
                    exit_day = d + 1
                    break

                # 6. 最大保有日数
                if d == MAX_HOLD_DAYS - 1:
                    if ci + 1 >= len(dates):
                        break
                    exit_price = float(opens[ci + 1])
                    exit_type = "expire"
                    exit_day = d + 1
                    break

                # トレイリングSL更新（引け後に終値ベース）
                close_gain = (close_val / entry_price - 1) * 100
                if close_gain > max_close_gain_pct:
                    max_close_gain_pct = close_gain

                if trailing_rules:
                    for thr, new_sl_pct in sorted(trailing_rules, reverse=True):
                        if max_close_gain_pct >= thr:
                            new_sl = entry_price * (1 + new_sl_pct / 100)
                            if new_sl > current_sl and (pending_sl is None or new_sl > pending_sl):
                                pending_sl = new_sl
                            break

            if exit_price is None:
                continue  # データ不足で未完了

            ret_pct = (exit_price / entry_price - 1) * 100
            pnl = int(round(entry_price * 100 * ret_pct / 100))

            results.append({
                "date": sd,
                "ticker": row["ticker"],
                "signal_type": sig_type,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "ret_pct": ret_pct,
                "pnl": pnl,
                "exit_type": exit_type,
                "exit_day": exit_day,
                "max_close_gain_pct": max_close_gain_pct,
                "win": ret_pct > 0,
            })

    return pd.DataFrame(results)


def main():
    print("=" * 90)
    print("Granville IFD Trailing SL Backtest（グランビル出口ベース, 26年分）")
    print("出口: TP+10% / A:SMA20回帰 / B:デッドクロス / 7日マイナス損切り / 最大60日")
    print("SL判定=Low(逆指値), トレイリング更新=Close(引け後), 翌日適用")
    print("=" * 90)
    t_start = time.time()

    try:
        signals, daily = load_signals()

        # ベースライン = 本番と同じ（グランビル出口 + SL-3.5% + TP+10%）
        # トレイリング戦略はその上にSL引き上げを追加
        strategies = {
            "A_baseline_sl35": {"sl_pct": 3.5, "trailing": None, "use_tp": True},
            "B_no_tp": {"sl_pct": 3.5, "trailing": None, "use_tp": False},
            "C_trail_breakeven": {"sl_pct": 3.5, "trailing": [(1.5, 0.0)], "use_tp": True},
            "D_progressive": {"sl_pct": 3.5, "trailing": [(1.0, -1.0), (2.0, 0.0), (3.0, 1.0)], "use_tp": True},
            "E_tight_sl25": {"sl_pct": 2.5, "trailing": None, "use_tp": True},
            "F_tight_progressive": {"sl_pct": 2.5, "trailing": [(0.5, -0.5), (1.5, 0.0), (2.5, 1.0)], "use_tp": True},
            "G_sl30_progressive": {"sl_pct": 3.0, "trailing": [(1.0, -0.5), (2.0, 0.0), (3.0, 1.0)], "use_tp": True},
            "H_no_tp_progressive": {"sl_pct": 3.5, "trailing": [(1.0, -1.0), (2.0, 0.0), (3.0, 1.0)], "use_tp": False},
        }

        print(f"\n[2/3] Simulating {len(strategies)} strategies...")
        t0 = time.time()
        all_results = {}
        for name, params in strategies.items():
            df = simulate_granville_trailing(
                daily, signals, name,
                params["sl_pct"], params.get("trailing"), params.get("use_tp", True)
            )
            all_results[name] = df
        print(f"  Done in {time.time()-t0:.1f}s")

        # === 結果比較 ===
        print(f"\n[3/3] Results（グランビル出口ベース, 26年分）")
        print("=" * 120)
        print(f"{'戦略':<25s} {'件数':>5s} {'勝率':>6s} {'平均%':>7s} {'PF':>6s} {'PnL(万)':>8s} "
              f"{'SL%':>5s} {'TP%':>4s} {'SMA20%':>6s} {'DC%':>4s} {'TC%':>4s} {'exp%':>4s} {'avg日':>5s}")
        print("-" * 120)

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
            tp_rate = (df["exit_type"] == "TP").mean() * 100
            sma20_rate = (df["exit_type"] == "SMA20_touch").mean() * 100
            dc_rate = (df["exit_type"] == "dead_cross").mean() * 100
            tc_rate = (df["exit_type"] == "time_cut").mean() * 100
            exp_rate = (df["exit_type"] == "expire").mean() * 100
            avg_hold = df["exit_day"].mean()

            marker = " ★★" if pf >= 2.0 else " ★" if pf >= 1.6 else ""

            print(f"{name:<25s} {n:>5d} {wr:>5.1f}% {mean_ret:>+6.2f}% {pf:>5.2f} "
                  f"{total_pnl/10000:>+7.1f} {sl_rate:>4.1f}% {tp_rate:>3.1f}% "
                  f"{sma20_rate:>5.1f}% {dc_rate:>3.1f}% {tc_rate:>3.1f}% "
                  f"{exp_rate:>3.1f}% {avg_hold:>4.1f}{marker}")

            summaries.append({"name": name, "n": n, "wr": wr, "pf": pf, "pnl": total_pnl})

        # === Exit Type別 損益 ===
        print(f"\n{'='*90}")
        print("Exit Type別 損益（全期間）")
        print("=" * 90)
        for name in ["A_baseline_sl35", "B_no_tp", "F_tight_progressive", "H_no_tp_progressive"]:
            if name not in all_results:
                continue
            df = all_results[name]
            print(f"\n  --- {name} ---")
            for et in ["SL", "TP", "SMA20_touch", "dead_cross", "time_cut", "expire"]:
                sub = df[df["exit_type"] == et]
                if len(sub) == 0:
                    continue
                print(f"  {et:<14s}: {len(sub):>5d}件 avg={sub['ret_pct'].mean():>+6.2f}% "
                      f"avg日={sub['exit_day'].mean():>4.1f} pnl=¥{sub['pnl'].sum():>+,}")

        # === 保有日数分布 ===
        print(f"\n{'='*90}")
        print("保有日数の分布: Baseline vs Best trailing")
        print("=" * 90)
        best_name = max(summaries, key=lambda x: x["pf"])["name"]
        for name in ["A_baseline_sl35", best_name]:
            df = all_results[name]
            print(f"\n  --- {name} ---")
            hold_bins = [(0, 1, "~1日"), (2, 7, "2-7日"), (8, 14, "8-14日"),
                         (15, 30, "15-30日"), (31, 60, "31-60日")]
            for lo, hi, label in hold_bins:
                sub = df[(df["exit_day"] >= lo) & (df["exit_day"] <= hi)]
                if len(sub) == 0:
                    continue
                print(f"  {label:<8s}: {len(sub):>5d}件 ({len(sub)/len(df)*100:>4.1f}%) "
                      f"avg={sub['ret_pct'].mean():>+5.2f}% pnl=¥{sub['pnl'].sum():>+,}")

        # === 年別比較 ===
        print(f"\n{'='*90}")
        print(f"年別比較: Baseline vs Best ({best_name})")
        print("=" * 90)

        base = all_results["A_baseline_sl35"].copy()
        best = all_results[best_name].copy()
        base["year"] = pd.to_datetime(base["date"]).dt.year
        best["year"] = pd.to_datetime(best["date"]).dt.year

        print(f"{'年':>6s} {'Base件':>5s} {'BasePnL':>9s} {'BasePF':>6s} "
              f"{'Best件':>5s} {'BestPnL':>9s} {'BestPF':>6s} {'diff':>9s}")
        print("-" * 65)
        for year in sorted(set(base["year"].unique()) | set(best["year"].unique())):
            b = base[base["year"] == year]
            t = best[best["year"] == year]
            b_pnl = b["pnl"].sum()
            t_pnl = t["pnl"].sum()
            b_w = b[b["ret_pct"]>0]["ret_pct"].sum()
            b_l = abs(b[b["ret_pct"]<=0]["ret_pct"].sum())
            b_pf = round(b_w/b_l, 2) if b_l > 0 else 999
            t_w = t[t["ret_pct"]>0]["ret_pct"].sum()
            t_l = abs(t[t["ret_pct"]<=0]["ret_pct"].sum())
            t_pf = round(t_w/t_l, 2) if t_l > 0 else 999
            print(f"{year:>6d} {len(b):>5d} {b_pnl:>+9,} {b_pf:>5.02f} "
                  f"{len(t):>5d} {t_pnl:>+9,} {t_pf:>5.02f} {t_pnl-b_pnl:>+9,}")

        elapsed = time.time() - t_start
        base_s = next(s for s in summaries if s["name"] == "A_baseline_sl35")
        best_s = max(summaries, key=lambda x: x["pf"])

        msg = (
            f"Granville 日足トレイリングSL 検証完了（グランビル出口ベース, 26年分）\n\n"
            f"出口: TP+10% / A:SMA20回帰 / B:デッドクロス / 7日マイナス損切り / 最大60日\n\n"
            f"Baseline (SL-3.5% + TP+10% + グランビル出口):\n"
            f"  {base_s['n']}件, 勝率{base_s['wr']:.1f}%, PF{base_s['pf']:.2f}, PnL¥{base_s['pnl']:+,}\n\n"
            f"Best ({best_s['name']}):\n"
            f"  {best_s['n']}件, 勝率{best_s['wr']:.1f}%, PF{best_s['pf']:.2f}, PnL¥{best_s['pnl']:+,}\n\n"
            f"処理時間: {elapsed:.0f}秒"
        )
        print(f"\n{'='*90}")
        print(msg)
        print("=" * 90)
        notify_slack(msg)

    except Exception as e:
        import traceback
        msg = f"Granville 日足トレイリング 失敗\n```\n{traceback.format_exc()}\n```"
        print(msg)
        notify_slack(msg)
        raise


if __name__ == "__main__":
    main()
