#!/usr/bin/env python3
"""
SQ-4 包括的ファクター分析
- Holding period: 1泊(月曜entry) vs 2泊+(金曜/祝日前entry)
- Sector: 外需 vs 内需（東証33業種分類）
- Beta: 市場感応度（AdjC rolling regression）
- Volume: 流動性（J-Quants CLI取得）
- Selection × Factor matrix

前提: prices_topix500_oc.parquet, calendar.parquet, meta_jquants.parquet,
      futures_prices_max_1d.parquet
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from common_cfg.paths import PARQUET_DIR

PRICES_PATH = PARQUET_DIR / "prices_topix500_oc.parquet"
CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"
FUTURES_PATH = PARQUET_DIR / "futures_prices_max_1d.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"
VOLUME_PATH = PARQUET_DIR / "volume_topix500.parquet"

BACKTEST_START = "2022-04-01"
PRICE_MIN = 1000
PRICE_MAX = 20000
TOP_N = 10

# 外需セクター（海外売上比率高い傾向の業種）
GAISHU_SECTORS = {
    "電気機器", "輸送用機器", "機械", "精密機器",
    "化学", "非鉄金属", "鉄鋼", "ゴム製品",
    "海運業", "鉱業", "石油･石炭製品",
}


def load_prices():
    ps = pd.read_parquet(PRICES_PATH)
    ps["Date"] = pd.to_datetime(ps["Date"])
    ps["Code"] = ps["Code"].astype(str)
    ps = ps.sort_values(["Code", "Date"]).drop_duplicates(subset=["Code", "Date"])
    ps["prev_close"] = ps.groupby("Code")["AdjC"].shift(1)
    ps["ret_1d"] = ps["AdjC"] / ps["prev_close"] - 1
    ps["ret_5d"] = ps.groupby("Code")["AdjC"].pct_change(5)
    return ps


def load_sq_schedule():
    cal = pd.read_parquet(CALENDAR_PATH)
    cal["date"] = pd.to_datetime(cal["date"])

    ps = pd.read_parquet(PRICES_PATH)
    ps["Date"] = pd.to_datetime(ps["Date"])
    bdays = sorted(ps["Date"].unique())
    bday_set = set(bdays)
    last_bday = bdays[-1]
    start_ts = pd.Timestamp(BACKTEST_START)

    sq4_dates = cal[cal["sq4_entry"] == True]["date"].tolist()
    sq3_dates = cal[cal["sq3_exit"] == True]["date"].tolist()

    results = []
    for sq4 in sq4_dates:
        if sq4 < start_ts or sq4 not in bday_set:
            continue
        sq3_after = [d for d in sq3_dates if d > sq4]
        if not sq3_after:
            continue
        sq3 = sq3_after[0]
        if sq3 > last_bday:
            continue
        sq4_idx = bdays.index(sq4)
        if sq4_idx < 5:
            continue
        prev_day = bdays[sq4_idx - 1]
        cal_nights = (sq3 - sq4).days
        results.append({
            "sq4_entry": sq4,
            "sq3_exit": sq3,
            "prev_day": prev_day,
            "cal_nights": cal_nights,
            "multi_night": cal_nights > 1,
        })
    return results


def load_meta():
    meta = pd.read_parquet(META_PATH)
    meta["code"] = meta["code"].astype(str)
    # prices Code is 5-digit (trailing 0), meta is 4-digit
    meta["Code"] = meta["code"] + "0"
    meta["is_gaishu"] = meta["sectors"].isin(GAISHU_SECTORS)
    return meta[["Code", "stock_name", "sectors", "is_gaishu"]]


def compute_beta(ps: pd.DataFrame, window: int = 60) -> pd.DataFrame:
    """各銘柄の市場betaを日次で計算（market = 全銘柄平均ret）"""
    daily_ret = ps.pivot_table(index="Date", columns="Code", values="ret_1d")
    market_ret = daily_ret.mean(axis=1)

    betas = {}
    for code in daily_ret.columns:
        stock = daily_ret[code]
        rolling_cov = stock.rolling(window).cov(market_ret)
        rolling_var = market_ret.rolling(window).var()
        beta = rolling_cov / rolling_var
        betas[code] = beta

    beta_df = pd.DataFrame(betas)
    beta_long = beta_df.stack().reset_index()
    beta_long.columns = ["Date", "Code", "beta"]
    return beta_long


def load_cme_filter(ps, sq_schedule):
    """CME下落判定"""
    cme_df = pd.read_parquet(FUTURES_PATH)
    cme = cme_df[cme_df["ticker"] == "NKD=F"][["date", "Close"]].copy()
    cme["date"] = pd.to_datetime(cme["date"])
    cme = cme.dropna(subset=["Close"]).sort_values("date").reset_index(drop=True)
    cme_map = {row["date"]: row["Close"] for _, row in cme.iterrows()}

    bdays = sorted(ps["Date"].unique())

    def is_cme_down(prev_day):
        prev_idx = bdays.index(prev_day) if prev_day in bdays else None
        if prev_idx is None:
            return None
        fri = None
        thu = None
        for offset in range(3):
            idx = prev_idx - offset
            if idx >= 0 and bdays[idx] in cme_map:
                if fri is None:
                    fri = cme_map[bdays[idx]]
                elif thu is None:
                    thu = cme_map[bdays[idx]]
                    break
        if fri is not None and thu is not None and thu != 0:
            return fri < thu
        return None

    cme_status = {}
    for sq in sq_schedule:
        cme_status[sq["sq4_entry"]] = is_cme_down(sq["prev_day"])
    return cme_status


def calc_pf(rets):
    if not rets:
        return None
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    return round(gp / gl, 2) if gl > 0 else None


def run_backtest(ps, sq_schedule, select_fn, meta=None, beta_df=None,
                 filter_fn=None, label=""):
    """
    バックテスト実行。filter_fnは (prev_data, sq_info) → bool で
    当月をスキップするかの追加フィルタ。
    各トレードにメタ情報（sector, beta等）を付与して返す。
    """
    all_trades = []

    for sq in sq_schedule:
        if filter_fn and not filter_fn(sq):
            continue

        sq4 = sq["sq4_entry"]
        sq3 = sq["sq3_exit"]
        prev = sq["prev_day"]

        prev_data = ps[ps["Date"] == prev][["Code", "AdjC", "ret_1d", "ret_5d"]].copy()
        prev_data = prev_data.rename(columns={"AdjC": "prev_close"})
        prev_data = prev_data[
            (prev_data["prev_close"] >= PRICE_MIN) &
            (prev_data["prev_close"] <= PRICE_MAX)
        ]
        if prev_data.empty:
            continue

        picks = select_fn(prev_data)
        if picks.empty:
            continue

        codes = picks["Code"].tolist()

        entry = ps[(ps["Date"] == sq4) & (ps["Code"].isin(codes))][["Code", "AdjO"]].rename(
            columns={"AdjO": "entry_open"})
        exit_ = ps[(ps["Date"] == sq3) & (ps["Code"].isin(codes))][["Code", "AdjC"]].rename(
            columns={"AdjC": "exit_close"})

        merged = entry.merge(exit_, on="Code")
        if merged.empty:
            continue

        merged["ret_pct"] = (merged["exit_close"] / merged["entry_open"] - 1) * 100
        merged["pnl_100"] = (merged["exit_close"] - merged["entry_open"]) * 100
        merged["sq4_date"] = sq4
        merged["prev_day"] = prev
        merged["multi_night"] = sq["multi_night"]
        merged["cal_nights"] = sq["cal_nights"]

        if meta is not None:
            merged = merged.merge(meta[["Code", "sectors", "is_gaishu"]], on="Code", how="left")

        if beta_df is not None:
            prev_beta = beta_df[beta_df["Date"] == prev][["Code", "beta"]]
            merged = merged.merge(prev_beta, on="Code", how="left")

        all_trades.append(merged)

    if not all_trades:
        return pd.DataFrame()
    return pd.concat(all_trades, ignore_index=True)


def summarize(trades_df, group_col=None, label=""):
    """集計結果を表示"""
    if group_col:
        groups = trades_df.groupby(group_col)
    else:
        groups = [("ALL", trades_df)]

    rows = []
    for name, grp in groups:
        rets = grp["ret_pct"].tolist()
        n = len(rets)
        if n == 0:
            continue
        wins = [r for r in rets if r > 0]
        pf = calc_pf(rets)
        wr = len(wins) / n * 100
        avg = np.mean(rets)
        total_pnl = int(grp["pnl_100"].sum())
        rows.append({
            "group": name,
            "n": n,
            "pf": pf,
            "wr": round(wr, 1),
            "avg_ret": round(avg, 3),
            "total_pnl": total_pnl,
        })
    return pd.DataFrame(rows)


def main():
    print("=" * 80)
    print("SQ-4 Comprehensive Factor Analysis")
    print("=" * 80)

    print("\n[1/5] Loading data...")
    ps = load_prices()
    sq_schedule = load_sq_schedule()
    meta = load_meta()
    cme_status = load_cme_filter(ps, sq_schedule)
    print(f"  {len(sq_schedule)} SQ-4 events, {ps['Code'].nunique()} stocks")

    print("\n[2/5] Computing beta (60-day rolling)...")
    beta_df = compute_beta(ps, window=60)
    print(f"  beta computed: {len(beta_df)} rows")

    # === Selection: 5日ret worst10（前回ベスト） ===
    def select_5d_worst(prev_data):
        valid = prev_data.dropna(subset=["ret_5d"])
        return valid.nsmallest(TOP_N, "ret_5d")

    print("\n[3/5] Running backtest (5日ret worst10)...")
    trades = run_backtest(ps, sq_schedule, select_5d_worst, meta=meta, beta_df=beta_df)
    print(f"  total trades: {len(trades)}")

    if trades.empty:
        print("No trades. Exiting.")
        return

    # Beta quintile
    trades["beta_q"] = pd.qcut(trades["beta"].dropna(), 5, labels=["Q1(low)", "Q2", "Q3", "Q4", "Q5(high)"],
                                duplicates="drop")

    # Price quintile
    trades["price_q"] = pd.qcut(trades["entry_open"], 5,
                                 labels=["P1(low)", "P2", "P3", "P4", "P5(high)"],
                                 duplicates="drop")

    # ========================================
    # Factor Analysis Results
    # ========================================
    print("\n" + "=" * 80)
    print("FACTOR ANALYSIS RESULTS")
    print("=" * 80)

    # --- A. Holding Period ---
    print("\n\n--- A. Holding Period (1泊 vs 2泊+) ---")
    trades["hold_type"] = trades["multi_night"].map({False: "1泊(月曜)", True: "2泊+(金曜等)"})
    hold_summary = summarize(trades, "hold_type")
    print(hold_summary.to_string(index=False))

    # --- B. Sector (外需 vs 内需) ---
    print("\n\n--- B. Sector (外需 vs 内需) ---")
    trades["sector_type"] = trades["is_gaishu"].map({True: "外需", False: "内需"})
    sector_summary = summarize(trades, "sector_type")
    print(sector_summary.to_string(index=False))

    # 33業種別
    print("\n--- B2. 33業種別 Top/Bottom ---")
    sector33 = summarize(trades, "sectors")
    sector33 = sector33.sort_values("pf", ascending=False)
    print("  [Top 5 PF]")
    print(sector33.head(5).to_string(index=False))
    print("  [Bottom 5 PF]")
    print(sector33.tail(5).to_string(index=False))

    # --- C. Beta ---
    print("\n\n--- C. Beta Quintile ---")
    beta_summary = summarize(trades.dropna(subset=["beta_q"]), "beta_q")
    print(beta_summary.to_string(index=False))

    # --- D. Price Band ---
    print("\n\n--- D. Price Band ---")
    price_summary = summarize(trades.dropna(subset=["price_q"]), "price_q")
    print(price_summary.to_string(index=False))

    # --- E. CME filter × factors ---
    print("\n\n--- E. CME下落フィルタ × Holding Period ---")
    trades["cme_down"] = trades["sq4_date"].map(lambda d: cme_status.get(d))
    cme_down_trades = trades[trades["cme_down"] == True]
    cme_up_trades = trades[trades["cme_down"] == False]
    print(f"  CME下落: {len(cme_down_trades)} trades / CME上昇: {len(cme_up_trades)} trades")

    if not cme_down_trades.empty:
        print("\n  [CME下落時]")
        hold_cme = summarize(cme_down_trades, "hold_type")
        print(hold_cme.to_string(index=False))

    if not cme_up_trades.empty:
        print("\n  [CME上昇時]")
        hold_cme_up = summarize(cme_up_trades, "hold_type")
        print(hold_cme_up.to_string(index=False))

    # --- F. CME × Sector ---
    print("\n\n--- F. CME下落 × Sector ---")
    if not cme_down_trades.empty:
        sector_cme = summarize(cme_down_trades, "sector_type")
        print(sector_cme.to_string(index=False))

    # --- G. CME × Beta ---
    print("\n\n--- G. CME下落 × Beta ---")
    if not cme_down_trades.empty:
        beta_cme = summarize(cme_down_trades.dropna(subset=["beta_q"]), "beta_q")
        print(beta_cme.to_string(index=False))

    # ========================================
    # Cross Matrix: Selection × Factor
    # ========================================
    print("\n\n" + "=" * 80)
    print("SELECTION × FACTOR CROSS MATRIX")
    print("=" * 80)

    selections = {
        "5日worst10": select_5d_worst,
        "前日worst10": lambda d: d.dropna(subset=["ret_1d"]).nsmallest(TOP_N, "ret_1d"),
        "5000+×5日worst": lambda d: d[d["prev_close"] >= 5000].dropna(subset=["ret_5d"]).nsmallest(TOP_N, "ret_5d") if len(d[d["prev_close"] >= 5000]) >= TOP_N else d.dropna(subset=["ret_5d"]).nsmallest(TOP_N, "ret_5d"),
    }

    for sel_name, sel_fn in selections.items():
        print(f"\n--- Selection: {sel_name} ---")
        t = run_backtest(ps, sq_schedule, sel_fn, meta=meta, beta_df=beta_df)
        if t.empty:
            print("  No trades")
            continue

        t["hold_type"] = t["multi_night"].map({False: "1泊", True: "2泊+"})
        t["sector_type"] = t["is_gaishu"].map({True: "外需", False: "内需"})
        t["cme_down"] = t["sq4_date"].map(lambda d: cme_status.get(d))

        # All
        all_rets = t["ret_pct"].tolist()
        print(f"  ALL: N={len(all_rets)}, PF={calc_pf(all_rets)}, WR={len([r for r in all_rets if r>0])/len(all_rets)*100:.1f}%")

        # CME下落
        cd = t[t["cme_down"] == True]
        if not cd.empty:
            r = cd["ret_pct"].tolist()
            print(f"  CME下落: N={len(r)}, PF={calc_pf(r)}, WR={len([r2 for r2 in r if r2>0])/len(r)*100:.1f}%")

        # CME下落 × 1泊
        cd1 = cd[cd["hold_type"] == "1泊"]
        if not cd1.empty:
            r = cd1["ret_pct"].tolist()
            print(f"    CME下落×1泊: N={len(r)}, PF={calc_pf(r)}, WR={len([r2 for r2 in r if r2>0])/len(r)*100:.1f}%")

        # CME下落 × 2泊+
        cd2 = cd[cd["hold_type"] == "2泊+"]
        if not cd2.empty:
            r = cd2["ret_pct"].tolist()
            print(f"    CME下落×2泊+: N={len(r)}, PF={calc_pf(r)}, WR={len([r2 for r2 in r if r2>0])/len(r)*100:.1f}%")

        # CME下落 × 外需
        cdg = cd[cd["sector_type"] == "外需"]
        if not cdg.empty:
            r = cdg["ret_pct"].tolist()
            print(f"    CME下落×外需: N={len(r)}, PF={calc_pf(r)}, WR={len([r2 for r2 in r if r2>0])/len(r)*100:.1f}%")

        # CME下落 × 内需
        cdn = cd[cd["sector_type"] == "内需"]
        if not cdn.empty:
            r = cdn["ret_pct"].tolist()
            print(f"    CME下落×内需: N={len(r)}, PF={calc_pf(r)}, WR={len([r2 for r2 in r if r2>0])/len(r)*100:.1f}%")

    # ========================================
    # Volume Analysis (if available)
    # ========================================
    if VOLUME_PATH.exists():
        print("\n\n" + "=" * 80)
        print("VOLUME FACTOR ANALYSIS")
        print("=" * 80)
        vol_df = pd.read_parquet(VOLUME_PATH)
        vol_df["Date"] = pd.to_datetime(vol_df["Date"])
        vol_df["Code"] = vol_df["Code"].astype(str)

        # Merge volume to trades
        trades_vol = trades.merge(
            vol_df.rename(columns={"volume": "vol_prev"}),
            left_on=["prev_day", "Code"],
            right_on=["Date", "Code"],
            how="left"
        )
        trades_vol = trades_vol.dropna(subset=["vol_prev"])
        if not trades_vol.empty:
            trades_vol["vol_q"] = pd.qcut(trades_vol["vol_prev"], 5,
                                           labels=["V1(low)", "V2", "V3", "V4", "V5(high)"],
                                           duplicates="drop")
            print("\n--- Volume Quintile ---")
            vol_summary = summarize(trades_vol, "vol_q")
            print(vol_summary.to_string(index=False))

            # CME下落 × Volume
            cd_vol = trades_vol[trades_vol["cme_down"] == True]
            if not cd_vol.empty:
                print("\n--- CME下落 × Volume ---")
                vol_cme = summarize(cd_vol, "vol_q")
                print(vol_cme.to_string(index=False))
    else:
        print(f"\n[SKIP] Volume data not found at {VOLUME_PATH}")
        print("  To generate: python3 scripts/analysis/fetch_volume_data.py")

    print("\n\n[OK] Factor analysis complete.")


if __name__ == "__main__":
    main()
