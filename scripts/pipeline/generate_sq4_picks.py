#!/usr/bin/env python3
"""
generate_sq4_picks.py
SQ-4銘柄選定 + 過去トレード結果生成

prices_topix500_oc.parquet (J-Quants AdjO+AdjC, TOPIX 500) から
各月のSQ-4日にGap-down Top10を選定し、翌日(SQ-3)大引成決済のバックテスト結果をJSON出力。

実行方法:
    python3 scripts/pipeline/generate_sq4_picks.py
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from common_cfg.paths import PARQUET_DIR

PRICES_PATH = PARQUET_DIR / "prices_topix500_oc.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"
FUTURES_PATH = PARQUET_DIR / "futures_prices_max_1d.parquet"
CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"
OUTPUT_PATH = ROOT / "data" / "analysis" / "sq4_trades.json"

BACKTEST_START = "2022-04-01"
PRICE_MIN = 1000
PRICE_MAX = 20000
GAP_FLOOR = -10.0
TOP_N = 10


def load_prices() -> pd.DataFrame:
    ps = pd.read_parquet(PRICES_PATH)
    ps["Date"] = pd.to_datetime(ps["Date"])
    ps["Code"] = ps["Code"].astype(str)
    ps = ps.sort_values(["Code", "Date"]).drop_duplicates(subset=["Code", "Date"])
    return ps


def load_cme() -> pd.DataFrame:
    """CME NKD=F 日足を読み込み"""
    if not FUTURES_PATH.exists():
        return pd.DataFrame()
    df = pd.read_parquet(FUTURES_PATH)
    cme = df[df["ticker"] == "NKD=F"][["date", "Close"]].copy()
    cme["date"] = pd.to_datetime(cme["date"])
    cme = cme.dropna(subset=["Close"]).sort_values("date").reset_index(drop=True)
    return cme


def compute_sq_dates(bdays: list[pd.Timestamp], start: str) -> list[dict]:
    bday_set = set(bdays)
    start_ts = pd.Timestamp(start)
    last_bday = bdays[-1]

    results = []
    years = sorted(set(d.year for d in bdays if d >= start_ts))

    for year in years:
        for month in range(1, 13):
            fridays = []
            d = date(year, month, 1)
            while d.month == month:
                if d.weekday() == 4:
                    fridays.append(pd.Timestamp(d))
                d = date.fromordinal(d.toordinal() + 1)

            if len(fridays) < 2:
                continue
            second_friday = fridays[1]

            if second_friday > last_bday:
                continue

            if second_friday in bday_set:
                sq_day = second_friday
            else:
                candidates = [b for b in bdays if b <= second_friday]
                if not candidates:
                    continue
                sq_day = candidates[-1]

            sq_idx = bdays.index(sq_day)
            if sq_idx < 4:
                continue

            sq4 = bdays[sq_idx - 4]
            sq3 = bdays[sq_idx - 3]

            if sq4 < start_ts:
                continue
            if sq3 > last_bday:
                continue

            sq4_prev_idx = bdays.index(sq4) - 1
            if sq4_prev_idx < 0:
                continue
            prev_day = bdays[sq4_prev_idx]

            results.append({
                "month": f"{year}-{month:02d}",
                "sq_day": sq_day,
                "sq4_entry": sq4,
                "sq3_exit": sq3,
                "prev_day": prev_day,
            })

    return results


def load_name_map() -> dict[str, str]:
    """5桁Code → stock_name のマッピング"""
    if not META_PATH.exists():
        return {}
    meta = pd.read_parquet(META_PATH)
    name_map = {}
    for _, row in meta.iterrows():
        code_5 = str(row["code"]) + "0"
        name_map[code_5] = row["stock_name"]
    return name_map


def select_picks(
    ps: pd.DataFrame,
    sq4_date: pd.Timestamp,
    prev_date: pd.Timestamp,
    sq3_date: pd.Timestamp,
    name_map: dict[str, str] | None = None,
) -> list[dict]:
    prev_data = ps[ps["Date"] == prev_date][["Code", "AdjC"]].rename(
        columns={"AdjC": "prev_close"}
    )
    entry_data = ps[ps["Date"] == sq4_date][["Code", "AdjO"]].rename(
        columns={"AdjO": "entry_open"}
    )
    exit_data = ps[ps["Date"] == sq3_date][["Code", "AdjC"]].rename(
        columns={"AdjC": "exit_close"}
    )

    merged = prev_data.merge(entry_data, on="Code").merge(exit_data, on="Code")
    merged = merged[
        (merged["prev_close"] >= PRICE_MIN)
        & (merged["prev_close"] <= PRICE_MAX)
    ]
    merged["gap_pct"] = (merged["entry_open"] / merged["prev_close"] - 1) * 100
    merged = merged[merged["gap_pct"] >= GAP_FLOOR]

    if merged.empty:
        return []

    picks = merged.nsmallest(TOP_N, "gap_pct")

    trades = []
    for _, row in picks.iterrows():
        code_5 = row["Code"]
        code_4 = code_5[:-1] if len(code_5) == 5 and code_5[-1] == "0" else code_5
        ret_pct = (row["exit_close"] / row["entry_open"] - 1) * 100
        trades.append({
            "code": code_4,
            "name": (name_map or {}).get(code_5, ""),
            "prev_close": round(float(row["prev_close"]), 1),
            "entry_price": round(float(row["entry_open"]), 1),
            "exit_price": round(float(row["exit_close"]), 1),
            "gap_pct": round(float(row["gap_pct"]), 2),
            "ret_pct": round(float(ret_pct), 2),
            "pnl_100": int(round((row["exit_close"] - row["entry_open"]) * 100)),
        })

    return trades


def calc_stats(all_trades: list[dict]) -> dict:
    if not all_trades:
        return {}

    rets = [t["ret_pct"] for t in all_trades]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]

    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    pf = round(gross_profit / gross_loss, 2) if gross_loss > 0 else None

    return {
        "total": len(rets),
        "wins": len(wins),
        "losses": len(losses),
        "wr": round(len(wins) / len(rets) * 100, 1),
        "avg_ret": round(np.mean(rets), 3),
        "median_ret": round(float(np.median(rets)), 3),
        "max_ret": round(max(rets), 2),
        "min_ret": round(min(rets), 2),
        "pf": pf,
        "total_ret": round(sum(rets), 2),
        "total_pnl_100": sum(t["pnl_100"] for t in all_trades),
    }


def calc_stats_by_price(all_trades: list[dict]) -> dict:
    segments = {
        "1000_5000": (1000, 5000),
        "5000_10000": (5000, 10000),
        "10000_20000": (10000, 20000),
    }
    result = {}
    for seg_name, (lo, hi) in segments.items():
        seg_trades = [t for t in all_trades if lo <= t["prev_close"] < hi]
        if seg_trades:
            result[seg_name] = calc_stats(seg_trades)
    return result


def get_next_sq4(calendar_path: Path) -> dict | None:
    if not calendar_path.exists():
        return None
    cal = pd.read_parquet(calendar_path)
    cal["date"] = pd.to_datetime(cal["date"])
    today = pd.Timestamp(date.today())

    future_sq4 = cal[(cal["sq4_entry"] == True) & (cal["date"] >= today)]
    if future_sq4.empty:
        return None

    next_row = future_sq4.iloc[0]
    next_date = next_row["date"]

    future_sq3 = cal[(cal["sq3_exit"] == True) & (cal["date"] > next_date)]
    exit_date = future_sq3.iloc[0]["date"] if not future_sq3.empty else None

    return {
        "entry_date": next_date.strftime("%Y-%m-%d"),
        "exit_date": exit_date.strftime("%Y-%m-%d") if exit_date else None,
    }


def get_candidates(ps: pd.DataFrame) -> dict:
    latest_date = ps["Date"].max()
    latest = ps[ps["Date"] == latest_date][["Code", "AdjC"]].copy()
    latest = latest[
        (latest["AdjC"] >= PRICE_MIN) & (latest["AdjC"] <= PRICE_MAX)
    ]
    return {
        "as_of": latest_date.strftime("%Y-%m-%d"),
        "count": len(latest),
        "price_5000_plus": int((latest["AdjC"] >= 5000).sum()),
        "price_under_5000": int((latest["AdjC"] < 5000).sum()),
    }


def main() -> int:
    print("=" * 60)
    print("Generate SQ-4 Picks (Gap-down Top 10)")
    print("=" * 60)

    print("\n[1] Loading prices_topix500_oc.parquet (J-Quants AdjO+AdjC)...")
    ps = load_prices()
    print(f"  {len(ps):,} rows, {ps['Code'].nunique()} codes (TOPIX 500)")
    print(f"  Range: {ps['Date'].min().date()} ~ {ps['Date'].max().date()}")

    print("\n[2] Loading name map from meta_jquants...")
    name_map = load_name_map()
    print(f"  {len(name_map)} codes mapped")

    print("\n[3] Loading CME NKD=F...")
    cme = load_cme()
    print(f"  {len(cme)} rows" if not cme.empty else "  not available")

    print("\n[4] Computing business days & SQ dates...")
    bdays = sorted(ps["Date"].unique())
    sq_schedule = compute_sq_dates(bdays, BACKTEST_START)
    print(f"  SQ-4 dates found: {len(sq_schedule)}")

    print("\n[5] Selecting picks for each SQ-4...")
    monthly_results = []
    all_trades = []
    cme_close_map = {}
    if not cme.empty:
        for _, row in cme.iterrows():
            cme_close_map[row["date"]] = row["Close"]

    for sq in sq_schedule:
        trades = select_picks(ps, sq["sq4_entry"], sq["prev_day"], sq["sq3_exit"], name_map)
        if not trades:
            continue

        month_ret = sum(t["ret_pct"] for t in trades)
        month_pnl = sum(t["pnl_100"] for t in trades)

        # CME金曜(=SQ-4前日)のリターン
        friday = sq["prev_day"]
        friday_idx = bdays.index(friday)
        thursday = bdays[friday_idx - 1] if friday_idx >= 1 else None
        cme_fri = cme_close_map.get(friday)
        cme_thu = cme_close_map.get(thursday) if thursday else None
        cme_change = None
        cme_ret = None
        if cme_fri is not None and cme_thu is not None and cme_thu != 0:
            cme_change = int(round(cme_fri - cme_thu))
            cme_ret = round((cme_fri / cme_thu - 1) * 100, 2)

        monthly_results.append({
            "month": sq["month"],
            "entry_date": sq["sq4_entry"].strftime("%Y-%m-%d"),
            "exit_date": sq["sq3_exit"].strftime("%Y-%m-%d"),
            "n_picks": len(trades),
            "total_ret": round(month_ret, 2),
            "total_pnl_100": month_pnl,
            "cme_change": cme_change,
            "cme_ret": cme_ret,
            "picks": trades,
        })
        all_trades.extend(
            {**t, "entry_date": sq["sq4_entry"].strftime("%Y-%m-%d"),
             "exit_date": sq["sq3_exit"].strftime("%Y-%m-%d")}
            for t in trades
        )
        symbol = "+" if month_ret > 0 else "-" if month_ret < 0 else "="
        cme_str = f"CME={cme_change:+,}" if cme_change is not None else ""
        print(f"  {sq['month']} | {sq['sq4_entry'].strftime('%m/%d')}→{sq['sq3_exit'].strftime('%m/%d')} "
              f"| N={len(trades):2d} | ret={month_ret:+.2f}% {symbol} {cme_str}")

    print(f"\n[6] Computing stats...")
    stats = calc_stats(all_trades)
    stats_by_price = calc_stats_by_price(all_trades)

    # CME分割stats
    cme_down_trades = []
    cme_up_trades = []
    for m in monthly_results:
        if m["cme_ret"] is not None:
            target = cme_down_trades if m["cme_ret"] < 0 else cme_up_trades
            target.extend(m["picks"])
    stats_cme_down = calc_stats(cme_down_trades) if cme_down_trades else {}
    stats_cme_up = calc_stats(cme_up_trades) if cme_up_trades else {}

    print(f"  Total trades: {stats.get('total', 0)}")
    print(f"  Win rate: {stats.get('wr', 0):.1f}%")
    print(f"  PF: {stats.get('pf', 'N/A')}")
    print(f"  Avg ret: {stats.get('avg_ret', 0):+.3f}%")
    print(f"  CME down: N={stats_cme_down.get('total',0)} PF={stats_cme_down.get('pf','N/A')}")
    print(f"  CME up:   N={stats_cme_up.get('total',0)} PF={stats_cme_up.get('pf','N/A')}")

    for seg, s in stats_by_price.items():
        print(f"  [{seg}] N={s['total']} PF={s['pf']} WR={s['wr']:.1f}%")

    print(f"\n[7] Next SQ-4...")
    next_sq4 = get_next_sq4(CALENDAR_PATH)
    candidates = get_candidates(ps)
    if next_sq4:
        print(f"  Next: {next_sq4['entry_date']} → {next_sq4['exit_date']}")
    print(f"  Candidates: {candidates['count']} stocks (≥5000: {candidates['price_5000_plus']})")

    print(f"\n[8] Saving {OUTPUT_PATH.name}...")
    output = {
        "generated": date.today().isoformat(),
        "params": {
            "backtest_start": BACKTEST_START,
            "price_min": PRICE_MIN,
            "price_max": PRICE_MAX,
            "gap_floor": GAP_FLOOR,
            "top_n": TOP_N,
        },
        "stats": stats,
        "stats_by_price": stats_by_price,
        "stats_cme_down": stats_cme_down,
        "stats_cme_up": stats_cme_up,
        "next_sq4": next_sq4,
        "candidates": candidates,
        "monthly": monthly_results,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  {OUTPUT_PATH} ({OUTPUT_PATH.stat().st_size:,} bytes)")
    print("\n[OK] Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
