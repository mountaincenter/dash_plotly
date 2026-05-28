#!/usr/bin/env python3
"""
generate_sq4_picks.py
SQ-4銘柄選定 + 過去トレード結果生成

選定ロジック: 外需セクター × 5日リターン worst10 (前営業日終値ベース)
エントリー: SQ-4日 寄成
イグジット: SQ-3日 大引成
Go/No-go: CME NKD=F 前日比下落 → Go

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
SIGNALS_PATH = PARQUET_DIR / "signals.parquet"

BACKTEST_START = "2022-04-01"
PRICE_MIN = 1000
PRICE_MAX = 20000
TOP_N = 10

GAISHU_SECTORS = frozenset({
    "電気機器", "輸送用機器", "機械", "精密機器",
    "化学", "非鉄金属", "鉄鋼", "ゴム製品",
    "海運業", "鉱業", "石油･石炭製品",
})


def load_prices() -> pd.DataFrame:
    ps = pd.read_parquet(PRICES_PATH)
    ps["Date"] = pd.to_datetime(ps["Date"])
    ps["Code"] = ps["Code"].astype(str)
    ps = ps.sort_values(["Code", "Date"]).drop_duplicates(subset=["Code", "Date"])
    ps["prev_close"] = ps.groupby("Code")["AdjC"].shift(1)
    ps["ret_5d"] = ps.groupby("Code")["AdjC"].pct_change(5)
    return ps


def load_gaishu_codes() -> set[str]:
    """外需セクターの5桁Codeセットを返す"""
    meta = pd.read_parquet(META_PATH)
    meta["Code"] = meta["code"].astype(str) + "0"
    return set(meta[meta["sectors"].isin(GAISHU_SECTORS)]["Code"])


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
    """calendar.parquetのsq4_entry/sq3_exitフラグからSQ日程を構築"""
    cal = pd.read_parquet(CALENDAR_PATH)
    cal["date"] = pd.to_datetime(cal["date"])
    start_ts = pd.Timestamp(start)
    last_bday = bdays[-1]
    bday_set = set(bdays)

    sq4_dates = cal[cal["sq4_entry"] == True]["date"].tolist()
    sq3_dates = cal[cal["sq3_exit"] == True]["date"].tolist()
    sq_days = cal[cal["sq_day"] == True]["date"].tolist()

    results = []
    for sq4 in sq4_dates:
        if sq4 < start_ts:
            continue
        if sq4 not in bday_set:
            continue

        sq3_after = [d for d in sq3_dates if d > sq4]
        if not sq3_after:
            continue
        sq3 = sq3_after[0]
        if sq3 > last_bday:
            continue

        sq_after = [d for d in sq_days if d > sq3]
        sq_day = sq_after[0] if sq_after else sq3

        sq4_idx = bdays.index(sq4)
        if sq4_idx < 1:
            continue
        prev_day = bdays[sq4_idx - 1]

        month_str = f"{sq_day.year}-{sq_day.month:02d}"

        results.append({
            "month": month_str,
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
    gaishu_codes: set[str],
    name_map: dict[str, str] | None = None,
) -> list[dict]:
    """外需セクター × 5日ret worst10 で選定"""
    prev_data = ps[ps["Date"] == prev_date][["Code", "AdjC", "ret_5d"]].copy()
    prev_data = prev_data.rename(columns={"AdjC": "prev_close"})
    prev_data = prev_data[
        (prev_data["prev_close"] >= PRICE_MIN)
        & (prev_data["prev_close"] <= PRICE_MAX)
        & (prev_data["Code"].isin(gaishu_codes))
    ]
    prev_data = prev_data.dropna(subset=["ret_5d"])

    if prev_data.empty:
        return []

    picks_df = prev_data.nsmallest(TOP_N, "ret_5d")
    codes = picks_df["Code"].tolist()

    entry_data = ps[(ps["Date"] == sq4_date) & (ps["Code"].isin(codes))][["Code", "AdjO"]].rename(
        columns={"AdjO": "entry_open"}
    )
    exit_data = ps[(ps["Date"] == sq3_date) & (ps["Code"].isin(codes))][["Code", "AdjC"]].rename(
        columns={"AdjC": "exit_close"}
    )

    merged = picks_df[["Code", "prev_close", "ret_5d"]].merge(entry_data, on="Code").merge(exit_data, on="Code")
    if merged.empty:
        return []

    trades = []
    for _, row in merged.iterrows():
        code_5 = row["Code"]
        code_4 = code_5[:-1] if len(code_5) == 5 and code_5[-1] == "0" else code_5
        ret_pct = (row["exit_close"] / row["entry_open"] - 1) * 100
        trades.append({
            "code": code_4,
            "name": (name_map or {}).get(code_5, ""),
            "prev_close": round(float(row["prev_close"]), 1),
            "ret_5d": round(float(row["ret_5d"] * 100), 2),
            "entry_price": round(float(row["entry_open"]), 1),
            "exit_price": round(float(row["exit_close"]), 1),
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


def calc_max_dd(monthly_results: list[dict]) -> dict:
    """月次PnLからMaxDD(金額・比率)を計算"""
    if not monthly_results:
        return {"amount": 0, "pct": 0.0}
    cum = 0
    peak = 0
    max_dd_amount = 0
    cum_rets = []
    cum_ret = 0.0
    peak_ret = 0.0
    max_dd_pct = 0.0
    for m in monthly_results:
        cum += m["total_pnl_100"]
        peak = max(peak, cum)
        dd = cum - peak
        max_dd_amount = min(max_dd_amount, dd)

        cum_ret += m["total_ret"]
        peak_ret = max(peak_ret, cum_ret)
        dd_pct = cum_ret - peak_ret
        max_dd_pct = min(max_dd_pct, dd_pct)

    return {"amount": int(max_dd_amount), "pct": round(max_dd_pct, 2)}


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
    prev_days = cal[cal["date"] < next_date].sort_values("date")
    prev_date = prev_days.iloc[-1]["date"] if not prev_days.empty else None

    return {
        "entry_date": next_date.strftime("%Y-%m-%d"),
        "prev_date": prev_date.strftime("%Y-%m-%d") if prev_date is not None else None,
        "exit_date": exit_date.strftime("%Y-%m-%d") if exit_date else None,
    }


def get_candidates(
    ps: pd.DataFrame,
    gaishu_codes: set[str],
    name_map: dict[str, str] | None = None,
    prev_date: str | None = None,
) -> dict:
    latest_date = ps["Date"].max()
    target_date = pd.Timestamp(prev_date) if prev_date else latest_date
    is_actionable = latest_date >= target_date
    if not is_actionable:
        return {
            "as_of": latest_date.strftime("%Y-%m-%d"),
            "required_as_of": target_date.strftime("%Y-%m-%d"),
            "actionable": False,
            "count": 0,
            "sector": "外需",
            "picks": [],
        }

    latest = ps[ps["Date"] == target_date][["Code", "AdjC", "ret_5d"]].copy()
    latest = latest[
        (latest["AdjC"] >= PRICE_MIN)
        & (latest["AdjC"] <= PRICE_MAX)
        & (latest["Code"].isin(gaishu_codes))
    ]
    latest = latest.dropna(subset=["ret_5d"])
    worst10 = latest.nsmallest(TOP_N, "ret_5d")
    picks = []
    for _, row in worst10.iterrows():
        code_5 = row["Code"]
        code_4 = code_5[:-1] if len(code_5) == 5 and code_5[-1] == "0" else code_5
        picks.append({
            "code": code_4,
            "name": (name_map or {}).get(code_5, ""),
            "prev_close": round(float(row["AdjC"]), 1),
            "ret_5d": round(float(row["ret_5d"] * 100), 2),
        })
    return {
        "as_of": target_date.strftime("%Y-%m-%d"),
        "required_as_of": target_date.strftime("%Y-%m-%d"),
        "actionable": True,
        "count": len(latest),
        "sector": "外需",
        "picks": picks,
    }


def main() -> int:
    print("=" * 60)
    print("Generate SQ-4 Picks (外需×5日ret worst10)")
    print("=" * 60)

    print("\n[1] Loading prices_topix500_oc.parquet...")
    ps = load_prices()
    print(f"  {len(ps):,} rows, {ps['Code'].nunique()} codes (TOPIX 500)")
    print(f"  Range: {ps['Date'].min().date()} ~ {ps['Date'].max().date()}")

    print("\n[1.5] Loading gaishu sector codes...")
    gaishu_codes = load_gaishu_codes()
    print(f"  {len(gaishu_codes)} codes in gaishu sectors")

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
        trades = select_picks(ps, sq["sq4_entry"], sq["prev_day"], sq["sq3_exit"], gaishu_codes, name_map)
        if not trades:
            continue

        month_ret = sum(t["ret_pct"] for t in trades)
        month_pnl = sum(t["pnl_100"] for t in trades)

        # CME: SQ-4前日付近の直近2営業日を探す（米休場対応）
        friday = sq["prev_day"]
        friday_idx = bdays.index(friday)
        cme_fri = None
        cme_thu = None
        for offset in range(3):
            idx = friday_idx - offset
            if idx >= 0 and bdays[idx] in cme_close_map:
                if cme_fri is None:
                    cme_fri = cme_close_map[bdays[idx]]
                elif cme_thu is None:
                    cme_thu = cme_close_map[bdays[idx]]
                    break
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


    print(f"\n[7] Next SQ-4...")
    next_sq4 = get_next_sq4(CALENDAR_PATH)
    candidates = get_candidates(ps, gaishu_codes, name_map, next_sq4.get("prev_date") if next_sq4 else None)
    if next_sq4:
        print(f"  Next: {next_sq4['entry_date']} → {next_sq4['exit_date']} (prev={next_sq4.get('prev_date')})")
    if candidates.get("actionable"):
        print(f"  Candidates: {candidates['count']} stocks (外需×{PRICE_MIN}-{PRICE_MAX}円)")
    else:
        print(
            "  Candidates: not actionable "
            f"(latest={candidates.get('as_of')}, required={candidates.get('required_as_of')})"
        )

    max_dd = calc_max_dd(monthly_results)
    # CME下落時のみのMaxDD
    cme_down_monthly = [m for m in monthly_results if m.get("cme_ret") is not None and m["cme_ret"] < 0]
    max_dd_cme_down = calc_max_dd(cme_down_monthly)

    print(f"\n[8] Saving {OUTPUT_PATH.name}...")
    output = {
        "generated": date.today().isoformat(),
        "params": {
            "backtest_start": BACKTEST_START,
            "price_min": PRICE_MIN,
            "price_max": PRICE_MAX,
            "top_n": TOP_N,
            "selection": "gaishu_5d_ret_worst10",
        },
        "stats": stats,
        "stats_cme_down": stats_cme_down,
        "stats_cme_up": stats_cme_up,
        "max_dd": max_dd,
        "max_dd_cme_down": max_dd_cme_down,
        "next_sq4": next_sq4,
        "candidates": candidates,
        "monthly": monthly_results,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  {OUTPUT_PATH} ({OUTPUT_PATH.stat().st_size:,} bytes)")

    # signals.parquet に当日シグナル行を merge。
    # まだ前営業日データが揃っていない未来SQ-4候補は、既存のsq4行も消して stale 表示を防ぐ。
    existing_without_sq4 = None
    if SIGNALS_PATH.exists():
        existing = pd.read_parquet(SIGNALS_PATH)
        existing_without_sq4 = existing[existing["strategy"] != "sq4"] if "strategy" in existing.columns else existing

    if next_sq4 and candidates and candidates.get("actionable") and candidates.get("picks"):
        sig_rows = []
        for p in candidates["picks"]:
            code = str(p["code"])
            ticker = code + ".T" if not code.endswith(".T") else code
            sig_rows.append({
                "signal_date": pd.Timestamp(next_sq4["entry_date"]),
                "ticker": ticker,
                "strategy": "sq4",
                "direction": "long",
                "pair_id": "",
                "stock_name": p.get("name", ""),
                "entry_price_est": p.get("prev_close"),
                "prev_close": p.get("prev_close"),
            })
        if sig_rows:
            new_sigs = pd.DataFrame(sig_rows)
            merged = pd.concat([new_sigs, existing_without_sq4], ignore_index=True) if existing_without_sq4 is not None else new_sigs
            SIGNALS_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = SIGNALS_PATH.parent / f"{SIGNALS_PATH.name}.tmp"
            merged.to_parquet(tmp, index=False)
            tmp.replace(SIGNALS_PATH)
            print(f"[OK] signals.parquet merged: {len(new_sigs)} rows (strategy=sq4 / total={len(merged)})")
    elif existing_without_sq4 is not None:
        tmp = SIGNALS_PATH.parent / f"{SIGNALS_PATH.name}.tmp"
        existing_without_sq4.to_parquet(tmp, index=False)
        tmp.replace(SIGNALS_PATH)
        print(f"[OK] signals.parquet stale sq4 rows removed (total={len(existing_without_sq4)})")

    print("\n[OK] Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
