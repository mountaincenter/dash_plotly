#!/usr/bin/env python3
"""曜日×US前夜エッジ 週次トレードデータ生成。

TOPIX500から選定した銘柄を曜日別にLONG/SHORT。
USフィルタ: LONG=US前夜+1%以下、SHORT=US前夜-1%以上。
寄成IN→引成OUT（日計り）。

出力: data/analysis/weekday_edge_trades.json
"""
from __future__ import annotations

import io
import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
import yfinance as yf

from common_cfg.paths import PARQUET_DIR

PRICES_PATH = PARQUET_DIR / "prices_topix500_oc.parquet"
OUTPUT_PATH = ROOT / "data" / "analysis" / "weekday_edge_trades.json"
SIGNALS_PATH = PARQUET_DIR / "signals.parquet"

# --- 銘柄定義 ---
# LONG Core 9銘柄 (アドバンテスト除外)
LONG_CORE = [
    {"code": "54060", "dow": 4, "name": "神戸製鋼所"},
    {"code": "50160", "dow": 2, "name": "ＪＸ金属"},
    {"code": "83030", "dow": 3, "name": "ＳＢＩ新生銀行"},
    {"code": "83770", "dow": 4, "name": "ほくほくフィナンシャルグループ"},
    {"code": "19510", "dow": 4, "name": "エクシオグループ"},
    {"code": "90720", "dow": 3, "name": "ニッコンホールディングス"},
    {"code": "61410", "dow": 3, "name": "ＤＭＧ森精機"},
    {"code": "59290", "dow": 4, "name": "三和ホールディングス"},
    {"code": "19440", "dow": 3, "name": "きんでん"},
]

# アドバンテスト スポット (US下落時のみ)
LONG_SPOT = [
    {"code": "68570", "dow": 2, "name": "アドバンテスト"},
]

# SHORT Core 10銘柄
SHORT_CORE = [
    {"code": "45160", "dow": 0, "name": "日本新薬"},
    {"code": "16050", "dow": 0, "name": "ＩＮＰＥＸ"},
    {"code": "42040", "dow": 0, "name": "積水化学工業"},
    {"code": "58440", "dow": 1, "name": "京都フィナンシャルグループ"},
    {"code": "58380", "dow": 1, "name": "楽天銀行"},
    {"code": "52010", "dow": 0, "name": "ＡＧＣ"},
    {"code": "44010", "dow": 0, "name": "ＡＤＥＫＡ"},
    {"code": "53320", "dow": 1, "name": "ＴＯＴＯ"},
    {"code": "28970", "dow": 0, "name": "日清食品ホールディングス"},
    {"code": "57110", "dow": 0, "name": "三菱マテリアル"},
]

DOW_LABELS = {0: "月", 1: "火", 2: "水", 3: "木", 4: "金"}


def _fetch_sp500() -> dict[pd.Timestamp, float]:
    """S&P500 日次リターンを取得"""
    sp = yf.download("^GSPC", start="2022-01-01", progress=False)
    sp = sp[["Close"]].copy()
    sp.columns = ["close"]
    sp["ret"] = sp["close"].pct_change()
    return sp["ret"].to_dict()


def _get_prev_us_ret(jp_date: pd.Timestamp, sp_ret: dict) -> float | None:
    target = jp_date - pd.Timedelta(days=1)
    for delta in range(0, 5):
        t = target - pd.Timedelta(days=delta)
        if t in sp_ret and not pd.isna(sp_ret[t]):
            return float(sp_ret[t])
    return None


def _evaluate(rets: list[float]) -> dict:
    if not rets:
        return {}
    arr = np.array(rets)
    gain = arr[arr > 0].sum()
    loss = -arr[arr < 0].sum()
    pf = round(float(gain / loss), 2) if loss > 0 else None
    return {
        "total": len(arr),
        "wins": int((arr > 0).sum()),
        "losses": int((arr <= 0).sum()),
        "wr": round(float((arr > 0).mean() * 100), 1),
        "avg_ret": round(float(arr.mean()), 2),
        "median_ret": round(float(np.median(arr)), 2),
        "max_ret": round(float(arr.max()), 2),
        "min_ret": round(float(arr.min()), 2),
        "pf": pf,
        "total_ret": round(float(arr.sum()), 2),
        "total_pnl_100": int(round(arr.sum())),
    }


def _max_dd_from_daily(daily_records: list[dict]) -> dict:
    """日付付きレコードから日次集約MaxDDを計算。
    daily_records: [{"date": ..., "pnl_100": ..., "ret_pct": ...}, ...]
    """
    if not daily_records:
        return {"amount": 0, "pct": 0.0}
    df = pd.DataFrame(daily_records)
    daily = df.groupby("date").agg({"pnl_100": "sum", "ret_pct": "sum"}).sort_index()

    cum_pnl = daily["pnl_100"].cumsum().values
    peak_pnl = np.maximum.accumulate(cum_pnl)
    dd_pnl = (cum_pnl - peak_pnl).min()

    cum_ret = daily["ret_pct"].cumsum().values
    peak_ret = np.maximum.accumulate(cum_ret)
    dd_ret = (cum_ret - peak_ret).min()

    return {"amount": int(round(dd_pnl)), "pct": round(float(dd_ret), 2)}


def main() -> int:
    print("=" * 60)
    print("Generate Weekday Edge Trades JSON")
    print("=" * 60)

    prices = pd.read_parquet(PRICES_PATH)
    prices.columns = ["date", "code", "adj_open", "adj_close"]
    prices["date"] = pd.to_datetime(prices["date"])
    prices["dow"] = prices["date"].dt.dayofweek
    prices = prices.sort_values(["code", "date"])

    print("\n[1] S&P500 リターン取得")
    sp_ret = _fetch_sp500()

    unique_dates = sorted(prices["date"].unique())
    date_us_ret = {d: _get_prev_us_ret(d, sp_ret) for d in unique_dates}
    prices["us_prev_ret"] = prices["date"].map(date_us_ret)

    all_configs = []
    for stock in LONG_CORE:
        all_configs.append({**stock, "direction": "LONG", "group": "core"})
    for stock in LONG_SPOT:
        all_configs.append({**stock, "direction": "LONG", "group": "spot"})
    for stock in SHORT_CORE:
        all_configs.append({**stock, "direction": "SHORT", "group": "core"})

    print(f"\n[2] トレード計算 ({len(all_configs)}銘柄)")

    weekly_trades: dict[str, list] = {}  # "YYYY-WW" → trades
    all_daily_records: list[dict] = []
    us_filtered_daily_records: list[dict] = []

    stock_stats: list[dict] = []

    for cfg in all_configs:
        code, dow, direction = cfg["code"], cfg["dow"], cfg["direction"]
        sign = -1 if direction == "SHORT" else 1
        is_spot = cfg["group"] == "spot"

        sub = prices[(prices["code"] == code) & (prices["dow"] == dow)].dropna(
            subset=["adj_open", "adj_close"]
        ).copy()

        # 寄→引PnL
        sub["ret_pct"] = (sub["adj_close"] / sub["adj_open"] - 1) * sign * 100
        sub["pnl_100"] = (sub["adj_close"] - sub["adj_open"]) * sign * 100

        # USフィルタ
        if direction == "LONG":
            if is_spot:
                # アドバンテスト: US下落時のみ
                us_mask = sub["us_prev_ret"] < -0.01
            else:
                us_mask = sub["us_prev_ret"] <= 0.01
        else:
            us_mask = sub["us_prev_ret"] >= -0.01

        sub_all = sub.copy()
        sub_filtered = sub[us_mask].copy()

        # 全トレード記録
        for _, row in sub_filtered.iterrows():
            iso = row["date"].isocalendar()
            week_key = f"{iso[0]}-W{iso[1]:02d}"
            if week_key not in weekly_trades:
                weekly_trades[week_key] = []

            date_str = row["date"].strftime("%Y-%m-%d")
            ret_val = round(float(row["ret_pct"]), 2)
            pnl_val = int(round(float(row["pnl_100"])))

            weekly_trades[week_key].append({
                "date": date_str,
                "code": code,
                "name": cfg["name"],
                "direction": direction,
                "group": cfg["group"],
                "dow_label": DOW_LABELS[dow],
                "adj_open": round(float(row["adj_open"]), 1),
                "adj_close": round(float(row["adj_close"]), 1),
                "ret_pct": ret_val,
                "pnl_100": pnl_val,
                "us_prev_ret": round(float(row["us_prev_ret"]) * 100, 2) if pd.notna(row["us_prev_ret"]) else None,
            })

            us_filtered_daily_records.append({"date": date_str, "pnl_100": float(row["pnl_100"]), "ret_pct": float(row["ret_pct"])})

        for _, row in sub_all.iterrows():
            date_str = row["date"].strftime("%Y-%m-%d")
            all_daily_records.append({"date": date_str, "pnl_100": float(row["pnl_100"]), "ret_pct": float(row["ret_pct"])})

        # 銘柄別統計
        if len(sub_filtered) > 0:
            s_filt = _evaluate(sub_filtered["ret_pct"].tolist())
            s_filt["pnl_100_total"] = int(round(sub_filtered["pnl_100"].sum()))
        else:
            s_filt = {}

        if len(sub_all) > 0:
            s_all = _evaluate(sub_all["ret_pct"].tolist())
        else:
            s_all = {}

        stock_stats.append({
            "code": code,
            "name": cfg["name"],
            "direction": direction,
            "group": cfg["group"],
            "dow": dow,
            "dow_label": DOW_LABELS[dow],
            "stats_filtered": s_filt,
            "stats_all": s_all,
            "n_filtered": len(sub_filtered),
            "n_all": len(sub_all),
        })

    # 週次集約
    print(f"\n[3] 週次集約 ({len(weekly_trades)}週)")
    weekly_summary = []
    for week_key in sorted(weekly_trades.keys()):
        trades = weekly_trades[week_key]
        week_pnl_ret = [t["ret_pct"] for t in trades]
        week_pnl_100 = [t["pnl_100"] for t in trades]
        dates = sorted(set(t["date"] for t in trades))

        weekly_summary.append({
            "week": week_key,
            "start_date": dates[0],
            "end_date": dates[-1],
            "n_trades": len(trades),
            "total_ret": round(sum(week_pnl_ret), 2),
            "total_pnl_100": int(round(sum(week_pnl_100))),
            "picks": trades,
        })

    # 年別統計（日次集約MaxDD）
    year_records: dict[int, list[dict]] = {}
    for ws in weekly_summary:
        year = int(ws["start_date"][:4])
        if year not in year_records:
            year_records[year] = []
        for t in ws["picks"]:
            year_records[year].append({"date": t["date"], "pnl_100": t["pnl_100"], "ret_pct": t["ret_pct"]})

    yearly = []
    for year in sorted(year_records.keys()):
        rets = [r["ret_pct"] for r in year_records[year]]
        pnl_100s = [r["pnl_100"] for r in year_records[year]]
        ys = _evaluate(rets)
        ys["year"] = year
        ys["total_pnl_100"] = int(round(sum(pnl_100s)))
        ys["max_dd"] = _max_dd_from_daily(year_records[year])
        yearly.append(ys)

    # 全体統計（日次集約MaxDD）
    us_filtered_rets = [r["ret_pct"] for r in us_filtered_daily_records]
    us_filtered_pnls = [r["pnl_100"] for r in us_filtered_daily_records]
    stats_filtered = _evaluate(us_filtered_rets)
    stats_filtered["total_pnl_100"] = int(round(sum(us_filtered_pnls)))

    all_rets = [r["ret_pct"] for r in all_daily_records]
    all_pnls = [r["pnl_100"] for r in all_daily_records]
    stats_all = _evaluate(all_rets)
    stats_all["total_pnl_100"] = int(round(sum(all_pnls)))

    # 次のエントリー日
    today = date.today()
    today_dow = today.weekday()
    next_entries = []
    for d_offset in range(0, 7):
        from datetime import timedelta
        check = today + timedelta(days=d_offset)
        cdow = check.weekday()
        for cfg in LONG_CORE + LONG_SPOT + SHORT_CORE:
            if cfg["dow"] == cdow:
                next_entries.append({
                    "date": check.isoformat(),
                    "code": cfg["code"],
                    "name": cfg["name"],
                    "direction": "SHORT" if cfg in SHORT_CORE else "LONG",
                    "dow_label": DOW_LABELS[cdow],
                })
    next_entries = next_entries[:20]

    output = {
        "generated": datetime.now().isoformat(),
        "params": {
            "strategy": "Weekday Edge",
            "long_core": len(LONG_CORE),
            "long_spot": len(LONG_SPOT),
            "short_core": len(SHORT_CORE),
            "us_filter_long": "US前夜 ≤ +1%",
            "us_filter_short": "US前夜 ≥ -1%",
            "us_filter_spot": "US前夜 < -1% (アドバンテストのみ)",
            "hold": "寄成IN → 引成OUT (日計り)",
        },
        "stats_filtered": stats_filtered,
        "stats_all": stats_all,
        "max_dd_filtered": _max_dd_from_daily(us_filtered_daily_records),
        "max_dd_all": _max_dd_from_daily(all_daily_records),
        "yearly": yearly,
        "stock_stats": stock_stats,
        "next_entries": next_entries,
        "weekly": weekly_summary,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n[結果]")
    print(f"  USフィルタ有: PF={stats_filtered.get('pf', '—')}  WR={stats_filtered.get('wr', '—')}%  n={stats_filtered.get('total', 0)}")
    print(f"  USフィルタ無: PF={stats_all.get('pf', '—')}  WR={stats_all.get('wr', '—')}%  n={stats_all.get('total', 0)}")
    dd = _max_dd_from_daily(us_filtered_daily_records)
    print(f"  MaxDD(フィルタ有): {dd['amount']}円 / {dd['pct']}%")
    print(f"  週数: {len(weekly_summary)}")
    print(f"\n[OK] {OUTPUT_PATH}")

    # signals.parquet に当日シグナル行を merge
    if next_entries:
        sig_rows = []
        for e in next_entries:
            code = str(e["code"])
            ticker = code[:4] + ".T" if len(code) == 5 else code + ".T"
            sig_rows.append({
                "signal_date": pd.Timestamp(e["date"]),
                "ticker": ticker,
                "strategy": "weekday",
                "direction": e["direction"].lower(),
                "pair_id": "",
                "stock_name": e.get("name", ""),
                "entry_price_est": None,
                "prev_close": None,
            })
        if sig_rows:
            new_sigs = pd.DataFrame(sig_rows)
            if SIGNALS_PATH.exists():
                existing = pd.read_parquet(SIGNALS_PATH)
                other = existing[existing["strategy"] != "weekday"] if "strategy" in existing.columns else existing
                merged = pd.concat([new_sigs, other], ignore_index=True)
            else:
                merged = new_sigs
            SIGNALS_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = SIGNALS_PATH.parent / f"{SIGNALS_PATH.name}.tmp"
            merged.to_parquet(tmp, index=False)
            tmp.replace(SIGNALS_PATH)
            print(f"[OK] signals.parquet merged: {len(new_sigs)} rows (strategy=weekday / total={len(merged)})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
