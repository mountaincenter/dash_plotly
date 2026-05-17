#!/usr/bin/env python3
"""曜日×US前夜エッジ 週次トレードデータ生成。

J-Quants 10年検証で絞った運用候補を曜日別にLONG/SHORT。
USフィルタ: LONG=US前夜0%以下、SHORT=US前夜0%以上。
原則は寄成IN→引成OUT、例外は事前固定利確指値→大引不成。

出力: data/analysis/weekday_edge_trades.json
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

JST = timezone(timedelta(hours=9))

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
CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"

# --- 運用銘柄定義 ---
# J-Quants 10年検証(2016-05-15..2026-05-15)から、価格上限・USフィルタ・
# 同一銘柄同一曜日のLONG/SHORT衝突解消を通した「実運用候補」だけを採用する。
ACTIVE_WEEKDAY_CONFIGS = [
    {"code": "19510", "dow": 2, "name": "エクシオグループ", "direction": "LONG", "group": "principle", "capital_tier": "principle_under_15000", "us_filter": "us_red_green", "order_style": "open_close", "take_profit_pct": None, "exit_rule": "寄成IN→引成OUT", "risk_grade": "B_candidate", "expected_pf": 2.15, "expected_wr": 58.4, "expected_avg_ret": 0.3085},
    {"code": "68490", "dow": 0, "name": "日本光電工業", "direction": "SHORT", "group": "exception", "capital_tier": "principle_under_15000", "us_filter": "us_red_green", "order_style": "limit_close", "take_profit_pct": 2.0, "exit_rule": "寄成IN→利確2.0%指値/大引不成", "risk_grade": "B_candidate", "expected_pf": 1.75, "expected_wr": 52.2, "expected_avg_ret": 0.2579},
    {"code": "71640", "dow": 2, "name": "全国保証", "direction": "SHORT", "group": "exception", "capital_tier": "principle_under_15000", "us_filter": "us_red_green", "order_style": "limit_close", "take_profit_pct": 2.0, "exit_rule": "寄成IN→利確2.0%指値/大引不成", "risk_grade": "B_candidate", "expected_pf": 1.74, "expected_wr": 57.8, "expected_avg_ret": 0.2275},
    {"code": "80860", "dow": 0, "name": "ニプロ", "direction": "SHORT", "group": "exception", "capital_tier": "principle_under_15000", "us_filter": "us_red_green", "order_style": "limit_close", "take_profit_pct": 1.5, "exit_rule": "寄成IN→利確1.5%指値/大引不成", "risk_grade": "B_candidate", "expected_pf": 1.68, "expected_wr": 55.7, "expected_avg_ret": 0.2088},
    {"code": "94330", "dow": 0, "name": "ＫＤＤＩ", "direction": "LONG", "group": "principle", "capital_tier": "principle_under_15000", "us_filter": "us_red_green", "order_style": "open_close", "take_profit_pct": None, "exit_rule": "寄成IN→引成OUT", "risk_grade": "B_candidate", "expected_pf": 1.68, "expected_wr": 55.2, "expected_avg_ret": 0.2073},
    {"code": "23310", "dow": 0, "name": "ＡＬＳＯＫ", "direction": "LONG", "group": "principle", "capital_tier": "principle_under_15000", "us_filter": "us_red_green", "order_style": "open_close", "take_profit_pct": None, "exit_rule": "寄成IN→引成OUT", "risk_grade": "B_candidate", "expected_pf": 1.60, "expected_wr": 50.5, "expected_avg_ret": 0.2348},
    {"code": "34070", "dow": 2, "name": "旭化成", "direction": "SHORT", "group": "principle", "capital_tier": "principle_under_15000", "us_filter": "us_red_green", "order_style": "open_close", "take_profit_pct": None, "exit_rule": "寄成IN→引成OUT", "risk_grade": "B_candidate", "expected_pf": 1.60, "expected_wr": 57.4, "expected_avg_ret": 0.2292},
    {"code": "19440", "dow": 2, "name": "きんでん", "direction": "LONG", "group": "exception", "capital_tier": "principle_under_15000", "us_filter": "us_red_green", "order_style": "limit_close", "take_profit_pct": 2.0, "exit_rule": "寄成IN→利確2.0%指値/大引不成", "risk_grade": "B_candidate", "expected_pf": 1.66, "expected_wr": 54.7, "expected_avg_ret": 0.2028},
    {"code": "80310", "dow": 2, "name": "三井物産", "direction": "LONG", "group": "exception", "capital_tier": "principle_under_15000", "us_filter": "us_red_green", "order_style": "limit_close", "take_profit_pct": 1.25, "exit_rule": "寄成IN→利確1.25%指値/大引不成", "risk_grade": "B_candidate", "expected_pf": 1.55, "expected_wr": 59.2, "expected_avg_ret": 0.2008},
]

DOW_LABELS = {0: "月", 1: "火", 2: "水", 3: "木", 4: "金"}


def _fetch_sp500() -> dict[pd.Timestamp, float]:
    """S&P500 日次リターンを取得。ネットワーク不能時はローカルParquetを使う。"""
    local_path = PARQUET_DIR / "index_prices_max_1d.parquet"
    if local_path.exists():
        local = pd.read_parquet(local_path)
        sp = local[local["ticker"] == "^GSPC"][["date", "Close"]].copy()
        sp["date"] = pd.to_datetime(sp["date"])
        sp = sp.sort_values("date").set_index("date")
        sp["ret"] = sp["Close"].pct_change()
        return sp["ret"].to_dict()

    sp = yf.download("^GSPC", start="2016-05-01", progress=False, auto_adjust=False)
    if sp.empty:
        return {}
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


def _us_mask(sub: pd.DataFrame, cfg: dict) -> pd.Series:
    """前日夜に確定できるUS前夜条件だけでシグナルを絞る。"""
    direction = cfg["direction"]
    us_filter = cfg.get("us_filter")
    if us_filter != "us_red_green":
        return pd.Series(True, index=sub.index)
    if direction == "LONG":
        return sub["us_prev_ret"] <= 0.0
    return sub["us_prev_ret"] >= 0.0


def _order_fields(cfg: dict) -> dict:
    return {
        "capital_tier": cfg.get("capital_tier"),
        "order_style": cfg.get("order_style"),
        "take_profit_pct": cfg.get("take_profit_pct"),
        "exit_rule": cfg.get("exit_rule"),
        "risk_grade": cfg.get("risk_grade"),
        "expected_pf": cfg.get("expected_pf"),
        "expected_wr": cfg.get("expected_wr"),
        "expected_avg_ret": cfg.get("expected_avg_ret"),
    }


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

    all_configs = ACTIVE_WEEKDAY_CONFIGS

    print(f"\n[2] トレード計算 ({len(all_configs)}銘柄)")

    weekly_trades: dict[str, list] = {}  # "YYYY-WW" → trades
    all_daily_records: list[dict] = []
    us_filtered_daily_records: list[dict] = []

    stock_stats: list[dict] = []

    for cfg in all_configs:
        code, dow, direction = cfg["code"], cfg["dow"], cfg["direction"]
        sign = -1 if direction == "SHORT" else 1

        sub = prices[(prices["code"] == code) & (prices["dow"] == dow)].dropna(
            subset=["adj_open", "adj_close"]
        ).copy()

        # 寄→引PnL
        sub["ret_pct"] = (sub["adj_close"] / sub["adj_open"] - 1) * sign * 100
        sub["pnl_100"] = (sub["adj_close"] - sub["adj_open"]) * sign * 100

        us_mask = _us_mask(sub, cfg)

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
                **_order_fields(cfg),
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
            **_order_fields(cfg),
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

    # 次のエントリー日（calendar.parquetの営業日のみ）
    # 15時以降は当日取引終了済みなので翌営業日を起点にする
    now_jst = datetime.now(JST)
    today = now_jst.date()
    _cal = pd.read_parquet(CALENDAR_PATH)
    _cal["date"] = pd.to_datetime(_cal["date"])
    trading_days: set = set(_cal["date"].dt.date.tolist())
    trading_days_sorted = sorted(trading_days)

    def _next_trading_day_from(d: date) -> date | None:
        for td in trading_days_sorted:
            if td > d:
                return td
        return None

    if now_jst.hour >= 15:
        start_date = _next_trading_day_from(today)
        if start_date is None:
            start_date = today
    else:
        start_date = today

    # 決算発表日: code(5桁) → set of dates
    # announcements.parquet(次回予定) + fins_summary.parquet(過去実績)
    earnings_dates: dict[str, set[date]] = {}
    ann_path = PARQUET_DIR / "announcements.parquet"
    if ann_path.exists():
        ann = pd.read_parquet(ann_path)
        for _, r in ann.iterrows():
            raw = str(r.get("ticker", r.get("code", ""))).replace(".T", "")
            code5 = raw + "0" if len(raw) == 4 else raw
            try:
                d = date.fromisoformat(str(r["announcementDate"]))
                earnings_dates.setdefault(code5, set()).add(d)
            except (ValueError, KeyError):
                pass
    fins_path = PARQUET_DIR / "fins_summary.parquet"
    if fins_path.exists():
        fins = pd.read_parquet(fins_path)
        for _, r in fins.iterrows():
            code5 = str(r["Code"])
            try:
                d = pd.to_datetime(r["DiscDate"]).date()
                earnings_dates.setdefault(code5, set()).add(d)
            except (ValueError, KeyError):
                pass

    def _next_trading_day(d: date) -> date | None:
        return _next_trading_day_from(d)

    next_entries = []
    if start_date and start_date in trading_days:
        cdow = start_date.weekday()
        for cfg in ACTIVE_WEEKDAY_CONFIGS:
            if cfg["dow"] != cdow:
                continue
            entry: dict = {
                "date": start_date.isoformat(),
                "code": cfg["code"],
                "name": cfg["name"],
                "direction": cfg["direction"],
                "dow_label": DOW_LABELS[cdow],
                **_order_fields(cfg),
            }
            edates = earnings_dates.get(cfg["code"], set())
            for ed in edates:
                ntd = _next_trading_day(ed)
                if start_date == ed or (ntd and start_date == ntd):
                    entry["earnings_alert"] = ed.isoformat()
                    break
            next_entries.append(entry)

    output = {
        "generated": datetime.now(JST).isoformat(),
        "next_trading_date": start_date.isoformat() if start_date else None,
        "params": {
            "strategy": "Weekday Edge Operable v20260515",
            "active_configs": len(ACTIVE_WEEKDAY_CONFIGS),
            "principle": "寄成IN→引成OUT",
            "exception": "寄成IN→事前固定利確指値/大引不成",
            "us_filter": "us_red_green: LONG=US前夜<=0%, SHORT=US前夜>=0%",
            "capital_rule": "原則: 15000円以下。例外/特別枠は別管理。",
            "conflict_rule": "同一銘柄・同一曜日のLONG/SHORT衝突は片方向のみ採用",
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
                "expected_pf": e.get("expected_pf"),
                "expected_wr": e.get("expected_wr"),
                "expected_avg_ret": e.get("expected_avg_ret"),
                "order_style": e.get("order_style"),
                "take_profit_pct": e.get("take_profit_pct"),
                "exit_rule": e.get("exit_rule"),
                "capital_tier": e.get("capital_tier"),
                "risk_grade": e.get("risk_grade"),
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
