#!/usr/bin/env python3
"""SQ+1d ショートトレード月次データ生成。

sq4_trades.json と同一フォーマットで sq_plus1_trades.json を生成。
SQ翌営業日(月曜) に前日(SQ金曜)上昇Top N を寄成ショート→引成買戻し。
CME下落→Top5, それ以外→Top10。
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

from common_cfg.paths import PARQUET_DIR

PRICES_PATH = PARQUET_DIR / "prices_topix500_oc.parquet"
FUTURES_PATH = PARQUET_DIR / "futures_prices_max_1d.parquet"
CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"
OUTPUT_PATH = ROOT / "data" / "analysis" / "sq_plus1_trades.json"
SIGNALS_PATH = PARQUET_DIR / "signals.parquet"


def _load_master() -> dict[str, str]:
    """jquants eq master から Code→CoName マッピング取得"""
    try:
        result = subprocess.run(
            ["jquants", "--output", "csv", "-f", "Code,CoName", "eq", "master"],
            capture_output=True, text=True, check=True,
        )
        df = pd.read_csv(io.StringIO(result.stdout))
        df["Code"] = df["Code"].astype(str).str.zfill(5)
        return df.set_index("Code")["CoName"].to_dict()
    except Exception as e:
        print(f"[WARN] master取得失敗: {e}")
        return {}


def _get_sq_dates(cal: pd.DataFrame) -> list[date]:
    """calendar.parquet から SQ日リストを取得"""
    sq_rows = cal[cal["sq_day"] == True]
    return sorted(sq_rows["date"].tolist())


def _evaluate(pnl_list: list[float]) -> dict:
    if not pnl_list:
        return {}
    arr = np.array(pnl_list)
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


def _max_dd(pnl_list: list[float]) -> dict:
    if not pnl_list:
        return {"amount": 0, "pct": 0.0}
    cum = np.cumsum(pnl_list)
    peak = np.maximum.accumulate(cum)
    dd = cum - peak
    return {"amount": int(round(dd.min())), "pct": round(float(dd.min()), 2)}


def main() -> int:
    print("=" * 60)
    print("Generate SQ+1d Short Trades JSON")
    print("=" * 60)

    master = _load_master()

    prices = pd.read_parquet(PRICES_PATH)
    prices.columns = ["date", "code", "adj_open", "adj_close"]
    prices["date"] = pd.to_datetime(prices["date"]).dt.date
    prices["code"] = prices["code"].astype(str).str.zfill(5)
    prices = prices.sort_values(["code", "date"])

    prices["prev_close"] = prices.groupby("code")["adj_close"].shift(1)
    prices = prices.dropna(subset=["prev_close"])
    prices["ret_total"] = prices["adj_close"] / prices["prev_close"] - 1

    trading_days = sorted(prices["date"].unique())
    td_idx = {d: i for i, d in enumerate(trading_days)}

    # CME
    fut = pd.read_parquet(FUTURES_PATH)
    nkd = fut[fut["ticker"] == "NKD=F"].dropna(subset=["Close"]).copy()
    nkd["date"] = pd.to_datetime(nkd["date"]).dt.date
    nkd = nkd.sort_values("date")
    nkd["prev_close"] = nkd["Close"].shift(1)
    nkd["cme_ret"] = nkd["Close"] / nkd["prev_close"] - 1
    cme_map = {}
    for _, row in nkd.iterrows():
        cme_map[row["date"]] = {
            "close": float(row["Close"]),
            "prev_close": float(row["prev_close"]) if pd.notna(row["prev_close"]) else None,
            "ret": float(row["cme_ret"]) if pd.notna(row["cme_ret"]) else None,
        }

    # calendar
    cal = pd.read_parquet(CALENDAR_PATH)
    sq_dates = _get_sq_dates(cal)

    # SQ+1日を特定
    plus1_dates = cal[cal.get("sq_plus1_short", pd.Series(dtype=bool)) == True]["date"].tolist()

    monthly = []
    all_pnl_ret = []
    all_pnl_100 = []
    cme_down_pnl_ret = []
    cme_down_pnl_100 = []
    cme_up_pnl_ret = []
    cme_up_pnl_100 = []

    for sq_date in sq_dates:
        if sq_date not in td_idx:
            continue
        sq_i = td_idx[sq_date]
        if sq_i + 1 >= len(trading_days):
            continue
        plus1_date = trading_days[sq_i + 1]

        # SQ+1日の株価データ
        day = prices[prices["date"] == plus1_date].copy()
        if day.empty:
            continue

        # 価格フィルタ 1000-20000円
        day = day[(day["prev_close"] >= 1000) & (day["prev_close"] <= 20000)]

        # 前日リターン（= SQ日のリターン）
        sq_day = prices[prices["date"] == sq_date].copy()
        sq_ret_map = sq_day.set_index("code")["ret_total"].to_dict()
        day["prev_day_ret"] = day["code"].map(sq_ret_map)
        day = day.dropna(subset=["prev_day_ret"])

        if len(day) < 10:
            continue

        # CME判定
        cme_info = cme_map.get(sq_date)
        cme_down = cme_info and cme_info["ret"] is not None and cme_info["ret"] < 0
        top_n = 5 if cme_down else 10

        # 前日上昇Top N
        top_up = day.nlargest(top_n, "prev_day_ret")

        picks = []
        month_pnl_ret = []
        month_pnl_100 = []

        for _, row in top_up.iterrows():
            # ショートPnL: 寄成売り → 引成買戻し
            short_ret = -(row["adj_close"] / row["adj_open"] - 1) * 100
            short_pnl_100 = -(row["adj_close"] - row["adj_open"]) * 100

            month_pnl_ret.append(short_ret)
            month_pnl_100.append(short_pnl_100)

            picks.append({
                "code": row["code"],
                "name": master.get(row["code"], ""),
                "prev_close": round(float(row["prev_close"]), 1),
                "prev_day_ret": round(float(row["prev_day_ret"] * 100), 2),
                "entry_price": round(float(row["adj_open"]), 1),
                "exit_price": round(float(row["adj_close"]), 1),
                "ret_pct": round(float(short_ret), 2),
                "pnl_100": int(round(short_pnl_100)),
            })

        cme_change = None
        cme_ret_pct = None
        if cme_info and cme_info["prev_close"]:
            cme_change = int(round(cme_info["close"] - cme_info["prev_close"]))
            cme_ret_pct = round(cme_info["ret"] * 100, 2) if cme_info["ret"] else None

        monthly.append({
            "month": sq_date.strftime("%Y-%m") if isinstance(sq_date, date) else str(sq_date)[:7],
            "sq_date": str(sq_date),
            "entry_date": str(plus1_date),
            "exit_date": str(plus1_date),
            "n_picks": len(picks),
            "total_ret": round(sum(month_pnl_ret), 2),
            "total_pnl_100": int(round(sum(month_pnl_100))),
            "cme_change": cme_change,
            "cme_ret": cme_ret_pct,
            "cme_direction": "DOWN" if cme_down else "UP",
            "picks": picks,
        })

        all_pnl_ret.extend(month_pnl_ret)
        all_pnl_100.extend(month_pnl_100)
        if cme_down:
            cme_down_pnl_ret.extend(month_pnl_ret)
            cme_down_pnl_100.extend(month_pnl_100)
        else:
            cme_up_pnl_ret.extend(month_pnl_ret)
            cme_up_pnl_100.extend(month_pnl_100)

    # next SQ+1 — calendar.parquetから翌営業日を取得（pricesにない未来日にも対応）
    today = date.today()
    cal_dates = sorted(cal[cal["date"] >= today]["date"].tolist())
    next_sq_plus1 = None
    for sq_d in sq_dates:
        if sq_d < today:
            if sq_d in td_idx:
                sq_i = td_idx[sq_d]
                if sq_i + 1 < len(trading_days):
                    p1 = trading_days[sq_i + 1]
                    if p1 < today:
                        continue
                else:
                    # pricesの最終日がSQ日: calendarから翌営業日を取得
                    p1_candidates = [d for d in cal_dates if d > sq_d]
                    if not p1_candidates:
                        continue
                    p1 = p1_candidates[0]
            else:
                continue
        else:
            p1_candidates = [d for d in cal_dates if d > sq_d]
            if not p1_candidates:
                continue
            p1 = p1_candidates[0]

        next_info: dict = {"sq_date": str(sq_d), "entry_date": str(p1)}
        # SQ日の価格が確定していれば候補銘柄を生成
        sq_prices = prices[prices["date"] == sq_d].copy()
        if not sq_prices.empty:
            sq_prices = sq_prices[(sq_prices["prev_close"] >= 1000) & (sq_prices["prev_close"] <= 20000)]
            sq_prices = sq_prices.dropna(subset=["ret_total"])
            if len(sq_prices) >= 10:
                cme_info = cme_map.get(sq_d)
                cme_down = cme_info and cme_info["ret"] is not None and cme_info["ret"] < 0
                top_n = 5 if cme_down else 10
                top_up = sq_prices.nlargest(top_n, "ret_total")
                next_info["cme_direction"] = "DOWN" if cme_down else "UP"
                next_info["top_n"] = top_n
                next_info["picks"] = [
                    {
                        "code": row["code"],
                        "name": master.get(row["code"], ""),
                        "prev_close": round(float(row["prev_close"]), 1),
                        "prev_day_ret": round(float(row["ret_total"] * 100), 2),
                    }
                    for _, row in top_up.iterrows()
                ]
        next_sq_plus1 = next_info
        break

    stats_ret = _evaluate(all_pnl_ret)
    stats_ret["total_pnl_100"] = int(round(sum(all_pnl_100))) if all_pnl_100 else 0

    stats_cme_down = _evaluate(cme_down_pnl_ret)
    stats_cme_down["total_pnl_100"] = int(round(sum(cme_down_pnl_100))) if cme_down_pnl_100 else 0

    stats_cme_up = _evaluate(cme_up_pnl_ret)
    stats_cme_up["total_pnl_100"] = int(round(sum(cme_up_pnl_100))) if cme_up_pnl_100 else 0

    output = {
        "generated": datetime.now().isoformat(),
        "params": {
            "strategy": "SQ+1d SHORT",
            "selection": "前日(SQ日)上昇Top N",
            "top_n_cme_down": 5,
            "top_n_default": 10,
            "price_range": [1000, 20000],
            "hold": "寄成SHORT→引成買戻し",
        },
        "stats": stats_ret,
        "stats_cme_down": stats_cme_down,
        "stats_cme_up": stats_cme_up,
        "max_dd": _max_dd(all_pnl_100),
        "max_dd_cme_down": _max_dd(cme_down_pnl_100),
        "next_sq_plus1": next_sq_plus1,
        "monthly": monthly,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n  月数: {len(monthly)}")
    print(f"  トレード数: {stats_ret.get('total', 0)}")
    print(f"  PF: {stats_ret.get('pf', '—')}")
    print(f"  WR: {stats_ret.get('wr', '—')}%")
    print(f"  累計PnL(100株): {stats_ret.get('total_pnl_100', 0):,}")
    print(f"  CME↓ PF: {stats_cme_down.get('pf', '—')} (n={stats_cme_down.get('total', 0)})")
    print(f"  CME↑ PF: {stats_cme_up.get('pf', '—')} (n={stats_cme_up.get('total', 0)})")
    print(f"\n[OK] {OUTPUT_PATH}")

    # signals.parquet に当日シグナル行を merge
    if next_sq_plus1 and "picks" in next_sq_plus1:
        sig_rows = []
        for p in next_sq_plus1["picks"]:
            code = str(p["code"])
            ticker = code[:4] + ".T" if len(code) == 5 else code + ".T"
            sig_rows.append({
                "signal_date": pd.Timestamp(next_sq_plus1["entry_date"]),
                "ticker": ticker,
                "strategy": "sq_plus1",
                "direction": "short",
                "pair_id": "",
                "stock_name": p.get("name", ""),
                "entry_price_est": p.get("prev_close"),
                "prev_close": p.get("prev_close"),
            })
        if sig_rows:
            new_sigs = pd.DataFrame(sig_rows)
            if SIGNALS_PATH.exists():
                existing = pd.read_parquet(SIGNALS_PATH)
                other = existing[existing["strategy"] != "sq_plus1"] if "strategy" in existing.columns else existing
                merged = pd.concat([new_sigs, other], ignore_index=True)
            else:
                merged = new_sigs
            SIGNALS_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = SIGNALS_PATH.parent / f"{SIGNALS_PATH.name}.tmp"
            merged.to_parquet(tmp, index=False)
            tmp.replace(SIGNALS_PATH)
            print(f"[OK] signals.parquet merged: {len(new_sigs)} rows (strategy=sq_plus1 / total={len(merged)})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
