#!/usr/bin/env python3
"""
generate_quarter_end.py
1306 ETF 四半期末戦略のバックテスト結果を生成 → quarter_end_effect.json

戦略:
  - 四半期末(3/6/9/12)の残4日に引成買い → 残3日に引成売り (残4戦略)
  - 四半期末(3/6/9/12)の残3日に引成買い → 残2日に引成売り (残3戦略)

実行方法:
    python3 scripts/pipeline/generate_quarter_end.py
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from common_cfg.paths import PARQUET_DIR

CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"
ETF_PATH = PARQUET_DIR / "etf_1306_prices.parquet"
OUTPUT_PATH = ROOT / "data" / "analysis" / "quarter_end_effect.json"


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    cal = pd.read_parquet(CALENDAR_PATH)
    cal["date"] = pd.to_datetime(cal["date"])

    prices = pd.read_parquet(ETF_PATH)
    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices.sort_values("date").reset_index(drop=True)

    return cal, prices


def generate_trades(cal: pd.DataFrame, prices: pd.DataFrame) -> list[dict]:
    price_map = {}
    for _, row in prices.iterrows():
        price_map[row["date"].strftime("%Y-%m-%d")] = row["Close"]

    trades = []

    for qe_month in [3, 6, 9, 12]:
        for year in cal["date"].dt.year.unique():
            month_rows = cal[
                (cal["date"].dt.year == year) & (cal["date"].dt.month == qe_month)
            ]
            if month_rows.empty:
                continue

            # 残4→残3 戦略
            buy_4 = month_rows[month_rows["qe_remain"] == 4]
            sell_3 = month_rows[month_rows["qe_remain"] == 3]
            if not buy_4.empty and not sell_3.empty:
                entry_date = buy_4.iloc[0]["date"].strftime("%Y-%m-%d")
                exit_date = sell_3.iloc[0]["date"].strftime("%Y-%m-%d")
                entry_p = price_map.get(entry_date)
                exit_p = price_map.get(exit_date)
                if entry_p is not None and exit_p is not None and entry_p > 0:
                    ret_pct = round((exit_p / entry_p - 1) * 100, 4)
                    trades.append({
                        "entry_date": entry_date,
                        "exit_date": exit_date,
                        "strategy": "remain4",
                        "year": int(year),
                        "quarter": f"{qe_month // 3}Q",
                        "ret_pct": ret_pct,
                    })

            # 残3→残2 戦略
            buy_3 = month_rows[month_rows["qe_remain"] == 3]
            sell_2 = month_rows[month_rows["qe_remain"] == 2]
            if not buy_3.empty and not sell_2.empty:
                entry_date = buy_3.iloc[0]["date"].strftime("%Y-%m-%d")
                exit_date = sell_2.iloc[0]["date"].strftime("%Y-%m-%d")
                entry_p = price_map.get(entry_date)
                exit_p = price_map.get(exit_date)
                if entry_p is not None and exit_p is not None and entry_p > 0:
                    ret_pct = round((exit_p / entry_p - 1) * 100, 4)
                    trades.append({
                        "entry_date": entry_date,
                        "exit_date": exit_date,
                        "strategy": "remain3",
                        "year": int(year),
                        "quarter": f"{qe_month // 3}Q",
                        "ret_pct": ret_pct,
                    })

    trades.sort(key=lambda t: t["entry_date"])
    return trades


def calc_stats(trades: list[dict]) -> dict:
    if not trades:
        return {"total": 0, "wins": 0, "losses": 0, "wr": 0, "avg": 0,
                "median": 0, "max": 0, "min": 0, "pf": 0, "total_ret": 0}

    rets = [t["ret_pct"] for t in trades]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]

    total = len(rets)
    pf = round(sum(wins) / abs(sum(losses)), 2) if losses and sum(losses) != 0 else None

    return {
        "total": total,
        "wins": len(wins),
        "losses": len(losses),
        "wr": round(len(wins) / total * 100, 1),
        "avg": round(sum(rets) / total, 4),
        "median": round(median(rets), 4),
        "max": round(max(rets), 4),
        "min": round(min(rets), 4),
        "pf": pf,
        "total_ret": round(sum(rets), 4),
    }


def main() -> int:
    print("=" * 60)
    print("Generate Quarter End Effect JSON (1306 ETF)")
    print("=" * 60)

    cal, prices = load_data()
    print(f"  Calendar: {len(cal)} days")
    print(f"  1306 prices: {len(prices)} days ({prices['date'].min().date()} → {prices['date'].max().date()})")

    trades = generate_trades(cal, prices)
    stats = calc_stats(trades)

    print(f"\n  Trades: {stats['total']} (W:{stats['wins']} L:{stats['losses']})")
    print(f"  WR: {stats['wr']}%  PF: {stats['pf']}  Total: {stats['total_ret']}%")

    result = {
        "generated": datetime.now().isoformat(),
        "trades": trades,
        "stats": stats,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\n[OK] {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
