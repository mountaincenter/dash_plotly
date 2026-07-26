from __future__ import annotations

import csv
import gzip
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

import generate_technical_entry_training as base


DASH_ROOT = Path(__file__).resolve().parents[4]
DAILY_PATH = DASH_ROOT / "data/parquet/grok_prices_max_1d.parquet"
MINUTE_ROOT = DASH_ROOT / "data/research/jquants_all_market/minute"
RAW_TICK_ROOT = DASH_ROOT / "data/research/jquants_tick/raw"
OUTPUT_ROOT = (
    DASH_ROOT
    / "data/research/grok_session_handoffs_20260718/04_technical_entry_training"
    / "output/nextjs"
)
OUTPUT_PUBLIC = OUTPUT_ROOT / "supplemental_2459_public.json"
OUTPUT_RESULTS = OUTPUT_ROOT / "supplemental_2459_results.json"
DEFAULT_CUTOFF = "09:30"
TICKER = "2459.T"
JQUANTS_CODE = "24590"
NAME = "アウンコンサルティング"


CASE_SPECS: list[dict[str, str]] = [
    {
        "id": "2459_20260721_0930",
        "ticker": TICKER,
        "date": "2026-07-21",
        "cutoff": DEFAULT_CUTOFF,
        "dailyGuide": "sell",
        "intradayGuide": "wait",
        "pattern": "下降基調・薄商い",
        "lesson": "日足下降基調 × 09:30まで8約定 × 静観",
        "rationale": (
            "日足は25・75・200SMAの下で推移。一方、09:30までのtickは"
            "8約定だけで、最後の約定も09:22。売り目線でも執行判断に必要な"
            "流動性と値動きが不足しているため静観する例。"
        ),
        "invalidation": "約定が継続し、VWAPの片側へ値幅を伴って定着したら再評価する。",
        "warning": "少ない約定を通常の連続した値動きとして読まない。",
    },
    {
        "id": "2459_20260722_0930",
        "ticker": TICKER,
        "date": "2026-07-22",
        "cutoff": DEFAULT_CUTOFF,
        "dailyGuide": "sell",
        "intradayGuide": "wait",
        "pattern": "方向なし・約定空白",
        "lesson": "日足下降基調 × 09:04以降約定なし × 静観",
        "rationale": (
            "日足は主要SMAの下で弱いが、09:30までのtickは4約定だけで、"
            "09:04から判断時刻まで約定がない。方向より先に執行可能性を"
            "確認すべき場面。"
        ),
        "invalidation": "約定頻度が回復し、朝レンジを出来高付きで抜けたら再評価する。",
        "warning": "値段が動いていないことと、支持・抵抗が機能したことを混同しない。",
    },
    {
        "id": "2459_20260723_0930",
        "ticker": TICKER,
        "date": "2026-07-23",
        "cutoff": DEFAULT_CUTOFF,
        "dailyGuide": "wait",
        "intradayGuide": "wait",
        "pattern": "遅い初約定・急騰",
        "lesson": "下降日足 × 09:21初約定 × 急騰後の静観",
        "rationale": (
            "前日までの日足は下降基調だが、当日は09:21に初約定して"
            "161円から211円まで急変。09:30時点では分足が1本しかなく、"
            "次の約定も未確認のため、買い追随・逆張り売りとも静観する例。"
        ),
        "invalidation": "継続約定が始まり、急騰レンジからの離脱方向が確認できたら再評価する。",
        "warning": "結果を見て、1本だけの分足に継続性を後付けしない。",
    },
    {
        "id": "2459_20260724_0930",
        "ticker": TICKER,
        "date": "2026-07-24",
        "cutoff": DEFAULT_CUTOFF,
        "dailyGuide": "wait",
        "intradayGuide": "sell",
        "pattern": "急騰翌日の戻り失敗",
        "lesson": "前日急騰 × GD × VWAP・短期線下",
        "rationale": (
            "前日は161円から211円へ急騰してRSIも高い。当日はGD後に"
            "207円まで戻したが、09:30時点の196円はVWAP・5SMA・20SMAを"
            "下回る。高ボラを前提に戻り失敗を売りで判断する例。"
        ),
        "invalidation": "VWAPと直近戻り高値を回復し、その上で定着する。",
        "warning": "前日の強さだけで買わず、当日09:30までの戻りの成否を分けて見る。",
    },
]


def load_daily(max_date: str) -> pd.DataFrame:
    daily = pd.read_parquet(
        DAILY_PATH,
        filters=[("ticker", "==", TICKER)],
    ).rename(
        columns={
            "date": "trading_date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    daily["trading_date"] = pd.to_datetime(daily["trading_date"]).dt.normalize()
    daily = daily[
        ["trading_date", "ticker", "open", "high", "low", "close", "volume"]
    ].dropna()

    latest = daily["trading_date"].max()
    additions: list[dict[str, Any]] = []
    for path in sorted(MINUTE_ROOT.glob("trading_date=*/part-000.parquet")):
        trading_date = pd.Timestamp(path.parent.name.split("=", 1)[1])
        if trading_date <= latest or trading_date > pd.Timestamp(max_date):
            continue
        minute = pd.read_parquet(
            path,
            filters=[("ticker", "==", TICKER)],
            columns=[
                "ticker",
                "datetime",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ],
        ).sort_values("datetime")
        if minute.empty:
            continue
        additions.append(
            {
                "trading_date": trading_date,
                "ticker": TICKER,
                "open": float(minute["open"].iloc[0]),
                "high": float(minute["high"].max()),
                "low": float(minute["low"].min()),
                "close": float(minute["close"].iloc[-1]),
                "volume": float(minute["volume"].sum()),
            }
        )

    if additions:
        daily = pd.concat([daily, pd.DataFrame(additions)], ignore_index=True)
    return (
        daily.drop_duplicates(["ticker", "trading_date"], keep="last")
        .sort_values(["ticker", "trading_date"])
        .reset_index(drop=True)
    )


def load_minute_day(trading_date: str) -> pd.DataFrame:
    path = MINUTE_ROOT / f"trading_date={trading_date}" / "part-000.parquet"
    minute = pd.read_parquet(
        path,
        filters=[("ticker", "==", TICKER)],
        columns=[
            "ticker",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "value",
        ],
    )
    minute["datetime"] = pd.to_datetime(minute["datetime"])
    return minute


def load_tick_day(trading_date: str) -> pd.DataFrame:
    date_token = trading_date.replace("-", "")
    path = (
        RAW_TICK_ROOT
        / f"trading_date={trading_date}"
        / f"equities_trades_{date_token}.csv.gz"
    )
    rows: list[dict[str, Any]] = []
    with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("Code") != JQUANTS_CODE:
                continue
            price = float(row["Price"])
            volume = float(row["TradingVolume"])
            rows.append(
                {
                    "ticker": TICKER,
                    "datetime": pd.Timestamp(f"{trading_date} {row['Time']}"),
                    "price": price,
                    "trading_volume": volume,
                    "turnover": price * volume,
                }
            )
    return pd.DataFrame(rows).sort_values("datetime").reset_index(drop=True)


def validate_payloads(
    public_cases: list[dict[str, Any]],
    result_cases: list[dict[str, Any]],
) -> None:
    if len(public_cases) != len(CASE_SPECS) or len(result_cases) != len(CASE_SPECS):
        raise AssertionError("2459 supplemental case count mismatch")
    if {case["id"] for case in public_cases} != {
        case["id"] for case in result_cases
    }:
        raise AssertionError("2459 public/result case IDs mismatch")

    result_by_id = {case["id"]: case for case in result_cases}
    forbidden = {"guidance", "outcomes", "fullIntraday", "fullTickTempo"}
    for public_case in public_cases:
        case_id = public_case["id"]
        cutoff = pd.Timestamp(f"{public_case['date']} {public_case['cutoff']}")
        if public_case["cutoff"] != DEFAULT_CUTOFF:
            raise AssertionError(f"{case_id}: cutoff is not {DEFAULT_CUTOFF}")
        if forbidden.intersection(public_case):
            raise AssertionError(f"{case_id}: future data leaked into public payload")
        if pd.Timestamp(public_case["daily"][-1]["date"]) >= pd.Timestamp(
            public_case["date"]
        ):
            raise AssertionError(f"{case_id}: target daily bar leaked")
        if max(
            pd.Timestamp(row["datetime"]) for row in public_case["intraday"]
        ) >= cutoff:
            raise AssertionError(f"{case_id}: future minute leaked")
        if max(pd.Timestamp(row["end"]) for row in public_case["tickTempo"]) > cutoff:
            raise AssertionError(f"{case_id}: future tick tempo leaked")

        outcomes = result_by_id[case_id]["outcomes"]
        buy = outcomes["buy"]
        sell = outcomes["sell"]
        if pd.Timestamp(buy["entryTime"]) <= cutoff:
            raise AssertionError(f"{case_id}: entry is not after cutoff")
        if not math.isclose(buy["stopLevel"], buy["entryPrice"] - 50):
            raise AssertionError(f"{case_id}: buy stop level mismatch")
        if not math.isclose(sell["stopLevel"], sell["entryPrice"] + 50):
            raise AssertionError(f"{case_id}: sell stop level mismatch")
        if not math.isclose(buy["closePnl"], -sell["closePnl"]):
            raise AssertionError(f"{case_id}: close P&L mismatch")
        if not math.isclose(buy["mfe"], -sell["mae"]):
            raise AssertionError(f"{case_id}: buy MFE/sell MAE mismatch")
        if not math.isclose(buy["mae"], -sell["mfe"]):
            raise AssertionError(f"{case_id}: buy MAE/sell MFE mismatch")


def main() -> None:
    dates = sorted({spec["date"] for spec in CASE_SPECS})
    base.validate_manifests(dates)
    daily = load_daily(max(dates))
    public_cases: list[dict[str, Any]] = []
    result_cases: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []

    for spec in CASE_SPECS:
        public_case, result_case, audit = base.build_case(
            spec,
            NAME,
            daily,
            load_minute_day(spec["date"]),
            load_tick_day(spec["date"]),
        )
        public_cases.append(public_case)
        result_cases.append(result_case)
        audits.append(audit)

    validate_payloads(public_cases, result_cases)
    generated_at = pd.Timestamp.now(tz="Asia/Tokyo").isoformat()
    public_data = {
        "generatedAt": generated_at,
        "caseCount": len(public_cases),
        "sources": {
            "daily": str(DAILY_PATH.relative_to(DASH_ROOT)),
            "minute": str(MINUTE_ROOT.relative_to(DASH_ROOT)),
            "tick": str(RAW_TICK_ROOT.relative_to(DASH_ROOT)),
        },
        "cases": public_cases,
    }
    result_data = {
        "generatedAt": generated_at,
        "caseCount": len(result_cases),
        "cases": result_cases,
    }

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    OUTPUT_PUBLIC.write_text(
        json.dumps(public_data, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    OUTPUT_RESULTS.write_text(
        json.dumps(result_data, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    print(f"generated: {OUTPUT_PUBLIC}")
    print(f"generated: {OUTPUT_RESULTS}")
    print(f"cases: {len(public_cases)}")
    for audit in audits:
        print(
            "audit:",
            audit["id"],
            f"minute={audit['minuteRows']}",
            f"tick={audit['tickRows']}",
            f"entry={audit['entryTime']}@{audit['entryPrice']}",
        )
    print("validation: future_fields=absent, result_case_ids=matched, cutoff=09:30")


if __name__ == "__main__":
    main()
