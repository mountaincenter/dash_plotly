#!/usr/bin/env python3
"""
generate_calendar.py
営業日カレンダーにSQ-4/1306四半期末フラグを付与してcalendar.parquet生成

年1回実行（年初 or 臨時休場時に再生成）
データソース: jquants mkt calendar

実行方法:
    python3 scripts/pipeline/generate_calendar.py
"""
from __future__ import annotations

import io
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from common_cfg.paths import PARQUET_DIR

START_YEAR = 2022
END_YEAR = date.today().year
OUTPUT_PATH = PARQUET_DIR / "calendar.parquet"


def fetch_business_days(year: int) -> list[date]:
    """jquants mkt calendar から営業日リストを取得"""
    result = subprocess.run(
        [
            "jquants", "--output", "csv",
            "mkt", "calendar",
            "--from", f"{year}-01-01",
            "--to", f"{year}-12-31",
        ],
        capture_output=True, text=True, check=True,
    )
    df = pd.read_csv(io.StringIO(result.stdout))
    # HolDiv==1 が営業日（スキーマ説明と実データが逆）
    bdays = df[df["HolDiv"] == 1]["Date"].apply(date.fromisoformat).tolist()
    if not bdays:
        raise ValueError(f"{year}年の営業日が0件。jquants認証またはAPI応答を確認")
    print(f"[INFO] {year}年 営業日: {len(bdays)}日")
    return bdays


def find_sq_days(bdays: list[date]) -> dict[int, date]:
    """各月のSQ日（第2金曜、祝日なら前営業日）を特定"""
    bday_set = set(bdays)
    sq_days: dict[int, date] = {}
    year = bdays[0].year

    for month in range(1, 13):
        fridays = []
        d = date(year, month, 1)
        while d.month == month:
            if d.weekday() == 4:  # Friday
                fridays.append(d)
            d = _next_day(d)

        second_friday = fridays[1] if len(fridays) >= 2 else fridays[-1]

        if second_friday in bday_set:
            sq_days[month] = second_friday
        else:
            # _bday_index は target 以下の最大営業日を返す
            idx = _bday_index(bdays, second_friday)
            sq_days[month] = bdays[idx]

    return sq_days


def _next_day(d: date) -> date:
    return date.fromordinal(d.toordinal() + 1)


def _bday_index(bdays: list[date], target: date) -> int:
    """target以下の最大の営業日インデックスを返す"""
    for i in range(len(bdays) - 1, -1, -1):
        if bdays[i] <= target:
            return i
    return 0


def calc_sq_flags(bdays: list[date], sq_days: dict[int, date]) -> dict[date, dict]:
    """SQ-4（エントリー）とSQ-3（イグジット）を営業日逆算"""
    flags: dict[date, dict] = {}
    bday_list = list(bdays)

    for month, sq_date in sq_days.items():
        sq_idx = bday_list.index(sq_date)

        # SQ-4 = 4営業日前、SQ-3 = 3営業日前
        if sq_idx >= 4:
            sq4 = bday_list[sq_idx - 4]
            sq3 = bday_list[sq_idx - 3]

            flags.setdefault(sq_date, {})["sq_day"] = True
            flags.setdefault(sq4, {})["sq4_entry"] = True
            flags.setdefault(sq3, {})["sq3_exit"] = True

            print(f"  {month:2d}月 SQ={sq_date} | SQ-4={sq4}(買) → SQ-3={sq3}(売)")

    return flags


def calc_quarter_end_flags(bdays: list[date]) -> dict[date, dict]:
    """四半期末(3/6/9/12)の残営業日フラグ"""
    flags: dict[date, dict] = {}

    for qe_month in [3, 6, 9, 12]:
        # 当月の営業日を抽出
        month_bdays = [d for d in bdays if d.month == qe_month]
        if not month_bdays:
            continue

        # 末尾から残日数を付与（最終営業日=残1）
        for i, d in enumerate(reversed(month_bdays)):
            remain = i + 1
            if remain > 4:
                break

            entry = flags.setdefault(d, {})
            entry["qe_remain"] = remain

            # 1306: 残4日・残3日 = 引成買い
            if remain in (4, 3):
                entry["qe_1306_buy"] = True
            # 1306: 残3日・残2日 = 引成売り
            if remain in (3, 2):
                entry["qe_1306_sell"] = True

        print(f"  {qe_month:2d}月末 残4={month_bdays[-4]} 残3={month_bdays[-3]} 残2={month_bdays[-2]} 残1={month_bdays[-1]}")

    return flags


def build_calendar(bdays: list[date], sq_flags: dict, qe_flags: dict) -> pd.DataFrame:
    """全フラグを統合してDataFrame化"""
    rows = []
    for d in bdays:
        row = {"date": d}
        sf = sq_flags.get(d, {})
        qf = qe_flags.get(d, {})

        row["sq_day"] = sf.get("sq_day", False)
        row["sq4_entry"] = sf.get("sq4_entry", False)
        row["sq3_exit"] = sf.get("sq3_exit", False)
        row["qe_remain"] = qf.get("qe_remain", None)
        row["qe_1306_buy"] = qf.get("qe_1306_buy", False)
        row["qe_1306_sell"] = qf.get("qe_1306_sell", False)

        rows.append(row)

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["qe_remain"] = df["qe_remain"].astype("Int64")
    return df


def main() -> int:
    print("=" * 60)
    print(f"Generate Calendar Parquet ({START_YEAR}-{END_YEAR})")
    print("=" * 60)

    print("\n[1] 営業日取得 (jquants mkt calendar)")
    all_bdays = []
    for year in range(START_YEAR, END_YEAR + 1):
        all_bdays.extend(fetch_business_days(year))
    all_bdays.sort()

    print("\n[2] SQ日特定 + SQ-4/SQ-3フラグ")
    all_sq_flags = {}
    for year in range(START_YEAR, END_YEAR + 1):
        year_bdays = [d for d in all_bdays if d.year == year]
        sq_days = find_sq_days(year_bdays)
        sq_flags = calc_sq_flags(year_bdays, sq_days)
        all_sq_flags.update(sq_flags)

    print("\n[3] 四半期末フラグ")
    all_qe_flags = {}
    for year in range(START_YEAR, END_YEAR + 1):
        year_bdays = [d for d in all_bdays if d.year == year]
        qe_flags = calc_quarter_end_flags(year_bdays)
        all_qe_flags.update(qe_flags)

    print("\n[4] calendar.parquet 生成")
    df = build_calendar(all_bdays, all_sq_flags, all_qe_flags)

    print(f"\n  営業日数: {len(df)}")
    print(f"  SQ-4エントリー日: {df['sq4_entry'].sum()}回")
    print(f"  1306買い日: {df['qe_1306_buy'].sum()}回")
    print(f"  1306売り日: {df['qe_1306_sell'].sum()}回")

    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"\n[OK] {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
