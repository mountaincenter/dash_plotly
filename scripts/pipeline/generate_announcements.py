#!/usr/bin/env python3
"""
generate_announcements.py
J-Quants /fins/summary の過去パターンから次回決算発表予定日を推定し、
announcements.parquet として保存
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR
from scripts.lib.jquants_client import JQuantsClient

# 出力ファイル
OUTPUT_FILE = PARQUET_DIR / "announcements.parquet"
ALL_STOCKS_FILE = PARQUET_DIR / "all_stocks.parquet"

# レート制限
RATE_LIMIT_DELAY = 5.5

# 四半期の順序
QUARTER_ORDER = ["1Q", "2Q", "3Q", "FY"]


def get_next_quarter(current: str) -> str:
    """次の四半期を取得"""
    if current == "1Q":
        return "2Q"
    elif current == "2Q":
        return "3Q"
    elif current == "3Q":
        return "FY"
    else:  # FY
        return "1Q"


def get_quarter_end_date(quarter: str, fiscal_year_end_month: int = 3) -> tuple[int, int]:
    """四半期の期末月日を取得 (month, day)"""
    # 3月決算企業の場合
    if fiscal_year_end_month == 3:
        if quarter == "1Q":
            return (6, 30)
        elif quarter == "2Q":
            return (9, 30)
        elif quarter == "3Q":
            return (12, 31)
        else:  # FY
            return (3, 31)
    # その他の決算月は簡易対応
    return (fiscal_year_end_month, 28)


def adjust_for_weekday(date: datetime, reference_weekday: int) -> datetime:
    """
    参照曜日に近い平日に調整
    reference_weekday: 0=月, 1=火, ..., 4=金
    """
    # まず同じ曜日にする
    current_weekday = date.weekday()
    diff = reference_weekday - current_weekday

    # 差が大きすぎる場合は調整
    if diff > 3:
        diff -= 7
    elif diff < -3:
        diff += 7

    adjusted = date + timedelta(days=diff)

    # 土日なら平日に調整
    if adjusted.weekday() == 5:  # 土曜
        adjusted -= timedelta(days=1)  # 金曜に
    elif adjusted.weekday() == 6:  # 日曜
        adjusted += timedelta(days=1)  # 月曜に

    return adjusted


def estimate_next_announcement(
    summary_data: list[dict],
    today: datetime,
) -> Optional[dict]:
    """
    1年前の同四半期発表日から次回発表予定日を推定（曜日調整付き）
    """
    if not summary_data:
        return None

    # 開示日でソート（新しい順）
    sorted_data = sorted(summary_data, key=lambda x: x.get("DiscDate", ""), reverse=True)

    # 最新の開示情報を取得
    latest = sorted_data[0]
    latest_disc_date_str = latest.get("DiscDate")
    latest_period_type = latest.get("CurPerType")

    if not latest_disc_date_str or not latest_period_type:
        return None

    latest_disc_date = datetime.strptime(latest_disc_date_str, "%Y-%m-%d")

    # 次の四半期を特定
    next_quarter = get_next_quarter(latest_period_type)

    # 1年前の同四半期を探す
    last_year_same_quarter = None
    for item in sorted_data:
        period_type = item.get("CurPerType")
        disc_date_str = item.get("DiscDate")

        if period_type == next_quarter and disc_date_str:
            try:
                disc_date = datetime.strptime(disc_date_str, "%Y-%m-%d")
                # 最新発表より前で、かつ6ヶ月以上前（1年前の同四半期）
                if disc_date < latest_disc_date and (latest_disc_date - disc_date).days > 180:
                    last_year_same_quarter = disc_date
                    break
            except ValueError:
                continue

    if last_year_same_quarter:
        # 1年前の同四半期発表日を基準に推定
        reference_weekday = last_year_same_quarter.weekday()

        # 1年後の同日
        estimated_date = last_year_same_quarter + relativedelta(years=1)

        # 曜日調整
        estimated_date = adjust_for_weekday(estimated_date, reference_weekday)

        confidence = "high"
        method = "1年前同四半期"
    else:
        # フォールバック: 期末から40日後
        month, day = get_quarter_end_date(next_quarter)

        # 次の期末日を計算
        if latest_period_type == "FY":
            next_year = latest_disc_date.year
        else:
            next_year = latest_disc_date.year
            if month <= latest_disc_date.month:
                next_year += 1

        try:
            next_period_end = datetime(next_year, month, day)
        except ValueError:
            next_period_end = datetime(next_year, month, 28)

        estimated_date = next_period_end + timedelta(days=40)

        # 土日調整
        if estimated_date.weekday() == 5:
            estimated_date -= timedelta(days=1)
        elif estimated_date.weekday() == 6:
            estimated_date += timedelta(days=1)

        confidence = "medium"
        method = "期末+40日"

    # 既に過ぎている場合は次の四半期を計算
    if estimated_date.date() < today.date():
        next_quarter = get_next_quarter(next_quarter)

        # 再度1年前を探す
        for item in sorted_data:
            period_type = item.get("CurPerType")
            disc_date_str = item.get("DiscDate")

            if period_type == next_quarter and disc_date_str:
                try:
                    disc_date = datetime.strptime(disc_date_str, "%Y-%m-%d")
                    reference_weekday = disc_date.weekday()
                    estimated_date = disc_date + relativedelta(years=1)
                    estimated_date = adjust_for_weekday(estimated_date, reference_weekday)
                    confidence = "high"
                    method = "1年前同四半期"
                    break
                except ValueError:
                    continue
        else:
            # フォールバック
            month, day = get_quarter_end_date(next_quarter)
            next_year = today.year
            if month < today.month or (month == today.month and day < today.day):
                next_year += 1
            try:
                next_period_end = datetime(next_year, month, day)
            except ValueError:
                next_period_end = datetime(next_year, month, 28)
            estimated_date = next_period_end + timedelta(days=40)

            if estimated_date.weekday() == 5:
                estimated_date -= timedelta(days=1)
            elif estimated_date.weekday() == 6:
                estimated_date += timedelta(days=1)

            confidence = "low"
            method = "期末+40日(フォールバック)"

    return {
        "estimatedDate": estimated_date.strftime("%Y-%m-%d"),
        "nextQuarter": next_quarter,
        "confidence": confidence,
        "method": method,
        "weekday": ["月", "火", "水", "木", "金", "土", "日"][estimated_date.weekday()],
    }


def fetch_and_estimate(client: JQuantsClient, code: str, today: datetime) -> Optional[dict]:
    """1銘柄の次回発表予定を推定"""
    try:
        response = client.request("/fins/summary", params={"code": code})
        data = response.get("data", [])

        if not data:
            return None

        result = estimate_next_announcement(data, today)
        if result:
            return {
                "code": code,
                "ticker": f"{code}.T",
                "announcementDate": result["estimatedDate"],
                "nextQuarter": result["nextQuarter"],
                "confidence": result["confidence"],
            }
        return None

    except Exception as e:
        print(f"  [WARN] Failed for {code}: {e}")
        return None


def load_target_codes() -> list[str]:
    """all_stocks.parquetから対象銘柄コードを取得"""
    if not ALL_STOCKS_FILE.exists():
        print(f"[ERROR] {ALL_STOCKS_FILE} not found")
        return []

    df = pd.read_parquet(ALL_STOCKS_FILE)
    codes = df["code"].dropna().unique().tolist()
    return [str(c) for c in codes]


def main() -> int:
    print("=" * 60)
    print("Generate Announcements (Estimated from /fins/summary)")
    print("=" * 60)

    today = datetime.now()
    print(f"Today: {today.strftime('%Y-%m-%d')}")

    # [STEP 1] 対象銘柄を取得
    print("\n[STEP 1] Loading target tickers...")
    codes = load_target_codes()
    if not codes:
        print("  [ERROR] No tickers found")
        return 1
    print(f"  ✓ Found {len(codes)} tickers")

    # [STEP 2] 各銘柄の次回発表予定を推定
    print("\n[STEP 2] Estimating next announcement dates...")
    print(f"  Rate limit: {RATE_LIMIT_DELAY}s between requests")

    client = JQuantsClient()
    records = []
    failed = []

    for i, code in enumerate(codes):
        if (i + 1) % 20 == 0:
            print(f"  [{i + 1}/{len(codes)}] Processing...")

        result = fetch_and_estimate(client, code, today)
        if result:
            records.append(result)
        else:
            failed.append(code)

        if i < len(codes) - 1:
            time.sleep(RATE_LIMIT_DELAY)

    print(f"\n  ✓ Successfully estimated: {len(records)} / {len(codes)}")

    # [STEP 3] 保存
    print("\n[STEP 3] Saving to announcements.parquet...")

    if not records:
        print("  [WARN] No records to save")
        df = pd.DataFrame(columns=["code", "ticker", "announcementDate", "nextQuarter", "confidence"])
        df.to_parquet(OUTPUT_FILE, index=False, engine="pyarrow")
        return 0

    df = pd.DataFrame(records)
    df = df.sort_values("announcementDate")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False, engine="pyarrow")

    print(f"  ✓ Saved: {OUTPUT_FILE}")
    print(f"    Rows: {len(df)}")

    # サンプル
    print("\n[SAMPLE] First 10 records:")
    print(df.head(10).to_string(index=False))

    print("\n✅ Announcements generation completed!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
