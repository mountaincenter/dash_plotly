#!/usr/bin/env python3
"""
generate_fins_data.py
J-Quants /fins/summary から財務データと決算発表予定を一括生成

統合前:
  - generate_financials.py: ~600秒
  - generate_announcements.py: ~600秒
  合計: ~1200秒

統合後:
  - generate_fins_data.py: ~600秒
  合計: ~600秒（半減）
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR
from scripts.lib.jquants_client import JQuantsClient

# 出力ファイル
FINANCIALS_FILE = PARQUET_DIR / "financials.parquet"
ANNOUNCEMENTS_FILE = PARQUET_DIR / "announcements.parquet"
ALL_STOCKS_FILE = PARQUET_DIR / "all_stocks.parquet"

# レート制限: J-Quants Free/Light plan は 12 calls/min
RATE_LIMIT_DELAY = 5.5

# 四半期の順序
QUARTER_ORDER = ["1Q", "2Q", "3Q", "FY"]


def to_oku(val: Any) -> float | None:
    """円単位を億円に変換"""
    if val is None or val == "":
        return None
    try:
        return round(float(val) / 100_000_000, 1)
    except (ValueError, TypeError):
        return None


def to_float(val: Any) -> float | None:
    """floatに変換"""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def get_next_quarter(current: str) -> str:
    """次の四半期を取得"""
    if current == "1Q":
        return "2Q"
    elif current == "2Q":
        return "3Q"
    elif current == "3Q":
        return "FY"
    else:
        return "1Q"


def get_quarter_end_date(quarter: str, fiscal_year_end_month: int = 3) -> tuple[int, int]:
    """四半期の期末月日を取得 (month, day)"""
    if fiscal_year_end_month == 3:
        if quarter == "1Q":
            return (6, 30)
        elif quarter == "2Q":
            return (9, 30)
        elif quarter == "3Q":
            return (12, 31)
        else:
            return (3, 31)
    return (fiscal_year_end_month, 28)


def adjust_for_weekday(date: datetime, reference_weekday: int) -> datetime:
    """参照曜日に近い平日に調整"""
    current_weekday = date.weekday()
    diff = reference_weekday - current_weekday
    if diff > 3:
        diff -= 7
    elif diff < -3:
        diff += 7
    adjusted = date + timedelta(days=diff)
    if adjusted.weekday() == 5:
        adjusted -= timedelta(days=1)
    elif adjusted.weekday() == 6:
        adjusted += timedelta(days=1)
    return adjusted


def extract_financial_data(code: str, data: list[dict]) -> dict[str, Any] | None:
    """APIレスポンスから財務データを抽出"""
    if not data:
        return None

    latest = data[-1]
    return {
        "code": code,
        "ticker": f"{code}.T",
        "fiscalPeriod": latest.get("CurPerType"),
        "periodEnd": latest.get("CurPerEn"),
        "disclosureDate": latest.get("DiscDate"),
        "sales": to_oku(latest.get("Sales")),
        "operatingProfit": to_oku(latest.get("OP")),
        "ordinaryProfit": to_oku(latest.get("OdP")),
        "netProfit": to_oku(latest.get("NP")),
        "eps": to_float(latest.get("EPS")),
        "totalAssets": to_oku(latest.get("TA")),
        "equity": to_oku(latest.get("Eq")),
        "equityRatio": to_float(latest.get("EqAR")),
        "bps": to_float(latest.get("BPS")),
        "sharesOutstanding": to_float(latest.get("ShOutFY")),
    }


def estimate_next_announcement(data: list[dict], today: datetime) -> dict | None:
    """APIレスポンスから次回発表予定を推定"""
    if not data:
        return None

    sorted_data = sorted(data, key=lambda x: x.get("DiscDate", ""), reverse=True)
    latest = sorted_data[0]
    latest_disc_date_str = latest.get("DiscDate")
    latest_period_type = latest.get("CurPerType")

    if not latest_disc_date_str or not latest_period_type:
        return None

    latest_disc_date = datetime.strptime(latest_disc_date_str, "%Y-%m-%d")
    next_quarter = get_next_quarter(latest_period_type)

    # 1年前の同四半期を探す
    last_year_same_quarter = None
    for item in sorted_data:
        period_type = item.get("CurPerType")
        disc_date_str = item.get("DiscDate")
        if period_type == next_quarter and disc_date_str:
            try:
                disc_date = datetime.strptime(disc_date_str, "%Y-%m-%d")
                if disc_date < latest_disc_date and (latest_disc_date - disc_date).days > 180:
                    last_year_same_quarter = disc_date
                    break
            except ValueError:
                continue

    if last_year_same_quarter:
        reference_weekday = last_year_same_quarter.weekday()
        estimated_date = last_year_same_quarter + relativedelta(years=1)
        estimated_date = adjust_for_weekday(estimated_date, reference_weekday)
        confidence = "high"
    else:
        month, day = get_quarter_end_date(next_quarter)
        next_year = latest_disc_date.year
        if month <= latest_disc_date.month:
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
        confidence = "medium"

    # 既に過ぎている場合は次の四半期を計算
    if estimated_date.date() < today.date():
        next_quarter = get_next_quarter(next_quarter)
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
                    break
                except ValueError:
                    continue
        else:
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

    return {
        "estimatedDate": estimated_date.strftime("%Y-%m-%d"),
        "nextQuarter": next_quarter,
        "confidence": confidence,
    }


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
    print("Generate Financials & Announcements from J-Quants /fins/summary")
    print("=" * 60)

    today = datetime.now()
    print(f"Today: {today.strftime('%Y-%m-%d')}")

    # [STEP 1] 対象銘柄を取得
    print("\n[STEP 1] Loading target tickers from all_stocks.parquet...")
    codes = load_target_codes()
    if not codes:
        print("  [ERROR] No tickers found")
        return 1
    print(f"  Found {len(codes)} tickers")

    # [STEP 2] J-Quants APIから財務データを取得（1回のAPI呼び出しで両方のデータを抽出）
    print("\n[STEP 2] Fetching financial data from J-Quants API...")
    print(f"  Rate limit: {RATE_LIMIT_DELAY}s between requests")
    print(f"  Estimated time: {len(codes) * RATE_LIMIT_DELAY / 60:.1f} minutes")
    print("")

    client = JQuantsClient()
    financial_records: list[dict] = []
    announcement_records: list[dict] = []
    failed: list[str] = []

    for i, code in enumerate(codes):
        status = "..."
        try:
            response = client.request("/fins/summary", params={"code": code})
            data = response.get("data", [])

            if not data:
                failed.append(code)
                status = "SKIP (no data)"
            else:
                # 財務データを抽出
                fin_record = extract_financial_data(code, data)
                if fin_record:
                    financial_records.append(fin_record)

                # 決算発表予定を推定
                ann_result = estimate_next_announcement(data, today)
                if ann_result:
                    announcement_records.append({
                        "code": code,
                        "ticker": f"{code}.T",
                        "announcementDate": ann_result["estimatedDate"],
                        "nextQuarter": ann_result["nextQuarter"],
                        "confidence": ann_result["confidence"],
                    })
                status = "OK"

        except Exception as e:
            failed.append(code)
            err_type = type(e).__name__
            status = f"ERROR ({err_type})"

        # 1行で完結するログ出力
        print(f"  [{i + 1}/{len(codes)}] {code} {status}", flush=True)

        # レート制限対策
        if i < len(codes) - 1:
            time.sleep(RATE_LIMIT_DELAY)

    print("")
    print(f"  Fetched: {len(financial_records)} / {len(codes)}")
    if failed:
        print(f"  Failed: {len(failed)} ({', '.join(failed[:10])}{'...' if len(failed) > 10 else ''})")

    # [STEP 3] financials.parquet を保存
    print("\n[STEP 3] Saving financials.parquet...")
    if financial_records:
        df_fin = pd.DataFrame(financial_records)
        schema_cols = [
            "ticker", "code", "fiscalPeriod", "periodEnd", "disclosureDate",
            "sales", "operatingProfit", "ordinaryProfit", "netProfit", "eps",
            "totalAssets", "equity", "equityRatio", "bps", "sharesOutstanding",
        ]
        df_fin = df_fin[schema_cols]
        FINANCIALS_FILE.parent.mkdir(parents=True, exist_ok=True)
        df_fin.to_parquet(FINANCIALS_FILE, index=False, engine="pyarrow")
        print(f"  Saved: {FINANCIALS_FILE}")
        print(f"  Rows: {len(df_fin)}")
    else:
        print("  [WARN] No financial data to save")

    # [STEP 4] announcements.parquet を保存
    print("\n[STEP 4] Saving announcements.parquet...")
    if announcement_records:
        df_ann = pd.DataFrame(announcement_records)
        df_ann = df_ann.sort_values("announcementDate")
        ANNOUNCEMENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        df_ann.to_parquet(ANNOUNCEMENTS_FILE, index=False, engine="pyarrow")
        print(f"  Saved: {ANNOUNCEMENTS_FILE}")
        print(f"  Rows: {len(df_ann)}")
    else:
        print("  [WARN] No announcement data to save")

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Financials: {len(financial_records)} records")
    print(f"Announcements: {len(announcement_records)} records")
    print(f"Failed: {len(failed)}")
    print("=" * 60)

    print("\n✅ Fins data generation completed!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
