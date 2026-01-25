#!/usr/bin/env python3
"""
generate_financials.py
J-Quants /fins/summary から全銘柄の財務データを取得し、financials.parquet として保存
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR
from scripts.lib.jquants_client import JQuantsClient

# 出力ファイル
OUTPUT_FILE = PARQUET_DIR / "financials.parquet"
ALL_STOCKS_FILE = PARQUET_DIR / "all_stocks.parquet"

# レート制限: J-Quants Free/Light plan は 12 calls/min
RATE_LIMIT_DELAY = 5.5  # 秒（安全マージン込み）


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


def fetch_financial_summary(client: JQuantsClient, code: str) -> dict[str, Any] | None:
    """銘柄の財務サマリーを取得"""
    try:
        response = client.request("/fins/summary", params={"code": code})
        data = response.get("data", [])

        if not data:
            return None

        # 最新データを取得（配列の最後）
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

    except Exception as e:
        print(f"  [WARN] Failed to fetch {code}: {e}")
        return None


def load_target_tickers() -> list[str]:
    """all_stocks.parquetから対象銘柄コードを取得"""
    if not ALL_STOCKS_FILE.exists():
        print(f"[ERROR] {ALL_STOCKS_FILE} not found")
        return []

    df = pd.read_parquet(ALL_STOCKS_FILE)
    # codeカラムから銘柄コードを取得
    codes = df["code"].dropna().unique().tolist()
    return [str(c) for c in codes]


def main() -> int:
    print("=" * 60)
    print("Generate Financials from J-Quants /fins/summary")
    print("=" * 60)

    # [STEP 1] 対象銘柄を取得
    print("\n[STEP 1] Loading target tickers from all_stocks.parquet...")
    codes = load_target_tickers()
    if not codes:
        print("  [ERROR] No tickers found")
        return 1
    print(f"  ✓ Found {len(codes)} tickers")

    # [STEP 2] J-Quants APIから財務データを取得
    print("\n[STEP 2] Fetching financial data from J-Quants API...")
    print(f"  Rate limit: {RATE_LIMIT_DELAY}s between requests")

    client = JQuantsClient()
    records: list[dict[str, Any]] = []
    failed: list[str] = []

    for i, code in enumerate(codes):
        print(f"  [{i + 1}/{len(codes)}] Fetching {code}...", end=" ")

        record = fetch_financial_summary(client, code)
        if record:
            records.append(record)
            print("✓")
        else:
            failed.append(code)
            print("✗")

        # レート制限対策（最後の銘柄以外）
        if i < len(codes) - 1:
            time.sleep(RATE_LIMIT_DELAY)

    print(f"\n  ✓ Successfully fetched: {len(records)} / {len(codes)}")
    if failed:
        print(f"  ⚠ Failed: {len(failed)} ({', '.join(failed[:10])}{'...' if len(failed) > 10 else ''})")

    # [STEP 3] parquetに保存
    print("\n[STEP 3] Saving to financials.parquet...")
    if not records:
        print("  [ERROR] No data to save")
        return 1

    df = pd.DataFrame(records)

    # スキーマを明示的に定義
    schema_cols = [
        "ticker", "code", "fiscalPeriod", "periodEnd", "disclosureDate",
        "sales", "operatingProfit", "ordinaryProfit", "netProfit", "eps",
        "totalAssets", "equity", "equityRatio", "bps", "sharesOutstanding",
    ]

    # カラム順序を整える
    df = df[schema_cols]

    # 出力ディレクトリを作成
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(OUTPUT_FILE, index=False, engine="pyarrow")
    print(f"  ✓ Saved: {OUTPUT_FILE}")
    print(f"    Rows: {len(df)}")
    print(f"    Columns: {df.columns.tolist()}")

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Output: {OUTPUT_FILE}")
    print(f"Total records: {len(df)}")
    print(f"Failed: {len(failed)}")
    print("=" * 60)

    print("\n✅ Financials generation completed!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
