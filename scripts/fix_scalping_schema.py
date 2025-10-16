#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
既存の scalping_*.parquet に series と topixnewindexseries を追加する応急処理
"""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR

DATA_J_CSV = ROOT / "data" / "csv" / "data_j.csv"
SCALPING_ENTRY = PARQUET_DIR / "scalping_entry.parquet"
SCALPING_ACTIVE = PARQUET_DIR / "scalping_active.parquet"


def fix_scalping_parquet(scalping_path: Path, category: str):
    """scalping_*.parquet に series と topixnewindexseries を追加

    Args:
        scalping_path: scalping_entry.parquet or scalping_active.parquet
        category: "entry" or "active"
    """
    print(f"\n{'='*60}")
    print(f"Fixing {scalping_path.name} ({category})")
    print(f"{'='*60}")

    # 1. scalping_*.parquet 読み込み
    print("\n[1] Loading scalping parquet...")
    df_scalping = pd.read_parquet(scalping_path)
    print(f"  ✓ Loaded {len(df_scalping)} rows")
    print(f"  ✓ Columns: {list(df_scalping.columns)}")

    # 2. data_j.csv 読み込み
    print("\n[2] Loading data_j.csv...")
    if not DATA_J_CSV.exists():
        print(f"  ✗ data_j.csv not found: {DATA_J_CSV}")
        return

    df_master = pd.read_csv(DATA_J_CSV, dtype=str)
    print(f"  ✓ Loaded {len(df_master)} rows")

    # ticker列を作成（コード[4桁] + ".T"）
    # 例: "8714" → "8714.T"
    df_master["ticker"] = df_master["コード"].astype(str) + ".T"

    # 必要なカラムのみ抽出
    cols_needed = ["ticker", "17業種区分", "規模区分"]
    df_master = df_master[cols_needed].copy()
    df_master = df_master.rename(columns={
        "17業種区分": "series",
        "規模区分": "topixnewindexseries"
    })

    print(f"  ✓ Extracted columns: {list(df_master.columns)}")

    # 3. マージ（既存のカラムがあれば削除してから）
    print("\n[3] Merging series and topixnewindexseries...")

    # 既存のseries/topixnewindexseriesカラムを削除
    if "series" in df_scalping.columns:
        df_scalping = df_scalping.drop(columns=["series"])
    if "topixnewindexseries" in df_scalping.columns:
        df_scalping = df_scalping.drop(columns=["topixnewindexseries"])

    df_merged = df_scalping.merge(
        df_master[["ticker", "series", "topixnewindexseries"]],
        on="ticker",
        how="left"
    )

    # 4. "-" を null に統一（高市銘柄との整合性）
    print("\n[4] Converting '-' to null for consistency...")
    before_dash_count = (df_merged["topixnewindexseries"] == "-").sum()
    df_merged["topixnewindexseries"] = df_merged["topixnewindexseries"].replace("-", None)
    after_null_count = df_merged["topixnewindexseries"].isna().sum()
    print(f"  ✓ Converted {before_dash_count} '-' values to null")

    # マージ結果確認
    missing_series = df_merged["series"].isna().sum()
    missing_topix = df_merged["topixnewindexseries"].isna().sum()

    if missing_series > 0:
        print(f"  ⚠ {missing_series} stocks missing 'series'")
    if missing_topix > 0:
        print(f"  ⚠ {missing_topix} stocks with null 'topixnewindexseries' (TOPIX非構成)")

    print(f"  ✓ Merged successfully")

    # 5. サンプル表示
    print("\n[5] Sample data:")
    sample_cols = ["ticker", "stock_name", "sectors", "series", "topixnewindexseries"]
    print(df_merged[sample_cols].head(3).to_string(index=False))

    # 6. 保存
    print(f"\n[6] Saving to {scalping_path}...")
    df_merged.to_parquet(scalping_path, index=False)
    print(f"  ✓ Saved {len(df_merged)} rows")

    # 7. 検証
    print("\n[7] Verification:")
    df_verify = pd.read_parquet(scalping_path)
    print(f"  ✓ Columns: {list(df_verify.columns)}")
    print(f"  ✓ series null count: {df_verify['series'].isna().sum()}")
    print(f"  ✓ topixnewindexseries null count: {df_verify['topixnewindexseries'].isna().sum()}")


def main():
    print("="*60)
    print("Fix Scalping Parquet Schema")
    print("="*60)

    # Entry
    if SCALPING_ENTRY.exists():
        fix_scalping_parquet(SCALPING_ENTRY, "entry")
    else:
        print(f"\n⚠ {SCALPING_ENTRY} not found, skipping")

    # Active
    if SCALPING_ACTIVE.exists():
        fix_scalping_parquet(SCALPING_ACTIVE, "active")
    else:
        print(f"\n⚠ {SCALPING_ACTIVE} not found, skipping")

    print("\n" + "="*60)
    print("✓ All done!")
    print("="*60)


if __name__ == "__main__":
    raise SystemExit(main())
