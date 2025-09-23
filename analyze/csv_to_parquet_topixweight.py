#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
csv_to_parquet_topixweight.py
- data/csv/topixweight_j.csv → data/parquet/topixweight_j.parquet
- manifest upsert（PIPELINE_NO_MANIFEST=1 で抑止）
"""

from pathlib import Path
import sys
import os
import json
from datetime import datetime, timezone
import pandas as pd

from common_cfg.paths import (
    TOPIX_WEIGHT_PARQUET as OUTPUT_PARQUET,
    MANIFEST_JSON as MANIFEST_PATH,
)
from common_cfg.manifest import sha256_of, load_manifest_items, upsert_manifest_item, write_manifest_atomic


INPUT_CSV = Path("./data/csv/topixweight_j.csv")

COLUMNS_MAP = {
    "日付": "date",
    "銘柄名": "stock_name",
    "コード": "code",
    "業種": "sector_indices",
    "TOPIXに占める個別銘柄のウエイト": "weight",
    "ニューインデックス区分": "size_class",
}


def _read_csv_flex(path: Path) -> pd.DataFrame:
    encodings = ["utf-8", "cp932", "utf-8-sig", "utf-16"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError as e:
            last_err = e
    print(f"[ERROR] 文字コード自動判別に失敗しました: {last_err}", file=sys.stderr)
    return pd.DataFrame()


def _to_ticker(x) -> str:
    s = str(x).strip()
    return s if s.endswith(".T") else f"{s}.T"


def _to_float_percent(series: pd.Series) -> pd.Series:
    s = series.astype("string")
    s = s.str.replace(",", "", regex=False).str.replace("%", "", regex=False).str.strip()
    return pd.to_numeric(s, errors="coerce")


def main() -> int:
    if not INPUT_CSV.exists():
        print(f"[ERROR] 入力CSVが見つかりません: {INPUT_CSV}", file=sys.stderr)
        return 1

    df = _read_csv_flex(INPUT_CSV)
    if df.empty:
        print("[ERROR] CSVの読込に失敗したか、内容が空です。", file=sys.stderr)
        return 1

    df = df.rename(columns=COLUMNS_MAP)

    if "code" in df.columns:
        df["code"] = df["code"].astype("string")
        if "ticker" not in df.columns:
            df["ticker"] = df["code"].map(_to_ticker)
    else:
        print("[WARN] 'コード'列が見つからず、'ticker' を生成できませんでした。", file=sys.stderr)

    if "weight" in df.columns:
        df["weight"] = _to_float_percent(df["weight"])

    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(OUTPUT_PARQUET, engine="pyarrow", compression="snappy", index=False)
    except Exception as e:
        print(f"[ERROR] Parquet保存に失敗しました: {e}", file=sys.stderr)
        return 1

    print(f"[OK] saved: {OUTPUT_PARQUET}  (rows={len(df)}, cols={len(df.columns)})")

    if os.getenv("PIPELINE_NO_MANIFEST") == "1":
        print("[INFO] PIPELINE_NO_MANIFEST=1 → manifest 更新はスキップします。")
        return 0

    try:
        items = load_manifest_items(MANIFEST_PATH)
        items = upsert_manifest_item(items, OUTPUT_PARQUET.name, OUTPUT_PARQUET)
        write_manifest_atomic(items, MANIFEST_PATH)
        print(f"[OK] manifest updated: {MANIFEST_PATH}")
    except Exception as e:
        print(f"[WARN] manifest 更新に失敗しました: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
