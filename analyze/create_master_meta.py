#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
create_master_meta.py
- Build unified meta parquet from CSV sources.
    * TOPIX Core30 metadata (topixweight_j.csv + data_j.csv)
    * Takaichi picks metadata (takaichi_stock_issue.csv + data_j.csv)
- Output: data/parquet/meta.parquet
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from common_cfg.paths import MASTER_META_PARQUET, PARQUET_DIR
INPUT_TOPIX = ROOT / "data" / "csv" / "topixweight_j.csv"
INPUT_DATA = ROOT / "data" / "csv" / "data_j.csv"
INPUT_TAKAICHI = ROOT / "data" / "csv" / "takaichi_stock_issue.csv"
INPUT_SCALPING_ENTRY = PARQUET_DIR / "scalping_entry.parquet"
INPUT_SCALPING_ACTIVE = PARQUET_DIR / "scalping_active.parquet"
OUTPUT_PARQUET = MASTER_META_PARQUET

ENCODINGS: Iterable[str] = ("utf-8", "cp932", "utf-8-sig", "utf-16")


def read_csv_flex(path: Path, **kwargs) -> pd.DataFrame:
    last_err: Exception | None = None
    for enc in ENCODINGS:
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except UnicodeDecodeError as exc:
            last_err = exc
    if last_err:
        raise last_err
    raise RuntimeError(f"Unexpected failure reading CSV: {path}")


def normalize_code(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()
    # drop placeholder values
    s = s.mask(s.isin({"", "nan", "NaN"}))

    def _clean(value):
        if value is pd.NA or value is None:
            return pd.NA
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return pd.NA
        if "." in text:
            try:
                text = str(int(float(text)))
            except ValueError:
                pass
        # Left-pad 4-digit numeric codes; leave ETFs/others as-is
        if text.isdigit() and len(text) <= 4:
            text = text.zfill(4)
        return text

    return s.apply(_clean).astype("string")


def to_ticker(series: pd.Series) -> pd.Series:
    def _to(x):
        if x is pd.NA or x is None:
            return pd.NA
        text = str(x).strip()
        if not text or text.lower() == "nan":
            return pd.NA
        return text if text.endswith(".T") else f"{text}.T"

    return series.apply(_to).astype("string")


def build_data_master() -> pd.DataFrame:
    df = read_csv_flex(INPUT_DATA, dtype=str)
    if "日付" in df.columns:
        df = df.sort_values("日付", ascending=False)
    df = df.drop_duplicates("コード", keep="first")
    data_map = {
        "コード": "code",
        "銘柄名": "stock_name",
        "市場・商品区分": "market",
        "33業種区分": "sectors",
        "17業種区分": "series",
    }
    df = df.rename(columns=data_map)
    df["code"] = normalize_code(df["code"])
    keep_cols = ["code", "stock_name", "market", "sectors", "series"]
    for col in keep_cols:
        if col not in df.columns:
            df[col] = pd.NA
    return df[keep_cols]


def build_topix_meta(base_lookup: pd.DataFrame) -> pd.DataFrame:
    df = read_csv_flex(INPUT_TOPIX)
    if df.empty:
        return pd.DataFrame()
    if "日付" in df.columns:
        df = df.sort_values("日付", ascending=False)
    df = df.drop_duplicates("コード", keep="first")
    topix_map = {
        "銘柄名": "stock_name",
        "コード": "code",
        "市場・商品区分": "market",
        "33業種区分": "sectors",
        "ニューインデックス区分": "topixnewindexseries",
    }
    df = df.rename(columns=topix_map)
    df["code"] = normalize_code(df["code"])
    df = df[df["topixnewindexseries"].astype("string") == "TOPIX Core30"]
    merged = df.merge(base_lookup, on="code", how="left", suffixes=("", "_base"))
    for col in ["stock_name", "market", "sectors", "series"]:
        base_col = f"{col}_base"
        if base_col in merged.columns:
            merged[col] = merged[col].combine_first(merged[base_col])
            merged = merged.drop(columns=[base_col])
        else:
            merged[col] = merged.get(col, pd.Series(dtype="string"))
    merged["ticker"] = to_ticker(merged["code"])
    merged["tag1"] = "TOPIX_CORE30"
    merged["tag2"] = pd.NA
    merged["tag3"] = pd.NA
    required_cols = ["ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries", "tag1", "tag2", "tag3"]
    for col in required_cols:
        if col not in merged.columns:
            merged[col] = pd.NA
    return merged[required_cols]


def build_takaichi_meta(base_lookup: pd.DataFrame) -> pd.DataFrame:
    df = read_csv_flex(INPUT_TAKAICHI, dtype=str)
    if df.empty:
        return pd.DataFrame()
    df = df.drop_duplicates("コード", keep="first")
    takaichi_map = {
        "銘柄名": "stock_name",
        "コード": "code",
        "市場・商品区分": "market",
        "33業種区分": "sectors",
        "ニューインデックス区分": "topixnewindexseries",
        "tag1": "tag1",
        "tag2": "tag2",
        "tag3": "tag3",
    }
    df = df.rename(columns=takaichi_map)
    df["code"] = normalize_code(df["code"])
    merged = df.merge(base_lookup, on="code", how="left", suffixes=("", "_base"))
    for col in ["stock_name", "market", "sectors", "series"]:
        base_col = f"{col}_base"
        merged[col] = merged.get(col)
        if base_col in merged.columns:
            merged[col] = merged[col].combine_first(merged[base_col])
            merged = merged.drop(columns=[base_col])
        else:
            merged[col] = merged.get(col, pd.Series(dtype="string"))
    merged["topixnewindexseries"] = merged.get("topixnewindexseries")
    merged["tag1"] = "高市銘柄"
    merged["tag2"] = merged.get("tag2")
    merged["tag3"] = merged.get("tag3")
    merged["ticker"] = to_ticker(merged["code"])
    required_cols = ["ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries", "tag1", "tag2", "tag3"]
    for col in required_cols:
        if col not in merged.columns:
            merged[col] = pd.NA
    return merged[required_cols]


def build_scalping_meta(parquet_path: Path, tag1_value: str) -> pd.DataFrame:
    """スキャルピング銘柄メタデータを読み込み、meta.parquet互換形式に変換"""
    if not parquet_path.exists():
        print(f"[INFO] scalping parquet not found, skipping: {parquet_path.name}")
        return pd.DataFrame()

    df = pd.read_parquet(parquet_path)
    if df.empty:
        return pd.DataFrame()

    # ticker から code を抽出（"1234.T" → "1234"）
    df["code"] = df["ticker"].str.replace(".T", "", regex=False)

    # tag1, tag2, tag3 を設定
    df["tag1"] = tag1_value
    df["tag2"] = pd.NA
    df["tag3"] = pd.NA

    # 必要なカラムを選択
    required_cols = ["ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries", "tag1", "tag2", "tag3"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    return df[required_cols].drop_duplicates(subset=["ticker"])


def main() -> int:
    base_lookup = build_data_master()

    frames = [
        build_topix_meta(base_lookup),
        build_takaichi_meta(base_lookup),
        build_scalping_meta(INPUT_SCALPING_ENTRY, "SCALPING_ENTRY"),
        build_scalping_meta(INPUT_SCALPING_ACTIVE, "SCALPING_ACTIVE"),
    ]
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["code"], how="all")
    combined = combined.drop_duplicates(subset=["code", "tag1"])
    combined = combined.sort_values(["tag1", "code"]).reset_index(drop=True)

    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUTPUT_PARQUET, engine="pyarrow", compression="snappy", index=False)

    print(f"[OK] saved: {OUTPUT_PARQUET} (rows={len(combined)}, cols={len(combined.columns)})")
    print(f"     - TOPIX_CORE30: {len(combined[combined['tag1'] == 'TOPIX_CORE30'])}")
    print(f"     - 高市銘柄: {len(combined[combined['tag1'] == '高市銘柄'])}")
    print(f"     - SCALPING_ENTRY: {len(combined[combined['tag1'] == 'SCALPING_ENTRY'])}")
    print(f"     - SCALPING_ACTIVE: {len(combined[combined['tag1'] == 'SCALPING_ACTIVE'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
