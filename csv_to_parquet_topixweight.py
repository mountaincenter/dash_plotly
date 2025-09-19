#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 目的:
# ./data/csv/topixweight_j.csv を読み込み、
# - 文字コードの違い(utf-8/cp932/utf-8-sig/utf-16)にロバストに対応
# - 列名を英語に統一（値は日本語のまま）
# - code -> ticker を追加（.T 付与／既に .T ならそのまま）
# - weight は "0.0066%" → 0.0066 のように %記号を除去して float 化（百分率）
# を行い、./data/parquet/topixweight_j.parquet へ保存する。

from pathlib import Path
import sys
import pandas as pd

# ===== 入出力（指定通り） =====
INPUT_CSV  = Path("./data/csv/topixweight_j.csv")
OUTPUT_DIR = Path("./data/parquet")
OUTPUT_PARQUET = OUTPUT_DIR / f"{INPUT_CSV.stem}.parquet"  # => topixweight_j.parquet

# 列名マッピング（ご指定反映: classification → size_class）
COLUMNS_MAP = {
    "日付": "date",
    "銘柄名": "stock_name",
    "コード": "code",
    "業種": "sector_indices",
    "TOPIXに占める個別銘柄のウエイト": "weight",
    "ニューインデックス区分": "size_class",
}

def _read_csv_flex(path: Path) -> pd.DataFrame:
    """日本語CSVの実務で多いエンコーディング候補を順に試す。"""
    encodings = ["utf-8", "cp932", "utf-8-sig", "utf-16"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError as e:
            last_err = e
            continue
    print(f"[ERROR] 文字コード自動判別に失敗しました: {last_err}", file=sys.stderr)
    return pd.DataFrame()

def _to_ticker(x) -> str:
    """'code' を 東証形式 'XXXX.T' に正規化。"""
    s = str(x).strip()
    return s if s.endswith(".T") else f"{s}.T"

def _to_float_percent(series: pd.Series) -> pd.Series:
    """'12.34%' → 12.34 に変換（カンマ/空白/None混在に耐性）。"""
    s = series.astype("string")
    s = s.str.replace(",", "", regex=False).str.replace("%", "", regex=False).str.strip()
    return pd.to_numeric(s, errors="coerce")

def main() -> int:
    # 入力存在チェック
    if not INPUT_CSV.exists():
        print(f"[ERROR] 入力CSVが見つかりません: {INPUT_CSV}", file=sys.stderr)
        return 1

    # 読み込み（エンコーディングを順に試行）
    df = _read_csv_flex(INPUT_CSV)
    if df.empty:
        print("[ERROR] CSVの読込に失敗したか、内容が空です。", file=sys.stderr)
        return 1

    # 列名英語化（指定列のみ）
    df = df.rename(columns=COLUMNS_MAP)

    # code -> ticker 生成（codeがある前提）
    if "code" in df.columns:
        df["code"] = df["code"].astype("string")
        if "ticker" not in df.columns:
            df["ticker"] = df["code"].map(_to_ticker)
    else:
        print("[WARN] 'コード'列が見つからず、'ticker' を生成できませんでした。", file=sys.stderr)

    # weight を float (百分率) に正規化
    if "weight" in df.columns:
        df["weight"] = _to_float_percent(df["weight"])

    # 出力ディレクトリ作成
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Parquet 保存（pyarrow / snappy）
    try:
        df.to_parquet(OUTPUT_PARQUET, engine="pyarrow", compression="snappy", index=False)
    except Exception as e:
        print(f"[ERROR] Parquet保存に失敗しました: {e}", file=sys.stderr)
        return 1

    print(f"[OK] saved: {OUTPUT_PARQUET}  (rows={len(df)}, cols={len(df.columns)})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
