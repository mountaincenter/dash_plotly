#!/usr/bin/env python3
"""
Create J-Quants minute-fetch universe masters.

Outputs:
  - data/jquants_csv/master/topix_etf_universe.csv
  - data/parquet/topix_etf_universe.parquet

Universe scope:
  - TOPIX Core30 / Large70 / Mid400
  - Semiconductor monitoring ETFs
  - Semiconductor / AI / data-center watch stocks, including names outside TOPIX500
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

META_PATH = PARQUET_DIR / "meta_jquants.parquet"
CSV_OUT = ROOT / "data" / "jquants_csv" / "master" / "topix_etf_universe.csv"
PARQUET_OUT = PARQUET_DIR / "topix_etf_universe.parquet"

TOPIX_GROUPS = {
    "TOPIX Core30": ("topix_core30", "Core30", 10),
    "TOPIX Large70": ("topix_large70", "Large70", 20),
    "TOPIX Mid400": ("topix_mid400", "Mid400", 30),
}

ETF_DEFINITIONS = [
    {
        "code": "200A",
        "stock_name": "NEXT FUNDS 日経半導体株指数連動型上場投信",
        "groups": ["etf_live_training", "etf_live_candidate"],
        "priority": 1,
        "display_order": 3,
        "execution_rank": 1,
        "notes": "実弾・訓練 / 実弾候補",
    },
    {
        "code": "2644",
        "stock_name": "グローバルX 半導体関連-日本株式 ETF",
        "groups": ["etf_live_training", "etf_live_candidate"],
        "priority": 1,
        "display_order": 2,
        "execution_rank": 2,
        "notes": "実弾・訓練 / 実弾候補",
    },
    {
        "code": "2243",
        "stock_name": "グローバルX 半導体 ETF",
        "groups": ["etf_direction"],
        "priority": 2,
        "display_order": 1,
        "execution_rank": None,
        "notes": "方向確認",
    },
    {
        "code": "213A",
        "stock_name": "日経半導体株ETF",
        "groups": ["etf_watch"],
        "priority": 2,
        "display_order": 4,
        "execution_rank": None,
        "notes": "半導体ETF監視",
    },
    {
        "code": "346A",
        "stock_name": "NEXT FUNDS S&P 500 半導体・半導体製造装置35％キャップ指数連動型上場投信",
        "groups": ["etf_watch"],
        "priority": 2,
        "display_order": 5,
        "execution_rank": None,
        "notes": "半導体ETF監視",
    },
    {
        "code": "1321",
        "stock_name": "NEXT FUNDS 日経225連動型上場投信",
        "groups": ["etf_market"],
        "priority": 3,
        "display_order": 6,
        "execution_rank": None,
        "notes": "地合い確認",
    },
    {
        "code": "1306",
        "stock_name": "NEXT FUNDS TOPIX連動型上場投信",
        "groups": ["etf_market"],
        "priority": 3,
        "display_order": 7,
        "execution_rank": None,
        "notes": "地合い確認",
    },
    {
        "code": "1570",
        "stock_name": "NEXT FUNDS 日経平均レバレッジ・インデックス連動型上場投信",
        "groups": ["etf_market"],
        "priority": 3,
        "display_order": 8,
        "execution_rank": None,
        "notes": "地合い確認",
    },
]

SEMICONDUCTOR_DEFINITIONS = [
    {"code": "6981", "groups": ["semicon_core", "mlcc"], "priority": 4, "notes": "主役 / MLCC"},
    {"code": "6976", "groups": ["semicon_core", "mlcc"], "priority": 4, "notes": "主役 / MLCC"},
    {"code": "9984", "groups": ["semicon_core"], "priority": 4, "notes": "主役 / AI投資"},
    {"code": "285A", "groups": ["semicon_core", "memory"], "priority": 4, "notes": "主役 / メモリー"},
    {"code": "6323", "groups": ["equipment_peripheral"], "priority": 9, "notes": "装置周辺 / 低優先監視"},
    {"code": "6055", "groups": ["material_peripheral"], "priority": 9, "notes": "材料周辺 / 低優先監視"},
    {"code": "8035", "groups": ["equipment"], "priority": 5, "notes": "装置主力"},
    {"code": "6857", "groups": ["equipment"], "priority": 5, "notes": "装置主力"},
    {"code": "6146", "groups": ["equipment"], "priority": 5, "notes": "装置主力"},
    {"code": "6920", "groups": ["equipment"], "priority": 5, "notes": "装置主力"},
    {"code": "7735", "groups": ["equipment"], "priority": 5, "notes": "装置主力"},
    {"code": "6525", "groups": ["equipment"], "priority": 5, "notes": "装置主力"},
    {"code": "6315", "groups": ["equipment"], "priority": 5, "notes": "装置周辺"},
    {"code": "4186", "groups": ["material"], "priority": 5, "notes": "材料"},
    {"code": "4062", "groups": ["substrate"], "priority": 6, "notes": "基板/PKG"},
    {"code": "6787", "groups": ["substrate"], "priority": 6, "notes": "基板/PKG"},
    {"code": "3436", "groups": ["wafer"], "priority": 6, "notes": "ウェハ"},
    {"code": "6967", "groups": ["substrate"], "priority": 6, "notes": "新光電気工業 / メタ未収録なら要確認"},
    {"code": "5331", "groups": ["substrate", "mlcc"], "priority": 6, "notes": "基板/MLCC周辺"},
    {"code": "6245", "groups": ["substrate"], "priority": 6, "notes": "基板周辺"},
    {"code": "6762", "groups": ["mlcc"], "priority": 7, "notes": "電子部品/MLCC"},
    {"code": "6971", "groups": ["mlcc"], "priority": 7, "notes": "電子部品/MLCC"},
    {"code": "5367", "groups": ["mlcc"], "priority": 7, "notes": "MLCC材料/低位"},
    {"code": "4092", "groups": ["mlcc"], "priority": 7, "notes": "MLCC材料"},
    {"code": "4078", "groups": ["mlcc"], "priority": 7, "notes": "MLCC材料"},
    {"code": "4100", "groups": ["mlcc"], "priority": 7, "notes": "MLCC材料"},
    {"code": "6779", "groups": ["mlcc", "optical_dc"], "priority": 7, "notes": "電子部品/光通信"},
    {"code": "6962", "groups": ["mlcc"], "priority": 7, "notes": "電子部品"},
    {"code": "6666", "groups": ["mlcc"], "priority": 7, "notes": "電子部品低位"},
    {"code": "5801", "groups": ["optical_dc"], "priority": 8, "notes": "光通信/DC"},
    {"code": "5803", "groups": ["optical_dc"], "priority": 8, "notes": "光通信/DC"},
    {"code": "5985", "groups": ["optical_dc"], "priority": 8, "notes": "光部品/DC"},
    {"code": "6503", "groups": ["optical_dc"], "priority": 8, "notes": "半導体/光デバイス"},
    {"code": "4410", "groups": ["material"], "priority": 8, "notes": "電子材/新聞テーマ"},
]

ROLE_COLUMNS = {
    "etf_live_training": "role_live_training",
    "etf_live_candidate": "role_live_candidate",
    "etf_direction": "role_direction",
    "etf_market": "role_market",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create TOPIX Core/Large/Mid + ETF J-Quants universe masters.")
    parser.add_argument("--meta-path", type=Path, default=META_PATH)
    parser.add_argument("--csv-out", type=Path, default=CSV_OUT)
    parser.add_argument("--parquet-out", type=Path, default=PARQUET_OUT)
    parser.add_argument("--no-csv", action="store_true")
    parser.add_argument("--no-parquet", action="store_true")
    return parser.parse_args()


def build_topix_rows(meta_path: Path) -> pd.DataFrame:
    if not meta_path.exists():
        raise FileNotFoundError(f"meta_jquants not found: {meta_path}")

    meta = pd.read_parquet(meta_path)
    required = {"ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries"}
    missing = sorted(required - set(meta.columns))
    if missing:
        raise ValueError(f"missing columns in {meta_path}: {missing}")

    df = meta[meta["topixnewindexseries"].isin(TOPIX_GROUPS)].copy()
    df["code"] = df["code"].astype(str)
    df["jquants_query_code"] = df["code"]
    df["instrument_type"] = "stock"
    df["source"] = "meta_jquants"
    df["universe"] = "topix_etf"
    df["universe_group"] = df["topixnewindexseries"].map(lambda x: TOPIX_GROUPS[x][0])
    df["topix_class"] = df["topixnewindexseries"].map(lambda x: TOPIX_GROUPS[x][1])
    df["priority"] = df["topixnewindexseries"].map(lambda x: TOPIX_GROUPS[x][2])
    df["display_order"] = None
    df["execution_rank"] = None
    df["groups"] = df["universe_group"]
    df["notes"] = ""
    df["fetch_minute"] = True
    df["fetch_tick"] = False
    df["active"] = True
    for col in ROLE_COLUMNS.values():
        df[col] = False
    return df


def build_etf_rows() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for item in ETF_DEFINITIONS:
        groups = list(item["groups"])
        row = {
            "ticker": f"{item['code']}.T",
            "code": item["code"],
            "jquants_query_code": item["code"],
            "stock_name": item["stock_name"],
            "market": "ETF",
            "sectors": "ETF",
            "series": "ETF",
            "topixnewindexseries": None,
            "instrument_type": "etf",
            "source": "manual_etf",
            "universe": "topix_etf",
            "universe_group": groups[0],
            "topix_class": "ETF",
            "priority": item["priority"],
            "display_order": item["display_order"],
            "execution_rank": item["execution_rank"],
            "groups": "|".join(groups),
            "notes": item["notes"],
            "fetch_minute": True,
            "fetch_tick": False,
            "active": True,
        }
        for group, col in ROLE_COLUMNS.items():
            row[col] = group in groups
        rows.append(row)
    return pd.DataFrame(rows)


def build_semiconductor_rows(meta_path: Path) -> pd.DataFrame:
    if not meta_path.exists():
        raise FileNotFoundError(f"meta_jquants not found: {meta_path}")

    meta = pd.read_parquet(meta_path)
    required = {"ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries"}
    missing = sorted(required - set(meta.columns))
    if missing:
        raise ValueError(f"missing columns in {meta_path}: {missing}")

    meta = meta.copy()
    meta["code"] = meta["code"].astype(str)
    by_code = meta.drop_duplicates("code").set_index("code")
    rows: list[dict[str, object]] = []
    missing_codes: list[str] = []

    for item in SEMICONDUCTOR_DEFINITIONS:
        code = item["code"]
        if code not in by_code.index:
            missing_codes.append(code)
            continue
        src = by_code.loc[code]
        groups = list(item["groups"])
        rows.append(
            {
                "ticker": src["ticker"],
                "code": code,
                "jquants_query_code": code,
                "stock_name": src["stock_name"],
                "market": src["market"],
                "sectors": src["sectors"],
                "series": src["series"],
                "topixnewindexseries": src["topixnewindexseries"],
                "instrument_type": "stock",
                "source": "semiconductor_watch",
                "universe": "topix_etf",
                "universe_group": groups[0],
                "topix_class": "semiconductor_watch",
                "priority": item["priority"],
                "display_order": item.get("display_order"),
                "execution_rank": None,
                "groups": "|".join(groups),
                "notes": item["notes"],
                "fetch_minute": True,
                "fetch_tick": False,
                "active": True,
                **{col: False for col in ROLE_COLUMNS.values()},
            }
        )

    if missing_codes:
        print(f"[WARN] missing semiconductor codes in meta_jquants: {', '.join(missing_codes)}")
    return pd.DataFrame(rows)


def merge_text(values: pd.Series) -> str:
    parts: list[str] = []
    for value in values.dropna().astype(str):
        for part in value.split("|"):
            part = part.strip()
            if part and part not in parts:
                parts.append(part)
    return "|".join(parts)


def merge_duplicate_tickers(df: pd.DataFrame) -> pd.DataFrame:
    if not df["ticker"].duplicated().any():
        return df

    merged_rows: list[dict[str, object]] = []
    for _, group in df.sort_values(["priority", "ticker"]).groupby("ticker", sort=False):
        row = group.iloc[0].to_dict()
        row["source"] = merge_text(group["source"])
        row["groups"] = merge_text(group["groups"])
        row["notes"] = merge_text(group["notes"])
        row["fetch_minute"] = bool(group["fetch_minute"].fillna(False).any())
        row["fetch_tick"] = bool(group["fetch_tick"].fillna(False).any())
        row["active"] = bool(group["active"].fillna(False).any())
        row["priority"] = int(pd.to_numeric(group["priority"], errors="coerce").min())
        if "display_order" in group.columns:
            display = pd.to_numeric(group["display_order"], errors="coerce").dropna()
            row["display_order"] = int(display.min()) if not display.empty else None
        if "execution_rank" in group.columns:
            execution = pd.to_numeric(group["execution_rank"], errors="coerce").dropna()
            row["execution_rank"] = int(execution.min()) if not execution.empty else None
        for col in ROLE_COLUMNS.values():
            row[col] = bool(group[col].fillna(False).any())
        merged_rows.append(row)
    return pd.DataFrame(merged_rows)


def build_universe(meta_path: Path) -> pd.DataFrame:
    topix = build_topix_rows(meta_path)
    etf = build_etf_rows()
    semiconductor = build_semiconductor_rows(meta_path)
    frames = [topix, semiconductor, etf]
    for frame in frames:
        for col in ["priority", "display_order", "execution_rank"]:
            frame[col] = pd.to_numeric(frame[col], errors="coerce").astype("Int64")
    df = pd.concat(frames, ignore_index=True)
    df = merge_duplicate_tickers(df)

    if df["ticker"].duplicated().any():
        dupes = df.loc[df["ticker"].duplicated(keep=False), "ticker"].tolist()
        raise ValueError(f"duplicate tickers in universe: {dupes}")

    columns = [
        "universe",
        "instrument_type",
        "source",
        "ticker",
        "code",
        "jquants_query_code",
        "stock_name",
        "market",
        "sectors",
        "series",
        "topixnewindexseries",
        "topix_class",
        "universe_group",
        "groups",
        "priority",
        "display_order",
        "execution_rank",
        "fetch_minute",
        "fetch_tick",
        "active",
        "role_live_training",
        "role_live_candidate",
        "role_direction",
        "role_market",
        "notes",
    ]
    df = df[columns].copy()
    df = df.sort_values(["display_order", "priority", "instrument_type", "code"], na_position="last").reset_index(drop=True)
    return df


def save_outputs(df: pd.DataFrame, csv_out: Path, parquet_out: Path, no_csv: bool, no_parquet: bool) -> None:
    if not no_csv:
        csv_out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_out, index=False)
        print(f"[OK] saved CSV: {csv_out}")

    if not no_parquet:
        parquet_out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(parquet_out, engine="pyarrow", index=False)
        print(f"[OK] saved Parquet: {parquet_out}")


def print_summary(df: pd.DataFrame) -> None:
    print("\nSummary")
    print("=" * 60)
    print(f"rows: {len(df)}")
    print("\nby instrument_type")
    print(df["instrument_type"].value_counts().to_string())
    print("\nby topix_class")
    print(df["topix_class"].value_counts().to_string())
    print("\nby universe_group")
    print(df["universe_group"].value_counts().to_string())
    print("\nsemiconductor groups")
    semicon = df[df["groups"].str.contains("semicon|equipment|substrate|mlcc|optical_dc|material|memory|wafer", na=False)]
    print(f"{len(semicon)} rows")
    print("\nETF rows")
    cols = ["ticker", "stock_name", "groups", "display_order", "execution_rank", "notes"]
    print(df[df["instrument_type"].eq("etf")].sort_values("display_order")[cols].to_string(index=False))


def main() -> int:
    args = parse_args()
    df = build_universe(args.meta_path)
    save_outputs(df, args.csv_out, args.parquet_out, args.no_csv, args.no_parquet)
    print_summary(df)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
