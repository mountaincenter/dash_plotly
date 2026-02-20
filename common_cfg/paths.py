# common_cfg/paths.py
# -*- coding: utf-8 -*-
"""
common_cfg.paths: パス定義（常にプロジェクトルート基準）
"""
from pathlib import Path

# このファイルの位置（.../common_cfg/paths.py）からプロジェクトルートを解決
ROOT = Path(__file__).resolve().parents[1]

PARQUET_DIR = ROOT / "data" / "parquet"
REPORTS_DIR = ROOT / "data" / "reports"
MASTER_META_PARQUET = PARQUET_DIR / "meta.parquet"
CORE30_META_PARQUET = MASTER_META_PARQUET  # backward compatibility alias

PRICE_FILE_TEMPLATE = "prices_{period}_{interval}.parquet"


def price_parquet(period: str, interval: str):
    return PARQUET_DIR / PRICE_FILE_TEMPLATE.format(period=period, interval=interval)


PRICE_SPECS = [
    ("max", "1d"),
    ("max", "1mo"),
    ("max", "1h"),  # 730d → max に変更（新規上場銘柄対応）
    ("60d", "5m"),
    ("60d", "15m"),
]

PRICES_MAX_1D_PARQUET = price_parquet("max", "1d")
CORE30_PRICES_PARQUET = PRICES_MAX_1D_PARQUET  # backward compatibility alias
MANIFEST_JSON = PARQUET_DIR / "manifest.json"
TECH_SNAPSHOT_PARQUET = PARQUET_DIR / "tech_snapshot_1d.parquet"
CORE30_TECH_SNAPSHOT_PARQUET = TECH_SNAPSHOT_PARQUET  # backward compatibility alias

# ---- 後方互換エイリアス（ノート/旧コードが期待する名前）----
OUT_META = MASTER_META_PARQUET
OUT_PRICES = CORE30_PRICES_PARQUET
MANIFEST_PATH = MANIFEST_JSON
META_PARQUET = MASTER_META_PARQUET
