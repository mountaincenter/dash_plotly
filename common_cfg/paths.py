# common_cfg/paths.py
# -*- coding: utf-8 -*-
"""
common_cfg.paths: パス定義（常にプロジェクトルート基準）
"""
from pathlib import Path

# このファイルの位置（.../common_cfg/paths.py）からプロジェクトルートを解決
ROOT = Path(__file__).resolve().parents[1]

PARQUET_DIR = ROOT / "data" / "parquet"
TOPIX_WEIGHT_PARQUET = PARQUET_DIR / "topixweight_j.parquet"
CORE30_META_PARQUET = PARQUET_DIR / "core30_meta.parquet"
CORE30_PRICES_PARQUET = PARQUET_DIR / "core30_prices_1y_1d.parquet"
CORE30_ANOMALY_PARQUET = PARQUET_DIR / "core30_anomaly.parquet"
MANIFEST_JSON = PARQUET_DIR / "manifest.json"

# ---- 後方互換エイリアス（ノート/旧コードが期待する名前）----
WEIGHT_PARQUET = TOPIX_WEIGHT_PARQUET
OUT_META = CORE30_META_PARQUET
OUT_PRICES = CORE30_PRICES_PARQUET
OUT_ANOMALY = CORE30_ANOMALY_PARQUET
MANIFEST_PATH = MANIFEST_JSON
META_PARQUET = CORE30_META_PARQUET
