#!/usr/bin/env python3
"""
create_universe.py
戦略スクリーニング用のユニバース定義ファイル（universe.parquet）を作成

入力:
  - data/parquet/meta_jquants.parquet（銘柄マスター）
  - data/parquet/granville/prices_topix.parquet（日足価格）

出力:
  - data/parquet/universe.parquet

各銘柄に以下の区分ラベルを付与:
  - topix_class: Core30 / Large70 / Mid400 / Small1 / Small2 / None
  - market: プライム / スタンダード / グロース
  - sector33: 東証33業種
  - sector17: 東証17業種
  - price_tier: ~500 / 500-2000 / 2000-5000 / 5000+
  - liquidity_tier: Low / Mid / High / VeryHigh
  - avg_volume_20d: 直近20営業日の平均出来高
  - latest_close: 直近終値
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

META_PATH = PARQUET_DIR / "meta_jquants.parquet"
PRICES_PATH = PARQUET_DIR / "granville" / "prices_topix.parquet"
OUT_PATH = PARQUET_DIR / "universe.parquet"


def load_meta() -> pd.DataFrame:
    """銘柄マスター読み込み"""
    meta = pd.read_parquet(META_PATH)
    print(f"  meta_jquants: {len(meta)} stocks")

    # topix_class の正規化
    topix_map = {
        "TOPIX Core30": "Core30",
        "TOPIX Large70": "Large70",
        "TOPIX Mid400": "Mid400",
        "TOPIX Small 1": "Small1",
        "TOPIX Small 2": "Small2",
    }
    meta["topix_class"] = meta["topixnewindexseries"].map(topix_map).fillna("None")

    return meta[["ticker", "code", "stock_name", "market", "sectors", "series", "topix_class"]].copy()


def compute_price_volume(meta: pd.DataFrame) -> pd.DataFrame:
    """直近20営業日の株価・出来高統計を計算"""
    prices = pd.read_parquet(PRICES_PATH)
    prices["date"] = pd.to_datetime(prices["date"])

    # prices_topixに含まれる銘柄のみ対象
    tickers_in_prices = set(prices["ticker"].unique())
    print(f"  prices_topix: {len(tickers_in_prices)} tickers")

    # 直近20営業日
    dates_sorted = sorted(prices["date"].unique())
    cutoff_20d = dates_sorted[-20] if len(dates_sorted) >= 20 else dates_sorted[0]
    recent = prices[prices["date"] >= cutoff_20d].copy()

    # 直近終値（最終日のデータ）
    latest_date = prices["date"].max()
    latest_prices = prices[prices["date"] == latest_date][["ticker", "Close"]].rename(
        columns={"Close": "latest_close"}
    )

    # 20日平均出来高
    vol_stats = recent.groupby("ticker").agg(
        avg_volume_20d=("Volume", "mean"),
    ).reset_index()

    # 結合
    stats = latest_prices.merge(vol_stats, on="ticker", how="outer")

    # metaと結合（prices_topixにある銘柄のみ）
    df = meta.merge(stats, on="ticker", how="inner")
    print(f"  Merged: {len(df)} stocks (meta ∩ prices)")

    return df


def assign_tiers(df: pd.DataFrame) -> pd.DataFrame:
    """株価帯・流動性帯を割り当て"""
    # 株価帯
    bins_price = [0, 500, 2000, 5000, float("inf")]
    labels_price = ["~500", "500-2000", "2000-5000", "5000+"]
    df["price_tier"] = pd.cut(
        df["latest_close"], bins=bins_price, labels=labels_price, include_lowest=True
    ).astype(str)

    # 流動性帯（20日平均出来高ベース）
    # 四分位で区切る
    q = df["avg_volume_20d"].quantile([0.25, 0.50, 0.75])
    bins_vol = [0, q[0.25], q[0.50], q[0.75], float("inf")]
    labels_vol = ["Low", "Mid", "High", "VeryHigh"]
    df["liquidity_tier"] = pd.cut(
        df["avg_volume_20d"], bins=bins_vol, labels=labels_vol, include_lowest=True
    ).astype(str)

    return df


def main() -> int:
    print("=" * 60)
    print("Create universe.parquet")
    print("=" * 60)

    print("\n[1/3] Loading data...")
    meta = load_meta()
    df = compute_price_volume(meta)

    print("\n[2/3] Assigning tiers...")
    df = assign_tiers(df)

    # NaN除去
    df = df.dropna(subset=["latest_close", "avg_volume_20d"]).reset_index(drop=True)

    # 最終カラム順
    cols = [
        "ticker", "code", "stock_name",
        "market", "sectors", "series", "topix_class",
        "latest_close", "avg_volume_20d",
        "price_tier", "liquidity_tier",
    ]
    df = df[cols]

    print(f"\n[3/3] Saving...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, engine="pyarrow", index=False)
    print(f"  Saved: {OUT_PATH}")

    # サマリー
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total: {len(df)} stocks")

    print(f"\n■ TOPIX区分")
    print(df["topix_class"].value_counts().to_string())

    print(f"\n■ 市場")
    print(df["market"].value_counts().to_string())

    print(f"\n■ 株価帯")
    print(df["price_tier"].value_counts().reindex(["~500", "500-2000", "2000-5000", "5000+"]).to_string())

    print(f"\n■ 流動性")
    q = df["avg_volume_20d"].quantile([0.25, 0.50, 0.75])
    print(f"  Q25={q[0.25]:,.0f}  Q50={q[0.50]:,.0f}  Q75={q[0.75]:,.0f}")
    print(df["liquidity_tier"].value_counts().reindex(["Low", "Mid", "High", "VeryHigh"]).to_string())

    print("\n" + "=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
