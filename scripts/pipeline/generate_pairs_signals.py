#!/usr/bin/env python3
"""
generate_pairs_signals.py  (v2)
ペアトレード シグナル生成（夜間バッチ）

Phase 70-75 検証済みパラメータ:
  - 共和分ベースの161ペア（Phase 70）
  - ペア固有の最適LOOKBACK（Phase 71）
  - Z_ENTRY=2.0, イントラデイ決済（Phase 72-73）
  - 100株単位の等金額ポジション（Phase 73b-74）
  - |z|降順で優先順位（Phase 73d）
  - レジーム・フィルターなし（Phase 75）

出力:
  data/parquet/pairs/pairs_signals_YYYY-MM-DD.parquet
"""
from __future__ import annotations

import math
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

load_dotenv_cascade()

PAIRS_DIR = PARQUET_DIR / "pairs"
PRICES_TOPIX = PARQUET_DIR / "granville" / "prices_topix.parquet"
PRICES_1D_CL = PARQUET_DIR / "screening" / "prices_max_1d_core_large.parquet"
PRICES_1D_MID = PARQUET_DIR / "screening" / "prices_max_1d_mid400.parquet"
PRICES_FALLBACK = PARQUET_DIR / "prices_max_1d.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"

Z_ENTRY = 2.0
CAPITAL = 2_000_000
MAX_RECOMMEND = 3  # |z|上位の推奨ペア数
BUFFER_COUNT = 3   # entry次点のバッファペア数（6種類足取得対象）

# Phase 70-71 共和分ベースペア (tk1, tk2, optimal_lookback, 5yr_pf, 5yr_n, half_life)
V2_PAIRS = [
    ("8336.T", "8399.T", 120, 4.45, 114, 31.3),
    ("8331.T", "8354.T", 250, 3.41, 107, 37.1),
    ("8331.T", "8551.T", 20, 3.38, 163, 34.2),
    ("7186.T", "8551.T", 10, 3.36, 183, 44.7),
    ("2053.T", "2060.T", 15, 3.28, 145, 54.2),
    ("4045.T", "7942.T", 15, 3.28, 157, 30.8),
    ("8345.T", "8399.T", 15, 3.06, 150, 42.6),
    ("3289.T", "8801.T", 10, 3.04, 197, 32.2),
    ("8367.T", "8551.T", 10, 3.03, 187, 37.8),
    ("8343.T", "8395.T", 15, 2.99, 173, 52.3),
    ("7167.T", "8411.T", 10, 2.89, 177, 36.6),
    ("8346.T", "8399.T", 10, 2.85, 146, 44.5),
    ("8012.T", "9934.T", 20, 2.85, 162, 34.6),
    ("7186.T", "7337.T", 30, 2.84, 159, 39.6),
    ("6479.T", "6981.T", 40, 2.76, 160, 27.2),
    ("4118.T", "4205.T", 10, 2.74, 194, 35.1),
    ("6407.T", "6490.T", 10, 2.69, 184, 46.2),
    ("4088.T", "4401.T", 30, 2.67, 146, 48.0),
    ("7267.T", "7270.T", 10, 2.66, 190, 38.4),
    ("4187.T", "4203.T", 250, 2.65, 99, 111.9),
    ("8341.T", "8359.T", 250, 2.63, 94, 50.1),
    ("8331.T", "8336.T", 10, 2.60, 186, 43.0),
    ("3050.T", "8218.T", 10, 2.59, 208, 36.9),
    ("6479.T", "6861.T", 40, 2.56, 136, 55.7),
    ("6470.T", "6471.T", 10, 2.55, 174, 46.5),
    ("4187.T", "4401.T", 120, 2.54, 118, 45.8),
    ("8604.T", "8707.T", 15, 2.50, 184, 22.8),
    ("7167.T", "7327.T", 15, 2.47, 193, 37.6),
    ("4183.T", "4208.T", 10, 2.46, 211, 29.3),
    ("3003.T", "3231.T", 10, 2.45, 163, 55.8),
    ("4045.T", "4956.T", 30, 2.45, 153, 52.1),
    ("3407.T", "4631.T", 250, 2.44, 93, 53.7),
    ("7182.T", "8336.T", 60, 2.43, 153, 37.6),
    ("6995.T", "7282.T", 30, 2.43, 197, 61.6),
    ("4095.T", "4401.T", 20, 2.43, 168, 47.2),
    ("2784.T", "7459.T", 10, 2.42, 217, 62.5),
    ("6113.T", "6301.T", 120, 2.41, 109, 26.8),
    ("5440.T", "5463.T", 20, 2.39, 141, 43.2),
    ("7189.T", "8341.T", 10, 2.38, 192, 50.0),
    ("4045.T", "4095.T", 60, 2.37, 129, 29.9),
    ("8306.T", "8411.T", 15, 2.36, 183, 74.0),
    ("9503.T", "9505.T", 40, 2.34, 141, 74.3),
    ("8308.T", "8551.T", 10, 2.32, 177, 55.7),
    ("7173.T", "7189.T", 120, 2.31, 87, 55.8),
    ("4118.T", "4401.T", 10, 2.30, 189, 74.5),
    ("8345.T", "8387.T", 15, 2.29, 162, 41.7),
    ("8343.T", "8370.T", 10, 2.28, 189, 58.2),
    ("6302.T", "6471.T", 15, 2.28, 171, 67.8),
    ("6954.T", "6981.T", 120, 2.27, 122, 39.2),
    ("8370.T", "8600.T", 15, 2.27, 158, 59.1),
    ("8359.T", "8411.T", 40, 2.27, 143, 59.2),
    ("8802.T", "8804.T", 15, 2.25, 186, 54.5),
    ("8015.T", "8058.T", 250, 2.24, 131, 70.0),
    ("8336.T", "8346.T", 250, 2.23, 121, 41.3),
    ("8343.T", "8551.T", 10, 2.23, 167, 72.4),
    ("6113.T", "6407.T", 10, 2.22, 185, 46.3),
    ("8346.T", "8387.T", 20, 2.20, 156, 55.4),
    ("8367.T", "8370.T", 10, 2.20, 185, 54.6),
    ("8392.T", "8544.T", 30, 2.20, 166, 40.6),
    ("8360.T", "8386.T", 10, 2.20, 171, 56.9),
    ("8346.T", "8395.T", 15, 2.15, 151, 55.0),
    ("7202.T", "7269.T", 250, 2.15, 109, 56.3),
    ("8386.T", "8392.T", 10, 2.14, 175, 61.9),
    ("8425.T", "8593.T", 60, 2.09, 134, 38.6),
    ("7182.T", "8331.T", 120, 2.09, 116, 35.8),
    ("6995.T", "7246.T", 250, 2.09, 95, 60.6),
    ("6856.T", "8035.T", 250, 2.08, 83, 46.1),
    ("8411.T", "8524.T", 10, 2.08, 154, 54.1),
    ("9101.T", "9104.T", 250, 2.06, 97, 38.2),
    ("2378.T", "4801.T", 10, 2.05, 182, 76.9),
    ("4063.T", "4187.T", 10, 2.04, 200, 97.5),
    ("6971.T", "6981.T", 10, 2.03, 207, 59.4),
    ("8359.T", "8714.T", 30, 2.01, 192, 59.3),
    ("4401.T", "4631.T", 10, 2.01, 188, 53.3),
    ("4042.T", "4401.T", 40, 1.97, 168, 45.7),
    ("8002.T", "8015.T", 40, 1.97, 163, 97.6),
    ("4095.T", "7942.T", 10, 1.96, 176, 48.6),
    ("8804.T", "8830.T", 10, 1.95, 180, 53.0),
    ("9005.T", "9021.T", 120, 1.95, 105, 48.7),
    ("8015.T", "8053.T", 250, 1.94, 123, 74.1),
    ("3099.T", "8242.T", 30, 1.92, 165, 52.5),
    ("5844.T", "8381.T", 250, 1.91, 131, 52.9),
    ("8344.T", "8600.T", 40, 1.90, 177, 48.4),
    ("8804.T", "8850.T", 20, 1.90, 169, 55.2),
    ("7167.T", "8524.T", 60, 1.90, 149, 66.8),
    ("3050.T", "7516.T", 250, 1.89, 103, 67.9),
    ("7173.T", "8341.T", 120, 1.89, 106, 35.4),
    ("8306.T", "8316.T", 10, 1.89, 200, 53.5),
    ("7421.T", "8160.T", 120, 1.88, 96, 45.9),
    ("3407.T", "4401.T", 40, 1.87, 148, 53.4),
    ("6305.T", "6473.T", 30, 1.86, 158, 36.6),
    ("8802.T", "8830.T", 30, 1.86, 162, 47.4),
    ("8343.T", "8367.T", 10, 1.86, 189, 27.4),
    ("8354.T", "8381.T", 15, 1.85, 188, 49.9),
    ("8343.T", "8345.T", 10, 1.83, 172, 67.0),
    ("8074.T", "8078.T", 60, 1.83, 125, 46.1),
    ("4095.T", "7988.T", 10, 1.82, 161, 55.0),
    ("2768.T", "8053.T", 40, 1.81, 173, 53.2),
    ("5703.T", "5714.T", 10, 1.79, 182, 36.8),
    ("8766.T", "8795.T", 30, 1.79, 185, 69.9),
    ("7911.T", "7912.T", 10, 1.78, 190, 58.5),
    ("5844.T", "7337.T", 120, 1.78, 160, 50.2),
    ("8345.T", "8346.T", 15, 1.77, 175, 46.8),
    ("9042.T", "9045.T", 10, 1.76, 166, 61.8),
    ("4042.T", "4045.T", 120, 1.76, 118, 56.3),
    ("7182.T", "8346.T", 60, 1.73, 158, 47.1),
    ("4095.T", "4631.T", 20, 1.72, 155, 37.0),
    ("8359.T", "8524.T", 10, 1.70, 169, 76.5),
    ("7182.T", "8387.T", 10, 1.69, 187, 45.1),
    ("7327.T", "8411.T", 60, 1.69, 155, 40.2),
    ("8439.T", "8591.T", 120, 1.67, 136, 75.6),
    ("7202.T", "7203.T", 40, 1.67, 173, 54.9),
    ("3923.T", "3994.T", 40, 1.66, 166, 42.2),
    ("5844.T", "8331.T", 20, 1.66, 176, 44.9),
    ("7246.T", "7282.T", 120, 1.64, 116, 57.2),
    ("8439.T", "8566.T", 10, 1.63, 189, 70.7),
    ("8338.T", "8399.T", 10, 1.63, 160, 33.3),
    ("5844.T", "8354.T", 250, 1.63, 91, 45.2),
    ("6103.T", "6305.T", 15, 1.62, 170, 36.6),
    ("8524.T", "8714.T", 15, 1.61, 154, 53.6),
    ("8001.T", "8020.T", 10, 1.61, 164, 48.1),
    ("8368.T", "8524.T", 10, 1.59, 157, 45.4),
    ("9001.T", "9048.T", 10, 1.58, 189, 61.7),
    ("8346.T", "8600.T", 60, 1.58, 123, 55.7),
    ("8345.T", "8600.T", 10, 1.57, 156, 41.2),
    ("8386.T", "8544.T", 10, 1.57, 175, 38.4),
    ("8020.T", "8031.T", 120, 1.55, 118, 65.3),
    ("9508.T", "9509.T", 10, 1.55, 181, 41.4),
    ("8078.T", "8084.T", 10, 1.51, 175, 37.3),
    ("8020.T", "8053.T", 250, 1.50, 102, 50.2),
    ("7182.T", "8399.T", 60, 1.50, 179, 36.6),
    ("5844.T", "7186.T", 10, 1.49, 190, 54.5),
    ("8331.T", "8381.T", 10, 1.47, 190, 51.5),
    ("2768.T", "8020.T", 10, 1.45, 165, 69.0),
    ("6302.T", "6473.T", 20, 1.45, 171, 61.5),
    ("8334.T", "8522.T", 10, 1.44, 175, 51.5),
    ("9021.T", "9042.T", 20, 1.44, 173, 66.3),
    ("8338.T", "8387.T", 20, 1.43, 143, 56.0),
    ("8386.T", "8524.T", 15, 1.42, 154, 44.4),
    ("7272.T", "7313.T", 10, 1.40, 183, 49.2),
    ("7267.T", "7272.T", 15, 1.38, 180, 64.9),
    ("9021.T", "9142.T", 10, 1.37, 184, 118.6),
    ("9007.T", "9048.T", 250, 1.37, 84, 99.7),
    ("4911.T", "4922.T", 250, 1.36, 125, 47.5),
    ("7173.T", "7184.T", 40, 1.32, 134, 48.0),
    ("7182.T", "8354.T", 120, 1.31, 109, 50.4),
    ("7182.T", "8338.T", 20, 1.29, 141, 50.6),
    ("9008.T", "9048.T", 250, 1.27, 93, 88.1),
    ("3994.T", "4443.T", 250, 1.26, 85, 65.3),
    ("7327.T", "8524.T", 40, 1.24, 174, 41.0),
    ("8020.T", "8058.T", 10, 1.22, 179, 63.9),
    ("6103.T", "6473.T", 10, 1.21, 181, 70.4),
    ("9005.T", "9042.T", 15, 1.21, 172, 78.4),
    ("7184.T", "8341.T", 10, 1.21, 171, 50.9),
    ("8358.T", "8524.T", 120, 1.20, 105, 87.8),
    ("7181.T", "8750.T", 120, 1.20, 137, 75.4),
    ("9003.T", "9042.T", 10, 1.16, 154, 63.5),
    ("9001.T", "9007.T", 10, 1.13, 183, 107.0),
    ("8053.T", "8058.T", 10, 1.10, 175, 84.1),
    ("8031.T", "8058.T", 10, 1.08, 166, 51.4),
    ("9041.T", "9048.T", 60, 0.87, 167, 71.6),
]


def load_names() -> dict[str, str]:
    if META_PATH.exists():
        meta = pd.read_parquet(META_PATH)
        return dict(zip(meta["ticker"], meta["stock_name"]))
    return {}


def load_prices(max_lookback: int) -> pd.DataFrame:
    """価格データ読み込み（prices_topix優先、なければscreening→prices_max_1d）"""
    days = max_lookback + 10  # lookback + 余裕
    frames = []

    if PRICES_TOPIX.exists():
        frames.append(pd.read_parquet(PRICES_TOPIX))
        print(f"  [prices] Using prices_topix ({PRICES_TOPIX.name})")

    if not frames:
        if PRICES_1D_CL.exists():
            frames.append(pd.read_parquet(PRICES_1D_CL))
        if PRICES_1D_MID.exists():
            frames.append(pd.read_parquet(PRICES_1D_MID))
        if frames:
            print("  [prices] Fallback: screening parquets")

    if not frames and PRICES_FALLBACK.exists():
        frames.append(pd.read_parquet(PRICES_FALLBACK))
        print("  [prices] Fallback: prices_max_1d")

    if not frames:
        return pd.DataFrame()

    ps = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ticker", "date"])
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"])
    latest_dates = sorted(ps["date"].unique())[-days:]
    ps = ps[ps["date"].isin(latest_dates)].copy()
    return ps


def calc_shares_min_lot(c1: float, c2: float) -> tuple[int, int]:
    """最小100株単位ペアサイジング（PF差なし確認済み、不均衡+1.5%程度）"""
    if c1 <= c2:
        s1 = max(1, round(c2 / c1)) * 100
        s2 = 100
    else:
        s1 = 100
        s2 = max(1, round(c1 / c2)) * 100
    return s1, s2


def calc_pair_signal(
    ps: pd.DataFrame, tk1: str, tk2: str, lookback: int,
) -> dict | None:
    """1ペアのz-score・閾値・ポジションサイズを計算"""
    d1 = ps[ps["ticker"] == tk1].set_index("date").sort_index()
    d2 = ps[ps["ticker"] == tk2].set_index("date").sort_index()
    common = d1.index.intersection(d2.index)
    if len(common) < lookback + 5:
        return None
    d1 = d1.loc[common]
    d2 = d2.loc[common]

    spread_close = np.log(d1["Close"].values / d2["Close"].values)

    mu = spread_close[-lookback:].mean()
    sigma = spread_close[-lookback:].std()
    if sigma < 1e-8:
        return None

    latest_spread = spread_close[-1]
    z_latest = (latest_spread - mu) / sigma

    # エントリー閾値（tk1の理論価格）
    spread_entry_long = mu + Z_ENTRY * sigma
    spread_entry_short = mu - Z_ENTRY * sigma

    c1_last = float(d1["Close"].iloc[-1])
    c2_last = float(d2["Close"].iloc[-1])

    ratio_upper = np.exp(spread_entry_long)
    ratio_lower = np.exp(spread_entry_short)

    tk1_upper = c2_last * ratio_upper
    tk1_lower = c2_last * ratio_lower

    # 最小100株単位ポジション
    shares1, shares2 = calc_shares_min_lot(c1_last, c2_last)
    notional1 = c1_last * shares1
    notional2 = c2_last * shares2
    imbalance_pct = abs(notional1 - notional2) / max(notional1, notional2) * 100

    return {
        "tk1": tk1,
        "tk2": tk2,
        "c1": round(c1_last, 1),
        "c2": round(c2_last, 1),
        "z_latest": round(z_latest, 3),
        "z_abs": round(abs(z_latest), 3),
        "mu": round(mu, 6),
        "sigma": round(sigma, 6),
        "lookback": lookback,
        "tk1_upper": round(tk1_upper, 1),
        "tk1_lower": round(tk1_lower, 1),
        "shares1": shares1,
        "shares2": shares2,
        "notional1": round(notional1),
        "notional2": round(notional2),
        "imbalance_pct": round(imbalance_pct, 1),
        "is_entry": abs(z_latest) >= Z_ENTRY,
        "direction": "short_tk1" if z_latest > 0 else "long_tk1",
        "signal_date": common[-1],
    }


def main() -> int:
    print("=" * 60)
    print("Generate Pairs Trading Signals (v2)")
    print(f"  {datetime.now().isoformat()}")
    print(f"  Pairs: {len(V2_PAIRS)}, Z_ENTRY: {Z_ENTRY}")
    print("=" * 60)

    PAIRS_DIR.mkdir(parents=True, exist_ok=True)

    max_lb = max(lb for _, _, lb, *_ in V2_PAIRS)
    print(f"\n[1/3] Loading prices (max lookback={max_lb})...")
    ps = load_prices(max_lookback=max_lb)
    if ps.empty:
        print("[ERROR] No price data available")
        return 1
    print(f"  {len(ps):,} rows, {ps['ticker'].nunique()} tickers")

    names = load_names()

    print("\n[2/3] Calculating pair signals...")
    rows: list[dict] = []
    skip_count = 0
    for tk1, tk2, lookback, full_pf, full_n, half_life in V2_PAIRS:
        r = calc_pair_signal(ps, tk1, tk2, lookback)
        if r:
            r["name1"] = names.get(tk1, tk1)
            r["name2"] = names.get(tk2, tk2)
            r["full_pf"] = full_pf
            r["full_n"] = full_n
            r["half_life"] = half_life
            rows.append(r)
        else:
            skip_count += 1

    latest_date = ps["date"].max()
    date_str = latest_date.strftime("%Y-%m-%d")

    if rows:
        df = pd.DataFrame(rows)
        df = df.sort_values("z_abs", ascending=False)
        # entry次点のバッファ（6種類足取得対象、is_entry=Falseの|z|上位）
        non_entry = df[~df["is_entry"]].head(BUFFER_COUNT).index
        df["is_buffer"] = False
        df.loc[non_entry, "is_buffer"] = True
    else:
        df = pd.DataFrame()

    signal_path = PAIRS_DIR / f"pairs_signals_{date_str}.parquet"
    df.to_parquet(signal_path, index=False)

    entry_count = int(df["is_entry"].sum()) if not df.empty else 0
    buffer_count = int(df["is_buffer"].sum()) if not df.empty and "is_buffer" in df.columns else 0
    print(f"\n  Computed: {len(df)}, Skipped: {skip_count}")
    print(f"  Entry signals (|z|>={Z_ENTRY}): {entry_count}, Buffer: {buffer_count}")

    # 推奨ペア表示（|z|上位）
    if not df.empty:
        hot = df[df["is_entry"]].head(MAX_RECOMMEND)
        if not hot.empty:
            print(f"\n  === TOP {len(hot)} RECOMMENDATIONS ===")
            for _, r in hot.iterrows():
                arrow = "SHORT" if r["direction"] == "short_tk1" else "LONG"
                print(
                    f"  {r['name1']}/{r['name2']}  "
                    f"z={r['z_latest']:+.2f}  {arrow} tk1  "
                    f"LB={r['lookback']}  PF={r['full_pf']:.2f}  "
                    f"{r['shares1']}株/{r['shares2']}株"
                )
        else:
            print("\n  No entry signals today")

    # S3アップロード
    print("\n[3/3] Uploading to S3...")
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            upload_file(cfg, signal_path, f"pairs/pairs_signals_{date_str}.parquet")
        else:
            print("  [INFO] S3 bucket not configured")
    except Exception as e:
        print(f"  [WARN] S3 upload failed: {e}")

    print(f"\n{'=' * 60}")
    print(f"Date: {date_str}")
    print(f"Pairs: {len(df)}, Entry: {entry_count}, Buffer: {buffer_count}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
