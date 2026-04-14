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
META_PATH = PARQUET_DIR / "meta_jquants.parquet"

Z_ENTRY = 2.0
CAPITAL = 2_000_000
MAX_RECOMMEND = 3  # |z|上位の推奨ペア数
BUFFER_COUNT = 3   # entry次点のバッファペア数（6種類足取得対象）

# Phase 70-71 共和分ベースペア (tk1, tk2, optimal_lookback, 5yr_pf, 5yr_n, revert_1d)
# revert_1d: |z|≥2発動時に翌日|z|が縮小した割合(%). 1日完結戦略の期待勝率
V2_PAIRS = [
    ("8336.T", "8399.T", 120, 4.45, 114, 68.3),
    ("8331.T", "8354.T", 250, 3.41, 107, 62.4),
    ("8331.T", "8551.T", 20, 3.38, 163, 78.7),
    ("7186.T", "8551.T", 10, 3.36, 183, 91.9),
    ("2053.T", "2060.T", 15, 3.28, 145, 90.1),
    ("4045.T", "7942.T", 15, 3.28, 157, 86.6),
    ("8345.T", "8399.T", 15, 3.06, 150, 87.1),
    ("3289.T", "8801.T", 10, 3.04, 197, 93.5),
    ("8367.T", "8551.T", 10, 3.03, 187, 91.5),
    ("8343.T", "8395.T", 15, 2.99, 173, 83.5),
    ("7167.T", "8411.T", 10, 2.89, 177, 89.0),
    ("8346.T", "8399.T", 10, 2.85, 146, 87.2),
    ("8012.T", "9934.T", 20, 2.85, 162, 87.2),
    ("7186.T", "7337.T", 30, 2.84, 159, 77.5),
    ("6479.T", "6981.T", 40, 2.76, 160, 77.3),
    ("4118.T", "4205.T", 10, 2.74, 194, 89.2),
    ("6407.T", "6490.T", 10, 2.69, 184, 92.4),
    ("4088.T", "4401.T", 30, 2.67, 146, 74.8),
    ("7267.T", "7270.T", 10, 2.66, 190, 91.3),
    ("4187.T", "4203.T", 250, 2.65, 99, 65.6),
    ("8341.T", "8359.T", 250, 2.63, 94, 61.6),
    ("8331.T", "8336.T", 10, 2.60, 186, 91.0),
    ("3050.T", "8218.T", 10, 2.59, 208, 95.3),
    ("6479.T", "6861.T", 40, 2.56, 136, 78.0),
    ("6470.T", "6471.T", 10, 2.55, 174, 95.3),
    ("4187.T", "4401.T", 120, 2.54, 118, 67.3),
    ("8604.T", "8707.T", 15, 2.50, 184, 81.2),
    ("7167.T", "7327.T", 15, 2.47, 193, 83.7),
    ("4183.T", "4208.T", 10, 2.46, 211, 90.7),
    ("3003.T", "3231.T", 10, 2.45, 163, 86.5),
    ("4045.T", "4956.T", 30, 2.45, 153, 81.6),
    ("3407.T", "4631.T", 250, 2.44, 93, 62.6),
    ("7182.T", "8336.T", 60, 2.43, 153, 68.7),
    ("6995.T", "7282.T", 30, 2.43, 197, 80.4),
    ("4095.T", "4401.T", 20, 2.43, 168, 83.9),
    ("2784.T", "7459.T", 10, 2.42, 217, 90.2),
    ("6113.T", "6301.T", 120, 2.41, 109, 72.8),
    ("5440.T", "5463.T", 20, 2.39, 141, 89.7),
    ("7189.T", "8341.T", 10, 2.38, 192, 94.3),
    ("4045.T", "4095.T", 60, 2.37, 129, 74.6),
    ("8306.T", "8411.T", 15, 2.36, 183, 81.8),
    ("9503.T", "9505.T", 40, 2.34, 141, 76.3),
    ("8308.T", "8551.T", 10, 2.32, 177, 96.0),
    ("7173.T", "7189.T", 120, 2.31, 87, 71.4),
    ("4118.T", "4401.T", 10, 2.30, 189, 91.0),
    ("8345.T", "8387.T", 15, 2.29, 162, 82.6),
    ("8343.T", "8370.T", 10, 2.28, 189, 90.7),
    ("6302.T", "6471.T", 15, 2.28, 171, 87.5),
    ("6954.T", "6981.T", 120, 2.27, 122, 68.9),
    ("8370.T", "8600.T", 15, 2.27, 158, 86.6),
    ("8359.T", "8411.T", 40, 2.27, 143, 74.6),
    ("8802.T", "8804.T", 15, 2.25, 186, 80.7),
    ("8015.T", "8058.T", 250, 2.24, 131, 60.2),
    ("8336.T", "8346.T", 250, 2.23, 121, 55.7),
    ("8343.T", "8551.T", 10, 2.23, 167, 89.2),
    ("6113.T", "6407.T", 10, 2.22, 185, 86.7),
    ("8346.T", "8387.T", 20, 2.20, 156, 84.1),
    ("8367.T", "8370.T", 10, 2.20, 185, 89.4),
    ("8392.T", "8544.T", 30, 2.20, 166, 72.5),
    ("8360.T", "8386.T", 10, 2.20, 171, 87.9),
    ("8346.T", "8395.T", 15, 2.15, 151, 85.0),
    ("7202.T", "7269.T", 250, 2.15, 109, 59.4),
    ("8386.T", "8392.T", 10, 2.14, 175, 92.3),
    ("8425.T", "8593.T", 60, 2.09, 134, 72.6),
    ("7182.T", "8331.T", 120, 2.09, 116, 61.2),
    ("6995.T", "7246.T", 250, 2.09, 95, 53.6),
    ("6856.T", "8035.T", 250, 2.08, 83, 62.7),
    ("8411.T", "8524.T", 10, 2.08, 154, 90.6),
    ("9101.T", "9104.T", 250, 2.06, 97, 55.7),
    ("2378.T", "4801.T", 10, 2.05, 182, 96.7),
    ("4063.T", "4187.T", 10, 2.04, 200, 92.5),
    ("6971.T", "6981.T", 10, 2.03, 207, 94.9),
    ("8359.T", "8714.T", 30, 2.01, 192, 78.9),
    ("4401.T", "4631.T", 10, 2.01, 188, 92.2),
    ("4042.T", "4401.T", 40, 1.97, 168, 72.5),
    ("8002.T", "8015.T", 40, 1.97, 163, 79.1),
    ("4095.T", "7942.T", 10, 1.96, 176, 89.8),
    ("8804.T", "8830.T", 10, 1.95, 180, 89.9),
    ("9005.T", "9021.T", 120, 1.95, 105, 64.6),
    ("8015.T", "8053.T", 250, 1.94, 123, 60.2),
    ("3099.T", "8242.T", 30, 1.92, 165, 77.6),
    ("5844.T", "8381.T", 250, 1.91, 131, 61.0),
    ("8344.T", "8600.T", 40, 1.90, 177, 69.4),
    ("8804.T", "8850.T", 20, 1.90, 169, 79.2),
    ("7167.T", "8524.T", 60, 1.90, 149, 72.4),
    ("3050.T", "7516.T", 250, 1.89, 103, 60.6),
    ("7173.T", "8341.T", 120, 1.89, 106, 70.0),
    ("8306.T", "8316.T", 10, 1.89, 200, 89.3),
    ("7421.T", "8160.T", 120, 1.88, 96, 68.0),
    ("3407.T", "4401.T", 40, 1.87, 148, 78.3),
    ("6305.T", "6473.T", 30, 1.86, 158, 78.9),
    ("8802.T", "8830.T", 30, 1.86, 162, 75.6),
    ("8343.T", "8367.T", 10, 1.86, 189, 88.9),
    ("8354.T", "8381.T", 15, 1.85, 188, 82.3),
    ("8343.T", "8345.T", 10, 1.83, 172, 92.9),
    ("8074.T", "8078.T", 60, 1.83, 125, 71.1),
    ("4095.T", "7988.T", 10, 1.82, 161, 89.7),
    ("2768.T", "8053.T", 40, 1.81, 173, 75.3),
    ("5703.T", "5714.T", 10, 1.79, 182, 90.0),
    ("8766.T", "8795.T", 30, 1.79, 185, 80.2),
    ("7911.T", "7912.T", 10, 1.78, 190, 90.5),
    ("5844.T", "7337.T", 120, 1.78, 160, 66.0),
    ("8345.T", "8346.T", 15, 1.77, 175, 87.2),
    ("9042.T", "9045.T", 10, 1.76, 166, 88.8),
    ("4042.T", "4045.T", 120, 1.76, 118, 62.2),
    ("7182.T", "8346.T", 60, 1.73, 158, 68.3),
    ("4095.T", "4631.T", 20, 1.72, 155, 86.0),
    ("8359.T", "8524.T", 10, 1.70, 169, 94.1),
    ("7182.T", "8387.T", 10, 1.69, 187, 91.5),
    ("7327.T", "8411.T", 60, 1.69, 155, 69.1),
    ("8439.T", "8591.T", 120, 1.67, 136, 66.9),
    ("7202.T", "7203.T", 40, 1.67, 173, 79.2),
    ("3923.T", "3994.T", 40, 1.66, 166, 73.9),
    ("5844.T", "8331.T", 20, 1.66, 176, 78.4),
    ("7246.T", "7282.T", 120, 1.64, 116, 68.3),
    ("8439.T", "8566.T", 10, 1.63, 189, 90.6),
    ("8338.T", "8399.T", 10, 1.63, 160, 87.5),
    ("5844.T", "8354.T", 250, 1.63, 91, 60.5),
    ("6103.T", "6305.T", 15, 1.62, 170, 90.8),
    ("8524.T", "8714.T", 15, 1.61, 154, 85.8),
    ("8001.T", "8020.T", 10, 1.61, 164, 93.1),
    ("8368.T", "8524.T", 10, 1.59, 157, 92.3),
    ("9001.T", "9048.T", 10, 1.58, 189, 87.3),
    ("8346.T", "8600.T", 60, 1.58, 123, 73.7),
    ("8345.T", "8600.T", 10, 1.57, 156, 89.3),
    ("8386.T", "8544.T", 10, 1.57, 175, 90.1),
    ("8020.T", "8031.T", 120, 1.55, 118, 66.7),
    ("9508.T", "9509.T", 10, 1.55, 181, 94.4),
    ("8078.T", "8084.T", 10, 1.51, 175, 90.0),
    ("8020.T", "8053.T", 250, 1.50, 102, 64.6),
    ("7182.T", "8399.T", 60, 1.50, 179, 69.6),
    ("5844.T", "7186.T", 10, 1.49, 190, 93.2),
    ("8331.T", "8381.T", 10, 1.47, 190, 85.1),
    ("2768.T", "8020.T", 10, 1.45, 165, 87.9),
    ("6302.T", "6473.T", 20, 1.45, 171, 82.9),
    ("8334.T", "8522.T", 10, 1.44, 175, 87.1),
    ("9021.T", "9042.T", 20, 1.44, 173, 79.6),
    ("8338.T", "8387.T", 20, 1.43, 143, 77.0),
    ("8386.T", "8524.T", 15, 1.42, 154, 86.3),
    ("7272.T", "7313.T", 10, 1.40, 183, 92.4),
    ("7267.T", "7272.T", 15, 1.38, 180, 89.1),
    ("9021.T", "9142.T", 10, 1.37, 184, 92.3),
    ("9007.T", "9048.T", 250, 1.37, 84, 58.8),
    ("4911.T", "4922.T", 250, 1.36, 125, 56.9),
    ("7173.T", "7184.T", 40, 1.32, 134, 76.8),
    ("7182.T", "8354.T", 120, 1.31, 109, 59.3),
    ("7182.T", "8338.T", 20, 1.29, 141, 79.8),
    ("9008.T", "9048.T", 250, 1.27, 93, 58.9),
    ("3994.T", "4443.T", 250, 1.26, 85, 66.3),
    ("7327.T", "8524.T", 40, 1.24, 174, 81.9),
    ("8020.T", "8058.T", 10, 1.22, 179, 90.4),
    ("6103.T", "6473.T", 10, 1.21, 181, 90.2),
    ("9005.T", "9042.T", 15, 1.21, 172, 80.6),
    ("7184.T", "8341.T", 10, 1.21, 171, 85.7),
    ("8358.T", "8524.T", 120, 1.20, 105, 68.9),
    ("7181.T", "8750.T", 120, 1.20, 137, 67.5),
    ("9003.T", "9042.T", 10, 1.16, 154, 87.6),
    ("9001.T", "9007.T", 10, 1.13, 183, 90.2),
    ("8053.T", "8058.T", 10, 1.10, 175, 87.3),
    ("8031.T", "8058.T", 10, 1.08, 166, 86.3),
    ("9041.T", "9048.T", 60, 0.87, 167, 67.7),
]


def load_names() -> dict[str, str]:
    if META_PATH.exists():
        meta = pd.read_parquet(META_PATH)
        return dict(zip(meta["ticker"], meta["stock_name"]))
    return {}


def load_prices(max_lookback: int) -> pd.DataFrame:
    """価格データ読み込み（prices_topixのみ）"""
    days = max_lookback + 10  # lookback + 余裕

    if not PRICES_TOPIX.exists():
        print(f"  [ERROR] Missing prices file: {PRICES_TOPIX}")
        return pd.DataFrame()

    print(f"  [prices] Using prices_topix ({PRICES_TOPIX.name})")
    ps = pd.read_parquet(PRICES_TOPIX).drop_duplicates(subset=["ticker", "date"])
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
    for tk1, tk2, lookback, full_pf, full_n, revert_1d in V2_PAIRS:
        r = calc_pair_signal(ps, tk1, tk2, lookback)
        if r:
            r["name1"] = names.get(tk1, tk1)
            r["name2"] = names.get(tk2, tk2)
            r["full_pf"] = full_pf
            r["full_n"] = full_n
            r["revert_1d"] = revert_1d
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
