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
SIGNALS_PATH = PARQUET_DIR / "signals.parquet"

Z_ENTRY = 2.0
CAPITAL = 2_000_000
MAX_RECOMMEND = 3  # |z|上位の推奨ペア数
BUFFER_COUNT = 3   # entry次点のバッファペア数（6種類足取得対象）

# 運用除外セクター（V2_PAIRS定義は161ペアのまま保持、ここで運用時のみ除外）
# 陸運(9000-9099): 2024+ PF=0.87 の地雷。詳細は strategy_verification/chapters/76_pairs_exit/
EXCLUDE_SECTORS = [(9000, 9099)]

# 個別除外ペア (2020-2026 walk-forward PF<1.0 かつ 直近2年PF<0.9 の破綻組)
# 詳細: strategy_verification/chapters/11_pairs_v2_bt/backtest.py
EXCLUDE_PAIRS = {
    ("2768.T", "8020.T"),  # full_PF=0.87, recent_PF=0.64
    ("6103.T", "6305.T"),  # full_PF=0.70, recent_PF=0.80
    ("6103.T", "6473.T"),  # full_PF=0.52, recent_PF=0.30
    ("7167.T", "8411.T"),  # full_PF=0.77, recent_PF=0.56
    ("7272.T", "7313.T"),  # full_PF=0.82, recent_PF=0.73
    ("8031.T", "8058.T"),  # full_PF=0.84, recent_PF=0.35
    ("8306.T", "8316.T"),  # full_PF=0.72, recent_PF=0.37
    ("8338.T", "8399.T"),  # full_PF=0.88, recent_PF=0.77
    ("8354.T", "8381.T"),  # full_PF=0.95, recent_PF=0.89
    ("8360.T", "8386.T"),  # full_PF=0.89, recent_PF=0.77
    ("8386.T", "8392.T"),  # full_PF=0.90, recent_PF=0.66
    ("8411.T", "8524.T"),  # full_PF=0.56, recent_PF=0.54
}

# Phase 70-71 共和分ベースペア (tk1, tk2, optimal_lookback, actual_pf, actual_n, revert_1d)
# PF/N は 2020-01 〜 2026-04 walk-forward 実測値（In-Sample バイアス除去）
V2_PAIRS = [
    ("8336.T", "8399.T", 120, 1.50, 168, 68.3),
    ("8331.T", "8354.T", 250, 1.41, 182, 62.4),
    ("8331.T", "8551.T", 20, 1.51, 155, 78.7),
    ("7186.T", "8551.T", 10, 1.46, 90, 91.9),
    ("2053.T", "2060.T", 15, 2.29, 121, 90.1),
    ("4045.T", "7942.T", 15, 1.39, 117, 86.6),
    ("8345.T", "8399.T", 15, 1.82, 135, 87.1),
    ("3289.T", "8801.T", 10, 1.34, 93, 93.5),
    ("8367.T", "8551.T", 10, 1.58, 103, 91.5),
    ("8343.T", "8395.T", 15, 1.70, 117, 83.5),
    ("7167.T", "8411.T", 10, 0.77, 82, 89.0),
    ("8346.T", "8399.T", 10, 1.11, 82, 87.2),
    ("8012.T", "9934.T", 20, 1.60, 146, 87.2),
    ("7186.T", "7337.T", 30, 1.37, 128, 77.5),
    ("6479.T", "6981.T", 40, 1.43, 204, 77.3),
    ("4118.T", "4205.T", 10, 1.35, 102, 89.2),
    ("6407.T", "6490.T", 10, 1.61, 86, 92.4),
    ("4088.T", "4401.T", 30, 1.62, 155, 74.8),
    ("7267.T", "7270.T", 10, 1.70, 90, 91.3),
    ("4187.T", "4203.T", 250, 1.56, 146, 65.6),
    ("8341.T", "8359.T", 250, 1.46, 171, 61.6),
    ("8331.T", "8336.T", 10, 1.18, 99, 91.0),
    ("3050.T", "8218.T", 10, 1.53, 99, 95.3),
    ("6479.T", "6861.T", 40, 1.30, 166, 78.0),
    ("6470.T", "6471.T", 10, 1.94, 101, 95.3),
    ("4187.T", "4401.T", 120, 1.66, 132, 67.3),
    ("8604.T", "8707.T", 15, 1.32, 153, 81.2),
    ("7167.T", "7327.T", 15, 1.10, 144, 83.7),
    ("4183.T", "4208.T", 10, 2.56, 95, 90.7),
    ("3003.T", "3231.T", 10, 1.34, 91, 86.5),
    ("4045.T", "4956.T", 30, 1.13, 144, 81.6),
    ("3407.T", "4631.T", 250, 1.53, 123, 62.6),
    ("7182.T", "8336.T", 60, 1.34, 185, 68.7),
    ("6995.T", "7282.T", 30, 1.97, 190, 80.4),
    ("4095.T", "4401.T", 20, 1.42, 153, 83.9),
    ("2784.T", "7459.T", 10, 1.24, 95, 90.2),
    ("6113.T", "6301.T", 120, 1.44, 135, 72.8),
    ("5440.T", "5463.T", 20, 1.08, 155, 89.7),
    ("7189.T", "8341.T", 10, 1.51, 93, 94.3),
    ("4045.T", "4095.T", 60, 1.83, 153, 74.6),
    ("8306.T", "8411.T", 15, 1.20, 153, 81.8),
    ("9503.T", "9505.T", 40, 1.63, 157, 76.3),
    ("8308.T", "8551.T", 10, 1.30, 94, 96.0),
    ("7173.T", "7189.T", 120, 1.51, 149, 71.4),
    ("4118.T", "4401.T", 10, 1.56, 103, 91.0),
    ("8345.T", "8387.T", 15, 2.14, 126, 82.6),
    ("8343.T", "8370.T", 10, 1.38, 84, 90.7),
    ("6302.T", "6471.T", 15, 1.37, 131, 87.5),
    ("6954.T", "6981.T", 120, 1.49, 168, 68.9),
    ("8370.T", "8600.T", 15, 1.67, 111, 86.6),
    ("8359.T", "8411.T", 40, 1.46, 172, 74.6),
    ("8802.T", "8804.T", 15, 1.29, 126, 80.7),
    ("8015.T", "8058.T", 250, 1.60, 169, 60.2),
    ("8336.T", "8346.T", 250, 1.46, 149, 55.7),
    ("8343.T", "8551.T", 10, 1.28, 94, 89.2),
    ("6113.T", "6407.T", 10, 1.28, 100, 86.7),
    ("8346.T", "8387.T", 20, 1.29, 153, 84.1),
    ("8367.T", "8370.T", 10, 1.00, 94, 89.4),
    ("8392.T", "8544.T", 30, 1.22, 191, 72.5),
    ("8360.T", "8386.T", 10, 0.89, 95, 87.9),
    ("8346.T", "8395.T", 15, 1.02, 139, 85.0),
    ("7202.T", "7269.T", 250, 1.18, 174, 59.4),
    ("8386.T", "8392.T", 10, 0.90, 89, 92.3),
    ("8425.T", "8593.T", 60, 1.47, 174, 72.6),
    ("7182.T", "8331.T", 120, 1.46, 134, 61.2),
    ("6995.T", "7246.T", 250, 1.17, 157, 53.6),
    ("6856.T", "8035.T", 250, 1.33, 126, 62.7),
    ("8411.T", "8524.T", 10, 0.56, 95, 90.6),
    ("9101.T", "9104.T", 250, 1.05, 160, 55.7),
    ("2378.T", "4801.T", 10, 1.43, 97, 96.7),
    ("4063.T", "4187.T", 10, 1.15, 104, 92.5),
    ("6971.T", "6981.T", 10, 1.73, 95, 94.9),
    ("8359.T", "8714.T", 30, 1.38, 179, 78.9),
    ("4401.T", "4631.T", 10, 1.48, 110, 92.2),
    ("4042.T", "4401.T", 40, 1.37, 182, 72.5),
    ("8002.T", "8015.T", 40, 1.65, 185, 79.1),
    ("4095.T", "7942.T", 10, 1.25, 98, 89.8),
    ("8804.T", "8830.T", 10, 1.07, 90, 89.9),
    ("9005.T", "9021.T", 120, 1.95, 105, 64.6),  # excluded by sector filter (既存値)
    ("8015.T", "8053.T", 250, 1.05, 192, 60.2),
    ("3099.T", "8242.T", 30, 1.49, 169, 77.6),
    ("5844.T", "8381.T", 250, 1.38, 160, 61.0),
    ("8344.T", "8600.T", 40, 1.15, 199, 69.4),
    ("8804.T", "8850.T", 20, 1.50, 158, 79.2),
    ("7167.T", "8524.T", 60, 1.45, 166, 72.4),
    ("3050.T", "7516.T", 250, 1.06, 139, 60.6),
    ("7173.T", "8341.T", 120, 1.16, 193, 70.0),
    ("8306.T", "8316.T", 10, 0.72, 87, 89.3),
    ("7421.T", "8160.T", 120, 1.23, 140, 68.0),
    ("3407.T", "4401.T", 40, 1.33, 175, 78.3),
    ("6305.T", "6473.T", 30, 1.23, 172, 78.9),
    ("8802.T", "8830.T", 30, 1.25, 156, 75.6),
    ("8343.T", "8367.T", 10, 1.22, 94, 88.9),
    ("8354.T", "8381.T", 15, 0.95, 148, 82.3),
    ("8343.T", "8345.T", 10, 1.43, 87, 92.9),
    ("8074.T", "8078.T", 60, 1.25, 157, 71.1),
    ("4095.T", "7988.T", 10, 0.99, 93, 89.7),
    ("2768.T", "8053.T", 40, 1.36, 203, 75.3),
    ("5703.T", "5714.T", 10, 1.23, 88, 90.0),
    ("8766.T", "8795.T", 30, 1.54, 200, 80.2),
    ("7911.T", "7912.T", 10, 1.61, 106, 90.5),
    ("5844.T", "7337.T", 120, 1.86, 167, 66.0),
    ("8345.T", "8346.T", 15, 1.25, 120, 87.2),
    ("9042.T", "9045.T", 10, 1.76, 166, 88.8),  # excluded by sector filter (既存値)
    ("4042.T", "4045.T", 120, 1.19, 154, 62.2),
    ("7182.T", "8346.T", 60, 1.48, 160, 68.3),
    ("4095.T", "4631.T", 20, 1.13, 158, 86.0),
    ("8359.T", "8524.T", 10, 0.79, 90, 94.1),
    ("7182.T", "8387.T", 10, 1.02, 91, 91.5),
    ("7327.T", "8411.T", 60, 1.06, 179, 69.1),
    ("8439.T", "8591.T", 120, 1.67, 177, 66.9),
    ("7202.T", "7203.T", 40, 1.15, 179, 79.2),
    ("3923.T", "3994.T", 40, 1.14, 174, 73.9),
    ("5844.T", "8331.T", 20, 1.07, 156, 78.4),
    ("7246.T", "7282.T", 120, 1.16, 157, 68.3),
    ("8439.T", "8566.T", 10, 1.31, 82, 90.6),
    ("8338.T", "8399.T", 10, 0.88, 93, 87.5),
    ("5844.T", "8354.T", 250, 1.18, 104, 60.5),
    ("6103.T", "6305.T", 15, 0.70, 138, 90.8),
    ("8524.T", "8714.T", 15, 1.17, 126, 85.8),
    ("8001.T", "8020.T", 10, 0.89, 103, 93.1),
    ("8368.T", "8524.T", 10, 1.17, 100, 92.3),
    ("9001.T", "9048.T", 10, 1.58, 189, 87.3),  # excluded by sector filter (既存値)
    ("8346.T", "8600.T", 60, 1.56, 172, 73.7),
    ("8345.T", "8600.T", 10, 1.16, 97, 89.3),
    ("8386.T", "8544.T", 10, 1.12, 91, 90.1),
    ("8020.T", "8031.T", 120, 1.59, 177, 66.7),
    ("9508.T", "9509.T", 10, 1.33, 101, 94.4),
    ("8078.T", "8084.T", 10, 1.39, 95, 90.0),
    ("8020.T", "8053.T", 250, 0.95, 197, 64.6),
    ("7182.T", "8399.T", 60, 1.11, 211, 69.6),
    ("5844.T", "7186.T", 10, 1.04, 99, 93.2),
    ("8331.T", "8381.T", 10, 0.93, 116, 85.1),
    ("2768.T", "8020.T", 10, 0.87, 104, 87.9),
    ("6302.T", "6473.T", 20, 0.99, 157, 82.9),
    ("8334.T", "8522.T", 10, 0.93, 86, 87.1),
    ("9021.T", "9042.T", 20, 1.44, 173, 79.6),  # excluded by sector filter (既存値)
    ("8338.T", "8387.T", 20, 1.00, 147, 77.0),
    ("8386.T", "8524.T", 15, 0.94, 141, 86.3),
    ("7272.T", "7313.T", 10, 0.82, 104, 92.4),
    ("7267.T", "7272.T", 15, 1.14, 146, 89.1),
    ("9021.T", "9142.T", 10, 1.14, 110, 92.3),
    ("9007.T", "9048.T", 250, 1.37, 84, 58.8),  # excluded by sector filter (既存値)
    ("4911.T", "4922.T", 250, 1.64, 158, 56.9),
    ("7173.T", "7184.T", 40, 1.25, 161, 76.8),
    ("7182.T", "8354.T", 120, 1.16, 153, 59.3),
    ("7182.T", "8338.T", 20, 1.03, 152, 79.8),
    ("9008.T", "9048.T", 250, 1.27, 93, 58.9),  # excluded by sector filter (既存値)
    ("3994.T", "4443.T", 250, 0.87, 124, 66.3),
    ("7327.T", "8524.T", 40, 1.18, 197, 81.9),
    ("8020.T", "8058.T", 10, 1.31, 88, 90.4),
    ("6103.T", "6473.T", 10, 0.52, 91, 90.2),
    ("9005.T", "9042.T", 15, 1.21, 172, 80.6),  # excluded by sector filter (既存値)
    ("7184.T", "8341.T", 10, 1.39, 99, 85.7),
    ("8358.T", "8524.T", 120, 1.05, 191, 68.9),
    ("7181.T", "8750.T", 120, 0.97, 173, 67.5),
    ("9003.T", "9042.T", 10, 1.16, 154, 87.6),  # excluded by sector filter (既存値)
    ("9001.T", "9007.T", 10, 1.13, 183, 90.2),  # excluded by sector filter (既存値)
    ("8053.T", "8058.T", 10, 0.90, 95, 87.3),
    ("8031.T", "8058.T", 10, 0.84, 122, 86.3),
    ("9041.T", "9048.T", 60, 0.87, 167, 67.7),  # excluded by sector filter (既存値)
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
        "signal_date": common[-1],
        "ticker": tk1,
        "strategy": "pairs",
        "pair_id": f"{tk1}|{tk2}",
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
    excluded_sector = 0
    excluded_pair = 0
    for tk1, tk2, lookback, full_pf, full_n, revert_1d in V2_PAIRS:
        n1, n2 = int(tk1[:4]), int(tk2[:4])
        if any(lo <= n1 <= hi and lo <= n2 <= hi for lo, hi in EXCLUDE_SECTORS):
            excluded_sector += 1
            continue
        if (tk1, tk2) in EXCLUDE_PAIRS:
            excluded_pair += 1
            continue
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

    # signals.parquet に pairs 行をマージ（他 strategy を保持）
    if not df.empty:
        if SIGNALS_PATH.exists():
            existing = pd.read_parquet(SIGNALS_PATH)
            other = (existing[existing["strategy"] != "pairs"]
                     if "strategy" in existing.columns else existing)
            merged = pd.concat([df, other], ignore_index=True) if len(other) else df
        else:
            merged = df
        SIGNALS_PATH.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(SIGNALS_PATH, index=False)

    entry_count = int(df["is_entry"].sum()) if not df.empty else 0
    buffer_count = int(df["is_buffer"].sum()) if not df.empty and "is_buffer" in df.columns else 0
    print(f"\n  Computed: {len(df)}, Skipped: {skip_count}, Excluded(sector): {excluded_sector}, Excluded(pair): {excluded_pair}")
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

    # S3アップロード（signals.parquet を top-level に）
    print("\n[3/3] Uploading to S3...")
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket and SIGNALS_PATH.exists():
            upload_file(cfg, SIGNALS_PATH, "signals.parquet")
        else:
            print("  [INFO] S3 bucket not configured or signals.parquet missing")
    except Exception as e:
        print(f"  [WARN] S3 upload failed: {e}")

    print(f"\n{'=' * 60}")
    print(f"Date: {date_str}")
    print(f"Pairs: {len(df)}, Entry: {entry_count}, Buffer: {buffer_count}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
