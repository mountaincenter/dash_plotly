#!/usr/bin/env python3
"""
generate_pairs_signals.py
ペアトレード シグナル生成（夜間バッチ）

TOP_PAIRS 30ペアに対し:
  - 直近60日の価格から z-score 計算
  - 翌朝エントリー閾値（tk1の価格）を算出
  - 直近バックテスト成績を計算

出力:
  data/parquet/pairs/pairs_signals_YYYY-MM-DD.parquet
"""
from __future__ import annotations

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
# 価格ソース: prices_topix（review phaseで毎日更新、TOPIX全1660銘柄）
PRICES_TOPIX = PARQUET_DIR / "granville" / "prices_topix.parquet"
# フォールバック: screening → prices_max_1d
PRICES_1D_CL = PARQUET_DIR / "screening" / "prices_max_1d_core_large.parquet"
PRICES_1D_MID = PARQUET_DIR / "screening" / "prices_max_1d_mid400.parquet"
PRICES_FALLBACK = PARQUET_DIR / "prices_max_1d.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"

LOOKBACK = 20
Z_ENTRY = 2.0
CAPITAL = 2_000_000

# 67_pairs_full_scan.py で PF≥1.5 の上位30ペア (tk1, tk2, 5年通期PF, 5年n数)
TOP_PAIRS = [
    ("1721.T", "1951.T", 2.92, 221),
    ("1942.T", "1944.T", 2.65, 188),
    ("3132.T", "5471.T", 2.44, 155),
    ("1721.T", "6592.T", 2.37, 171),
    ("1721.T", "3003.T", 2.34, 170),
    ("1721.T", "9364.T", 2.24, 172),
    ("5929.T", "7741.T", 2.22, 179),
    ("5110.T", "5471.T", 2.15, 151),
    ("6479.T", "6806.T", 2.14, 166),
    ("5929.T", "6592.T", 2.12, 176),
    ("5844.T", "6503.T", 2.11, 155),
    ("1721.T", "6845.T", 2.11, 163),
    ("2810.T", "9735.T", 2.11, 168),
    ("4194.T", "4543.T", 2.09, 149),
    ("1951.T", "9364.T", 2.09, 183),
    ("1812.T", "8309.T", 2.08, 155),
    ("3941.T", "5471.T", 2.08, 142),
    ("6113.T", "7276.T", 2.06, 158),
    ("2802.T", "6806.T", 2.06, 155),
    ("1803.T", "8410.T", 2.06, 169),
    ("4183.T", "6728.T", 2.06, 163),
    ("7186.T", "8331.T", 2.05, 199),
    ("3360.T", "8113.T", 2.03, 172),
    ("2502.T", "2587.T", 2.03, 182),
    ("3861.T", "4502.T", 2.02, 144),
    ("7337.T", "8309.T", 2.01, 169),
    ("5108.T", "5201.T", 2.00, 163),
    ("1803.T", "1808.T", 1.99, 169),
    ("3407.T", "8830.T", 1.99, 133),
    ("4088.T", "6971.T", 1.98, 188),
]


def load_names() -> dict[str, str]:
    if META_PATH.exists():
        meta = pd.read_parquet(META_PATH)
        return dict(zip(meta["ticker"], meta["stock_name"]))
    return {}


def load_prices(days: int = 60) -> pd.DataFrame:
    """価格データ読み込み（prices_topix優先、なければscreening→prices_max_1d）"""
    frames = []

    # 優先: prices_topix（review phaseで毎日更新、TOPIX全1660銘柄）
    if PRICES_TOPIX.exists():
        frames.append(pd.read_parquet(PRICES_TOPIX))
        print(f"  [prices] Using prices_topix ({PRICES_TOPIX.name})")

    # フォールバック: screening → prices_max_1d
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


def calc_pair_signal(ps: pd.DataFrame, tk1: str, tk2: str) -> dict | None:
    """1ペアのz-score・閾値・直近成績を計算"""
    d1 = ps[ps["ticker"] == tk1].set_index("date").sort_index()
    d2 = ps[ps["ticker"] == tk2].set_index("date").sort_index()
    common = d1.index.intersection(d2.index)
    if len(common) < LOOKBACK + 5:
        return None
    d1 = d1.loc[common]
    d2 = d2.loc[common]

    spread_close = np.log(d1["Close"].values / d2["Close"].values)

    mu = spread_close[-LOOKBACK:].mean()
    sigma = spread_close[-LOOKBACK:].std()
    if sigma < 1e-8:
        return None

    latest_spread = spread_close[-1]
    z_latest = (latest_spread - mu) / sigma

    spread_entry_long = mu + Z_ENTRY * sigma
    spread_entry_short = mu - Z_ENTRY * sigma

    c1_last = float(d1["Close"].iloc[-1])
    c2_last = float(d2["Close"].iloc[-1])

    ratio_upper = np.exp(spread_entry_long)
    ratio_lower = np.exp(spread_entry_short)

    tk1_upper = c2_last * ratio_upper
    tk1_lower = c2_last * ratio_lower

    # 直近バックテスト
    half = CAPITAL / 2
    pnls = []
    for i in range(LOOKBACK, len(common)):
        w = spread_close[i - LOOKBACK:i]
        m = w.mean()
        s = w.std()
        if s < 1e-8:
            continue
        sp_open = np.log(d1["Open"].iloc[i] / d2["Open"].iloc[i])
        z = (sp_open - m) / s
        if abs(z) < Z_ENTRY:
            continue
        o1 = d1["Open"].iloc[i]
        c1 = d1["Close"].iloc[i]
        o2 = d2["Open"].iloc[i]
        c2 = d2["Close"].iloc[i]
        if z > Z_ENTRY:
            pnl = half * (o1 - c1) / o1 + half * (c2 - o2) / o2
        else:
            pnl = half * (c1 - o1) / o1 + half * (o2 - c2) / o2
        pnls.append(pnl)

    if len(pnls) >= 3:
        pa = np.array(pnls)
        gw = pa[pa > 0].sum()
        gl = abs(pa[pa < 0].sum())
        recent_pf = round(gw / gl, 2) if gl > 0 else 999.0
        recent_wr = round(float((pa > 0).mean()) * 100, 1)
        recent_n = len(pa)
    else:
        recent_pf = 0.0
        recent_wr = 0.0
        recent_n = len(pnls)

    return {
        "tk1": tk1,
        "tk2": tk2,
        "c1": round(c1_last, 1),
        "c2": round(c2_last, 1),
        "z_latest": round(z_latest, 3),
        "mu": round(mu, 6),
        "sigma": round(sigma, 6),
        "tk1_upper": round(tk1_upper, 1),
        "tk1_lower": round(tk1_lower, 1),
        "recent_n": recent_n,
        "recent_wr": recent_wr,
        "recent_pf": recent_pf,
        "is_hot": abs(z_latest) > 1.5,
        "direction": "short_tk1" if z_latest > 0 else "long_tk1",
        "signal_date": common[-1],
    }


def main() -> int:
    print("=" * 60)
    print("Generate Pairs Trading Signals")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    PAIRS_DIR.mkdir(parents=True, exist_ok=True)

    print("\n[1/3] Loading prices...")
    ps = load_prices(days=60)
    if ps.empty:
        print("[ERROR] No price data available")
        return 1
    print(f"  {len(ps):,} rows, {ps['ticker'].nunique()} tickers")

    names = load_names()

    print("\n[2/3] Calculating pair signals...")
    rows: list[dict] = []
    for tk1, tk2, full_pf, full_n in TOP_PAIRS:
        r = calc_pair_signal(ps, tk1, tk2)
        if r:
            r["name1"] = names.get(tk1, tk1)
            r["name2"] = names.get(tk2, tk2)
            r["full_pf"] = full_pf
            r["full_n"] = full_n
            rows.append(r)
            flag = " ★" if r["is_hot"] else ""
            print(f"  {r['name1']}/{r['name2']}  z={r['z_latest']:+.2f}{flag}")
        else:
            print(f"  {tk1}/{tk2}  SKIP (data insufficient)")

    latest_date = ps["date"].max()
    date_str = latest_date.strftime("%Y-%m-%d")

    if rows:
        df = pd.DataFrame(rows)
        # z絶対値降順
        df = df.sort_values("z_latest", key=lambda x: x.abs(), ascending=False)
    else:
        df = pd.DataFrame(columns=[
            "tk1", "tk2", "name1", "name2", "c1", "c2",
            "z_latest", "mu", "sigma", "tk1_upper", "tk1_lower",
            "recent_n", "recent_wr", "recent_pf", "is_hot",
            "direction", "full_pf", "full_n", "signal_date",
        ])

    signal_path = PAIRS_DIR / f"pairs_signals_{date_str}.parquet"
    df.to_parquet(signal_path, index=False)
    hot_count = int(df["is_hot"].sum()) if not df.empty else 0
    print(f"\n[OK] Saved: {signal_path.name} ({len(df)} pairs, {hot_count} hot)")

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
    print(f"Pairs: {len(df)}, Hot (|z|>1.5): {hot_count}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
