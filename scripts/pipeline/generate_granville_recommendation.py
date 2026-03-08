#!/usr/bin/env python3
"""
generate_granville_recommendation.py
Granville推奨銘柄リスト生成

1. 当日シグナル（RSI14付き）を読み込み
2. B4 > B1 > B3 > B2 優先順位、同一ルール内はRSI14昇順（lowest first）
3. 証拠金計算: upper_limit(entry_price) × 100株
4. 集中制限15%チェック
5. 既存ポジション重複排除
6. 出力: recommendations_YYYY-MM-DD.parquet
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

load_dotenv_cascade()

GRANVILLE_DIR = PARQUET_DIR / "granville"
CSV_DIR = ROOT / "data" / "csv"
CREDIT_CSV = CSV_DIR / "credit_capacity.csv"
HOLD_CSV = CSV_DIR / "hold_stocks.csv"

CONCENTRATION_LIMIT = 0.15  # 1銘柄あたり最大15%
MAX_HOLD = {"B1": 7, "B2": 30, "B3": 5, "B4": 13}
RULE_PRIORITY = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}

_LIMIT_TABLE = [
    (100, 30), (200, 50), (500, 80), (700, 100),
    (1000, 150), (1500, 300), (2000, 400), (3000, 500),
    (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000),
    (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000),
    (100000, 15000), (150000, 30000), (200000, 40000), (300000, 50000),
    (500000, 70000), (700000, 100000), (1000000, 150000),
]


def _upper_limit(price: float) -> float:
    for threshold, limit in _LIMIT_TABLE:
        if price < threshold:
            return price + limit
    return price + 150000


def required_margin(entry_price: float) -> float:
    return _upper_limit(entry_price) * 100


def load_available_margin() -> float:
    """利用可能証拠金を読み込み"""
    if not CREDIT_CSV.exists():
        print(f"  [WARN] {CREDIT_CSV} not found, using default 3,000,000")
        return 3_000_000
    df = pd.read_csv(CREDIT_CSV)
    if df.empty:
        return 3_000_000
    return float(df.iloc[-1]["available_margin"])


def load_hold_stocks() -> set[str]:
    """保有銘柄のtickerセットを返す"""
    if not HOLD_CSV.exists():
        return set()
    df = pd.read_csv(HOLD_CSV)
    if df.empty:
        return set()
    return set(df["ticker"].tolist())


def main() -> int:
    print("=" * 60)
    print("Generate Granville Recommendations")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    # 最新シグナルファイルを特定
    signal_files = sorted(GRANVILLE_DIR.glob("signals_*.parquet"))
    if not signal_files:
        print("[ERROR] No signal files found")
        return 1

    signal_path = signal_files[-1]
    print(f"[1/3] Loading signals: {signal_path.name}")
    signals = pd.read_parquet(signal_path)

    if signals.empty:
        print("[INFO] No signals to recommend")
        return 0

    date_str = signal_path.stem.replace("signals_", "")

    # 証拠金と保有銘柄
    available = load_available_margin()
    hold_tickers = load_hold_stocks()
    print(f"  Available margin: ¥{available:,.0f}")
    print(f"  Existing positions: {len(hold_tickers)}")

    # 保有銘柄除外
    before = len(signals)
    signals = signals[~signals["ticker"].isin(hold_tickers)].copy()
    if before > len(signals):
        print(f"  Excluded {before - len(signals)} held stocks")

    if signals.empty:
        print("[INFO] No new recommendations (all held)")
        return 0

    # 証拠金計算
    print("\n[2/3] Computing margins...")
    signals["margin"] = signals["entry_price_est"].apply(required_margin)
    signals["concentration_pct"] = signals["margin"] / available * 100

    # ソート: ルール優先→RSI14昇順（lowest first）
    signals["_priority"] = signals["rule"].map(RULE_PRIORITY)
    if "rsi14" in signals.columns:
        signals = signals.sort_values(["_priority", "rsi14"], ascending=[True, True])
    else:
        signals = signals.sort_values(["_priority", "dev_from_sma20"])

    # 資金制約フィルタリング
    print("\n[3/3] Applying constraints...")
    recommended: list[dict] = []
    remaining = available

    for _, row in signals.iterrows():
        margin = float(row["margin"])

        # 集中制限チェック
        if margin / available > CONCENTRATION_LIMIT:
            continue

        # 残余証拠金チェック
        if margin > remaining:
            continue

        remaining -= margin
        rec = {
            "signal_date": row["signal_date"],
            "ticker": row["ticker"],
            "stock_name": row.get("stock_name", ""),
            "sector": row.get("sector", ""),
            "rule": row["rule"],
            "close": float(row["close"]),
            "entry_price_est": float(row["entry_price_est"]),
            "sma20": float(row["sma20"]),
            "dev_from_sma20": float(row["dev_from_sma20"]),
            "margin": int(margin),
            "concentration_pct": round(margin / available * 100, 1),
            "max_hold": MAX_HOLD[row["rule"]],
            "rsi14": round(float(row.get("rsi14", 0)), 2),
        }
        recommended.append(rec)

    result = pd.DataFrame(recommended)
    if result.empty:
        print("[INFO] No recommendations (all filtered by constraints)")
        return 0

    # 保存
    rec_path = GRANVILLE_DIR / f"recommendations_{date_str}.parquet"
    result.to_parquet(rec_path, index=False)
    print(f"\n[OK] {len(result)} recommendations → {rec_path.name}")

    total_margin = result["margin"].sum()
    print(f"  Total margin: ¥{total_margin:,.0f} / ¥{available:,.0f} "
          f"({total_margin / available * 100:.1f}%)")

    for rule in ["B4", "B1", "B3", "B2"]:
        rdf = result[result["rule"] == rule]
        if not rdf.empty:
            print(f"  {rule}: {len(rdf)} stocks, margin ¥{rdf['margin'].sum():,.0f}")

    print(f"\nTop recommendations:")
    for _, row in result.head(10).iterrows():
        rsi = f" RSI={row['rsi14']:.1f}" if "rsi14" in row else ""
        print(f"  [{row['rule']}] {row['ticker']} {row['stock_name']} "
              f"¥{row['entry_price_est']:,.0f} margin=¥{row['margin']:,.0f}{rsi}")

    # S3アップロード
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            upload_file(cfg, rec_path, f"granville/recommendations_{date_str}.parquet")
    except Exception as e:
        print(f"[WARN] S3 upload failed: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
