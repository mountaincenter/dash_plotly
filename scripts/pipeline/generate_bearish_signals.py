#!/usr/bin/env python3
"""
generate_bearish_signals.py
大陰線(-5%) 逆張りシグナル生成（Core30 + Large70 = 100銘柄）

確定パラメータ:
  シグナル: 日足実体 ≤ -5%（大陰線）
  VIフィルタ: 日経VI ≥ 20
  急騰フィルタ: SMA20/60/100の60日max上昇が15%/20%/30%未満
  株価フィルタ: ≤ 15,000円
  エントリー: 翌営業日寄付
  エグジット: SMA20回帰（終値 > SMA20）→ 翌営業日寄付
  損切り: Day3終値で -3%以下 → Day4寄付で決済
  MAX_HOLD: 30日
  対象: Core30 + Large70

出力:
  data/parquet/reversal/bearish_signals_YYYY-MM-DD.parquet
  data/parquet/reversal/bearish_positions_YYYY-MM-DD.parquet
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

REVERSAL_DIR = PARQUET_DIR / "reversal"
PRICES_PATH = PARQUET_DIR / "granville" / "prices_topix.parquet"  # 18:00 reviewで当日終値まで更新済み
VI_PATH = PARQUET_DIR / "nikkei_vi_max_1d.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"
META_FALLBACK = PARQUET_DIR / "meta.parquet"

# 統合 parquet (granville と共通、strategy="bearish" 行として merge)
SIGNALS_PATH = PARQUET_DIR / "signals.parquet"
POSITIONS_PATH = PARQUET_DIR / "positions.parquet"

# 確定パラメータ
BEARISH_THRESHOLD = -5.0      # 実体 ≤ -5%
VI_MIN = 20.0                 # 日経VI ≥ 20
PRICE_MAX = 15_000            # 株価 ≤ 15,000円
MAX_HOLD = 30
STOP_LOSS_DAY = 3             # Day3終値チェック
STOP_LOSS_PCT = -3.0          # -3%以下で損切り
# 急騰フィルタ閾値
SURGE_SMA20 = 15.0
SURGE_SMA60 = 20.0
SURGE_SMA100 = 30.0
# 対象区分（meta_jquantsのtopixnewindexseriesカラム）
TARGET_TIERS = {"TOPIX Core30", "TOPIX Large70"}


def load_core_large_tickers() -> set[str]:
    """meta_jquants.parquetからCore30+Large70の銘柄セットを返す"""
    for p in [META_PATH, META_FALLBACK]:
        if p.exists():
            m = pd.read_parquet(p)
            if "topixnewindexseries" in m.columns:
                tickers = set(m[m["topixnewindexseries"].isin(TARGET_TIERS)]["ticker"].astype(str))
                print(f"  Core+Large: {len(tickers)} tickers (from {p.name})")
                return tickers
    print("  [WARN] meta not found, no ticker filter applied")
    return set()


def load_vi() -> pd.DataFrame:
    """日経VIデータを読み込み"""
    if VI_PATH.exists():
        vi = pd.read_parquet(VI_PATH)
        vi["date"] = pd.to_datetime(vi["date"])
        vi = vi.sort_values("date")
        return vi[["date", "close"]].rename(columns={"close": "vi_close"})
    return pd.DataFrame(columns=["date", "vi_close"])


def load_meta() -> pd.DataFrame:
    for p in [META_PATH, META_FALLBACK]:
        if p.exists():
            m = pd.read_parquet(p)
            if "sectors" in m.columns:
                m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)
            return m
    return pd.DataFrame(columns=["ticker", "stock_name", "sectors"])


def load_prices(core_large: set[str]) -> pd.DataFrame:
    """価格データ読み込み+テクニカル指標計算"""
    print("[1/4] Loading prices...")
    ps = pd.read_parquet(PRICES_PATH)
    ps["date"] = pd.to_datetime(ps["date"])

    # Core+Largeのみ抽出
    if core_large:
        ps = ps[ps["ticker"].isin(core_large)].copy()
    print(f"  {len(ps):,} rows, {ps['ticker'].nunique()} tickers")

    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)
    g = ps.groupby("ticker")

    # SMA
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ps["sma60"] = g["Close"].transform(lambda x: x.rolling(60, min_periods=60).mean())
    ps["sma100"] = g["Close"].transform(lambda x: x.rolling(100, min_periods=100).mean())

    # 乖離率
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["dev60"] = (ps["Close"] - ps["sma60"]) / ps["sma60"] * 100
    ps["dev100"] = (ps["Close"] - ps["sma100"]) / ps["sma100"] * 100

    # 急騰フィルタ用: 60日ローリングmax上方乖離
    ps["max_up20"] = g["dev_from_sma20"].transform(lambda x: x.rolling(60, min_periods=1).max())
    ps["max_up60"] = g["dev60"].transform(lambda x: x.rolling(60, min_periods=1).max())
    ps["max_up100"] = g["dev100"].transform(lambda x: x.rolling(60, min_periods=1).max())

    # 大陰線判定用: 実体変化率
    ps["body_pct"] = (ps["Close"] - ps["Open"]) / ps["Open"] * 100

    ps["prev_close"] = g["Close"].shift(1)

    ps = ps.dropna(subset=["sma20"])
    return ps


def detect_bearish_signals(ps: pd.DataFrame, vi_df: pd.DataFrame) -> pd.DataFrame:
    """大陰線シグナル検出"""
    latest_date = ps["date"].max()
    print(f"\n[2/4] Bearish signal detection for {latest_date.date()}...")

    latest = ps[ps["date"] == latest_date].copy()

    # VI取得
    vi_today = vi_df[vi_df["date"] <= latest_date].tail(1)
    if vi_today.empty:
        print("  [WARN] No VI data available — skipping all signals")
        return pd.DataFrame()
    vi_val = float(vi_today.iloc[0]["vi_close"])
    print(f"  VI: {vi_val:.1f}")

    if vi_val < VI_MIN:
        print(f"  VI < {VI_MIN} → no signals (bearish in calm market = bad news)")
        return pd.DataFrame()

    # 大陰線フィルタ: 実体 ≤ -5%
    bearish = latest[latest["body_pct"] <= BEARISH_THRESHOLD].copy()
    print(f"  Bearish candles (body ≤ {BEARISH_THRESHOLD}%): {len(bearish)}")

    if bearish.empty:
        return pd.DataFrame()

    # 株価フィルタ
    before = len(bearish)
    bearish = bearish[bearish["Close"] <= PRICE_MAX]
    print(f"  Price filter (≤ ¥{PRICE_MAX:,}): {before} → {len(bearish)}")

    if bearish.empty:
        return pd.DataFrame()

    # 急騰フィルタ
    before = len(bearish)
    surge_mask = (
        (bearish["max_up20"] >= SURGE_SMA20) |
        (bearish["max_up60"] >= SURGE_SMA60) |
        (bearish["max_up100"] >= SURGE_SMA100)
    )
    bearish = bearish[~surge_mask]
    print(f"  Surge filter: {before} → {len(bearish)} ({before - len(bearish)} excluded)")

    if bearish.empty:
        return pd.DataFrame()

    # メタデータ結合
    meta = load_meta()
    if not meta.empty:
        bearish = bearish.merge(
            meta[["ticker", "stock_name", "sectors"]].drop_duplicates(subset="ticker"),
            on="ticker", how="left",
        )
    else:
        bearish["stock_name"] = ""
        bearish["sectors"] = ""

    out = pd.DataFrame({
        "signal_date": bearish["date"],
        "ticker": bearish["ticker"],
        "stock_name": bearish.get("stock_name", pd.Series("", index=bearish.index)),
        "sector": bearish.get("sectors", pd.Series("", index=bearish.index)),
        "strategy": "bearish",
        "direction": "long",
        "pair_id": "",
        "close": bearish["Close"].round(1),
        "open": bearish["Open"].round(1),
        "entry_price_est": bearish["Close"].round(1),  # 翌寄付想定だが算出時点は close
        "prev_close": bearish["prev_close"].round(1),
        "sma20": bearish["sma20"].round(2),
        "dev_from_sma20": bearish["dev_from_sma20"].round(3),
        "body_pct": bearish["body_pct"].round(2),
        "vi": vi_val,
        "max_hold": MAX_HOLD,
    })

    # 下落率が深い順
    out = out.sort_values("body_pct", ascending=True).reset_index(drop=True)
    return out


def generate_positions(ps: pd.DataFrame, vi_df: pd.DataFrame, latest_date: pd.Timestamp) -> pd.DataFrame:
    """保有中ポジションの計算（SMA20回帰exit / Day3損切り / MAX_HOLD）"""
    print("\n[3/4] Generating positions...")

    cutoff = latest_date - pd.Timedelta(days=MAX_HOLD * 2)
    recent = ps[ps["date"] >= cutoff].copy()

    # 過去の大陰線シグナルを再検出
    vi_merged = recent.merge(vi_df, on="date", how="left")
    vi_merged["vi_close"] = vi_merged["vi_close"].ffill()

    sig_mask = (
        (vi_merged["body_pct"] <= BEARISH_THRESHOLD) &
        (vi_merged["vi_close"] >= VI_MIN) &
        (vi_merged["Close"] <= PRICE_MAX) &
        (vi_merged["max_up20"] < SURGE_SMA20) &
        (vi_merged["max_up60"] < SURGE_SMA60) &
        (vi_merged["max_up100"] < SURGE_SMA100)
    )
    sigs = vi_merged[sig_mask].copy()

    if sigs.empty:
        print("  No recent bearish signals")
        return pd.DataFrame()

    meta = load_meta()
    if not meta.empty:
        sigs = sigs.merge(
            meta[["ticker", "stock_name"]].drop_duplicates(subset="ticker"),
            on="ticker", how="left",
        )
        sigs["stock_name"] = sigs["stock_name"].fillna("")
    else:
        sigs["stock_name"] = ""

    ticker_groups = {tk: gdf.sort_values("date").reset_index(drop=True)
                     for tk, gdf in ps.groupby("ticker")}

    rows: list[dict] = []
    for _, sig in sigs.iterrows():
        tk_all = ticker_groups.get(sig["ticker"])
        if tk_all is None:
            continue

        entry_mask = tk_all["date"] > sig["date"]
        if not entry_mask.any():
            continue
        entry_iloc = entry_mask.idxmax()

        ep = float(tk_all.iloc[entry_iloc]["Open"])
        if pd.isna(ep) or ep <= 0:
            continue
        e_date = tk_all.iloc[entry_iloc]["date"]

        exited = False
        exit_type = ""
        exit_price = 0.0
        hold_end = min(entry_iloc + MAX_HOLD, len(tk_all))

        for day in range(entry_iloc, hold_end):
            row = tk_all.iloc[day]
            hold_day = day - entry_iloc

            # Day3損切り: Day3終値で -3%以下
            if hold_day == STOP_LOSS_DAY - 1:
                day_ret = (float(row["Close"]) / ep - 1) * 100
                if day_ret <= STOP_LOSS_PCT:
                    exit_price = float(row["Close"])
                    if row["date"] >= latest_date:
                        exit_type = "stop_loss"
                    exited = True
                    break

            # SMA20回帰: 終値 > SMA20
            if hold_day > 0 and not pd.isna(row.get("sma20", float("nan"))):
                if float(row["Close"]) > float(row["sma20"]):
                    exit_price = float(row["Close"])
                    if row["date"] == latest_date:
                        exit_type = "sma20_return"
                    exited = True
                    break

            # MAX_HOLD
            if hold_day >= MAX_HOLD - 1:
                exit_price = float(row["Close"])
                if row["date"] == latest_date:
                    exit_type = "max_hold"
                exited = True
                break

        cur_day = min(hold_end - 1, len(tk_all) - 1)
        cur = tk_all.iloc[cur_day]
        cp = float(cur["Close"])
        hold_days = cur_day - entry_iloc + 1

        base = {
            "ticker": sig["ticker"],
            "stock_name": sig.get("stock_name", ""),
            "strategy": "bearish",
            "direction": "long",
            "pair_id": "",
            "entry_date": e_date,
            "entry_price": round(ep, 1),
            "current_price": round(cp, 1),
            "sma20": round(float(cur.get("sma20", 0)), 1) if not pd.isna(cur.get("sma20", float("nan"))) else 0.0,
            "pct": round((cp / ep - 1) * 100, 2),
            "pnl": int((cp - ep) * 100),
            "hold_days": hold_days,
            "max_hold": MAX_HOLD,
            "as_of": latest_date,
        }

        if not exited:
            rows.append({**base, "status": "open", "exit_type": ""})
        elif exit_type:
            rows.append({**base, "status": "exit", "exit_type": exit_type})

    result = pd.DataFrame(rows)
    if not result.empty:
        open_n = (result["status"] == "open").sum()
        exit_n = (result["status"] == "exit").sum()
        print(f"  {open_n} open, {exit_n} exit candidates")
    else:
        print("  No active positions")

    return result


def main() -> int:
    print("=" * 60)
    print("Generate Bearish Reversal Signals")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    if not PRICES_PATH.exists():
        print(f"[ERROR] Price data not found: {PRICES_PATH}")
        return 1

    REVERSAL_DIR.mkdir(parents=True, exist_ok=True)

    core_large = load_core_large_tickers()

    ps = load_prices(core_large)
    vi_df = load_vi()
    out = detect_bearish_signals(ps, vi_df)

    if out.empty:
        print("\n[INFO] No bearish signals today")
    else:
        print(f"\n  {len(out)} bearish signals")
        for _, row in out.iterrows():
            print(f"    {row['ticker']} {row['stock_name']} "
                  f"¥{row['close']:,.0f} ({row['body_pct']:+.1f}%)")

    latest_date = ps["date"].max()
    date_str = latest_date.strftime("%Y-%m-%d")

    if out.empty:
        out = pd.DataFrame(columns=[
            "signal_date", "ticker", "stock_name", "sector",
            "strategy", "direction", "pair_id",
            "close", "open", "entry_price_est", "prev_close",
            "sma20", "dev_from_sma20", "body_pct", "vi", "max_hold",
        ])

    # ポジション計算
    positions = generate_positions(ps, vi_df, latest_date)
    if positions.empty:
        positions = pd.DataFrame(columns=[
            "ticker", "stock_name", "strategy", "direction", "pair_id",
            "entry_date", "entry_price", "current_price", "sma20",
            "pct", "pnl", "hold_days", "max_hold", "as_of",
            "status", "exit_type",
        ])

    # ===== 統合 signals.parquet に merge (strategy="bearish" 行のみ差し替え) =====
    if SIGNALS_PATH.exists():
        existing_sigs = pd.read_parquet(SIGNALS_PATH)
        other = (existing_sigs[existing_sigs["strategy"] != "bearish"]
                 if "strategy" in existing_sigs.columns else existing_sigs)
        merged_sigs = pd.concat([out, other], ignore_index=True) if len(other) else out
    else:
        merged_sigs = out
    merged_sigs.to_parquet(SIGNALS_PATH, index=False)
    print(f"\n[OK] bearish signals merged into {SIGNALS_PATH.name}")
    print(f"     bearish={len(out)} / total={len(merged_sigs)}")

    # ===== 統合 positions.parquet に merge (strategy="bearish" 行のみ差し替え) =====
    if POSITIONS_PATH.exists():
        existing_pos = pd.read_parquet(POSITIONS_PATH)
        other_pos = (existing_pos[existing_pos["strategy"] != "bearish"]
                     if "strategy" in existing_pos.columns else existing_pos)
        merged_pos = pd.concat([positions, other_pos], ignore_index=True) if len(other_pos) else positions
    else:
        merged_pos = positions
    merged_pos.to_parquet(POSITIONS_PATH, index=False)
    b_open = int((positions["status"] == "open").sum()) if not positions.empty else 0
    b_exit = int((positions["status"] == "exit").sum()) if not positions.empty else 0
    print(f"[OK] bearish positions merged into {POSITIONS_PATH.name}")
    print(f"     bearish={len(positions)} (open={b_open}, exit={b_exit}) / total={len(merged_pos)}")

    # S3アップロード
    print("\n[4/4] Uploading to S3...")
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            upload_file(cfg, SIGNALS_PATH, "signals.parquet")
            upload_file(cfg, POSITIONS_PATH, "positions.parquet")
        else:
            print("  [INFO] S3 bucket not configured")
    except Exception as e:
        print(f"  [WARN] S3 upload failed: {e}")

    print(f"\n{'=' * 60}")
    print(f"Date: {date_str}")
    print(f"Bearish signals: {len(out)}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
