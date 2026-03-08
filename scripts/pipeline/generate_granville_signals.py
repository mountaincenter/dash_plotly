#!/usr/bin/env python3
"""
generate_granville_signals.py
グランビル B1-B4 シグナル生成（TOPIX 1,660銘柄）

新体系:
  B1: 前日Close < SMA20, 当日Close > SMA20, SMA20上昇
  B2: SMA20上昇, 乖離 [-5, 0), Close < SMA20, 陽線
  B3: SMA20上昇, Close > SMA20, 乖離 [0, 3], 乖離縮小, 陽線
  B4: 乖離 < -8%, 陽線

出口:
  20日高値: High >= rolling(20).max()
  MAX_HOLD: B1=7日, B2=30日, B3=5日, B4=13日
  SLなし

出力:
  data/parquet/granville/signals_YYYY-MM-DD.parquet
  (S3: s3://<bucket>/parquet/granville/)
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

GRANVILLE_DIR = PARQUET_DIR / "granville"
PRICES_PATH = GRANVILLE_DIR / "prices_topix.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"
META_FALLBACK = PARQUET_DIR / "meta.parquet"

MAX_HOLD = {"B1": 7, "B2": 30, "B3": 5, "B4": 13}
RULE_PRIORITY = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}


def load_prices() -> pd.DataFrame:
    """TOPIX価格データを読み込み、テクニカル指標を計算"""
    print("[1/4] Loading prices...")
    ps = pd.read_parquet(PRICES_PATH)
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)
    print(f"  {len(ps):,} rows, {ps['ticker'].nunique()} tickers")

    # テクニカル指標
    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["prev_close"] = g["Close"].shift(1)
    ps["prev_sma20"] = g["sma20"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)

    # RSI 14（同一ルール内ソートに使用）
    delta = g["Close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.groupby(ps["ticker"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    avg_loss = loss.groupby(ps["ticker"]).transform(lambda x: x.rolling(14, min_periods=14).mean())
    rs = avg_gain / avg_loss.replace(0, np.nan)
    ps["rsi14"] = 100 - (100 / (1 + rs))

    # 派生フラグ
    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["above"] = ps["Close"] > ps["sma20"]
    ps["below"] = ps["Close"] < ps["sma20"]
    ps["prev_below"] = ps["prev_close"] < ps["prev_sma20"]
    ps["up_day"] = ps["Close"] > ps["prev_close"]

    ps = ps.dropna(subset=["sma20"])

    return ps


def load_meta() -> pd.DataFrame:
    """銘柄メタデータ読み込み"""
    if META_PATH.exists():
        m = pd.read_parquet(META_PATH)
    elif META_FALLBACK.exists():
        m = pd.read_parquet(META_FALLBACK)
    else:
        return pd.DataFrame(columns=["ticker", "stock_name", "sectors"])

    if "sectors" in m.columns:
        m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)
    return m


def detect_signals(df: pd.DataFrame) -> pd.DataFrame:
    """B1-B4 シグナルを検出"""
    df = df.copy()
    dev = df["dev_from_sma20"]
    sma_up = df["sma20_up"]

    # B1: ゴールデンクロス
    df["B1"] = df["prev_below"] & df["above"] & sma_up

    # B2: 押し目買い
    df["B2"] = sma_up & dev.between(-5, 0) & df["up_day"] & df["below"]

    # B3: SMA支持
    df["B3"] = sma_up & df["above"] & dev.between(0, 3) & (df["prev_dev"] > dev) & df["up_day"]

    # B4: 売られすぎ反発
    df["B4"] = (dev < -8) & df["up_day"]

    return df


def generate_signals() -> pd.DataFrame:
    """最新日のシグナルを生成"""
    ps = load_prices()
    ps = detect_signals(ps)
    meta = load_meta()

    latest_date = ps["date"].max()
    print(f"\n[2/4] Signal detection for {latest_date.date()}...")

    latest = ps[ps["date"] == latest_date].copy()

    rules = ["B1", "B2", "B3", "B4"]
    for r in rules:
        print(f"  {r}: {latest[r].sum()} signals")

    sig_mask = latest["B1"] | latest["B2"] | latest["B3"] | latest["B4"]
    signals = latest[sig_mask].copy()
    print(f"  Total raw: {len(signals)}")

    if signals.empty:
        return pd.DataFrame()

    # ルール割り当て（優先順位: B4 > B1 > B3 > B2）
    def assign_rule(row: pd.Series) -> str:
        if row["B4"]:
            return "B4"
        if row["B1"]:
            return "B1"
        if row["B3"]:
            return "B3"
        return "B2"

    signals["rule"] = signals.apply(assign_rule, axis=1)

    # メタ結合
    if not meta.empty:
        signals = signals.merge(
            meta[["ticker", "stock_name", "sectors"]].drop_duplicates(subset="ticker"),
            on="ticker", how="left",
        )
    else:
        signals["stock_name"] = ""
        signals["sectors"] = ""

    # 出力スキーマ
    out = pd.DataFrame({
        "signal_date": signals["date"],
        "ticker": signals["ticker"],
        "stock_name": signals.get("stock_name", pd.Series("", index=signals.index)),
        "sector": signals.get("sectors", pd.Series("", index=signals.index)),
        "rule": signals["rule"],
        "close": signals["Close"].round(1),
        "open": signals["Open"].round(1),
        "sma20": signals["sma20"].round(2),
        "dev_from_sma20": signals["dev_from_sma20"].round(3),
        "sma20_slope": signals["sma20_slope"].round(4),
        "entry_price_est": signals["Close"].round(1),
        "rsi14": signals["rsi14"].round(2),
    })

    # ルール優先順位 → 同一ルール内RSI14昇順（lowest first）
    out["_priority"] = out["rule"].map(RULE_PRIORITY)
    out = out.sort_values(["_priority", "rsi14"], ascending=[True, True]).drop(columns=["_priority"]).reset_index(drop=True)

    return out


def generate_positions(ps: pd.DataFrame, latest_date: pd.Timestamp) -> pd.DataFrame:
    """過去シグナルから保有ポジション + Exit候補を計算"""
    print("\n[3/4] Generating positions...")

    # 過去60日のシグナルを検出
    max_lookback = max(MAX_HOLD.values())
    cutoff = latest_date - pd.Timedelta(days=max_lookback * 2)
    recent = ps[ps["date"] >= cutoff].copy()
    recent = detect_signals(recent)

    sig_mask = recent["B1"] | recent["B2"] | recent["B3"] | recent["B4"]
    sigs = recent[sig_mask].copy()

    if sigs.empty:
        print("  No recent signals")
        return pd.DataFrame()

    def assign_rule(row: pd.Series) -> str:
        if row["B4"]:
            return "B4"
        if row["B1"]:
            return "B1"
        if row["B3"]:
            return "B3"
        return "B2"

    sigs["rule"] = sigs.apply(assign_rule, axis=1)

    rows: list[dict] = []
    for _, sig in sigs.iterrows():
        rule = sig["rule"]
        max_hold = MAX_HOLD[rule]
        tk = ps[(ps["ticker"] == sig["ticker"]) & (ps["date"] > sig["date"])].sort_values("date")

        if tk.empty:
            continue

        ep = float(tk.iloc[0]["Open"])
        if pd.isna(ep) or ep <= 0:
            continue
        e_date = tk.iloc[0]["date"]

        exited = False
        exit_type = ""

        for i in range(min(len(tk), max_hold)):
            row = tk.iloc[i]

            # 20日高値Exit
            if i > 0:
                past_highs = tk.iloc[max(0, i - 19):i]["High"]
                if float(row["High"]) >= float(past_highs.max()):
                    if row["date"] == latest_date:
                        exit_type = "20d_high"
                    exited = True
                    break

            # MAX_HOLD
            if i >= max_hold - 1:
                if row["date"] == latest_date:
                    exit_type = "max_hold"
                exited = True
                break

        cur = tk.iloc[min(len(tk) - 1, max_hold - 1)]
        cp = float(cur["Close"])
        hold_days = min(len(tk), max_hold)

        if not exited:
            rows.append({
                "status": "open",
                "ticker": sig["ticker"],
                "rule": rule,
                "entry_date": e_date,
                "entry_price": round(ep, 1),
                "current_price": round(cp, 1),
                "pct": round((cp / ep - 1) * 100, 2),
                "pnl": int((cp - ep) * 100),
                "hold_days": hold_days,
                "max_hold": max_hold,
                "exit_type": "",
                "as_of": latest_date,
            })
        elif exit_type:
            rows.append({
                "status": "exit",
                "ticker": sig["ticker"],
                "rule": rule,
                "entry_date": e_date,
                "entry_price": round(ep, 1),
                "current_price": round(cp, 1),
                "pct": round((cp / ep - 1) * 100, 2),
                "pnl": int((cp - ep) * 100),
                "hold_days": hold_days,
                "max_hold": max_hold,
                "exit_type": exit_type,
                "as_of": latest_date,
            })

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
    print("Generate Granville Signals (B1-B4, TOPIX 1,660)")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    if not PRICES_PATH.exists():
        print(f"[ERROR] Price data not found: {PRICES_PATH}")
        print("  Run update_granville_topix_prices.py first")
        return 1

    GRANVILLE_DIR.mkdir(parents=True, exist_ok=True)

    # シグナル生成
    out = generate_signals()

    if out.empty:
        print("\n[INFO] No signals today")
    else:
        print(f"\n  {len(out)} signals generated")
        for _, row in out.iterrows():
            print(f"    [{row['rule']}] {row['ticker']} {row['stock_name']} "
                  f"¥{row['close']:,.0f} ({row['dev_from_sma20']:+.1f}%)")

    # 日付別ファイル保存
    ps = load_prices()
    ps = detect_signals(ps)
    latest_date = ps["date"].max()
    date_str = latest_date.strftime("%Y-%m-%d")

    signal_path = GRANVILLE_DIR / f"signals_{date_str}.parquet"
    if out.empty:
        out = pd.DataFrame(columns=[
            "signal_date", "ticker", "stock_name", "sector", "rule",
            "close", "open", "sma20", "dev_from_sma20", "sma20_slope",
            "entry_price_est", "rsi14",
        ])
    out.to_parquet(signal_path, index=False)
    print(f"\n[OK] Saved: {signal_path.name} ({len(out)} rows)")

    # ポジション計算
    positions = generate_positions(ps, latest_date)
    if not positions.empty:
        pos_path = GRANVILLE_DIR / f"positions_{date_str}.parquet"
        positions.to_parquet(pos_path, index=False)
        print(f"[OK] Saved: {pos_path.name} ({len(positions)} rows)")

    # S3アップロード
    print("\n[4/4] Uploading to S3...")
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            upload_file(cfg, signal_path, f"granville/signals_{date_str}.parquet")
            if not positions.empty:
                upload_file(cfg, pos_path, f"granville/positions_{date_str}.parquet")
        else:
            print("  [INFO] S3 bucket not configured")
    except Exception as e:
        print(f"  [WARN] S3 upload failed: {e}")

    print(f"\n{'=' * 60}")
    print(f"Date: {date_str}")
    print(f"Signals: {len(out)}")
    for rule in ["B4", "B1", "B3", "B2"]:
        n = (out["rule"] == rule).sum() if not out.empty else 0
        print(f"  {rule}: {n}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
