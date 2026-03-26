#!/usr/bin/env python3
"""
generate_granville_signals.py
グランビル B1-B4 シグナル生成（TOPIX 約1,660銘柄）

IMPLEMENTATION.md §3-§5 準拠:
  B1: prev_below AND above AND sma_up
  B2: sma_up AND dev ∈ [-5%, 0%] AND up_day AND below
  B3: sma_up AND above AND dev ∈ [0%, +3%] AND (prev_dev > dev) AND up_day
  B4: (dev < -8%) AND up_day

  up_day = Close > prev_close（前日比陽線。Close > Open ではない）
  優先順位: B4 > B1 > B3 > B2
  同一ルール内: 到着順（RSI sortは有害、禁止）

Exit（§6）:
  20日高値: High[t] >= max(High[t-19:t+1])
  MAX_HOLD: ルール別（B4=19, B1=13, B3=14, B2=15）ポートフォリオ最適化結果
  SLなし

出力:
  data/parquet/granville/signals_YYYY-MM-DD.parquet
  data/parquet/granville/positions_YYYY-MM-DD.parquet
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
CSV_DIR = ROOT / "data" / "csv"
CREDIT_CSV = CSV_DIR / "credit_capacity.csv"

RULE_PRIORITY = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}
# 全ルールMH15（資本効率最大: 全期間+直近2年検証で確定）
RULE_MAX_HOLD = {"B4": 15, "B1": 15, "B3": 15, "B2": 15}


def _get_max_hold(rule: str) -> int:
    """ルール別MAX_HOLD"""
    return RULE_MAX_HOLD.get(rule, 15)


def _load_capital() -> float:
    """信用余力を読み込み"""
    if CREDIT_CSV.exists():
        try:
            cc = pd.read_csv(CREDIT_CSV)
            if not cc.empty:
                return float(cc.iloc[-1]["available_margin"])
        except Exception:
            pass
    return 4_650_000  # §7: デフォルト465万


def load_prices() -> pd.DataFrame:
    """TOPIX価格データを読み込み、テクニカル指標を計算（§3準拠）"""
    print("[1/4] Loading prices...")
    ps = pd.read_parquet(PRICES_PATH)
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)
    print(f"  {len(ps):,} rows, {ps['ticker'].nunique()} tickers")

    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["prev_close"] = g["Close"].shift(1)
    ps["prev_sma20"] = g["sma20"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)

    # ATR(10): ポジション管理用
    ps["prev_close_tr"] = g["Close"].shift(1)
    ps["tr"] = np.maximum(
        ps["High"] - ps["Low"],
        np.maximum(abs(ps["High"] - ps["prev_close_tr"]), abs(ps["Low"] - ps["prev_close_tr"]))
    )
    ps["atr10"] = g["tr"].transform(lambda x: x.rolling(10, min_periods=10).mean())
    ps["atr10_pct"] = ps["atr10"] / ps["Close"] * 100
    ps["ret5d"] = g["Close"].pct_change(5) * 100

    # §3: up_day = Close > prev_close（前日比陽線）
    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["above"] = ps["Close"] > ps["sma20"]
    ps["below"] = ps["Close"] < ps["sma20"]
    ps["prev_below"] = ps["prev_close"] < ps["prev_sma20"]
    ps["up_day"] = ps["Close"] > ps["prev_close"]

    ps = ps.dropna(subset=["sma20"])
    return ps


def load_meta() -> pd.DataFrame:
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
    """§4: B1-B4 シグナル検出"""
    df = df.copy()
    dev = df["dev_from_sma20"]
    sma_up = df["sma20_up"]

    df["B1"] = df["prev_below"] & df["above"] & sma_up
    df["B2"] = sma_up & dev.between(-5, 0) & df["up_day"] & df["below"]
    df["B3"] = sma_up & df["above"] & dev.between(0, 3) & (df["prev_dev"] > dev) & df["up_day"]
    df["B4"] = (dev < -8) & df["up_day"]

    return df


def assign_rule(row: pd.Series) -> str:
    """§5: B4 > B1 > B3 > B2"""
    if row["B4"]:
        return "B4"
    if row["B1"]:
        return "B1"
    if row["B3"]:
        return "B3"
    return "B2"


def generate_signals(ps: pd.DataFrame) -> pd.DataFrame:
    """最新日のシグナルを生成"""
    ps = detect_signals(ps)
    meta = load_meta()

    latest_date = ps["date"].max()
    print(f"\n[2/4] Signal detection for {latest_date.date()}...")

    latest = ps[ps["date"] == latest_date].copy()

    for r in ["B1", "B2", "B3", "B4"]:
        print(f"  {r}: {latest[r].sum()} signals")

    sig_mask = latest["B1"] | latest["B2"] | latest["B3"] | latest["B4"]
    signals = latest[sig_mask].copy()
    print(f"  Total raw: {len(signals)}")

    if signals.empty:
        return pd.DataFrame()

    signals["rule"] = signals.apply(assign_rule, axis=1)

    if not meta.empty:
        signals = signals.merge(
            meta[["ticker", "stock_name", "sectors"]].drop_duplicates(subset="ticker"),
            on="ticker", how="left",
        )
    else:
        signals["stock_name"] = ""
        signals["sectors"] = ""

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
        "prev_close": signals["prev_close"].round(1),
        "atr10_pct": signals["atr10_pct"].round(2),
        "ret5d": signals["ret5d"].round(2),
    })

    # §5: ルール優先のみ。同一ルール内は到着順（ソートなし）
    out["_priority"] = out["rule"].map(RULE_PRIORITY)
    out = out.sort_values("_priority", ascending=True).drop(columns=["_priority"]).reset_index(drop=True)

    return out


def generate_positions(ps: pd.DataFrame, latest_date: pd.Timestamp) -> pd.DataFrame:
    """§6: 過去シグナルから保有ポジション + Exit候補を計算"""
    print("\n[3/4] Generating positions...")

    capital = _load_capital()
    max_max_hold = max(RULE_MAX_HOLD.values())
    print(f"  Capital: ¥{capital:,.0f} → MAX_HOLD: {RULE_MAX_HOLD}")

    cutoff = latest_date - pd.Timedelta(days=max_max_hold * 2)
    recent = ps[ps["date"] >= cutoff].copy()
    recent = detect_signals(recent)

    sig_mask = recent["B1"] | recent["B2"] | recent["B3"] | recent["B4"]
    sigs = recent[sig_mask].copy()

    if sigs.empty:
        print("  No recent signals")
        return pd.DataFrame()

    sigs["rule"] = sigs.apply(assign_rule, axis=1)

    # メタデータ結合（stock_name）
    meta = load_meta()
    if not meta.empty:
        sigs = sigs.merge(
            meta[["ticker", "stock_name"]].drop_duplicates(subset="ticker"),
            on="ticker", how="left",
        )
        sigs["stock_name"] = sigs["stock_name"].fillna("")
    else:
        sigs["stock_name"] = ""

    # ticker別に事前グループ化（高速化）
    ticker_groups = {tk: gdf.sort_values("date").reset_index(drop=True)
                     for tk, gdf in ps.groupby("ticker")}

    rows: list[dict] = []
    for _, sig in sigs.iterrows():
        tk_all = ticker_groups.get(sig["ticker"])
        if tk_all is None:
            continue
        # エントリー日（シグナル翌営業日）のインデックスを特定
        entry_mask = tk_all["date"] > sig["date"]
        if not entry_mask.any():
            continue
        entry_iloc = entry_mask.idxmax()  # 最初のTrueのインデックス

        # §5: エントリーはシグナル翌営業日の寄付(Open)
        ep = float(tk_all.iloc[entry_iloc]["Open"])
        if pd.isna(ep) or ep <= 0:
            continue
        e_date = tk_all.iloc[entry_iloc]["date"]

        max_hold = _get_max_hold(sig["rule"])
        exited = False
        exit_type = ""
        exit_price = 0.0
        hold_end = min(entry_iloc + max_hold, len(tk_all))

        for day in range(entry_iloc, hold_end):
            row = tk_all.iloc[day]
            hold_day = day - entry_iloc

            # §6: 直近高値更新Exit — High[t] >= エントリー後rolling高値
            # 発火したら翌営業日寄付で決済（ユーザーが執行）
            if hold_day > 0:
                w_start = max(0, day - 19)
                window_highs = tk_all.iloc[w_start:day + 1]["High"]
                high_20d = float(window_highs.max())
                if float(row["High"]) >= high_20d:
                    exit_price = float(row["Close"])  # 参考値（実約定は翌寄付）
                    if row["date"] == latest_date:
                        exit_type = "high_update"
                    exited = True
                    break

            # §6: MAX_HOLD到達 → 翌営業日寄付で決済
            if hold_day >= max_hold - 1:
                exit_price = float(row["Close"])  # 参考値（実約定は翌寄付）
                if row["date"] == latest_date:
                    exit_type = "max_hold"
                exited = True
                break

        cur_day = min(entry_iloc + min(hold_end - entry_iloc, max_hold) - 1, len(tk_all) - 1)
        cur = tk_all.iloc[cur_day]
        cp = float(cur["Close"])
        hold_days = cur_day - entry_iloc + 1

        # エントリー後rolling高値（これを超えたら翌朝売り）
        entry_high = float(tk_all.iloc[entry_iloc:cur_day + 1]["High"].max())
        trigger_price = round(entry_high, 1)

        # ATR(10): 直近の平均true range
        atr10 = 0.0
        if "atr10" in cur.index and not pd.isna(cur.get("atr10")):
            atr10 = round(float(cur["atr10"]), 1)

        base = {
            "ticker": sig["ticker"],
            "stock_name": sig.get("stock_name", ""),
            "rule": sig["rule"],
            "entry_date": e_date,
            "entry_price": round(ep, 1),
            "current_price": round(cp, 1),
            "trigger_price": trigger_price,
            "atr10": atr10,
            "pct": round((cp / ep - 1) * 100, 2),
            "pnl": int((cp - ep) * 100),
            "hold_days": hold_days,
            "max_hold": max_hold,
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
    print("Generate Granville Signals (IMPLEMENTATION.md §3-§6)")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    if not PRICES_PATH.exists():
        print(f"[ERROR] Price data not found: {PRICES_PATH}")
        print("  Run fetch_prices.py first")
        return 1

    GRANVILLE_DIR.mkdir(parents=True, exist_ok=True)

    ps = load_prices()
    out = generate_signals(ps)

    if out.empty:
        print("\n[INFO] No signals today")
    else:
        print(f"\n  {len(out)} signals generated")
        for _, row in out.iterrows():
            print(f"    [{row['rule']}] {row['ticker']} {row['stock_name']} "
                  f"¥{row['close']:,.0f} ({row['dev_from_sma20']:+.1f}%)")

    latest_date = ps["date"].max()
    date_str = latest_date.strftime("%Y-%m-%d")

    signal_path = GRANVILLE_DIR / f"signals_{date_str}.parquet"
    if out.empty:
        out = pd.DataFrame(columns=[
            "signal_date", "ticker", "stock_name", "sector", "rule",
            "close", "open", "sma20", "dev_from_sma20", "sma20_slope",
            "entry_price_est", "prev_close",
        ])
    out.to_parquet(signal_path, index=False)
    print(f"\n[OK] Saved: {signal_path.name} ({len(out)} rows)")

    # ポジション計算
    ps = detect_signals(ps)
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
