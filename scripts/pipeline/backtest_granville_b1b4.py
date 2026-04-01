#!/usr/bin/env python3
"""
backtest_granville_b1b4.py
グランビル B1-B4 バックテスト（直近2年、TOPIX 1,660銘柄）

IMPLEMENTATION.md §3-§6 準拠:
  B1-B4シグナル検出 → 翌営業日寄付エントリー → 20日高値 or MAX_HOLD Exit
  出力: data/parquet/backtest/granville_b1b4_archive.parquet
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

from common_cfg.paths import PARQUET_DIR

PRICES_PATH = PARQUET_DIR / "granville" / "prices_topix.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"
META_FALLBACK = PARQUET_DIR / "meta.parquet"
OUT_PATH = PARQUET_DIR / "backtest" / "granville_b1b4_archive.parquet"

RULE_PRIORITY = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}
MAX_HOLD = 15  # 資本効率最大（全期間検証: SMA20回帰MH15 ¥331/日）
HIGH20D_PRE_ENTRY = 4  # 20日高値判定にエントリー前4日を含める（k=4最適、Codex検証済み）


def load_prices(lookback_years: int = 2) -> pd.DataFrame:
    """価格データ読み込み + テクニカル指標計算"""
    print("[1/4] Loading prices...")
    ps = pd.read_parquet(PRICES_PATH)
    ps["date"] = pd.to_datetime(ps["date"])

    cutoff = ps["date"].max() - pd.DateOffset(years=lookback_years)
    # SMA20計算のため少し余裕を持たせる
    buffer_cutoff = cutoff - pd.Timedelta(days=60)
    ps = ps[ps["date"] >= buffer_cutoff].copy()

    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)
    print(f"  {len(ps):,} rows, {ps['ticker'].nunique()} tickers")

    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["prev_close"] = g["Close"].shift(1)
    ps["prev_sma20"] = g["sma20"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)

    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["above"] = ps["Close"] > ps["sma20"]
    ps["below"] = ps["Close"] < ps["sma20"]
    ps["prev_below"] = ps["prev_close"] < ps["prev_sma20"]
    ps["up_day"] = ps["Close"] > ps["prev_close"]

    # スコアリング用特徴量
    prev_close = g["Close"].shift(1)
    tr = pd.concat([
        ps["High"] - ps["Low"],
        (ps["High"] - prev_close).abs(),
        (ps["Low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    ps["atr14"] = tr.groupby(ps["ticker"]).transform(
        lambda x: x.rolling(14, min_periods=14).mean()
    )
    ps["atr_pct"] = ps["atr14"] / ps["Close"] * 100
    ps["ret5d"] = g["Close"].pct_change(5) * 100

    # 急騰フィルター用
    ps["sma60"] = g["Close"].transform(lambda x: x.rolling(60, min_periods=60).mean())
    ps["sma100"] = g["Close"].transform(lambda x: x.rolling(100, min_periods=100).mean())
    ps["dev60"] = (ps["Close"] - ps["sma60"]) / ps["sma60"] * 100
    ps["dev100"] = (ps["Close"] - ps["sma100"]) / ps["sma100"] * 100
    ps["max_up20"] = g["dev_from_sma20"].transform(lambda x: x.rolling(60, min_periods=1).max())
    ps["max_up60"] = g["dev60"].transform(lambda x: x.rolling(60, min_periods=1).max())
    ps["max_up100"] = g["dev100"].transform(lambda x: x.rolling(60, min_periods=1).max())

    ps = ps.dropna(subset=["sma20"])
    # バックテスト対象期間のみ
    ps = ps[ps["date"] >= cutoff].copy()
    print(f"  Backtest period: {ps['date'].min().date()} ~ {ps['date'].max().date()}")
    return ps


def detect_signals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    dev = df["dev_from_sma20"]
    sma_up = df["sma20_up"]

    df["B1"] = df["prev_below"] & df["above"] & sma_up
    df["B2"] = sma_up & dev.between(-5, 0) & df["up_day"] & df["below"]
    df["B3"] = sma_up & df["above"] & dev.between(0, 3) & (df["prev_dev"] > dev) & df["up_day"]
    df["B4"] = (dev < -15) & df["up_day"]

    return df


def assign_rule(row: pd.Series) -> str:
    if row["B4"]:
        return "B4"
    if row["B1"]:
        return "B1"
    if row["B3"]:
        return "B3"
    return "B2"


def run_backtest(ps: pd.DataFrame) -> pd.DataFrame:
    """全期間バックテスト実行"""
    print("\n[2/4] Detecting signals...")
    ps = detect_signals(ps)

    # B4急騰フィルター
    b4_mask = ps["B4"].copy()
    if b4_mask.any():
        surge = (ps["max_up20"] >= 15) | (ps["max_up60"] >= 20) | (ps["max_up100"] >= 30)
        filtered_count = (b4_mask & surge).sum()
        ps["B4"] = b4_mask & ~surge
        print(f"  B4 surge filter: {filtered_count:,} excluded")

    sig_mask = ps["B1"] | ps["B2"] | ps["B3"] | ps["B4"]
    signals = ps[sig_mask].copy()
    signals["rule"] = signals.apply(assign_rule, axis=1)

    print(f"  Total signals: {len(signals):,}")
    for r in ["B4", "B1", "B3", "B2"]:
        print(f"    {r}: {(signals['rule'] == r).sum():,}")

    # メタ結合
    meta = pd.DataFrame()
    for p in [META_PATH, META_FALLBACK]:
        if p.exists():
            meta = pd.read_parquet(p)
            break

    if not meta.empty:
        name_map = dict(zip(meta["ticker"], meta.get("stock_name", "")))
        sector_map = dict(zip(meta["ticker"], meta.get("sectors", "")))
    else:
        name_map = {}
        sector_map = {}

    # ticker別に価格データをインデックス化（高速化）
    print("\n[3/4] Running backtest...")
    ticker_data = {}
    for tk, gdf in ps.groupby("ticker"):
        ticker_data[tk] = gdf.sort_values("date").reset_index(drop=True)

    dates = sorted(ps["date"].unique())
    date_set = set(dates)

    trades: list[dict] = []
    total = len(signals)
    milestone = max(1, total // 10)

    for idx, (_, sig) in enumerate(signals.iterrows()):
        if idx % milestone == 0:
            print(f"  {idx:,}/{total:,} ({idx/total*100:.0f}%)")

        tk = sig["ticker"]
        if tk not in ticker_data:
            continue

        tk_df = ticker_data[tk]
        # シグナル日の翌営業日を探す
        sig_date = sig["date"]
        future = tk_df[tk_df["date"] > sig_date]

        if future.empty:
            continue

        ep = float(future.iloc[0]["Open"])
        if pd.isna(ep) or ep <= 0:
            continue
        e_date = future.iloc[0]["date"]

        # エントリー日のtk_df上のiloc（k=4の高値窓で使用）
        entry_mask = tk_df["date"] == e_date
        if not entry_mask.any():
            continue
        entry_iloc = tk_df.index.get_loc(entry_mask.idxmax())

        # Exit判定
        exit_price = 0.0
        exit_date = None
        exit_type = ""
        exit_day = 0
        mae_pct = 0.0  # 最大逆行
        mfe_pct = 0.0  # 最大順行

        # Exit判定: 条件発火→翌営業日寄付で決済（再現可能）
        hold_limit = min(len(future) - 1, MAX_HOLD)  # 翌日寄付用に-1
        for i in range(hold_limit):
            row = future.iloc[i]
            cur_close = float(row["Close"])
            cur_high = float(row["High"])
            cur_low = float(row["Low"])

            # MAE/MFE
            day_low_pct = (cur_low / ep - 1) * 100
            day_high_pct = (cur_high / ep - 1) * 100
            mae_pct = min(mae_pct, day_low_pct)
            mfe_pct = max(mfe_pct, day_high_pct)

            # 直近高値更新Exit: エントリー前k日を含む窓で判定
            if i > 0:
                cur_iloc = entry_iloc + i
                start_iloc = max(entry_iloc - HIGH20D_PRE_ENTRY, cur_iloc - 19)
                start_iloc = max(0, start_iloc)
                window_highs = tk_df.iloc[start_iloc:cur_iloc + 1]["High"]
                high_20d = float(window_highs.max())
                if cur_high >= high_20d:
                    next_row = future.iloc[i + 1]
                    exit_price = float(next_row["Open"])
                    exit_date = next_row["date"]
                    exit_type = "high_update"
                    exit_day = i + 2  # エントリーからの日数
                    break

            # MAX_HOLD: 到達→翌営業日寄付で決済
            if i >= hold_limit - 1:
                next_row = future.iloc[i + 1]
                exit_price = float(next_row["Open"])
                exit_date = next_row["date"]
                exit_type = "max_hold"
                exit_day = i + 2
                break

        if exit_date is None:
            # 期間末: 最終日の翌営業日寄付が取れない場合は終値
            last_idx = min(len(future) - 1, MAX_HOLD)
            last = future.iloc[last_idx]
            exit_price = float(last["Close"])
            exit_date = last["date"]
            exit_type = "end_of_data"
            exit_day = last_idx + 1

        ret_pct = round((exit_price / ep - 1) * 100, 3)
        pnl_yen = int((exit_price - ep) * 100)

        trades.append({
            "signal_date": sig_date,
            "ticker": tk,
            "stock_name": name_map.get(tk, ""),
            "sector": sector_map.get(tk, ""),
            "rule": sig["rule"],
            "entry_date": e_date,
            "exit_date": exit_date,
            "entry_price": round(ep, 1),
            "exit_price": round(exit_price, 1),
            "ret_pct": ret_pct,
            "pnl_yen": pnl_yen,
            "exit_type": exit_type,
            "exit_day": exit_day,
            "mae_pct": round(mae_pct, 2),
            "mfe_pct": round(mfe_pct, 2),
            "dev_from_sma20": round(float(sig.get("dev_from_sma20", 0)), 2),
            "atr_pct": round(float(sig.get("atr_pct", 0)), 2),
            "ret5d": round(float(sig.get("ret5d", 0)), 2),
        })

    print(f"  {len(trades):,} completed trades")
    return pd.DataFrame(trades)


def main() -> int:
    print("=" * 60)
    print("Granville B1-B4 Backtest (2Y, TOPIX)")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    ps = load_prices(lookback_years=2)
    result = run_backtest(ps)

    if result.empty:
        print("[WARN] No trades")
        return 1

    # サマリー
    print("\n[4/4] Summary")
    print(f"  Total trades: {len(result):,}")
    for rule in ["B4", "B1", "B3", "B2"]:
        rdf = result[result["rule"] == rule]
        if rdf.empty:
            continue
        n = len(rdf)
        wins = (rdf["ret_pct"] > 0).sum()
        wr = wins / n * 100
        avg = rdf["ret_pct"].mean()
        total = rdf["pnl_yen"].sum()
        h20_exits = (rdf["exit_type"] == "20d_high").sum()
        mh_exits = (rdf["exit_type"] == "max_hold").sum()
        print(f"  {rule}: {n:,} trades | WR: {wr:.1f}% | Avg: {avg:+.2f}% | PnL: ¥{total:,} | 20dH:{h20_exits} MH:{mh_exits}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(OUT_PATH, index=False)
    print(f"\n[OK] Saved: {OUT_PATH}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
