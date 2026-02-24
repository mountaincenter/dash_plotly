#!/usr/bin/env python3
"""
S1逆引き分析: S1最適イグジット時点の各指標値を算出する。

目的:
  S1（結果的に最大利益だった時刻）の瞬間に、各テクニカル指標は何を示していたか？
  → リアルタイムでS1を近似するための「道具」を特定する。

使用方法:
    cd dash_plotly
    python improvement/analyze_s1_reverse_lookup.py

出力:
    improvement/output/s1_reverse_lookup.parquet  (全トレードの逆引きデータ)
    improvement/output/s1_reverse_lookup.html     (分析レポート)
"""

import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

_DASH_DIR = Path(__file__).resolve().parent.parent
if str(_DASH_DIR) not in sys.path:
    sys.path.insert(0, str(_DASH_DIR))

from server.services.macd_signals import compute_macd, compute_rsi
from server.services.granville import detect_granville_signals
from server.services.tech_utils_v2 import sma

# 既存の関数を再利用
from improvement.generate_exit_strategy_analysis import (
    load_archive, load_5m, _detect_split_ratio, SEG_COLS,
)


# ---------------------------------------------------------------------------
# 前日データを含む日中5分足抽出
# ---------------------------------------------------------------------------
def _extract_intraday_with_prev(
    df_5m: pd.DataFrame, ticker: str, date_str: str, buy_price: float
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, float]:
    """前日+当日の5分足を取得し、指標計算に使える形で返す。

    Returns:
        (today_sub, full_sub, split_ratio)
        - today_sub: 当日分のみ（S1特定・P&L計算用）
        - full_sub: 前日+当日（指標計算用。前日分でウォームアップ）
        - split_ratio: 分割比率
    """
    dt = pd.Timestamp(date_str)
    target_date = dt.date()

    # 当日分
    today_mask = (df_5m["ticker"] == ticker) & (df_5m["datetime"].dt.date == target_date)
    today_sub = df_5m.loc[today_mask].copy()
    if len(today_sub) < 5:
        return None, None, 1.0

    today_sub = today_sub.sort_values("datetime").reset_index(drop=True)

    # 分割比率を検出
    first_open = today_sub["open"].iloc[0]
    ratio = _detect_split_ratio(buy_price, first_open)

    # 前営業日を探す（最大5日遡る）
    prev_sub = pd.DataFrame()
    for days_back in range(1, 6):
        prev_date = (dt - pd.Timedelta(days=days_back)).date()
        prev_mask = (df_5m["ticker"] == ticker) & (df_5m["datetime"].dt.date == prev_date)
        cand = df_5m.loc[prev_mask]
        if len(cand) >= 5:
            prev_sub = cand.copy().sort_values("datetime").reset_index(drop=True)
            break

    # 分割補正
    if ratio > 1.0:
        for col in ["open", "high", "low", "close"]:
            today_sub[col] = today_sub[col] * ratio
        today_sub["volume"] = today_sub["volume"] / ratio
        if len(prev_sub) > 0:
            for col in ["open", "high", "low", "close"]:
                prev_sub[col] = prev_sub[col] * ratio
            prev_sub["volume"] = prev_sub["volume"] / ratio

    # 前日+当日を結合（指標計算用）
    if len(prev_sub) > 0:
        full_sub = pd.concat([prev_sub, today_sub], ignore_index=True)
    else:
        full_sub = today_sub.copy()

    return today_sub, full_sub, ratio


# ---------------------------------------------------------------------------
# S1最適時刻の特定
# ---------------------------------------------------------------------------
def find_s1_bar_index(sub: pd.DataFrame, buy_price: float) -> int | None:
    """5分足DataFrameからS1（最大ショート利益）のバーindexを返す。

    ここでは close が最も低いバー = ショート利益最大とする。
    """
    if sub is None or len(sub) == 0:
        return None
    # 各バーのショートP&L = (buy_price - close) * 100
    short_pnl = (buy_price - sub["close"]) * 100
    best_idx = short_pnl.idxmax()
    if pd.isna(short_pnl.loc[best_idx]):
        return None
    return best_idx


# ---------------------------------------------------------------------------
# 各指標の算出
# ---------------------------------------------------------------------------
def compute_indicators_on_full(
    full_sub: pd.DataFrame, today_start_idx: int
) -> pd.DataFrame:
    """前日+当日の結合DataFrameで指標を計算し、当日分だけ返す。

    Args:
        full_sub: 前日+当日の5分足（指標ウォームアップ用）
        today_start_idx: full_sub内で当日分が始まるindex位置
    """
    df = full_sub.copy()

    # RSI(9) — 前日データでウォームアップ済み
    df["rsi9"] = compute_rsi(df["close"], period=9)

    # MACD(5,20,9)
    macd = compute_macd(df["close"], fast=5, slow=20, signal_period=9)
    df["macd_line"] = macd["macd_line"]
    df["macd_signal"] = macd["signal_line"]
    df["macd_hist"] = macd["histogram"]

    # MA(25)
    df["ma25"] = sma(df["close"], 25)
    df["ma25_div_pct"] = np.where(
        df["ma25"] > 0,
        (df["close"] - df["ma25"]) / df["ma25"] * 100,
        np.nan,
    )

    # 当日部分だけ切り出し
    today = df.iloc[today_start_idx:].copy().reset_index(drop=True)

    # 出来高（当日のみの累積平均比）
    cum_avg_vol = today["volume"].expanding().mean()
    today["vol_ratio"] = np.where(cum_avg_vol > 0, today["volume"] / cum_avg_vol, np.nan)

    # 価格変化率（当日始値からの乖離）
    first_open = today["open"].iloc[0]
    if first_open > 0:
        today["pct_from_open"] = (today["close"] - first_open) / first_open * 100
    else:
        today["pct_from_open"] = np.nan

    return today


# ---------------------------------------------------------------------------
# メイン: 逆引きデータ算出
# ---------------------------------------------------------------------------
def build_reverse_lookup(archive: pd.DataFrame, df_5m: pd.DataFrame) -> pd.DataFrame:
    """全トレードに対してS1時点の指標値を算出する。"""
    records = []
    total = len(archive)
    matched = 0

    for idx, row in archive.iterrows():
        ticker = row["ticker"]
        date_str = str(row["backtest_date"])[:10]
        buy_price = row["buy_price"]

        rec = {
            "ticker": ticker,
            "date": date_str,
            "buy_price": buy_price,
            "stock_name": row.get("stock_name", ""),
            "weekday": int(row.get("weekday", -1)) if pd.notna(row.get("weekday")) else -1,
            "daily_close": row.get("daily_close", np.nan),
            "atr14_pct": row.get("atr14_pct", np.nan),
            "shortable": row.get("shortable", None),
        }

        # S7 P&L（ベースライン）
        dc = row.get("daily_close", np.nan)
        rec["s7_pnl"] = (buy_price - dc) * 100 if pd.notna(dc) else np.nan

        # 5分足取得（前日+当日）
        today_sub, full_sub, split_ratio = _extract_intraday_with_prev(
            df_5m, ticker, date_str, buy_price
        )
        if today_sub is None or len(today_sub) < 5:
            rec["has_5m"] = False
            records.append(rec)
            if (idx + 1) % 200 == 0:
                print(f"  {idx + 1}/{total}")
            continue

        matched += 1
        rec["has_5m"] = True

        # 当日の開始位置を特定
        today_start_idx = len(full_sub) - len(today_sub)

        # 指標算出（前日データでウォームアップ）
        sub_ind = compute_indicators_on_full(full_sub, today_start_idx)

        # --- S1: 最大ショート利益バー ---
        s1_idx = find_s1_bar_index(sub_ind, buy_price)
        if s1_idx is None:
            records.append(rec)
            continue

        s1_bar = sub_ind.loc[s1_idx]
        s1_pos = sub_ind.index.get_loc(s1_idx)  # 0-based position
        total_bars = len(sub_ind)

        rec["s1_pnl"] = (buy_price - s1_bar["close"]) * 100
        rec["s1_time"] = s1_bar["datetime"].strftime("%H:%M")
        rec["s1_price"] = s1_bar["close"]
        rec["s1_bar_position"] = s1_pos  # 何番目のバーか
        rec["s1_bar_pct"] = s1_pos / total_bars * 100  # 全体の何%時点か

        # S1時点の指標値
        rec["s1_rsi9"] = s1_bar.get("rsi9", np.nan)
        rec["s1_macd_line"] = s1_bar.get("macd_line", np.nan)
        rec["s1_macd_signal"] = s1_bar.get("macd_signal", np.nan)
        rec["s1_macd_hist"] = s1_bar.get("macd_hist", np.nan)
        rec["s1_ma25_div_pct"] = s1_bar.get("ma25_div_pct", np.nan)
        rec["s1_vol_ratio"] = s1_bar.get("vol_ratio", np.nan)
        rec["s1_pct_from_open"] = s1_bar.get("pct_from_open", np.nan)

        # --- S2-S6 の結果も記録 ---
        # S2: 最初のRSI<30
        rsi = sub_ind["rsi9"]
        s2_found = False
        for i in range(len(sub_ind)):
            if pd.notna(rsi.iloc[i]) and rsi.iloc[i] < 30:
                s2_bar = sub_ind.iloc[i]
                rec["s2_pnl"] = (buy_price - s2_bar["close"]) * 100
                rec["s2_time"] = s2_bar["datetime"].strftime("%H:%M")
                rec["s2_rsi9"] = rsi.iloc[i]
                rec["s2_bar_position"] = i
                s2_found = True
                break
        if not s2_found:
            rec["s2_pnl"] = np.nan
            rec["s2_time"] = None

        # S3: MACD GC
        ml = sub_ind["macd_line"]
        sl = sub_ind["macd_signal"]
        s3_found = False
        for i in range(1, len(sub_ind)):
            if (pd.notna(ml.iloc[i-1]) and pd.notna(sl.iloc[i-1])
                    and pd.notna(ml.iloc[i]) and pd.notna(sl.iloc[i])):
                if ml.iloc[i-1] <= sl.iloc[i-1] and ml.iloc[i] > sl.iloc[i]:
                    s3_bar = sub_ind.iloc[i]
                    rec["s3_pnl"] = (buy_price - s3_bar["close"]) * 100
                    rec["s3_time"] = s3_bar["datetime"].strftime("%H:%M")
                    rec["s3_bar_position"] = i
                    s3_found = True
                    break
        if not s3_found:
            rec["s3_pnl"] = np.nan
            rec["s3_time"] = None

        # S6: MA(25)上抜け
        ma25 = sub_ind["ma25"]
        s6_found = False
        for i in range(26, len(sub_ind)):
            if (pd.notna(ma25.iloc[i]) and pd.notna(ma25.iloc[i-1])):
                if sub_ind["close"].iloc[i-1] <= ma25.iloc[i-1] and sub_ind["close"].iloc[i] > ma25.iloc[i]:
                    s6_bar = sub_ind.iloc[i]
                    rec["s6_pnl"] = (buy_price - s6_bar["close"]) * 100
                    rec["s6_time"] = s6_bar["datetime"].strftime("%H:%M")
                    rec["s6_bar_position"] = i
                    s6_found = True
                    break
        if not s6_found:
            rec["s6_pnl"] = np.nan
            rec["s6_time"] = None

        # --- 偽底分析: S1より前にS2シグナルが出ていたか ---
        if s2_found and rec.get("s2_bar_position") is not None:
            rec["s2_before_s1"] = rec["s2_bar_position"] < s1_pos
            if rec["s2_before_s1"]:
                # S2で抜けていたらS1の利益をどれだけ逃したか
                rec["s2_missed_pnl"] = rec["s1_pnl"] - rec["s2_pnl"]
            else:
                rec["s2_missed_pnl"] = 0
        else:
            rec["s2_before_s1"] = None
            rec["s2_missed_pnl"] = np.nan

        # S3がS1より前に出ていたか
        if s3_found and rec.get("s3_bar_position") is not None:
            rec["s3_before_s1"] = rec["s3_bar_position"] < s1_pos
        else:
            rec["s3_before_s1"] = None

        # --- 当日のRSI最小値とS1のRSIの関係 ---
        rsi_valid = rsi.dropna()
        if len(rsi_valid) > 0:
            rec["rsi_min_value"] = rsi_valid.min()
            rsi_min_idx = rsi_valid.idxmin()
            rec["rsi_min_position"] = sub_ind.index.get_loc(rsi_min_idx)
            rec["rsi_min_is_s1"] = abs(rec["rsi_min_position"] - s1_pos) <= 2  # 前後2バー以内
        else:
            rec["rsi_min_value"] = np.nan

        # --- MACD最小値とS1の関係 ---
        macd_hist = sub_ind["macd_hist"].dropna()
        if len(macd_hist) > 0:
            rec["macd_hist_min"] = macd_hist.min()
            macd_min_idx = macd_hist.idxmin()
            rec["macd_hist_min_position"] = sub_ind.index.get_loc(macd_min_idx)
            rec["macd_min_is_s1"] = abs(rec["macd_hist_min_position"] - s1_pos) <= 2

        records.append(rec)
        if (idx + 1) % 200 == 0:
            print(f"  {idx + 1}/{total} (matched: {matched})")

    print(f"  完了: {total}行, 5分足マッチ: {matched}/{total}")
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("S1逆引き分析")
    print("=" * 60)

    print("\n[1/3] データ読み込み")
    archive = load_archive()
    df_5m = load_5m()

    print("\n[2/3] 逆引きデータ算出")
    rl_df = build_reverse_lookup(archive, df_5m)

    # 保存
    out_dir = Path(__file__).resolve().parent / "output"
    out_dir.mkdir(exist_ok=True)
    parquet_path = out_dir / "s1_reverse_lookup.parquet"
    rl_df.to_parquet(parquet_path, index=False)
    print(f"\n  保存: {parquet_path}")

    # --- サマリー出力 ---
    has5m = rl_df[rl_df["has_5m"] == True]
    print(f"\n[3/3] サマリー (5分足あり: {len(has5m)}件)")

    print("\n--- S1時点のRSI(9)分布 ---")
    rsi_col = has5m["s1_rsi9"].dropna()
    if len(rsi_col) > 0:
        for lo, hi in [(0,10),(10,20),(20,30),(30,40),(40,50),(50,60),(60,70),(70,100)]:
            cnt = ((rsi_col >= lo) & (rsi_col < hi)).sum()
            pct = cnt / len(rsi_col) * 100
            # このバケットのS1 P&L平均
            mask = (has5m["s1_rsi9"] >= lo) & (has5m["s1_rsi9"] < hi)
            avg_pnl = has5m.loc[mask, "s1_pnl"].mean()
            print(f"  RSI {lo:>2}-{hi:<3}: {cnt:>4}件 ({pct:>5.1f}%)  S1平均P&L: {avg_pnl:>+10,.0f}円")

    print("\n--- S1時点のMACDヒストグラム分布 ---")
    macd_col = has5m["s1_macd_hist"].dropna()
    if len(macd_col) > 0:
        for q in [0, 10, 25, 50, 75, 90, 100]:
            print(f"  {q:>3}%ile: {np.percentile(macd_col, q):>+.2f}")

    print("\n--- RSI最小値 ≈ S1 の一致率 ---")
    rsi_is_s1 = has5m["rsi_min_is_s1"].dropna()
    if len(rsi_is_s1) > 0:
        match_rate = rsi_is_s1.sum() / len(rsi_is_s1) * 100
        print(f"  RSI最小値がS1と前後2バー以内: {rsi_is_s1.sum()}/{len(rsi_is_s1)} ({match_rate:.1f}%)")

    print("\n--- MACDヒストグラム最小値 ≈ S1 の一致率 ---")
    macd_is_s1 = has5m["macd_min_is_s1"].dropna()
    if len(macd_is_s1) > 0:
        match_rate = macd_is_s1.sum() / len(macd_is_s1) * 100
        print(f"  MACDhist最小値がS1と前後2バー以内: {macd_is_s1.sum()}/{len(macd_is_s1)} ({match_rate:.1f}%)")

    print("\n--- S2(RSI<30)がS1より前に発生（偽底）の割合 ---")
    s2_before = has5m["s2_before_s1"].dropna()
    if len(s2_before) > 0:
        fake_rate = s2_before.sum() / len(s2_before) * 100
        missed = has5m.loc[has5m["s2_before_s1"] == True, "s2_missed_pnl"].dropna()
        print(f"  偽底率: {s2_before.sum()}/{len(s2_before)} ({fake_rate:.1f}%)")
        if len(missed) > 0:
            print(f"  偽底時の逸失利益平均: {missed.mean():+,.0f}円")

    print("\n--- S2-S6 の S1回収率 ---")
    for s_name, col in [("S2(RSI)", "s2_pnl"), ("S3(MACD GC)", "s3_pnl"), ("S6(MA25)", "s6_pnl"), ("S7(大引)", "s7_pnl")]:
        both = has5m[[col, "s1_pnl"]].dropna()
        if len(both) > 0:
            recovery = both[col].sum() / both["s1_pnl"].sum() * 100
            print(f"  {s_name:15s}: 回収率 {recovery:>6.1f}%  (合計P&L {both[col].sum():>+12,.0f} / S1合計 {both['s1_pnl'].sum():>+12,.0f})")

    print("\n✅ 完了")


if __name__ == "__main__":
    main()
