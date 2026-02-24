#!/usr/bin/env python3
"""
イグジット戦略バックテスト分析 HTML 生成スクリプト

エントリー固定（寄付ショート）に対して、7つの出口戦略を過去データで比較検証する。
楽天証券 MARKETSPEED パラメータ準拠。

使用方法:
    cd dash_plotly
    python improvement/generate_exit_strategy_analysis.py

出力:
    improvement/output/exit_strategy_analysis.html
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

# --- import 用パス設定 ---
_DASH_DIR = Path(__file__).resolve().parent.parent
if str(_DASH_DIR) not in sys.path:
    sys.path.insert(0, str(_DASH_DIR))

from server.services.macd_signals import compute_macd, compute_rsi
from server.services.granville import detect_granville_signals
from server.services.tech_utils_v2 import sma

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
WEEKDAY_NAMES = ["月", "火", "水", "木", "金", "土", "日"]

SEG_COLS = [
    "seg_0930", "seg_1000", "seg_1030", "seg_1100", "seg_1130",
    "seg_1300", "seg_1330", "seg_1400", "seg_1430", "seg_1500", "seg_1530",
]

STRATEGY_NAMES = {
    1: "時間帯別（統計ベスト）",
    2: "RSI利確 (RSI9<30)",
    3: "MACD GC",
    4: "MACDゼロ割れ復帰",
    5: "グランビル買いシグナル",
    6: "MA(25)上抜け",
    7: "大引け（ベースライン）",
}

RSI_BANDS = [(0, 30, "0-30"), (30, 50, "30-50"), (50, 70, "50-70"), (70, 100, "70-100")]
RSI_LOW_BUCKETS = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 30), (30, 35), (35, 40), (40, 100)]
ATR_BANDS = [(0, 3, "~3%"), (3, 5, "3-5%"), (5, 7, "5-7%"), (7, 100, "7%~")]
PRICE_BANDS = [(0, 1000, "~1000"), (1000, 3000, "1000-3000"), (3000, 5000, "3000-5000"), (5000, 1e9, "5000~")]


# ---------------------------------------------------------------------------
# データ読み込み
# ---------------------------------------------------------------------------
def load_archive() -> pd.DataFrame:
    path = _DASH_DIR / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
    if not path.exists():
        print(f"❌ アーカイブが見つかりません: {path}")
        sys.exit(1)
    df = pd.read_parquet(path)
    print(f"  archive: {len(df)} rows")
    return df


def load_5m() -> pd.DataFrame:
    """3つの5分足ファイルを結合し、JST に統一、重複除去"""
    base = _DASH_DIR / "data" / "parquet" / "backtest"
    files = [
        "grok_5m_60d_20251230.parquet",
        "grok_5m_60d_20260110.parquet",
        "grok_5m_60d_20260130.parquet",
    ]
    frames = []
    for f in files:
        p = base / f
        if not p.exists():
            print(f"  ⚠️ {f} が見つかりません、スキップ")
            continue
        tmp = pd.read_parquet(p)
        print(f"  {f}: {len(tmp)} rows")
        # タイムゾーン統一
        if tmp["datetime"].dt.tz is not None:
            if str(tmp["datetime"].dt.tz) == "UTC":
                tmp["datetime"] = tmp["datetime"].dt.tz_convert("Asia/Tokyo")
            # Asia/Tokyo ならそのまま
            tmp["datetime"] = tmp["datetime"].dt.tz_localize(None)
        frames.append(tmp)

    if not frames:
        print("❌ 5分足データが1つも見つかりません")
        sys.exit(1)

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["datetime", "ticker"]).sort_values(["ticker", "datetime"])
    df = df.reset_index(drop=True)
    print(f"  5分足合計: {len(df)} rows ({df['ticker'].nunique()} tickers)")
    return df


def load_daily() -> pd.DataFrame:
    path = _DASH_DIR / "data" / "parquet" / "grok_prices_max_1d.parquet"
    if not path.exists():
        print(f"❌ 日足データが見つかりません: {path}")
        sys.exit(1)
    df = pd.read_parquet(path)
    print(f"  daily: {len(df)} rows")
    return df


# ---------------------------------------------------------------------------
# 出口戦略 P&L 計算
# ---------------------------------------------------------------------------
def _detect_split_ratio(buy_price: float, first_open: float) -> float:
    """buy_price と5分足始値から株式分割比率を推定。分割なしなら 1.0"""
    if buy_price <= 0 or first_open <= 0:
        return 1.0
    raw = buy_price / first_open
    if raw < 1.3:
        return 1.0
    # よくある分割比: 2, 3, 4, 5, 10, 20
    candidates = [2, 3, 4, 5, 10, 20]
    best = min(candidates, key=lambda c: abs(raw - c))
    if abs(raw - best) / best < 0.15:
        return float(best)
    return 1.0


def _extract_intraday(
    df_5m: pd.DataFrame, ticker: str, date_str: str, buy_price: float
) -> tuple[Optional[pd.DataFrame], float]:
    """指定銘柄・日付の日中5分足を抽出。分割補正済み。

    Returns:
        (sub, split_ratio): 5分足DataFrame（補正済み）と分割比率
    """
    dt = pd.Timestamp(date_str)
    mask = (df_5m["ticker"] == ticker) & (df_5m["datetime"].dt.date == dt.date())
    sub = df_5m.loc[mask].copy()
    if len(sub) < 5:
        return None, 1.0
    sub = sub.sort_values("datetime").reset_index(drop=True)

    # 分割比率を検出し、価格列を補正
    first_open = sub["open"].iloc[0]
    ratio = _detect_split_ratio(buy_price, first_open)
    if ratio > 1.0:
        for col in ["open", "high", "low", "close"]:
            sub[col] = sub[col] * ratio
        sub["volume"] = sub["volume"] / ratio
    return sub, ratio


def strategy_1_seg_best(row: pd.Series) -> dict:
    """戦略1: 時間帯別セグメントの最大ショート利益時点

    seg列は (price_at_time - buy_price)*100 = ロング視点。
    ショートP&L = -seg なので、最小の seg が最大ショート利益。
    """
    best_short_pnl = np.nan
    best_time = None
    for col in SEG_COLS:
        val = row.get(col, np.nan)
        if pd.notna(val):
            short_pnl = -val  # ロング→ショート変換
            if pd.isna(best_short_pnl) or short_pnl > best_short_pnl:
                best_short_pnl = short_pnl
                best_time = col.replace("seg_", "")
    if pd.isna(best_short_pnl):
        return {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}
    return {"pnl": best_short_pnl, "exit_time": best_time, "exit_price": np.nan}


def strategy_2_rsi_exit(sub: pd.DataFrame, buy_price: float) -> dict:
    """戦略2: 5分足RSI(9)が30未満でカバー"""
    if len(sub) < 10:
        return {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}
    rsi = compute_rsi(sub["close"], period=9)
    for i in range(len(sub)):
        if pd.notna(rsi.iloc[i]) and rsi.iloc[i] < 30:
            ep = sub["close"].iloc[i]
            return {
                "pnl": (buy_price - ep) * 100,
                "exit_time": sub["datetime"].iloc[i].strftime("%H:%M"),
                "exit_price": ep,
            }
    return {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}


def strategy_3_macd_gc(sub: pd.DataFrame, buy_price: float) -> dict:
    """戦略3: 5分足MACD(5,20,9) GC でカバー"""
    if len(sub) < 30:
        return {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}
    macd = compute_macd(sub["close"], fast=5, slow=20, signal_period=9)
    ml = macd["macd_line"]
    sl = macd["signal_line"]
    for i in range(1, len(sub)):
        if pd.notna(ml.iloc[i - 1]) and pd.notna(sl.iloc[i - 1]) and pd.notna(ml.iloc[i]) and pd.notna(sl.iloc[i]):
            if ml.iloc[i - 1] <= sl.iloc[i - 1] and ml.iloc[i] > sl.iloc[i]:
                ep = sub["close"].iloc[i]
                return {
                    "pnl": (buy_price - ep) * 100,
                    "exit_time": sub["datetime"].iloc[i].strftime("%H:%M"),
                    "exit_price": ep,
                }
    return {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}


def strategy_4_macd_zero(sub: pd.DataFrame, buy_price: float) -> dict:
    """戦略4: MACDラインが0以下→0以上に復帰でカバー"""
    if len(sub) < 30:
        return {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}
    macd = compute_macd(sub["close"], fast=5, slow=20, signal_period=9)
    ml = macd["macd_line"]
    was_below_zero = False
    for i in range(len(sub)):
        if pd.isna(ml.iloc[i]):
            continue
        if ml.iloc[i] <= 0:
            was_below_zero = True
        elif was_below_zero and ml.iloc[i] > 0:
            ep = sub["close"].iloc[i]
            return {
                "pnl": (buy_price - ep) * 100,
                "exit_time": sub["datetime"].iloc[i].strftime("%H:%M"),
                "exit_price": ep,
            }
    return {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}


def strategy_5_granville(sub: pd.DataFrame, buy_price: float) -> dict:
    """戦略5: グランビル買いシグナル(buy_1〜buy_4)でカバー"""
    if len(sub) < 30:
        return {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}
    # detect_granville_signals は date, Open, High, Low, Close 列を要求
    gdf = sub.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"}).copy()
    gdf["date"] = gdf["datetime"]
    signals = detect_granville_signals(gdf, ma_period=25)
    for sig in signals:
        if sig["side"] == "buy":
            ep = sig["price"]
            return {
                "pnl": (buy_price - ep) * 100,
                "exit_time": sig["time"][-5:] if len(sig["time"]) >= 5 else sig["time"],
                "exit_price": ep,
            }
    return {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}


def strategy_6_ma_crossover(sub: pd.DataFrame, buy_price: float) -> dict:
    """戦略6: 終値がMA(25)を上抜けでカバー"""
    if len(sub) < 26:
        return {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}
    ma25 = sma(sub["close"], 25)
    for i in range(26, len(sub)):
        if pd.notna(ma25.iloc[i]) and pd.notna(ma25.iloc[i - 1]):
            if sub["close"].iloc[i - 1] <= ma25.iloc[i - 1] and sub["close"].iloc[i] > ma25.iloc[i]:
                ep = sub["close"].iloc[i]
                return {
                    "pnl": (buy_price - ep) * 100,
                    "exit_time": sub["datetime"].iloc[i].strftime("%H:%M"),
                    "exit_price": ep,
                }
    return {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}


def strategy_7_close(row: pd.Series) -> dict:
    """戦略7: 大引け（15:30）でカバー — ベースライン"""
    bp = row.get("buy_price", np.nan)
    dc = row.get("daily_close", np.nan)
    if pd.isna(bp) or pd.isna(dc):
        return {"pnl": np.nan, "exit_time": "15:30", "exit_price": dc}
    return {"pnl": (bp - dc) * 100, "exit_time": "15:30", "exit_price": dc}


# ---------------------------------------------------------------------------
# RSI × 日中最安値 分析
# ---------------------------------------------------------------------------
def analyze_rsi_at_daily_low(
    archive: pd.DataFrame, df_5m: pd.DataFrame
) -> pd.DataFrame:
    """各トレードで5分足の最安値時点のRSI、パターンを算出する。

    Returns:
        rsi_df: ticker, date, daily_low_time, rsi_at_daily_low,
                first_rsi30_time, first_rsi30_price,
                rsi_min_value, rsi_min_time, pattern を含む DataFrame
    """
    records: list[dict] = []
    total = len(archive)

    for idx, row in archive.iterrows():
        ticker = row["ticker"]
        date_str = str(row["backtest_date"])[:10]
        buy_price = row["buy_price"]

        rec: dict = {"ticker": ticker, "date": date_str}

        sub, _ = _extract_intraday(df_5m, ticker, date_str, buy_price)
        if sub is None or len(sub) < 10:
            rec.update({
                "daily_low_time": pd.NaT,
                "rsi_at_daily_low": np.nan,
                "first_rsi30_time": pd.NaT,
                "first_rsi30_price": np.nan,
                "rsi_min_value": np.nan,
                "rsi_min_time": pd.NaT,
                "pattern": "no_data",
            })
            records.append(rec)
            continue

        rsi = compute_rsi(sub["close"], period=9)

        # --- 最安値 ---
        low_idx = sub["low"].idxmin()
        rec["daily_low_time"] = sub.loc[low_idx, "datetime"]

        # 最安値足のRSI（low列で最安値の足のclose時点RSI）
        rec["rsi_at_daily_low"] = rsi.iloc[sub.index.get_loc(low_idx)]

        # --- RSI最小値 ---
        rsi_valid = rsi.dropna()
        if len(rsi_valid) > 0:
            rsi_min_pos = rsi_valid.idxmin()
            rec["rsi_min_value"] = rsi_valid.loc[rsi_min_pos]
            rec["rsi_min_time"] = sub.loc[rsi_min_pos, "datetime"]
        else:
            rec["rsi_min_value"] = np.nan
            rec["rsi_min_time"] = pd.NaT

        # --- 最初の RSI<30 ---
        rsi30_mask = rsi < 30
        rsi30_indices = rsi[rsi30_mask & rsi.notna()].index
        if len(rsi30_indices) > 0:
            first_i = rsi30_indices[0]
            rec["first_rsi30_time"] = sub.loc[first_i, "datetime"]
            rec["first_rsi30_price"] = sub.loc[first_i, "close"]
        else:
            rec["first_rsi30_time"] = pd.NaT
            rec["first_rsi30_price"] = np.nan

        # --- パターン分類 ---
        rec["pattern"] = _classify_rsi_pattern(sub, rsi, rec)
        records.append(rec)

        if (idx + 1) % 200 == 0:
            print(f"  RSI分析: {idx + 1}/{total}")

    rsi_df = pd.DataFrame(records)
    print(f"  RSI分析完了: {len(rsi_df)}行")
    return rsi_df


def _classify_rsi_pattern(
    sub: pd.DataFrame, rsi: pd.Series, rec: dict
) -> str:
    """パターンを分類する。

    - true_bottom: 最初のRSI<30が最安値の前後15分以内、かつその後1%以上下落しない
    - fake_bottom: 最初のRSI<30が最安値の30分以上前、かつ最安値がRSI<30時点より2%以上下
    - gradual_decline: RSI<35が60分以上継続、かつ最安値が13:30以降
    - no_signal: RSIが30を下回らない
    """
    first_rsi30_time = rec.get("first_rsi30_time")
    daily_low_time = rec.get("daily_low_time")
    first_rsi30_price = rec.get("first_rsi30_price", np.nan)

    if pd.isna(first_rsi30_time) or first_rsi30_time is pd.NaT:
        return "no_signal"

    low_price = sub["low"].min()

    # 時間差（分）
    time_diff_min = (daily_low_time - first_rsi30_time).total_seconds() / 60.0

    # true_bottom: 前後15分以内 & その後1%以上下落しない
    if abs(time_diff_min) <= 15:
        if pd.notna(first_rsi30_price) and first_rsi30_price > 0:
            drop_pct = (first_rsi30_price - low_price) / first_rsi30_price * 100
            if drop_pct <= 1.0:
                return "true_bottom"

    # fake_bottom: RSI<30が最安値の30分以上前 & 最安値が2%以上下
    if time_diff_min > 30:
        if pd.notna(first_rsi30_price) and first_rsi30_price > 0:
            drop_pct = (first_rsi30_price - low_price) / first_rsi30_price * 100
            if drop_pct >= 2.0:
                return "fake_bottom"

    # gradual_decline: RSI<35が60分以上継続 & 最安値が13:30以降
    rsi35_mask = (rsi < 35) & rsi.notna()
    rsi35_indices = sub.index[rsi35_mask]
    if len(rsi35_indices) >= 2:
        first_t = sub.loc[rsi35_indices[0], "datetime"]
        last_t = sub.loc[rsi35_indices[-1], "datetime"]
        duration_min = (last_t - first_t).total_seconds() / 60.0
        low_hour = daily_low_time.hour * 100 + daily_low_time.minute
        if duration_min >= 60 and low_hour >= 1330:
            return "gradual_decline"

    # どれにも該当しない場合、シグナルはあるがパターン不明
    return "no_signal"


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------
def compute_all_strategies(archive: pd.DataFrame, df_5m: pd.DataFrame) -> pd.DataFrame:
    """全行 × 7戦略の P&L を計算して DataFrame を返す"""
    results = []
    matched = 0
    total = len(archive)

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
            "rsi9": row.get("rsi9", np.nan),
            "atr14_pct": row.get("atr14_pct", np.nan),
        }
        # セグメント列をコピー（ヒートマップ用）
        for seg_col in SEG_COLS:
            rec[seg_col] = row.get(seg_col, np.nan)

        # 戦略1: セグメント
        s1 = strategy_1_seg_best(row)
        rec["s1_pnl"] = s1["pnl"]
        rec["s1_time"] = s1["exit_time"]

        # 戦略7: 大引け
        s7 = strategy_7_close(row)
        rec["s7_pnl"] = s7["pnl"]
        rec["s7_price"] = s7["exit_price"]

        # 5分足が必要な戦略 (2〜6)
        sub, split_ratio = _extract_intraday(df_5m, ticker, date_str, buy_price)
        rec["split_ratio"] = split_ratio
        if sub is not None:
            matched += 1
            s2 = strategy_2_rsi_exit(sub, buy_price)
            s3 = strategy_3_macd_gc(sub, buy_price)
            s4 = strategy_4_macd_zero(sub, buy_price)
            s5 = strategy_5_granville(sub, buy_price)
            s6 = strategy_6_ma_crossover(sub, buy_price)
        else:
            s2 = s3 = s4 = s5 = s6 = {"pnl": np.nan, "exit_time": None, "exit_price": np.nan}

        rec["s2_pnl"] = s2["pnl"]
        rec["s2_time"] = s2["exit_time"]
        rec["s3_pnl"] = s3["pnl"]
        rec["s3_time"] = s3["exit_time"]
        rec["s4_pnl"] = s4["pnl"]
        rec["s4_time"] = s4["exit_time"]
        rec["s5_pnl"] = s5["pnl"]
        rec["s5_time"] = s5["exit_time"]
        rec["s6_pnl"] = s6["pnl"]
        rec["s6_time"] = s6["exit_time"]

        results.append(rec)

        if (idx + 1) % 100 == 0:
            print(f"  処理中: {idx + 1}/{total}")

    match_rate = matched / total * 100 if total > 0 else 0
    rdf = pd.DataFrame(results)
    split_count = (rdf["split_ratio"] > 1.0).sum() if "split_ratio" in rdf.columns else 0
    print(f"  完了: {total}行, 5分足マッチ: {matched}/{total} ({match_rate:.1f}%)")
    print(f"  分割補正: {split_count}件")

    return rdf, match_rate


# ---------------------------------------------------------------------------
# 集計関数
# ---------------------------------------------------------------------------
def _summary_for_strategy(df: pd.DataFrame, col: str) -> dict:
    valid = df[col].dropna()
    if len(valid) == 0:
        return {"total_pnl": 0, "count": 0, "win_rate": 0, "avg": 0, "max_win": 0, "max_loss": 0, "signal_rate": 0}
    wins = (valid > 0).sum()
    return {
        "total_pnl": valid.sum(),
        "count": len(valid),
        "win_rate": wins / len(valid) * 100,
        "avg": valid.mean(),
        "max_win": valid.max(),
        "max_loss": valid.min(),
        "signal_rate": len(valid) / len(df) * 100,
    }


def _band_label(value: float, bands: list) -> str:
    for lo, hi, label in bands:
        if lo <= value < hi:
            return label
    return "N/A"


# ---------------------------------------------------------------------------
# HTML 生成
# ---------------------------------------------------------------------------
def _css() -> str:
    return """
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #e6edf3; padding: 24px; line-height: 1.5; }
.container { max-width: 1600px; margin: 0 auto; }
h1 { font-size: 24px; font-weight: 600; margin-bottom: 8px; }
h2 { font-size: 18px; font-weight: 600; margin: 32px 0 12px; border-bottom: 1px solid #30363d; padding-bottom: 8px; }
h3 { font-size: 15px; font-weight: 600; margin: 24px 0 12px; }
.subtitle { color: #7d8590; font-size: 13px; margin-bottom: 24px; }
.meta-grid { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }
.meta-item { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px 20px; }
.meta-value { font-size: 22px; font-weight: 700; }
.meta-label { font-size: 11px; color: #7d8590; margin-top: 2px; }

table { width: 100%; border-collapse: collapse; font-size: 13px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; overflow: hidden; margin-bottom: 16px; }
th { text-align: left; padding: 10px 12px; background: #21262d; border-bottom: 1px solid #30363d; color: #7d8590; font-weight: 500; font-size: 12px; white-space: nowrap; }
th.r { text-align: right; }
td { padding: 10px 12px; border-bottom: 1px solid #21262d; }
td.r { text-align: right; font-variant-numeric: tabular-nums; }
tr:hover { background: #1c2128; }

.pos { color: #3fb950; }
.neg { color: #f85149; }
.muted { color: #7d8590; }
.bold { font-weight: 700; }
.best { background: #1a3a2a !important; }
.worst { background: #3d1f1f !important; }

.heatmap-table td { text-align: center; font-weight: 600; padding: 8px; font-size: 13px; }
.heatmap-table th { text-align: center; }

.chip { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: 500; }
.chip-green { background: #238636; color: #fff; }
.chip-red { background: #da3633; color: #fff; }
.chip-gray { background: #30363d; color: #7d8590; }

.generated { font-size: 11px; color: #7d8590; margin-top: 24px; padding-top: 16px; border-top: 1px solid #30363d; }
"""


def _pnl_class(v: float) -> str:
    if pd.isna(v):
        return "muted"
    return "pos" if v > 0 else "neg" if v < 0 else ""


def _pnl_str(v: float) -> str:
    if pd.isna(v):
        return "-"
    return f"{v:+,.0f}"


def _pct_str(v: float) -> str:
    if pd.isna(v) or v == 0:
        return "-"
    return f"{v:.1f}%"


def _heatmap_bg(v: float, max_abs: float) -> str:
    if pd.isna(v) or max_abs == 0:
        return ""
    intensity = min(abs(v) / max_abs, 1.0) * 0.6
    if v > 0:
        return f"background: rgba(63,185,80,{intensity:.2f});"
    elif v < 0:
        return f"background: rgba(248,81,73,{intensity:.2f});"
    return ""


def generate_html(
    result_df: pd.DataFrame, match_rate: float, output_path: Path,
    rsi_df: Optional[pd.DataFrame] = None,
) -> None:
    """分析結果からHTML生成"""
    total = len(result_df)
    date_min = result_df["date"].min()
    date_max = result_df["date"].max()

    # --- サマリー集計 ---
    strategy_cols = {i: f"s{i}_pnl" for i in range(1, 8)}
    summaries = {}
    for sid, col in strategy_cols.items():
        summaries[sid] = _summary_for_strategy(result_df, col)

    # ベスト戦略
    best_sid = max(summaries, key=lambda s: summaries[s]["total_pnl"])

    # --- HTML 組み立て ---
    h = []
    h.append(f"""<!DOCTYPE html>
<html lang="ja"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>イグジット戦略バックテスト分析</title>
<style>{_css()}</style>
</head><body>
<div class="container">
<h1>イグジット戦略バックテスト分析</h1>
<p class="subtitle">エントリー固定（寄付ショート）に対する7つの出口戦略を過去データで比較検証</p>
""")

    # ヘッダーメタ
    h.append('<div class="meta-grid">')
    h.append(f'<div class="meta-item"><div class="meta-value">{total}</div><div class="meta-label">総トレード数</div></div>')
    h.append(f'<div class="meta-item"><div class="meta-value">{date_min} ~ {date_max}</div><div class="meta-label">検証期間</div></div>')
    h.append(f'<div class="meta-item"><div class="meta-value">{match_rate:.1f}%</div><div class="meta-label">5分足マッチ率</div></div>')
    best_pnl = summaries[best_sid]["total_pnl"]
    h.append(f'<div class="meta-item"><div class="meta-value {_pnl_class(best_pnl)}">{STRATEGY_NAMES[best_sid]}</div><div class="meta-label">最良戦略</div></div>')
    h.append("</div>")

    # --- セクション1: サマリーテーブル ---
    h.append("<h2>1. 戦略別サマリー</h2>")
    h.append("""<table>
<tr><th>#</th><th>戦略</th><th class="r">合計P&L (円)</th><th class="r">シグナル率</th><th class="r">勝率</th><th class="r">平均損益</th><th class="r">最大利益</th><th class="r">最大損失</th></tr>
""")
    for sid in range(1, 8):
        s = summaries[sid]
        row_cls = " class=\"best\"" if sid == best_sid else ""
        h.append(f"""<tr{row_cls}>
<td>{sid}</td>
<td><span class="bold">{STRATEGY_NAMES[sid]}</span></td>
<td class="r {_pnl_class(s['total_pnl'])}">{_pnl_str(s['total_pnl'])}</td>
<td class="r">{_pct_str(s['signal_rate'])}</td>
<td class="r">{_pct_str(s['win_rate'])}</td>
<td class="r {_pnl_class(s['avg'])}">{_pnl_str(s['avg'])}</td>
<td class="r pos">{_pnl_str(s['max_win'])}</td>
<td class="r neg">{_pnl_str(s['max_loss'])}</td>
</tr>
""")
    h.append("</table>")

    # --- セクション2: 時間帯別ヒートマップ ---
    h.append("<h2>2. 時間帯 x 曜日 P&L ヒートマップ</h2>")
    h.append('<table class="heatmap-table"><tr><th></th>')
    for wd in range(5):
        h.append(f"<th>{WEEKDAY_NAMES[wd]}</th>")
    h.append("<th>全体</th></tr>")

    # 時間帯ごとの平均P&Lを計算（seg列はロング視点なので符号反転してショートP&Lにする）
    h.append('<p class="muted" style="margin-bottom:8px;">※ ショート視点の平均P&L（円/100株）</p>')
    heatmap_data = {}
    for col in SEG_COLS:
        time_label = col.replace("seg_", "")
        heatmap_data[time_label] = {}
        for wd in range(5):
            subset = result_df[result_df["weekday"] == wd][col].dropna()
            heatmap_data[time_label][wd] = -subset.mean() if len(subset) > 0 else np.nan
        all_vals = result_df[col].dropna()
        heatmap_data[time_label]["all"] = -all_vals.mean() if len(all_vals) > 0 else np.nan

    # ヒートマップの色スケール用最大値
    all_hm_vals = []
    for tl in heatmap_data:
        for k, v in heatmap_data[tl].items():
            if pd.notna(v):
                all_hm_vals.append(abs(v))
    hm_max = max(all_hm_vals) if all_hm_vals else 1

    for time_label, row_data in heatmap_data.items():
        h.append(f"<tr><th>{time_label[:2]}:{time_label[2:]}</th>")
        for wd in range(5):
            v = row_data[wd]
            style = _heatmap_bg(v, hm_max)
            h.append(f'<td style="{style}" class="{_pnl_class(v)}">{_pnl_str(v)}</td>')
        v_all = row_data["all"]
        style = _heatmap_bg(v_all, hm_max)
        h.append(f'<td style="{style}" class="{_pnl_class(v_all)} bold">{_pnl_str(v_all)}</td>')
        h.append("</tr>")
    h.append("</table>")

    # --- セクション3: 曜日別クロス集計 ---
    _cross_section(h, result_df, "3. 戦略 x 曜日", "weekday",
                   lambda r: int(r["weekday"]) if pd.notna(r.get("weekday")) and 0 <= r.get("weekday", -1) <= 4 else -1,
                   {wd: WEEKDAY_NAMES[wd] for wd in range(5)})

    # --- セクション4: RSI帯別 ---
    def rsi_bander(r):
        v = r.get("rsi9", np.nan)
        if pd.isna(v):
            return "N/A"
        return _band_label(v, RSI_BANDS)

    _cross_section(h, result_df, "4. 戦略 x RSI帯", "rsi_band", rsi_bander,
                   {b[2]: b[2] for b in RSI_BANDS})

    # --- セクション5: ATR帯別 ---
    def atr_bander(r):
        v = r.get("atr14_pct", np.nan)
        if pd.isna(v):
            return "N/A"
        return _band_label(v, ATR_BANDS)

    _cross_section(h, result_df, "5. 戦略 x ATR帯", "atr_band", atr_bander,
                   {b[2]: b[2] for b in ATR_BANDS})

    # --- セクション6: 価格帯別 ---
    def price_bander(r):
        v = r.get("buy_price", np.nan)
        if pd.isna(v):
            return "N/A"
        return _band_label(v, PRICE_BANDS)

    _cross_section(h, result_df, "6. 戦略 x 価格帯", "price_band", price_bander,
                   {b[2]: b[2] for b in PRICE_BANDS})

    # --- セクション7: Top20 個別トレード ---
    _top_trades_section(h, result_df)

    # --- RSI × 日中最安値 分析セクション ---
    if rsi_df is not None and len(rsi_df) > 0:
        _rsi_low_sections(h, result_df, rsi_df)

    # フッター
    h.append(f'<p class="generated">Generated at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | '
             f'データ: {total}行 | 5分足マッチ率: {match_rate:.1f}%</p>')
    h.append("</div></body></html>")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("".join(h), encoding="utf-8")
    print(f"✅ HTML生成完了: {output_path}")


def _cross_section(h: list, df: pd.DataFrame, title: str, group_key: str,
                   bander, label_map: dict) -> None:
    """戦略 × カテゴリのクロス集計テーブル"""
    h.append(f"<h2>{title}</h2>")

    # グループラベルを付与
    df = df.copy()
    df["_group"] = df.apply(bander, axis=1)

    ordered_labels = list(label_map.keys())

    # 合計P&Lテーブル
    h.append("<h3>合計P&L (円)</h3>")
    h.append("<table><tr><th>戦略</th>")
    for lbl in ordered_labels:
        h.append(f"<th class=\"r\">{label_map[lbl]}</th>")
    h.append("</tr>")

    for sid in range(1, 8):
        col = f"s{sid}_pnl"
        h.append(f"<tr><td>{STRATEGY_NAMES[sid]}</td>")
        for lbl in ordered_labels:
            subset = df[df["_group"] == lbl][col].dropna()
            v = subset.sum() if len(subset) > 0 else np.nan
            h.append(f'<td class="r {_pnl_class(v)}">{_pnl_str(v)}</td>')
        h.append("</tr>")
    h.append("</table>")

    # 勝率テーブル
    h.append("<h3>勝率</h3>")
    h.append("<table><tr><th>戦略</th>")
    for lbl in ordered_labels:
        h.append(f"<th class=\"r\">{label_map[lbl]}</th>")
    h.append("</tr>")

    for sid in range(1, 8):
        col = f"s{sid}_pnl"
        h.append(f"<tr><td>{STRATEGY_NAMES[sid]}</td>")
        for lbl in ordered_labels:
            subset = df[df["_group"] == lbl][col].dropna()
            if len(subset) > 0:
                wr = (subset > 0).sum() / len(subset) * 100
                cls = "pos" if wr >= 55 else "neg" if wr < 45 else ""
                h.append(f'<td class="r {cls}">{wr:.1f}% ({len(subset)})</td>')
            else:
                h.append('<td class="r muted">-</td>')
        h.append("</tr>")
    h.append("</table>")


def _top_trades_section(h: list, df: pd.DataFrame) -> None:
    """最も利益が出たトレードTop20"""
    h.append("<h2>7. 個別トレード詳細 Top20</h2>")
    h.append("<p class=\"subtitle\">戦略7（大引け）基準で利益が大きいトレードTop20。各戦略での出口結果を比較。</p>")

    # 戦略7 で並べる
    top = df.nlargest(20, "s7_pnl")

    h.append("""<table>
<tr><th>日付</th><th>銘柄</th><th class="r">買価格</th>""")
    for sid in range(1, 8):
        h.append(f'<th class="r">S{sid} P&L</th><th>時刻</th>')
    h.append("</tr>")

    for _, row in top.iterrows():
        wd = int(row["weekday"]) if pd.notna(row.get("weekday")) and 0 <= row.get("weekday", -1) <= 6 else -1
        wd_str = WEEKDAY_NAMES[wd] if 0 <= wd <= 6 else "?"
        h.append(f'<tr><td>{row["date"]} ({wd_str})</td>')
        h.append(f'<td><span class="bold">{row["ticker"]}</span><br><span class="muted">{row.get("stock_name", "")}</span></td>')
        h.append(f'<td class="r">{row["buy_price"]:,.0f}</td>')

        # 各戦略のP&L と時刻
        pnls = {}
        for sid in range(1, 8):
            pnl_val = row.get(f"s{sid}_pnl", np.nan)
            pnls[sid] = pnl_val
        best_s = max(pnls, key=lambda s: pnls[s] if pd.notna(pnls[s]) else -1e18)

        for sid in range(1, 8):
            pnl = pnls[sid]
            time_val = row.get(f"s{sid}_time", "")
            time_str = time_val if time_val else "-"
            bold = " bold" if sid == best_s else ""
            h.append(f'<td class="r {_pnl_class(pnl)}{bold}">{_pnl_str(pnl)}</td>')
            h.append(f'<td class="muted">{time_str}</td>')
        h.append("</tr>")

    h.append("</table>")


# ---------------------------------------------------------------------------
# RSI × 日中最安値 HTMLセクション
# ---------------------------------------------------------------------------
def _rsi_low_sections(h: list, result_df: pd.DataFrame, rsi_df: pd.DataFrame) -> None:
    """5つの RSI × 日中最安値 分析セクションを追加"""
    # result_df に rsi_df をマージ
    merged = result_df.merge(rsi_df, on=["ticker", "date"], how="left")

    _section_rsi_distribution(h, merged)
    _section_pattern_summary(h, merged)
    _section_low_time_heatmap(h, merged)
    _section_rsi_threshold_optimization(h, merged)
    _section_fake_bottom_case_study(h, merged)


def _section_rsi_distribution(h: list, df: pd.DataFrame) -> None:
    """セクション8: 最安値時点のRSI分布"""
    h.append("<h2>8. 最安値時点の RSI 分布</h2>")
    h.append('<p class="subtitle">日中最安値をつけた足の RSI(9) をバケット別に集計。S7（大引け）基準の平均P&L。</p>')

    valid = df[df["rsi_at_daily_low"].notna()].copy()
    if len(valid) == 0:
        h.append('<p class="muted">データなし</p>')
        return

    def _bucket_label(v: float) -> str:
        for lo, hi in RSI_LOW_BUCKETS:
            if lo <= v < hi:
                return f"{lo}-{hi}" if hi < 100 else f"{lo}+"
        return "N/A"

    valid["rsi_bucket"] = valid["rsi_at_daily_low"].apply(_bucket_label)

    h.append("""<table>
<tr><th>RSI帯</th><th class="r">件数</th><th class="r">平均P&L</th><th class="r">合計P&L</th><th class="r">勝率</th></tr>
""")
    for lo, hi in RSI_LOW_BUCKETS:
        lbl = f"{lo}-{hi}" if hi < 100 else f"{lo}+"
        subset = valid[valid["rsi_bucket"] == lbl]
        if len(subset) == 0:
            h.append(f'<tr><td>{lbl}</td><td class="r muted">0</td>'
                     f'<td class="r muted">-</td><td class="r muted">-</td><td class="r muted">-</td></tr>')
            continue
        s7 = subset["s7_pnl"].dropna()
        total_pnl = s7.sum()
        avg_pnl = s7.mean() if len(s7) > 0 else np.nan
        wr = (s7 > 0).sum() / len(s7) * 100 if len(s7) > 0 else 0
        h.append(f'<tr><td>{lbl}</td>'
                 f'<td class="r">{len(subset)}</td>'
                 f'<td class="r {_pnl_class(avg_pnl)}">{_pnl_str(avg_pnl)}</td>'
                 f'<td class="r {_pnl_class(total_pnl)}">{_pnl_str(total_pnl)}</td>'
                 f'<td class="r">{wr:.1f}%</td></tr>')
    h.append("</table>")


def _section_pattern_summary(h: list, df: pd.DataFrame) -> None:
    """セクション9: パターン別集計"""
    h.append("<h2>9. RSI パターン別集計</h2>")
    h.append('<p class="subtitle">true_bottom / fake_bottom / gradual_decline / no_signal の分類別集計。S7（大引け）基準。</p>')

    patterns = ["true_bottom", "fake_bottom", "gradual_decline", "no_signal"]
    pattern_labels = {
        "true_bottom": "真の底 (True Bottom)",
        "fake_bottom": "偽底 (Fake Bottom)",
        "gradual_decline": "緩やかな下落 (Gradual Decline)",
        "no_signal": "シグナルなし (No Signal)",
    }

    h.append("""<table>
<tr><th>パターン</th><th class="r">件数</th><th class="r">勝率</th><th class="r">合計P&L</th><th class="r">平均P&L</th><th class="r">S2合計P&L</th><th class="r">S2勝率</th></tr>
""")
    for pat in patterns:
        subset = df[df["pattern"] == pat]
        cnt = len(subset)
        if cnt == 0:
            h.append(f'<tr><td>{pattern_labels.get(pat, pat)}</td>'
                     f'<td class="r muted">0</td>'
                     + '<td class="r muted">-</td>' * 5 + '</tr>')
            continue
        s7 = subset["s7_pnl"].dropna()
        s2 = subset["s2_pnl"].dropna()
        s7_total = s7.sum()
        s7_wr = (s7 > 0).sum() / len(s7) * 100 if len(s7) > 0 else 0
        s7_avg = s7.mean() if len(s7) > 0 else np.nan
        s2_total = s2.sum()
        s2_wr = (s2 > 0).sum() / len(s2) * 100 if len(s2) > 0 else 0
        h.append(f'<tr><td>{pattern_labels.get(pat, pat)}</td>'
                 f'<td class="r">{cnt}</td>'
                 f'<td class="r">{s7_wr:.1f}%</td>'
                 f'<td class="r {_pnl_class(s7_total)}">{_pnl_str(s7_total)}</td>'
                 f'<td class="r {_pnl_class(s7_avg)}">{_pnl_str(s7_avg)}</td>'
                 f'<td class="r {_pnl_class(s2_total)}">{_pnl_str(s2_total)}</td>'
                 f'<td class="r">{s2_wr:.1f}%</td></tr>')

    # no_data 行（5分足なし）
    no_data = df[df["pattern"] == "no_data"]
    if len(no_data) > 0:
        h.append(f'<tr><td class="muted">データなし</td>'
                 f'<td class="r muted">{len(no_data)}</td>'
                 + '<td class="r muted">-</td>' * 5 + '</tr>')
    h.append("</table>")


def _section_low_time_heatmap(h: list, df: pd.DataFrame) -> None:
    """セクション10: 最安値時刻ヒートマップ（30分帯 × 曜日）"""
    h.append("<h2>10. 最安値時刻ヒートマップ</h2>")
    h.append('<p class="subtitle">日中最安値を付けた時刻帯（30分刻み）× 曜日。件数をカウント。</p>')

    valid = df[df["daily_low_time"].notna()].copy()
    if len(valid) == 0:
        h.append('<p class="muted">データなし</p>')
        return

    # 30分帯ラベル
    time_slots = [
        "09:00", "09:30", "10:00", "10:30", "11:00", "11:30",
        "12:30", "13:00", "13:30", "14:00", "14:30", "15:00",
    ]

    def _time_slot(dt) -> str:
        if pd.isna(dt):
            return "N/A"
        hm = dt.hour * 60 + dt.minute
        for i, slot in enumerate(time_slots):
            sh, sm = int(slot[:2]), int(slot[3:])
            slot_min = sh * 60 + sm
            next_min = slot_min + 30
            if i + 1 < len(time_slots):
                nsh, nsm = int(time_slots[i + 1][:2]), int(time_slots[i + 1][3:])
                next_min = nsh * 60 + nsm
            else:
                next_min = 15 * 60 + 30  # 15:30
            if slot_min <= hm < next_min:
                return slot
        return "15:00"

    valid["time_slot"] = valid["daily_low_time"].apply(_time_slot)

    h.append('<table class="heatmap-table"><tr><th>時刻帯</th>')
    for wd in range(5):
        h.append(f"<th>{WEEKDAY_NAMES[wd]}</th>")
    h.append("<th>合計</th></tr>")

    # 全体最大値でヒートマップの色を決定
    max_count = 1
    for slot in time_slots:
        for wd in range(5):
            cnt = len(valid[(valid["time_slot"] == slot) & (valid["weekday"] == wd)])
            if cnt > max_count:
                max_count = cnt

    for slot in time_slots:
        h.append(f"<tr><th>{slot}</th>")
        total_slot = 0
        for wd in range(5):
            cnt = len(valid[(valid["time_slot"] == slot) & (valid["weekday"] == wd)])
            total_slot += cnt
            intensity = min(cnt / max_count, 1.0) * 0.7 if max_count > 0 else 0
            bg = f"background: rgba(56,139,253,{intensity:.2f});" if cnt > 0 else ""
            h.append(f'<td style="{bg}">{cnt if cnt > 0 else "-"}</td>')
        h.append(f'<td class="bold">{total_slot}</td></tr>')
    h.append("</table>")


def _section_rsi_threshold_optimization(h: list, df: pd.DataFrame) -> None:
    """セクション11: RSI閾値最適化"""
    h.append("<h2>11. RSI 閾値最適化</h2>")
    h.append('<p class="subtitle">RSI(9) の利確閾値を変えた場合の P&L・勝率比較。'
             '最安値時RSIがその閾値未満のトレードのみ対象。S7（大引け）P&L基準。</p>')

    thresholds = [10, 15, 20, 25, 30, 35]
    valid = df[df["rsi_at_daily_low"].notna()].copy()
    if len(valid) == 0:
        h.append('<p class="muted">データなし</p>')
        return

    h.append("""<table>
<tr><th>閾値</th><th class="r">該当件数</th><th class="r">全体比率</th><th class="r">S7合計P&L</th><th class="r">S7平均P&L</th><th class="r">S7勝率</th><th class="r">S2合計P&L</th><th class="r">S2勝率</th></tr>
""")
    for th in thresholds:
        subset = valid[valid["rsi_at_daily_low"] < th]
        cnt = len(subset)
        ratio = cnt / len(valid) * 100 if len(valid) > 0 else 0
        if cnt == 0:
            h.append(f'<tr><td>RSI &lt; {th}</td><td class="r">0</td>'
                     f'<td class="r">{ratio:.1f}%</td>'
                     + '<td class="r muted">-</td>' * 5 + '</tr>')
            continue
        s7 = subset["s7_pnl"].dropna()
        s2 = subset["s2_pnl"].dropna()
        s7_total = s7.sum()
        s7_avg = s7.mean() if len(s7) > 0 else np.nan
        s7_wr = (s7 > 0).sum() / len(s7) * 100 if len(s7) > 0 else 0
        s2_total = s2.sum()
        s2_wr = (s2 > 0).sum() / len(s2) * 100 if len(s2) > 0 else 0
        h.append(f'<tr><td>RSI &lt; {th}</td>'
                 f'<td class="r">{cnt}</td>'
                 f'<td class="r">{ratio:.1f}%</td>'
                 f'<td class="r {_pnl_class(s7_total)}">{_pnl_str(s7_total)}</td>'
                 f'<td class="r {_pnl_class(s7_avg)}">{_pnl_str(s7_avg)}</td>'
                 f'<td class="r">{s7_wr:.1f}%</td>'
                 f'<td class="r {_pnl_class(s2_total)}">{_pnl_str(s2_total)}</td>'
                 f'<td class="r">{s2_wr:.1f}%</td></tr>')
    h.append("</table>")


def _section_fake_bottom_case_study(h: list, df: pd.DataFrame) -> None:
    """セクション12: 偽底ケーススタディ — RSI<30で早抜けした損失Top10"""
    h.append("<h2>12. 偽底ケーススタディ — RSI&lt;30 早抜け損失 Top10</h2>")
    h.append('<p class="subtitle">S2（RSI&lt;30利確）で負けた中で損失が大きいTop10。'
             '最安値時刻・RSIとの乖離を確認。</p>')

    # S2でシグナルが出た（s2_pnlがNaNでない）かつ損失のもの
    valid = df[(df["s2_pnl"].notna()) & (df["s2_pnl"] < 0)].copy()
    if len(valid) == 0:
        h.append('<p class="muted">該当なし</p>')
        return

    top10 = valid.nsmallest(10, "s2_pnl")

    h.append("""<table>
<tr><th>日付</th><th>銘柄</th><th class="r">買価格</th><th class="r">S2 P&L</th><th>S2時刻</th>
<th class="r">S7 P&L</th><th>最安値時刻</th><th class="r">最安値RSI</th>
<th class="r">RSI最小</th><th>パターン</th></tr>
""")
    for _, row in top10.iterrows():
        wd = int(row["weekday"]) if pd.notna(row.get("weekday")) and 0 <= row.get("weekday", -1) <= 6 else -1
        wd_str = WEEKDAY_NAMES[wd] if 0 <= wd <= 6 else "?"

        low_time_str = "-"
        if pd.notna(row.get("daily_low_time")):
            try:
                low_time_str = row["daily_low_time"].strftime("%H:%M")
            except Exception:
                low_time_str = str(row["daily_low_time"])[-5:]

        rsi_low = row.get("rsi_at_daily_low", np.nan)
        rsi_low_str = f"{rsi_low:.1f}" if pd.notna(rsi_low) else "-"
        rsi_min = row.get("rsi_min_value", np.nan)
        rsi_min_str = f"{rsi_min:.1f}" if pd.notna(rsi_min) else "-"
        pattern = row.get("pattern", "-")

        pat_cls = ""
        if pattern == "fake_bottom":
            pat_cls = "neg"
        elif pattern == "true_bottom":
            pat_cls = "pos"

        h.append(f'<tr><td>{row["date"]} ({wd_str})</td>')
        h.append(f'<td><span class="bold">{row["ticker"]}</span><br>'
                 f'<span class="muted">{row.get("stock_name", "")}</span></td>')
        h.append(f'<td class="r">{row["buy_price"]:,.0f}</td>')
        h.append(f'<td class="r neg">{_pnl_str(row["s2_pnl"])}</td>')
        h.append(f'<td class="muted">{row.get("s2_time", "-")}</td>')
        s7_pnl = row.get("s7_pnl", np.nan)
        h.append(f'<td class="r {_pnl_class(s7_pnl)}">{_pnl_str(s7_pnl)}</td>')
        h.append(f'<td class="muted">{low_time_str}</td>')
        h.append(f'<td class="r">{rsi_low_str}</td>')
        h.append(f'<td class="r">{rsi_min_str}</td>')
        h.append(f'<td class="{pat_cls}">{pattern}</td></tr>')

    h.append("</table>")


# ---------------------------------------------------------------------------
# エントリーポイント
# ---------------------------------------------------------------------------
def main() -> None:
    print("=== イグジット戦略バックテスト分析 ===")

    print("\n[1/5] データ読み込み")
    archive = load_archive()
    df_5m = load_5m()
    # 日足は現時点では未使用（将来の日足レベル MACD/RSI 参照用に残す）
    # daily = load_daily()

    print("\n[2/5] 出口戦略 P&L 計算")
    result_df, match_rate = compute_all_strategies(archive, df_5m)

    print("\n[3/5] RSI × 日中最安値 分析")
    rsi_df = analyze_rsi_at_daily_low(archive, df_5m)

    print("\n[4/5] 集計・HTML生成")
    output_path = _DASH_DIR / "improvement" / "output" / "exit_strategy_analysis.html"
    generate_html(result_df, match_rate, output_path, rsi_df=rsi_df)

    print("\n[5/5] 完了")
    # 簡易サマリー表示
    for sid in range(1, 8):
        col = f"s{sid}_pnl"
        valid = result_df[col].dropna()
        total_pnl = valid.sum()
        wr = (valid > 0).sum() / len(valid) * 100 if len(valid) > 0 else 0
        print(f"  S{sid} {STRATEGY_NAMES[sid]:20s}: {total_pnl:>+12,.0f}円  勝率{wr:5.1f}%  ({len(valid)}件)")

    # RSIパターン分布
    if rsi_df is not None:
        print("\n  RSIパターン分布:")
        for pat, cnt in rsi_df["pattern"].value_counts().items():
            print(f"    {pat}: {cnt}件")


if __name__ == "__main__":
    main()
