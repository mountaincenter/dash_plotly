#!/usr/bin/env python3
"""
backtest_granville_ifd.py
グランビルIFDロング戦略: 完走済みトレードをアーカイブに追加

毎営業日16:45実行。granville_ifd_signals.parquetからシグナルを取得し、
グランビル出口ルール + TP+10%利確 + 翌日寄付決済を適用。
  TP: 高値≥エントリー+10% → その価格で利確（ザラ場自動執行）
  A: 終値≥SMA20 → 翌日寄付売り（SMA20回帰利確）
  B: SMA5がSMA20を下抜け → 翌日寄付売り（デッドクロス撤退）
  共通: SL -3%（IFD逆指値、ザラ場自動執行）/ 最大60営業日
結果を granville_ifd_archive.parquet に append。

パターン: save_backtest_to_archive.py (Grokと同じappend方式)
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file, download_file

BACKTEST_DIR = PARQUET_DIR / "backtest"
SIGNALS_PATH = PARQUET_DIR / "granville_ifd_signals.parquet"
ARCHIVE_PATH = BACKTEST_DIR / "granville_ifd_archive.parquet"

SL_PCT = 3.0        # SL -3%（IFD逆指値）
TP_PCT = 10.0       # TP +10%（利確）
MAX_HOLD_DAYS = 60  # 最大保有日数（安全弁）
MIN_DAYS_AGO = 10   # シグナルから10日以上経過したものを対象

S3_ARCHIVE_KEY = "backtest/granville_ifd_archive.parquet"


def load_signals_from_s3(cfg) -> pd.DataFrame:
    """S3からシグナルファイルを読み込み（本番用）"""
    local_path = PARQUET_DIR / "granville_ifd_signals.parquet"
    if local_path.exists():
        return pd.read_parquet(local_path)

    temp = PARQUET_DIR / "_tmp_granville_signals.parquet"
    if download_file(cfg, "granville_ifd_signals.parquet", temp):
        df = pd.read_parquet(temp)
        temp.unlink(missing_ok=True)
        return df

    return pd.DataFrame()


def load_archive(cfg) -> pd.DataFrame:
    """既存アーカイブを読み込み"""
    # ローカル優先
    if ARCHIVE_PATH.exists():
        return pd.read_parquet(ARCHIVE_PATH)

    # S3フォールバック
    if download_file(cfg, S3_ARCHIVE_KEY, ARCHIVE_PATH):
        return pd.read_parquet(ARCHIVE_PATH)

    return pd.DataFrame()


def simulate_trade(prices_df: pd.DataFrame, ticker: str,
                   signal_date: pd.Timestamp, signal_type: str) -> dict | None:
    """1トレードをTP+10% + グランビル出口ルール + 翌日寄付でシミュレート

    TP: 高値≥エントリー+10% → その価格で利確（ザラ場自動執行）
    A: 終値≥SMA20 → 翌日寄付売り / B: デッドクロス → 翌日寄付売り
    共通: SL -3%（IFD逆指値）/ 最大60営業日
    """
    tk = prices_df[prices_df["ticker"] == ticker].sort_values("date")
    if tk.empty:
        return None

    dates = tk["date"].values
    opens = tk["Open"].values
    highs = tk["High"].values
    lows = tk["Low"].values
    closes = tk["Close"].values
    sma5s = tk["sma5"].values
    sma20s = tk["sma20"].values
    date_idx = {d: i for i, d in enumerate(dates)}

    sd = np.datetime64(signal_date)
    if sd not in date_idx:
        return None

    idx = date_idx[sd]
    if idx + 1 >= len(dates):
        return None

    entry_idx = idx + 1
    entry_price = float(opens[entry_idx])
    if np.isnan(entry_price) or entry_price <= 0:
        return None

    entry_date = pd.Timestamp(dates[entry_idx])
    sl_price = entry_price * (1 - SL_PCT / 100)
    tp_price = entry_price * (1 + TP_PCT / 100)

    for d in range(MAX_HOLD_DAYS):
        ci = entry_idx + d
        if ci >= len(dates):
            return None  # データ不足、トレード未完了

        # SL判定（IFD逆指値、ザラ場中に自動執行）
        if float(lows[ci]) <= sl_price:
            return _result(entry_date, dates[ci], entry_price, sl_price, "SL")

        # TP+10%判定（ザラ場中に到達）
        if float(highs[ci]) >= tp_price:
            return _result(entry_date, dates[ci], entry_price, tp_price, "TP")

        # エントリー日は出口条件チェックしない
        if d == 0:
            continue

        close_val = float(closes[ci])
        sma5_val = float(sma5s[ci])
        sma20_val = float(sma20s[ci])

        # A: 終値≥SMA20 → 翌日寄付売り
        if signal_type in ("A", "A+B") and close_val >= sma20_val:
            if ci + 1 >= len(dates):
                return None
            return _result(entry_date, dates[ci + 1], entry_price,
                           float(opens[ci + 1]), "SMA20_touch")

        # デッドクロス（SMA5がSMA20を上から下に交差）→ 翌日寄付売り
        prev_sma5 = float(sma5s[ci - 1])
        prev_sma20 = float(sma20s[ci - 1])
        if prev_sma5 >= prev_sma20 and sma5_val < sma20_val:
            if ci + 1 >= len(dates):
                return None
            return _result(entry_date, dates[ci + 1], entry_price,
                           float(opens[ci + 1]), "dead_cross")

        # 最大保有日数到達 → 翌日寄付売り
        if d == MAX_HOLD_DAYS - 1:
            if ci + 1 >= len(dates):
                return None
            return _result(entry_date, dates[ci + 1], entry_price,
                           float(opens[ci + 1]), "expire")

    return None


def _result(entry_date, exit_date_raw, entry_price, exit_price, exit_type):
    exit_date = pd.Timestamp(exit_date_raw)
    ret_pct = (exit_price / entry_price - 1) * 100
    pnl_yen = int((exit_price - entry_price) * 100)  # 100株
    return {
        "entry_date": entry_date.strftime("%Y-%m-%d"),
        "exit_date": exit_date.strftime("%Y-%m-%d"),
        "entry_price": round(entry_price, 1),
        "exit_price": round(exit_price, 1),
        "ret_pct": round(ret_pct, 3),
        "pnl_yen": pnl_yen,
        "exit_type": exit_type,
    }


def run_backtest() -> int:
    print("=" * 60)
    print("Backtest Granville IFD Archive")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    cfg = load_s3_config()

    # 1. シグナル読み込み
    signals = load_signals_from_s3(cfg)
    if signals.empty:
        print("[INFO] No signals found, nothing to backtest")
        return 0

    signals["signal_date"] = pd.to_datetime(signals["signal_date"])
    print(f"[INFO] Loaded {len(signals)} signals")

    # 2. 既存アーカイブ読み込み
    archive = load_archive(cfg)
    archived_keys = set()
    if not archive.empty:
        archive["signal_date"] = pd.to_datetime(archive["signal_date"])
        archived_keys = set(zip(
            archive["signal_date"].dt.date,
            archive["ticker"]
        ))
        print(f"[INFO] Existing archive: {len(archive)} records")

    # 3. バックテスト対象のシグナルを抽出
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=MIN_DAYS_AGO)
    eligible = signals[signals["signal_date"] <= cutoff].copy()
    print(f"[INFO] Eligible signals (>= {MIN_DAYS_AGO} days ago): {len(eligible)}")

    # 既にアーカイブ済みを除外
    new_signals = []
    for _, row in eligible.iterrows():
        key = (row["signal_date"].date(), row["ticker"])
        if key not in archived_keys:
            new_signals.append(row)

    if not new_signals:
        print("[INFO] No new signals to backtest")
        return 0

    new_df = pd.DataFrame(new_signals)
    print(f"[INFO] New signals to backtest: {len(new_df)}")

    # 4. 価格データ読み込み + SMA計算
    prices = pd.read_parquet(PARQUET_DIR / "prices_max_1d.parquet")
    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices.sort_values(["ticker", "date"])
    g = prices.groupby("ticker")
    prices["sma5"] = g["Close"].transform(lambda x: x.rolling(5).mean())
    prices["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())

    # 5. 各シグナルをバックテスト
    results = []
    for _, sig in new_df.iterrows():
        trade = simulate_trade(prices, sig["ticker"], sig["signal_date"], sig.get("signal_type", "A"))
        if trade is None:
            continue

        results.append({
            "signal_date": sig["signal_date"].strftime("%Y-%m-%d"),
            "ticker": sig["ticker"],
            "stock_name": sig.get("stock_name", ""),
            "sector": sig.get("sector", ""),
            "signal_type": sig.get("signal_type", ""),
            "market_uptrend": sig.get("market_uptrend", True),
            "ci_expand": sig.get("ci_expand", True),
            **trade,
        })

    if not results:
        print("[INFO] No backtest results generated")
        return 0

    new_results = pd.DataFrame(results)
    print(f"[OK] Backtested {len(new_results)} trades")

    # 統計表示
    wins = new_results["ret_pct"] > 0
    sl_hits = new_results["exit_type"] == "SL"
    total_pnl = new_results["pnl_yen"].sum()
    print(f"  Win rate: {wins.mean() * 100:.1f}%")
    print(f"  SL hit rate: {sl_hits.mean() * 100:.1f}%")
    print(f"  Total PnL: ¥{total_pnl:+,}")

    # 6. アーカイブにappend
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)

    if not archive.empty:
        # 型統一
        for col in ["signal_date", "entry_date", "exit_date"]:
            if col in archive.columns:
                archive[col] = archive[col].astype(str)
            if col in new_results.columns:
                new_results[col] = new_results[col].astype(str)

        merged = pd.concat([archive, new_results], ignore_index=True)
    else:
        merged = new_results

    merged.to_parquet(ARCHIVE_PATH, index=False)
    print(f"[OK] Archive saved: {len(merged)} total records → {ARCHIVE_PATH}")

    # 7. S3アップロード
    if cfg and cfg.bucket:
        try:
            upload_file(cfg, ARCHIVE_PATH, S3_ARCHIVE_KEY)
            print(f"[OK] Uploaded to S3: {S3_ARCHIVE_KEY}")
        except Exception as e:
            print(f"[WARN] S3 upload failed: {e}")

    # 全体統計
    print(f"\n{'=' * 60}")
    print("Archive Summary")
    print(f"  Total records: {len(merged)}")
    if len(merged) > 0:
        m_pnl = merged["pnl_yen"].astype(int)
        m_wins = merged["ret_pct"].astype(float) > 0
        m_losses = merged["ret_pct"].astype(float) <= 0
        w_sum = m_pnl[m_wins].sum()
        l_sum = abs(m_pnl[m_losses].sum())
        pf = round(w_sum / l_sum, 2) if l_sum > 0 else 999
        print(f"  Total PnL: ¥{m_pnl.sum():+,}")
        print(f"  Win rate: {m_wins.mean() * 100:.1f}%")
        print(f"  PF: {pf}")
    print("=" * 60)

    return 0


def main() -> int:
    try:
        return run_backtest()
    except Exception as e:
        print(f"[ERROR] {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
