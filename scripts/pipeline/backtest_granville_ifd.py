#!/usr/bin/env python3
"""
backtest_granville_ifd.py
グランビルIFDロング戦略: 完走済みトレードをアーカイブに追加

毎営業日16:45実行。granville_ifd_signals.parquetからシグナルを取得し、
グランビル出口ルール + TP+10%利確 + 7日マイナス損切り + 翌日寄付決済を適用。
  TP: 高値≥エントリー+10% → その価格で利確（ザラ場自動執行）
  A: 終値≥SMA20 → 翌日寄付売り（SMA20回帰利確）
  B: SMA5がSMA20を下抜け → 翌日寄付売り（デッドクロス撤退）
  7日マイナス損切り: 7営業日目終値 < エントリー価格 → 翌日寄付売り
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
from common_cfg.s3io import upload_file

BACKTEST_DIR = PARQUET_DIR / "backtest"
ARCHIVE_PATH = BACKTEST_DIR / "granville_ifd_archive.parquet"

SL_PCT = 3.0        # SL -3%（IFD逆指値）
TP_PCT = 10.0       # TP +10%（利確）
MAX_HOLD_DAYS = 60  # 最大保有日数（安全弁）
MIN_DAYS_AGO = 1    # シグナル翌日にエントリーするので最低1日必要（未完了はsimulate_tradeがNone返却）

S3_ARCHIVE_KEY = "backtest/granville_ifd_archive.parquet"
COMPARISON_PATH = BACKTEST_DIR / "granville_ifd_comparison.parquet"
S3_COMPARISON_KEY = "backtest/granville_ifd_comparison.parquet"


def regenerate_all_signals(prices_df: pd.DataFrame) -> pd.DataFrame:
    """価格データから全期間のシグナルを再生成（generate_granville_signals.pyと同じロジック）"""
    from scripts.pipeline.generate_granville_signals import detect_signals, BAD_SECTORS

    ps = detect_signals(prices_df)

    # 全日のシグナルを抽出
    sig_mask = ps["sig_A"] | ps["sig_B"]
    sigs = ps[sig_mask].copy()

    # フィルター適用
    sigs = sigs[sigs["market_uptrend"] == True]
    sigs = sigs[sigs["macro_ci_expand"] == True]
    sigs = sigs[~sigs["sectors"].isin(BAD_SECTORS)]
    sigs = sigs[sigs["Close"] < 20000]

    # signal_type列
    sigs["signal_type"] = sigs.apply(
        lambda x: "A+B" if x["sig_A"] and x["sig_B"] else ("A" if x["sig_A"] else "B"), axis=1
    )

    # 出力カラム整形
    result = sigs[["date", "ticker", "stock_name", "sectors", "signal_type",
                    "market_uptrend", "macro_ci_expand"]].copy()
    result = result.rename(columns={
        "date": "signal_date",
        "sectors": "sector",
        "macro_ci_expand": "ci_expand",
    })

    return result



def simulate_trade(prices_df: pd.DataFrame, ticker: str,
                   signal_date: pd.Timestamp, signal_type: str) -> dict | None:
    """1トレードをTP+10% + trail_1/2 SL + グランビル出口ルール + 7日マイナス損切りでシミュレート

    TP: 高値≥エントリー+10% → その価格で利確（ザラ場自動執行）
    trail_1/2: エントリー翌日以降、含み益の半分をSLに引き上げ
    A: 終値≥SMA20 → 翌日寄付売り / B: デッドクロス → 翌日寄付売り
    7日マイナス: d==6で終値 < エントリー → 翌日寄付売り (time_cut)
    共通: SL -3%（IFD逆指値、初期値）/ 最大60営業日
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
    trail_sl = sl_price  # trail_1/2: 含み益の半分をSLに引き上げ
    tp_price = entry_price * (1 + TP_PCT / 100)

    for d in range(MAX_HOLD_DAYS):
        ci = entry_idx + d
        if ci >= len(dates):
            return None  # データ不足、トレード未完了

        # SL判定（trail_sl使用、ザラ場中に自動執行）
        if float(lows[ci]) <= trail_sl:
            return _result(entry_date, dates[ci], entry_price, trail_sl, "SL")

        # TP+10%判定（ザラ場中に到達）
        if float(highs[ci]) >= tp_price:
            return _result(entry_date, dates[ci], entry_price, tp_price, "TP")

        # エントリー日は出口条件チェックしない
        if d == 0:
            continue

        close_val = float(closes[ci])
        sma5_val = float(sma5s[ci])
        sma20_val = float(sma20s[ci])

        # trail_1/2: 含み益があればSLを entry + 含み益/2 に引き上げ
        if close_val > entry_price:
            trail_sl = max(trail_sl, entry_price + (close_val - entry_price) / 2)

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

        # 7日経過マイナスなら翌朝損切り
        if d == 6 and close_val < entry_price:
            if ci + 1 >= len(dates):
                return None
            return _result(entry_date, dates[ci + 1], entry_price,
                           float(opens[ci + 1]), "time_cut")

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

    # 1. 価格データ読み込み + シグナル再生成（generate_granville_signals.pyと同じロジック）
    from scripts.pipeline.generate_granville_signals import load_data
    print("[INFO] Loading price data and regenerating signals...")
    ps = load_data()

    # SMA5/SMA20はload_dataで計算済みだが、simulate_tradeに必要なので確認
    if "sma5" not in ps.columns:
        g = ps.groupby("ticker")
        ps["sma5"] = g["Close"].transform(lambda x: x.rolling(5).mean())
        ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())

    signals = regenerate_all_signals(ps)
    signals["signal_date"] = pd.to_datetime(signals["signal_date"])
    print(f"[INFO] Regenerated {len(signals)} signals from price data")

    # 2. バックテスト対象: 10日以上前のシグナルのみ
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=MIN_DAYS_AGO)
    eligible = signals[signals["signal_date"] <= cutoff].copy()
    print(f"[INFO] Eligible signals (>= {MIN_DAYS_AGO} days ago): {len(eligible)}")

    # 3. 全対象をバックテスト（アーカイブは毎回再生成）
    prices = ps.copy()
    results = []
    for _, sig in eligible.iterrows():
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

    # 6. アーカイブ保存（毎回全件再生成）
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
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

    # 8. 比較データ生成（3戦略 × 3期間）
    try:
        _generate_comparison(merged, prices, cfg)
    except Exception as e:
        print(f"[WARN] Comparison generation failed: {e}")

    return 0


def _simulate_alt(prices_df, ticker, signal_date, signal_type, tp_pct, use_7d):
    """7日引け / 利確なし用のシミュレーション"""
    tk = prices_df[prices_df["ticker"] == ticker].sort_values("date")
    if tk.empty:
        return None
    dates = tk["date"].values
    opens, highs, lows, closes = tk["Open"].values, tk["High"].values, tk["Low"].values, tk["Close"].values
    sma5s, sma20s = tk["sma5"].values, tk["sma20"].values
    date_idx = {d: i for i, d in enumerate(dates)}
    sd = np.datetime64(signal_date)
    if sd not in date_idx:
        return None
    idx = date_idx[sd]
    if idx + 1 >= len(dates):
        return None
    ei = idx + 1
    ep = float(opens[ei])
    if np.isnan(ep) or ep <= 0:
        return None
    ed = pd.Timestamp(dates[ei])
    sl = ep * 0.97
    tp_p = ep * (1 + tp_pct / 100) if tp_pct > 0 else None
    hold_limit = 7 if use_7d else 60

    for d in range(hold_limit):
        ci = ei + d
        if ci >= len(dates):
            return None
        if float(lows[ci]) <= sl:
            return _result(ed, dates[ci], ep, sl, "SL")
        if tp_p and float(highs[ci]) >= tp_p:
            return _result(ed, dates[ci], ep, tp_p, "TP")
        if use_7d:
            if d == hold_limit - 1:
                return _result(ed, dates[ci], ep, float(closes[ci]), "expire")
            continue
        if d == 0:
            continue
        cv = float(closes[ci])
        s5, s20 = float(sma5s[ci]), float(sma20s[ci])
        if signal_type in ("A", "A+B") and cv >= s20:
            if ci + 1 < len(dates):
                return _result(ed, dates[ci + 1], ep, float(opens[ci + 1]), "SMA20_touch")
            return None
        ps5, ps20 = float(sma5s[ci - 1]), float(sma20s[ci - 1])
        if ps5 >= ps20 and s5 < s20:
            if ci + 1 < len(dates):
                return _result(ed, dates[ci + 1], ep, float(opens[ci + 1]), "dead_cross")
            return None
        if d == 59:
            if ci + 1 < len(dates):
                return _result(ed, dates[ci + 1], ep, float(opens[ci + 1]), "expire")
            return None
    return None


def _generate_comparison(archive_df, prices_df, cfg):
    """3戦略の比較統計をparquetに保存"""
    print("\n[INFO] Generating comparison data...")

    archive_df = archive_df.copy()
    for col in ["signal_date", "entry_date", "exit_date"]:
        archive_df[col] = pd.to_datetime(archive_df[col])

    # TP+10%はアーカイブから
    tp10_entries = []
    for _, r in archive_df.iterrows():
        tp10_entries.append({
            "entry_date": r["entry_date"],
            "ret_pct": float(r["ret_pct"]),
            "pnl_yen": int(r["pnl_yen"]),
            "hold_days": (r["exit_date"] - r["entry_date"]).days,
        })
    tp10_df = pd.DataFrame(tp10_entries)

    # 7日引け / 利確なし
    signals = archive_df[["signal_date", "ticker", "signal_type"]].drop_duplicates()
    d7_entries, nolim_entries = [], []

    for _, sig in signals.iterrows():
        r7 = _simulate_alt(prices_df, sig["ticker"], sig["signal_date"], sig["signal_type"], 0, True)
        if r7:
            d7_entries.append({
                "entry_date": pd.Timestamp(r7["entry_date"]),
                "ret_pct": r7["ret_pct"],
                "pnl_yen": r7["pnl_yen"],
                "hold_days": (pd.Timestamp(r7["exit_date"]) - pd.Timestamp(r7["entry_date"])).days,
            })
        rn = _simulate_alt(prices_df, sig["ticker"], sig["signal_date"], sig["signal_type"], 0, False)
        if rn:
            nolim_entries.append({
                "entry_date": pd.Timestamp(rn["entry_date"]),
                "ret_pct": rn["ret_pct"],
                "pnl_yen": rn["pnl_yen"],
                "hold_days": (pd.Timestamp(rn["exit_date"]) - pd.Timestamp(rn["entry_date"])).days,
            })

    d7_df = pd.DataFrame(d7_entries)
    nolim_df = pd.DataFrame(nolim_entries)

    def calc(rdf, label, period_start=None):
        if period_start:
            rdf = rdf[rdf["entry_date"] >= period_start]
        n = len(rdf)
        if n == 0:
            return {"period": "", "label": label, "count": 0, "pnl": 0, "pf": 0.0, "win_rate": 0.0, "avg_hold": 0.0}
        pnl = int(rdf["pnl_yen"].sum())
        wr = round((rdf["ret_pct"] > 0).mean() * 100, 1)
        w = rdf.loc[rdf["ret_pct"] > 0, "pnl_yen"].sum()
        l = abs(rdf.loc[rdf["ret_pct"] <= 0, "pnl_yen"].sum())
        pf = round(w / l, 2) if l > 0 else 999.0
        hd = round(rdf["hold_days"].mean(), 1)
        return {"period": "", "label": label, "count": n, "pnl": pnl, "pf": pf, "win_rate": wr, "avg_hold": hd}

    rows = []
    for period_name, period_start in [("all", None), ("14m", "2025-01-01"), ("2026", "2026-01-01")]:
        for df, lbl in [(tp10_df, "TP+10%"), (d7_df, "7日引け"), (nolim_df, "利確なし")]:
            r = calc(df, lbl, period_start)
            r["period"] = period_name
            rows.append(r)

    comp_df = pd.DataFrame(rows)
    comp_df.to_parquet(COMPARISON_PATH, index=False)
    print(f"[OK] Comparison saved: {COMPARISON_PATH}")

    if cfg and cfg.bucket:
        try:
            upload_file(cfg, COMPARISON_PATH, S3_COMPARISON_KEY)
            print(f"[OK] Uploaded to S3: {S3_COMPARISON_KEY}")
        except Exception as e:
            print(f"[WARN] S3 upload failed: {e}")


def main() -> int:
    try:
        return run_backtest()
    except Exception as e:
        print(f"[ERROR] {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
