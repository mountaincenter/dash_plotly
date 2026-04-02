#!/usr/bin/env python3
"""
update_cme_close.py
====================
07:00パイプライン用。CME日足終値（06:00確定）を取得してS3にアップ。
22:00時点の暫定判定と比較し、変更があればフラグを立てる。

出力:
  /tmp/cme_update.json — 判定変更の有無
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR


def main():
    print("=" * 50)
    print("CME Close Update (07:00 pipeline)")
    print("=" * 50)

    # CME最新値取得
    try:
        import yfinance as yf
        nkd = yf.download("NKD=F", period="5d", interval="1d", progress=False)
        n225 = yf.download("^N225", period="5d", interval="1d", progress=False)
        if isinstance(nkd.columns, pd.MultiIndex):
            nkd.columns = [c[0] for c in nkd.columns]
        if isinstance(n225.columns, pd.MultiIndex):
            n225.columns = [c[0] for c in n225.columns]
    except Exception as e:
        print(f"[ERROR] yfinance download failed: {e}")
        return 1

    if nkd.empty or n225.empty:
        print("[WARN] No data from yfinance")
        return 1

    nkd_close = float(nkd["Close"].iloc[-1])
    n225_close = float(n225["Close"].iloc[-1])
    cme_gap = round((nkd_close - n225_close) / n225_close * 100, 2)
    nkd_date = nkd.index[-1].strftime("%Y-%m-%d")

    print(f"  N225: ¥{n225_close:,.0f}")
    print(f"  NKD(CME): ¥{nkd_close:,.0f} ({nkd_date})")
    print(f"  Gap: {cme_gap:+.2f}%")

    # N225変化率
    n225_chg = None
    if len(n225) >= 2:
        n225_chg = round((float(n225["Close"].iloc[-1]) / float(n225["Close"].iloc[-2]) - 1) * 100, 2)
        print(f"  N225 chg: {n225_chg:+.2f}%")

    # 日経VI
    vi = None
    vi_path = PARQUET_DIR / "nikkei_vi_max_1d.parquet"
    if vi_path.exists():
        try:
            vi_df = pd.read_parquet(vi_path)
            vi_df["date"] = pd.to_datetime(vi_df["date"])
            vi = float(vi_df.sort_values("date").iloc[-1]["close"])
            print(f"  VI: {vi:.1f}")
        except Exception:
            pass

    # 除外ルール判定
    excluded = []
    if vi and cme_gap is not None:
        if (vi >= 30) and (vi < 40) and (cme_gap >= -1) and (cme_gap < 1):
            excluded.append("VI30-40×膠着")
        if (vi >= 30) and (vi < 40) and (cme_gap >= 1):
            excluded.append("VI30-40×GU")
    if n225_chg is not None and n225_chg < -3:
        excluded.append("N225<-3%")

    if excluded:
        decision = "excluded"
    elif vi and vi >= 40:
        decision = "strong_entry"
    elif vi and vi >= 30:
        decision = "entry"
    elif vi and vi >= 25:
        decision = "consider"
    else:
        decision = "wait"

    print(f"\n  Decision: {decision}")
    if excluded:
        print(f"  Excluded: {', '.join(excluded)}")

    # 結果保存
    result = {
        "nkd_close": nkd_close,
        "n225_close": n225_close,
        "cme_gap": cme_gap,
        "n225_chg": n225_chg,
        "vi": vi,
        "decision": decision,
        "excluded": excluded,
        "nkd_date": nkd_date,
    }
    Path("/tmp/cme_update.json").write_text(json.dumps(result, ensure_ascii=False))
    print(f"\n[OK] Saved: /tmp/cme_update.json")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
