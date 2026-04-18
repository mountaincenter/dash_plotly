#!/usr/bin/env python3
"""
fetch_nikkei_vi_investing.py
investing.com JP から JNIVE (日経VI, pair_id=28878) の履歴データを取得し
nikkei_vi_max_1d.parquet をマージ更新する。

Why: yfinance に JNIVE が無く、従来は手動 CSV 依存だった。
investing.com JP の historical-data ページは Cloudflare を通過でき、
埋め込み JSON から直近 20〜30 営業日分の OHLC を取得できる。
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

JNIVE_PAIR_ID = 28878
JNIVE_URL = "https://jp.investing.com/indices/nikkei-volatility-historical-data"
VI_PARQUET_PATH = PARQUET_DIR / "nikkei_vi_max_1d.parquet"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def fetch_jnive_history() -> pd.DataFrame:
    req = urllib.request.Request(
        JNIVE_URL,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8", errors="ignore")

    m = re.search(r'"historicalData":\{"data":(\[[^\]]*\])', html)
    if not m:
        raise RuntimeError("historicalData block not found in JNIVE page")

    rows = json.loads(m.group(1))
    if not rows:
        raise RuntimeError("JNIVE historicalData array is empty")

    df = pd.DataFrame(
        [
            {
                "date": pd.to_datetime(r["rowDateTimestamp"]).normalize(),
                "open": float(r["last_openRaw"]),
                "high": float(r["last_maxRaw"]),
                "low": float(r["last_minRaw"]),
                "close": float(r["last_closeRaw"]),
            }
            for r in rows
        ]
    )
    df["date"] = df["date"].dt.tz_localize(None)
    return df.sort_values("date").reset_index(drop=True)


def merge_and_save(new_df: pd.DataFrame) -> pd.DataFrame:
    if VI_PARQUET_PATH.exists():
        existing = pd.read_parquet(VI_PARQUET_PATH)
        existing["date"] = pd.to_datetime(existing["date"])
        merged = pd.concat([existing, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["date"], keep="last")
    else:
        merged = new_df
    merged = merged.sort_values("date").reset_index(drop=True)
    VI_PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(VI_PARQUET_PATH, index=False)
    return merged


def main() -> int:
    print("=" * 80)
    print("Fetch Nikkei VI (JNIVE) from investing.com JP")
    print("=" * 80)

    new_df = fetch_jnive_history()
    print(
        f"[INFO] fetched {len(new_df)} rows "
        f"({new_df['date'].min().date()} 〜 {new_df['date'].max().date()})"
    )

    merged = merge_and_save(new_df)
    print(
        f"[INFO] merged parquet: {len(merged)} rows, "
        f"latest {merged['date'].max().date()} close={merged['close'].iloc[-1]}"
    )

    s3_cfg = load_s3_config()
    if s3_cfg.bucket:
        upload_file(s3_cfg, VI_PARQUET_PATH, "nikkei_vi_max_1d.parquet")
        print("[INFO] S3 upload OK")
    else:
        print("[INFO] S3 bucket not configured, skip upload")

    return 0


if __name__ == "__main__":
    sys.exit(main())
