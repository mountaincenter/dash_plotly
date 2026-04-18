#!/usr/bin/env python3
"""
fetch_nikkei_vi_rakuten.py
楽天証券のVIページ内 iframe (trkd-asia) から日経平均VI (JNIV) の当日OHLCを取得し
nikkei_vi_max_1d.parquet をマージ更新する。

Why: investing.com は GHA IP から 403 になる (Cloudflare)。
楽天証券 → trkd-asia iframe は認証なしで OHLC と日付を返し、
かつ GHA 環境からもアクセス可能。
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import urllib.request

JST = timezone(timedelta(hours=9))

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

# trkd-asia iframe: ind=1 (index), ric=24 (.JNIV = 日経平均VI)
TRKD_URL = "https://www.trkd-asia.com/rakutensecj/indx.jsp?ind=1&ric=24"
VI_PARQUET_PATH = PARQUET_DIR / "nikkei_vi_max_1d.parquet"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _fetch_html() -> str:
    req = urllib.request.Request(
        TRKD_URL,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
            "Referer": "https://www.rakuten-sec.co.jp/web/market/data/jniv.html",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _extract_ohlc(html: str) -> dict:
    """trkd iframe の HTML から当日 OHLC を抽出 (日付は呼び出し側で決定)"""
    # 「現在値[円]」→ 当日 close
    m_close = re.search(r"現在値\[円\].*?([\d,]+\.\d+)", html, re.DOTALL)
    # 「始値」→ 当日 open (数値の後に時刻カッコが続くケースあり)
    m_open = re.search(r"<th[^>]*>始値</th>.*?([\d,]+\.\d+)", html, re.DOTALL)
    # 「高値」
    m_high = re.search(r"<th[^>]*>高値</th>.*?([\d,]+\.\d+)", html, re.DOTALL)
    # 「安値」
    m_low = re.search(r"<th[^>]*>安値</th>.*?([\d,]+\.\d+)", html, re.DOTALL)

    if not all([m_close, m_open, m_high, m_low]):
        raise RuntimeError(
            "OHLC fields missing in trkd iframe "
            f"(close={bool(m_close)}, open={bool(m_open)}, "
            f"high={bool(m_high)}, low={bool(m_low)})"
        )

    def _to_float(x: str) -> float:
        return float(x.replace(",", ""))

    return {
        "open": _to_float(m_open.group(1)),
        "high": _to_float(m_high.group(1)),
        "low": _to_float(m_low.group(1)),
        "close": _to_float(m_close.group(1)),
    }


def fetch_jniv_today(target_date: pd.Timestamp) -> pd.DataFrame:
    html = _fetch_html()
    row = _extract_ohlc(html)
    row["date"] = target_date
    return pd.DataFrame([row])


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
    print("Fetch Nikkei VI (JNIV) from Rakuten-Sec / trkd-asia")
    print("=" * 80)

    # 実行時刻 (JST) の曜日で判定: 土日は前営業日 = 金曜のデータが既に保存済みのはず
    now_jst = datetime.now(JST)
    weekday = now_jst.weekday()  # 0=Mon, 5=Sat, 6=Sun
    if weekday >= 5:
        print(f"[INFO] today is {now_jst:%Y-%m-%d (%a)}, market closed. skip fetch.")
        return 0

    target_date = pd.Timestamp(now_jst.date())
    new_df = fetch_jniv_today(target_date)
    row = new_df.iloc[0]
    print(
        f"[INFO] fetched 1 row: {row['date'].date()} "
        f"O={row['open']} H={row['high']} L={row['low']} C={row['close']}"
    )

    # 既存 parquet の最新 close と一致したら「新データなし」とみなし skip (祝日ガード)
    if VI_PARQUET_PATH.exists():
        existing = pd.read_parquet(VI_PARQUET_PATH)
        existing["date"] = pd.to_datetime(existing["date"])
        latest = existing.sort_values("date").iloc[-1]
        if (
            pd.Timestamp(latest["date"]).date() < target_date.date()
            and abs(float(latest["close"]) - float(row["close"])) < 1e-6
            and abs(float(latest["open"]) - float(row["open"])) < 1e-6
        ):
            print(
                f"[INFO] OHLC matches existing latest {latest['date'].date()} "
                f"(holiday or market closed). skip save."
            )
            return 0

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
