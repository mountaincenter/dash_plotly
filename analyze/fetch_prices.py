#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_prices.py
- Read meta.parquet and extract the entire ticker universe
- Download price data via yfinance across predefined intervals
- Append/update parquet outputs under data/parquet/prices_*.parquet
"""

from __future__ import annotations

from pathlib import Path
from typing import List
import sys

import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.flags import NO_MANIFEST, NO_S3
from common_cfg.paths import (
    PARQUET_DIR,
    MASTER_META_PARQUET,
    MANIFEST_PATH,
    PRICE_SPECS,
    price_parquet,
)
from common_cfg.s3cfg import DATA_BUCKET, PARQUET_PREFIX, AWS_REGION, AWS_PROFILE
from common_cfg.manifest import sha256_of, write_manifest_atomic
from common_cfg.s3io import maybe_upload_files_s3


def load_universe() -> pd.DataFrame:
    meta_path = MASTER_META_PARQUET
    if not meta_path.exists():
        raise FileNotFoundError(f"not found: {meta_path}")

    meta_df = pd.read_parquet(meta_path, engine="pyarrow")
    required_cols = {"code", "stock_name", "ticker"}
    missing = required_cols.difference(meta_df.columns)
    if missing:
        raise KeyError(f"meta parquet missing columns: {sorted(missing)}")

    universe = (
        meta_df.loc[meta_df["ticker"].notna(), ["code", "stock_name", "ticker"]]
        .drop_duplicates(subset=["ticker"])
        .reset_index(drop=True)
    )
    universe["code"] = universe["code"].astype("string")
    universe["stock_name"] = universe["stock_name"].astype("string")
    universe["ticker"] = universe["ticker"].astype("string")

    if universe.empty:
        raise RuntimeError("Universe is empty. Check ticker values in meta.parquet.")
    return universe


def _flatten_multi(raw: pd.DataFrame, tickers: List[str], interval: str) -> pd.DataFrame:
    frames = []
    if isinstance(raw.columns, pd.MultiIndex):
        lv0 = raw.columns.get_level_values(0)
        for t in tickers:
            if t in lv0:
                sub = raw[t].copy()
                if sub.empty:
                    continue
                sub = sub.reset_index()
                if "Datetime" in sub.columns:
                    sub = sub.rename(columns={"Datetime": "date"})
                elif "Date" in sub.columns:
                    sub = sub.rename(columns={"Date": "date"})
                elif "index" in sub.columns:
                    sub = sub.rename(columns={"index": "date"})
                else:
                    sub.columns = ["date"] + [c for c in sub.columns[1:]]
                sub["ticker"] = t
                keep = [c for c in ["date", "Open", "High", "Low", "Close", "Volume", "ticker"] if c in sub.columns]
                frames.append(sub[keep].copy())
    else:
        sub = raw.reset_index()
        if "Datetime" in sub.columns:
            sub = sub.rename(columns={"Datetime": "date"})
        elif "Date" in sub.columns:
            sub = sub.rename(columns={"Date": "date"})
        elif "index" in sub.columns:
            sub = sub.rename(columns={"index": "date"})
        sub["ticker"] = tickers[0] if tickers else "UNKNOWN"
        keep = [c for c in ["date", "Open", "High", "Low", "Close", "Volume", "ticker"] if c in sub.columns]
        frames.append(sub[keep].copy())

    if not frames:
        return pd.DataFrame(columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"])

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")

    if interval in ("5m", "15m", "1h"):
        try:
            if out["date"].dt.tz is not None:
                out["date"] = out["date"].dt.tz_convert("Asia/Tokyo")
            else:
                out["date"] = out["date"].dt.tz_localize("UTC").dt.tz_convert("Asia/Tokyo")
            out["date"] = out["date"].dt.tz_localize(None)
        except Exception:
            try:
                out["date"] = out["date"].dt.tz_localize(None)
            except Exception:
                pass
    else:
        try:
            out["date"] = out["date"].dt.tz_localize(None)
        except Exception:
            pass

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out[out["date"].notna()].copy()
    need_ohlc = [c for c in ["Open", "High", "Low", "Close"] if c in out.columns]
    if need_ohlc:
        out = out.dropna(subset=need_ohlc, how="any")

    if "ticker" in out.columns:
        out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    else:
        out = out.sort_values(["date"]).reset_index(drop=True)
    return out


def _fetch_prices(tickers: List[str], period: str, interval: str) -> pd.DataFrame:
    try:
        raw = yf.download(
            tickers,
            period=period,
            interval=interval,
            group_by="ticker",
            threads=True,
            progress=False,
            auto_adjust=True,
        )
        df = _flatten_multi(raw, tickers, interval)
        if df.empty:
            raise RuntimeError("yf.download returned empty. fallback to per-ticker.")
    except Exception:
        frames = []
        for t in tickers:
            try:
                raw = yf.download(
                    t,
                    period=period,
                    interval=interval,
                    group_by="ticker",
                    threads=True,
                    progress=False,
                    auto_adjust=True,
                )
                sub = _flatten_multi(raw, [t], interval)
                if not sub.empty:
                    frames.append(sub)
            except Exception:
                continue
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    need = {"date", "Open", "High", "Low", "Close", "ticker"}
    if df.empty or not need.issubset(df.columns):
        raise RuntimeError(f"No price data collected or required columns missing for period={period} interval={interval}.")
    return df


def _append_or_create(path: Path, new_df: pd.DataFrame) -> int:
    cols = ["date", "Open", "High", "Low", "Close", "Volume", "ticker"]
    if path.exists():
        try:
            existing = pd.read_parquet(path, engine="pyarrow")
        except Exception:
            existing = pd.DataFrame(columns=cols)
    else:
        existing = pd.DataFrame(columns=cols)

    for col in cols:
        if col not in new_df.columns:
            new_df[col] = pd.Series(dtype=existing[col].dtype if col in existing.columns else "float64")
        if col not in existing.columns:
            existing[col] = pd.Series(dtype=new_df[col].dtype)

    frames = []
    if not existing.empty:
        frames.append(existing[cols])
    if not new_df.empty:
        frames.append(new_df[cols])
    both = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=cols)

    if not both.empty:
        na_cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in both.columns]
        both["_na"] = both[na_cols].isna().sum(axis=1)
        both = both.sort_values(["ticker", "date", "_na"], ascending=[True, True, True]).drop_duplicates(
            subset=["ticker", "date"], keep="first"
        )
        both = both.drop(columns=["_na"], errors="ignore")
        need_ohlc = [c for c in ["Open", "High", "Low", "Close"] if c in both.columns]
        if need_ohlc:
            both = both.dropna(subset=need_ohlc, how="any")
        both = both.sort_values(["ticker", "date"]).reset_index(drop=True)

    path.parent.mkdir(parents=True, exist_ok=True)
    both.to_parquet(path, engine="pyarrow", index=False)
    return len(both)


def main() -> int:
    load_dotenv_cascade()

    universe = load_universe()
    tickers: List[str] = universe["ticker"].tolist()
    print(f"[INFO] universe size: {len(tickers)}")

    written_files: List[Path] = []
    for period, interval in PRICE_SPECS:
        suffix = f"{period}_{interval}"
        out_path = price_parquet(period, interval)
        try:
            df_iv = _fetch_prices(tickers, period, interval)
        except Exception as e:
            print(f"[WARN] skipping prices ({suffix}) due to error: {e}")
            continue
        if df_iv.empty:
            print(f"[WARN] prices ({suffix}) returned empty dataframe. skipping write.")
            continue
        rows = _append_or_create(out_path, df_iv)
        print(f"[OK] prices ({suffix}) saved: {out_path} rows={rows}")
        written_files.append(out_path)

    if not written_files:
        raise RuntimeError("No price parquet files were written.")

    if not NO_MANIFEST:
        items = []
        for p in written_files:
            stat = p.stat()
            items.append({
                "key": p.name,
                "bytes": stat.st_size,
                "sha256": sha256_of(p),
                "mtime": pd.Timestamp(stat.st_mtime, unit="s", tz="UTC").isoformat(),
            })
        write_manifest_atomic(items, MANIFEST_PATH)
        print(f"[OK] manifest updated: {MANIFEST_PATH} (entries={len(items)})")
    else:
        print("[INFO] PIPELINE_NO_MANIFEST=1 → manifest 更新はスキップ")

    if not NO_S3:
        upload_targets = written_files + ([] if NO_MANIFEST else [MANIFEST_PATH])
        maybe_upload_files_s3(
            upload_targets,
            bucket=DATA_BUCKET,
            prefix=PARQUET_PREFIX,
            aws_region=AWS_REGION,
            aws_profile=AWS_PROFILE,
            dry_run=False,
        )
        print(f"[OK] S3 upload done (count={len(upload_targets)})")
    else:
        print("[INFO] PIPELINE_NO_S3=1 → S3 送信はスキップ")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
