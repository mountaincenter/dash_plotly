#!/usr/bin/env python3
"""
Generate price parquet files via yfinance using the same granularity and
post-processing as the main pipeline, then update S3 artifacts.
Also produces yfinance-smoke-test-{period}-{interval}.parquet samples.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.manifest import sha256_of, write_manifest_atomic
from common_cfg.paths import (
    MASTER_META_PARQUET,
    MANIFEST_PATH,
    PARQUET_DIR,
    PRICE_SPECS,
    TECH_SNAPSHOT_PARQUET,
    price_parquet,
)
from common_cfg.s3cfg import S3Config, load_s3_config
from common_cfg.s3io import download_file, upload_files

from analyze import fetch_prices as fp
from server.utils import read_prices_1d_df, normalize_prices
from server.services.tech_utils_v2 import evaluate_latest_snapshot

OUTPUT_TEMPLATE = "prices_{period}_{interval}.parquet"
DEFAULT_COLUMNS = ["date", "Open", "High", "Low", "Close", "Volume", "ticker"]


def _load_s3_cfg() -> S3Config:
    cfg = load_s3_config()
    bucket = cfg.bucket or "dash-plotly"
    prefix = cfg.prefix or "parquet/"
    return S3Config(
        bucket=bucket,
        prefix=prefix,
        region=cfg.region,
        profile=cfg.profile,
        endpoint_url=cfg.endpoint_url,
    )


def _ensure_meta_parquet() -> None:
    cfg = _load_s3_cfg()
    dest = MASTER_META_PARQUET
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(
        f"[INFO] local CSV sources missing; downloading meta parquet from "
        f"s3://{cfg.bucket}/{cfg.prefix}meta.parquet"
    )
    ok = download_file(cfg, "meta.parquet", dest)
    if not ok:
        if dest.exists():
            print("[WARN] S3 download failed; falling back to existing local meta.parquet")
            return
        raise RuntimeError("meta.parquet unavailable (no CSV sources and S3 download failed)")


def _prepare_universe() -> pd.DataFrame:
    _ensure_meta_parquet()
    universe = fp.load_universe()
    tickers = universe["ticker"].tolist()
    if not tickers:
        raise RuntimeError("Ticker universe is empty after meta preparation.")
    return universe


def _fetch_and_update_prices(tickers: List[str]) -> Tuple[List[Path], List[Path]]:
    generated_files: List[Path] = []
    sample_files: List[Path] = []

    for period, interval in PRICE_SPECS:
        suffix = f"{period}_{interval}"
        out_path = price_parquet(period, interval)
        sample_path = Path(OUTPUT_TEMPLATE.format(period=period, interval=interval))
        print(f"[STEP] fetching prices ({suffix}) for {len(tickers)} tickers")

        try:
            df_new = fp._fetch_prices(tickers, period, interval)  # noqa: SLF001
        except Exception as exc:
            print(f"[WARN] skipping prices ({suffix}) due to error: {exc}")
            if out_path.exists():
                print(f"[INFO] using existing parquet for {suffix}: {out_path}")
                generated_files.append(out_path)
            continue

        if df_new.empty:
            print(f"[WARN] prices ({suffix}) returned empty dataframe. skipping write.")
            if out_path.exists():
                print(f"[INFO] using existing parquet for {suffix}: {out_path}")
                generated_files.append(out_path)
            continue

        sample_df = df_new.copy()
        for col in DEFAULT_COLUMNS:
            if col not in sample_df.columns:
                sample_df[col] = pd.NA
        sample_df = sample_df[DEFAULT_COLUMNS].sort_values(["ticker", "date"]).reset_index(drop=True)
        sample_df.to_parquet(sample_path, index=False)
        sample_files.append(sample_path)
        print(f"[OK] saved sample parquet: {sample_path} rows={len(sample_df)}")

        rows = fp._append_or_create(out_path, df_new)  # noqa: SLF001
        print(f"[OK] prices ({suffix}) saved: {out_path} rows={rows}")
        generated_files.append(out_path)

    return generated_files, sample_files


def _generate_tech_snapshot() -> Path | None:
    print("[STEP] generate tech snapshot parquet")
    df = read_prices_1d_df()
    if df is None or df.empty:
        print("[WARN] prices_... files missing; tech snapshot skipped.")
        return None

    norm = normalize_prices(df)
    if norm is None or norm.empty:
        print("[WARN] normalized prices empty; tech snapshot skipped.")
        return None

    results = []
    for _, grp in norm.sort_values(["ticker", "date"]).groupby("ticker", sort=False):
        grp = grp.dropna(subset=["Close"]).copy()
        if grp.empty:
            continue
        grp = grp.set_index("date")
        try:
            results.append(evaluate_latest_snapshot(grp))
        except Exception as exc:
            ticker = grp["ticker"].iloc[0]
            print(f"[WARN] Failed to evaluate snapshot for {ticker}: {exc}")

    if not results:
        print("[WARN] No snapshot data generated.")
        return None

    snapshot_df = pd.DataFrame(results)
    for col in ["values", "votes", "overall"]:
        if col in snapshot_df.columns:
            snapshot_df[col] = snapshot_df[col].apply(json.dumps)

    TECH_SNAPSHOT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    snapshot_df.to_parquet(TECH_SNAPSHOT_PARQUET, engine="pyarrow", index=False)
    print(f"[OK] tech snapshot saved: {TECH_SNAPSHOT_PARQUET}")
    return TECH_SNAPSHOT_PARQUET


def _write_manifest(files: List[Path]) -> None:
    items = []
    for path in files:
        if not path.exists():
            print(f"[INFO] manifest skip (missing): {path.name}")
            continue
        stat = path.stat()
        items.append(
            {
                "key": path.name,
                "bytes": stat.st_size,
                "sha256": sha256_of(path),
                "mtime": pd.Timestamp(stat.st_mtime, unit="s", tz="UTC").isoformat(),
            }
        )

    if not items:
        raise RuntimeError("No files available to build manifest.")

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    write_manifest_atomic(items, MANIFEST_PATH)
    print(f"[OK] manifest updated: {MANIFEST_PATH} (entries={len(items)})")


def _upload_to_s3(files: List[Path]) -> None:
    cfg = _load_s3_cfg()
    if not cfg.bucket:
        print("[INFO] S3 bucket not configured; upload skipped.")
        return
    if not files:
        print("[INFO] No files to upload to S3.")
        return
    upload_files(cfg, files)


def _generate_scalping_lists() -> Tuple[Path, Path]:
    """Generate scalping watchlists"""
    print("[STEP] generate scalping watchlists")
    import subprocess
    scalping_script = ROOT / "analyze" / "generate_scalping_lists.py"
    subprocess.run([sys.executable, str(scalping_script)], check=True)

    scalping_entry = PARQUET_DIR / "scalping_entry.parquet"
    scalping_active = PARQUET_DIR / "scalping_active.parquet"
    return scalping_entry, scalping_active


def main() -> int:
    load_dotenv_cascade()

    universe = _prepare_universe()
    tickers = universe["ticker"].tolist()
    print(f"[INFO] universe size: {len(tickers)}")

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    price_files, sample_files = _fetch_and_update_prices(tickers)
    if not price_files:
        raise RuntimeError("No price parquet files were written.")

    tech_snapshot = _generate_tech_snapshot()

    # Generate scalping lists
    scalping_entry, scalping_active = _generate_scalping_lists()

    manifest_targets = [MASTER_META_PARQUET] + price_files
    if tech_snapshot:
        manifest_targets.append(tech_snapshot)
    if scalping_entry.exists():
        manifest_targets.append(scalping_entry)
    if scalping_active.exists():
        manifest_targets.append(scalping_active)
    _write_manifest(manifest_targets)

    upload_targets = list(price_files)
    if tech_snapshot:
        upload_targets.append(tech_snapshot)
    if scalping_entry.exists():
        upload_targets.append(scalping_entry)
    if scalping_active.exists():
        upload_targets.append(scalping_active)
    upload_targets.append(MANIFEST_PATH)
    _upload_to_s3(upload_targets)

    samples_summary = ", ".join(str(p) for p in sample_files)
    print(f"[INFO] generated sample files: {samples_summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
