#!/usr/bin/env python3
"""
Generate market basket turnover series.

Primary pipeline usage:
  - generate_trading_value_top100.py passes the full J-Quants daily response
    before it is reduced to Top150.

Local bootstrap usage:
  - This script can create TOPIX500 turnover from prices_topix500_oc.parquet
    without an additional J-Quants API call.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import download_file

META_JQUANTS_PATH = PARQUET_DIR / "meta_jquants.parquet"
TOPIX500_PRICES_PATH = PARQUET_DIR / "prices_topix500_oc.parquet"
N225_CONSTITUENTS_PATH = ROOT / "data" / "jquants_csv" / "master" / "nikkei225_constituents.csv"
MARKET_BASKET_TURNOVER_PATH = PARQUET_DIR / "market_basket_turnover.parquet"

TOPIX500_GROUPS = {"TOPIX Core30", "TOPIX Large70", "TOPIX Mid400"}
TOPIX_ALL_PREFIX = "TOPIX"


def resolve_storage_mode() -> tuple[str, bool]:
    app_env = (
        os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or os.getenv("STAGE")
        or "local"
    ).strip().lower()
    production = app_env in {"production", "prod"}
    expected = "s3" if production else "local"
    configured = (os.getenv("STORAGE_MODE") or expected).strip().lower()
    if configured != expected:
        raise RuntimeError(
            f"storage mode mismatch: APP_ENV={app_env} requires STORAGE_MODE={expected}, "
            f"got {configured}"
        )
    return app_env, production


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate market basket turnover parquet.")
    parser.add_argument("--out", type=Path, default=MARKET_BASKET_TURNOVER_PATH)
    parser.add_argument("--topix500-source", type=Path, default=TOPIX500_PRICES_PATH)
    parser.add_argument("--no-merge", action="store_true", help="Replace output instead of merging with existing rows.")
    return parser.parse_args()


def normalize_code(value: object) -> str:
    text = "" if value is None else str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if len(text) == 5 and text.endswith("0"):
        text = text[:4]
    return text.zfill(4) if text.isdigit() and len(text) < 4 else text


def _number(value: object) -> float | None:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric)


def _load_meta(meta_path: Path = META_JQUANTS_PATH) -> pd.DataFrame:
    if not meta_path.exists():
        return pd.DataFrame(columns=["code", "topixnewindexseries"])
    meta = pd.read_parquet(meta_path)
    for col in ["code", "topixnewindexseries"]:
        if col not in meta.columns:
            meta[col] = None
    meta = meta[["code", "topixnewindexseries"]].copy()
    meta["code"] = meta["code"].map(normalize_code)
    meta["topixnewindexseries"] = meta["topixnewindexseries"].replace({pd.NA: None, "": None, "-": None})
    return meta.dropna(subset=["code"]).drop_duplicates("code", keep="first")


def _load_n225_codes(path: Path = N225_CONSTITUENTS_PATH) -> set[str]:
    if not path.exists():
        print(f"[INFO] N225 constituents CSV not found, skipping n225 basket: {path}")
        return set()
    df = pd.read_csv(path)
    code_col = next((col for col in ["code", "Code", "ticker", "Ticker"] if col in df.columns), None)
    if not code_col:
        print(f"[WARN] N225 constituents CSV has no code column, skipping: {path}")
        return set()
    codes = {normalize_code(value) for value in df[code_col].tolist()}
    codes = {code for code in codes if code and code.lower() != "nan"}
    if len(codes) < 200:
        print(f"[WARN] N225 constituents CSV has only {len(codes)} codes: {path}")
    return codes


def _basket_definitions(meta: pd.DataFrame) -> list[dict[str, object]]:
    baskets: list[dict[str, object]] = []

    if not meta.empty:
        topix_series = meta["topixnewindexseries"].fillna("").astype(str)
        topix500_codes = set(meta.loc[topix_series.isin(TOPIX500_GROUPS), "code"].astype(str))
        topix_full_codes = set(meta.loc[topix_series.str.startswith(TOPIX_ALL_PREFIX), "code"].astype(str))

        if topix500_codes:
            baskets.append({
                "basket_key": "topix500",
                "basket_label": "TOPIX500 basket",
                "codes": topix500_codes,
                "source": "meta_jquants.topixnewindexseries in Core30/Large70/Mid400",
            })
        if topix_full_codes:
            baskets.append({
                "basket_key": "topix_full",
                "basket_label": "TOPIX full basket",
                "codes": topix_full_codes,
                "source": "meta_jquants.topixnewindexseries startswith TOPIX",
            })

    n225_codes = _load_n225_codes()
    if n225_codes:
        baskets.append({
            "basket_key": "n225",
            "basket_label": "N225 basket",
            "codes": n225_codes,
            "source": str(N225_CONSTITUENTS_PATH.relative_to(ROOT)),
        })

    return baskets


def _normalize_daily(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(columns=["date", "code", "turnover_value"])
    date_col = "Date" if "Date" in daily.columns else "date" if "date" in daily.columns else None
    code_col = "Code" if "Code" in daily.columns else "code" if "code" in daily.columns else None
    turnover_col = "Va" if "Va" in daily.columns else "TurnoverValue" if "TurnoverValue" in daily.columns else None
    if not date_col or not code_col or not turnover_col:
        raise ValueError(f"daily data missing required columns: date={date_col} code={code_col} turnover={turnover_col}")
    out = pd.DataFrame({
        "date": pd.to_datetime(daily[date_col], errors="coerce").dt.strftime("%Y-%m-%d"),
        "code": daily[code_col].map(normalize_code),
        "turnover_value": pd.to_numeric(daily[turnover_col], errors="coerce"),
    })
    return out.dropna(subset=["date", "code"]).copy()


def build_market_basket_turnover_from_daily(daily: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_daily(daily)
    if normalized.empty:
        return pd.DataFrame()

    baskets = _basket_definitions(_load_meta())
    rows: list[dict[str, object]] = []
    generated_at = datetime.now().isoformat()

    for date, group in normalized.groupby("date", sort=True):
        for basket in baskets:
            codes = basket["codes"]
            assert isinstance(codes, set)
            matched = group[group["code"].isin(codes)].copy()
            turnover = float(matched["turnover_value"].fillna(0).sum()) if not matched.empty else 0.0
            rows.append({
                "date": date,
                "basket_key": basket["basket_key"],
                "basket_label": basket["basket_label"],
                "turnover_value": turnover,
                "turnover_bil": turnover / 1_000_000_000.0,
                "constituent_count": len(codes),
                "matched_count": int(matched["code"].nunique()) if not matched.empty else 0,
                "positive_turnover_count": int(matched[matched["turnover_value"].fillna(0).gt(0)]["code"].nunique()) if not matched.empty else 0,
                "source": basket["source"],
                "generated_at": generated_at,
            })

    return _add_rolling_metrics(pd.DataFrame(rows))


def build_topix500_from_calendar_prices(path: Path = TOPIX500_PRICES_PATH) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"TOPIX500 source not found: {path}")
    source = pd.read_parquet(path)
    normalized = _normalize_daily(source)
    if normalized.empty:
        return pd.DataFrame()

    generated_at = datetime.now().isoformat()
    rows = []
    for date, group in normalized.groupby("date", sort=True):
        turnover = float(group["turnover_value"].fillna(0).sum())
        rows.append({
            "date": date,
            "basket_key": "topix500",
            "basket_label": "TOPIX500 basket",
            "turnover_value": turnover,
            "turnover_bil": turnover / 1_000_000_000.0,
            "constituent_count": int(group["code"].nunique()),
            "matched_count": int(group["code"].nunique()),
            "positive_turnover_count": int(group[group["turnover_value"].fillna(0).gt(0)]["code"].nunique()),
            "source": str(path.relative_to(ROOT)),
            "generated_at": generated_at,
        })
    return _add_rolling_metrics(pd.DataFrame(rows))


def _add_rolling_metrics(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["turnover_bil"] = pd.to_numeric(out["turnover_bil"], errors="coerce")
    out = out.dropna(subset=["date", "basket_key"]).sort_values(["basket_key", "date"], kind="mergesort").copy()
    parts = []
    for _, group in out.groupby("basket_key", sort=True):
        g = group.sort_values("date", kind="mergesort").copy()
        baseline = g["turnover_bil"].shift(1).rolling(5, min_periods=1).mean()
        g["avg_5d_turnover_bil"] = baseline
        g["latest_vs_5d_avg"] = g["turnover_bil"] / baseline
        g.loc[baseline.le(0) | baseline.isna(), "latest_vs_5d_avg"] = pd.NA
        g["tv5d_delta_pct"] = (g["latest_vs_5d_avg"] - 1.0) * 100.0
        parts.append(g)
    return pd.concat(parts, ignore_index=True).sort_values(["date", "basket_key"], ascending=[False, True], kind="mergesort").reset_index(drop=True)


def restore_existing_market_basket(path: Path) -> bool:
    _, use_s3 = resolve_storage_mode()
    if not use_s3:
        return path.exists()
    config = load_s3_config()
    if not config.bucket:
        raise RuntimeError("Production S3 bucket is not configured for market basket turnover")
    if not download_file(config, path.name, path):
        raise RuntimeError(
            f"Production S3 restore failed for market basket turnover: {path.name}"
        )
    return True


def save_market_basket_turnover(df: pd.DataFrame, path: Path = MARKET_BASKET_TURNOVER_PATH, *, merge_existing: bool = True) -> pd.DataFrame:
    if df.empty:
        print("[WARN] no market basket turnover rows to save")
        return df
    existing = pd.DataFrame()
    if merge_existing and restore_existing_market_basket(path):
        try:
            existing = pd.read_parquet(path)
        except Exception as exc:
            print(f"[WARN] cannot read existing market basket turnover, replacing it: {exc}")
    combined = pd.concat([existing, df], ignore_index=True) if not existing.empty else df.copy()
    combined = combined.drop_duplicates(["date", "basket_key"], keep="last")
    combined = _add_rolling_metrics(combined)
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(path, engine="pyarrow", index=False)
    dates = sorted(df["date"].astype(str).unique().tolist())
    keys = sorted(df["basket_key"].astype(str).unique().tolist())
    print(f"[OK] saved market basket turnover: {path} rows={len(combined)} added_dates={dates[0]}..{dates[-1]} baskets={keys}")
    return combined


def save_market_basket_turnover_from_daily(
    daily: pd.DataFrame,
    path: Path = MARKET_BASKET_TURNOVER_PATH,
    *,
    merge_existing: bool = True,
) -> pd.DataFrame:
    df = build_market_basket_turnover_from_daily(daily)
    return save_market_basket_turnover(df, path, merge_existing=merge_existing)


def main() -> int:
    args = parse_args()
    app_env, use_s3 = resolve_storage_mode()
    print(f"storage: {'s3' if use_s3 else 'local'} ({app_env})")
    df = build_topix500_from_calendar_prices(args.topix500_source)
    if df.empty:
        print("[WARN] TOPIX500 basket source produced no rows")
        return 1
    save_market_basket_turnover(df, args.out, merge_existing=not args.no_merge)
    print(df.head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
