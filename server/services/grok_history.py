from __future__ import annotations

import io
from pathlib import Path

import pandas as pd


ARCHIVE_S3_KEY = "parquet/backtest/grok_trending_archive.parquet"
CLOSE_EXECUTABLE_STATUS = "executable"
CLOSE_MARK_ONLY_STATUS = "mark_only_no_round_trip"
SEGMENT_COLUMNS = (
    "seg_0930",
    "seg_1000",
    "seg_1030",
    "seg_1100",
    "seg_1130",
    "seg_1300",
    "seg_1330",
    "seg_1400",
    "seg_1430",
    "seg_1500",
    "seg_1530",
)
REQUIRED_COLUMNS = {
    "backtest_date",
    "ticker",
    "buy_price",
    "close_execution_status",
    *SEGMENT_COLUMNS,
}

# These columns were produced by the legacy Yahoo/5-minute pipeline.  The
# canonical archive segments were rebuilt from J-Quants, but these path-
# dependent metrics were not.  Hide them from runtime consumers until a
# separate J-Quants derivation is available.
LEGACY_INTRADAY_COLUMNS = (
    "morning_high",
    "morning_low",
    "morning_max_gain_pct",
    "morning_max_drawdown_pct",
    "profit_per_100_shares_morning_early",
    "profit_per_100_shares_afternoon_early",
    "phase3_1pct_return",
    "phase3_1pct_win",
    "phase3_1pct_exit_reason",
    "profit_per_100_shares_phase3_1pct",
    "phase3_2pct_return",
    "phase3_2pct_win",
    "phase3_2pct_exit_reason",
    "profit_per_100_shares_phase3_2pct",
    "phase3_3pct_return",
    "phase3_3pct_win",
    "phase3_3pct_exit_reason",
    "profit_per_100_shares_phase3_3pct",
)


class GrokHistoryError(RuntimeError):
    pass


class GrokHistoryNotFound(GrokHistoryError):
    pass


def normalize_grok_history(frame: pd.DataFrame) -> pd.DataFrame:
    """Expose one runtime schema derived from the canonical archive segments."""
    missing = sorted(REQUIRED_COLUMNS.difference(frame.columns))
    if missing:
        raise GrokHistoryError(f"grok archive is missing required columns: {missing}")

    out = frame.copy()
    buy_price = pd.to_numeric(out["buy_price"], errors="coerce")
    denominator = (buy_price * 100.0).where(buy_price.ne(0))

    for phase, segment in (("phase1", "seg_1130"), ("phase2", "seg_1530")):
        pnl = pd.to_numeric(out[segment], errors="coerce")
        out[f"profit_per_100_shares_{phase}"] = pnl
        out[f"{phase}_return"] = pnl.div(denominator)
        out[f"{phase}_win"] = pnl.gt(0).where(pnl.notna()).astype("boolean")

    phase1_pnl = pd.to_numeric(out["seg_1130"], errors="coerce")
    out["sell_price"] = (buy_price - phase1_pnl / 100.0).where(
        buy_price.notna() & phase1_pnl.notna()
    )

    high = pd.to_numeric(
        out.get("high", pd.Series(index=out.index, dtype=float)),
        errors="coerce",
    )
    low = pd.to_numeric(
        out.get("low", pd.Series(index=out.index, dtype=float)),
        errors="coerce",
    )
    valid_buy = buy_price.gt(0)
    out["daily_max_gain_pct"] = (
        (high - buy_price).div(buy_price).mul(100.0)
    ).where(valid_buy & high.notna())
    out["daily_max_drawdown_pct"] = (
        (low - buy_price).div(buy_price).mul(100.0)
    ).where(valid_buy & low.notna())

    disabled_legacy_columns = [
        column for column in LEGACY_INTRADAY_COLUMNS if column in out.columns
    ]
    if disabled_legacy_columns:
        out = out.drop(columns=disabled_legacy_columns)

    out["analysis_source"] = "grok_trending_archive"
    out.attrs["analysis_source"] = "grok_trending_archive"
    out.attrs["price_basis"] = "jquants_minute"
    out.attrs["disabled_legacy_intraday_columns"] = disabled_legacy_columns

    total_rows = len(out)
    out.attrs["jq_buy_price_coverage"] = (
        round(float(buy_price.notna().mean()), 6) if total_rows else None
    )
    out.attrs["jq_seg_1530_coverage"] = (
        round(float(out["seg_1530"].notna().mean()), 6) if total_rows else None
    )
    status = out["close_execution_status"]
    executable = status.eq(CLOSE_EXECUTABLE_STATUS)
    mark_only = status.eq(CLOSE_MARK_ONLY_STATUS)
    known = executable | mark_only
    out.attrs["close_execution_rate"] = (
        round(float(executable.sum() / known.sum()), 6)
        if int(known.sum()) > 0
        else None
    )
    out.attrs["close_executable_rows"] = int(executable.sum())
    out.attrs["close_mark_only_rows"] = int(mark_only.sum())
    return out


def load_grok_history(
    archive_path: Path,
    *,
    s3_bucket: str | None = None,
    aws_region: str | None = None,
    s3_key: str = ARCHIVE_S3_KEY,
) -> pd.DataFrame:
    """Read the canonical archive locally, falling back to the same S3 object."""
    if archive_path.exists():
        return normalize_grok_history(pd.read_parquet(archive_path))

    if not s3_bucket:
        raise GrokHistoryNotFound(f"grok archive not found: {archive_path}")

    try:
        import boto3
        from botocore.exceptions import ClientError
    except Exception as exc:
        raise GrokHistoryError(f"S3 client is unavailable: {exc}") from exc

    try:
        client = boto3.client("s3", region_name=aws_region)
        buffer = io.BytesIO()
        client.download_fileobj(s3_bucket, s3_key, buffer)
        buffer.seek(0)
        return normalize_grok_history(pd.read_parquet(buffer))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchKey":
            raise GrokHistoryNotFound(
                f"grok archive not found in S3: s3://{s3_bucket}/{s3_key}"
            ) from exc
        raise GrokHistoryError(f"failed to read grok archive from S3: {exc}") from exc
    except GrokHistoryError:
        raise
    except Exception as exc:
        raise GrokHistoryError(f"failed to read grok archive: {exc}") from exc
