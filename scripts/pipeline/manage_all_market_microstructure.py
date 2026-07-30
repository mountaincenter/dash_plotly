#!/usr/bin/env python3
"""Manage append-only all-market J-Quants tick and one-minute datasets."""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.s3cfg import S3Config, load_s3_config


DEFAULT_MINUTE_ROOT = (
    ROOT / "data" / "research" / "jquants_all_market" / "minute"
)
DEFAULT_MINUTE_FETCH_MANIFEST = (
    ROOT / "data" / "research" / "jquants_all_market" / "fetch_manifest.parquet"
)
DEFAULT_TICK_ROOT = ROOT / "data" / "research" / "jquants_tick" / "raw"
ROOT_MANIFEST_PATH = ROOT / "data" / "parquet" / "manifest.json"

MINUTE_DATASET_ID = "jquants_all_market_minute"
TICK_DATASET_ID = "jquants_all_market_tick"
MINUTE_S3_ROOT = "jquants/all_market_minute"
TICK_S3_ROOT = "jquants/all_market_tick"
DATASET_MANIFEST_NAME = "_manifest.json"
TRADES_ENDPOINT = "/equities/trades"
SCHEMA_VERSION = 1
CHUNK_ROWS = 500_000


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def minute_partition_path(root: Path, trading_date: str) -> Path:
    return root / f"trading_date={trading_date}" / "part-000.parquet"


def tick_partition_path(root: Path, trading_date: str) -> Path:
    compact = trading_date.replace("-", "")
    return (
        root
        / f"trading_date={trading_date}"
        / f"equities_trades_{compact}.csv.gz"
    )


def minute_partition_key(trading_date: str) -> str:
    return (
        f"{MINUTE_S3_ROOT}/trading_date={trading_date}/part-000.parquet"
    )


def tick_partition_key(trading_date: str) -> str:
    compact = trading_date.replace("-", "")
    return (
        f"{TICK_S3_ROOT}/trading_date={trading_date}/"
        f"equities_trades_{compact}.csv.gz"
    )


def dataset_manifest_key(dataset_root: str) -> str:
    return f"{dataset_root}/{DATASET_MANIFEST_NAME}"


def prefixed_key(cfg: S3Config, relative_key: str) -> str:
    prefix = (cfg.prefix or "parquet/").strip("/")
    return f"{prefix}/{relative_key.lstrip('/')}"


def create_s3_client(cfg: S3Config):
    session_kwargs = {"profile_name": cfg.profile} if cfg.profile else {}
    client_kwargs: dict[str, str] = {}
    if cfg.region:
        client_kwargs["region_name"] = cfg.region
    if cfg.endpoint_url:
        client_kwargs["endpoint_url"] = cfg.endpoint_url
    session = (
        boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()
    )
    return session.client("s3", **client_kwargs)


def empty_dataset_manifest(
    dataset_id: str,
    dataset_root: str,
    storage_format: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "dataset": dataset_id,
        "root_key": dataset_root,
        "storage_format": storage_format,
        "partition_key": "trading_date",
        "write_policy": "append_only",
        "generated_at": now_iso(),
        "partitions": {},
    }


def load_local_dataset_manifest(
    path: Path,
    dataset_id: str,
    dataset_root: str,
    storage_format: str,
) -> dict[str, Any]:
    if not path.exists():
        return empty_dataset_manifest(dataset_id, dataset_root, storage_format)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("dataset") != dataset_id:
        raise RuntimeError(
            f"dataset manifest mismatch: {payload.get('dataset')} != {dataset_id}"
        )
    if payload.get("write_policy") != "append_only":
        raise RuntimeError("dataset manifest is not append-only")
    if not isinstance(payload.get("partitions"), dict):
        raise RuntimeError("dataset manifest partitions must be an object")
    return payload


def parquet_schema_hash(path: Path) -> str:
    schema = pq.ParquetFile(path).schema_arrow.remove_metadata()
    return sha256_bytes(str(schema).encode("utf-8"))


def validate_minute_partition(
    path: Path,
    trading_date: str,
    *,
    expected_rows: int | None = None,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        raise RuntimeError(f"minute partition is missing or empty: {path}")
    actual_sha256 = sha256_file(path)
    if expected_sha256 and actual_sha256 != expected_sha256:
        raise RuntimeError(
            f"{trading_date}: minute SHA mismatch "
            f"{actual_sha256} != {expected_sha256}"
        )
    parquet_file = pq.ParquetFile(path)
    rows = parquet_file.metadata.num_rows
    if expected_rows is not None and rows != expected_rows:
        raise RuntimeError(
            f"{trading_date}: minute row mismatch {rows} != {expected_rows}"
        )
    required_columns = {
        "trading_date",
        "time",
        "jquants_code",
        "ticker",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
    }
    columns = set(parquet_file.schema_arrow.names)
    if columns != required_columns:
        raise RuntimeError(
            f"{trading_date}: minute columns mismatch "
            f"missing={sorted(required_columns - columns)} "
            f"extra={sorted(columns - required_columns)}"
        )
    date_frame = (
        pq.ParquetFile(path)
        .read(columns=["trading_date"])
        .to_pandas()
    )
    values = pd.to_datetime(
        date_frame["trading_date"], errors="raise"
    ).dt.strftime("%Y-%m-%d")
    if set(values.unique()) != {trading_date}:
        raise RuntimeError(f"{trading_date}: minute partition contains mixed dates")
    return {
        "trading_date": trading_date,
        "key": minute_partition_key(trading_date),
        "sha256": actual_sha256,
        "bytes": path.stat().st_size,
        "rows": rows,
        "schema_sha256": parquet_schema_hash(path),
        "validation": "date_rows_schema_sha256",
    }


def validate_gzip(path: Path) -> None:
    if not path.exists() or path.stat().st_size == 0:
        raise RuntimeError(f"tick raw is missing or empty: {path}")
    try:
        with gzip.open(path, "rb") as stream:
            for _ in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                pass
    except (EOFError, OSError) as error:
        raise RuntimeError(f"invalid gzip: {path}: {error}") from error


def tick_partition_record(
    path: Path,
    trading_date: str,
    *,
    raw_key: str,
    tick_rows: int,
) -> dict[str, Any]:
    validate_gzip(path)
    return {
        "trading_date": trading_date,
        "key": tick_partition_key(trading_date),
        "source_bulk_key": raw_key,
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
        "rows": tick_rows,
        "validation": "gzip_date_schema_sha256",
    }


def append_partition(
    manifest: dict[str, Any],
    record: dict[str, Any],
) -> None:
    trading_date = str(record["trading_date"])
    existing = manifest["partitions"].get(trading_date)
    if existing:
        if (
            existing.get("sha256") == record.get("sha256")
            and existing.get("key") == record.get("key")
        ):
            return
        raise RuntimeError(
            f"append-only violation for {manifest['dataset']} {trading_date}: "
            "existing partition differs"
        )
    manifest["partitions"][trading_date] = record
    manifest["partitions"] = dict(
        sorted(manifest["partitions"].items())
    )
    manifest["generated_at"] = now_iso()


def upload_append_only(
    s3,
    cfg: S3Config,
    local_path: Path,
    relative_key: str,
    local_sha256: str,
) -> str:
    if not cfg.bucket:
        raise RuntimeError("S3 bucket is unavailable")
    key = prefixed_key(cfg, relative_key)
    try:
        head = s3.head_object(Bucket=cfg.bucket, Key=key, ChecksumMode="ENABLED")
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
        head = None
    if head:
        remote_sha = (head.get("Metadata") or {}).get("sha256")
        if (
            remote_sha == local_sha256
            and int(head["ContentLength"]) == local_path.stat().st_size
        ):
            print(f"  S3 cache hit: {relative_key}")
            return str(head.get("ETag", "")).strip('"')
        raise RuntimeError(
            f"append-only S3 collision: s3://{cfg.bucket}/{key}"
        )

    checksum = base64.b64encode(bytes.fromhex(local_sha256)).decode("ascii")
    s3.upload_file(
        str(local_path),
        cfg.bucket,
        key,
        ExtraArgs={
            "ContentType": "application/octet-stream",
            "CacheControl": "max-age=60",
            "ServerSideEncryption": "AES256",
            "Metadata": {"sha256": local_sha256},
            "ChecksumAlgorithm": "SHA256",
        },
    )
    head = s3.head_object(Bucket=cfg.bucket, Key=key, ChecksumMode="ENABLED")
    if int(head["ContentLength"]) != local_path.stat().st_size:
        raise RuntimeError(f"S3 size verification failed: {relative_key}")
    if (head.get("Metadata") or {}).get("sha256") != local_sha256:
        raise RuntimeError(f"S3 SHA metadata verification failed: {relative_key}")
    remote_checksum = head.get("ChecksumSHA256")
    if (
        remote_checksum
        and "-" not in remote_checksum
        and remote_checksum != checksum
    ):
        raise RuntimeError(f"S3 checksum verification failed: {relative_key}")
    print(f"  uploaded: {relative_key}")
    return str(head.get("ETag", "")).strip('"')


def put_json_verified(
    s3,
    cfg: S3Config,
    relative_key: str,
    payload: dict[str, Any],
) -> str:
    if not cfg.bucket:
        raise RuntimeError("S3 bucket is unavailable")
    body = (
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")
    digest = sha256_bytes(body)
    key = prefixed_key(cfg, relative_key)
    response = s3.put_object(
        Bucket=cfg.bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        CacheControl="max-age=60",
        ServerSideEncryption="AES256",
        Metadata={"sha256": digest},
        ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode("ascii"),
    )
    head = s3.head_object(Bucket=cfg.bucket, Key=key, ChecksumMode="ENABLED")
    if int(head["ContentLength"]) != len(body):
        raise RuntimeError(f"S3 JSON size verification failed: {relative_key}")
    if (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"S3 JSON SHA verification failed: {relative_key}")
    return str(response.get("ETag", "")).strip('"')


def get_s3_json(s3, cfg: S3Config, relative_key: str) -> dict[str, Any] | None:
    if not cfg.bucket:
        raise RuntimeError("S3 bucket is unavailable")
    key = prefixed_key(cfg, relative_key)
    try:
        response = s3.get_object(Bucket=cfg.bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        return None
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    body = response["Body"].read()
    metadata_sha = (response.get("Metadata") or {}).get("sha256")
    if metadata_sha and metadata_sha != sha256_bytes(body):
        raise RuntimeError(f"S3 JSON SHA metadata mismatch: {relative_key}")
    return json.loads(body)


def get_s3_json_with_sha(
    s3,
    cfg: S3Config,
    relative_key: str,
) -> tuple[dict[str, Any], str]:
    if not cfg.bucket:
        raise RuntimeError("S3 bucket is unavailable")
    key = prefixed_key(cfg, relative_key)
    response = s3.get_object(Bucket=cfg.bucket, Key=key)
    body = response["Body"].read()
    digest = sha256_bytes(body)
    metadata_sha = (response.get("Metadata") or {}).get("sha256")
    if metadata_sha and metadata_sha != digest:
        raise RuntimeError(f"S3 JSON SHA metadata mismatch: {relative_key}")
    return json.loads(body), digest


def publish_manifests(
    s3,
    cfg: S3Config,
    *,
    minute_manifest: dict[str, Any] | None,
    minute_manifest_path: Path | None,
    tick_manifest: dict[str, Any] | None,
    tick_manifest_path: Path | None,
) -> None:
    root = get_s3_json(s3, cfg, "manifest.json") or {"files": {}}
    datasets = root.setdefault("datasets", {})

    candidates = [
        (
            MINUTE_DATASET_ID,
            MINUTE_S3_ROOT,
            minute_manifest,
            minute_manifest_path,
        ),
        (
            TICK_DATASET_ID,
            TICK_S3_ROOT,
            tick_manifest,
            tick_manifest_path,
        ),
    ]
    for dataset_id, dataset_root, manifest, local_path in candidates:
        if manifest is None or local_path is None:
            continue
        manifest_key = dataset_manifest_key(dataset_root)
        put_json_verified(s3, cfg, manifest_key, manifest)
        manifest_bytes = local_path.read_bytes()
        partitions = manifest["partitions"]
        latest = max(partitions) if partitions else None
        datasets[dataset_id] = {
            "manifest_key": manifest_key,
            "manifest_sha256": sha256_bytes(manifest_bytes),
            "protected": True,
            "canonical": True,
            "write_policy": "append_only",
            "partition_count": len(partitions),
            "latest_trading_date": latest,
            "updated_at": now_iso(),
        }

    root["generated_at"] = now_iso()
    put_json_verified(s3, cfg, "manifest.json", root)
    ROOT_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(ROOT_MANIFEST_PATH, root)


def bootstrap_minute(args: argparse.Namespace) -> int:
    source_root = args.minute_root
    source_manifest = pd.read_parquet(args.minute_fetch_manifest)
    source_manifest["trading_date"] = pd.to_datetime(
        source_manifest["trading_date"], errors="raise"
    ).dt.strftime("%Y-%m-%d")
    source_manifest = source_manifest[
        source_manifest["status"].astype(str).eq("complete")
    ].copy()
    source_manifest = source_manifest.sort_values("trading_date")
    if source_manifest.empty:
        raise RuntimeError("minute fetch manifest has no complete partitions")

    local_manifest_path = source_root / DATASET_MANIFEST_NAME
    manifest = load_local_dataset_manifest(
        local_manifest_path,
        MINUTE_DATASET_ID,
        MINUTE_S3_ROOT,
        "parquet",
    )
    cfg = load_s3_config()
    s3 = None if args.no_upload else create_s3_client(cfg)

    print(
        f"bootstrap minute: {len(source_manifest)} partitions "
        f"{source_manifest.iloc[0]['trading_date']}.."
        f"{source_manifest.iloc[-1]['trading_date']}"
    )
    for index, row in enumerate(source_manifest.itertuples(index=False), 1):
        trading_date = str(row.trading_date)
        path = minute_partition_path(source_root, trading_date)
        record = validate_minute_partition(
            path,
            trading_date,
            expected_rows=int(row.rows),
            expected_sha256=str(row.sha256),
        )
        record.update(
            {
                "instruments": int(row.instruments),
                "min_datetime": str(row.min_datetime),
                "max_datetime": str(row.max_datetime),
                "source": str(row.source),
            }
        )
        append_partition(manifest, record)
        if s3 is not None:
            upload_append_only(
                s3,
                cfg,
                path,
                record["key"],
                record["sha256"],
            )
        if index % 25 == 0 or index == len(source_manifest):
            print(f"  verified {index}/{len(source_manifest)}")

    atomic_write_json(local_manifest_path, manifest)
    if s3 is not None:
        publish_manifests(
            s3,
            cfg,
            minute_manifest=manifest,
            minute_manifest_path=local_manifest_path,
            tick_manifest=None,
            tick_manifest_path=None,
        )
    print(
        f"minute bootstrap complete: {len(manifest['partitions'])} partitions"
    )
    return 0


def run_jquants(args: list[str]) -> str:
    completed = subprocess.run(
        ["jquants", *args],
        text=True,
        capture_output=True,
        timeout=300,
        check=False,
    )
    if completed.returncode != 0:
        message = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"jquants {' '.join(args)} failed: {message[:500]}")
    return completed.stdout


def find_daily_bulk_key(trading_date: str) -> str | None:
    month = trading_date[:7]
    output = run_jquants(
        [
            "-o",
            "json",
            "bulk",
            "list",
            "--endpoint",
            TRADES_ENDPOINT,
            "--date",
            month,
        ]
    )
    compact = trading_date.replace("-", "")
    expected_name = f"equities_trades_{compact}.csv.gz"
    for entry in json.loads(output):
        key = str(entry["Key"])
        if key.endswith(expected_name):
            return key
    return None


def wait_for_bulk_key(trading_date: str, timeout_seconds: int) -> str:
    deadline = time.monotonic() + timeout_seconds
    while True:
        key = find_daily_bulk_key(trading_date)
        if key:
            return key
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"daily bulk tick is not published for {trading_date}"
            )
        print(f"bulk tick unavailable for {trading_date}; retrying in 30s")
        time.sleep(30)


def download_bulk_tick(raw_key: str, destination: Path) -> None:
    if destination.exists():
        validate_gzip(destination)
        print(f"raw cache hit: {destination}")
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".partial")
    temporary.unlink(missing_ok=True)
    url = run_jquants(["bulk", "get", "--key", raw_key]).strip()
    if not url.startswith("https://"):
        raise RuntimeError("jquants bulk get returned an invalid URL")
    request = urllib.request.Request(url, headers={"User-Agent": "python-stock"})
    with urllib.request.urlopen(request, timeout=900) as response:
        with temporary.open("wb") as output:
            while True:
                block = response.read(8 * 1024 * 1024)
                if not block:
                    break
                output.write(block)
    validate_gzip(temporary)
    os.replace(temporary, destination)


def aggregate_partial_groups(partials: list[pd.DataFrame]) -> pd.DataFrame:
    combined = pd.concat(partials, ignore_index=True)
    return (
        combined.groupby(
            ["trading_date", "time", "jquants_code"],
            sort=False,
            observed=True,
        )
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            value=("value", "sum"),
            tick_count=("tick_count", "sum"),
        )
        .reset_index()
    )


def build_minute_from_tick(
    raw_path: Path,
    output_path: Path,
    trading_date: str,
) -> tuple[dict[str, Any], int]:
    partials: list[pd.DataFrame] = []
    tick_rows = 0
    dtype = {
        "Date": "string",
        "Code": "string",
        "Time": "string",
        "SessionDistinction": "int8",
        "Price": "float64",
        "TradingVolume": "int64",
        "TransactionId": "int64",
    }
    for chunk in pd.read_csv(raw_path, chunksize=CHUNK_ROWS, dtype=dtype):
        tick_rows += len(chunk)
        dates = chunk["Date"].astype(str)
        if set(dates.unique()) != {trading_date}:
            raise RuntimeError(f"{trading_date}: bulk tick contains mixed dates")
        chunk = chunk.sort_values(
            ["Code", "Time", "TransactionId"], kind="stable"
        )
        chunk["time"] = chunk["Time"].str.slice(0, 5)
        chunk["value"] = (
            chunk["Price"] * chunk["TradingVolume"]
        ).round().astype("int64")
        grouped = (
            chunk.groupby(
                ["Date", "time", "Code"],
                sort=False,
                observed=True,
            )
            .agg(
                open=("Price", "first"),
                high=("Price", "max"),
                low=("Price", "min"),
                close=("Price", "last"),
                volume=("TradingVolume", "sum"),
                value=("value", "sum"),
                tick_count=("TransactionId", "size"),
            )
            .reset_index()
            .rename(columns={"Date": "trading_date", "Code": "jquants_code"})
        )
        partials.append(grouped)
    if not partials:
        raise RuntimeError(f"{trading_date}: bulk tick is empty")

    minute = aggregate_partial_groups(partials)
    ticker_code = minute["jquants_code"].where(
        ~minute["jquants_code"].str.endswith("0"),
        minute["jquants_code"].str[:-1],
    )
    minute["ticker"] = ticker_code + ".T"
    minute["datetime"] = pd.to_datetime(
        minute["trading_date"] + " " + minute["time"], errors="raise"
    )
    minute["trading_date"] = pd.to_datetime(
        minute["trading_date"], errors="raise"
    ).dt.date
    minute = minute.sort_values(
        ["datetime", "ticker"], kind="stable"
    ).reset_index(drop=True)
    minute = minute[
        [
            "trading_date",
            "time",
            "jquants_code",
            "ticker",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "value",
            "tick_count",
        ]
    ]

    raw_daily = (
        minute.groupby("jquants_code", sort=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            value=("value", "sum"),
            tick_count=("tick_count", "sum"),
        )
        .sort_index()
    )
    if int(raw_daily["tick_count"].sum()) != tick_rows:
        raise RuntimeError(f"{trading_date}: tick row reconciliation failed")

    output = minute.drop(columns=["tick_count"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(".parquet.partial")
    output.to_parquet(
        temporary,
        index=False,
        compression="zstd",
        row_group_size=100_000,
    )
    reloaded = pq.ParquetFile(temporary).read().to_pandas()
    minute_daily = (
        reloaded.groupby("jquants_code", sort=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            value=("value", "sum"),
        )
        .sort_index()
    )
    pd.testing.assert_frame_equal(
        raw_daily.drop(columns=["tick_count"]),
        minute_daily,
        check_dtype=False,
        check_exact=True,
    )
    os.replace(temporary, output_path)
    record = validate_minute_partition(output_path, trading_date)
    record.update(
        {
            "instruments": int(output["ticker"].nunique()),
            "min_datetime": str(output["datetime"].min()),
            "max_datetime": str(output["datetime"].max()),
            "source": "jquants_bulk_equities_trades",
            "source_tick_sha256": sha256_file(raw_path),
            "validation": "tick_minute_daily_ohlcv_value_match",
        }
    )
    return record, tick_rows


def daily(args: argparse.Namespace) -> int:
    trading_date = date.fromisoformat(args.trading_date).isoformat()
    raw_path = (
        args.raw_path
        if args.raw_path
        else tick_partition_path(args.tick_root, trading_date)
    )
    output_path = minute_partition_path(args.minute_root, trading_date)

    if args.raw_path:
        raw_key = f"local:{args.raw_path.name}"
        validate_gzip(raw_path)
    else:
        raw_key = wait_for_bulk_key(
            trading_date, args.availability_timeout_seconds
        )
        download_bulk_tick(raw_key, raw_path)

    if output_path.exists() and not args.rebuild:
        minute_record = validate_minute_partition(output_path, trading_date)
        tick_rows = 0
        for chunk in pd.read_csv(
            raw_path,
            usecols=["Date"],
            chunksize=CHUNK_ROWS,
            dtype={"Date": "string"},
        ):
            if set(chunk["Date"].astype(str).unique()) != {trading_date}:
                raise RuntimeError(f"{trading_date}: bulk tick contains mixed dates")
            tick_rows += len(chunk)
    else:
        minute_record, tick_rows = build_minute_from_tick(
            raw_path, output_path, trading_date
        )

    tick_record = tick_partition_record(
        raw_path,
        trading_date,
        raw_key=raw_key,
        tick_rows=tick_rows,
    )
    minute_record["source_tick_sha256"] = tick_record["sha256"]

    minute_manifest_path = args.minute_root / DATASET_MANIFEST_NAME
    tick_manifest_path = args.tick_root / DATASET_MANIFEST_NAME
    cfg = None
    s3 = None
    if not args.no_upload:
        cfg = load_s3_config()
        s3 = create_s3_client(cfg)

    minute_manifest = None
    tick_manifest = None
    if s3 is not None and cfg is not None:
        minute_manifest = get_s3_json(
            s3, cfg, dataset_manifest_key(MINUTE_S3_ROOT)
        )
        tick_manifest = get_s3_json(
            s3, cfg, dataset_manifest_key(TICK_S3_ROOT)
        )
    if minute_manifest is None:
        minute_manifest = load_local_dataset_manifest(
            minute_manifest_path,
            MINUTE_DATASET_ID,
            MINUTE_S3_ROOT,
            "parquet",
        )
    if tick_manifest is None:
        tick_manifest = load_local_dataset_manifest(
            tick_manifest_path,
            TICK_DATASET_ID,
            TICK_S3_ROOT,
            "csv.gz",
        )
    append_partition(minute_manifest, minute_record)
    append_partition(tick_manifest, tick_record)
    atomic_write_json(minute_manifest_path, minute_manifest)
    atomic_write_json(tick_manifest_path, tick_manifest)

    if s3 is not None and cfg is not None:
        upload_append_only(
            s3,
            cfg,
            raw_path,
            tick_record["key"],
            tick_record["sha256"],
        )
        upload_append_only(
            s3,
            cfg,
            output_path,
            minute_record["key"],
            minute_record["sha256"],
        )
        publish_manifests(
            s3,
            cfg,
            minute_manifest=minute_manifest,
            minute_manifest_path=minute_manifest_path,
            tick_manifest=tick_manifest,
            tick_manifest_path=tick_manifest_path,
        )

    print(
        f"daily complete: {trading_date} ticks={tick_rows:,} "
        f"minutes={minute_record['rows']:,}"
    )
    return 0


def download_partition_verified(
    s3,
    cfg: S3Config,
    *,
    relative_key: str,
    local_path: Path,
    expected_sha256: str,
    expected_bytes: int,
) -> bool:
    if (
        local_path.exists()
        and local_path.stat().st_size == expected_bytes
        and sha256_file(local_path) == expected_sha256
    ):
        return False
    if local_path.exists():
        raise RuntimeError(
            f"local append-only partition differs from S3: {local_path}"
        )
    if not cfg.bucket:
        raise RuntimeError("S3 bucket is unavailable")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = local_path.with_suffix(local_path.suffix + ".partial")
    temporary.unlink(missing_ok=True)
    s3.download_file(
        cfg.bucket,
        prefixed_key(cfg, relative_key),
        str(temporary),
    )
    if temporary.stat().st_size != expected_bytes:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"downloaded size mismatch: {relative_key}")
    if sha256_file(temporary) != expected_sha256:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"downloaded SHA mismatch: {relative_key}")
    os.replace(temporary, local_path)
    return True


def sync_dataset(
    s3,
    cfg: S3Config,
    *,
    root_entry: dict[str, Any],
    dataset_id: str,
    local_root: Path,
    days: int | None,
    dry_run: bool,
) -> tuple[int, int]:
    manifest_key = str(root_entry["manifest_key"])
    remote_manifest, remote_manifest_sha = get_s3_json_with_sha(
        s3, cfg, manifest_key
    )
    if remote_manifest_sha != root_entry.get("manifest_sha256"):
        raise RuntimeError(
            f"{dataset_id}: root manifest does not match dataset manifest"
        )
    if remote_manifest.get("dataset") != dataset_id:
        raise RuntimeError(f"{dataset_id}: remote dataset identity mismatch")
    if remote_manifest.get("write_policy") != "append_only":
        raise RuntimeError(f"{dataset_id}: remote dataset is not append-only")

    cutoff = None
    if days is not None:
        cutoff = (datetime.now().date() - timedelta(days=days)).isoformat()
    selected = {
        trading_date: record
        for trading_date, record in remote_manifest["partitions"].items()
        if cutoff is None or trading_date >= cutoff
    }
    downloaded = 0
    skipped = 0
    for trading_date, record in sorted(selected.items()):
        relative_key = str(record["key"])
        if dataset_id == MINUTE_DATASET_ID:
            local_path = minute_partition_path(local_root, trading_date)
        elif dataset_id == TICK_DATASET_ID:
            local_path = tick_partition_path(local_root, trading_date)
        else:
            raise RuntimeError(f"unsupported dataset: {dataset_id}")
        if dry_run and not local_path.exists():
            print(f"  [DRY] would download {dataset_id} {trading_date}")
            downloaded += 1
            continue
        changed = download_partition_verified(
            s3,
            cfg,
            relative_key=relative_key,
            local_path=local_path,
            expected_sha256=str(record["sha256"]),
            expected_bytes=int(record["bytes"]),
        )
        if changed:
            downloaded += 1
            print(f"  downloaded {dataset_id} {trading_date}")
        else:
            skipped += 1

    if not dry_run:
        atomic_write_json(local_root / DATASET_MANIFEST_NAME, remote_manifest)
    return downloaded, skipped


def sync_all_market_datasets(
    *,
    days: int = 7,
    full: bool = False,
    dry_run: bool = False,
    minute_root: Path = DEFAULT_MINUTE_ROOT,
    tick_root: Path = DEFAULT_TICK_ROOT,
) -> tuple[int, int]:
    cfg = load_s3_config()
    s3 = create_s3_client(cfg)
    root, _ = get_s3_json_with_sha(s3, cfg, "manifest.json")
    datasets = root.get("datasets")
    if not isinstance(datasets, dict):
        print("all-market datasets are not registered in root manifest")
        return 0, 0

    selected_days = None if full else days
    totals = {"downloaded": 0, "skipped": 0}
    mappings = [
        (MINUTE_DATASET_ID, minute_root),
        (TICK_DATASET_ID, tick_root),
    ]
    for dataset_id, local_root in mappings:
        entry = datasets.get(dataset_id)
        if not isinstance(entry, dict):
            print(f"dataset not registered; skipping: {dataset_id}")
            continue
        downloaded, skipped = sync_dataset(
            s3,
            cfg,
            root_entry=entry,
            dataset_id=dataset_id,
            local_root=local_root,
            days=selected_days,
            dry_run=dry_run,
        )
        totals["downloaded"] += downloaded
        totals["skipped"] += skipped

    if not dry_run:
        atomic_write_json(ROOT_MANIFEST_PATH, root)
    print(
        "all-market sync complete: "
        f"downloaded={totals['downloaded']} skipped={totals['skipped']}"
    )
    return totals["downloaded"], totals["skipped"]


def sync_from_s3(args: argparse.Namespace) -> int:
    sync_all_market_datasets(
        days=args.days,
        full=args.full,
        dry_run=args.dry_run,
        minute_root=args.minute_root,
        tick_root=args.tick_root,
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser(
        "bootstrap-minute",
        help="Register and optionally upload existing minute partitions.",
    )
    bootstrap.add_argument("--minute-root", type=Path, default=DEFAULT_MINUTE_ROOT)
    bootstrap.add_argument(
        "--minute-fetch-manifest",
        type=Path,
        default=DEFAULT_MINUTE_FETCH_MANIFEST,
    )
    bootstrap.add_argument("--no-upload", action="store_true")
    bootstrap.set_defaults(handler=bootstrap_minute)

    day = subparsers.add_parser(
        "daily",
        help="Fetch one all-market tick bulk and build its minute partition.",
    )
    day.add_argument("--trading-date", required=True)
    day.add_argument("--minute-root", type=Path, default=DEFAULT_MINUTE_ROOT)
    day.add_argument("--tick-root", type=Path, default=DEFAULT_TICK_ROOT)
    day.add_argument("--raw-path", type=Path)
    day.add_argument("--availability-timeout-seconds", type=int, default=300)
    day.add_argument("--rebuild", action="store_true")
    day.add_argument("--no-upload", action="store_true")
    day.set_defaults(handler=daily)

    sync_parser = subparsers.add_parser(
        "sync",
        help="Manifest-driven S3-to-local sync for all-market datasets.",
    )
    sync_parser.add_argument("--minute-root", type=Path, default=DEFAULT_MINUTE_ROOT)
    sync_parser.add_argument("--tick-root", type=Path, default=DEFAULT_TICK_ROOT)
    sync_parser.add_argument("--days", type=int, default=7)
    sync_parser.add_argument("--full", action="store_true")
    sync_parser.add_argument("--dry-run", action="store_true")
    sync_parser.set_defaults(handler=sync_from_s3)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
