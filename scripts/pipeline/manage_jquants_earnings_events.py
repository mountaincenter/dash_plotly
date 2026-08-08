#!/usr/bin/env python3
"""Manage point-in-time J-Quants earnings schedules and actual disclosures."""

from __future__ import annotations

import argparse
import base64
import gzip
import io
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.s3cfg import S3Config, load_s3_config
from scripts.lib.jquants_client import JQuantsClient
from scripts.pipeline.manage_all_market_microstructure import (
    atomic_write_json,
    create_s3_client,
    get_s3_json,
    prefixed_key,
    put_json_verified,
    sha256_bytes,
    sha256_file,
)


EARNINGS_DATE_DATASET_ID = "jquants_fins_earnings_date"
FINS_SUMMARY_DATASET_ID = "jquants_fins_summary"
EARNINGS_DATE_S3_ROOT = "jquants/all_market_earnings_date"
FINS_SUMMARY_S3_ROOT = "jquants/all_market_fins_summary"
ROOT_MANIFEST_PATH = ROOT / "data" / "parquet" / "manifest.json"
FILE_TOKEN_RE = re.compile(r"_(\d{6}|\d{8})\.csv\.gz$")

EARNINGS_DATE_COLUMNS = (
    "PubDate",
    "SchDate",
    "FQName",
    "FYE",
    "Code",
    "CoName",
    "CoNameEn",
)

FINS_SUMMARY_COLUMNS = (
    "DiscDate", "DiscTime", "Code", "DiscNo", "DocType", "CurPerType",
    "CurPerSt", "CurPerEn", "CurFYSt", "CurFYEn", "NxtFYSt", "NxtFYEn",
    "Sales", "OP", "OdP", "NP", "EPS", "DEPS", "TA", "Eq", "EqAR",
    "BPS", "CFO", "CFI", "CFF", "CashEq", "Div1Q", "Div2Q", "Div3Q",
    "DivFY", "DivAnn", "DivUnit", "DivTotalAnn", "PayoutRatioAnn", "FDiv1Q",
    "FDiv2Q", "FDiv3Q", "FDivFY", "FDivAnn", "FDivUnit", "FDivTotalAnn",
    "FPayoutRatioAnn", "NxFDiv1Q", "NxFDiv2Q", "NxFDiv3Q", "NxFDivFY",
    "NxFDivAnn", "NxFDivUnit", "NxFPayoutRatioAnn", "FSales2Q", "FOP2Q",
    "FOdP2Q", "FNP2Q", "FEPS2Q", "NxFSales2Q", "NxFOP2Q", "NxFOdP2Q",
    "NxFNp2Q", "NxFEPS2Q", "FSales", "FOP", "FOdP", "FNP", "FEPS",
    "NxFSales", "NxFOP", "NxFOdP", "NxFNp", "NxFEPS", "MatChgSub",
    "SigChgInC", "ChgByASRev", "ChgNoASRev", "ChgAcEst", "RetroRst",
    "ShOutFY", "TrShFY", "AvgSh", "NCSales", "NCOP", "NCOdP", "NCNP",
    "NCEPS", "NCTA", "NCEq", "NCEqAR", "NCBPS", "FNCSales2Q", "FNCOP2Q",
    "FNCOdP2Q", "FNCNP2Q", "FNCEPS2Q", "NxFNCSales2Q", "NxFNCOP2Q",
    "NxFNCOdP2Q", "NxFNCNP2Q", "NxFNCEPS2Q", "FNCSales", "FNCOP",
    "FNCOdP", "FNCNP", "FNCEPS", "NxFNCSales", "NxFNCOP", "NxFNCOdP",
    "NxFNCNP", "NxFNCEPS", "ShEq", "NCShEq", "ROE", "NCROE",
)


@dataclass(frozen=True)
class DatasetSpec:
    dataset_id: str
    endpoint: str
    local_root: Path
    s3_root: str
    filename_prefix: str
    date_column: str
    columns: tuple[str, ...]
    required_columns: tuple[str, ...]
    sort_columns: tuple[str, ...]
    unique_columns: tuple[str, ...]


DATASETS = (
    DatasetSpec(
        dataset_id=EARNINGS_DATE_DATASET_ID,
        endpoint="/fins/earnings-date",
        local_root=ROOT / "data/research/jquants_all_market/earnings_date/raw",
        s3_root=EARNINGS_DATE_S3_ROOT,
        filename_prefix="fins_earnings-date",
        date_column="PubDate",
        columns=EARNINGS_DATE_COLUMNS,
        required_columns=EARNINGS_DATE_COLUMNS,
        sort_columns=("PubDate", "Code", "FQName", "FYE", "SchDate"),
        unique_columns=EARNINGS_DATE_COLUMNS,
    ),
    DatasetSpec(
        dataset_id=FINS_SUMMARY_DATASET_ID,
        endpoint="/fins/summary",
        local_root=ROOT / "data/research/jquants_all_market/fins_summary/raw",
        s3_root=FINS_SUMMARY_S3_ROOT,
        filename_prefix="fins_summary",
        date_column="DiscDate",
        columns=FINS_SUMMARY_COLUMNS,
        required_columns=(
            "DiscDate",
            "DiscTime",
            "Code",
            "DiscNo",
            "DocType",
            "CurPerType",
            "CurFYEn",
        ),
        sort_columns=("DiscDate", "DiscTime", "Code", "DiscNo"),
        unique_columns=("DiscNo",),
    ),
)


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def manifest_path(spec: DatasetSpec) -> Path:
    return spec.local_root / "_bulk_manifest.json"


def remote_manifest_key(spec: DatasetSpec) -> str:
    return f"{spec.s3_root}/_manifest.json"


def filename_for_date(spec: DatasetSpec, query_date: str) -> str:
    return f"{spec.filename_prefix}_{query_date.replace('-', '')}.csv.gz"


def immutable_object_key(spec: DatasetSpec, filename: str, digest: str) -> str:
    return f"{spec.s3_root}/objects/{filename}/{digest}.csv.gz"


def empty_manifest(spec: DatasetSpec) -> dict[str, Any]:
    return {
        "schema_version": 2,
        "dataset": spec.dataset_id,
        "source_endpoint": spec.endpoint,
        "root_key": spec.s3_root,
        "storage_format": "csv.gz",
        "partition_key": spec.date_column,
        "write_policy": "logical_upsert_immutable_objects",
        "fetched_at": now_iso(),
        "files": [],
        "checks": {},
    }


def normalize_manifest(payload: dict[str, Any], spec: DatasetSpec) -> dict[str, Any]:
    if payload.get("dataset") != spec.dataset_id:
        raise RuntimeError(
            f"dataset mismatch: {payload.get('dataset')} != {spec.dataset_id}"
        )
    if payload.get("source_endpoint") != spec.endpoint:
        raise RuntimeError(
            f"endpoint mismatch: {payload.get('source_endpoint')} != {spec.endpoint}"
        )
    if not isinstance(payload.get("files"), list):
        raise RuntimeError(f"{spec.dataset_id}: manifest files must be a list")
    result = dict(payload)
    result["schema_version"] = 2
    result["root_key"] = spec.s3_root
    result["storage_format"] = "csv.gz"
    result["partition_key"] = spec.date_column
    result["write_policy"] = "logical_upsert_immutable_objects"
    if not isinstance(result.get("checks"), dict):
        result["checks"] = {}
    return result


def load_local_manifest(spec: DatasetSpec) -> dict[str, Any]:
    path = manifest_path(spec)
    if not path.exists():
        return empty_manifest(spec)
    return normalize_manifest(json.loads(path.read_text(encoding="utf-8")), spec)


def load_working_manifest(
    spec: DatasetSpec,
    *,
    s3=None,
    cfg: S3Config | None = None,
) -> dict[str, Any]:
    if s3 is not None and cfg is not None:
        remote = get_s3_json(s3, cfg, remote_manifest_key(spec))
        if remote is not None:
            return normalize_manifest(remote, spec)
    return load_local_manifest(spec)


def file_token(filename: str) -> str:
    match = FILE_TOKEN_RE.search(filename)
    if not match:
        raise RuntimeError(f"unexpected earnings filename: {filename}")
    return match.group(1)


def validate_frame(
    frame: pd.DataFrame,
    spec: DatasetSpec,
    *,
    expected_date: str | None,
) -> pd.DataFrame:
    missing = sorted(set(spec.required_columns) - set(frame.columns))
    if missing:
        raise RuntimeError(f"{spec.dataset_id}: missing columns: {missing}")
    extras = [column for column in frame.columns if column not in spec.columns]
    ordered = [column for column in spec.columns if column in frame.columns] + sorted(extras)
    result = frame[ordered].copy()
    for column in result.columns:
        result[column] = result[column].fillna("").astype(str).str.strip()

    dates = pd.to_datetime(result[spec.date_column], errors="raise")
    normalized_dates = dates.dt.strftime("%Y-%m-%d")
    if expected_date is not None and set(normalized_dates.unique()) != {expected_date}:
        raise RuntimeError(
            f"{spec.dataset_id}: expected {expected_date}, got "
            f"{sorted(normalized_dates.unique())}"
        )
    result[spec.date_column] = normalized_dates

    codes = result["Code"]
    if not codes.str.len().eq(5).all():
        raise RuntimeError(f"{spec.dataset_id}: Code must have five characters")
    if result.duplicated(list(spec.unique_columns)).any():
        raise RuntimeError(
            f"{spec.dataset_id}: duplicate keys: {list(spec.unique_columns)}"
        )
    return result.sort_values(list(spec.sort_columns)).reset_index(drop=True)


def read_and_validate_file(path: Path, spec: DatasetSpec) -> dict[str, Any]:
    token = file_token(path.name)
    frame = pd.read_csv(path, compression="gzip", dtype=str, keep_default_na=False)
    validated = validate_frame(frame, spec, expected_date=None)
    values = validated[spec.date_column]
    if len(token) == 8 and (validated.empty or set(values) != {
        f"{token[:4]}-{token[4:6]}-{token[6:8]}"
    }):
        raise RuntimeError(f"{path.name}: filename/date mismatch")
    if len(token) == 6 and (
        validated.empty or not values.str.startswith(f"{token[:4]}-{token[4:6]}").all()
    ):
        raise RuntimeError(f"{path.name}: filename/month mismatch")
    return {
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "rows": len(validated),
        "columns": list(validated.columns),
        "date_min": values.min() if not validated.empty else None,
        "date_max": values.max() if not validated.empty else None,
    }


def deterministic_gzip_csv(frame: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".partial")
    temporary.unlink(missing_ok=True)
    with temporary.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with io.TextIOWrapper(compressed, encoding="utf-8", newline="") as text:
                frame.to_csv(text, index=False, lineterminator="\n")
    os.replace(temporary, destination)


def fetch_date_rows(
    client: JQuantsClient,
    spec: DatasetSpec,
    query_date: str,
    *,
    retries: int = 3,
) -> list[dict[str, Any]]:
    for attempt in range(1, retries + 1):
        try:
            return client.request_with_pagination(
                spec.endpoint,
                params={"date": query_date},
                max_pages=100,
                timeout=60,
            )
        except Exception:  # noqa: BLE001
            if attempt == retries:
                raise
            time.sleep(5 * attempt)
    raise AssertionError("unreachable")


def run_jquants(args: list[str]) -> str:
    completed = subprocess.run(
        ["jquants", *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=300,
        check=False,
    )
    if completed.returncode != 0:
        message = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"jquants {' '.join(args)} failed: {message[:500]}")
    return completed.stdout


def list_bulk_entries(
    spec: DatasetSpec, dates: list[str]
) -> dict[str, dict[str, Any]]:
    selected = set(dates)
    entries: dict[str, dict[str, Any]] = {}
    for month in sorted({value[:7] for value in dates}):
        output = run_jquants(
            ["-o", "json", "bulk", "list", "--endpoint", spec.endpoint, "--date", month]
        )
        for raw in json.loads(output):
            key = str(raw["Key"])
            filename = key.rsplit("/", 1)[-1]
            try:
                token = file_token(filename)
            except RuntimeError:
                continue
            if len(token) != 8:
                continue
            query_date = f"{token[:4]}-{token[4:6]}-{token[6:8]}"
            if query_date in selected:
                entries[query_date] = {
                    "key": key,
                    "filename": filename,
                    "last_modified": str(raw.get("LastModified", "")),
                    "source_size": int(float(raw.get("Size", 0))),
                }
    return entries


def download_bulk_file(raw_key: str, destination: Path) -> None:
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
    try:
        with gzip.open(temporary, "rb") as stream:
            for _ in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                pass
    except (EOFError, OSError) as error:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"invalid bulk gzip: {raw_key}") from error
    os.replace(temporary, destination)


def upsert_file_record(
    manifest: dict[str, Any],
    spec: DatasetSpec,
    path: Path,
    stats: dict[str, Any],
    *,
    query_date: str,
) -> dict[str, Any]:
    files = {str(item["filename"]): dict(item) for item in manifest["files"]}
    previous = files.get(path.name)
    revision = int((previous or {}).get("revision", 0))
    if previous and previous.get("sha256") != stats["sha256"]:
        revision += 1
    record = {
        **(previous or {}),
        "key": f"api:{spec.endpoint}?date={query_date}",
        "last_modified": now_iso(),
        "bytes": stats["bytes"],
        "filename": path.name,
        "sha256": stats["sha256"],
        "rows": stats["rows"],
        "columns": stats["columns"],
        "query_date": query_date,
        "date_min": stats["date_min"],
        "date_max": stats["date_max"],
        "source": "jquants_date_api",
        "revision": revision,
        "s3_key": immutable_object_key(spec, path.name, stats["sha256"]),
    }
    if previous and previous.get("sha256") != stats["sha256"]:
        record["previous_sha256"] = previous.get("sha256")
    files[path.name] = record
    manifest["files"] = [files[name] for name in sorted(files)]
    manifest["fetched_at"] = now_iso()
    return record


def update_check(
    manifest: dict[str, Any],
    query_date: str,
    *,
    rows: int,
    status: str,
) -> None:
    manifest["checks"][query_date] = {
        "checked_at": now_iso(),
        "rows": rows,
        "status": status,
    }
    manifest["checks"] = dict(sorted(manifest["checks"].items()))


def upload_immutable_file(
    s3,
    cfg: S3Config,
    path: Path,
    relative_key: str,
    digest: str,
) -> None:
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
    if head is not None:
        if (
            int(head["ContentLength"]) == path.stat().st_size
            and (head.get("Metadata") or {}).get("sha256") == digest
        ):
            print(f"  S3 cache hit: {relative_key}")
            return
        raise RuntimeError(f"immutable S3 collision: {relative_key}")

    s3.upload_file(
        str(path),
        cfg.bucket,
        key,
        ExtraArgs={
            "ContentType": "application/gzip",
            "CacheControl": "max-age=60",
            "ServerSideEncryption": "AES256",
            "Metadata": {"sha256": digest},
            "ChecksumAlgorithm": "SHA256",
        },
    )
    head = s3.head_object(Bucket=cfg.bucket, Key=key, ChecksumMode="ENABLED")
    if int(head["ContentLength"]) != path.stat().st_size:
        raise RuntimeError(f"S3 size verification failed: {relative_key}")
    if (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"S3 SHA verification failed: {relative_key}")
    expected = base64.b64encode(bytes.fromhex(digest)).decode("ascii")
    remote = head.get("ChecksumSHA256")
    if remote and "-" not in remote and remote != expected:
        raise RuntimeError(f"S3 checksum verification failed: {relative_key}")
    print(f"  uploaded: {relative_key}")


def publish_dataset_manifest(
    s3,
    cfg: S3Config,
    spec: DatasetSpec,
    manifest: dict[str, Any],
) -> None:
    path = manifest_path(spec)
    atomic_write_json(path, manifest)
    manifest_digest = sha256_file(path)
    put_json_verified(s3, cfg, remote_manifest_key(spec), manifest)

    root = get_s3_json(s3, cfg, "manifest.json") or {"files": {}}
    datasets = root.setdefault("datasets", {})
    daily_dates = sorted(
        token
        for item in manifest["files"]
        for token in [file_token(str(item["filename"]))]
        if len(token) == 8
    )
    datasets[spec.dataset_id] = {
        "manifest_key": remote_manifest_key(spec),
        "manifest_sha256": manifest_digest,
        "protected": True,
        "canonical": True,
        "write_policy": "logical_upsert_immutable_objects",
        "file_count": len(manifest["files"]),
        "latest_source_date": (
            f"{daily_dates[-1][:4]}-{daily_dates[-1][4:6]}-{daily_dates[-1][6:8]}"
            if daily_dates
            else None
        ),
        "updated_at": now_iso(),
    }
    root["generated_at"] = now_iso()
    put_json_verified(s3, cfg, "manifest.json", root)
    atomic_write_json(ROOT_MANIFEST_PATH, root)


def date_range(date_from: str, date_to: str) -> list[str]:
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)
    if end < start:
        raise ValueError(f"invalid date range: {date_from}..{date_to}")
    return [
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def update_dates(args: argparse.Namespace) -> int:
    dates = date_range(args.date_from, args.date_to)
    cfg = None if args.no_upload else load_s3_config()
    s3 = None if cfg is None else create_s3_client(cfg)
    client = JQuantsClient()

    print(f"earnings event update: {dates[0]}..{dates[-1]} upload={not args.no_upload}")
    for spec in DATASETS:
        manifest = load_working_manifest(spec, s3=s3, cfg=cfg)
        changed: list[tuple[Path, dict[str, Any]]] = []
        for query_date in dates:
            rows = fetch_date_rows(client, spec, query_date)
            if not rows:
                update_check(manifest, query_date, rows=0, status="empty_response")
                print(f"  {spec.dataset_id} {query_date}: 0 rows")
                continue
            frame = validate_frame(
                pd.DataFrame(rows), spec, expected_date=query_date
            )
            path = spec.local_root / filename_for_date(spec, query_date)
            deterministic_gzip_csv(frame, path)
            stats = read_and_validate_file(path, spec)
            record = upsert_file_record(
                manifest, spec, path, stats, query_date=query_date
            )
            update_check(manifest, query_date, rows=len(frame), status="complete")
            changed.append((path, record))
            print(
                f"  {spec.dataset_id} {query_date}: {len(frame):,} rows "
                f"sha256={stats['sha256']}"
            )

        atomic_write_json(manifest_path(spec), manifest)
        if s3 is not None and cfg is not None:
            for path, record in changed:
                upload_immutable_file(
                    s3, cfg, path, str(record["s3_key"]), str(record["sha256"])
                )
            publish_dataset_manifest(s3, cfg, spec, manifest)
    return 0


def close_update(args: argparse.Namespace) -> int:
    target = args.as_of_date or datetime.now().astimezone().date().isoformat()
    args.date_from = target
    args.date_to = target
    return update_dates(args)


def reconcile_update(args: argparse.Namespace) -> int:
    target = date.fromisoformat(
        args.as_of_date or datetime.now().astimezone().date().isoformat()
    )
    dates = date_range(
        (target - timedelta(days=args.lookback_days)).isoformat(),
        (target - timedelta(days=1)).isoformat(),
    )
    cfg = None if args.no_upload else load_s3_config()
    s3 = None if cfg is None else create_s3_client(cfg)
    print(
        f"earnings bulk reconcile: {dates[0]}..{dates[-1]} "
        f"upload={not args.no_upload}"
    )
    for spec in DATASETS:
        manifest = load_working_manifest(spec, s3=s3, cfg=cfg)
        listed = {str(item["filename"]): dict(item) for item in manifest["files"]}
        entries = list_bulk_entries(spec, dates)
        changed: list[tuple[Path, dict[str, Any]]] = []
        for query_date in dates:
            entry = entries.get(query_date)
            if entry is None:
                update_check(
                    manifest, query_date, rows=0, status="bulk_file_unavailable"
                )
                continue
            previous = listed.get(str(entry["filename"]))
            if (
                previous
                and previous.get("key") == entry["key"]
                and previous.get("last_modified") == entry["last_modified"]
                and int(previous.get("bytes", -1)) == entry["source_size"]
            ):
                update_check(
                    manifest,
                    query_date,
                    rows=int(previous.get("rows", 0)),
                    status="bulk_current",
                )
                print(f"  {spec.dataset_id} {query_date}: bulk current")
                continue
            path = spec.local_root / str(entry["filename"])
            download_bulk_file(str(entry["key"]), path)
            stats = read_and_validate_file(path, spec)
            record = upsert_file_record(
                manifest, spec, path, stats, query_date=query_date
            )
            record.update(
                {
                    "key": entry["key"],
                    "last_modified": entry["last_modified"],
                    "source": "jquants_bulk_csv",
                    "source_size": entry["source_size"],
                }
            )
            update_check(
                manifest, query_date, rows=int(stats["rows"]), status="bulk_complete"
            )
            changed.append((path, record))
            listed[path.name] = record
            print(
                f"  {spec.dataset_id} {query_date}: bulk {stats['rows']:,} rows "
                f"sha256={stats['sha256']}"
            )
        atomic_write_json(manifest_path(spec), manifest)
        if s3 is not None and cfg is not None:
            for path, record in changed:
                upload_immutable_file(
                    s3, cfg, path, str(record["s3_key"]), str(record["sha256"])
                )
            publish_dataset_manifest(s3, cfg, spec, manifest)
    return 0


def bootstrap(args: argparse.Namespace) -> int:
    cfg = None if args.no_upload else load_s3_config()
    s3 = None if cfg is None else create_s3_client(cfg)
    for spec in DATASETS:
        manifest = load_local_manifest(spec)
        physical = {path.name: path for path in spec.local_root.glob("*.csv.gz")}
        listed = {str(item["filename"]): dict(item) for item in manifest["files"]}
        if set(physical) != set(listed):
            raise RuntimeError(
                f"{spec.dataset_id}: physical/manifest mismatch "
                f"missing={sorted(set(listed) - set(physical))} "
                f"unlisted={sorted(set(physical) - set(listed))}"
            )
        refreshed: list[dict[str, Any]] = []
        for index, filename in enumerate(sorted(physical), 1):
            path = physical[filename]
            stats = read_and_validate_file(path, spec)
            item = listed[filename]
            if item.get("sha256") != stats["sha256"]:
                raise RuntimeError(f"{spec.dataset_id}: SHA mismatch: {filename}")
            item.update(stats)
            item["s3_key"] = immutable_object_key(spec, filename, stats["sha256"])
            item.setdefault("revision", 0)
            refreshed.append(item)
            if s3 is not None and cfg is not None:
                upload_immutable_file(
                    s3, cfg, path, str(item["s3_key"]), str(item["sha256"])
                )
            if index % 25 == 0 or index == len(physical):
                print(f"  {spec.dataset_id}: verified {index}/{len(physical)}")
        manifest["files"] = refreshed
        manifest["fetched_at"] = now_iso()
        atomic_write_json(manifest_path(spec), manifest)
        if s3 is not None and cfg is not None:
            publish_dataset_manifest(s3, cfg, spec, manifest)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    update = subparsers.add_parser("update", help="Fetch an inclusive date range.")
    update.add_argument("--from", dest="date_from", required=True)
    update.add_argument("--to", dest="date_to", required=True)
    update.add_argument("--no-upload", action="store_true")
    update.set_defaults(handler=update_dates)

    close = subparsers.add_parser("close", help="Fetch the current calendar date.")
    close.add_argument("--as-of-date")
    close.add_argument("--no-upload", action="store_true")
    close.set_defaults(handler=close_update)

    reconcile = subparsers.add_parser(
        "reconcile", help="Re-fetch prior calendar dates for delayed disclosures."
    )
    reconcile.add_argument("--as-of-date")
    reconcile.add_argument("--lookback-days", type=int, default=7)
    reconcile.add_argument("--no-upload", action="store_true")
    reconcile.set_defaults(handler=reconcile_update)

    boot = subparsers.add_parser(
        "bootstrap", help="Validate and publish the complete local history."
    )
    boot.add_argument("--no-upload", action="store_true")
    boot.set_defaults(handler=bootstrap)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
