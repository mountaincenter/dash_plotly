"""Guarded S3 publishing for the canonical Grok archive."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

from common_cfg.s3cfg import S3Config
from common_cfg.s3io import create_s3_client


ARCHIVE_NAME = "backtest/grok_trending_archive.parquet"
MANIFEST_NAME = "manifest.json"


class ProtectedArchiveError(RuntimeError):
    """Raised when canonical archive protection cannot be proven."""


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _s3_key(cfg: S3Config, name: str) -> str:
    prefix = (cfg.prefix or "").strip("/")
    return f"{prefix}/{name}" if prefix else name


def _require_client(cfg: S3Config, client: Any | None) -> Any:
    if not cfg.bucket:
        raise ProtectedArchiveError("S3 bucket is not configured")
    resolved = client or create_s3_client(cfg)
    if resolved is None:
        raise ProtectedArchiveError("Unable to create S3 client")
    return resolved


def _get_current_object_if_match(
    s3: Any,
    *,
    bucket: str,
    key: str,
    etag: str | None,
    label: str,
) -> Any:
    """Read current bytes only if they still match the preceding HEAD."""
    if not etag:
        raise ProtectedArchiveError(f"S3 {label} head has no ETag")
    try:
        return s3.get_object(Bucket=bucket, Key=key, IfMatch=etag)
    except Exception as error:
        raise ProtectedArchiveError(
            f"S3 {label} changed after HEAD; conditional download refused: {error}"
        ) from error


def read_remote_manifest(
    cfg: S3Config,
    *,
    client: Any | None = None,
) -> dict[str, Any]:
    """Read the current manifest without writing a local file."""
    return read_remote_manifest_snapshot(cfg, client=client)["manifest"]


def read_remote_manifest_snapshot(
    cfg: S3Config,
    *,
    client: Any | None = None,
) -> dict[str, Any]:
    """Read one exact manifest version and its concurrency identifiers."""
    s3 = _require_client(cfg, client)
    key = _s3_key(cfg, MANIFEST_NAME)
    head = s3.head_object(Bucket=cfg.bucket, Key=key)
    etag = head.get("ETag")
    response = _get_current_object_if_match(
        s3,
        bucket=cfg.bucket,
        key=key,
        etag=etag,
        label="manifest",
    )
    try:
        payload = response["Body"].read()
        manifest = json.loads(payload.decode("utf-8"))
    except Exception as error:
        raise ProtectedArchiveError(f"Unable to parse remote manifest: {error}") from error
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), dict):
        raise ProtectedArchiveError("Remote manifest has no files mapping")
    return {
        "manifest": manifest,
        "key": key,
        "etag": etag,
        "version_id": head.get("VersionId"),
    }


def download_verified_archive(
    cfg: S3Config,
    destination: Path,
    *,
    client: Any | None = None,
) -> dict[str, Any]:
    """Download an exact S3 version after validating its protected manifest entry."""
    s3 = _require_client(cfg, client)
    archive_key = _s3_key(cfg, ARCHIVE_NAME)
    head = s3.head_object(Bucket=cfg.bucket, Key=archive_key)
    version_id = head.get("VersionId")
    etag = head.get("ETag")
    if not etag:
        raise ProtectedArchiveError("S3 archive head has no ETag")

    response = _get_current_object_if_match(
        s3,
        bucket=cfg.bucket,
        key=archive_key,
        etag=etag,
        label="archive",
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as handle:
        body = response["Body"]
        while True:
            chunk = body.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)
        handle.flush()
        os.fsync(handle.fileno())

    sha256 = file_sha256(destination)
    manifest_snapshot = read_remote_manifest_snapshot(cfg, client=s3)
    manifest = manifest_snapshot["manifest"]
    entry = manifest["files"].get(ARCHIVE_NAME)
    if not isinstance(entry, dict):
        raise ProtectedArchiveError(
            f"Remote manifest has no protected entry for {ARCHIVE_NAME}"
        )
    if entry.get("protected") is not True or entry.get("canonical") is not True:
        raise ProtectedArchiveError("Remote archive protection flags are missing")
    if entry.get("sha256") != sha256:
        raise ProtectedArchiveError(
            "Remote archive bytes do not match the manifest SHA256"
        )
    if entry.get("s3_version_id") and entry["s3_version_id"] != version_id:
        raise ProtectedArchiveError(
            "Remote archive VersionId does not match the manifest"
        )
    if entry.get("s3_etag") and entry["s3_etag"] != etag:
        raise ProtectedArchiveError("Remote archive ETag does not match the manifest")

    return {
        "key": archive_key,
        "etag": etag,
        "version_id": version_id,
        "sha256": sha256,
        "size_bytes": destination.stat().st_size,
        "manifest_sha256": entry.get("sha256"),
        "manifest": manifest,
        "manifest_key": manifest_snapshot["key"],
        "manifest_etag": manifest_snapshot["etag"],
        "manifest_version_id": manifest_snapshot["version_id"],
    }


def publish_guarded_archive(
    cfg: S3Config,
    candidate: Path,
    source: dict[str, Any],
    *,
    backtest_date: str,
    row_count: int,
    client: Any | None = None,
) -> dict[str, Any]:
    """Conditionally replace the archive only if its source object is unchanged."""
    s3 = _require_client(cfg, client)
    archive_key = _s3_key(cfg, ARCHIVE_NAME)
    current = s3.head_object(Bucket=cfg.bucket, Key=archive_key)
    if current.get("ETag") != source.get("etag"):
        raise ProtectedArchiveError(
            "S3 archive changed after download (ETag mismatch); publish refused"
        )
    if source.get("version_id") and current.get("VersionId") != source.get("version_id"):
        raise ProtectedArchiveError(
            "S3 archive changed after download (VersionId mismatch); publish refused"
        )

    digest = hashlib.sha256(candidate.read_bytes()).digest()
    sha256 = digest.hex()
    checksum_b64 = base64.b64encode(digest).decode("ascii")
    metadata = {
        "sha256": sha256,
        "protected": "true",
        "canonical": "true",
        "data-source": "jquants-1m",
        "backtest-date": str(backtest_date),
        "row-count": str(int(row_count)),
        "segment-definition": "first-executable-open-v1",
        "phase2-definition": "open-to-official-close-mark",
        "execution-status": "separate-evaluation-field",
    }
    with candidate.open("rb") as handle:
        response = s3.put_object(
            Bucket=cfg.bucket,
            Key=archive_key,
            Body=handle,
            ContentType="application/octet-stream",
            CacheControl="max-age=60",
            ServerSideEncryption="AES256",
            Metadata=metadata,
            ChecksumSHA256=checksum_b64,
            IfMatch=source["etag"],
        )

    verified = s3.head_object(
        Bucket=cfg.bucket,
        Key=archive_key,
        ChecksumMode="ENABLED",
    )
    if verified.get("Metadata", {}).get("sha256") != sha256:
        raise ProtectedArchiveError("Published archive metadata SHA256 mismatch")
    remote_checksum = verified.get("ChecksumSHA256") or response.get("ChecksumSHA256")
    if remote_checksum and remote_checksum != checksum_b64:
        raise ProtectedArchiveError("Published archive object checksum mismatch")
    if response.get("VersionId") and verified.get("VersionId") != response.get("VersionId"):
        raise ProtectedArchiveError("Published archive VersionId verification failed")

    return {
        "status": "uploaded_and_verified",
        "s3_key": archive_key,
        "sha256": sha256,
        "size_bytes": candidate.stat().st_size,
        "row_count": int(row_count),
        "backtest_date": str(backtest_date),
        "source_s3_etag": source.get("etag"),
        "source_s3_version_id": source.get("version_id"),
        "s3_etag": verified.get("ETag"),
        "s3_version_id": verified.get("VersionId"),
        "s3_checksum_sha256_base64": remote_checksum or checksum_b64,
        "verified_at": datetime.now().astimezone().isoformat(),
        "data_source": "jquants_1m",
        "segment_definition": "first executable trade open at or after target after entry",
    }


def publish_guarded_manifest_entry(
    cfg: S3Config,
    source: dict[str, Any],
    archive_state: dict[str, Any],
    *,
    columns: list[str],
    date_min: str,
    date_max: str,
    unique_ticker_date_keys: int,
    client: Any | None = None,
) -> dict[str, Any]:
    """Conditionally advance only the protected archive entry in manifest.json."""
    s3 = _require_client(cfg, client)
    manifest_key = source.get("manifest_key") or _s3_key(cfg, MANIFEST_NAME)
    current = s3.head_object(Bucket=cfg.bucket, Key=manifest_key)
    if current.get("ETag") != source.get("manifest_etag"):
        raise ProtectedArchiveError(
            "S3 manifest changed after archive download; manifest publish refused"
        )
    if (
        source.get("manifest_version_id")
        and current.get("VersionId") != source.get("manifest_version_id")
    ):
        raise ProtectedArchiveError(
            "S3 manifest VersionId changed after archive download; publish refused"
        )

    manifest = deepcopy(source.get("manifest"))
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), dict):
        raise ProtectedArchiveError("Source manifest snapshot is invalid")
    old_entry = manifest["files"].get(ARCHIVE_NAME)
    if not isinstance(old_entry, dict) or old_entry.get("sha256") != source.get("sha256"):
        raise ProtectedArchiveError(
            "Source manifest archive entry no longer identifies the downloaded archive"
        )

    now = datetime.now().astimezone().isoformat()
    entry = dict(old_entry)
    for stale_key in ["segment_master_sha256", "segment_validation_sha256"]:
        entry.pop(stale_key, None)
    entry.update(
        {
            "exists": True,
            "size_bytes": archive_state["size_bytes"],
            "row_count": archive_state["row_count"],
            "columns": columns,
            "updated_at": now,
            "protected": True,
            "canonical": True,
            "upload_policy": "guarded_pipeline_only",
            "automatic_bulk_upload": False,
            "protection_reason": "canonical archive; checksum verification required",
            "sha256": archive_state["sha256"],
            "date_min": date_min,
            "date_max": date_max,
            "unique_ticker_date_keys": int(unique_ticker_date_keys),
            "s3_key": archive_state["s3_key"],
            "s3_etag": archive_state.get("s3_etag"),
            "s3_version_id": archive_state.get("s3_version_id"),
            "s3_checksum_sha256_base64": archive_state.get(
                "s3_checksum_sha256_base64"
            ),
            "s3_verified_at": archive_state.get("verified_at"),
            "s3_sync_status": archive_state.get("status"),
            "source_s3_sha256": source.get("sha256"),
            "source_s3_etag": archive_state.get("source_s3_etag"),
            "source_s3_version_id": archive_state.get("source_s3_version_id"),
            "data_source": archive_state.get("data_source"),
            "segment_definition": archive_state.get("segment_definition"),
            "verification": (
                f"jquants_unadjusted_ohlcv_{archive_state['row_count']}_of_"
                f"{archive_state['row_count']}"
            ),
            "segment_verification": (
                f"jquants_execution_v1_{archive_state['row_count']}_keys_passed"
            ),
        }
    )
    for field in [
        "phase1_mark_status_verification",
        "close_execution_status_verification",
        "phase2_seg1530_definition",
        "local_revision_reason",
    ]:
        if archive_state.get(field) is not None:
            entry[field] = archive_state[field]
    manifest["files"][ARCHIVE_NAME] = entry
    manifest["generated_at"] = now
    manifest["canonical_archive_updated_at"] = now

    payload = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode(
        "utf-8"
    )
    checksum_b64 = base64.b64encode(hashlib.sha256(payload).digest()).decode("ascii")
    response = s3.put_object(
        Bucket=cfg.bucket,
        Key=manifest_key,
        Body=payload,
        ContentType="application/json",
        CacheControl="no-cache",
        ServerSideEncryption="AES256",
        ChecksumSHA256=checksum_b64,
        IfMatch=source["manifest_etag"],
    )
    verified = s3.head_object(
        Bucket=cfg.bucket,
        Key=manifest_key,
        ChecksumMode="ENABLED",
    )
    if response.get("VersionId") and verified.get("VersionId") != response.get("VersionId"):
        raise ProtectedArchiveError("Published manifest VersionId verification failed")
    remote_checksum = verified.get("ChecksumSHA256") or response.get("ChecksumSHA256")
    if remote_checksum and remote_checksum != checksum_b64:
        raise ProtectedArchiveError("Published manifest checksum mismatch")
    return {
        "manifest_s3_etag": verified.get("ETag"),
        "manifest_s3_version_id": verified.get("VersionId"),
        "manifest_s3_checksum_sha256_base64": remote_checksum or checksum_b64,
        "manifest_verified_at": now,
    }




def verify_publish_state(
    cfg: S3Config,
    state: dict[str, Any],
    *,
    client: Any | None = None,
) -> dict[str, Any]:
    """Prove that a local publish receipt still identifies the current S3 object."""
    if state.get("status") != "uploaded_and_verified" or not state.get("sha256"):
        raise ProtectedArchiveError("Archive publish receipt is incomplete")
    s3 = _require_client(cfg, client)
    head = s3.head_object(
        Bucket=cfg.bucket,
        Key=_s3_key(cfg, ARCHIVE_NAME),
        ChecksumMode="ENABLED",
    )
    checks = {
        "ETag": (head.get("ETag"), state.get("s3_etag")),
        "VersionId": (head.get("VersionId"), state.get("s3_version_id")),
        "SHA256 metadata": (
            head.get("Metadata", {}).get("sha256"),
            state.get("sha256"),
        ),
    }
    mismatches = [
        name
        for name, (actual, expected) in checks.items()
        if expected is not None and actual != expected
    ]
    if mismatches:
        raise ProtectedArchiveError(
            "Archive publish receipt no longer matches S3: " + ", ".join(mismatches)
        )
    checksum = head.get("ChecksumSHA256")
    expected_checksum = state.get("s3_checksum_sha256_base64")
    if checksum and expected_checksum and checksum != expected_checksum:
        raise ProtectedArchiveError("Archive publish receipt checksum differs from S3")
    return head


def write_publish_state(path: Path, state: dict[str, Any]) -> None:
    """Atomically save evidence consumed by update_manifest.py."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    payload = json.dumps(state, ensure_ascii=False, indent=2) + "\n"
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
