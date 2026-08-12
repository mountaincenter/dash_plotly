"""Read-only verification helpers for the canonical Grok archive."""

from __future__ import annotations

import hashlib
import json
import os
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
    """Refuse canonical archive publication from automated code paths."""
    raise ProtectedArchiveError(
        "Canonical grok_trending_archive.parquet is read-only; automated "
        "publication is disabled"
    )


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
    """Refuse automated advancement of the canonical archive manifest entry."""
    raise ProtectedArchiveError(
        "Canonical archive manifest entry is read-only; automated publication "
        "is disabled"
    )




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
