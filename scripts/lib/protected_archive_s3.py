"""Read-only canonical verification and guarded non-canonical Grok publishing."""

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
TRENDING_NAME = "grok_trending.parquet"
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


def _require_bucket_versioning(s3: Any, cfg: S3Config) -> None:
    status = s3.get_bucket_versioning(Bucket=cfg.bucket).get("Status")
    if status != "Enabled":
        raise ProtectedArchiveError(
            f"S3 bucket versioning must be Enabled, actual={status!r}"
        )


def _rollback_exact_version(
    s3: Any,
    cfg: S3Config,
    *,
    key: str,
    new_version_id: str,
    expected_etag: str | None,
    expected_version_id: str | None,
    label: str,
) -> None:
    """Delete only one failed publication version and prove restoration."""
    s3.delete_object(
        Bucket=cfg.bucket,
        Key=key,
        VersionId=new_version_id,
    )
    restored = s3.head_object(Bucket=cfg.bucket, Key=key)
    if restored.get("ETag") != expected_etag or (
        expected_version_id
        and restored.get("VersionId") != expected_version_id
    ):
        raise ProtectedArchiveError(
            f"{label} rollback did not restore the exact source object"
        )


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


def publish_guarded_trending_and_manifest(
    cfg: S3Config,
    candidate: Path,
    manifest: dict[str, Any],
    *,
    entry_metadata: dict[str, Any],
    client: Any | None = None,
) -> dict[str, Any]:
    """Publish finalized Grok bytes, then its existing-format manifest entry.

    The fixed S3 key and manifest are two objects, so the data object is written
    first and the manifest pointer last.  Bucket versioning is mandatory: if
    the manifest write fails, the exact newly-created data version is deleted
    and the previous manifest-pinned version becomes current again.
    """
    if not candidate.exists() or candidate.stat().st_size == 0:
        raise ProtectedArchiveError("Finalized grok_trending candidate is absent")
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), dict):
        raise ProtectedArchiveError("Candidate manifest has no files mapping")

    s3 = _require_client(cfg, client)
    _require_bucket_versioning(s3, cfg)
    key = _s3_key(cfg, TRENDING_NAME)
    manifest_snapshot = read_remote_manifest_snapshot(cfg, client=s3)

    current = s3.head_object(Bucket=cfg.bucket, Key=key, ChecksumMode="ENABLED")
    current_version = current.get("VersionId")
    if not current_version:
        raise ProtectedArchiveError(
            "S3 bucket versioning is required for recoverable Grok publication"
        )

    if current.get("Metadata", {}).get("publication-state") == "pending-manifest":
        raise ProtectedArchiveError(
            "Current Grok object is an unresolved pending publication"
        )

    source_reference = {
        "ETag": current.get("ETag"),
        "VersionId": current.get("VersionId"),
    }

    def current_matches_source(head: dict[str, Any]) -> bool:
        return (
            head.get("ETag") == source_reference["ETag"]
            and head.get("VersionId") == source_reference["VersionId"]
        )

    source_response = s3.get_object(
        Bucket=cfg.bucket,
        Key=key,
        VersionId=current["VersionId"],
    )
    source_bytes = source_response["Body"].read()
    source_sha256 = hashlib.sha256(source_bytes).hexdigest()

    candidate_digest = hashlib.sha256(candidate.read_bytes()).digest()
    candidate_sha256 = candidate_digest.hex()
    candidate_checksum = base64.b64encode(candidate_digest).decode("ascii")
    changed = candidate_sha256 != source_sha256
    now = datetime.now().astimezone().isoformat()

    if changed:
        object_metadata = {
            "sha256": candidate_sha256,
            "publication-state": "pending-manifest",
            "data-source": str(entry_metadata.get("data_source", "")),
            "market-cap-source": str(
                entry_metadata.get("market_cap_source", "")
            ),
            "market-cap-asof-date": str(
                entry_metadata.get("market_cap_asof_date", "")
            ),
        }
        with candidate.open("rb") as handle:
            response = s3.put_object(
                Bucket=cfg.bucket,
                Key=key,
                Body=handle,
                ContentType="application/octet-stream",
                CacheControl="max-age=60",
                ServerSideEncryption="AES256",
                Metadata=object_metadata,
                ChecksumSHA256=candidate_checksum,
                IfMatch=current["ETag"],
            )
        published = s3.head_object(
            Bucket=cfg.bucket,
            Key=key,
            ChecksumMode="ENABLED",
        )
        if not response.get("VersionId") or (
            published.get("VersionId") != response.get("VersionId")
        ):
            raise ProtectedArchiveError(
                "Finalized grok_trending VersionId verification failed"
            )
        if published.get("Metadata", {}).get("sha256") != candidate_sha256:
            raise ProtectedArchiveError(
                "Finalized grok_trending metadata SHA256 mismatch"
            )
        remote_checksum = (
            published.get("ChecksumSHA256") or response.get("ChecksumSHA256")
        )
        if remote_checksum and remote_checksum != candidate_checksum:
            raise ProtectedArchiveError(
                "Finalized grok_trending object checksum mismatch"
            )
    else:
        published = s3.head_object(
            Bucket=cfg.bucket,
            Key=key,
            ChecksumMode="ENABLED",
        )
        if not current_matches_source(published):
            raise ProtectedArchiveError(
                "Current grok_trending changed during finalization"
            )
        response = {"VersionId": current["VersionId"]}
        remote_checksum = published.get("ChecksumSHA256")

    candidate_manifest = deepcopy(manifest)
    candidate_manifest["files"][TRENDING_NAME] = {
        "exists": True,
        "size_bytes": candidate.stat().st_size,
        "row_count": int(entry_metadata["row_count"]),
        "columns": list(entry_metadata["columns"]),
        "updated_at": datetime.fromtimestamp(
            candidate.stat().st_mtime
        ).astimezone().isoformat(),
    }
    candidate_manifest["generated_at"] = now
    payload = (json.dumps(candidate_manifest, ensure_ascii=False, indent=2) + "\n").encode(
        "utf-8"
    )
    manifest_checksum = base64.b64encode(hashlib.sha256(payload).digest()).decode(
        "ascii"
    )

    try:
        manifest_response = s3.put_object(
            Bucket=cfg.bucket,
            Key=manifest_snapshot["key"],
            Body=payload,
            ContentType="application/json",
            CacheControl="no-cache",
            ServerSideEncryption="AES256",
            ChecksumSHA256=manifest_checksum,
            IfMatch=manifest_snapshot["etag"],
        )
    except Exception as error:
        if changed and response.get("VersionId"):
            try:
                s3.delete_object(
                    Bucket=cfg.bucket,
                    Key=key,
                    VersionId=response["VersionId"],
                )
                restored = s3.head_object(Bucket=cfg.bucket, Key=key)
                if not current_matches_source(restored):
                    raise ProtectedArchiveError(
                        "Rollback did not restore the verified source Grok version"
                    )
            except Exception as rollback_error:
                raise ProtectedArchiveError(
                    "Manifest publish failed and Grok rollback also failed: "
                    f"publish={error}; rollback={rollback_error}"
                ) from rollback_error
        raise ProtectedArchiveError(
            f"Finalized Grok manifest publish failed; data rollback completed: {error}"
        ) from error

    verified_manifest = s3.head_object(
        Bucket=cfg.bucket,
        Key=manifest_snapshot["key"],
        ChecksumMode="ENABLED",
    )
    if manifest_response.get("VersionId") and (
        verified_manifest.get("VersionId") != manifest_response.get("VersionId")
    ):
        raise ProtectedArchiveError("Final Grok manifest VersionId verification failed")
    verified_manifest_checksum = (
        verified_manifest.get("ChecksumSHA256")
        or manifest_response.get("ChecksumSHA256")
    )
    if (
        verified_manifest_checksum
        and verified_manifest_checksum != manifest_checksum
    ):
        raise ProtectedArchiveError("Final Grok manifest checksum mismatch")

    return {
        "manifest": candidate_manifest,
        "sha256": candidate_sha256,
        "s3_etag": published.get("ETag"),
        "s3_version_id": published.get("VersionId"),
        "manifest_s3_etag": verified_manifest.get("ETag"),
        "manifest_s3_version_id": verified_manifest.get("VersionId"),
        "changed": changed,
        "verified_at": now,
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
