from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import boto3

import export_nextjs_replay_payloads as exporter


DASH_ROOT = Path(__file__).resolve().parents[4]
PAYLOAD_ROOT = (
    DASH_ROOT
    / "data/research/grok_session_handoffs_20260718/04_technical_entry_training"
    / "output/nextjs"
)
PUBLIC_PATH = PAYLOAD_ROOT / "public.json"
RESULT_PATH = PAYLOAD_ROOT / "results.json"
DEFAULT_PREFIX = "training/replay/v1"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_and_validate() -> tuple[bytes, bytes, dict[str, Any]]:
    public_bytes = PUBLIC_PATH.read_bytes()
    result_bytes = RESULT_PATH.read_bytes()
    public_data = json.loads(public_bytes)
    result_data = json.loads(result_bytes)
    exporter.validate_payloads(public_data, result_data)
    bundle_version = sha256(public_bytes + b"\0" + result_bytes)
    manifest = {
        "version": bundle_version,
        "generatedAt": public_data.get("generatedAt"),
        "caseCount": public_data.get("caseCount"),
        "files": {
            "public.json": {
                "bytes": len(public_bytes),
                "sha256": sha256(public_bytes),
            },
            "results.json": {
                "bytes": len(result_bytes),
                "sha256": sha256(result_bytes),
            },
        },
    }
    return public_bytes, result_bytes, manifest


def build_client(region: str | None, endpoint_url: str | None):
    client_kwargs: dict[str, str] = {}
    if region:
        client_kwargs["region_name"] = region
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    return boto3.Session().client("s3", **client_kwargs)


def put_json(
    client: Any,
    *,
    bucket: str,
    key: str,
    body: bytes,
    checksum: str,
    cache_control: str,
) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
        CacheControl=cache_control,
        ServerSideEncryption="AES256",
        Metadata={"sha256": checksum},
    )
    response = client.get_object(Bucket=bucket, Key=key)
    uploaded = response["Body"].read()
    if sha256(uploaded) != checksum:
        raise RuntimeError(f"uploaded checksum mismatch: s3://{bucket}/{key}")
    print(f"verified: s3://{bucket}/{key} sha256={checksum}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload versioned trading replay payloads to private S3."
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("S3_BUCKET") or os.getenv("DATA_BUCKET"),
    )
    parser.add_argument(
        "--prefix",
        default=os.getenv("TRADING_REPLAY_S3_PREFIX") or DEFAULT_PREFIX,
    )
    parser.add_argument("--region", default=os.getenv("AWS_REGION"))
    parser.add_argument("--endpoint-url", default=os.getenv("AWS_ENDPOINT_URL"))
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform S3 writes. Without this flag, only print the upload plan.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bucket = (args.bucket or "").strip()
    if not bucket:
        raise RuntimeError("--bucket or S3_BUCKET/DATA_BUCKET is required")
    prefix = str(args.prefix).strip("/")
    public_bytes, result_bytes, manifest = load_and_validate()
    version = manifest["version"]
    version_root = f"{prefix}/versions/{version}"
    manifest_bytes = json.dumps(
        manifest,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    pointer = {
        **manifest,
        "publishedAt": datetime.now(ZoneInfo("Asia/Tokyo")).isoformat(),
    }
    pointer_bytes = json.dumps(
        pointer,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    uploads = [
        (
            f"{version_root}/public.json",
            public_bytes,
            sha256(public_bytes),
            "private, max-age=31536000, immutable",
        ),
        (
            f"{version_root}/results.json",
            result_bytes,
            sha256(result_bytes),
            "private, max-age=31536000, immutable",
        ),
        (
            f"{version_root}/manifest.json",
            manifest_bytes,
            sha256(manifest_bytes),
            "private, max-age=31536000, immutable",
        ),
        (
            f"{prefix}/current.json",
            pointer_bytes,
            sha256(pointer_bytes),
            "private, no-cache",
        ),
    ]

    print(f"bucket: {bucket}")
    print(f"version: {version}")
    print(f"cases: {manifest['caseCount']}")
    for key, body, checksum, _ in uploads:
        print(f"{'PUT' if args.execute else 'DRY'} s3://{bucket}/{key}")
        print(f"  bytes={len(body)} sha256={checksum}")
    if not args.execute:
        print("dry-run: no S3 writes performed; add --execute after approval")
        return

    client = build_client(args.region, args.endpoint_url)
    for key, body, checksum, cache_control in uploads:
        put_json(
            client,
            bucket=bucket,
            key=key,
            body=body,
            checksum=checksum,
            cache_control=cache_control,
        )
    print("upload complete: current.json was published last")


if __name__ == "__main__":
    main()
