#!/usr/bin/env python3
"""
Upload parquet artifacts and manifest described by data/parquet/manifest.json to S3.
Used by CI workflows to reconcile S3 with the generated manifest.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

import boto3

# Ensure project root is importable when running via `python scripts/...`
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common_cfg.manifest import load_manifest_items

DEFAULT_MANIFEST = Path("data/parquet/manifest.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST, help="Path to manifest.json")
    parser.add_argument("--bucket", required=True, help="Destination S3 bucket")
    parser.add_argument(
        "--prefix",
        default="parquet/",
        help="Destination S3 prefix (default: parquet/). Trailing slash is optional.",
    )
    parser.add_argument("--region", default=None, help="AWS region (optional; falls back to AWS env/credentials config)")
    parser.add_argument(
        "--no-delete",
        dest="delete",
        action="store_false",
        help="Do not delete extra objects that are not listed in the manifest.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show planned actions without calling S3 APIs.")
    return parser.parse_args()


def _content_type_for(key: str) -> str:
    if key.endswith(".json"):
        return "application/json"
    if key.endswith(".parquet"):
        return "application/octet-stream"
    return "application/octet-stream"


def _iter_existing_keys(s3_client, bucket: str, prefix: str) -> Iterable[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def main() -> int:
    args = parse_args()
    manifest_path: Path = args.manifest

    if not manifest_path.exists():
        print(f"manifest not found: {manifest_path}", file=sys.stderr)
        return 1

    items = load_manifest_items(manifest_path)
    if not items:
        print("manifest has no items to upload.", file=sys.stderr)
        return 1

    prefix = args.prefix.rstrip("/") + "/" if args.prefix else ""
    base_dir = manifest_path.parent
    bucket = args.bucket

    try:
        session = boto3.session.Session(region_name=args.region)
        s3 = session.client("s3")
    except Exception as exc:
        print(f"failed to initialize boto3 session/client: {exc}", file=sys.stderr)
        return 1

    uploaded_keys: list[str] = []
    for entry in items:
        key = entry.get("key")
        if not key:
            print("manifest entry missing key; aborting.", file=sys.stderr)
            return 1
        local_path = base_dir / key
        if not local_path.exists():
            print(f"local file missing (listed in manifest): {local_path}", file=sys.stderr)
            return 1
        dest_key = f"{prefix}{key}"
        ct = _content_type_for(dest_key)
        if args.dry_run:
            print(f"[dry-run] PUT s3://{bucket}/{dest_key} <- {local_path}")
        else:
            extra = {
                "ServerSideEncryption": "AES256",
                "CacheControl": "max-age=60",
                "ContentType": ct,
            }
            s3.upload_file(str(local_path), bucket, dest_key, ExtraArgs=extra)
            print(f"[PUT] s3://{bucket}/{dest_key}")
        uploaded_keys.append(dest_key)

    manifest_dest = f"{prefix}manifest.json"
    if args.dry_run:
        print(f"[dry-run] PUT s3://{bucket}/{manifest_dest} <- {manifest_path}")
    else:
        extra = {
            "ServerSideEncryption": "AES256",
            "CacheControl": "max-age=60",
            "ContentType": "application/json",
        }
        s3.upload_file(str(manifest_path), bucket, manifest_dest, ExtraArgs=extra)
        print(f"[PUT] s3://{bucket}/{manifest_dest}")
    uploaded_keys.append(manifest_dest)

    if args.delete:
        existing_keys = list(_iter_existing_keys(s3, bucket, prefix))
        extras = sorted(set(existing_keys) - set(uploaded_keys))
        if extras:
            print(f"Deleting {len(extras)} extra object(s) not present in manifest...")
            if args.dry_run:
                for key in extras:
                    print(f"[dry-run] DEL s3://{bucket}/{key}")
            else:
                for idx in range(0, len(extras), 1000):
                    batch = extras[idx : idx + 1000]
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": key} for key in batch]})
                    for key in batch:
                        print(f"[DEL] s3://{bucket}/{key}")
        else:
            print("No extra S3 objects to delete.")
    else:
        print("Skipping deletion of extra S3 objects (--no-delete).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
