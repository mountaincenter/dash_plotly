# common_cfg/s3io.py
# -*- coding: utf-8 -*-
"""
common_cfg.s3io: S3 入出力
"""
from __future__ import annotations
from pathlib import Path
from .s3cfg import S3Config, load_s3_config
import sys


def _init_s3_client(cfg: S3Config):
    try:
        import boto3
    except Exception as exc:
        print(f"[WARN] boto3 import failed: {exc}", file=sys.stderr)
        return None

    session_kwargs: dict = {}
    if cfg.profile:
        session_kwargs["profile_name"] = cfg.profile
    try:
        session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()
    except Exception as exc:
        print(f"[WARN] boto3 session init failed: {exc}", file=sys.stderr)
        return None

    client_kwargs: dict = {}
    if cfg.region:
        client_kwargs["region_name"] = cfg.region
    if cfg.endpoint_url:
        client_kwargs["endpoint_url"] = cfg.endpoint_url
    try:
        return session.client("s3", **client_kwargs)
    except Exception as exc:
        print(f"[WARN] boto3 client init failed: {exc}", file=sys.stderr)
        return None

def upload_files(cfg: S3Config, files: list[Path], base_dir: Path | None = None) -> None:
    """
    Upload files to S3, preserving subdirectory structure if base_dir is provided.

    Args:
        cfg: S3 configuration
        files: List of file paths to upload
        base_dir: Base directory to calculate relative paths from (preserves subdirectories)
    """
    if not cfg.bucket:
        print("[INFO] S3 upload skipped: bucket not set.", file=sys.stderr)
        return
    s3 = _init_s3_client(cfg)
    if s3 is None:
        return

    for p in files:
        # If base_dir provided, preserve subdirectory structure
        if base_dir and p.is_relative_to(base_dir):
            relative_path = p.relative_to(base_dir)
            key = f"{cfg.prefix}{relative_path}"
        else:
            key = f"{cfg.prefix}{p.name}"

        extra = {
            "ContentType": "application/octet-stream",
            "CacheControl": "max-age=60",
            "ServerSideEncryption": "AES256",
        }
        try:
            s3.upload_file(str(p), cfg.bucket, key, ExtraArgs=extra)
            print(f"[OK] uploaded: s3://{cfg.bucket}/{key}")
        except Exception as e:
            print(f"[WARN] upload failed: {p} → s3://{cfg.bucket}/{key} : {e}", file=sys.stderr)

def download_file(cfg: S3Config, filename: str, dest: Path) -> bool:
    if not cfg.bucket:
        print("[INFO] S3 download skipped: bucket not set.", file=sys.stderr)
        return False
    s3 = _init_s3_client(cfg)
    if s3 is None:
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)
    key = f"{cfg.prefix}{filename}"
    try:
        s3.download_file(cfg.bucket, key, str(dest))
        print(f"[OK] downloaded: s3://{cfg.bucket}/{key} -> {dest}")
        return True
    except Exception as exc:
        print(f"[WARN] download failed: s3://{cfg.bucket}/{key} : {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False

def list_s3_files(cfg: S3Config) -> list[str]:
    """
    S3バケットのprefix以下のファイル一覧を取得

    Args:
        cfg: S3設定

    Returns:
        ファイル名のリスト（prefixを除いた相対パス）
    """
    if not cfg.bucket:
        print("[INFO] S3 list skipped: bucket not set.", file=sys.stderr)
        return []
    s3 = _init_s3_client(cfg)
    if s3 is None:
        return []

    try:
        response = s3.list_objects_v2(Bucket=cfg.bucket, Prefix=cfg.prefix)

        if "Contents" not in response:
            return []

        # prefixを除いたファイル名のみ返す
        files = []
        for obj in response["Contents"]:
            key = obj["Key"]
            # prefixを除去
            if key.startswith(cfg.prefix):
                filename = key[len(cfg.prefix):]
                if filename:  # 空文字列を除外
                    files.append(filename)

        return files

    except Exception as exc:
        print(f"[WARN] S3 list failed: s3://{cfg.bucket}/{cfg.prefix} : {exc}", file=sys.stderr)
        return []

def upload_file(cfg: S3Config, file_path: Path, s3_key_name: str) -> bool:
    """
    単一ファイルをS3にアップロード

    Args:
        cfg: S3設定
        file_path: アップロードするローカルファイルのパス
        s3_key_name: S3でのファイル名（prefixは自動追加される）

    Returns:
        成功/失敗
    """
    if not cfg.bucket:
        print("[INFO] S3 upload skipped: bucket not set.", file=sys.stderr)
        return False
    s3 = _init_s3_client(cfg)
    if s3 is None:
        return False

    key = f"{cfg.prefix}{s3_key_name}"
    extra = {
        "ContentType": "application/octet-stream",
        "CacheControl": "max-age=60",
        "ServerSideEncryption": "AES256",
    }

    try:
        s3.upload_file(str(file_path), cfg.bucket, key, ExtraArgs=extra)
        print(f"[OK] uploaded: s3://{cfg.bucket}/{key}")
        return True
    except Exception as e:
        print(f"[WARN] upload failed: {file_path} → s3://{cfg.bucket}/{key} : {e}", file=sys.stderr)
        return False

# ---- 後方互換ラッパー（ノートブックが直接呼ぶ想定のシグネチャ）----
def maybe_upload_files_s3(
    files: list[Path],
    *,
    bucket: str | None,
    prefix: str = "parquet/",
    aws_region: str | None = None,
    aws_profile: str | None = None,
    dry_run: bool = False,
) -> None:
    if dry_run:
        for p in files:
            print(f"[PUT] s3://{bucket}/{prefix}{p.name} (dry-run)")
        return
    cfg = load_s3_config()
    # パラメータ優先で上書き（bucketだけは必須）
    cfg = S3Config(
        bucket=bucket or cfg.bucket,
        prefix=prefix or cfg.prefix,
        region=aws_region or cfg.region,
        profile=aws_profile or cfg.profile,
        endpoint_url=cfg.endpoint_url,
    )
    upload_files(cfg, files)
