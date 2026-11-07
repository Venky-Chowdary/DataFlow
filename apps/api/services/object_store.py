"""Object storage for uploaded file staging (MinIO / S3-compatible with local fallback).

Railway multi-replica HA requires uploads to be readable from any Worker.
When object store is configured, stage_bytes uploads and materialize_local
downloads to a cache path on the claiming worker.
"""

from __future__ import annotations

import logging
import os
from services.brand_env import getenv_brand
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from services.config import settings

_logger = logging.getLogger(__name__)

_client: Any | None = None
_bucket_ready = False
_backend: str | None = None  # "minio" | "s3" | None


def _disabled() -> bool:
    return getenv_brand("DISABLE_OBJECT_STORE", "").lower() in ("1", "true", "yes")


def _s3_env_configured() -> bool:
    return bool(
        getenv_brand("S3_BUCKET")
        or os.getenv("AWS_S3_BUCKET")
        or os.getenv("S3_BUCKET")
    )


def _get_boto3_client() -> Any | None:
    try:
        import boto3
        from botocore.client import Config

        endpoint = (
            getenv_brand("S3_ENDPOINT")
            or os.getenv("AWS_ENDPOINT_URL")
            or os.getenv("MINIO_ENDPOINT")
            or ""
        ).strip()
        if endpoint and "://" not in endpoint:
            secure = getenv_brand("S3_SECURE", os.getenv("MINIO_SECURE", "0")).lower() in (
                "1",
                "true",
                "yes",
            )
            endpoint = f"{'https' if secure else 'http'}://{endpoint}"
        region = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or "us-east-1"
        kwargs: dict[str, Any] = {
            "service_name": "s3",
            "region_name": region,
            "config": Config(s3={"addressing_style": "path" if getenv_brand("S3_PATH_STYLE", "1") != "0" else "auto"}),
        }
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        access = (
            getenv_brand("S3_ACCESS_KEY")
            or os.getenv("AWS_ACCESS_KEY_ID")
            or os.getenv("MINIO_ACCESS_KEY")
            or settings.minio_access_key
        )
        secret = (
            getenv_brand("S3_SECRET_KEY")
            or os.getenv("AWS_SECRET_ACCESS_KEY")
            or os.getenv("MINIO_SECRET_KEY")
            or settings.minio_secret_key
        )
        if access and secret:
            kwargs["aws_access_key_id"] = access
            kwargs["aws_secret_access_key"] = secret
        return boto3.client(**kwargs)
    except Exception:
        _logger.debug("boto3 S3 client unavailable", exc_info=True)
        return None


def _get_client() -> Any | None:
    global _client, _backend
    if _disabled():
        return None
    if _client is not None:
        return _client
    # Prefer explicit MinIO SDK when MINIO_ENDPOINT is set (dev compose).
    if os.getenv("MINIO_ENDPOINT") or not _s3_env_configured():
        try:
            from minio import Minio

            _client = Minio(
                settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
                secure=settings.minio_secure,
            )
            _backend = "minio"
            return _client
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    if _s3_env_configured() or os.getenv("MINIO_ENDPOINT"):
        client = _get_boto3_client()
        if client is not None:
            _client = client
            _backend = "s3"
            return _client
    return None


def _bucket_name() -> str:
    return (
        getenv_brand("S3_BUCKET")
        or os.getenv("AWS_S3_BUCKET")
        or os.getenv("S3_BUCKET")
        or settings.minio_bucket
    )


def _ensure_bucket(client: Any) -> bool:
    global _bucket_ready
    if _bucket_ready:
        return True
    bucket = _bucket_name()
    try:
        if _backend == "minio":
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
        else:
            try:
                client.head_bucket(Bucket=bucket)
            except Exception:
                client.create_bucket(Bucket=bucket)
        _bucket_ready = True
        return True
    except Exception:
        _logger.debug("ensure_bucket failed for %s", bucket, exc_info=True)
        return False


_LOCAL_BUCKET = "local"


def blob_root() -> Path | None:
    """Shared filesystem stand-in for object storage.

    Set ``BLOB_ROOT`` when the install has no S3 bucket and more than one
    process must read the same upload. Unset it when ``S3_BUCKET`` is the
    store — a set root is used first so a compose volume does not depend on
    MinIO being up.
    """
    raw = (getenv_brand("BLOB_ROOT", "") or "").strip()
    if not raw:
        return None
    path = Path(raw)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _local_blob_path(key: str) -> Path | None:
    root = blob_root()
    if root is None or not key:
        return None
    parts = Path(key).parts
    if not parts or any(part in ("", ".", "..") for part in parts):
        return None
    dest = (root / key).resolve()
    root_resolved = root.resolve()
    if dest != root_resolved and root_resolved not in dest.parents:
        return None
    return dest


def _stage_blob_root(key: str, content: bytes) -> str | None:
    dest = _local_blob_path(key)
    if dest is None:
        return None
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(content)
    return f"s3://{_LOCAL_BUCKET}/{key}"


def stage_bytes(key: str, content: bytes, content_type: str = "application/octet-stream") -> str | None:
    """Upload bytes to object store. Returns s3:// URI or None if unavailable."""
    if _disabled():
        return None
    rooted = _stage_blob_root(key, content)
    if rooted:
        return rooted
    client = _get_client()
    if not client or not _ensure_bucket(client):
        return None
    bucket = _bucket_name()
    try:
        if _backend == "minio":
            client.put_object(
                bucket,
                key,
                BytesIO(content),
                length=len(content),
                content_type=content_type,
            )
        else:
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=content,
                ContentType=content_type,
            )
        return f"s3://{bucket}/{key}"
    except Exception:
        _logger.debug("stage_bytes failed for %s", key, exc_info=True)
        return None


def fetch_bytes(uri: str) -> bytes | None:
    """Download object bytes from an s3:// URI."""
    if not uri or not uri.startswith("s3://"):
        return None
    parsed = urlparse(uri)
    if parsed.netloc == _LOCAL_BUCKET:
        path = _local_blob_path(parsed.path.lstrip("/"))
        if path is None or not path.is_file():
            return None
        return path.read_bytes()
    client = _get_client()
    if not client:
        return None
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        return None
    try:
        if _backend == "minio":
            resp = client.get_object(bucket, key)
            try:
                return resp.read()
            finally:
                resp.close()
                resp.release_conn()
        resp = client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except Exception:
        _logger.debug("fetch_bytes failed for %s", uri, exc_info=True)
        return None


def materialize_local(uri: str, dest: Path) -> Path | None:
    """Download ``uri`` to ``dest`` so local file readers can open it."""
    data = fetch_bytes(uri)
    if data is None:
        return None
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return dest


def storage_status() -> dict[str, str | bool]:
    client = _get_client()
    if not client:
        return {"backend": "local", "available": False}
    ok = _ensure_bucket(client)
    return {
        "backend": _backend or "unknown",
        "available": ok,
        "endpoint": getenv_brand("S3_ENDPOINT") or settings.minio_endpoint,
        "bucket": _bucket_name(),
    }


# ------------------------------------------------- keyed file operations ----
# Journals and other multi-file artifacts need more than single-blob staging:
# stream a file up without loading it, list a prefix, and delete a prefix.


def _blob_or_client() -> tuple[Path | None, Any | None]:
    if _disabled():
        return None, None
    root = blob_root()
    if root is not None:
        return root, None
    client = _get_client()
    if client is None or not _ensure_bucket(client):
        return None, None
    return None, client


def durable_backend() -> str:
    """``blob_root`` / ``minio`` / ``s3`` when keyed files survive this host, else ``""``."""
    root, client = _blob_or_client()
    if root is not None:
        return "blob_root"
    if client is not None:
        return _backend or "s3"
    return ""


def key_uri(key: str) -> str:
    root, _client = _blob_or_client()
    bucket = _LOCAL_BUCKET if root is not None else _bucket_name()
    return f"s3://{bucket}/{key}"


def stage_file(key: str, path: Path, content_type: str = "application/octet-stream") -> str | None:
    """Upload a local file under ``key`` without reading it into memory."""
    root, client = _blob_or_client()
    if root is not None:
        dest = _local_blob_path(key)
        if dest is None:
            return None
        import shutil

        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(f".{dest.name}.partial")
        shutil.copyfile(path, tmp)
        os.replace(tmp, dest)
        return f"s3://{_LOCAL_BUCKET}/{key}"
    if client is None:
        return None
    bucket = _bucket_name()
    try:
        if _backend == "minio":
            client.fput_object(bucket, key, str(path), content_type=content_type)
        else:
            client.upload_file(str(path), bucket, key, ExtraArgs={"ContentType": content_type})
        return f"s3://{bucket}/{key}"
    except Exception:
        _logger.warning("stage_file failed for %s", key, exc_info=True)
        return None


def fetch_to_file(key: str, dest: Path) -> bool:
    """Download ``key`` to ``dest`` (atomically). False when absent or unavailable."""
    root, client = _blob_or_client()
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.partial")
    try:
        if root is not None:
            src = _local_blob_path(key)
            if src is None or not src.is_file():
                return False
            import shutil

            shutil.copyfile(src, tmp)
        elif client is not None:
            if _backend == "minio":
                client.fget_object(_bucket_name(), key, str(tmp))
            else:
                client.download_file(_bucket_name(), key, str(tmp))
        else:
            return False
        os.replace(tmp, dest)
        return True
    except Exception:
        _logger.debug("fetch_to_file failed for %s", key, exc_info=True)
        try:
            tmp.unlink()
        except OSError:
            pass
        return False


def list_keys(prefix: str) -> list[str]:
    """Every key under ``prefix`` (recursive). Empty when storage is unavailable."""
    root, client = _blob_or_client()
    if root is not None:
        base = _local_blob_path(prefix.rstrip("/")) if prefix.strip("/") else root.resolve()
        if base is None or not base.is_dir():
            return []
        root_resolved = root.resolve()
        return sorted(
            p.relative_to(root_resolved).as_posix()
            for p in base.rglob("*")
            if p.is_file() and not p.name.endswith(".partial")
        )
    if client is None:
        return []
    bucket = _bucket_name()
    try:
        if _backend == "minio":
            return sorted(o.object_name for o in client.list_objects(bucket, prefix=prefix, recursive=True))
        keys: list[str] = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents") or [])
        return sorted(keys)
    except Exception:
        _logger.warning("list_keys failed for %s", prefix, exc_info=True)
        return []


def delete_prefix(prefix: str) -> int:
    """Delete every key under ``prefix``. Returns how many were removed."""
    if not prefix.strip("/"):
        raise ValueError("refusing to delete the whole store")
    keys = list_keys(prefix)
    root, client = _blob_or_client()
    removed = 0
    for key in keys:
        try:
            if root is not None:
                path = _local_blob_path(key)
                if path is not None and path.is_file():
                    path.unlink()
            elif _backend == "minio":
                client.remove_object(_bucket_name(), key)
            else:
                client.delete_object(Bucket=_bucket_name(), Key=key)
            removed += 1
        except Exception:
            _logger.warning("delete_prefix could not remove %s", key, exc_info=True)
    return removed
