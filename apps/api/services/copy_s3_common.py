"""Shared S3/MinIO identity-COPY helpers.

Dest COUNT is ``destination_row_count`` / object-store artifact COUNT
(GET streams or Parquet footers) — never ListObjects length, never
writer PUT ack, never ``aws s3 cp`` / ``aws s3 sync``. Same
endpoint+bucket+key declines. Cross-endpoint CopyObject declines
(server-side copy cannot leave the cluster).
"""

from __future__ import annotations

from typing import Any

from connectors.aws_common import boto3_client, is_local_endpoint, resolve_endpoint_url
from connectors.object_store_common import (
    normalize_object_base_key,
    object_parts_prefix,
    object_store_read_keys,
)
from services.copy_fast_path import FastPathResult, FastPathUnavailable

COPY_SAFE_EXTS = frozenset({"json", "jsonl", "ndjson", "csv", "tsv", "parquet"})
_S3_FAMILY = frozenset({
    "s3",
    "amazon_s3",
    "aws_s3",
    "minio",
    "s3_compatible",
    "wasabi",
    "backblaze_b2",
    "digitalocean_spaces",
    "cloudflare_r2",
})


def s3_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _S3_FAMILY:
        return "s3"
    return n


def s3_ext(key: str) -> str:
    name = str(key or "").rsplit("/", 1)[-1].lower()
    if name.endswith(".ndjson"):
        return "jsonl"
    if "." not in name:
        return ""
    return name.rsplit(".", 1)[-1]


def s3_type_is_copy_safe(declared_or_key: str) -> bool:
    raw = (declared_or_key or "").strip().lower()
    if not raw:
        return True
    ext = s3_ext(raw) if "." in raw else raw.lstrip(".")
    return ext in COPY_SAFE_EXTS


def s3_bucket(cfg: dict[str, Any]) -> str:
    bucket = str(cfg.get("database") or "").strip()
    if not bucket:
        raise FastPathUnavailable("S3 bucket required")
    return bucket


def s3_client(cfg: dict[str, Any]) -> Any:
    merged = dict(cfg)
    if is_local_endpoint(cfg) and not merged.get("path_style"):
        merged["path_style"] = True
    return boto3_client("s3", merged)


def s3_endpoint_key(cfg: dict[str, Any]) -> str:
    raw = (resolve_endpoint_url(cfg) or "").strip().lower().rstrip("/")
    return raw.replace("://localhost", "://127.0.0.1") or "aws-default"


def s3_object_id(cfg: dict[str, Any], key: str) -> tuple[str, str, str]:
    return (
        s3_endpoint_key(cfg),
        s3_bucket(cfg).lower(),
        normalize_object_base_key(key).strip().lower(),
    )


def s3_dest_count(cfg: dict[str, Any], key: str) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "s3",
        {**cfg, "database": s3_bucket(cfg), "type": "s3"},
        schema="",
        table_name=key,
    )
    if n is None:
        raise ValueError(f"S3 dest COUNT unmeasured for {key}")
    return int(n)


def s3_list_keys(cfg: dict[str, Any], key: str) -> list[str]:
    from connectors.s3_reader import list_objects

    bucket = s3_bucket(cfg)
    base = normalize_object_base_key(key)
    prefix = object_parts_prefix(base)
    try:
        listed = list_objects(cfg, bucket, prefix) if prefix else []
    except Exception as exc:
        raise FastPathUnavailable(f"S3 list failed: {exc}") from exc
    keys = list(object_store_read_keys(base, listed))
    if keys == [base]:
        client = s3_client(cfg)
        try:
            client.head_object(Bucket=bucket, Key=base)
        except Exception:
            return []
    return keys


def s3_delete_keys(cfg: dict[str, Any], keys: list[str]) -> None:
    if not keys:
        return
    client = s3_client(cfg)
    bucket = s3_bucket(cfg)
    for key in keys:
        try:
            client.delete_object(Bucket=bucket, Key=key)
        except Exception:
            continue


def s3_ensure_bucket(cfg: dict[str, Any]) -> None:
    from connectors.s3_writer import _ensure_bucket

    _ensure_bucket(s3_client(cfg), s3_bucket(cfg), cfg)


def s3_copy_object(
    *,
    src_client: Any,
    src_bucket: str,
    src_key: str,
    dest_bucket: str,
    dest_key: str,
) -> None:
    """Server-side CopyObject / UploadPartCopy. Never GET+PUT the payload."""
    src_client.copy(
        CopySource={"Bucket": src_bucket, "Key": src_key},
        Bucket=dest_bucket,
        Key=dest_key,
    )


def s3_remap_keys(
    src_keys: list[str], src_table: str, dest_table: str
) -> list[tuple[str, str]]:
    dest_base = normalize_object_base_key(dest_table)
    src_base = normalize_object_base_key(src_table)
    if len(src_keys) == 1:
        src_ext = s3_ext(src_keys[0])
        dest_ext = s3_ext(dest_base)
        if src_ext and dest_ext and src_ext != dest_ext:
            raise FastPathUnavailable(
                f"S3 COPY refuses {src_ext} bytes at a .{dest_ext} key"
            )
        return [(src_keys[0], dest_base)]
    dest_prefix = object_parts_prefix(dest_base)
    out: list[tuple[str, str]] = []
    for src_key in src_keys:
        name = src_key.rsplit("/", 1)[-1]
        out.append((src_key, f"{dest_prefix}{name}"))
    if out:
        src_ext = s3_ext(out[0][0])
        dest_ext = s3_ext(out[0][1])
        if src_ext and dest_ext and src_ext != dest_ext:
            raise FastPathUnavailable(
                f"S3 COPY refuses {src_ext} bytes at a .{dest_ext} key"
            )
    return out


def skip_complete_s3(
    *,
    source_count: int,
    dest_count: int,
    extra_snapshot: dict[str, Any] | None = None,
) -> FastPathResult:
    proof = f"dest_count:{dest_count}"
    snapshot = {
        "copy_workers": 1,
        "copy_split": "skip",
        "copy_partitions": 1,
        "partitions_skipped": 1,
        "partitions_loaded": 0,
        "shard_mode": "object",
        **(extra_snapshot or {}),
    }
    return FastPathResult(
        rows_copied=source_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        proof_scope="dest_count_equals_source_snapshot_count",
    )
