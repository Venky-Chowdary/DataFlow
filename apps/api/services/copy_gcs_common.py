"""Shared GCS identity-COPY helpers.

Dest COUNT is ``destination_row_count`` / object-store artifact COUNT
(GET streams or Parquet footers) — never ListObjects length, never
writer rewrite ack, never ``gsutil cp`` / GET+PUT. Same
endpoint+bucket+object declines. Cross-endpoint copy_blob declines
(server-side rewrite cannot leave the cluster).
"""

from __future__ import annotations

import logging
from typing import Any

from connectors.gcs_common import _resolve_endpoint, gcs_client, gcs_emulator_kwargs
from connectors.object_store_common import (
    normalize_object_base_key,
    object_parts_prefix,
    object_store_read_keys,
)
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_s3_common import COPY_SAFE_EXTS, s3_ext

logger = logging.getLogger(__name__)

_GCS_FAMILY = frozenset({
    "gcs",
    "google_cloud_storage",
    "google_gcs",
    "gcp_storage",
})


def gcs_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _GCS_FAMILY:
        return "gcs"
    return n


def gcs_ext(key: str) -> str:
    return s3_ext(key)


def gcs_type_is_copy_safe(declared_or_key: str) -> bool:
    raw = (declared_or_key or "").strip().lower()
    if not raw:
        return True
    ext = gcs_ext(raw) if "." in raw else raw.lstrip(".")
    return ext in COPY_SAFE_EXTS


def gcs_bucket(cfg: dict[str, Any]) -> str:
    bucket = str(cfg.get("database") or "").strip()
    if not bucket:
        raise FastPathUnavailable("GCS bucket required")
    return bucket


def gcs_endpoint_key(cfg: dict[str, Any]) -> str:
    raw = (
        _resolve_endpoint(cfg)
        or str(cfg.get("endpoint_url") or cfg.get("connection_string") or "")
    ).strip().lower().rstrip("/")
    return raw.replace("://localhost", "://127.0.0.1") or "gcp-default"


def gcs_object_id(cfg: dict[str, Any], key: str) -> tuple[str, str, str]:
    return (
        gcs_endpoint_key(cfg),
        gcs_bucket(cfg).lower(),
        normalize_object_base_key(key).strip().lower(),
    )


def gcs_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "endpoint_url", "dsn")
    )


def gcs_dest_count(cfg: dict[str, Any], key: str) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "gcs",
        {**cfg, "database": gcs_bucket(cfg), "type": "gcs"},
        schema="",
        table_name=key,
    )
    if n is None:
        raise ValueError(f"GCS dest COUNT unmeasured for {key}")
    return int(n)


def gcs_list_keys(cfg: dict[str, Any], key: str) -> list[str]:
    from connectors.gcs_reader import list_objects

    bucket = gcs_bucket(cfg)
    base = normalize_object_base_key(key)
    prefix = object_parts_prefix(base)
    try:
        listed = list_objects(cfg, bucket, prefix) if prefix else []
    except Exception as exc:
        raise FastPathUnavailable(f"GCS list failed: {exc}") from exc
    keys = list(object_store_read_keys(base, listed))
    if keys == [base]:
        client = gcs_client(cfg)
        blob = client.bucket(bucket).blob(base)
        kw = gcs_emulator_kwargs(cfg)
        try:
            if not blob.exists(**kw):
                return []
        except Exception:
            return []
    return keys


def gcs_delete_keys(cfg: dict[str, Any], keys: list[str]) -> None:
    if not keys:
        return
    client = gcs_client(cfg)
    bucket = gcs_bucket(cfg)
    kw = gcs_emulator_kwargs(cfg)
    for key in keys:
        try:
            client.bucket(bucket).blob(key).delete(**kw)
        except Exception:
            continue


def gcs_ensure_bucket(cfg: dict[str, Any]) -> None:
    client = gcs_client(cfg)
    bucket = gcs_bucket(cfg)
    kw = gcs_emulator_kwargs(cfg)
    handle = client.bucket(bucket)
    try:
        if handle.exists(**kw):
            return
    except Exception:
        logger.debug("GCS bucket exists probe skipped", exc_info=True)
    try:
        client.create_bucket(bucket, **kw)
    except Exception as exc:
        try:
            if handle.exists(**kw):
                return
        except Exception:
            logger.debug("GCS bucket re-probe skipped", exc_info=True)
        raise FastPathUnavailable(f"GCS bucket create failed: {exc}") from exc


def gcs_copy_object(
    *,
    src_cfg: dict[str, Any],
    src_bucket: str,
    src_key: str,
    dest_bucket: str,
    dest_key: str,
) -> None:
    """Server-side rewrite / copy_blob. Never GET+PUT the payload."""
    client = gcs_client(src_cfg)
    kw = gcs_emulator_kwargs(src_cfg)
    src_blob = client.bucket(src_bucket).blob(src_key)
    dest_bkt = client.bucket(dest_bucket)
    dest_bkt.copy_blob(src_blob, dest_bkt, dest_key, **kw)


def gcs_remap_keys(
    src_keys: list[str], src_table: str, dest_table: str
) -> list[tuple[str, str]]:
    dest_base = normalize_object_base_key(dest_table)
    if len(src_keys) == 1:
        src_ext = gcs_ext(src_keys[0])
        dest_ext = gcs_ext(dest_base)
        if src_ext and dest_ext and src_ext != dest_ext:
            raise FastPathUnavailable(
                f"GCS COPY refuses {src_ext} bytes at a .{dest_ext} key"
            )
        return [(src_keys[0], dest_base)]
    dest_prefix = object_parts_prefix(dest_base)
    out: list[tuple[str, str]] = []
    for src_key in src_keys:
        name = src_key.rsplit("/", 1)[-1]
        out.append((src_key, f"{dest_prefix}{name}"))
    if out:
        src_ext = gcs_ext(out[0][0])
        dest_ext = gcs_ext(out[0][1])
        if src_ext and dest_ext and src_ext != dest_ext:
            raise FastPathUnavailable(
                f"GCS COPY refuses {src_ext} bytes at a .{dest_ext} key"
            )
    return out


def skip_complete_gcs(
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
