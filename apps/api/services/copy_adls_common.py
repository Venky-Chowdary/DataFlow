"""Shared ADLS identity-COPY helpers.

Dest COUNT is ``destination_row_count`` / object-store artifact COUNT
(GET streams or Parquet footers) — never ListBlobs length, never copy
ack, never ``azcopy`` / GET+PUT. Same endpoint+container+blob declines.
Cross-endpoint ``start_copy_from_url`` declines (server-side COPY cannot
leave the account). Azurite on :10000 is an emulator, not a
customer-tenant PRODUCTION_SKU.
"""

from __future__ import annotations

import logging
from typing import Any

from connectors.adls_common import _account_url, blob_service_client
from connectors.object_store_common import (
    normalize_object_base_key,
    object_parts_prefix,
    object_store_read_keys,
)
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_s3_common import COPY_SAFE_EXTS, s3_ext

logger = logging.getLogger(__name__)

_ADLS_FAMILY = frozenset({
    "adls",
    "azure_blob_storage",
    "azure_data_lake",
    "azure_data_lake_storage",
})


def adls_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _ADLS_FAMILY:
        return "adls"
    return n


def adls_ext(key: str) -> str:
    return s3_ext(key)


def adls_type_is_copy_safe(declared_or_key: str) -> bool:
    raw = (declared_or_key or "").strip().lower()
    if not raw:
        return True
    ext = adls_ext(raw) if "." in raw else raw.lstrip(".")
    return ext in COPY_SAFE_EXTS


def adls_container(cfg: dict[str, Any]) -> str:
    container = str(cfg.get("database") or "").strip()
    if not container:
        raise FastPathUnavailable("ADLS container required")
    return container


def adls_endpoint_key(cfg: dict[str, Any]) -> str:
    try:
        raw = _account_url(cfg)
    except Exception:
        raw = str(
            cfg.get("endpoint_url")
            or cfg.get("connection_string")
            or cfg.get("host")
            or ""
        )
    return str(raw or "").strip().lower().rstrip("/").replace(
        "://localhost", "://127.0.0.1"
    ) or "azure-default"


def adls_object_id(cfg: dict[str, Any], key: str) -> tuple[str, str, str]:
    return (
        adls_endpoint_key(cfg),
        adls_container(cfg).lower(),
        normalize_object_base_key(key).strip().lower(),
    )


def adls_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "endpoint_url", "dsn")
    )


def adls_dest_count(cfg: dict[str, Any], key: str) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "adls",
        {**cfg, "database": adls_container(cfg), "type": "adls"},
        schema="",
        table_name=key,
    )
    if n is None:
        raise ValueError(f"ADLS dest COUNT unmeasured for {key}")
    return int(n)


def adls_list_keys(cfg: dict[str, Any], key: str) -> list[str]:
    from connectors.adls_reader import list_objects

    container = adls_container(cfg)
    base = normalize_object_base_key(key)
    prefix = object_parts_prefix(base)
    try:
        listed = list_objects(cfg, container, prefix) if prefix else []
    except Exception as exc:
        raise FastPathUnavailable(f"ADLS list failed: {exc}") from exc
    keys = list(object_store_read_keys(base, listed))
    if keys == [base]:
        client = blob_service_client(cfg)
        blob = client.get_blob_client(container, base)
        try:
            if not blob.exists():
                return []
        except Exception:
            return []
    return keys


def adls_delete_keys(cfg: dict[str, Any], keys: list[str]) -> None:
    if not keys:
        return
    client = blob_service_client(cfg)
    container = adls_container(cfg)
    for key in keys:
        try:
            client.get_blob_client(container, key).delete_blob()
        except Exception:
            continue


def adls_ensure_container(cfg: dict[str, Any]) -> None:
    client = blob_service_client(cfg)
    name = adls_container(cfg)
    handle = client.get_container_client(name)
    try:
        if handle.exists():
            return
    except Exception:
        logger.debug("ADLS container exists probe skipped", exc_info=True)
    try:
        handle.create_container()
    except Exception as exc:
        try:
            if handle.exists():
                return
        except Exception:
            logger.debug("ADLS container re-probe skipped", exc_info=True)
        raise FastPathUnavailable(f"ADLS container create failed: {exc}") from exc


def adls_copy_object(
    *,
    src_cfg: dict[str, Any],
    src_container: str,
    src_key: str,
    dest_container: str,
    dest_key: str,
) -> None:
    """Server-side start_copy_from_url. Never GET+PUT the payload. Not azcopy."""
    client = blob_service_client(src_cfg)
    src_blob = client.get_blob_client(src_container, src_key)
    dest_blob = client.get_blob_client(dest_container, dest_key)
    props = dest_blob.start_copy_from_url(src_blob.url, requires_sync=True)
    status = ""
    if isinstance(props, dict):
        status = str(props.get("copy_status") or "")
    else:
        status = str(getattr(props, "copy_status", "") or "")
    if str(status).lower() != "success":
        try:
            copy = dest_blob.get_blob_properties().copy
            status = str(getattr(copy, "status", "") or "")
        except Exception:
            pass
    if str(status).lower() != "success":
        raise ValueError(
            f"ADLS start_copy_from_url did not succeed for {src_key!r} → {dest_key!r} "
            f"(copy_status={status!r})"
        )


def adls_remap_keys(
    src_keys: list[str], src_table: str, dest_table: str
) -> list[tuple[str, str]]:
    dest_base = normalize_object_base_key(dest_table)
    if len(src_keys) == 1:
        src_ext = adls_ext(src_keys[0])
        dest_ext = adls_ext(dest_base)
        if src_ext and dest_ext and src_ext != dest_ext:
            raise FastPathUnavailable(
                f"ADLS COPY refuses {src_ext} bytes at a .{dest_ext} key"
            )
        return [(src_keys[0], dest_base)]
    dest_prefix = object_parts_prefix(dest_base)
    out: list[tuple[str, str]] = []
    for src_key in src_keys:
        name = src_key.rsplit("/", 1)[-1]
        out.append((src_key, f"{dest_prefix}{name}"))
    if out:
        src_ext = adls_ext(out[0][0])
        dest_ext = adls_ext(out[0][1])
        if src_ext and dest_ext and src_ext != dest_ext:
            raise FastPathUnavailable(
                f"ADLS COPY refuses {src_ext} bytes at a .{dest_ext} key"
            )
    return out


def skip_complete_adls(
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
