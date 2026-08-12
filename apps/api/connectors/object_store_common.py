"""Shared helpers for object-store readers and multi-chunk writers."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Callable

from services.object_streaming import (
    download_for_object_store,
    download_object,
    read_rows_from_spill,
)

from connectors.base import ReadBatch

__all__ = [
    "ObjectWriteLayout",
    "ReadBatch",
    "normalize_object_base_key",
    "object_parts_prefix",
    "object_run_token",
    "object_staging_key",
    "object_store_read_keys",
    "purge_object_store_parts",
    "read_object_from_store",
    "resolve_object_store_write_dest_types",
    "resolve_object_write_key",
    "resolve_object_write_layout",
    "_object_version_token",
]

_PART_NAME_RE = re.compile(r"^part-\d{5}\.(json|jsonl|csv)$", re.IGNORECASE)


def normalize_object_base_key(table_name: str, schema: str = "") -> str:
    """Canonical object key from table/schema (single-chunk layout)."""
    key = (table_name or schema or "exports/dataflow_export.json").strip()
    if not key.endswith((".json", ".jsonl", ".csv")):
        key = f"{key.rstrip('/')}/export.json"
    return key


def object_parts_prefix(base_key: str) -> str:
    """Listing prefix that holds every part object for ``base_key``."""
    base = normalize_object_base_key(base_key)
    parent, _, name = base.rpartition("/")
    stem = name.rsplit(".", 1)[0] if "." in name else name
    return f"{parent}/{stem}/" if parent else f"{stem}/"


def object_run_token(job_id: str | None) -> str:
    """Filesystem-safe run token used to isolate append runs."""
    raw = str(job_id or "").strip()
    token = re.sub(r"[^A-Za-z0-9_-]", "", raw)[:40]
    return f"run-{token}" if token else ""


def resolve_object_write_key(
    base_key: str,
    *,
    file_batch_idx: int = 0,
    total_chunks: int = 1,
    run_token: str = "",
) -> tuple[str, str]:
    """Resolve the object key for one writer chunk.

    Single-chunk jobs keep ``base_key`` for backward compatibility. Multi-chunk
    jobs write ``{stem}/part-{NNNNN}{ext}`` so later chunks cannot silently
    overwrite earlier ones (the prior fixed-key bug that kept only the last
    20k rows while reporting the full count).

    ``run_token`` nests the parts one level deeper (``{stem}/{run}/part-N``).
    Append syncs need this: without it a second run rewrites ``part-00001..M``
    while leaving a longer first run's ``part-M+1..N`` in place, producing a
    single object set mixing two generations of data.

    Returns ``(write_key, parts_prefix)``. ``parts_prefix`` is empty for
    single-chunk writes; otherwise it is the listing prefix for this run.
    """
    base = normalize_object_base_key(base_key) if base_key else normalize_object_base_key("")
    total = max(1, int(total_chunks or 1))
    if total <= 1:
        return base, ""

    idx = int(file_batch_idx or 0)
    # Engine passes 1-based chunk indexes; tolerate 0-based callers.
    part_n = idx if idx >= 1 else 1
    name = base.rpartition("/")[2]
    stem, _, ext_raw = name.rpartition(".")
    ext = f".{ext_raw}" if stem else ".json"
    parts_dir = object_parts_prefix(base).rstrip("/")
    if run_token:
        parts_dir = f"{parts_dir}/{run_token}"
    return f"{parts_dir}/part-{part_n:05d}{ext}", f"{parts_dir}/"


@dataclass(frozen=True)
class ObjectWriteLayout:
    """Where one chunk writes, and what an overwrite must clear after commit.

    ``should_purge`` is true on the *last* overwrite chunk only — writers must
    promote staging→live successfully before deleting stale parts, so a failed
    upload cannot leave the destination empty (Bugbot ADLS/S3/GCS class).
    """

    base_key: str
    write_key: str
    parts_prefix: str
    purge_prefix: str
    purge_legacy_key: str
    should_purge: bool
    # When purging after a multi-part overwrite, keep part-00001..part-N.
    keep_part_count: int = 0


def object_staging_key(write_key: str) -> str:
    """Sibling key for bytes that must land before the live object is replaced."""
    key = (write_key or "").strip()
    if not key:
        return ".__df_staging__"
    return f"{key}.__df_staging__"


def resolve_object_write_layout(
    *,
    table_name: str,
    schema: str = "",
    sync_mode: str = "",
    file_batch_idx: int = 0,
    total_chunks: int = 1,
    job_id: str = "",
) -> ObjectWriteLayout:
    """Single source of truth for S3/GCS/ADLS chunked object layout.

    Overwrite syncs reuse one stable part set and clear stale parts once, on
    the *last* successful chunk (after staging→live promote). Append syncs
    isolate each run under a token so reruns cannot interleave with a previous
    run's parts.

    Raises ``ValueError`` when a multi-chunk append has no ``job_id`` to derive
    a run token from — writing colliding part keys would silently mix runs.
    """
    from services.sync_cursor import is_overwrite_sync

    base = normalize_object_base_key(table_name or schema)
    total = max(1, int(total_chunks or 1))
    overwrite = is_overwrite_sync(sync_mode)

    run_token = ""
    if total > 1 and not overwrite:
        run_token = object_run_token(job_id)
        if not run_token:
            raise ValueError(
                "Multi-chunk append to an object store requires a job id to "
                "isolate this run's part files; without it a rerun would "
                "overwrite part of a previous run and leave the rest, mixing "
                "two generations of data in one export."
            )

    write_key, parts_prefix = resolve_object_write_key(
        base,
        file_batch_idx=file_batch_idx,
        total_chunks=total,
        run_token=run_token,
    )
    idx = int(file_batch_idx or 0)
    part_n = idx if idx >= 1 else 1
    is_last_chunk = part_n >= total
    return ObjectWriteLayout(
        base_key=base,
        write_key=write_key,
        parts_prefix=parts_prefix,
        # Purge the whole stem prefix so stale parts from a previous run with a
        # different chunk count (or a previous append run token) cannot survive.
        purge_prefix=object_parts_prefix(base),
        purge_legacy_key=base if total > 1 else "",
        should_purge=overwrite and is_last_chunk,
        keep_part_count=total if (overwrite and total > 1) else 0,
    )


def object_store_read_keys(base_key: str, listed_under_prefix: list[str]) -> list[str]:
    """Keys to read for Gate-8: prefer ``part-*`` layout when present.

    Multi-chunk writers emit ``{stem}/part-NNNNN{ext}`` and may delete the
    legacy single object. Verifiers that only open ``base_key`` would report
    missing read-back (or empty) while rows live in part objects — a Gate-8
    false pass under writer-ack fallback.
    """
    base = normalize_object_base_key(base_key)
    parts_prefix = object_parts_prefix(base)
    # Sorted so parts read back in write order, including append runs nested
    # one level deeper under a run token.
    parts = sorted(
        k
        for k in listed_under_prefix
        if k.startswith(parts_prefix) and _PART_NAME_RE.match(k.rsplit("/", 1)[-1])
    )
    if parts:
        return parts
    return [base]


def purge_object_store_parts(
    *,
    list_keys: Callable[[str], list[str]],
    delete_key: Callable[[str], None],
    parts_prefix: str,
    legacy_base_key: str = "",
    keep_part_count: int = 0,
    keep_keys: list[str] | tuple[str, ...] | None = None,
) -> list[str]:
    """Delete stale part objects (and optional legacy single-object key).

    Called after a successful last-chunk overwrite promote so a failed upload
    cannot wipe the previous export. ``keep_part_count`` preserves
    ``part-00001``..``part-N`` from the run that just landed.
    """
    removed: list[str] = []
    keep = {str(k) for k in (keep_keys or []) if k}
    keep_n = max(0, int(keep_part_count or 0))
    if parts_prefix:
        for key in list_keys(parts_prefix):
            if key in keep:
                continue
            name = key.rsplit("/", 1)[-1]
            # Only delete part-* under the prefix — never wipe sibling objects.
            m = _PART_NAME_RE.match(name)
            if not m:
                continue
            if keep_n > 0:
                try:
                    part_num = int(name[5:10])
                except ValueError:
                    part_num = -1
                if 1 <= part_num <= keep_n:
                    continue
            delete_key(key)
            removed.append(key)
    if legacy_base_key and legacy_base_key not in keep:
        try:
            delete_key(legacy_base_key)
            removed.append(legacy_base_key)
        except Exception:
            # Missing legacy key is fine; other errors surface on the write.
            pass
    return removed


def _object_version_token(store: str, cfg: dict[str, Any], bucket: str, key: str) -> str:
    """Best-effort content version for spill cache keys (ETag / generation / size+mtime).

    When version metadata is unavailable, return empty — callers should force refresh
    rather than reuse a TTL cache that can serve stale overwritten objects.
    """
    try:
        if store == "s3":
            from connectors.aws_common import boto3_client

            head = boto3_client("s3", cfg).head_object(Bucket=bucket, Key=key)
            etag = str(head.get("ETag") or "").strip('"')
            ver = str(head.get("VersionId") or "")
            return f"{etag}:{ver}" if etag or ver else ""
        if store == "gcs":
            from connectors.gcs_common import gcs_client

            blob = gcs_client(cfg).bucket(bucket).get_blob(key)
            if blob is None:
                return ""
            return f"{blob.generation or ''}:{blob.metageneration or ''}:{blob.etag or ''}"
        if store == "adls":
            from connectors.adls_common import blob_service_client

            props = blob_service_client(cfg).get_blob_client(bucket, key).get_blob_properties()
            etag = str(getattr(props, "etag", "") or "").strip('"')
            ver = str(getattr(props, "version_id", "") or "")
            return f"{etag}:{ver}"
    except Exception:
        return ""
    return ""


def read_object_from_store(
    store: str,
    cfg: dict[str, Any],
    bucket: str,
    key: str,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
    """Download an object from ``store`` to a local spill file and read rows.

    Spill cache keys include source version metadata when available so an overwritten
    object is never served as current under a stale TTL entry.
    """
    version = _object_version_token(store, cfg, bucket, key)
    cache_key = f"{store}:{bucket}:{key}:{version or 'nov'}"
    # No version ⇒ force download (refuse TTL reuse of potentially stale bytes).
    path = download_object(
        cache_key,
        lambda p: download_for_object_store(store, p, cfg, bucket, key),
        force=not bool(version),
    )
    headers, rows, total = read_rows_from_spill(
        path,
        key,
        offset=offset,
        limit=limit,
        known_total=known_total_rows,
    )
    return ReadBatch(
        headers=headers,
        rows=rows,
        offset=offset,
        total_rows=total,
        meta={"native_types": _inferred_native_types(headers, rows)},
    )


def _inferred_native_types(headers: list[str], rows: list[list[Any]]) -> dict[str, str]:
    """Column types read from the object's own rows.

    An object store holds the same CSV/JSON/Parquet payload an upload does, but
    the reader handed the engine bare strings and no types, so every column
    landed as text: the identical file uploaded directly produced
    ``bigint``/``numeric``/``date`` while the S3 copy produced three ``text``
    columns. The transfer still reported success, which is the part that makes
    it worth inferring here rather than leaving to the destination.

    This is the same ``infer_columns_from_rows`` the file parser uses, so both
    paths reach one answer instead of two, and readers already carry types to
    the engine through ``meta['native_types']``.
    """
    if not headers or not rows:
        return {}
    try:
        from services.schema_inference import infer_columns_from_rows

        return {
            str(col["name"]): str(col.get("inferred_type") or "VARCHAR")
            for col in infer_columns_from_rows(list(headers), list(rows))
            if col.get("name")
        }
    except Exception as exc:  # noqa: BLE001 — types are an enrichment, not a gate
        logging.getLogger(__name__).info(
            "object store type inference unavailable for %s: %s", key, exc
        )
        return {}


def resolve_object_store_write_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str] | None,
    *,
    logical_types: list[str] | None = None,
    destination_column_types: dict[str, Any] | None = None,
) -> tuple[dict[str, str], str | None]:
    """Prefer Studio/probed live carriers over Map stamps for serialize.

    S3/GCS/ADLS/SFTP must quarantine DECIMAL/BINARY/VARCHAR(n) against the
    destination schema Studio probed — never ignore live types and soft-bind
    Map VARCHAR (overflow / empty→null invent on JSON/CSV/Parquet export).

    When Studio ``destination_column_types`` is present (non-empty), every mapped
    column must be covered — partial Studio must not fall through to Map
    VARCHAR invent. When Studio is absent, Map stamps are allowed for first-write
    export (no live object schema to probe).

    Returns ``(dest_types, None)`` or ``(partial, error)``.
    """
    from connectors.writer_common import resolve_studio_or_map_dest_types

    return resolve_studio_or_map_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        studio_types=destination_column_types,
        product="Object-store",
    )
