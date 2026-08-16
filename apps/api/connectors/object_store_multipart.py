"""Object-store multipart upload — S3 / GCS / ADLS SSOT.

Writers serialize into a ``SpooledTemporaryFile`` (rolls to disk above the
spill cap), then land staging→live. A single ``PutObject`` times out or hits
the 5 GiB S3 single-PUT ceiling on large bodies.

Multipart splits the serialized export into parts — from bytes or from the
spool, one part in RAM at a time:

- S3 / MinIO: ``CreateMultipartUpload`` → ``UploadPart`` → ``Complete``.
  Failure **aborts** the upload (no silent leftover parts billed as storage).
- GCS: component objects + ``compose`` (tree-compose when >32 sources).
  Failure deletes components and does not compose the live key.
- ADLS: ``stage_block`` + ``commit_block_list``. Failure does not commit.

Small bodies stay on the single-request path so existing tests and tiny
exports do not pay a multipart round-trip.

Honesty: ``mapped_rows`` stay in RAM. Parquet still builds an Arrow table.
This is not a source-stream spill. Still at-least-once (staging + live).
Not exactly-once. Not row-level MERGE. Not a live cloud matrix.
"""

from __future__ import annotations

import base64
import math
from typing import Any, BinaryIO, Callable, Iterator

from services.brand_env import getenv_brand

# S3 requires every part except the last to be at least 5 MiB.
S3_MIN_PART_SIZE = 5 * 1024 * 1024
S3_MAX_PARTS = 10_000
GCS_COMPOSE_LIMIT = 32
DEFAULT_MULTIPART_THRESHOLD = 8 * 1024 * 1024
DEFAULT_MULTIPART_PART_SIZE = 8 * 1024 * 1024


def resolve_multipart_limits(extra: dict[str, Any] | None = None) -> tuple[int, int]:
    """Return ``(threshold_bytes, part_size_bytes)`` from dest extra / env."""
    extra = extra if isinstance(extra, dict) else {}
    threshold = int(
        extra.get("multipart_threshold")
        or extra.get("object_store_multipart_threshold")
        or getenv_brand("DATAFLOW_OBJECT_STORE_MULTIPART_THRESHOLD", str(DEFAULT_MULTIPART_THRESHOLD))
        or DEFAULT_MULTIPART_THRESHOLD
    )
    part_size = int(
        extra.get("multipart_part_size")
        or extra.get("object_store_multipart_part_size")
        or getenv_brand("DATAFLOW_OBJECT_STORE_MULTIPART_PART_SIZE", str(DEFAULT_MULTIPART_PART_SIZE))
        or DEFAULT_MULTIPART_PART_SIZE
    )
    return max(1, threshold), max(1, part_size)


def resolve_spill_max(extra: dict[str, Any] | None = None) -> int:
    """Bytes kept in the serialize spool before rolling to a temp file."""
    extra = extra if isinstance(extra, dict) else {}
    raw = (
        extra.get("spill_max")
        or extra.get("object_store_spill_max")
        or getenv_brand("DATAFLOW_OBJECT_STORE_SPILL_MAX", "")
        or ""
    )
    if raw:
        return max(1, int(raw))
    return resolve_multipart_limits(extra)[0]


def should_use_object_store_multipart(
    body: bytes | int,
    *,
    threshold: int | None = None,
    part_size: int | None = None,
) -> bool:
    """Multipart only when the body needs at least two parts at/above threshold."""
    n = int(body) if isinstance(body, int) else len(body)
    limit = DEFAULT_MULTIPART_THRESHOLD if threshold is None else int(threshold)
    size = DEFAULT_MULTIPART_PART_SIZE if part_size is None else int(part_size)
    if n < max(1, limit):
        return False
    return math.ceil(n / max(1, size)) >= 2


def iter_object_store_parts(
    body: bytes | None = None,
    *,
    part_size: int,
    source: BinaryIO | None = None,
    size: int | None = None,
) -> Iterator[tuple[int, bytes]]:
    """Yield 1-indexed ``(part_number, chunk)``. Last part may be short."""
    chunk_size = max(1, int(part_size))
    if source is not None:
        total = int(size) if size is not None else None
        if total == 0:
            return
        n_parts = math.ceil(total / chunk_size) if total is not None else None
        if n_parts is not None and n_parts > S3_MAX_PARTS:
            raise ValueError(
                f"Object store multipart would need {n_parts} parts "
                f"(max {S3_MAX_PARTS}). Increase part size."
            )
        idx = 1
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                break
            yield idx, chunk
            idx += 1
        return
    if not body:
        return
    total = len(body)
    n_parts = math.ceil(total / chunk_size)
    if n_parts > S3_MAX_PARTS:
        raise ValueError(
            f"Object store multipart would need {n_parts} parts "
            f"(max {S3_MAX_PARTS}). Increase part size."
        )
    view = memoryview(body)
    for idx in range(n_parts):
        start = idx * chunk_size
        chunk = view[start : start + chunk_size].tobytes()
        yield idx + 1, chunk


def _iter_upload_parts(
    body: bytes | None,
    export: Any,
    part_size: int,
) -> Iterator[tuple[int, bytes]]:
    if export is not None:
        yield from export.iter_parts(part_size)
        return
    yield from iter_object_store_parts(body or b"", part_size=part_size)


def upload_object_store_bytes(
    dialect: str,
    *,
    body: bytes | None = None,
    export: Any = None,
    key: str,
    content_type: str = "",
    client: Any = None,
    bucket: str = "",
    bucket_obj: Any = None,
    blob_client_factory: Callable[[str], Any] | None = None,
    threshold: int | None = None,
    part_size: int | None = None,
) -> dict[str, Any]:
    """Upload bytes or a spilled export. Returns method metadata."""
    engine = (dialect or "").strip().lower()
    if engine in {"minio", "amazon_s3"}:
        engine = "s3"
    if engine in {"gcp_storage", "google_cloud_storage"}:
        engine = "gcs"
    if engine in {"azure_blob", "azure_data_lake", "azure_datalake"}:
        engine = "adls"

    if export is not None:
        n = int(export.size)
        content_type = content_type or str(export.content_type or "")
    else:
        n = len(body or b"")
    use_mp = should_use_object_store_multipart(
        n, threshold=threshold, part_size=part_size
    )
    size = DEFAULT_MULTIPART_PART_SIZE if part_size is None else int(part_size)
    payload = None if use_mp else (export.read_all() if export is not None else body)
    if engine == "s3":
        if not use_mp:
            client.put_object(Bucket=bucket, Key=key, Body=payload, ContentType=content_type)
            return {"method": "put_object", "parts": 1, "engine": "s3"}
        return _upload_s3_multipart(
            client,
            bucket=bucket,
            key=key,
            body=body,
            export=export,
            content_type=content_type,
            part_size=size,
        )
    if engine == "gcs":
        if not use_mp:
            bucket_obj.blob(key).upload_from_string(payload, content_type=content_type)
            return {"method": "upload_from_string", "parts": 1, "engine": "gcs"}
        return _upload_gcs_compose(
            bucket_obj,
            key=key,
            body=body,
            export=export,
            content_type=content_type,
            part_size=size,
        )
    if engine == "adls":
        if blob_client_factory is None:
            raise ValueError("ADLS multipart requires blob_client_factory")
        blob = blob_client_factory(key)
        if not use_mp:
            blob.upload_blob(payload, overwrite=True, content_type=content_type)
            return {"method": "upload_blob", "parts": 1, "engine": "adls"}
        return _upload_adls_blocks(
            blob, body=body, export=export, content_type=content_type, part_size=size
        )
    raise ValueError(f"Unsupported object-store multipart dialect: {dialect!r}")


def land_object_store_export(
    dialect: str,
    *,
    export: Any,
    staging_key: str,
    live_key: str,
    threshold: int | None = None,
    part_size: int | None = None,
    **upload_kw: Any,
) -> None:
    """Staging→live from one spool. Rewinds between puts. Does not close ``export``."""
    common = dict(upload_kw)
    common.update(export=export, threshold=threshold, part_size=part_size)
    upload_object_store_bytes(dialect, key=staging_key, **common)
    export.rewind()
    upload_object_store_bytes(dialect, key=live_key, **common)


def _upload_s3_multipart(
    client: Any,
    *,
    bucket: str,
    key: str,
    body: bytes | None = None,
    export: Any = None,
    content_type: str,
    part_size: int,
) -> dict[str, Any]:
    started = client.create_multipart_upload(
        Bucket=bucket, Key=key, ContentType=content_type
    )
    upload_id = str((started or {}).get("UploadId") or "")
    if not upload_id:
        raise RuntimeError("S3 CreateMultipartUpload returned no UploadId")
    parts: list[dict[str, Any]] = []
    try:
        for number, chunk in _iter_upload_parts(body, export, part_size):
            resp = client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=number,
                Body=chunk,
            )
            etag = str((resp or {}).get("ETag") or "")
            if not etag:
                raise RuntimeError(f"S3 UploadPart {number} returned no ETag")
            parts.append({"PartNumber": number, "ETag": etag})
        client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        try:
            client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except Exception:
            pass
        raise
    return {
        "method": "multipart",
        "parts": len(parts),
        "engine": "s3",
        "upload_id": upload_id,
    }


def _gcs_component_key(key: str, label: str) -> str:
    return f"{key}.__df_mp__/{label}"


def _upload_gcs_compose(
    bucket_obj: Any,
    *,
    key: str,
    body: bytes | None = None,
    export: Any = None,
    content_type: str,
    part_size: int,
) -> dict[str, Any]:
    component_keys: list[str] = []
    intermediates: list[str] = []
    try:
        for number, chunk in _iter_upload_parts(body, export, part_size):
            ck = _gcs_component_key(key, f"{number:05d}")
            bucket_obj.blob(ck).upload_from_string(chunk, content_type=content_type)
            component_keys.append(ck)
        _gcs_compose_tree(
            bucket_obj,
            dest_key=key,
            source_keys=component_keys,
            content_type=content_type,
            intermediates=intermediates,
        )
    except Exception:
        for doomed in component_keys + intermediates:
            try:
                bucket_obj.blob(doomed).delete()
            except Exception:
                pass
        raise
    for doomed in component_keys + intermediates:
        try:
            bucket_obj.blob(doomed).delete()
        except Exception:
            pass
    return {"method": "compose", "parts": len(component_keys), "engine": "gcs"}


def _gcs_compose_tree(
    bucket_obj: Any,
    *,
    dest_key: str,
    source_keys: list[str],
    content_type: str,
    intermediates: list[str],
) -> None:
    remaining = list(source_keys)
    generation = 0
    while len(remaining) > GCS_COMPOSE_LIMIT:
        nxt: list[str] = []
        for i in range(0, len(remaining), GCS_COMPOSE_LIMIT):
            batch = remaining[i : i + GCS_COMPOSE_LIMIT]
            if len(batch) == 1:
                nxt.append(batch[0])
                continue
            mid = _gcs_component_key(dest_key, f"c{generation:02d}-{i // GCS_COMPOSE_LIMIT:03d}")
            dest = bucket_obj.blob(mid)
            if hasattr(dest, "content_type"):
                dest.content_type = content_type
            dest.compose([bucket_obj.blob(k) for k in batch])
            intermediates.append(mid)
            nxt.append(mid)
        remaining = nxt
        generation += 1
    dest = bucket_obj.blob(dest_key)
    if hasattr(dest, "content_type"):
        dest.content_type = content_type
    dest.compose([bucket_obj.blob(k) for k in remaining])


def _adls_block_id(number: int) -> str:
    # Azure requires equal-length block IDs; base64 of a fixed-width token.
    return base64.b64encode(f"{int(number):06d}".encode("ascii")).decode("ascii")


def _upload_adls_blocks(
    blob: Any,
    *,
    body: bytes | None = None,
    export: Any = None,
    content_type: str,
    part_size: int,
) -> dict[str, Any]:
    block_ids: list[str] = []
    for number, chunk in _iter_upload_parts(body, export, part_size):
        bid = _adls_block_id(number)
        blob.stage_block(bid, chunk)
        block_ids.append(bid)
    settings = None
    try:
        from azure.storage.blob import ContentSettings

        settings = ContentSettings(content_type=content_type)
    except Exception:
        settings = None
    if settings is not None:
        blob.commit_block_list(block_ids, content_settings=settings)
    else:
        blob.commit_block_list(block_ids)
    return {"method": "block_list", "parts": len(block_ids), "engine": "adls"}
