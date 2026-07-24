"""Spill object-store payloads to disk and read them without loading full bytes into RAM.

This module replaces the previous `Body.read()` / `download_as_bytes()` pattern for
S3, GCS, ADLS and SFTP with a streaming download to a temporary file, followed by
disk-backed parsing for CSV/JSONL/JSON/Excel/Parquet. Binary file types that cannot
be streamed (Avro/ORC/XML) still fall back to reading the spilled file into memory,
but at least the network payload is no longer held as a single byte buffer.
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
import threading
from pathlib import Path
from typing import Any, Callable

from services.value_serializer import cell_to_string

try:
    from services.platform_config import data_dir
except ImportError:  # pragma: no cover - tests launched with src on path
    from src.services.platform_config import data_dir

try:
    from services.file_parser import FileParser
except ImportError:
    from src.services.file_parser import FileParser

_logger = logging.getLogger(__name__)

_SPILL_DIR: Path | None = None
_SPILL_CACHE: dict[str, tuple[Path, float]] = {}
_SPILL_LOCK = threading.Lock()

# Default TTL for cached spilled objects (seconds).  A TTL defends against
# stale data without forcing a re-download on every call; callers that need a
# guaranteed fresh copy (e.g. per-transfer runs) can pass force=True.
_SPILL_TTL_SECONDS = int(os.getenv("DATAFLOW_SPILL_TTL", "300"))

_STREAMABLE = {"csv", "tsv", "jsonl", "ndjson", "json", "excel", "parquet", "avro", "orc"}


def _spill_directory() -> Path:
    global _SPILL_DIR
    if _SPILL_DIR is None:
        base = Path(data_dir()) if "data_dir" in globals() else Path(tempfile.gettempdir())
        _SPILL_DIR = base / "object_spill"
        _SPILL_DIR.mkdir(parents=True, exist_ok=True)
    return _SPILL_DIR


def _sanitize_cache_key(key: str) -> str:
    """Turn any cache key into a safe filesystem name."""
    return re.sub(r"[^a-zA-Z0-9._-]", "_", key)[:120]


def spill_path(cache_key: str) -> Path:
    """Return a deterministic temporary path for an object cache key."""
    return _spill_directory() / f"{_sanitize_cache_key(cache_key)}.tmp"


def clear_spill_cache() -> None:
    with _SPILL_LOCK:
        _SPILL_CACHE.clear()


def _cache_is_fresh(key: str, now: float) -> bool:
    with _SPILL_LOCK:
        entry = _SPILL_CACHE.get(key)
        if not entry:
            return False
        path, expires_at = entry
        return path.exists() and now < expires_at


def download_object(
    cache_key: str,
    downloader: Callable[[Path], None],
    *,
    force: bool = False,
    ttl_seconds: int = _SPILL_TTL_SECONDS,
) -> Path:
    """Download an object to a spilled temp file unless a fresh copy is cached.

    ``downloader`` receives a temporary target ``Path`` and must write the full
    object bytes to it.  On success the temp file is atomically renamed to the
    deterministic spill path; on failure the partial temp file is removed and the
    cache entry is invalidated so the next call re-downloads.

    Use ``force=True`` when the caller needs a guaranteed fresh copy (e.g. each
    transfer run) and ``force=False`` when repeated reads of the same object are
    expected within the TTL.
    """
    import time

    now = time.monotonic()
    if not force and _cache_is_fresh(cache_key, now):
        with _SPILL_LOCK:
            path, _ = _SPILL_CACHE[cache_key]
        _logger.info("Reusing fresh spilled object %s at %s", cache_key, path)
        return path

    final_path = spill_path(cache_key)
    spill_dir = _spill_directory()
    tmp_path: Path | None = None
    try:
        fd, tmp_name = tempfile.mkstemp(dir=spill_dir, prefix=".spill_", suffix=".part")
        os.close(fd)
        tmp_path = Path(tmp_name)
        _logger.info("Spilling object %s to temp %s", cache_key, tmp_path)
        downloader(tmp_path)
        # Atomic rename so other readers never see a partially written file.
        os.replace(tmp_path, final_path)
        expires_at = now + ttl_seconds
        with _SPILL_LOCK:
            _SPILL_CACHE[cache_key] = (final_path, expires_at)
        _logger.info("Spilled object %s atomically to %s", cache_key, final_path)
        return final_path
    except Exception:
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass
        with _SPILL_LOCK:
            _SPILL_CACHE.pop(cache_key, None)
            if final_path.exists():
                try:
                    final_path.unlink()
                except Exception:
                    pass
        raise


def _records_to_matrix(records: list[dict[str, Any]], columns: list[str]) -> list[list[str]]:
    return [[cell_to_string(record.get(c)) for c in columns] for record in records]


def _collect_records(path: Path, file_type: str, offset: int, limit: int) -> list[dict[str, Any]]:
    """Stream rows from the spilled file until ``limit`` are collected."""
    try:
        from src.transfer.file_stream import _batch_iterator_for_type
    except ImportError:  # pragma: no cover
        from transfer.file_stream import _batch_iterator_for_type

    records: list[dict[str, Any]] = []
    batch_size = max(limit, 1000)
    skipped = 0
    for batch in _batch_iterator_for_type(file_type, path, batch_size):
        for record in batch:
            if skipped < offset:
                skipped += 1
                continue
            records.append(record)
            if len(records) >= limit:
                return records
    return records


def read_rows_from_spill(
    path: Path,
    filename: str,
    *,
    offset: int = 0,
    limit: int = 500,
    known_total: int | None = None,
) -> tuple[list[str], list[list[str]], int]:
    """Read a row window from a spilled object file.

    Returns ``(headers, rows, total_rows)``.  CSV/JSONL/JSON/Excel/Parquet are
    parsed from disk; other formats fall back to reading the whole spilled file.
    """
    file_type = FileParser.detect_file_type(filename, b"")

    if file_type in _STREAMABLE:
        try:
            from src.transfer.file_stream import peek_file_source
        except ImportError:  # pragma: no cover
            from transfer.file_stream import peek_file_source

        headers, _schema, total, sample = peek_file_source(path, filename)
        if offset == 0 and limit <= len(sample):
            records = sample[:limit]
        else:
            records = _collect_records(path, file_type, offset, limit)
        rows = _records_to_matrix(records, headers)
        return headers, rows, known_total if known_total is not None else total

    # Fallback for Avro / ORC / XML / unknown: read the spilled file and parse.
    raw = path.read_bytes()
    result = FileParser.parse(raw, filename)
    if not result.success:
        raise ValueError(result.error or f"Cannot parse object `{filename}`")
    headers = result.columns
    records = result.data[offset : offset + limit]
    rows = _records_to_matrix(records, headers)
    return headers, rows, known_total if known_total is not None else len(result.data)


def download_s3_object(path: Path, cfg: dict[str, Any], bucket: str, key: str) -> None:
    from connectors.aws_common import boto3_client

    obj = boto3_client("s3", cfg).get_object(Bucket=bucket, Key=key)
    with open(path, "wb") as f:
        for chunk in obj["Body"].iter_chunks(chunk_size=8 * 1024 * 1024):
            if chunk:
                f.write(chunk)


def download_gcs_object(path: Path, cfg: dict[str, Any], bucket: str, key: str) -> None:
    from connectors.gcs_common import gcs_client

    gcs_client(cfg).bucket(bucket).blob(key).download_to_filename(str(path))


def download_adls_object(path: Path, cfg: dict[str, Any], bucket: str, key: str) -> None:
    from connectors.adls_common import blob_service_client

    blob = blob_service_client(cfg).get_blob_client(bucket, key)
    with open(path, "wb") as f:
        for chunk in blob.download_blob().chunks():
            if chunk:
                f.write(chunk)


def download_sftp_object(path: Path, cfg: dict[str, Any], bucket: str, key: str) -> None:
    from connectors.sftp_common import (
        connect_sftp,
        parse_sftp_config,
        split_remote_path,
    )

    merged = dict(cfg)
    if bucket:
        merged["database"] = bucket
    if key:
        merged["table"] = key
    sftp_cfg = parse_sftp_config(**merged)
    if not sftp_cfg.host:
        raise ValueError("SFTP host is required")
    if not sftp_cfg.path:
        raise ValueError("SFTP remote path is required")

    directory, filename = split_remote_path(sftp_cfg.path)
    remote_path = sftp_cfg.path if directory else f"/{filename}"
    transport, sftp = connect_sftp(sftp_cfg)
    try:
        with open(path, "wb") as f:
            sftp.getfo(remote_path, f)
    finally:
        sftp.close()
        transport.close()


def download_for_object_store(src_type: str, path: Path, cfg: dict[str, Any], bucket: str, key: str) -> None:
    """Download an object to a spilled path based on the source type."""
    dispatch = {
        "s3": download_s3_object,
        "gcs": download_gcs_object,
        "adls": download_adls_object,
        "sftp": download_sftp_object,
    }
    downloader = dispatch.get(src_type)
    if not downloader:
        raise ValueError(f"No downloader implemented for source type '{src_type}'")
    downloader(path, cfg, bucket, key)


def stream_spilled_file_to_database(
    path: Path,
    filename: str,
    destination: Any,
    mappings: list[dict[str, Any]],
    schema: dict[str, str],
    *,
    sync_mode: str = "full_refresh_overwrite",
    stream_contracts: list[dict] | None = None,
    job_id: str | None = None,
    checkpoint: Any | None = None,
    checkpoint_service: Any | None = None,
    retry_budget: Any | None = None,
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    source_filter: dict[str, Any] | None = None,
    skip_preflight: bool = False,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Stream a spilled object file to a database destination without loading it."""
    try:
        from src.transfer.file_stream import stream_file_to_database
    except ImportError:  # pragma: no cover
        from transfer.file_stream import stream_file_to_database

    return stream_file_to_database(
        content=path,
        filename=filename,
        destination=destination,
        mappings=mappings,
        schema=schema,
        sync_mode=sync_mode,
        stream_contracts=stream_contracts,
        job_id=job_id,
        checkpoint=checkpoint,
        checkpoint_service=checkpoint_service,
        retry_budget=retry_budget,
        backfill_new_fields=backfill_new_fields,
        validation_mode=validation_mode,
        source_filter=source_filter,
        skip_preflight=skip_preflight,
    )
