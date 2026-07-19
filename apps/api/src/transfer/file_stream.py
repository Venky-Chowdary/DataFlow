"""Streaming file → database transfer for CSV, TSV, JSONL, NDJSON, JSON arrays,
Excel, and Parquet.  Supports in-memory ``bytes`` as well as on-disk paths so
billion-row files can be processed without loading the whole payload into RAM.
"""

from __future__ import annotations

import csv
import gzip
import io
import itertools
import json
import os
import sys
import tempfile
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Any, Callable

from .models import EndpointConfig
from .type_mapper import ddl_type, normalize_inferred

try:
    from services.checkpoint_service import Checkpoint, CheckpointService
    from services.error_handling import RetryBudget, with_retry
    from services.parallel_chunks import OrderedChunkRunner
    from services.resilience import adaptive_chunk_size
    from services.row_filter import apply_row_filter
except ImportError:  # pragma: no cover - tests with api root on path
    from src.services.checkpoint_service import Checkpoint, CheckpointService
    from src.services.error_handling import RetryBudget, with_retry
    from src.services.parallel_chunks import OrderedChunkRunner
    from src.services.resilience import adaptive_chunk_size
    from src.services.row_filter import apply_row_filter

_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from connectors.writer_common import (  # noqa: E402
    CHUNK_SIZE,
    build_mapped_rows,
    resolve_target_columns,
    row_fingerprints,
    transform_error_policy_for_validation_mode,
)
from services.reconciliation import FingerprintAccumulator  # noqa: E402

try:
    from services.csv_profiler import (  # noqa: E402
        count_csv_rows,
        detect_delimiter,
        detect_encoding,
        parse_csv_preview,
    )
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.csv_profiler import (  # noqa: E402
        count_csv_rows,
        detect_delimiter,
        detect_encoding,
        parse_csv_preview,
    )

from .adapters import records_to_matrix, resolve_connector_config, resolve_dest_table
from .stream import _write_batch

STREAMABLE_TYPES = {"csv", "tsv", "jsonl", "ndjson", "json", "excel", "parquet"}
STREAM_THRESHOLD = int(os.getenv("DATAFLOW_STREAM_FILE_ROWS", "1"))
FILE_SPILL_THRESHOLD = int(os.getenv("DATAFLOW_FILE_SPILL_THRESHOLD", str(50 * 1024 * 1024)))
SPILL_DIR = os.getenv("DATAFLOW_SPILL_DIR") or None


def _is_path(value: Any) -> bool:
    return isinstance(value, (str, os.PathLike))


def _source_suffix(filename: str) -> str:
    name = os.path.basename(filename or "upload")
    _, ext = os.path.splitext(name)
    return ext or ".tmp"


def prepare_stream_content(
    content: bytes = b"",
    filename: str = "upload.csv",
    source_path: str = "",
) -> bytes | str:
    """Return the most efficient source reference for streaming.

    If an explicit ``source_path`` is provided and exists, it is used.
    If ``content`` is larger than ``FILE_SPILL_THRESHOLD`` bytes, it is written
    to a temporary file and that path is returned so iteration can stream from
    disk.  Otherwise the original ``bytes`` payload is returned.
    """
    if source_path and os.path.isfile(source_path):
        return source_path
    if not content:
        return content
    if len(content) <= FILE_SPILL_THRESHOLD:
        return content

    suffix = _source_suffix(filename)
    fd, path = tempfile.mkstemp(suffix=suffix, prefix="dataflow_spill_", dir=SPILL_DIR)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(content)
    except Exception:
        os.close(fd)
        raise
    return path


def _is_gzip_bytes(sample: bytes) -> bool:
    return bool(sample) and sample[:2] == b"\x1f\x8b"


def _is_gzip_path(path: str | os.PathLike) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(2) == b"\x1f\x8b"
    except Exception:
        return False


def _first_bytes(content: bytes | str | os.PathLike, size: int = 8192) -> bytes:
    """Return a decompressed prefix for sniffing."""
    if _is_path(content):
        if _is_gzip_path(content):
            with gzip.open(content, "rb") as f:
                return f.read(size)
        with open(content, "rb") as f:
            return f.read(size)
    if isinstance(content, (bytes, bytearray)) and _is_gzip_bytes(content):
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
            return f.read(size)
    return bytes(content[:size])


def _open_binary(content: bytes | str | os.PathLike) -> Any:
    """Open a binary stream, transparently decompressing gzip payloads."""
    if _is_path(content):
        if _is_gzip_path(content):
            return gzip.open(content, "rb")
        return open(content, "rb")
    if isinstance(content, (bytes, bytearray)) and _is_gzip_bytes(content):
        return gzip.GzipFile(fileobj=io.BytesIO(content))
    return io.BytesIO(content)


@contextmanager
def _text_reader(content: bytes | str | os.PathLike, encoding: str | None = None, newline: str = ""):
    binary = _open_binary(content)
    text = None
    try:
        if encoding is None:
            encoding = detect_encoding(_first_bytes(content))
        text = io.TextIOWrapper(binary, encoding=encoding, errors="replace", newline=newline)
        yield text
    finally:
        if text is not None:
            try:
                text.close()
            except Exception:
                pass
        else:
            binary.close()


def _excel_preview(content: bytes | str | os.PathLike, preview_rows: int = 100) -> tuple[list[str], list[list[str]], int]:
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise ValueError(
            "Excel import is not ready on this platform node. DataFlow bundles file parsers — retry shortly."
        ) from exc

    wb = load_workbook(content, read_only=True, data_only=True) if _is_path(content) else load_workbook(
        io.BytesIO(content), read_only=True, data_only=True
    )
    try:
        ws = wb.active
        if ws is None:
            return [], [], 0

        row_iter = ws.iter_rows(values_only=True)
        first = next(row_iter, None)
        if not first:
            return [], [], 0

        headers = [str(c).strip() if c is not None else f"col_{i}" for i, c in enumerate(first)]
        preview: list[list[str]] = []
        total = 0

        for row in row_iter:
            total += 1
            if len(preview) < preview_rows:
                preview.append([str(c).strip() if c is not None else "" for c in row])

        return headers, preview, total
    finally:
        wb.close()


def _excel_batches(content: bytes | str | os.PathLike, chunk_size: int):
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise ValueError(
            "Excel import is not ready on this platform node. DataFlow bundles file parsers — retry shortly."
        ) from exc

    wb = load_workbook(content, read_only=True, data_only=True) if _is_path(content) else load_workbook(
        io.BytesIO(content), read_only=True, data_only=True
    )
    try:
        ws = wb.active
        if ws is None:
            return

        row_iter = ws.iter_rows(values_only=True)
        first = next(row_iter, None)
        if not first:
            return

        headers = [str(c).strip() if c is not None else f"col_{i}" for i, c in enumerate(first)]
        batch: list[dict] = []
        for row in row_iter:
            record = {
                headers[i]: ("" if c is None else str(c).strip())
                for i, c in enumerate(row[: len(headers)])
            }
            batch.append(record)
            if len(batch) >= chunk_size:
                yield batch
                batch = []
        if batch:
            yield batch
    finally:
        wb.close()


def _excel_count(content: bytes | str | os.PathLike) -> int:
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise ValueError(
            "Excel import is not ready on this platform node. DataFlow bundles file parsers — retry shortly."
        ) from exc

    wb = load_workbook(content, read_only=True, data_only=True) if _is_path(content) else load_workbook(
        io.BytesIO(content), read_only=True, data_only=True
    )
    try:
        ws = wb.active
        if ws is None:
            return 0
        return max(0, (ws.max_row or 1) - 1)
    finally:
        wb.close()


def supports_file_streaming(source_kind: str, filename: str, destination: EndpointConfig) -> bool:
    if source_kind != "file" or destination.kind != "database":
        return False
    try:
        from services.file_parser import FileParser
    except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
        from src.services.file_parser import FileParser

    return FileParser.detect_file_type(filename) in STREAMABLE_TYPES


def _decompress_bytes_if_gzip(data: bytes) -> bytes:
    """Decompress an in-memory gzip payload when applicable."""
    if _is_gzip_bytes(data):
        try:
            return gzip.decompress(data)
        except Exception:
            pass
    return data


def peek_file_source(
    content: bytes | str | os.PathLike,
    filename: str,
) -> tuple[list[str], dict[str, str], int, list[dict]]:
    """Return headers, inferred schema, total row count, and a sample of <=100 rows.

    Accepts either an in-memory ``bytes`` payload or an on-disk path so the
    whole file never has to be loaded at once.  Gzip-compressed payloads are
    decompressed on the fly.
    """
    try:
        from services.file_parser import FileParser
    except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
        from src.services.file_parser import FileParser

    raw_bytes = content if isinstance(content, bytes) else b""
    file_type = FileParser.detect_file_type(filename, raw_bytes or None)

    if file_type in ("csv", "tsv"):
        # Fast path for in-memory payloads that already fit in RAM.
        if isinstance(content, bytes):
            raw = _decompress_bytes_if_gzip(content)
            headers, rows, _enc, _delim = parse_csv_preview(raw, preview_rows=100)
            if not headers:
                raise ValueError("CSV file has no header row")
            total = count_csv_rows(raw)
            sample = [dict(zip(headers, row)) for row in rows[:100]]
            schema = FileParser.infer_schema(sample)
            return headers, schema, total, sample

        # Path-based streaming: read only the preview rows we need and count the
        # rest in a single pass without materializing every cell.
        sample_bytes = _first_bytes(content)
        delim = detect_delimiter(sample_bytes.decode(detect_encoding(sample_bytes), errors="replace"))
        preview_rows: list[list[str]] = []
        total = 0
        headers: list[str] = []
        with _text_reader(content) as reader_file:
            reader = csv.reader(reader_file, delimiter=delim)
            try:
                headers = next(reader)
            except StopIteration:
                raise ValueError("CSV file has no header row") from None
            for i, row in enumerate(reader):
                total += 1
                if i < 100:
                    preview_rows.append(row)
        sample = [dict(zip(headers, row)) for row in preview_rows]
        schema = FileParser.infer_schema(sample)
        return headers, schema, total, sample

    if file_type in ("jsonl", "ndjson"):
        sample_objs: list[dict] = []
        columns: set[str] = set()
        total = 0
        with _text_reader(content) as reader_file:
            for line in reader_file:
                line = line.strip()
                if not line:
                    continue
                total += 1
                if len(sample_objs) < 100:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        sample_objs.append(obj)
                        columns.update(obj.keys())
        if total == 0:
            raise ValueError("JSONL file is empty")
        headers = sorted(columns)
        schema = FileParser.infer_schema(sample_objs)
        return headers, schema, total, sample_objs[:100]

    if file_type == "excel":
        headers, rows, total = _excel_preview(content, preview_rows=100)
        if not headers:
            raise ValueError("Excel file has no header row")
        sample = [dict(zip(headers, row)) for row in rows[:100]]
        schema = FileParser.infer_schema(sample)
        return headers, schema, total, sample

    if file_type == "parquet":
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(content) if _is_path(content) else pq.ParquetFile(io.BytesIO(content))
        try:
            total = pf.metadata.num_rows
            headers = [str(c) for c in pf.schema_arrow.names]
            sample: list[dict] = []
            for batch in pf.iter_batches(batch_size=100):
                batch_df = batch.to_pandas()
                for _, row in batch_df.iterrows():
                    rec = {str(k): (row[k].item() if hasattr(row[k], "item") else (None if row[k] != row[k] else row[k])) for k in batch_df.columns}
                    sample.append(rec)
                    if len(sample) >= 100:
                        break
                if len(sample) >= 100:
                    break
        finally:
            pf.close()
        schema = FileParser.infer_schema(sample)
        return headers, schema, total, sample

    if file_type == "json":
        import ijson

        sample_objs: list[dict] = []
        columns: set[str] = set()
        total = 0
        with _open_binary(content) as bio:
            for obj in ijson.items(bio, "item"):
                if not isinstance(obj, dict):
                    continue
                total += 1
                if len(sample_objs) < 100:
                    sample_objs.append(obj)
                    columns.update(obj.keys())
        if total == 0:
            raise ValueError("JSON file must be an array of objects")
        headers = sorted(columns)
        schema = FileParser.infer_schema(sample_objs)
        return headers, schema, total, sample_objs[:100]

    raise ValueError(f"File type '{file_type}' does not support streaming ingest")


def _iter_csv_batches(
    content: bytes | str | os.PathLike,
    chunk_size: int,
):
    sample_bytes = _first_bytes(content)
    enc = detect_encoding(sample_bytes)
    sample = sample_bytes.decode(enc, errors="replace")
    delim = detect_delimiter(sample)
    with _text_reader(content, encoding=enc, newline="") as reader_file:
        reader = csv.DictReader(reader_file, delimiter=delim)
        batch: list[dict] = []
        for row in reader:
            batch.append(dict(row))
            if len(batch) >= chunk_size:
                yield batch
                batch = []
        if batch:
            yield batch


def _iter_jsonl_batches(
    content: bytes | str | os.PathLike,
    chunk_size: int,
):
    with _text_reader(content) as reader_file:
        batch: list[dict] = []
        for line in reader_file:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict):
                continue
            batch.append(obj)
            if len(batch) >= chunk_size:
                yield batch
                batch = []
        if batch:
            yield batch


def _iter_json_array_batches(
    content: bytes | str | os.PathLike,
    chunk_size: int,
):
    import ijson

    batch: list[dict] = []
    with _open_binary(content) as bio:
        for obj in ijson.items(bio, "item"):
            if not isinstance(obj, dict):
                continue
            batch.append(obj)
            if len(batch) >= chunk_size:
                yield batch
                batch = []
    if batch:
        yield batch


def _batch_iterator_for_type(
    file_type: str,
    content: bytes | str | os.PathLike,
    batch_size: int,
):
    """Return a fresh batch iterator for the given file type.

    Used to re-scan a file from the beginning (e.g. on resume) without mutating
    the primary streaming iterator.  Accepts either ``bytes`` or an on-disk path.
    """
    if file_type in ("csv", "tsv"):
        return _iter_csv_batches(content, batch_size)
    if file_type == "json":
        return _iter_json_array_batches(content, batch_size)
    if file_type == "jsonl" or file_type == "ndjson":
        return _iter_jsonl_batches(content, batch_size)
    if file_type == "excel":
        return _excel_batches(content, batch_size)
    if file_type == "parquet":
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(content) if _is_path(content) else pq.ParquetFile(io.BytesIO(content))

        def _parquet_batches():
            batch: list[dict] = []
            try:
                for record_batch in pf.iter_batches(batch_size=batch_size):
                    chunk_df = record_batch.to_pandas()
                    for _, row in chunk_df.iterrows():
                        rec = {str(k): (None if row[k] != row[k] else (row[k].item() if hasattr(row[k], "item") else row[k])) for k in chunk_df.columns}
                        batch.append(rec)
                        if len(batch) >= batch_size:
                            yield batch
                            batch = []
                    if batch:
                        yield batch
                        batch = []
            finally:
                pf.close()

        return _parquet_batches()
    raise ValueError(f"File type '{file_type}' does not support streaming ingest")


def should_stream_file(
    content: bytes | str | os.PathLike,
    filename: str,
    destination: EndpointConfig,
) -> bool:
    if not supports_file_streaming("file", filename, destination):
        return False
    if _is_path(content):
        return True
    if STREAM_THRESHOLD <= 1 and content:
        return True
    if not content:
        return False
    try:
        _, _, total, _ = peek_file_source(content, filename)
        return total >= STREAM_THRESHOLD
    except Exception:
        return False


def stream_file_to_database(
    content: bytes | str | os.PathLike,
    filename: str,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Callable[..., None] | None = None,
    *,
    sync_mode: str = "full_refresh_overwrite",
    stream_contracts: list[dict] | None = None,
    job_id: str | None = None,
    checkpoint: Checkpoint | None = None,
    checkpoint_service: CheckpointService | None = None,
    retry_budget: RetryBudget | None = None,
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    source_filter: dict[str, Any] | None = None,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    try:
        from services.file_parser import FileParser
    except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
        from src.services.file_parser import FileParser

    file_type = FileParser.detect_file_type(filename)
    columns, probe_schema, total_rows, sample_rows = peek_file_source(content, filename)
    if not schema:
        schema = probe_schema

    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in columns]

    try:
        from .connector_capabilities import resolve_driver_type
    except ImportError:
        from transfer.connector_capabilities import resolve_driver_type
    dest_type = resolve_driver_type(destination.format)
    dest_cfg = resolve_connector_config(destination)

    from services.value_serializer import json_default

    avg_row_size = 100
    if sample_rows:
        avg_row_size = max(1, int(sum(len(json.dumps(row, default=json_default)) for row in sample_rows) / len(sample_rows)))
    # MongoDB can safely ingest larger batches; keep other destinations under 8 MB
    # to avoid payload limits (e.g. BigQuery streaming insert ~10 MB).
    target_memory_bytes = 64 * 1024 * 1024 if dest_type == "mongodb" else 8 * 1024 * 1024
    batch_size = adaptive_chunk_size(CHUNK_SIZE, avg_row_size, max_size=CHUNK_SIZE, target_memory_bytes=target_memory_bytes)
    # Align file batches to the proxy writer commit size so a dropped socket never
    # straddles tens of thousands of already-committed rows inside one call.
    try:
        from connectors.write_resilience import proxy_stream_batch_size
    except ImportError:
        proxy_stream_batch_size = None  # type: ignore
    if proxy_stream_batch_size is not None:
        batch_size = proxy_stream_batch_size(
            dest_cfg.get("host"),
            connection_string=dest_cfg.get("connection_string")
            or dest_cfg.get("uri")
            or dest_cfg.get("url")
            or "",
            default=batch_size,
        )
    # Object-store writers (S3/GCS/ADLS) emit a single destination object per call.
    # Writing multiple batches would overwrite the same key and silently lose data,
    # so force a single batch for those destinations.
    if dest_type in ("s3", "gcs", "adls") and total_rows:
        batch_size = max(1, total_rows)
    chunks = max(1, (total_rows + batch_size - 1) // batch_size)
    dest_table = resolve_dest_table(dest_type, destination, "import")

    ddl_log: list[str] = [
        f"STREAM FILE {filename} → {dest_type}.{dest_table} ({total_rows:,} rows, {chunks} batches)",
    ]
    for col in columns:
        ddl_log.append(f"{dest_type.upper()} COLUMN {col} {ddl_type(dest_type, schema.get(col, 'string'))}")

    batch_iter = _batch_iterator_for_type(file_type, content, batch_size)

    column_types = {c: normalize_inferred(schema.get(c, "string")).upper() for c in columns}
    target_cols, _ = resolve_target_columns(mappings, column_types, preserve_case=True)

    try:
        from services.sync_cursor import (
            map_source_to_target,
            requires_upsert,
            resolve_sync_contract,
        )
    except ImportError:
        from src.services.sync_cursor import (
            map_source_to_target,
            requires_upsert,
            resolve_sync_contract,
        )
    contract = resolve_sync_contract(stream_contracts)
    effective_sync = contract.sync_mode if contract else sync_mode
    pk_target_cols: list[str] = []
    if contract and contract.primary_key:
        pk_target_cols = [map_source_to_target(contract.primary_key, mappings)]
    write_mode = "upsert" if requires_upsert(effective_sync) and pk_target_cols else "insert"

    checkpoint_service = checkpoint_service or CheckpointService()
    checkpoint = checkpoint or Checkpoint(job_id=job_id or "")
    checkpoint.source_type = "file"
    checkpoint.dest_type = dest_type
    checkpoint.write_mode = write_mode
    checkpoint.conflict_columns = pk_target_cols or []
    checkpoint.chunk_total = chunks
    retry = retry_budget or RetryBudget()

    written = checkpoint.rows_processed or 0
    chunk_idx = checkpoint.chunk_index or 0
    resumed = chunk_idx > 0 or written > 0
    dest_summary: dict[str, Any] = {}
    last_checksum = ""
    rejected_total = 0
    coerced_null_total = 0
    # Strict/maximum FAIL-FAST on coercion errors; balanced quarantines them.
    stream_error_policy = transform_error_policy_for_validation_mode(validation_mode)
    warning_samples: list[str] = []
    rejected_details: list[dict] = []

    # Resume: skip chunks that were already committed
    if chunk_idx > 0:
        batch_iter = itertools.islice(batch_iter, chunk_idx, None)

    if source_filter:
        batch_iter = (apply_row_filter(batch, source_filter) for batch in batch_iter)

    fp_accumulator = FingerprintAccumulator()
    batch_quality_enabled = validation_mode in ("strict", "maximum")
    try:
        from services.data_quality import (  # noqa: E402
            BatchDriftDetector,
            run_integrity_audit,
        )
    except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
        from src.services.data_quality import (  # noqa: E402
            BatchDriftDetector,
            run_integrity_audit,
        )
    drift_detector = BatchDriftDetector()

    max_workers = int(os.getenv("DATAFLOW_PARALLEL_WORKERS", str(min(2, os.cpu_count() or 1))))
    # SQLite handles concurrency poorly with a single shared file, so keep it sequential.
    # Snowflake COPY INTO uses a named temporary stage per table; concurrent batches
    # overwrite each other's stage files, so it must also be sequential.
    # Public TCP proxies (Railway, Neon, etc.) drop when multiple bulk writers share
    # the same host — force a single writer connection for those destinations.
    if dest_type in ("sqlite", "snowflake"):
        max_workers = 1
    else:
        try:
            from connectors.write_resilience import is_public_proxy_host
        except ImportError:  # pragma: no cover
            from write_resilience import is_public_proxy_host  # type: ignore
        proxy_host = str(dest_cfg.get("host") or "")
        proxy_cs = str(
            dest_cfg.get("connection_string")
            or dest_cfg.get("uri")
            or dest_cfg.get("url")
            or ""
        )
        if is_public_proxy_host(proxy_host) or is_public_proxy_host(proxy_cs):
            max_workers = 1

    def _process_file_chunk(idx: int, batch: list[dict]) -> dict[str, Any]:
        if not batch:
            return {
                "batch_written": 0,
                "last_checksum": "",
                "dest_summary": {},
                "fingerprints": [],
                "rejected": 0,
                "coerced_null": 0,
                "warnings": [],
                "rejected_details": [],
                "batch_rows": 0,
            }
        headers, data_rows = records_to_matrix(batch, columns)
        local_warnings: list[str] = []

        # Per-batch data-quality / anomaly gate.
        if batch_quality_enabled:
            audit = run_integrity_audit(
                headers=headers,
                rows=data_rows,
                column_types=column_types,
                mappings=mappings,
                validation_mode=validation_mode,
            )
            if audit.issues:
                local_warnings.extend(audit.issues[:10])
            if audit.warnings:
                local_warnings.extend(audit.warnings[:10])
            if not audit.passed:
                raise ValueError(f"Batch {idx} failed data-quality audit: {'; '.join(audit.issues[:5])}")

            # Cross-batch drift detection against the first batch's statistics.
            drift_warnings = drift_detector.check(audit.stats or {})
            if drift_warnings:
                if validation_mode == "maximum":
                    raise ValueError(f"Batch {idx} drift detected: {'; '.join(drift_warnings[:3])}")
                local_warnings.extend(drift_warnings[:3])

        # Compute source fingerprints from the mapped batch without materializing
        # the whole file.  This replaces the final FileParser.parse() pass.
        mapped_rows, _ = build_mapped_rows(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            error_policy=stream_error_policy,
            preserve_case=True,
        )
        fingerprints = row_fingerprints(mapped_rows, target_cols) if mapped_rows else []

        write_op = partial(
            _write_batch,
            dest_type,
            destination,
            dest_cfg,
            dest_table,
            headers,
            data_rows,
            mappings,
            column_types,
            create_table=(idx == first_index),
            on_checkpoint=None,
            chunk_idx=idx,
            total_chunks=chunks,
            rows_so_far=0,
            write_mode=write_mode,
            conflict_columns=pk_target_cols,
            backfill_new_fields=backfill_new_fields,
            error_policy=stream_error_policy,
            job_id=job_id,
        )
        batch_written, last_checksum, dest_summary = with_retry(
            write_op,
            budget=RetryBudget(
                max_attempts=retry.max_attempts,
                base_delay_seconds=retry.base_delay_seconds,
                max_delay_seconds=retry.max_delay_seconds,
                exponential_base=retry.exponential_base,
                jitter=retry.jitter,
            ),
        )
        return {
            "batch_written": batch_written,
            "last_checksum": last_checksum,
            "dest_summary": dest_summary,
            "fingerprints": fingerprints,
            "rejected": int(dest_summary.get("rejected_rows", 0) or 0),
            "coerced_null": int(dest_summary.get("coerced_null_rows", 0) or 0),
            "warnings": (dest_summary.get("warnings") or [])[:10] + local_warnings,
            "rejected_details": (dest_summary.get("rejected_details") or [])[:200],
            "batch_rows": len(data_rows),
        }

    first_index = chunk_idx + 1
    batch_enum = enumerate(batch_iter, start=first_index)

    def _apply_file_result(idx: int, result: dict[str, Any]) -> None:
        nonlocal written, rejected_total, coerced_null_total, last_checksum
        if result["fingerprints"]:
            fp_accumulator.add_many(result["fingerprints"])
        written += result["batch_written"]
        rejected_total += result["rejected"]
        coerced_null_total += result.get("coerced_null", 0)
        warning_samples.extend(result["warnings"])
        rejected_details.extend(result["rejected_details"])
        last_checksum = result["last_checksum"] or last_checksum

        checkpoint.chunk_index = idx
        checkpoint.rows_processed = written
        checkpoint.checksum = last_checksum
        checkpoint.phase = "writing"
        checkpoint.status = "running"
        checkpoint_service.save(checkpoint)
        if on_checkpoint:
            on_checkpoint(idx, chunks, written, checkpoint.to_dict())

    try:
        first_idx, first_batch = next(batch_enum)
    except StopIteration:
        raise ValueError("No records found in file")

    # Process the first batch synchronously so DDL (table/index creation) is
    # committed before any parallel workers try to insert into the new table.
    _apply_file_result(first_idx, _process_file_chunk(first_idx, first_batch))

    with OrderedChunkRunner(max_workers=max_workers) as runner:
        for idx, result in runner.run(batch_enum, _process_file_chunk):
            _apply_file_result(idx, result)

    if written == 0:
        raise ValueError("No records found in file")

    # The source checksum has been accumulated incrementally from each batch's
    # mapped fingerprints, so we do not need to parse the entire file a second
    # time.  The FingerprintAccumulator spills to disk above the threshold, so
    # even billion-row transfers stay memory-bounded by a single batch.
    # If the job resumed, we must re-scan the whole file so the fingerprint
    # covers all source rows, not only the ones processed after the checkpoint.
    if resumed and fp_accumulator.total < total_rows:
        full_iter = _batch_iterator_for_type(file_type, content, batch_size)
        full_accumulator = FingerprintAccumulator()
        for batch in full_iter:
            if not batch:
                continue
            headers, data_rows = records_to_matrix(batch, columns)
            mapped_rows, _ = build_mapped_rows(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                error_policy=stream_error_policy,
                preserve_case=True,
            )
            if mapped_rows:
                full_accumulator.add_many(row_fingerprints(mapped_rows, target_cols))
        final_checksum = full_accumulator.digest() if full_accumulator.total else last_checksum
    else:
        final_checksum = fp_accumulator.digest() if fp_accumulator.total else last_checksum

    dest_summary["checksum"] = final_checksum or last_checksum
    dest_summary["rejected_rows"] = rejected_total
    dest_summary["coerced_null_rows"] = coerced_null_total
    dest_summary["rejected_details"] = rejected_details[:200]
    dest_summary["warnings"] = warning_samples[:10]
    dest_summary["error_policy"] = "quarantine" if (rejected_total or coerced_null_total) else "none"

    if dest_type in ("postgresql", "mysql", "redshift") and job_id:
        try:
            from connectors.write_resilience import cleanup_write_ledger
        except ImportError:
            cleanup_write_ledger = None  # type: ignore
        if cleanup_write_ledger is not None:
            cleanup_write_ledger(dest_type=dest_type, cfg=dest_cfg, job_id=job_id)

    return written, ddl_log, dest_summary, columns
