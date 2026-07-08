"""Streaming file → database transfer for CSV, TSV, and JSONL."""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable

from .models import EndpointConfig
from .type_mapper import ddl_type, normalize_inferred

_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from connectors.writer_common import CHUNK_SIZE  # noqa: E402
from services.csv_profiler import count_csv_rows, detect_delimiter, detect_encoding, parse_csv_preview  # noqa: E402

from .adapters import records_to_matrix, resolve_connector_config
from .stream import _write_batch

STREAMABLE_TYPES = {"csv", "tsv", "jsonl"}
STREAM_THRESHOLD = int(os.getenv("DATAFLOW_STREAM_FILE_ROWS", "1"))


def _decode(content: bytes) -> str:
    enc = detect_encoding(content)
    return content.decode(enc, errors="replace")


def supports_file_streaming(source_kind: str, filename: str, destination: EndpointConfig) -> bool:
    if source_kind != "file" or destination.kind != "database":
        return False
    from ..services.file_parser import FileParser

    return FileParser.detect_file_type(filename) in STREAMABLE_TYPES


def peek_file_source(content: bytes, filename: str) -> tuple[list[str], dict[str, str], int, list[dict]]:
    from ..services.file_parser import FileParser

    file_type = FileParser.detect_file_type(filename)
    if file_type in ("csv", "tsv"):
        headers, rows, _enc, delim = parse_csv_preview(content, preview_rows=100)
        if not headers:
            raise ValueError("CSV file has no header row")
        total = count_csv_rows(content)
        sample = [dict(zip(headers, row)) for row in rows[:100]]
        schema = FileParser.infer_schema(sample)
        return headers, schema, total, sample

    if file_type == "jsonl":
        text = _decode(content)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if not lines:
            raise ValueError("JSONL file is empty")
        sample_objs: list[dict] = []
        columns: set[str] = set()
        for line in lines[:100]:
            obj = json.loads(line)
            if isinstance(obj, dict):
                sample_objs.append(obj)
                columns.update(obj.keys())
        headers = sorted(columns)
        schema = FileParser.infer_schema(sample_objs)
        return headers, schema, len(lines), sample_objs[:100]

    raise ValueError(f"File type '{file_type}' does not support streaming ingest")


def _iter_csv_batches(content: bytes, chunk_size: int):
    text = _decode(content)
    delim = detect_delimiter(text[:8192])
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)
    batch: list[dict] = []
    for row in reader:
        batch.append(dict(row))
        if len(batch) >= chunk_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _iter_jsonl_batches(content: bytes, chunk_size: int):
    text = _decode(content)
    batch: list[dict] = []
    for line in text.splitlines():
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


def stream_file_to_database(
    content: bytes,
    filename: str,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    from ..services.file_parser import FileParser

    file_type = FileParser.detect_file_type(filename)
    columns, probe_schema, total_rows, _ = peek_file_source(content, filename)
    if not schema:
        schema = probe_schema

    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in columns]

    dest_type = destination.format.lower()
    dest_cfg = resolve_connector_config(destination)
    chunks = max(1, (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE)
    base = re.sub(r"[^a-zA-Z0-9_]", "_", (destination.table or destination.collection or "import").lower())[:40]
    dest_table = destination.table or destination.collection or f"dt_{base}"

    ddl_log: list[str] = [
        f"STREAM FILE {filename} → {dest_type}.{dest_table} ({total_rows:,} rows, {chunks} batches)",
    ]
    for col in columns:
        ddl_log.append(f"{dest_type.upper()} COLUMN {col} {ddl_type(dest_type, schema.get(col, 'string'))}")

    if file_type in ("csv", "tsv"):
        batch_iter = _iter_csv_batches(content, CHUNK_SIZE)
    else:
        batch_iter = _iter_jsonl_batches(content, CHUNK_SIZE)

    column_types = {c: normalize_inferred(schema.get(c, "string")).upper() for c in columns}
    written = 0
    chunk_idx = 0
    dest_summary: dict[str, Any] = {}
    last_checksum = ""
    rejected_total = 0
    warning_samples: list[str] = []

    for batch in batch_iter:
        if not batch:
            continue
        chunk_idx += 1
        headers, data_rows = records_to_matrix(batch, columns)
        batch_written, last_checksum, dest_summary = _write_batch(
            dest_type,
            destination,
            dest_cfg,
            dest_table,
            headers,
            data_rows,
            mappings,
            column_types,
            create_table=(chunk_idx == 1),
            on_checkpoint=on_checkpoint,
            chunk_idx=chunk_idx,
            total_chunks=chunks,
            rows_so_far=written,
        )
        written += batch_written
        rejected_total += int(dest_summary.get("rejected_rows", 0) or 0)
        warning_samples.extend(dest_summary.get("warnings", []) or [])
        if on_checkpoint:
            on_checkpoint(chunk_idx, chunks, written)

    if written == 0:
        raise ValueError("No records found in file")

    dest_summary["checksum"] = last_checksum
    dest_summary["rejected_rows"] = rejected_total
    dest_summary["warnings"] = warning_samples[:10]
    dest_summary["error_policy"] = "quarantine" if rejected_total else "none"
    return written, ddl_log, dest_summary, columns


def should_stream_file(content: bytes, filename: str, destination: EndpointConfig) -> bool:
    if not supports_file_streaming("file", filename, destination):
        return False
    try:
        _, _, total, _ = peek_file_source(content, filename)
        return total >= STREAM_THRESHOLD
    except Exception:
        return False
