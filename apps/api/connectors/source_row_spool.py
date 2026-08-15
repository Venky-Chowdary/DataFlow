"""Disk-backed source-row spool for object-store and SQL/warehouse materialize.

The engine still holds the current chunk's ``records`` (that contract does
not change). This module removes the *second* full matrix — ``records_to_matrix``
plus STRUCT explode/flatten — from S3/GCS/ADLS/SFTP/Email and from
PostgreSQL/Redshift/MySQL/Snowflake/BigQuery/SQLite/generic SQL.

Algorithm (Spark external spill / Beam bundle):

1. Convert one source record (or matrix row) at a time.
2. Run STRUCT flatten/explode through ``iter_struct_materialized_rows`` so a
   20k × 256 explode cannot become a 5.1M-row Python list.
3. Write each expanded row as JSONL onto a ``SpooledTemporaryFile`` that
   rolls to disk above ``DATAFLOW_SOURCE_ROW_SPILL_MAX``.
4. Materialize reads ``batch_size`` rows, maps, quarantines, encodes, drops
   the bundle.

Honesty: engine ``records`` stay in RAM until the write returns. This is not
a source-file stream (file_stream already chunks) and not exactly-once.
Sparse CDC ``DF_MISSING`` survives as the durable wire spelling.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Iterable, Iterator
from typing import Any

from services.brand_env import getenv_brand
from services.value_serializer import (
    DF_MISSING_SENTINEL,
    is_missing_sentinel,
    json_default,
)

DEFAULT_SOURCE_SPILL_MAX = 8 * 1024 * 1024
OBJECT_STORE_WRITE_KINDS = frozenset({
    "s3",
    "minio",
    "gcs",
    "gcp_storage",
    "adls",
    "azure_blob",
    "azure_data_lake",
    "sftp",
    "email",
    "smtp",
})


def resolve_source_spill_max(extra: dict[str, Any] | None = None) -> int:
    extra = extra if isinstance(extra, dict) else {}
    raw = extra.get("source_spill_max")
    if raw is None:
        raw = extra.get("object_store_source_spill_max")
    if raw is None or raw == "":
        raw = getenv_brand("DATAFLOW_SOURCE_ROW_SPILL_MAX", "") or ""
    if raw != "" and raw is not None:
        return max(1, int(raw))
    return DEFAULT_SOURCE_SPILL_MAX


def matrix_row_from_record(record: dict[str, Any], columns: list[str]) -> list[Any]:
    """One source record → one matrix row. Absent key is DF_MISSING, not NULL."""
    from services.value_serializer import cell_to_string

    row: list[Any] = []
    for col in columns:
        if col not in record:
            row.append(DF_MISSING_SENTINEL)
            continue
        val = record[col]
        if is_missing_sentinel(val):
            row.append(DF_MISSING_SENTINEL)
        elif val is None:
            row.append(None)
        else:
            row.append(cell_to_string(val))
    return row


def iter_matrix_rows(
    records: Iterable[dict[str, Any]], columns: list[str]
) -> Iterator[list[Any]]:
    for rec in records:
        yield matrix_row_from_record(rec, columns)


def _spool_cell(value: Any) -> Any:
    if is_missing_sentinel(value):
        return DF_MISSING_SENTINEL
    return value


def _load_cell(value: Any) -> Any:
    if value == DF_MISSING_SENTINEL:
        return DF_MISSING_SENTINEL
    return value


class SourceRowSpool:
    """JSONL matrix spool. Peak RAM after ingest is one bundle, not the batch."""

    def __init__(self, *, spill_max_size: int = DEFAULT_SOURCE_SPILL_MAX) -> None:
        self.spill_max_size = max(1, int(spill_max_size))
        self.headers: list[str] = []
        self.row_count = 0
        self.size = 0
        self.spilled = False
        self._spool = tempfile.SpooledTemporaryFile(
            max_size=self.spill_max_size, mode="w+b"
        )
        self._closed = False

    def ingest(
        self,
        headers: list[str],
        rows: Iterable[list[Any]],
        mappings: list[dict[str, Any]] | None = None,
    ) -> None:
        from services.json_intelligence import iter_struct_materialized_rows

        header_list, row_iter = iter_struct_materialized_rows(headers, rows, mappings)
        self.headers = list(header_list)
        for row in row_iter:
            payload = [_spool_cell(c) for c in row]
            self._spool.write(
                json.dumps(
                    payload,
                    default=json_default,
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
            )
            self._spool.write(b"\n")
            self.row_count += 1
        self.size = int(self._spool.tell())
        self.spilled = self.size > self.spill_max_size

    def ingest_records(
        self,
        columns: list[str],
        records: Iterable[dict[str, Any]],
        mappings: list[dict[str, Any]] | None = None,
    ) -> None:
        self.ingest(columns, iter_matrix_rows(records, columns), mappings)

    def ingest_matrix(
        self,
        headers: list[str],
        data_rows: Iterable[list[Any]],
        mappings: list[dict[str, Any]] | None = None,
    ) -> None:
        self.ingest(headers, data_rows, mappings)

    def iter_bundles(self, batch_size: int) -> Iterator[tuple[int, list[list[Any]]]]:
        """Yield ``(1-based start_row, bundle)`` after rewind. ``start_row`` is global."""
        if self._closed or self._spool is None:
            return
        self._spool.seek(0)
        bundle_n = max(1, int(batch_size))
        bundle: list[list[Any]] = []
        start = 1
        seen = 0
        for raw in self._spool:
            line = raw.decode("utf-8").rstrip("\n")
            if not line:
                continue
            row = [_load_cell(c) for c in json.loads(line)]
            if not bundle:
                start = seen + 1
            bundle.append(row)
            seen += 1
            if len(bundle) >= bundle_n:
                yield start, bundle
                bundle = []
        if bundle:
            yield start, bundle

    def close(self) -> None:
        self._closed = True
        spool = self._spool
        self._spool = None  # type: ignore[assignment]
        if spool is not None:
            try:
                spool.close()
            except Exception:
                pass
