"""Bounded map → quarantine → serialize for every object-store writer.

S3 / GCS / ADLS / SFTP / Email used to materialize the full accepted
``mapped_rows`` list, run the write-quarantine matrix, then serialize. That
is a second full copy of the engine batch in RAM (Airbyte/Fivetran file
destinations flush a record buffer; they do not keep the mapped image).

This module is the single algorithm:

1. Expand STRUCT/explode policies once on the engine ``data_rows`` batch
   (already in RAM — that contract does not change).
2. Map + quarantine in bounded bundles (Beam/Dataflow bundle size; default
   1024). Accepted tuples are not retained after the bundle is encoded.
3. Encode each bundle onto the shared spill spool. JSON/CSV/JSONL/TSV write
   one record at a time. Parquet writes one Arrow ``RecordBatch`` per bundle
   (not a full-table ``pa.table``).
4. Gate-8 sample (first 50), ``FingerprintAccumulator`` checksum, and
   reject details accumulate without the accepted-row list.
5. ``fail`` / FAIL_JOB still collect every reject in the engine batch, then
   discard the spool — never upload/send a partial primary write.

Honesty: ``data_rows`` stay in RAM. This is not a source-stream spill and
not exactly-once. Quarantine stays per-row. Catalog tiles ≠ transfer-live.
"""

from __future__ import annotations

import csv
import io
import json
import tempfile
from dataclasses import dataclass, field
from typing import Any, Sequence

from services.brand_env import getenv_brand
from services.value_serializer import cell_to_string, json_default

DEFAULT_MATERIALIZE_BATCH = 1024
_SAMPLE_LIMIT = 50


def resolve_materialize_batch(extra: dict[str, Any] | None = None) -> int:
    """Accepted-row bundle size before encode. Peak mapped RAM is this many tuples."""
    extra = extra if isinstance(extra, dict) else {}
    raw = (
        extra.get("materialize_batch")
        or extra.get("object_store_materialize_batch")
        or getenv_brand("DATAFLOW_OBJECT_STORE_MATERIALIZE_BATCH", "")
        or ""
    )
    if raw:
        return max(1, int(raw))
    return DEFAULT_MATERIALIZE_BATCH


@dataclass
class ObjectStoreMaterializeResult:
    """Outcome of one engine-batch materialize. ``export`` is None on abort."""

    export: Any | None
    rows_written: int
    rejected_details: list[dict[str, Any]]
    transform_errors: list[str]
    checksum: str
    meta: dict[str, Any]
    abort_error: str | None
    rejected_rows: int
    coerced_null_rows: int
    batch_sizes: list[int] = field(default_factory=list)


class ObjectStoreEncoder:
    """Streaming encoder onto a ``SpooledTemporaryFile``.

    Format trailer (JSON ``]``, Parquet footer) is written only in ``finish``.
    ``abort`` discards the spool so a fail-closed job cannot upload bytes.
    """

    def __init__(
        self,
        *,
        key: str,
        target_cols: list[str],
        dest_types: dict[str, str] | None = None,
        spill_max_size: int = 8 * 1024 * 1024,
    ) -> None:
        self.key = key or ""
        self.key_l = self.key.lower()
        self.target_cols = list(target_cols)
        self.dest_types = dest_types or {}
        self.spill_max_size = max(1, int(spill_max_size))
        self._spool = tempfile.SpooledTemporaryFile(
            max_size=self.spill_max_size, mode="w+b"
        )
        self._records_written = 0
        self._header_written = False
        self._text: io.TextIOWrapper | None = None
        self._csv: csv.DictWriter | None = None
        self._pq_writer: Any = None
        self._arrow_schema: Any = None
        self._arrow_types: list[Any] | None = None
        self._closed = False
        self._aborted = False
        self.content_type = self._content_type_for_key()

    def _content_type_for_key(self) -> str:
        if self.key_l.endswith(".parquet"):
            return "application/vnd.apache.parquet"
        if self.key_l.endswith(".csv"):
            return "text/csv"
        if self.key_l.endswith(".tsv"):
            return "text/tab-separated-values"
        if self.key_l.endswith(".jsonl"):
            return "application/x-ndjson"
        return "application/json"

    def append_rows(self, mapped_rows: Sequence[tuple]) -> None:
        if self._closed or self._aborted:
            raise RuntimeError("ObjectStoreEncoder is closed")
        if not mapped_rows:
            return
        if self.key_l.endswith(".parquet"):
            self._append_parquet(mapped_rows)
        elif self.key_l.endswith(".csv") or self.key_l.endswith(".tsv"):
            self._append_delimited(mapped_rows)
        elif self.key_l.endswith(".jsonl"):
            self._append_jsonl(mapped_rows)
        else:
            self._append_json_array(mapped_rows)
        self._records_written += len(mapped_rows)

    def _ensure_csv(self) -> csv.DictWriter:
        if self._csv is not None:
            return self._csv
        delim = "\t" if self.key_l.endswith(".tsv") else ","
        self._text = io.TextIOWrapper(
            self._spool, encoding="utf-8", newline="", write_through=True
        )
        self._csv = csv.DictWriter(
            self._text,
            fieldnames=self.target_cols,
            delimiter=delim,
            extrasaction="ignore",
        )
        self._csv.writeheader()
        self._header_written = True
        return self._csv

    def _append_delimited(self, mapped_rows: Sequence[tuple]) -> None:
        from connectors.writer_common import iter_mapped_json_records

        writer = self._ensure_csv()
        for record in iter_mapped_json_records(
            list(mapped_rows), self.target_cols, self.dest_types
        ):
            writer.writerow({k: cell_to_string(v) for k, v in record.items()})
        if self._text is not None:
            self._text.flush()

    def _append_jsonl(self, mapped_rows: Sequence[tuple]) -> None:
        from connectors.writer_common import iter_mapped_json_records

        for record in iter_mapped_json_records(
            list(mapped_rows), self.target_cols, self.dest_types
        ):
            if self._records_written or self._header_written:
                self._spool.write(b"\n")
            self._header_written = True
            self._spool.write(
                json.dumps(
                    record,
                    default=json_default,
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
            )

    def _emit_json_record(self, rec: dict[str, Any], *, comma: bool) -> None:
        dumped = json.dumps(
            rec, indent=2, default=json_default, ensure_ascii=False, allow_nan=False
        )
        indented = "\n".join(
            (f"  {line}" if line else line) for line in dumped.split("\n")
        )
        if comma:
            self._spool.write(b",\n")
        self._spool.write(indented.encode("utf-8"))

    def _append_json_array(self, mapped_rows: Sequence[tuple]) -> None:
        from connectors.writer_common import iter_mapped_json_records

        records = iter_mapped_json_records(
            list(mapped_rows), self.target_cols, self.dest_types
        )
        for record in records:
            if not self._header_written:
                self._spool.write(b"[\n")
                self._emit_json_record(record, comma=False)
                self._header_written = True
            else:
                self._emit_json_record(record, comma=True)

    def _ensure_parquet(self) -> None:
        if self._pq_writer is not None:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        from services.arrow_write import logical_to_arrow_type

        self._arrow_types = [
            logical_to_arrow_type(
                str(self.dest_types.get(c, "TEXT") or "TEXT"), pa, dialect="parquet"
            )
            for c in self.target_cols
        ]
        self._arrow_schema = pa.schema(
            [(c, t) for c, t in zip(self.target_cols, self._arrow_types)]
        )
        self._pq_writer = pq.ParquetWriter(
            self._spool, self._arrow_schema, compression="snappy"
        )
        self._header_written = True

    def _append_parquet(self, mapped_rows: Sequence[tuple]) -> None:
        import pyarrow as pa

        from services.arrow_write import coerce_arrow_cell

        self._ensure_parquet()
        assert self._arrow_types is not None and self._arrow_schema is not None
        columns: dict[str, list[Any]] = {c: [] for c in self.target_cols}
        for row in mapped_rows:
            for col, val, at in zip(self.target_cols, row, self._arrow_types):
                columns[col].append(coerce_arrow_cell(val, at, pa, dialect="parquet"))
        batch = pa.RecordBatch.from_pydict(columns, schema=self._arrow_schema)
        self._pq_writer.write_batch(batch)

    def finish(self):
        """Write format trailer and return a rewindable ``ObjectStoreExport``."""
        from connectors.object_store_common import ObjectStoreExport

        if self._aborted:
            raise RuntimeError("ObjectStoreEncoder was aborted")
        if self._closed:
            raise RuntimeError("ObjectStoreEncoder already finished")
        try:
            if self.key_l.endswith(".parquet"):
                if self._pq_writer is None:
                    self._write_empty_parquet()
                else:
                    self._pq_writer.close()
                    self._pq_writer = None
            elif self.key_l.endswith(".csv") or self.key_l.endswith(".tsv"):
                if not self._header_written:
                    self._ensure_csv()
                if self._text is not None:
                    self._text.flush()
                    self._text.detach()
                    self._text = None
            elif self.key_l.endswith(".jsonl"):
                pass
            else:
                if not self._header_written:
                    self._spool.write(b"[]")
                else:
                    self._spool.write(b"\n]")
            size = int(self._spool.tell())
            self._spool.seek(0)
            export = ObjectStoreExport(
                content_type=self.content_type,
                size=size,
                spilled=size > self.spill_max_size,
                _spool=self._spool,
            )
            self._spool = None
            self._closed = True
            return export
        except Exception:
            self.abort()
            raise

    def _write_empty_parquet(self) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        from services.arrow_write import logical_to_arrow_type

        arrow_types = [
            logical_to_arrow_type(
                str(self.dest_types.get(c, "TEXT") or "TEXT"), pa, dialect="parquet"
            )
            for c in self.target_cols
        ]
        schema = pa.schema([(c, t) for c, t in zip(self.target_cols, arrow_types)])
        table = pa.table({c: [] for c in self.target_cols}, schema=schema)
        pq.write_table(table, self._spool, compression="snappy")

    def abort(self) -> None:
        """Discard the spool. Safe to call more than once."""
        self._aborted = True
        self._closed = True
        writer = self._pq_writer
        self._pq_writer = None
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        text = self._text
        self._text = None
        if text is not None:
            try:
                text.detach()
            except Exception:
                pass
        spool = self._spool
        self._spool = None
        if spool is not None:
            try:
                spool.close()
            except Exception:
                pass


def materialize_object_store_export(
    *,
    key: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    target_cols: list[str],
    column_types: dict[str, str] | None,
    dest_types: dict[str, str] | None,
    error_policy: str | None,
    dest_kind: str,
    dialect_label: str,
    spill_max_size: int,
    batch_size: int = DEFAULT_MATERIALIZE_BATCH,
    dest_db_type: str = "",
    preserve_case: bool = True,
    destination_column_types: dict[str, str] | None = None,
    destination_column_nullability: dict[str, bool] | None = None,
    empty_cells_as_null: bool = False,
) -> ObjectStoreMaterializeResult:
    """Map, quarantine, and encode one engine batch without retaining mapped_rows.

    ``destination_column_types`` is accepted for call-site symmetry; dest types
    must already be resolved by the writer (Studio coverage gate).
    """
    del destination_column_types
    from connectors.writer_common import (
        _coerced_null_row_count,
        _rejected_row_count,
        apply_write_quarantine_matrix,
        build_mapped_rows_with_details,
        gate8_writer_meta,
        reject_on_strict_policy,
        transform_error_policy,
    )
    from services.fingerprint_accumulator import FingerprintAccumulator
    from services.json_intelligence import materialize_struct_policies
    from services.reconciliation import _iter_fingerprints

    policy = transform_error_policy(error_policy)
    dest_types = dest_types or {}
    column_types = column_types or {}
    tgt_types = [str(dest_types.get(c, "") or "") for c in target_cols]
    headers, data_rows = materialize_struct_policies(headers, data_rows, mappings)

    encoder = ObjectStoreEncoder(
        key=key,
        target_cols=target_cols,
        dest_types=dest_types,
        spill_max_size=spill_max_size,
    )
    acc = FingerprintAccumulator()
    sample_rows: list[tuple] = []
    rejected_details: list[dict[str, Any]] = []
    transform_errors: list[str] = []
    batch_sizes: list[int] = []
    rows_written = 0
    encoding = True
    bundle = max(1, int(batch_size))

    try:
        for start in range(0, len(data_rows), bundle):
            chunk = data_rows[start : start + bundle]
            accepted_nums: list[int] = []
            mapped, errors, details = build_mapped_rows_with_details(
                headers=headers,
                data_rows=chunk,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                dest_types=dest_types,
                error_policy=policy,
                preserve_case=preserve_case,
                dest_kind=dest_kind,
                destination_pk_columns=None,
                destination_column_nullability=destination_column_nullability,
                empty_cells_as_null=empty_cells_as_null,
                row_number_start=start + 1,
                accepted_source_rows=accepted_nums,
                struct_already_materialized=True,
            )
            transform_errors.extend(errors)
            rejected_details.extend(details)
            mapped = apply_write_quarantine_matrix(
                mapped,
                target_cols,
                tgt_types,
                rejected_details,
                policy,
                dialect_label=dialect_label,
                mappings=mappings,
                source_row_numbers=accepted_nums,
            )
            # FAIL_JOB / strict: keep mapping the rest of the engine batch so
            # the operator sees every reject, but stop encoding immediately.
            if encoding and reject_on_strict_policy(
                policy, rejected_details, dialect_label, transform_errors
            ):
                encoding = False
                encoder.abort()
            if encoding:
                encoder.append_rows(mapped)
                batch_sizes.append(len(mapped))
                acc.add_many(
                    _iter_fingerprints(
                        mapped,
                        target_cols,
                        dest_db_type=dest_db_type,
                        dest_types=dest_types,
                    )
                )
                need = _SAMPLE_LIMIT - len(sample_rows)
                if need > 0:
                    sample_rows.extend(mapped[:need])
                rows_written += len(mapped)
            del mapped

        abort_error = reject_on_strict_policy(
            policy, rejected_details, dialect_label, transform_errors
        )
        if abort_error:
            encoder.abort()
            return ObjectStoreMaterializeResult(
                export=None,
                rows_written=0,
                rejected_details=rejected_details,
                transform_errors=transform_errors[:10],
                checksum="",
                meta={},
                abort_error=abort_error,
                rejected_rows=len(
                    {d.get("row") for d in rejected_details if d.get("row") is not None}
                )
                or len(rejected_details),
                coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
                batch_sizes=batch_sizes,
            )

        export = encoder.finish()
        checksum = acc.digest()
        meta = gate8_writer_meta(sample_rows, target_cols)
        meta["source_row_count"] = rows_written
        return ObjectStoreMaterializeResult(
            export=export,
            rows_written=rows_written,
            rejected_details=rejected_details,
            transform_errors=transform_errors[:10],
            checksum=checksum,
            meta=meta,
            abort_error=None,
            rejected_rows=_rejected_row_count(
                data_rows, [()] * rows_written, rejected_details, policy
            ),
            coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
            batch_sizes=batch_sizes,
        )
    except Exception:
        encoder.abort()
        raise
