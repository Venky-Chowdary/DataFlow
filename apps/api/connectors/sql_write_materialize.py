"""Disk-backed source-row spool and bounded mapped-image stream for SQL writers.

Object-store writers already stream STRUCT flatten/explode through
``SourceRowSpool``. PostgreSQL, Redshift, MySQL, Snowflake, BigQuery,
SQLite, and generic SQL used to concatenate every mapped bundle into one
Python list before COPY / INSERT / MERGE. A 20k × 256 explode became a
5.1M-row mapped image held until the write returned.

This module is the single algorithm:

1. Ingest engine ``records`` (preferred) or the unexpanded matrix through
   ``SourceRowSpool``. STRUCT flatten/explode is streaming — the expanded
   source matrix is never a Python list.
2. Map each bundle with ``struct_already_materialized=True`` and a 1-based
   global ``row_number_start`` (spool starts are already 1-based — do not
   add 1 again).
3. Finish each bundle (shared quarantine matrix, in-bundle last-write-wins
   dedupe, optional bind). Peak *mapped* RAM is one bundle (~1024), not
   the full explode.
4. ``SqlWriteAccumulator`` streams Gate-8 sample (first 50) and
   ``FingerprintAccumulator`` checksum without retaining accepted tuples.
5. ``fail`` / FAIL_JOB scan every bundle, collect every reject, then refuse
   the primary write — never INSERT a prefix and abort after commit.

Honesty: ``build_mapped_rows_from_source`` concatenates the mapped image
(retain contract for unit tests). Production ``write_mapped_rows`` for
PostgreSQL/Redshift, MySQL, SQLite, Snowflake, BigQuery, and generic SQL
iterates finished bundles after live DDL is settled and drops each bundle
after COPY/INSERT/MERGE. Engine ``records`` are spilled before this module
runs when the adapter passed ``source_spool``. This is not a source-file
stream and not exactly-once. CDC default remains at-least-once upsert.
Cross-bundle duplicate PKs land via ON CONFLICT / dest LSN — we do not
invent a forward seen-set that drops the later row. Catalog tiles ≠
transfer-live.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any

from connectors.source_row_spool import (
    SourceRowSpool,
    matrix_row_from_record,
    resolve_source_spill_max,
)
from services.brand_env import getenv_brand

_SAMPLE_LIMIT = 50

SQL_SPOOL_WRITE_KINDS = frozenset({
    "postgresql",
    "postgres",
    "redshift",
    "amazon_redshift",
    "redshift_serverless",
    "mysql",
    "mariadb",
    "snowflake",
    "bigquery",
    "sqlite",
    "generic_sql",
})


def resolve_sql_materialize_batch(extra: dict[str, Any] | None = None) -> int:
    """Map-bundle size for warehouse writers. Peak mapped-source RAM is this many rows."""
    extra = extra if isinstance(extra, dict) else {}
    raw = extra.get("sql_materialize_batch")
    if raw is None or raw == "":
        raw = getenv_brand("DATAFLOW_SQL_MATERIALIZE_BATCH", "") or ""
    if raw != "" and raw is not None:
        return max(1, int(raw))
    from connectors.object_store_materialize import resolve_materialize_batch

    return resolve_materialize_batch(extra)


def sql_source_from_writer(
    _kwargs: dict[str, Any], extra: dict[str, Any] | None
) -> dict[str, Any]:
    """Records + spill / bundle settings from a writer ``**_kwargs`` / dest_extra.

    An engine-owned ``source_spool`` wins: writers must not re-ingest
    ``records`` (that would replay STRUCT explode onto a second file).
    """
    source_spool = _kwargs.get("source_spool")
    if source_spool is not None and not hasattr(source_spool, "iter_bundles"):
        source_spool = None
    records = _kwargs.get("records")
    if source_spool is not None or not isinstance(records, list):
        records = None
    return {
        "records": records,
        "source_spool": source_spool,
        "source_spill_max": resolve_source_spill_max(extra),
        "materialize_batch": resolve_sql_materialize_batch(extra),
    }


def sample_sql_source_values(
    headers: list[str],
    data_rows: list[list[Any]] | None,
    mappings: list[dict[str, Any]],
    *,
    records: list[dict[str, Any]] | None = None,
    limit: int = 200,
) -> dict[str, list[str]]:
    """DDL samples from the unexpanded engine chunk — never the STRUCT explode."""
    from connectors.writer_common import sample_values_by_source_from_batch

    if data_rows:
        return sample_values_by_source_from_batch(
            headers, data_rows, mappings, limit=limit
        )
    if records:
        rows = [matrix_row_from_record(rec, headers) for rec in records[:limit]]
        return sample_values_by_source_from_batch(headers, rows, mappings, limit=limit)
    return {}


def ingest_sql_source_spool(
    *,
    headers: list[str],
    data_rows: list[list[Any]] | None = None,
    records: Iterable[dict[str, Any]] | None = None,
    mappings: list[dict[str, Any]] | None = None,
    extra: dict[str, Any] | None = None,
    spill_max: int | None = None,
) -> SourceRowSpool:
    """Stream STRUCT flatten/explode onto the shared JSONL spool."""
    spool = SourceRowSpool(
        spill_max_size=int(spill_max or resolve_source_spill_max(extra))
    )
    if records is not None:
        spool.ingest_records(headers, records, mappings)
    else:
        spool.ingest_matrix(headers, data_rows or [], mappings)
    return spool


@dataclass
class SqlMappedRows:
    """Mapped tuples plus the expanded source count (explode-aware)."""

    mapped_rows: list[tuple]
    transform_errors: list[str]
    rejected_details: list[dict[str, Any]]
    source_row_count: int
    headers: list[str]
    batch_sizes: list[int] = field(default_factory=list)


@dataclass
class SqlMappedBundle:
    """One mapped source bundle. Caller finishes, writes, then drops it."""

    start: int
    mapped_rows: list[tuple]
    transform_errors: list[str]
    rejected_details: list[dict[str, Any]]
    accepted_source_rows: list[int]
    headers: list[str]
    source_row_count: int


@dataclass
class FinishedSqlBundle:
    """Quarantined, in-bundle-deduped image ready for COPY/INSERT/MERGE."""

    start: int
    dense_rows: list[tuple]
    sparse_rows: list[tuple]
    dense_row_numbers: list[int]
    sparse_row_numbers: list[int]
    checksum_rows: list[tuple]
    rejected_details: list[dict[str, Any]]
    transform_errors: list[str]
    source_row_count: int
    target_types: list[str] = field(default_factory=list)
    bind_types: list[str] = field(default_factory=list)


class SqlWriteAccumulator:
    """Checksum + Gate-8 sample + reject details without the accepted-row list.

    Mirrors ``object_store_materialize``: ``FingerprintAccumulator`` on the
    accepted post-bind image, first 50 sample rows, and a ``writing`` flag so
    fail/FAIL_JOB can finish scanning rejects after the primary write is
    refused.
    """

    def __init__(
        self,
        *,
        target_cols: list[str],
        dest_db_type: str = "",
        dest_types: dict[str, str] | None = None,
        dialect_label: str = "SQL",
    ) -> None:
        from services.fingerprint_accumulator import FingerprintAccumulator

        self.target_cols = list(target_cols)
        self.dest_db_type = dest_db_type
        self.dest_types = dest_types or {}
        self.dialect_label = dialect_label
        self.acc = FingerprintAccumulator()
        self.sample_rows: list[tuple] = []
        self.rejected_details: list[dict[str, Any]] = []
        self.transform_errors: list[str] = []
        self.writing = True
        self.accepted_row_count = 0
        self.batch_sizes: list[int] = []

    def note_rejects(
        self,
        details: list[dict[str, Any]] | None,
        errors: list[str] | None = None,
    ) -> None:
        if details:
            self.rejected_details.extend(details)
        if errors:
            self.transform_errors.extend(errors)

    def add_accepted(
        self,
        rows: list[tuple] | None,
        *,
        dest_types: dict[str, str] | None = None,
    ) -> None:
        if not self.writing or not rows:
            return
        from services.reconciliation import _iter_fingerprints

        types = dest_types if dest_types is not None else self.dest_types
        self.acc.add_many(
            _iter_fingerprints(
                rows,
                self.target_cols,
                dest_db_type=self.dest_db_type,
                dest_types=types,
            )
        )
        need = _SAMPLE_LIMIT - len(self.sample_rows)
        if need > 0:
            self.sample_rows.extend(rows[:need])
        self.accepted_row_count += len(rows)
        self.batch_sizes.append(len(rows))

    def stop_writing(self) -> None:
        self.writing = False

    def digest(self) -> str:
        if self.accepted_row_count <= 0:
            return ""
        return self.acc.digest()

    def gate8_meta(
        self,
        *,
        conflict_columns: list[str] | None = None,
    ) -> dict[str, Any]:
        from connectors.writer_common import gate8_writer_meta

        meta = gate8_writer_meta(
            self.sample_rows,
            self.target_cols,
            conflict_columns=conflict_columns or None,
        )
        meta["source_row_count"] = self.accepted_row_count
        return meta

    def abort_error(
        self,
        policy: str | None,
        extra_errors: list[str] | None = None,
    ) -> str | None:
        from connectors.writer_common import reject_on_strict_policy

        return reject_on_strict_policy(
            policy,
            self.rejected_details,
            self.dialect_label,
            extra_errors if extra_errors is not None else self.transform_errors,
        )


def ensure_sql_source_spool(
    *,
    headers: list[str],
    data_rows: list[list[Any]] | None = None,
    records: list[dict[str, Any]] | None = None,
    mappings: list[dict[str, Any]] | None = None,
    extra: dict[str, Any] | None = None,
    source_spool: Any = None,
    spill_max: int | None = None,
) -> tuple[SourceRowSpool, bool]:
    """Return ``(spool, close_when_done)``. Engine-owned spools are not closed."""
    if source_spool is not None and hasattr(source_spool, "iter_bundles"):
        return source_spool, False
    return (
        ingest_sql_source_spool(
            headers=headers,
            data_rows=data_rows,
            records=records,
            mappings=mappings,
            extra=extra,
            spill_max=spill_max,
        ),
        True,
    )


def dest_types_signature(dest_types: dict[str, str] | None, target_cols: list[str]) -> tuple[str, ...]:
    """Compare Map vs live carriers without holding the mapped image."""
    dest_types = dest_types or {}
    return tuple(
        f"{c}={str(dest_types.get(c) or '').strip().upper()}" for c in target_cols
    )


def iter_mapped_bundles_from_source(
    *,
    headers: list[str],
    data_rows: list[list[Any]] | None = None,
    mappings: list[dict],
    target_cols: list[str],
    column_types: dict[str, str] | None = None,
    error_policy: str | None = None,
    dest_types: dict[str, str] | None = None,
    preserve_case: bool = False,
    allow_job_coerce_null: bool | None = None,
    dest_kind: str = "",
    destination_pk_columns: list[str] | None = None,
    contract_primary_key: str | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
    destination_column_nullability: dict[str, bool] | None = None,
    empty_cells_as_null: bool = False,
    records: list[dict[str, Any]] | None = None,
    source_spool: SourceRowSpool | None = None,
    extra: dict[str, Any] | None = None,
    batch_size: int | None = None,
) -> Iterator[SqlMappedBundle]:
    """Yield one mapped bundle at a time. Accepted tuples are not concatenated.

    Callers that already ingested a spool (write + live-DDL rematerialize)
    pass ``source_spool`` so explode is not replayed onto a second file.
    """
    from connectors.writer_common import build_mapped_rows_with_details

    spool, close_spool = ensure_sql_source_spool(
        headers=headers,
        data_rows=data_rows,
        records=records,
        mappings=mappings,
        extra=extra,
        source_spool=source_spool,
    )
    bundle_n = resolve_sql_materialize_batch(extra) if batch_size is None else max(1, int(batch_size))
    try:
        headers_out = list(spool.headers or headers)
        source_row_count = int(spool.row_count)
        for start, chunk in spool.iter_bundles(bundle_n):
            accepted_nums: list[int] = []
            part, part_errors, part_details = build_mapped_rows_with_details(
                headers=headers_out,
                data_rows=chunk,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                dest_types=dest_types,
                error_policy=error_policy,
                preserve_case=preserve_case,
                allow_job_coerce_null=allow_job_coerce_null,
                dest_kind=dest_kind,
                destination_pk_columns=destination_pk_columns,
                contract_primary_key=contract_primary_key,
                stream_contracts=stream_contracts,
                destination_column_nullability=destination_column_nullability,
                empty_cells_as_null=empty_cells_as_null,
                row_number_start=start,
                accepted_source_rows=accepted_nums,
                struct_already_materialized=True,
            )
            yield SqlMappedBundle(
                start=int(start),
                mapped_rows=part,
                transform_errors=list(part_errors or []),
                rejected_details=list(part_details or []),
                accepted_source_rows=accepted_nums,
                headers=headers_out,
                source_row_count=source_row_count,
            )
            del part
    finally:
        if close_spool:
            spool.close()


def build_mapped_rows_from_source(
    *,
    headers: list[str],
    data_rows: list[list[Any]] | None = None,
    mappings: list[dict],
    target_cols: list[str],
    column_types: dict[str, str] | None = None,
    error_policy: str | None = None,
    dest_types: dict[str, str] | None = None,
    preserve_case: bool = False,
    allow_job_coerce_null: bool | None = None,
    dest_kind: str = "",
    destination_pk_columns: list[str] | None = None,
    contract_primary_key: str | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
    destination_column_nullability: dict[str, bool] | None = None,
    empty_cells_as_null: bool = False,
    records: list[dict[str, Any]] | None = None,
    source_spool: SourceRowSpool | None = None,
    extra: dict[str, Any] | None = None,
    batch_size: int | None = None,
    accepted_source_rows: list[int] | None = None,
) -> SqlMappedRows:
    """Same contract as ``build_mapped_rows_with_details`` with streaming STRUCT.

    Concatenates bundles for callers that still need the full mapped image
    (unit tests, writers not yet on the write-as-we-go loop). Peak RAM for
    those callers is still the explode; prefer ``iter_mapped_bundles_from_source``.
    """
    mapped: list[tuple] = []
    errors: list[str] = []
    details: list[dict[str, Any]] = []
    batch_sizes: list[int] = []
    headers_out = list(headers)
    source_row_count = 0
    for bundle in iter_mapped_bundles_from_source(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        error_policy=error_policy,
        dest_types=dest_types,
        preserve_case=preserve_case,
        allow_job_coerce_null=allow_job_coerce_null,
        dest_kind=dest_kind,
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
        stream_contracts=stream_contracts,
        destination_column_nullability=destination_column_nullability,
        empty_cells_as_null=empty_cells_as_null,
        records=records,
        source_spool=source_spool,
        extra=extra,
        batch_size=batch_size,
    ):
        mapped.extend(bundle.mapped_rows)
        errors.extend(bundle.transform_errors)
        details.extend(bundle.rejected_details)
        batch_sizes.append(len(bundle.mapped_rows))
        if accepted_source_rows is not None:
            accepted_source_rows.extend(bundle.accepted_source_rows)
        headers_out = bundle.headers
        source_row_count = bundle.source_row_count
        del bundle
    return SqlMappedRows(
        mapped_rows=mapped,
        transform_errors=errors,
        rejected_details=details,
        source_row_count=source_row_count,
        headers=headers_out,
        batch_sizes=batch_sizes,
    )


def finish_sql_mapped_bundle(
    bundle: SqlMappedBundle,
    *,
    target_cols: list[str],
    target_types: list[str],
    policy: Any,
    dialect_label: str,
    dest_db: str = "",
    mappings: list[dict[str, Any]] | None = None,
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
) -> FinishedSqlBundle:
    """Shared quarantine + in-bundle last-write-wins. No cross-bundle seen-set.

    Duplicate PKs inside this bundle keep the last arrival (or highest LSN).
    The same PK in a later bundle is a later ON CONFLICT / dest-LSN apply —
    at-least-once upsert, not a silent drop.
    """
    from connectors.lsn_guards import DF_LSN_COL, dedupe_rows_by_pk_and_lsn_keeping_numbers
    from connectors.writer_common import (
        apply_write_quarantine_matrix_keeping_numbers,
        combined_mapped_rows_for_checksum,
        dedupe_rows_keeping_numbers,
        split_dense_sparse_rows_with_numbers,
    )

    details = list(bundle.rejected_details)
    errors = list(bundle.transform_errors)
    source_nums = bundle.accepted_source_rows or None
    mapped, nums = apply_write_quarantine_matrix_keeping_numbers(
        bundle.mapped_rows,
        target_cols,
        target_types,
        details,
        policy,
        dialect_label=dialect_label,
        mappings=mappings,
        dest_db=dest_db,
        source_row_numbers=source_nums,
    )
    sparse: list[tuple] = []
    sparse_nums: list[int] = []
    if write_mode == "upsert" and conflict_columns:
        mapped, sparse, nums, sparse_nums = split_dense_sparse_rows_with_numbers(
            mapped, source_row_numbers=nums
        )
        if DF_LSN_COL in target_cols:
            mapped, nums = dedupe_rows_by_pk_and_lsn_keeping_numbers(
                mapped, conflict_columns, target_cols, nums
            )
        else:
            mapped, nums = dedupe_rows_keeping_numbers(
                mapped, conflict_columns, target_cols, nums
            )
    return FinishedSqlBundle(
        start=bundle.start,
        dense_rows=mapped,
        sparse_rows=sparse,
        dense_row_numbers=list(nums or []),
        sparse_row_numbers=list(sparse_nums or []),
        checksum_rows=combined_mapped_rows_for_checksum(mapped, sparse),
        rejected_details=details,
        transform_errors=errors,
        source_row_count=bundle.source_row_count,
        target_types=list(target_types),
    )


def iter_finished_sql_bundles(
    *,
    finish: Callable[[SqlMappedBundle], FinishedSqlBundle],
    **map_kwargs: Any,
) -> Iterator[FinishedSqlBundle]:
    """Map → finish → yield. Caller writes and drops. One algorithm, every dialect."""
    for bundle in iter_mapped_bundles_from_source(**map_kwargs):
        finished = finish(bundle)
        del bundle
        yield finished
