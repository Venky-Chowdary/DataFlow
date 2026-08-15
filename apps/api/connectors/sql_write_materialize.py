"""Disk-backed source-row spool for SQL / warehouse writers.

Object-store writers already stream STRUCT flatten/explode through
``SourceRowSpool``. PostgreSQL, Redshift, MySQL, Snowflake, BigQuery,
SQLite, and generic SQL still called ``materialize_struct_policies``
(the list form) before COPY / INSERT / MERGE. A 20k × 256 explode became
a 5.1M-row Python list before the first bind.

This module is the single algorithm:

1. Ingest engine ``records`` (preferred) or the unexpanded matrix through
   ``SourceRowSpool``. STRUCT flatten/explode is streaming — the expanded
   source matrix is never a Python list.
2. Map each bundle with ``struct_already_materialized=True`` and a 1-based
   global ``row_number_start`` (spool starts are already 1-based — do not
   add 1 again).
3. Dialect writers run their quarantine / bind / PK+LSN dedupe on the
   mapped image they still need for COPY/INSERT. Peak *source* RAM after
   ingest is one bundle, not the explode.

Honesty: the mapped image for a warehouse write is still held until the
write returns (COPY/INSERT/MERGE need the tuples; dest LSN / ON CONFLICT
apply on the written batch). Engine ``records`` are spilled before this
module runs when the adapter passed ``source_spool``. This is not a
source-file stream and not exactly-once. CDC default remains at-least-once
upsert. Catalog tiles ≠ transfer-live. Fail/FAIL_JOB still collect every
reject in the engine batch before the writer refuses the primary write.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from connectors.source_row_spool import (
    SourceRowSpool,
    matrix_row_from_record,
    resolve_source_spill_max,
)
from services.brand_env import getenv_brand

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
    records: list[dict[str, Any]] | None = None,
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

    Callers that already ingested a spool (write + live-DDL rematerialize)
    pass ``source_spool`` so explode is not replayed onto a second file.
    """
    from connectors.writer_common import build_mapped_rows_with_details

    close_spool = source_spool is None
    spool = source_spool or ingest_sql_source_spool(
        headers=headers,
        data_rows=data_rows,
        records=records,
        mappings=mappings,
        extra=extra,
    )
    bundle_n = resolve_sql_materialize_batch(extra) if batch_size is None else max(1, int(batch_size))
    mapped: list[tuple] = []
    errors: list[str] = []
    details: list[dict[str, Any]] = []
    batch_sizes: list[int] = []
    try:
        headers_out = list(spool.headers or headers)
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
            mapped.extend(part)
            errors.extend(part_errors)
            details.extend(part_details)
            batch_sizes.append(len(part))
            if accepted_source_rows is not None:
                accepted_source_rows.extend(accepted_nums)
            del part
        return SqlMappedRows(
            mapped_rows=mapped,
            transform_errors=errors,
            rejected_details=details,
            source_row_count=int(spool.row_count),
            headers=headers_out,
            batch_sizes=batch_sizes,
        )
    finally:
        if close_spool:
            spool.close()
