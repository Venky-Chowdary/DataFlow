"""Engine-owned source-row spill for the buffered write path.

The engine used to hold the current chunk's ``records`` (``list[dict]``)
until ``write_mapped_rows`` returned — and then again for mirror key
collection and Gate-8 remap. Writer-side ``SourceRowSpool`` already
removed the *second* matrix / STRUCT-explode copy. This module removes
the engine dict list after the last pre-write consumer
(``prepare_keyed_upsert``) has finished.

Algorithm (Spark external spill / Beam bundle):

1. Ingest one record at a time through ``SourceRowSpool`` (streaming
   STRUCT flatten/explode; ``DF_MISSING`` survives JSONL).
2. When mirror keys are requested, write complete PK tuples onto a
   keys-only spool in the *same* pass — raw values, unexpanded. Incomplete
   identity is skip, not invent. EXISTS on dest staging is set-based, so
   this spool does not hold a Python unique-key set.
3. Caller clears the ``records`` list so peak RAM after ingest is the
   payload spool + keys-only spool + one writer bundle, not the dict chunk.
4. Writers receive ``source_spool`` and must not re-ingest (no second
   explode-to-disk).
5. Post-write Gate-8 remaps from the payload spool in bundles
   (``FingerprintAccumulator``). Mirror streams the keys-only spool into
   dest staging.

Honesty: SQL/warehouse writers iterate finished bundles (peak mapped RAM
is one bundle). File-stream spool destinations reuse this spill so the
chunk never becomes a second ``records_to_matrix`` copy. SCD2 history
merge uses the same payload ``SourceRowSpool``. File-stream is disabled
for mirror/SCD2 so inferred-delete always sees the full snapshot. This is
not exactly-once. CDC default remains at-least-once upsert. Catalog tiles
≠ transfer-live.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from connectors.source_row_spool import SourceRowSpool, resolve_source_spill_max
from connectors.sql_write_materialize import (
    SQL_SPOOL_WRITE_KINDS,
    ingest_sql_source_spool,
)
from services.mirror_engine import (
    complete_mirror_pk_tuple,
    iter_mirror_pk_tuples_from_spool,
    unique_mirror_pk_tuples,
)

# dest_summary handoff — engine MUST pop before persisting the summary.
ENGINE_SPILL_SUMMARY_KEY = "_df_engine_record_spill"
MIRROR_PK_SUMMARY_KEY = "mirror_source_pk_tuples"


def spool_write_kinds() -> frozenset[str]:
    from connectors.source_row_spool import OBJECT_STORE_WRITE_KINDS

    return OBJECT_STORE_WRITE_KINDS | SQL_SPOOL_WRITE_KINDS


def collect_mirror_pk_tuples(
    records: list[dict[str, Any]], pk_sources: list[str]
) -> list[tuple[Any, ...]]:
    """Unique complete PK tuples. Incomplete identity is skipped, not invented."""
    from services.mirror_engine import _source_pk_tuples

    return _source_pk_tuples(records, pk_sources)


def mirror_pk_sources(
    conflict_columns: list[str], mappings: list[dict[str, Any]] | None
) -> list[str]:
    """Map destination PK columns back to source names (canonical: mirror_engine)."""
    from services.mirror_engine import mirror_pk_sources as _impl

    return _impl(conflict_columns, mappings)


def _iter_records_collecting_keys(
    records: list[dict[str, Any]],
    pk_sources: list[str],
    key_spool: SourceRowSpool,
) -> Iterator[dict[str, Any]]:
    """One pass: append complete unexpanded keys, then yield the record."""
    for rec in records:
        if isinstance(rec, dict):
            tup = complete_mirror_pk_tuple(rec.get(c) for c in pk_sources)
            if tup is not None:
                key_spool.append_row(list(tup))
        yield rec


@dataclass
class EngineRecordSpill:
    """Live payload spool plus optional keys-only mirror census. Close after Gate-8."""

    spool: SourceRowSpool
    unexpanded_row_count: int
    pk_sources: list[str] | None = None
    key_spool: SourceRowSpool | None = None
    _closed: bool = field(default=False, init=False, repr=False)

    @property
    def spilled(self) -> bool:
        return bool(self.spool.spilled or (self.key_spool is not None and self.key_spool.spilled))

    @property
    def source_row_count(self) -> int:
        return int(self.spool.row_count)

    @property
    def mirror_pk_tuples(self) -> list[tuple[Any, ...]] | None:
        """Unique keys for tests and small fixtures. Apply must stream, not call this."""
        if self.pk_sources is None:
            return None
        if self.key_spool is not None:
            return unique_mirror_pk_tuples(
                iter_mirror_pk_tuples_from_spool(self.key_spool, self.pk_sources)
            )
        return unique_mirror_pk_tuples(
            iter_mirror_pk_tuples_from_spool(self.spool, self.pk_sources)
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.spool.close()
        if self.key_spool is not None:
            self.key_spool.close()


def spill_engine_write_records(
    records: list[dict[str, Any]],
    columns: list[str],
    mappings: list[dict[str, Any]] | None,
    *,
    extra: dict[str, Any] | None = None,
    collect_pk_sources: list[str] | None = None,
    clear_records: bool = True,
) -> EngineRecordSpill:
    """Ingest ``records`` onto the shared spool.

    ``clear_records`` is the engine-level contract: every alias of this list
    drops the dict payload. Direct adapter callers keep their list unless
    they opt in. The spool is the durable source for write + remap. Mirror
    keys are written in this same pass — the dict list is not walked first.
    """
    unexpanded = len(records)
    key_spool = None
    ingest_records: Any = records
    if collect_pk_sources:
        key_spool = SourceRowSpool(spill_max_size=resolve_source_spill_max(extra))
        key_spool.headers = list(collect_pk_sources)
        ingest_records = _iter_records_collecting_keys(
            records, collect_pk_sources, key_spool
        )
    try:
        spool = ingest_sql_source_spool(
            headers=columns,
            records=ingest_records,
            mappings=mappings,
            extra=extra,
        )
    except Exception:
        if key_spool is not None:
            key_spool.close()
        raise
    if clear_records:
        records.clear()
    return EngineRecordSpill(
        spool=spool,
        unexpanded_row_count=unexpanded,
        pk_sources=list(collect_pk_sources) if collect_pk_sources else None,
        key_spool=key_spool,
    )


def iter_fingerprints_from_spool(
    source_spool: Any,
    mappings: list[dict[str, Any]],
    target_cols: list[str],
    *,
    headers: list[str] | None = None,
    column_types: dict[str, str] | None = None,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
    error_policy: str | None = None,
    destination_pk_columns: list[str] | None = None,
    empty_cells_as_null: bool = False,
    batch_size: int | None = None,
) -> Iterator[tuple[str, str]]:
    """Remap Gate-8 / file-stream fingerprints from the spool in bundles.

    Same algorithm as the engine remap: ``struct_already_materialized=True``,
    1-based spool starts (do not add 1 again). Peak mapped RAM is one bundle.
    Callers that need a digest add these tuples to ``FingerprintAccumulator``.
    """
    from connectors.sql_write_materialize import resolve_sql_materialize_batch
    from connectors.writer_common import map_rows_for_fingerprint, row_fingerprints

    headers = list(
        headers
        or getattr(source_spool, "headers", None)
        or []
    )
    bundle_n = (
        max(1, int(batch_size))
        if batch_size is not None
        else resolve_sql_materialize_batch(None)
    )
    for start, chunk in source_spool.iter_bundles(bundle_n):
        mapped, _rejected = map_rows_for_fingerprint(
            headers=headers,
            data_rows=chunk,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types or {},
            error_policy=error_policy,
            dest_types=dest_types or {},
            preserve_case=True,
            dest_kind=dest_db_type or "",
            destination_pk_columns=destination_pk_columns,
            empty_cells_as_null=empty_cells_as_null,
            row_number_start=start,
            struct_already_materialized=True,
        )
        yield from row_fingerprints(
            mapped,
            target_cols,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
        )
        del mapped


def fingerprints_from_spool(
    source_spool: Any,
    mappings: list[dict[str, Any]],
    target_cols: list[str],
    **kwargs: Any,
) -> list[tuple[str, str]]:
    """Collect spool fingerprints for a single file-stream chunk (worker-local)."""
    return list(iter_fingerprints_from_spool(source_spool, mappings, target_cols, **kwargs))
