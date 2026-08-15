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
2. Optionally collect mirror PK tuples while iterating (keys only — not
   full rows). Fivetran-class inferred-delete needs the key set, not the
   payload.
3. Caller clears the ``records`` list so peak RAM after ingest is the
   spool + one writer bundle, not the dict chunk.
4. Writers receive ``source_spool`` and must not re-ingest (no second
   explode-to-disk).
5. Post-write Gate-8 remaps from the spool in bundles
   (``FingerprintAccumulator``). Mirror uses the key set.

Honesty: SQL/warehouse writers still hold the mapped image until
COPY/INSERT/MERGE returns. SCD2 is a separate path and still uses the
dict list. This is not a source-file stream and not exactly-once. CDC
default remains at-least-once upsert. Catalog tiles ≠ transfer-live.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from connectors.source_row_spool import SourceRowSpool
from connectors.sql_write_materialize import (
    SQL_SPOOL_WRITE_KINDS,
    ingest_sql_source_spool,
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
    """Map destination PK columns back to source names (same as mirror_engine)."""
    pk_sources: list[str] = []
    for pk_target in conflict_columns:
        if not pk_target:
            continue
        pk_source = pk_target
        for m in mappings or []:
            if (m.get("target") or m.get("source")) == pk_target:
                src = m.get("source")
                if src:
                    pk_source = str(src)
                    break
        pk_sources.append(str(pk_source))
    return pk_sources


@dataclass
class EngineRecordSpill:
    """Live spool plus optional mirror keys. Close after Gate-8 / mirror."""

    spool: SourceRowSpool
    unexpanded_row_count: int
    mirror_pk_tuples: list[tuple[Any, ...]] | None = None
    _closed: bool = field(default=False, init=False, repr=False)

    @property
    def spilled(self) -> bool:
        return bool(self.spool.spilled)

    @property
    def source_row_count(self) -> int:
        return int(self.spool.row_count)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.spool.close()


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
    they opt in. The spool is the durable source for write + remap.
    """
    unexpanded = len(records)
    mirror_keys = None
    if collect_pk_sources:
        mirror_keys = collect_mirror_pk_tuples(records, collect_pk_sources)
    spool = ingest_sql_source_spool(
        headers=columns,
        records=records,
        mappings=mappings,
        extra=extra,
    )
    if clear_records:
        records.clear()
    return EngineRecordSpill(
        spool=spool,
        unexpanded_row_count=unexpanded,
        mirror_pk_tuples=mirror_keys,
    )
