"""Shared state of one ``_stream_database_transfer_impl`` run.

The streaming loop is split into phase functions (``stream_db_*``) that used to
be closures over the streaming function's locals. The locals they shared now live
on these dataclasses, grouped by lifetime, so each phase takes ``run`` explicitly
instead of capturing it. A field holds ``None`` until the streaming function
binds it as the run progresses.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunConfig:
    """Resolved plan of one database->database run: endpoints, mapping, schema, cursor plan."""

    source: Any = None
    destination: Any = None
    job_id: Any = None
    limit: Any = None
    chunk_size: Any = None
    phase_profile: Any = None
    src_type: Any = None
    src_read_type: Any = None
    src_cfg: Any = None
    src_db: Any = None
    src_scan: Any = None
    sdk_source: Any = None
    initial_sdk_state: Any = None
    table: Any = None
    incremental: Any = None
    schema: Any = None
    columns: Any = None
    column_types: Any = None
    cursor_source_col: Any = None
    cursor_pk_source: Any = None
    watermark: Any = None
    keyset_col: Any = None
    keyset_order_cols: Any = None
    keyset_tiebreak: Any = None
    use_keyset: Any = None
    total_rows: Any = None
    bulk_export_iter: Any = None
    _scan_kw: Any = None
    _filtered_scan_reason: Any = None
    dest_cfg: Any = None
    dest_type: Any = None
    dest_table: Any = None
    dest_bind_dialect: Any = None
    effective_sync: Any = None
    mappings: Any = None
    target_cols: Any = None
    pk_target_cols: Any = None
    pk_source_cols: Any = None
    pk_audit_cols: Any = None
    digest_dest_types: Any = None
    stream_error_policy: Any = None
    client_cursor_bound: Any = None
    fk_orphan_guard: Any = None
    pin_ledger: Any = None
    shape_inputs: Any = None
    shape_runner: Any = None
    source_filter: Any = None
    validation_mode: Any = None
    _tie_dedupe: Any = None


@dataclass
class ReadCursors:
    """Paging position: where the next source page starts and what the committed prefix is."""

    fetch_cursor: Any = None
    fetch_from_seed: Any = None
    fetch_offset: Any = None
    keyset_after: Any = None
    keyset_run_seed: Any = None
    kafka_cursor: Any = None
    ddb_cursor: Any = None
    ddb_total: Any = None
    es_search_after: Any = None
    qdrant_offset: Any = None
    redis_scan_state: Any = None
    _boundary_fps_state: Any = None


@dataclass
class RunTally:
    """Running totals, accumulators and held connections of the write side."""

    shape_divert_key_tuples: Any = None
    shape_diverted_total: Any = None


@dataclass
class DbRunContext:
    """Everything the phases of one run share."""

    cfg: RunConfig = field(default_factory=RunConfig)
    cur: ReadCursors = field(default_factory=ReadCursors)
    tally: RunTally = field(default_factory=RunTally)
