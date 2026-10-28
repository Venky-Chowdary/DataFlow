"""Streaming DB→DB transfer — batched read/write without loading full dataset into RAM."""

from __future__ import annotations

import dataclasses
import logging
import os
from services.brand_env import getenv_brand
import sys
import threading
import time
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

_stream = sys.modules[__name__]

from .models import EndpointConfig
from .type_mapper import ddl_carrier_type, ddl_type, normalize_inferred

_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from connectors.writer_common import (  # noqa: E402
    CHUNK_SIZE,
    map_rows_for_fingerprint,
    resolve_target_columns,
    row_fingerprints,
    transform_error_policy_for_validation_mode,
)
from services.keyset_pagination import header_column_index  # noqa: E402

# Keep resilient batch/quarantine path importable for streaming callers.
from services.bounded_collections import BoundedStrings
from services.dest_precount import (
    PRECOUNT_KEY,
    begin_overwrite_source_keys,
    matrix_to_key_tuples,
    precount_table,
    stamp_overwrite_source_keys,
)
from services.dialect_profiles import schema_from_cfg
from services.source_reread import (
    REREAD_SCAN_SOURCES,
    align_source_populations,
    alignment_when_write_pass_empty,
    reread_pagination_plan,
    should_reread_source,
)
from services.primary_key import dest_is_key_addressed
from services.row_conservation import (
    CENSUS_KEY,
    KEY_ADDRESSED_KEY,
    KeyCensusAccumulator,
    live_rows_for_digest,
    observe_keyed_batch,
    record_tombstone_digest_scope,
)
from services.resilience import (  # noqa: E402, F401
    ResilientBatcher,
    adaptive_chunk_size,
)

from .adapters import (
    WriteBatchBlocked,
    _introspect_table_schema,
    _introspect_table_schema_rich,
    resolve_connector_config,
    resolve_dest_table,
)
from .connector_capabilities import resolve_driver_type, source_read_driver
from .job_quarantine import align_reported_rejects
from .stream_row_accounting import (
    _mark_raw_page,
    _raw_page_cursor,
    _raw_page_cursor_bounded,
    _raw_page_filtered,
    _raw_page_keyset,
    _raw_page_marked,
    _raw_page_rows,
    stamp_incremental_no_op,
    reader_population,
    stamp_source_row_count,
)
from .stream_foreign_keys import (
    carry_single_table_foreign_keys as _carry_single_table_foreign_keys,
)

from services.phase_profile import (  # noqa: E402
    PHASE_BULK_COPY,
    PHASE_CHECKSUM,
    PHASE_READ,
    PHASE_TRANSFORM_WRITE,
    PhaseProfile,
)
from services.connector_telemetry import log_retry

logger = logging.getLogger(__name__)


def _stream_source_read_driver(source_format: str) -> str:
    return source_read_driver(resolve_driver_type(source_format))


#: Distinct warning messages retained per transfer. Only the first 10 are shown,
#: but keeping a few more lets the summary report an honest suppressed count.
MAX_WARNING_SAMPLES = 200
#: Distinct load methods observed across batches (copy_into, merge_batch, ...).
#: There are only a handful in the whole product, so this is generous.
MAX_LOAD_METHODS = 16

try:
    from services.checkpoint_service import Checkpoint, CheckpointService
    from services.data_quality import BatchDriftDetector, run_integrity_audit
    from services.error_handling import RetryBudget, with_retry
    from services.parallel_chunks import drive_ordered_batches
    from services.reconciliation import FingerprintAccumulator
    from services.replay_safety import classify_replay_safety
    from services.resilience import adaptive_chunk_size
    from services.row_filter import apply_row_filter_to_matrix
    from services.shape_apply import ShapeRunner
except ImportError:  # pragma: no cover - tests with api root on path
    from src.services.checkpoint_service import Checkpoint, CheckpointService
    from src.services.data_quality import BatchDriftDetector, run_integrity_audit
    from src.services.error_handling import RetryBudget, with_retry
    from src.services.parallel_chunks import drive_ordered_batches
    from src.services.reconciliation import FingerprintAccumulator
    from src.services.replay_safety import classify_replay_safety
    from src.services.resilience import adaptive_chunk_size
    from src.services.row_filter import apply_row_filter_to_matrix
    from src.services.shape_apply import ShapeRunner


def _merge_embedding_usage(previous: Any, incoming: Any) -> dict[str, Any] | None:
    if not isinstance(incoming, dict):
        return None
    if not isinstance(previous, dict) or not previous:
        return dict(incoming)
    return {
        "provider": incoming.get("provider") or previous.get("provider"),
        "model": incoming.get("model") or previous.get("model"),
        "calls": int(previous.get("calls") or 0) + int(incoming.get("calls") or 0),
        "retries": int(previous.get("retries") or 0)
        + int(incoming.get("retries") or 0),
        "input_tokens": int(previous.get("input_tokens") or 0)
        + int(incoming.get("input_tokens") or 0),
        "token_count_estimated": bool(
            previous.get("token_count_estimated")
            or incoming.get("token_count_estimated")
        ),
        "cache_hits": int(previous.get("cache_hits") or 0)
        + int(incoming.get("cache_hits") or 0),
        "cache_misses": int(previous.get("cache_misses") or 0)
        + int(incoming.get("cache_misses") or 0),
        "estimated_cost_usd": (
            None
            if previous.get("estimated_cost_usd") is None
            or incoming.get("estimated_cost_usd") is None
            else float(previous.get("estimated_cost_usd") or 0)
            + float(incoming.get("estimated_cost_usd") or 0)
        ),
    }


_STREAMING_TYPES = frozenset({
    "postgresql", "mysql", "mongodb", "snowflake", "bigquery", "redshift",
    "sqlserver", "oracle",
    "s3", "gcs", "adls", "sftp", "dynamodb", "elasticsearch", "redis", "sqlite", "generic_sql",
    "iceberg", "kafka", "qdrant",
    # Salesforce and HubSpot paginate with opaque cursors / capped OFFSET — do not
    # claim resumable numeric-offset streaming until continuation state is wired.
})
# Neo4j is a source (HTTP Cypher, offset by elementId). It is not a destination.
_STREAMING_SOURCES = _STREAMING_TYPES | frozenset({"neo4j"})
# Vector engines already have a batch writer. They are destinations only —
# adding them as sources would offer a read the catalog does not implement.
_STREAMING_DESTINATIONS = _STREAMING_TYPES | frozenset({
    "pgvector", "weaviate", "pinecone", "milvus",
})


def _source_name(source: EndpointConfig) -> str:
    fmt = resolve_driver_type(source.format or "")
    if fmt == "mongodb":
        return source.collection or source.table or ""
    if fmt == "dynamodb":
        return source.table or source.collection or source.database or ""
    if fmt == "elasticsearch":
        return source.table or source.database or source.collection or ""
    if fmt == "s3":
        return source.table or source.collection or source.schema or ""
    if fmt == "gcs":
        return source.table or source.collection or source.schema or ""
    if fmt == "adls":
        return source.table or source.collection or source.schema or ""
    if fmt == "redis":
        return source.table or source.collection or source.schema or "*"
    if fmt == "qdrant":
        return source.table or source.collection or source.database or ""
    return source.table or source.collection or ""


# Per-source read dispatch lives in ``batch_readers``; kept importable here for
# the historical ``stream._read_batch_impl`` surface.
from .batch_readers import _read_batch_impl  # noqa: E402


def _read_batch(*args, **kwargs):
    """Retry transient source-read errors without leaking partial state."""
    budget = RetryBudget(
        max_attempts=3,
        base_delay_seconds=0.5,
        max_delay_seconds=5.0,
        exponential_base=2.0,
        jitter=True,
    )
    source_type = args[0] if args else kwargs.get("src_type", "")
    return with_retry(
        lambda: _read_batch_impl(*args, **kwargs),
        budget=budget,
        on_transient=log_retry(
            "read_batch",
            driver_family=str(source_type or ""),
            budget=budget,
        ),
    )


def _unwrap_read(result):
    """Normalize _read_batch return — dynamodb returns (batch, cursor)."""
    if isinstance(result, tuple) and len(result) == 2 and hasattr(result[0], "headers"):
        return result
    return result, None


from .stream_catalog import (  # noqa: E402
    declared_destination_key_columns as _declared_destination_key_columns,
)


def _merge_chunk_write_summaries(
    merged: dict[str, Any], incoming: dict[str, Any]
) -> dict[str, Any]:
    """Merge per-route write summaries into one batch ledger.

    Row counts sum; rejected-details and warnings concatenate (each detail
    already names its route's table); anything else takes the latest write's
    value.
    """
    incoming = dict(incoming or {})
    if not merged:
        return incoming
    for key in ("rejected_rows", "coerced_null_rows", "orphan_fk_rows", "rows_expanded"):
        merged[key] = int(merged.get(key) or 0) + int(incoming.get(key) or 0)
    for key in ("rejected_details", "warnings"):
        merged[key] = list(merged.get(key) or []) + list(incoming.get(key) or [])
    for key, value in incoming.items():
        if key in ("rejected_rows", "coerced_null_rows", "orphan_fk_rows", "rows_expanded", "rejected_details", "warnings"):
            continue
        merged[key] = value
    return merged


def _arm_strict_keyed_backup(
    destination: EndpointConfig,
    *,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    job_id: str,
) -> dict[str, Any]:
    """D111 — row-copy a populated PG destination before a strict keyed write.

    A failed verification then restores the exact prior state (DELETE + INSERT
    FROM ``__dfbak_<job>``) instead of leaving the rejected batch in place.
    Armed once per run (a COPY attempt and the row path share it); returns the
    ``route_backups`` stamp on ``destination.extra``. A backup that cannot be
    taken never aborts the write, but a table whose triggers this role cannot
    suspend is refused up front (D122) — that restore could never be exact.
    """
    from connectors.table_manager import PgBackupRestoreRefused, create_pg_table_backup

    extra = dict(getattr(destination, "extra", None) or {})
    backups = dict(extra.get("route_backups") or {})
    if backups:
        return backups
    try:
        pair = create_pg_table_backup(
            dest_cfg,
            schema=schema_from_cfg(dest_type, dest_cfg),
            table=dest_table,
            job_id=job_id,
        )
    except PgBackupRestoreRefused:
        raise
    except Exception as exc:  # noqa: BLE001 - never abort a write for the safety net
        logger.warning("Strict-run destination backup of %s failed: %s", dest_table, exc)
        return backups
    if pair:
        backups[str(pair.get("table") or dest_table)] = pair
        extra["route_backups"] = backups
        destination.extra = extra
    return backups


def _fk_orphans_fail_closed(validation_mode: str) -> bool:
    return str(validation_mode or "").strip().lower() in {"strict", "maximum"}


def _fk_orphan_violation_message(details: list[dict[str, Any]]) -> str:
    samples = "; ".join(str(d.get("reason") or "") for d in details[:3])
    return (
        f"Referential integrity failed before write: {len(details)} orphan "
        f"child row(s) in this page reference a parent absent at the destination "
        f"({samples}). Strict validation refuses to commit an orphan row — load "
        "the parent rows first or run in balanced mode to quarantine them."
    )


def stream_database_transfer(
    source: EndpointConfig,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Callable[..., None] | None = None,
    *,
    sync_mode: str = "full_refresh_append",
    stream_contracts: list[dict] | None = None,
    job_id: str | None = None,
    checkpoint: Checkpoint | None = None,
    checkpoint_service: CheckpointService | None = None,
    retry_budget: RetryBudget | None = None,
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    source_filter: dict[str, Any] | None = None,
    limit: int = 0,
    skip_preflight: bool = False,
    shape_runner: ShapeRunner | None = None,
    shape_steps: list[dict] | None = None,
    mappings_inherited: bool = False,
    initial_batch: Any = None,
    workspace_id: str = "",
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """
    Extract source table in CHUNK_SIZE batches and load to destination.
    Returns (rows_written, ddl_log, dest_summary, columns).

    ``mappings_inherited`` is true when this stream did not declare its own
    map and the list belongs to another table. A mismatch becomes an identity
    map of this stream's columns so those columns are not dropped.

    ``shape_runner`` is the recipe Validate approved, applied to every page as it
    is read — so the writer, the DDL, the digest and the destination all describe
    the shaped rows, and the recipe's effects are counted once for the whole run.
    ``shape_steps`` is the same recipe for one stream of a multi-table run,
    built into a runner once that stream's columns are known. A server-side
    COPY cannot apply either, so both decline the fast path.

    Property 3: full-refresh PostgreSQL/SQLite reads bind one snapshot session
    for the whole pagination lifetime (see ``services.source_snapshot``).
    """
    ok = False
    try:
        result = _stream_database_transfer_impl(
            source,
            destination,
            mappings,
            schema,
            on_checkpoint,
            sync_mode=sync_mode,
            stream_contracts=stream_contracts,
            job_id=job_id,
            checkpoint=checkpoint,
            checkpoint_service=checkpoint_service,
            retry_budget=retry_budget,
            backfill_new_fields=backfill_new_fields,
            validation_mode=validation_mode,
            source_filter=source_filter,
            limit=limit,
            skip_preflight=skip_preflight,
            shape_runner=shape_runner,
            shape_steps=shape_steps,
            mappings_inherited=mappings_inherited,
            initial_batch=initial_batch,
            workspace_id=workspace_id,
        )
        ok = True
        return result
    finally:
        try:
            from services.source_snapshot import release_active_snapshot

            release_active_snapshot(commit=ok)
        except Exception as exc:
            logger.warning("source snapshot release failed: %s", exc, exc_info=exc)
        try:
            from services.procedure_source import close_callable_spool

            if job_id:
                close_callable_spool(job_id=job_id)
        except Exception as exc:
            logger.warning("callable spool release failed: %s", exc, exc_info=exc)


from .copy_route import _try_copy_fast_path  # noqa: E402 — see module docstring


#: SQL sources whose catalog (types, nullability, defaults, keys) is read for
#: Property 6 create-new fidelity and keyset pagination.
_PK_INTROSPECT_TYPES: tuple[str, ...] = (
    "postgresql",
    "redshift",
    "mysql",
    "snowflake",
    "sqlserver",
    "oracle",
    "sqlite",
    "generic_sql",
    "bigquery",
    "databricks",
)


from .stream_catalog import fast_path_source_catalog as _fast_path_source_catalog  # noqa: E402


def _stream_database_transfer_impl(
    source: EndpointConfig,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Callable[..., None] | None = None,
    *,
    sync_mode: str = "full_refresh_append",
    stream_contracts: list[dict] | None = None,
    job_id: str | None = None,
    checkpoint: Checkpoint | None = None,
    checkpoint_service: CheckpointService | None = None,
    retry_budget: RetryBudget | None = None,
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    source_filter: dict[str, Any] | None = None,
    limit: int = 0,
    skip_preflight: bool = False,
    shape_runner: ShapeRunner | None = None,
    shape_steps: list[dict] | None = None,
    mappings_inherited: bool = False,
    initial_batch: Any = None,
    workspace_id: str = "",
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """
    Extract source table in CHUNK_SIZE batches and load to destination.
    Returns (rows_written, ddl_log, dest_summary, columns).
    """
    # A recipe that arrives as steps is built after this stream's columns are
    # known. Until then it still has to keep the run off a server-side COPY,
    # which would write the raw rows the operator asked to change.
    pending_shape = shape_runner is not None or bool(shape_steps)
    from .connector_capabilities import resolve_bind_dialect
    src_type = resolve_driver_type(source.format)
    src_read_type = _stream_source_read_driver(source.format)
    dest_type = resolve_driver_type(destination.format)
    from connectors.sdk import get_descriptor

    sdk_descriptor = get_descriptor(src_type)
    sdk_source = bool(
        sdk_descriptor
        and "source" in sdk_descriptor.roles
        and sdk_descriptor.catalog_ids
    )
    initial_sdk_state = (
        getattr(checkpoint, "cursor_value", None) if sdk_source else None
    )
    src_cfg = resolve_connector_config(source)
    dest_cfg = resolve_connector_config(destination)
    # Gate-8 fingerprints must name the dialect the writer binds against, not
    # the driver family: generic_sql hides DuckDB/ClickHouse carrier rules.
    dest_bind_dialect = resolve_bind_dialect(
        destination.format, config_type=str(dest_cfg.get("type") or "")
    )
    from services.procedure_source import is_callable_source, stamp_callable_job_id

    src_cfg = stamp_callable_job_id(src_cfg, job_id)

    from services.sync_cursor import (
        compare_cursor_values,
        is_overwrite_sync,
        map_source_to_target,
        max_cursor_value,
        requires_incremental,
        requires_upsert,
        resolve_effective_sync_mode,
        resolve_incremental_read_scope,
        resolve_sync_contract,
        row_in_run_window,
        set_watermark,
        source_bounds_cursor_reads,
    )

    contract = resolve_sync_contract(stream_contracts)
    effective_sync = resolve_effective_sync_mode(
        sync_mode,
        contract.sync_mode if contract else None,
    )
    incremental = requires_incremental(effective_sync)
    cursor_source_col = contract.cursor_field if contract else ""
    pk_target_cols: list[str] = []
    pk_source_cols: list[str] = []
    cursor_pk_source = ""
    if contract and contract.primary_key:
        pk_source_cols = contract.primary_key_columns()
        pk_target_cols = [
            map_source_to_target(col, mappings) for col in pk_source_cols
        ]
    if incremental and cursor_source_col:
        # Tie-break incremental watermarks with a stable source PK (Airbyte
        # gap), or with the engine's own row identity (Mongo ``_id``) when the
        # contract declares none — rows sharing a cursor value across a page
        # edge are never skipped. Same owner as the preflight read scope.
        from services.keyset_pagination import incremental_tiebreak_column

        cursor_pk_source = incremental_tiebreak_column(
            src_read_type, cursor_source_col, pk_source_cols
        )
    if not pk_target_cols:
        from services.copy_kafka_common import kafka_family_name
        if kafka_family_name(dest_type) == "kafka":
            # A Kafka key is partitioning/compaction identity, not a merge
            # constraint — any declared identity (primary_key, upsert_key,
            # dedupe_key, merge_key) is the right produce key (D88).
            from services.sync_cursor import declared_identity_columns
            pk_source_cols = declared_identity_columns(
                SimpleNamespace(
                    stream_contracts=stream_contracts or [],
                    read_options={},
                    source_filter=source_filter or {},
                )
            )
            pk_target_cols = [
                map_source_to_target(c, mappings) for c in pk_source_cols
            ]
    if requires_upsert(effective_sync) and not pk_target_cols:
        # No contract key, but the destination table may declare one. That is
        # catalog evidence, not a guess, so an upsert keyed on it resolves rows
        # on the same identity the destination already enforces.
        pk_source_cols, pk_target_cols = _declared_destination_key_columns(
            dest_type,
            dest_cfg,
            resolve_dest_table(dest_type, destination, _source_name(source)),
            mappings,
        )
    # Source-catalog primary key is read with the schema below. Refusing here
    # made incremental_upsert fail before that evidence existed.
    write_mode = "insert"

    from .stream_dest_procedure import open_destination_row_plan

    dest_proc_plan, row_apply_plan = open_destination_row_plan(destination)

    # A server-side COPY never materializes a row in this process, so a recipe
    # could not be applied to one. A shaped run takes the paged route instead of
    # silently copying the raw rows the operator asked to change.
    from services.copy_fast_path import (
        begin_copy_decline_capture,
        begin_fast_path_create_scope,
        reset_copy_decline_capture,
        reset_fast_path_create_scope,
    )

    copy_decline: list[str] = []
    _decline_token, _ = begin_copy_decline_capture(copy_decline)
    # Property 6 — the source catalog (types, nullability, defaults, keys) is
    # read once here so a fast path that creates the destination plans its
    # DDL through the same schema-fidelity owner as the row path, and so the
    # paged read below does not introspect a second time.
    _src_rich_catalog: tuple[dict[str, str], dict[str, bool], dict[str, Any]] = (
        {},
        {},
        {},
    )
    if src_read_type in _PK_INTROSPECT_TYPES:
        try:
            _src_rich_catalog = _introspect_table_schema_rich(
                src_read_type,
                src_cfg,
                _source_name(source),
                [str(m.get("source") or "") for m in mappings if m.get("source")],
            )
        except Exception as exc:
            logger.debug("source schema introspection failed: %s", exc, exc_info=exc)
    if incremental and cursor_source_col and not cursor_pk_source:
        # One tie-break for every read path of this run (QA ACC-02). The COPY
        # fast path and the pre-copy watermark scope run before the row path,
        # so deriving the catalog tie-break only there let run 1 write a
        # composite watermark that run 2's COPY then decoded single-column.
        from services.keyset_pagination import catalog_incremental_tiebreak

        _tb_types, _tb_nulls, _tb_keys = _src_rich_catalog
        cursor_pk_source = catalog_incremental_tiebreak(
            src_read_type,
            cursor_source_col,
            contract_pk=pk_source_cols,
            catalog_pk=list(_tb_keys.get("primary_key_columns") or []),
            unique_keys=list(_tb_keys.get("unique_keys") or []),
            nullable=_tb_nulls,
        )
    if requires_upsert(effective_sync) and not pk_target_cols:
        from services.primary_key import mapped_catalog_upsert_key

        pk_source_cols, pk_target_cols = mapped_catalog_upsert_key(
            (_src_rich_catalog[2] or {}).get("primary_key_columns"),
            mappings,
        )
        if pk_source_cols and incremental and cursor_source_col and not cursor_pk_source:
            cursor_pk_source = incremental_tiebreak_column(
                src_read_type, cursor_source_col, pk_source_cols
            )
    if not pk_target_cols:
        from services.copy_kafka_common import kafka_family_name
        from services.primary_key import mapped_catalog_upsert_key

        if kafka_family_name(dest_type) == "kafka":
            # D88: with no declared identity, the source table's own primary
            # key is the message key; append runs went out key=None, which
            # breaks per-key ordering and log compaction downstream.
            pk_source_cols, pk_target_cols = mapped_catalog_upsert_key(
                (_src_rich_catalog[2] or {}).get("primary_key_columns"),
                mappings,
            )
    if requires_upsert(effective_sync) and not pk_target_cols:
        raise ValueError(
            f"Sync mode `{effective_sync}` requires primary_key for upsert; "
            "refuse silent insert fallback (set primary_key on the stream contract)"
        )
    write_mode = (
        "upsert" if requires_upsert(effective_sync) and pk_target_cols else "insert"
    )
    _create_scope_token, _create_scope = begin_fast_path_create_scope(
        _fast_path_source_catalog(src_read_type, mappings, schema, _src_rich_catalog),
        mappings,
        resolve_dest_table(dest_type, destination, _source_name(source)),
    )
    pre_copy_cursor_key = ""
    pre_copy_watermark = None
    if incremental and cursor_source_col and not pending_shape:
        _tbl = _source_name(source)
        if _tbl and not (is_callable_source(source) or is_callable_source(src_cfg)):
            from services.preflight_cursor_gate import refuse_unusable_cursor_state

            _dest_obj = resolve_dest_table(dest_type, destination, _tbl)
            _scope = resolve_incremental_read_scope(
                sync_mode=effective_sync,
                stream_contracts=stream_contracts,
                source_type=src_read_type,
                source_database=source.database or src_cfg.get("database", ""),
                source_object=_tbl,
                dest_type=dest_type,
                dest_database=destination.database or dest_cfg.get("database", ""),
                dest_object=_dest_obj,
                source=src_cfg,
                destination=destination,
                destination_config=dest_cfg,
            )
            pre_copy_cursor_key = _scope.cursor_key
            pre_copy_watermark = _scope.watermark
            from services.sync_cursor import reconcile_cursor_tiebreak

            cursor_pk_source = reconcile_cursor_tiebreak(_scope, cursor_pk_source)
            refuse_unusable_cursor_state(
                _scope, dest_type, dest_cfg, _dest_obj
            )
    # Gate-8 append proof needs the destination cardinality from before the
    # first write, whichever path performs it. Measured once here, ahead of the
    # COPY fast path, and reused by the row path below. A resumed run already
    # holds rows this job wrote, so its live COUNT is not a "before".
    pre_write_rows_before: int | None = None
    pinned_before = (getattr(destination, "extra", None) or {}).get("overwrite_rows_before")
    if isinstance(pinned_before, int):
        # Counted before overwrite DROP. A live COUNT now is the empty
        # replacement, which would authorize deleting a table that held rows.
        pre_write_rows_before = int(pinned_before)
    elif not (
        checkpoint
        and (
            getattr(checkpoint, "rows_processed", 0)
            or getattr(checkpoint, "offset", 0)
        )
    ):
        pre_write_rows_before = precount_table(
            dest_type,
            dest_cfg,
            resolve_dest_table(dest_type, destination, _source_name(source)),
        )
    # Schema evolution (ADD COLUMN / widen under backfill_new_fields) is owned
    # by the destination writer; a server-to-server copy would write into the
    # destination's current shape and fail on the column the source just grew.
    # An occupied destination under backfill therefore stays on the row path.
    writer_owns_evolution = bool(backfill_new_fields) and pre_write_rows_before is not None
    if writer_owns_evolution and not pending_shape:
        logger.info(
            "COPY fast path declined: backfill_new_fields on an existing %s "
            "destination — schema evolution runs on the writer path",
            dest_type,
        )
    # Write-path referential-integrity guard (QA fk__pg-maria / fk__pg-pg):
    # child rows whose destination parent does not exist are quarantined, not
    # committed. Built before the COPY decision: a server-to-server copy never
    # brings a row into this process, so it cannot hold an orphan back — a
    # guarded route takes the row path instead of landing orphans unchecked.
    # Strict/maximum fail the run on the first orphan before it is committed;
    # balanced/warn quarantine it and Gate-8 G22 reports the count as a warn.
    fk_orphan_guard = None
    try:
        from .stream_foreign_keys import build_fk_orphan_guard

        fk_orphan_guard = build_fk_orphan_guard(
            source,
            destination,
            _source_name(source),
            dest_type=dest_type,
            dest_cfg=dest_cfg,
        )
    except Exception as exc:
        logger.debug("FK orphan guard not built for %s: %s", _source_name(source), exc)
    if fk_orphan_guard is not None:
        copy_decline.append(
            "COPY fast path declined: source foreign keys need a per-row "
            "destination parent check (orphans are quarantined on the row path)"
        )
    # Signed Risk Contracts (QUARANTINE_ROW / SKIP_ROW / CAST_AND_CONTINUE)
    # are per-row dispositions. A server-to-server copy never sees a row, so a
    # value the narrower destination cannot hold aborted the whole COPY instead
    # of being held out as the contract instructs — those routes stream rows.
    risk_contract_columns = [
        str(m.get("source") or m.get("target") or "")
        for m in (mappings or [])
        if isinstance(m, dict) and m.get("risk_contract")
    ]
    if risk_contract_columns:
        logger.info(
            "COPY fast path declined for %s: Risk Contract on %s needs per-row "
            "disposition on the writer path",
            _source_name(source),
            ", ".join(risk_contract_columns),
        )
        copy_decline.append(
            "COPY fast path declined: signed Risk Contract columns ("
            + ", ".join(risk_contract_columns)
            + ") need per-row quarantine/skip on the row path"
        )
    # D111/D122 — the strict keyed backup is the rollback for *either* write
    # path. Armed here, a COPY upsert is undone exactly like the row path
    # instead of leaving its rejected batch live; the row path reuses it.
    copy_route_backups: dict[str, Any] = {}
    if (
        write_mode == "upsert"
        and pk_target_cols
        and dest_type == "postgresql"
        and not pending_shape
        and str(validation_mode or "").strip().lower() in {"strict", "maximum", ""}
        and int(pre_write_rows_before or 0) > 0
    ):
        copy_route_backups = _arm_strict_keyed_backup(
            destination,
            dest_type=dest_type,
            dest_cfg=dest_cfg,
            dest_table=resolve_dest_table(dest_type, destination, _source_name(source)),
            job_id=str(job_id or ""),
        )
    _copy_profile = PhaseProfile()
    _copy_started = time.perf_counter()
    try:
        from .stream_dest_procedure import decline_copy_for_destination_sql

        decline_copy_for_destination_sql(dest_proc_plan)
        fast = None if (
            pending_shape
            or writer_owns_evolution
            or dest_proc_plan is not None
            or fk_orphan_guard is not None
            or bool(risk_contract_columns)
        ) else _try_copy_fast_path(
            source=source,
            destination=destination,
            mappings=mappings,
            schema=schema,
            src_type=src_read_type,
            dest_type=dest_type,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            effective_sync=effective_sync,
            incremental=incremental,
            source_filter=source_filter,
            limit=limit,
            checkpoint=checkpoint,
            incremental_cursor=cursor_source_col if incremental else "",
            incremental_watermark=pre_copy_watermark,
            incremental_pk=(cursor_pk_source or "") if incremental else "",
            incremental_key=(
                (pk_source_cols[0] if pk_source_cols else "")
                if incremental else ""
            ),
            job_id=job_id,
        )
    finally:
        reset_copy_decline_capture(_decline_token)
        reset_fast_path_create_scope(_create_scope_token)
    if fast is not None:
        rows_copied, ddl_log, dest_summary, columns = fast
        dest_summary["copy_fast_path"] = "used"
        if copy_route_backups:
            dest_summary["route_backups"] = dict(copy_route_backups)
        _copy_profile.add(
            PHASE_BULK_COPY,
            time.perf_counter() - _copy_started,
            rows=int(rows_copied or 0),
        )
        dest_summary.setdefault("phase_profile", _copy_profile.snapshot())
        _certificate = _create_scope.certificate()
        if _certificate is not None:
            dest_summary.setdefault("schema_fidelity", _certificate)
        _fast_census = (dest_summary.get("source_snapshot") or {}).get(CENSUS_KEY)
        if _fast_census:
            dest_summary.setdefault(CENSUS_KEY, _fast_census)
        _fast_table = _source_name(source)
        _carry_single_table_foreign_keys(
            source,
            destination,
            _fast_table,
            resolve_dest_table(dest_type, destination, _fast_table),
            mappings,
            dest_summary,
            ddl_log,
        )
        # Bulk COPY never pages the source; say so instead of leaving the
        # pagination fields absent (which reads as "unknown" to the operator).
        dest_summary.setdefault("pagination_mode", "bulk_copy")
        if pre_write_rows_before is not None:
            dest_summary.setdefault(PRECOUNT_KEY, int(pre_write_rows_before))
        if incremental:
            dest_summary["sync_mode"] = effective_sync
            wm = str(dest_summary.get("incremental_watermark") or "").strip()
            if int(rows_copied or 0) == 0:
                stamp_incremental_no_op(dest_summary)
                dest_summary["watermark"] = pre_copy_watermark
            elif wm and pre_copy_cursor_key:
                set_watermark(
                    pre_copy_cursor_key,
                    wm,
                    metadata={
                        "job_id": job_id,
                        "sync_mode": effective_sync,
                        "cursor_column": cursor_source_col,
                        # The column a composite watermark is decodable on (QA ACC-02).
                        "tiebreak_column": cursor_pk_source,
                    },
                )
                dest_summary["watermark"] = wm
                # D110 — if verification removes this run's batch, the route
                # watermark must roll back to this pre-run value.
                dest_summary.setdefault(
                    "watermark_restore_point",
                    {
                        "cursor_key": pre_copy_cursor_key,
                        "watermark": pre_copy_watermark,
                    },
                )
            dest_summary["cursor_key"] = pre_copy_cursor_key
            if pre_copy_watermark not in (None, "") and cursor_source_col:
                # D99 — the COPY seed drops NULL-cursor rows exactly as the
                # row path's seek does; the job says so on both paths.
                from services.null_cursor_probe import stamp_skipped_null_cursor_rows

                stamp_skipped_null_cursor_rows(
                    dest_summary,
                    _null_cursor_rows_skipped(
                        src_cfg, _source_name(source), cursor_source_col
                    ),
                    cursor_source_col,
                )
        return rows_copied, ddl_log, dest_summary, columns
    # Parallel/chunked resume is only safe with idempotent writes.
    resuming = bool(checkpoint and getattr(checkpoint, "chunk_index", 0) > 0)
    resume_key_resolved = False
    from services.copy_kafka_common import kafka_family_name as _kafka_family
    if resuming and write_mode == "insert" and _kafka_family(dest_type) != "kafka":
        resume_key_resolved = True
        if not pk_target_cols:
            # No stream contract, but the destination itself may enforce a key.
            # Resuming an append re-delivers the interrupted batch, so honouring
            # that key turns an unavoidable duplicate-key abort into an
            # idempotent apply instead of stranding a half-loaded destination.
            pk_source_cols, pk_target_cols = _declared_destination_key_columns(
                dest_type,
                dest_cfg,
                resolve_dest_table(dest_type, destination, _source_name(source)),
                mappings,
            )
        if pk_target_cols:
            write_mode = "upsert"
        else:
            raise ValueError(
                "Cannot resume a streaming insert without a primary key. "
                "Re-run this transfer with Full refresh · Overwrite to reload it "
                "safely, or set a primary key to make the replay idempotent."
            )
    # Decide up front whether an interrupted batch may be re-sent. Append-only
    # destinations get retries only for failures that prove nothing landed;
    # everything else keeps the full retry budget.
    replay_safety = classify_replay_safety(
        dest_type=dest_type,
        write_mode=write_mode,
        conflict_columns=pk_target_cols or None,
        job_id=job_id,
        has_primary_key=bool(pk_target_cols),
    )
    if src_read_type not in _STREAMING_SOURCES and not sdk_source:
        raise ValueError(f"Streaming source '{src_type}' not supported")
    if dest_type not in _STREAMING_DESTINATIONS:
        raise ValueError(f"Streaming destination '{dest_type}' not supported")

    table = _source_name(source)
    if is_callable_source(source) or is_callable_source(src_cfg):
        from services.procedure_source import (
            parse_callable_source,
            procedure_params_of,
            procedure_text_of,
            source_object_for_cursor,
            source_read_mode_of,
            stream_name_for_callable,
        )

        if not table:
            try:
                spec = parse_callable_source(
                    procedure_text_of(source) or procedure_text_of(src_cfg),
                    dialect=src_read_type,
                    mode=source_read_mode_of(source) or source_read_mode_of(src_cfg),
                    params=procedure_params_of(source) or procedure_params_of(src_cfg),
                )
                table = stream_name_for_callable(spec)
            except Exception:
                table = "procedure_result"
        cursor_source_object = (
            source_object_for_cursor(source)
            or source_object_for_cursor(src_cfg)
            or table
        )
    else:
        cursor_source_object = table
    if not table:
        raise ValueError("Source table/collection name required for streaming transfer")

    watermark = None
    cursor_key = ""
    if incremental and cursor_source_col:
        # One resolver for the whole product: Validate's gates read this route's
        # watermark through the same call, so what they judge is what this run
        # will read. Callable identity includes SQL+binds, not just the stream label.
        scope = resolve_incremental_read_scope(
            sync_mode=effective_sync,
            stream_contracts=stream_contracts,
            source_type=src_read_type,
            source_database=source.database or src_cfg.get("database", ""),
            source_object=cursor_source_object,
            dest_type=dest_type,
            dest_database=destination.database or dest_cfg.get("database", ""),
            dest_object=resolve_dest_table(dest_type, destination, table),
            source=source if is_callable_source(source) or is_callable_source(src_cfg) else src_cfg,
            destination=destination,
            destination_config=dest_cfg,
        )
        cursor_key = scope.cursor_key
        watermark = scope.read_watermark
        from services.preflight_cursor_gate import refuse_unusable_cursor_state
        from services.sync_cursor import reconcile_cursor_tiebreak

        cursor_pk_source = reconcile_cursor_tiebreak(scope, cursor_pk_source)

        refuse_unusable_cursor_state(
            scope, dest_type, dest_cfg, resolve_dest_table(dest_type, destination, table)
        )
    # A source whose read call cannot carry the cursor bound (Redis SCAN, Dynamo
    # Scan, an ES search) hands its whole population over, so the bound is
    # applied to each page instead. Without it "incremental" degraded into a
    # full re-read and the second run appended the population again.
    client_cursor_bound = bool(
        incremental
        and cursor_source_col
        and watermark
        and not source_bounds_cursor_reads(src_read_type)
    )

    # D99: when the source bounds the delta in SQL, ``cursor > watermark``
    # silently drops every NULL-cursor row — the job must say how many it
    # skipped rather than report verified. Not only the pushdown family:
    # SQLite / SQL Server / Oracle reads are SQL-bounded as well (their seek or
    # filtered scan drops NULLs before the client-side bound could refuse
    # them), so the probe runs for every source it can measure; a source that
    # hands NULL-cursor rows over is refused by the client-side bound instead.
    null_cursor_rows_skipped: int | None = None
    if incremental and cursor_source_col and watermark:
        null_cursor_rows_skipped = _null_cursor_rows_skipped(
            src_cfg, cursor_source_object or table, cursor_source_col
        )

    src_db = source.database or src_cfg.get("database") or ("test" if src_type == "mongodb" else "")

    if src_type == "gcs" and not src_cfg.get("database"):
        raise ValueError("GCS source requires bucket name in the database field")
    if src_type == "s3" and not src_cfg.get("database"):
        raise ValueError("S3 source requires bucket name in the database field")

    if src_type in ("s3", "gcs", "adls", "sftp"):
        from services.object_streaming import (
            download_for_object_store,
            download_object,
            stream_spilled_file_to_database,
        )

        bucket = source.database or src_cfg.get("database", "")
        key = table
        if src_type in ("s3", "gcs") and not bucket:
            raise ValueError(f"{src_type.upper()} source requires bucket name in the database field")
        from services.file_pickup import is_file_pattern

        extra = getattr(source, "extra", None) or {}
        wait_seconds = int(extra.get("file_wait_seconds") or 0)
        if is_file_pattern(key) or wait_seconds:
            from services.file_pickup_load import load_picked_files

            return load_picked_files(
                src_type=src_type,
                src_cfg=src_cfg,
                bucket=bucket,
                key=key,
                dest_type=dest_type,
                dest_cfg=dest_cfg,
                source=source,
                destination=destination,
                mappings=mappings,
                schema=schema,
                effective_sync=effective_sync,
                stream_contracts=stream_contracts,
                job_id=job_id,
                checkpoint=checkpoint,
                checkpoint_service=checkpoint_service,
                retry_budget=retry_budget,
                backfill_new_fields=backfill_new_fields,
                validation_mode=validation_mode,
                source_filter=source_filter,
                skip_preflight=skip_preflight,
                shape_runner=shape_runner,
                pre_write_rows_before=pre_write_rows_before,
            )
        cache_key = f"{src_type}:{bucket}:{key}"
        # Always fetch a fresh copy for a transfer run; atomic download protects
        # against partial/corrupt reuse.
        path = download_object(
            cache_key,
            lambda p: download_for_object_store(src_type, p, src_cfg, bucket, key),
            force=True,
        )
        return stream_spilled_file_to_database(
            path=path,
            filename=key,
            destination=destination,
            mappings=mappings,
            schema=schema,
            sync_mode=effective_sync,
            stream_contracts=stream_contracts,
            job_id=job_id,
            checkpoint=checkpoint,
            checkpoint_service=checkpoint_service,
            retry_budget=retry_budget,
            backfill_new_fields=backfill_new_fields,
            validation_mode=validation_mode,
            source_filter=source_filter,
            skip_preflight=skip_preflight,
            shape_runner=shape_runner,
        )

    # Property 3 — one MVCC/transaction snapshot for the whole full-refresh read
    # (sample probe + pages + optional checksum re-read). Incremental syncs
    # intentionally follow watermarks and do not bind a frozen snapshot.
    if not incremental and src_read_type in ("postgresql", "redshift"):
        try:
            from services.source_snapshot import (
                activate_snapshot,
                begin_postgresql_repeatable_read,
                end_postgresql_snapshot,
            )

            _snap_conn, _snap_meta = begin_postgresql_repeatable_read(
                host=str(src_cfg.get("host") or ""),
                port=int(src_cfg.get("port") or (5439 if src_type == "redshift" else 5432)),
                database=str(src_cfg.get("database") or ""),
                username=str(src_cfg.get("username") or ""),
                password=str(src_cfg.get("password") or ""),
                connection_string=str(src_cfg.get("connection_string") or ""),
                ssl=bool(src_cfg.get("ssl", False)),
            )
            activate_snapshot(_snap_conn, _snap_meta, end_postgresql_snapshot)
            logger.info(
                "Property 3 source snapshot bound — engine=postgresql "
                "isolation=repeatable_read lsn=%s",
                _snap_meta.get("snapshot_lsn"),
            )
        except Exception as exc:
            logger.warning(
                "Property 3 PostgreSQL REPEATABLE READ snapshot unavailable (%s) — "
                "falling back to per-page READ COMMITTED (weaker guarantee)",
                exc,
                exc_info=exc,
            )
    elif not incremental and src_type == "sqlite":
        try:
            from services.source_snapshot import (
                activate_snapshot,
                begin_sqlite_snapshot,
                end_sqlite_snapshot,
            )

            _snap_conn, _snap_meta = begin_sqlite_snapshot(
                database=str(src_cfg.get("database") or ""),
                connection_string=str(
                    src_cfg.get("connection_string") or src_cfg.get("uri") or ""
                ),
                host=str(src_cfg.get("host") or ""),
            )
            activate_snapshot(_snap_conn, _snap_meta, end_sqlite_snapshot)
            logger.info("Property 3 source snapshot bound — engine=sqlite")
        except Exception as exc:
            logger.warning(
                "Property 3 SQLite transaction snapshot unavailable (%s) — "
                "falling back to per-page connections",
                exc,
                exc_info=exc,
            )
    elif not incremental and src_type == "snowflake":
        try:
            from services.connector_auth import snowflake_session_kwargs
            from services.source_snapshot import (
                activate_snapshot,
                begin_snowflake_time_travel,
                end_snowflake_time_travel,
            )

            session = snowflake_session_kwargs(src_cfg)
            _snap_conn, _snap_meta = begin_snowflake_time_travel(
                host=str(src_cfg.get("host") or ""),
                database=str(src_cfg.get("database") or ""),
                username=str(src_cfg.get("username") or ""),
                password=str(src_cfg.get("password") or ""),
                schema=str(src_cfg.get("schema") or "PUBLIC"),
                warehouse=str(src_cfg.get("warehouse") or ""),
                connection_string=str(src_cfg.get("connection_string") or ""),
                role=str(session.get("role") or src_cfg.get("role") or ""),
                private_key=str(session.get("private_key") or src_cfg.get("private_key") or ""),
            )
            activate_snapshot(_snap_conn, _snap_meta, end_snowflake_time_travel)
            logger.info(
                "Property 3 source snapshot bound — engine=snowflake "
                "isolation=time_travel ts=%s",
                _snap_meta.get("time_travel_ts"),
            )
        except Exception as exc:
            logger.warning(
                "Property 3 Snowflake Time Travel snapshot unavailable (%s) — "
                "falling back to live scans (re-read may see concurrent writes)",
                exc,
                exc_info=exc,
            )

    # Memory-safe chunk sizing: sample a few rows, then size batches to keep
    # per-batch memory within a destination-safe limit while respecting CHUNK_SIZE.
    sample_limit = 100 if limit == 0 else min(100, limit)
    # Created before the first read so the sizing probe and the first data page
    # are attributed too. Both are real source reads; leaving them out reported
    # "Reading source 0.0s" on any transfer that fit in a single batch.
    phase_profile = PhaseProfile()
    # Baselines for the engine/schema caches so the run summary reports what
    # *this* transfer reused, not the process lifetime totals.
    try:
        from services.engine_pool import stats as _engine_stats
        from services.reflection_cache import stats as _schema_stats

        _pool_baseline = _engine_stats().to_dict()
        _schema_baseline = _schema_stats().to_dict()
    except Exception:
        _pool_baseline = None
        _schema_baseline = None

    from services.keyset_pagination import (
        catalog_incremental_tiebreak,
        cursor_unique_evidence,
        incremental_read_needs_filtered_scan,
        safe_keyset_unique_columns,
    )

    _cat_types, _cat_nulls, _cat_keys = _src_rich_catalog
    if incremental and cursor_source_col and not cursor_pk_source:
        # The keyset decision later seeks on the catalog primary key. The
        # filtered-scan check has to see that same column or the two owners
        # disagree on the first incremental run (E3-002).
        cursor_pk_source = catalog_incremental_tiebreak(
            src_read_type,
            cursor_source_col,
            contract_pk=pk_source_cols,
            catalog_pk=list(_cat_keys.get("primary_key_columns") or []),
            unique_keys=list(_cat_keys.get("unique_keys") or []),
            nullable=_cat_nulls,
        )
    _cursor_is_unique = cursor_unique_evidence(
        cursor_source_col,
        primary_key_columns=pk_source_cols or (_cat_keys.get("primary_key_columns") or []),
        unique_keys=_cat_keys.get("unique_keys") or [],
        nullable=_cat_nulls,
    )
    _filtered_scan_reason = incremental_read_needs_filtered_scan(
        src_type=src_read_type,
        incremental=bool(incremental),
        cursor_column=cursor_source_col,
        tiebreak_column=cursor_pk_source,
        cursor_is_unique=_cursor_is_unique,
        callable_source=bool(is_callable_source(source) or is_callable_source(src_cfg)),
    )
    if _filtered_scan_reason:
        logger.info("%s.%s: %s", src_type, table, _filtered_scan_reason)

    def _cursor_read_args(
        cursor_after: str | None, *, run_seed: bool = False
    ) -> dict[str, Any]:
        """The cursor arguments every read of this run must agree on.

        A watermark is composite when a tie-break primary key is in play, and
        only a read that also names that column can decode it. Reads that left
        ``cursor_primary_key`` out bound the whole composite against the cursor
        column instead, so every run after the first died on the sizing probe.

        ``run_seed`` marks a read bounded by the stored watermark itself rather
        than by a page edge this run advanced. D124/D60/D98: the stored
        ``(cursor, last key)`` only orders *this* run's pages — between runs a
        row can be inserted or restamped at the watermark cursor with a lower
        key, so the seed re-reads the whole tie group (``cursor >= watermark``).
        """
        if not incremental or not cursor_source_col:
            return {}
        return {
            "cursor_column": cursor_source_col,
            "cursor_after": cursor_after,
            "cursor_inclusive": bool(run_seed and cursor_after not in (None, "")),
            "cursor_type": (
                normalize_inferred(schema.get(cursor_source_col, "string")).upper()
                if schema
                else None
            ),
            "cursor_primary_key": cursor_pk_source or None,
        }

    _sample_started = time.perf_counter()
    # Snowflake snapshot scan opens one warehouse session on the first write
    # page. A separate OFFSET sizing login resumes the warehouse twice — that
    # is how a 150k TPC-H extract spent minutes before the first real page.
    skip_sf_sizing_probe = (src_type == "snowflake" and not incremental) or sdk_source
    if skip_sf_sizing_probe:
        sample_probe = type("SizingSkip", (), {"rows": [], "headers": [], "total_rows": None})()
        sample_rows = []
        avg_row_size = 256
        phase_profile.add(PHASE_READ, time.perf_counter() - _sample_started, rows=0)
    else:
        sample_probe, _ = _unwrap_read(
            _read_batch(
                src_read_type, src_cfg, table, None, 0, sample_limit, database=src_db,
                **_cursor_read_args(watermark, run_seed=True),
            )
        )
        phase_profile.add(
            PHASE_READ,
            time.perf_counter() - _sample_started,
            rows=len(sample_probe.rows or []),
        )
        sample_rows = sample_probe.rows or []
        avg_row_size = 100
    if sample_rows:
        # Prefer mapped cell byte lengths — len(str(row)) under-counts nested/wide payloads.
        sizes: list[int] = []
        for row in sample_rows:
            if isinstance(row, dict):
                sizes.append(sum(len(str(v).encode("utf-8", errors="replace")) for v in row.values()) + 64)
            else:
                sizes.append(max(1, len(str(row).encode("utf-8", errors="replace"))))
        avg_row_size = max(1, int(sum(sizes) / len(sizes)))
    # Warehouses benefit from larger stream chunks so batches clear COPY/MERGE
    # thresholds; Mongo already used 64MB — extend the same class to SF/BQ/RS.
    # PostgreSQL/MySQL stay tight: wide JSON/TEXT rows OOM mid-COPY otherwise.
    _warehouse_dests = {"mongodb", "snowflake", "bigquery", "redshift"}
    _sql_tight = {"postgresql", "postgres", "mysql", "mariadb", "sqlserver", "mssql"}
    if dest_type in _warehouse_dests:
        target_memory_bytes = 64 * 1024 * 1024
    elif dest_type in _sql_tight and avg_row_size >= 4096:
        target_memory_bytes = 4 * 1024 * 1024  # wide-row PG/MySQL ceiling
    elif dest_type in _sql_tight:
        target_memory_bytes = 8 * 1024 * 1024
    else:
        target_memory_bytes = 8 * 1024 * 1024
    requested_chunk = int(CHUNK_SIZE)
    chunk_size = adaptive_chunk_size(CHUNK_SIZE, avg_row_size, max_size=CHUNK_SIZE, target_memory_bytes=target_memory_bytes)
    adaptive_chunk = int(chunk_size)
    proxy_capped = False
    object_store_single_shot = False
    # Cap stream batches on public TCP proxies so one socket never holds a 20k-row window.
    try:
        from connectors.write_resilience import proxy_stream_batch_size
    except ImportError:
        proxy_stream_batch_size = None  # type: ignore
    if proxy_stream_batch_size is not None:
        before_proxy = int(chunk_size)
        chunk_size = proxy_stream_batch_size(
            dest_cfg.get("host"),
            connection_string=dest_cfg.get("connection_string") or dest_cfg.get("uri") or "",
            default=chunk_size,
        )
        proxy_capped = int(chunk_size) < before_proxy
    # Object-store writers (S3/GCS/ADLS) emit a single destination object per call.
    # Chunked writes would overwrite the same key and silently lose data.
    # Always force a single shot — gating on truthy total_rows failed open after
    # honesty waves set total_rows=None for unknown cardinality.
    if dest_type in ("s3", "gcs", "adls"):
        chunk_size = max(1, int(sample_probe.total_rows or 10_000_000))
        object_store_single_shot = True
    chunk_policy = {
        "requested": requested_chunk,
        "adaptive": adaptive_chunk,
        "final": int(chunk_size),
        "avg_row_bytes": int(avg_row_size),
        "target_memory_bytes": int(target_memory_bytes),
        "proxy_capped": bool(proxy_capped),
        "object_store_single_shot": bool(object_store_single_shot),
        "wide_row": bool(avg_row_size >= 4096),

        "reason": (
            "object-store single-shot"
            if object_store_single_shot
            else "proxy batch cap"
            if proxy_capped
            else "adaptive memory target"
            if adaptive_chunk != requested_chunk
            else "default chunk"
        ),
    }

    def _batch_limit(offset: int = 0, *, default: int = chunk_size) -> int:
        if limit > 0:
            return max(0, min(default, limit - offset))
        return default

    _probe_started = time.perf_counter()
    # Warehouse/SQL snapshot without a keyset must not OFFSET-page (O(n²) +
    # new login per chunk). One SELECT + fetchmany, started on this first page.
    from connectors.sql_snapshot_scan import SNAPSHOT_SCAN_SOURCES as _SNAPSHOT_SCAN_SOURCES

    src_scan: dict[str, Any] = {}
    # A held scan always starts at row 0. A resume with a keyset bookmark
    # seeks and must not open that scan. A resume that only has a row count
    # keeps the scan: one SELECT, then drain the committed prefix — OFFSET
    # pages are O(n²) and can skip/duplicate under concurrent writes.
    _is_resume = bool(
        checkpoint
        and (
            int(getattr(checkpoint, "offset", 0) or 0) > 0
            or int(getattr(checkpoint, "chunk_index", 0) or 0) > 0
        )
    )
    _resume_bookmark = (
        getattr(checkpoint, "cursor_value", None) if checkpoint is not None else None
    )
    # An incremental cursor with no unique tie-break also holds a scan: the
    # filter is the run watermark and paging is fetchmany, so the bookmark a
    # resume carries is a row count, never a seek key.
    _hold_snapshot = src_read_type in _SNAPSHOT_SCAN_SOURCES and (
        bool(_filtered_scan_reason)
        or (
            not incremental
            and not (_is_resume and _resume_bookmark not in (None, ""))
        )
    )
    _scan_kw: dict[str, Any] = {"scan_state": src_scan} if _hold_snapshot else {}
    if _filtered_scan_reason:
        _scan_kw["scan_filter"] = (cursor_source_col, watermark)
        _probe_cursor_kw: dict[str, Any] = {}
    else:
        # The probe page is the run's first written batch, read from the seed.
        _probe_cursor_kw = _cursor_read_args(watermark, run_seed=True)
    if sdk_source and initial_batch is not None:
        probe, ddb_cursor = initial_batch, None
    else:
        probe, ddb_cursor = _unwrap_read(
            _read_batch(
                src_read_type, src_cfg, table, None, 0, _batch_limit(0), database=src_db,
                **_probe_cursor_kw,
                **_scan_kw,
                **(
                    {"sdk_state": initial_sdk_state}
                    if sdk_source and initial_sdk_state
                    else {}
                ),
            )
        )
    # This page is not thrown away — it becomes the first written batch.
    phase_profile.add(
        PHASE_READ, time.perf_counter() - _probe_started, rows=len(probe.rows or [])
    )
    columns = probe.headers
    if not columns and src_type == "dynamodb":
        # Empty or key-only Dynamo tables still have a KeySchema — seed from
        # describe so Map/Validate can proceed instead of aborting the demo.
        try:
            from connectors.dynamodb_reader import describe_table_schema

            key_names, key_types = describe_table_schema(src_cfg, table)
            if key_names:
                columns = list(key_names)
                for name, lt in (key_types or {}).items():
                    schema.setdefault(name, lt)
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "DynamoDB empty-probe KeySchema seed failed for %s: %s", table, exc
            )
    if not columns and incremental and schema:
        # A schemaless source names its columns from the documents it returns, so
        # an incremental page with nothing past the watermark comes back with no
        # headers. That is a zero-row run at the current mark, not an empty
        # source: the declared shape carries the run to the no-op close.
        columns = list(schema.keys())
    if not columns:
        raise ValueError(f"Source table `{table}` has no columns or is empty")

    # Schemaless sources — absorb sparse attrs discovered mid-transfer so they
    # are never silently dropped from destination writes (Dynamo/Mongo/ES/Redis).
    #
    # This runs on chunk-writer threads, and it rebinds state the reader thread
    # and sibling chunks read (columns, mappings, column_types, schema,
    # target_cols). Two pages that each discover a new attribute used to
    # interleave their read-modify-write and lose one of the attributes — a
    # silent column drop, which is exactly what this function exists to prevent.
    _schema_absorb_lock = threading.Lock()

    def _absorb_schemaless_discovered_attrs(batch) -> None:
        nonlocal columns, mappings, column_types, schema, backfill_new_fields, target_cols
        from connectors.header_union import (
            SCHEMALESS_SOURCE_TYPES,
            union_attribute_keys,
        )

        if src_type not in SCHEMALESS_SOURCE_TYPES or not batch or not getattr(batch, "headers", None):
            return

        with _schema_absorb_lock:
            known = set(columns)
            new_headers = [h for h in batch.headers if h and h not in known]
            if not new_headers and set(batch.headers).issubset(known):
                return
            columns = union_attribute_keys(columns, batch.headers)
            mapped_sources = {str(m.get("source") or "") for m in mappings}
            meta = getattr(batch, "meta", None) or {}
            native_types = meta.get("native_types") if isinstance(meta, dict) else {}
            if not isinstance(native_types, dict):
                native_types = {}
            for h in new_headers:
                lt = str(native_types.get(h) or schema.get(h) or "VARCHAR")
                schema[h] = lt
                column_types[h] = ddl_carrier_type(lt)
                if h not in mapped_sources:
                    mappings.append({
                        "source": h,
                        "target": h,
                        "confidence": 0.9,
                        # Force ADD COLUMN on SQL dests when attrs appear after CREATE.
                        "create_new": True,
                        "assignment_strategy": "create_compatible_new",
                    })
                    mapped_sources.add(h)
                    ddl_log.append(
                        f"{dest_type.upper()} COLUMN {h} {ddl_type(dest_type, schema.get(h, 'string'))} "
                        f"(discovered mid-transfer from {src_type} — no silent drop)"
                    )
            if new_headers:
                backfill_new_fields = True
                target_cols, _ = resolve_target_columns(mappings, column_types, preserve_case=True)

    if not schema:
        if src_type in ("s3", "gcs", "adls"):
            try:
                from services.object_store_introspect import profile_object_batch
                profiled = profile_object_batch(columns, probe.rows)
                schema = profiled.get("schema") or {c: "string" for c in columns}
            except Exception:
                schema = {c: "string" for c in columns}
        elif src_type == "redis":
            try:
                from services.object_store_introspect import (
                    profile_schemaless_source_schema,
                )

                schema = profile_schemaless_source_schema(
                    columns, probe.rows, source_format="redis"
                ) or {c: "string" for c in columns}
            except Exception:
                schema = {c: "string" for c in columns}
        else:
            # Prefer reader-declared native_types (Kafka, thin SaaS typed flatten,
            # Dynamo) over all-string fiction from a failed SQL introspect.
            probe_meta = getattr(probe, "meta", None) or {}
            native = probe_meta.get("native_types") or probe_meta.get("schema") or {}
            if isinstance(native, dict) and native:
                schema = {c: str(native.get(c) or "string") for c in columns}
            elif src_type == "kafka":
                try:
                    from services.file_parser import FileParser

                    sample_recs = [
                        dict(zip(columns, row)) for row in (probe.rows or [])[:50]
                    ]
                    inferred = FileParser.infer_schema(sample_recs) if sample_recs else {}
                    schema = {c: inferred.get(c, "TEXT") for c in columns}
                except Exception:
                    schema = {c: "TEXT" for c in columns}
            else:
                schema = _introspect_table_schema(src_read_type, src_cfg, table, columns)
                if not schema:
                    schema = {c: "string" for c in columns}

    # Built after schema resolution so ordering steps compare under the
    # source column's declared type, never wire text (DD-1).
    if shape_steps and shape_runner is None:
        from .stream_dest_procedure import runner_for_stream_steps

        shape_runner = runner_for_stream_steps(
            shape_steps,
            columns=list(columns),
            table=table,
            effective_sync=effective_sync,
            cursor_field=cursor_source_col or "",
            key_columns=pk_source_cols,
            workspace_id=workspace_id,
            source_types=dict(schema or {}),
            typed_values=_typed_value_source(src_read_type),
        )
    # G-1d: pin every keyed raw row the write consumed so the post-write
    # proof can pin source stability per key instead of inferring drift.
    # The ledger key resolves the same way the engine keys elsewhere:
    # the declared contract PK, then the source catalog PRIMARY KEY, then
    # a fully non-null enforced unique key. Keyless sources keep the
    # proof's multiset fallback (G-1f).
    ledger_key_cols = [c for c in (pk_source_cols or []) if c in columns]
    if not ledger_key_cols:
        ledger_key_cols = [
            c for c in (_cat_keys.get("primary_key_columns") or [])
            if c in columns
        ]
    if not ledger_key_cols:
        ledger_key_cols = list(
            safe_keyset_unique_columns(
                _cat_keys.get("unique_keys") or [],
                list(columns or []),
                _cat_nulls,
            )
            or []
        )
    pin_ledger = (
        _StepRawLedger(
            ledger_key_cols,
            col_ddl={c: str(schema.get(c, "")) for c in columns},
        )
        if shape_runner is not None and ledger_key_cols
        else None
    )

    # ``columns`` stays the source's own column list: it is what every read
    # projects on. Under a recipe the writer sees different columns, so the
    # carriers, the mappings and the DDL are decided from the recipe's output.
    write_columns = list(shape_runner.output_columns or columns) if shape_runner else columns
    column_types = {c: ddl_carrier_type(schema.get(c, "string")) for c in write_columns}
    from .stream_dest_procedure import adopt_stream_column_map

    mappings, inherit_note = adopt_stream_column_map(
        mappings, write_columns, inherited=mappings_inherited
    )
    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in write_columns]
    from services.shape_contract import write_ready_mappings

    # Pending extras and intentional omits stay off INSERT/MERGE (name-addressed).
    mappings = write_ready_mappings(mappings)
    # Schemaless sources: always allow ADD COLUMN when attrs appear mid-transfer.
    if src_type in ("dynamodb", "mongodb", "elasticsearch", "redis", "kafka"):
        backfill_new_fields = True
        probe_meta = getattr(probe, "meta", None) or {}
        for name, lt in (probe_meta.get("native_types") or {}).items():
            if name in schema and schema[name] in {"string", "VARCHAR", "TEXT", "S"}:
                schema[name] = str(lt)
                column_types[name] = ddl_carrier_type(str(lt))
    target_cols, logical_types = resolve_target_columns(
        mappings, column_types, preserve_case=True
    )
    # Write-path dest_types for Gate-8 fingerprint remap parity — including the
    # drift widen the backfill write applies, so the digest is taken against the
    # carrier the rows actually land in.
    from connectors.writer_common import effective_dest_types_under_backfill

    fingerprint_dest_types = effective_dest_types_under_backfill(
        {target_cols[i]: logical_types[i] for i in range(len(target_cols))},
        list(mappings or []),
        backfill=bool(backfill_new_fields),
    )

    total_rows = probe.total_rows
    if total_rows is not None and limit > 0:
        total_rows = min(total_rows, limit)

    checkpoint_service = checkpoint_service or CheckpointService()
    checkpoint = checkpoint or Checkpoint(job_id=job_id or "")
    checkpoint.source_type = src_type
    checkpoint.dest_type = dest_type
    checkpoint.write_mode = write_mode
    checkpoint.conflict_columns = pk_target_cols or []

    # Account for resume state when calculating progress so the UI doesn't see
    # a chunk index greater than the total chunk estimate.
    resume_offset = checkpoint.offset or 0
    chunk_idx = checkpoint.chunk_index or 0
    if chunk_idx == 0 and resume_offset == 0:
        rows_accounted_for = len(probe.rows)
        completed_chunks = 0
    else:
        rows_accounted_for = resume_offset
        completed_chunks = chunk_idx
    remaining_rows = max(0, (total_rows or 0) - rows_accounted_for)
    remaining_chunks = (remaining_rows + chunk_size - 1) // chunk_size if remaining_rows else 0
    if not total_rows and (ddb_cursor or (src_type == "kafka" and probe.rows)):
        remaining_chunks = max(remaining_chunks, 1)
    chunks = max(1, completed_chunks + (1 if chunk_idx == 0 and resume_offset == 0 else 0) + remaining_chunks)
    dest_table = resolve_dest_table(dest_type, destination, table)
    # An existing destination column decides what an instant keeps, not the plan,
    # and this digest must be taken at that granularity or the destination re-read
    # disagrees with it and a correct load fails Gate-8 on two opaque hashes.
    # Kept separate from the remap types on purpose: those drive the write-path
    # quarantine matrix, where a declared precision means "refuse to truncate" —
    # feeding the carrier's precision into it would hold out every row whose
    # fractional seconds the carrier rounds, which is a narrowing to disclose,
    # not a row to reject.
    digest_dest_types = dict(fingerprint_dest_types)
    try:
        from services.dest_physical_types import (
            apply_physical_temporal_precision,
            physical_column_types,
        )
        from services.reconciliation import overlay_physical_dest_types

        # Same type basis as the dest read-back: the carrier the rows land in
        # steers the digest — a pre-existing non-text column (DECLARED DECIMAL
        # over a Mongo string carrier) and a degraded TEXT carrier both follow
        # overlay_physical_dest_types, or write-pass and read-back hash
        # identical rows differently.
        digest_dest_types = overlay_physical_dest_types(
            digest_dest_types,
            physical_column_types(
                dest_type, dest_cfg, table=str(dest_table), columns=list(target_cols)
            ),
        )
        digest_dest_types = apply_physical_temporal_precision(
            digest_dest_types, dest_type, dest_cfg, table=str(dest_table)
        )
    except Exception as exc:
        logger.warning(
            "physical destination temporal precision unavailable for the write-pass "
            "digest: %s",
            exc,
            exc_info=exc,
        )

    ddl_log: list[str] = [
        f"STREAM {src_type}.{table} → {dest_type}.{dest_table} "
        f"({('unknown' if total_rows is None else f'{total_rows:,}')} rows est., "
        f"{chunks} batches, sync={effective_sync})"
    ]
    if incremental and watermark:
        ddl_log.append(f"INCREMENTAL cursor {cursor_source_col} > {watermark}")
    for col in write_columns:
        ddl_log.append(f"{dest_type.upper()} COLUMN {col} {ddl_type(dest_type, schema.get(col, 'string'))}")
    if shape_runner is not None:
        ddl_log.append(
            f"SHAPE recipe {shape_runner.recipe_hash} applied on the read: "
            f"{shape_runner.recipe.describe()}"
        )
    if inherit_note:
        ddl_log.append(inherit_note)

    written = checkpoint.rows_processed or 0
    offset = checkpoint.offset or 0
    # A resumed pass only ever fingerprints the rows it writes, while Gate-8
    # compares against the whole destination — the write-pass digest is not a
    # population checksum for this run.
    resumed_pass = bool(written or offset)
    dest_summary: dict[str, Any] = {}
    # Failed-Gate-8 restore guard: renamed-aside route tables (armed once,
    # inside the held txn) — stamped onto dest_summary at stream end.
    route_backup_state: dict[str, Any] = {}
    # R2: shape-diverted rows are quarantine, not removal — counted here and
    # stamped as rejected_rows so the ledger reads in = out + diverted.
    shape_diverted_total: list[int] = [0]
    # Keys of rows the recipe diverted (unmatched=divert etc.): keyed routed
    # runs delete these from every route table so a key that stopped matching
    # loses its stale copy (P1-B).
    shape_divert_key_tuples: list[tuple] = []
    # Defensive early binding — _persist_shape_diverts is a closure that can
    # run before the pk-audit assignment later in this function
    # (pk_source_cols is already bound unconditionally near line 630).
    pk_audit_cols: list[str] = []
    if copy_decline:
        dest_summary["copy_fast_path"] = "declined"
        dest_summary["copy_decline_reason"] = copy_decline[0]
        dest_summary["copy_decline_reasons"] = copy_decline[:8]
    # Gate-8 append proof needs the cardinality from before the first batch. A
    # resumed run already appended rows, so its count is not a "before" and the
    # delta stays unproven rather than being reported wrong.
    if not (written or offset):
        rows_before = pre_write_rows_before
        _rstep0 = (
            shape_runner.recipe.router_step
            if shape_runner is not None and getattr(shape_runner, "recipe", None) is not None
            else None
        )
        if _rstep0 is not None:
            # A routed run never writes the base table — each route lands in
            # its own table, so the "before" cardinality is the sum over every
            # declared route table. Measuring the base name here leaves the
            # post-write sum of all route tables compared against zero, and
            # append-delta reconciliation then reads the whole dest as one
            # run's growth (RT-4).
            _rtargets0 = dict(_rstep0.options.get("targets") or {})
            _rbase0 = resolve_dest_table(dest_type, destination, _source_name(source))
            _rsum0 = 0
            _rper0: dict[str, int] = {}
            for _rroute in _rstep0.options.get("routes") or []:
                _rname0 = str(_rroute.get("name") or "")
                if not _rname0:
                    continue
                _rtable0 = str(_rtargets0.get(_rname0) or f"{_rbase0}__{_rname0}")
                _rn0 = precount_table(dest_type, dest_cfg, _rtable0)
                if _rn0 is None:
                    # One unreadable route table keeps the sum honest: report
                    # no "before" at all rather than a wrong one.
                    _rsum0 = None
                    break
                _rsum0 += int(_rn0)
                _rper0[_rname0] = int(_rn0)
            rows_before = _rsum0
            if _rper0:
                dest_summary["route_precount"] = dict(_rper0)
                route_backup_state["precount"] = dict(_rper0)
                # RT-6: an append into an occupied route table cannot be
                # proven by the canonical whole-table digest, but it can by
                # the additive multiset digest: post − pre must equal this
                # run's digest. Measure the pre-write additive sum for each
                # occupied route table now (postgres only — other engines
                # stay honestly "not comparable").
                if dest_type == "postgresql" and not is_overwrite_sync(
                    str(effective_sync or "")
                ):
                    from services.reconciliation import (
                        verify_postgres_route_table,
                    )

                    _rpred: dict[str, Any] = {}
                    _rmap0 = {
                        str(_rr.get("name")): str(
                            _rtargets0.get(_rr.get("name"))
                            or f"{_rbase0}__{_rr.get('name')}"
                        )
                        for _rr in (_rstep0.options.get("routes") or [])
                        if _rr.get("name")
                    }
                    for _rname1, _rn1 in _rper0.items():
                        if int(_rn1 or 0) <= 0:
                            continue
                        _ptable = _rmap0.get(_rname1) or f"{_rbase0}__{_rname1}"
                        _pc, _pchk, _padd = verify_postgres_route_table(
                            host=str(dest_cfg.get("host") or ""),
                            port=int(dest_cfg.get("port") or 5432),
                            database=str(dest_cfg.get("database") or ""),
                            username=str(
                                dest_cfg.get("username") or dest_cfg.get("user") or ""
                            ),
                            password=str(dest_cfg.get("password") or ""),
                            schema=str(dest_cfg.get("schema") or "public"),
                            connection_string=str(
                                dest_cfg.get("connection_string") or ""
                            ),
                            ssl=bool(dest_cfg.get("ssl") or dest_cfg.get("sslmode")),
                            table_name=_ptable,
                            # Digest only the columns the run writes — a
                            # customer column (default/generated) must not
                            # enter the additive delta.
                            target_columns=list(
                                shape_runner.recipe.route_output_columns(_rname1)
                            ),
                        )
                        if _padd:
                            _rpred[_rname1] = {
                                "rows": int(_rn1),
                                "additive": _padd,
                            }
                    if _rpred:
                        dest_summary["route_predigest"] = _rpred
        if rows_before is not None:
            dest_summary[PRECOUNT_KEY] = int(rows_before)
            checkpoint.target_rows_before = int(rows_before)
        if not resumed_pass:
            # X-9: same existence stamp as the file path — a precount of 0
            # cannot tell "created this run" from "emptied existing", so the
            # undo needs it to DROP (create-new) vs DELETE. None is not
            # stamped: unknown must never justify a drop. Worker-path
            # failures reach the undo through the engine's job summary, so
            # both stamps ride destination.extra as well.
            try:
                from services.dest_precount import destination_table_exists

                _existed = destination_table_exists(
                    dest_type,
                    dest_cfg,
                    schema=schema_from_cfg(dest_type, dest_cfg),
                    table_name=str(dest_table),
                )
                if _existed is not None:
                    dest_summary["dest_table_existed_before"] = bool(_existed)
                    _dextra = getattr(destination, "extra", None)
                    if isinstance(_dextra, dict):
                        _dextra["dest_table_existed_before"] = bool(_existed)
                if rows_before is not None:
                    _dextra = getattr(destination, "extra", None)
                    if isinstance(_dextra, dict):
                        _dextra["dest_rows_before"] = int(rows_before)
            except Exception:  # noqa: BLE001 - probe failure must not abort the run
                pass
    elif checkpoint.target_rows_before is not None:
        # Resume: restore the first-pass dest-before. A live COUNT now includes
        # rows this job already wrote and is not a pre-write cardinality.
        dest_summary[PRECOUNT_KEY] = int(checkpoint.target_rows_before)
    if resumed_pass:
        # D134 — but it is what a failed *resumed* pass may not undo: rows an
        # earlier pass committed (reported as kept when it was cancelled) are
        # not this pass's batch, so the undo must never clear or drop them.
        try:
            _resume_live = precount_table(dest_type, dest_cfg, str(dest_table))
        except Exception:  # noqa: BLE001 - unknown stays unknown (undo retains)
            _resume_live = None
        dest_summary["resume_rows_before"] = _resume_live
    # A key-addressed destination replaces the value held at the key on every
    # write, so even an append needs the dest-engine census (new keys vs
    # replaced keys) to close conservation — COUNT(*) growth alone reads a
    # correct re-write of the same keys as silent loss.
    dest_key_addressed = dest_is_key_addressed(dest_type, pk_target_cols)
    if dest_key_addressed:
        dest_summary[KEY_ADDRESSED_KEY] = True
    keyed_upsert_scope = write_mode == "upsert" and bool(pk_target_cols)
    # D111 — strict keyed upsert on a populated PG destination: row-copy the
    # pre-run table to __dfbak_<job> so a failed verification restores the
    # exact prior state (DELETE + INSERT FROM backup) instead of leaving the
    # rejected batch in place. Armed only on the first pass — a resume must
    # not snapshot mid-run state — and only where undo is contractually
    # allowed (strict family). Other engines/modes keep the honest
    # "left in place" report.
    if (
        keyed_upsert_scope
        and dest_type == "postgresql"
        and not resumed_pass
        and str(validation_mode or "").strip().lower() in {"strict", "maximum", ""}
        and int(dest_summary.get(PRECOUNT_KEY) or 0) > 0
        and (shape_runner is None or shape_runner.recipe.router_step is None)
    ):
        _route_backups = _arm_strict_keyed_backup(
            destination,
            dest_type=dest_type,
            dest_cfg=dest_cfg,
            dest_table=str(dest_table),
            job_id=str(job_id or ""),
        )
        if _route_backups:
            dest_summary["route_backups"] = dict(_route_backups)
    keyed_census_acc = (
        KeyCensusAccumulator()
        if pk_target_cols and (keyed_upsert_scope or dest_key_addressed)
        else None
    )
    overwrite_keys_acc = begin_overwrite_source_keys(
        effective_sync, pk_target_cols, resumed=resumed_pass
    )
    last_checksum = ""
    # Phase F1 — accumulate fingerprints during the write pass (avoids double source I/O).
    write_pass_fp = FingerprintAccumulator()
    # …but only when this route's Gate-8 will actually use them. A route that
    # re-reads the source for an independent digest (heterogeneous warehouse
    # full refresh) mapped and fingerprinted every cell twice and discarded the
    # write-pass copy. ``partial_write_pass`` can only turn re-read *on*, and a
    # source outside the re-read scan set falls back to the write-pass digest,
    # so both stay eligible here.
    inline_fingerprints_used = not (
        should_reread_source(
            src_type=src_read_type,
            dest_type=dest_type,
            incremental=incremental,
            partial_write_pass=False,
        )
        and src_read_type in REREAD_SCAN_SOURCES
        # M3 — an incremental write pass is batch-sized, so keeping its
        # fingerprints is cheap and they are the fallback digest when the
        # re-read window cannot be bounded (no cursor column / no observed
        # max). Whether they are used is decided at reconcile time.
        and not incremental
    )
    # Restore cumulative quarantine counts on resume — Gate-8 conservation is
    # source - (rejected - coerced_null) - skipped. committed_offset already spans
    # the full population on resume, so these counters must be cumulative too or a
    # correct resumed load that quarantined rows in the first pass fails Gate-8.
    rejected_total = int(getattr(checkpoint, "rejected_rows", 0) or 0) if resumed_pass else 0
    coerced_null_total = (
        int(getattr(checkpoint, "coerced_null_rows", 0) or 0) if resumed_pass else 0
    )
    # STRUCT explode expansion — cumulative for the same reason the quarantine
    # counters are: a resumed pass re-commits earlier pages' exploded children.
    expanded_total = (
        int(getattr(checkpoint, "rows_expanded", 0) or 0) if resumed_pass else 0
    )
    # Rows a declared source filter or an approved recipe removed on the read,
    # counted only for pages that actually committed and cumulative across
    # resumes for the same reason the quarantine counters are.
    removed_on_read_total = (
        int(checkpoint.rows_removed_on_read or 0) if resumed_pass else 0
    )
    # The filter's share of that total, kept apart so proof can say which
    # authority removed a row: the operator's declared scope, or the recipe.
    filtered_on_read_total = (
        int(checkpoint.rows_source_filtered or 0) if resumed_pass else 0
    )
    # Rows the incremental cursor bound excluded after the read (a source that
    # cannot push the bound down). Outside this run's read scope, so they are
    # subtracted from the population instead of being charged to a removal.
    cursor_bounded_total = (
        int(getattr(checkpoint, "rows_cursor_bounded", 0) or 0) if resumed_pass else 0
    )
    # Cumulative held-buffered rows at the last committed page — the page's
    # held contribution is the delta so buffered rows aren't counted as
    # recipe removals (DD-3).
    shape_held_seen = [0]
    # Writer-skipped rows and upsert-collapsed older key images, summed over
    # pages. A page's summary used to replace the last one, and the collapsed
    # images were never counted, so a changelog upsert that kept the latest
    # image of a repeated key failed Gate-8 one row short per repeat (D89).
    page_skips = {"skipped": 0, "collapsed": 0}
    # Strict/maximum FAIL-FAST on coercion errors; balanced quarantines them.
    # Threaded to every writer so the streaming path matches the buffered path.
    stream_error_policy = transform_error_policy_for_validation_mode(validation_mode)
    # Bounded: warnings arrive per batch, so an uncapped list grows with the
    # batch count on large transfers. Truncating at emit time bounded the output
    # but never the memory.
    warning_samples: BoundedStrings = BoundedStrings(cap=MAX_WARNING_SAMPLES)
    ddb_total = probe.total_rows if src_type == "dynamodb" else None
    running_cursor = checkpoint.cursor_value if checkpoint.cursor_value is not None else watermark
    es_search_after = checkpoint.es_search_after or (ddb_cursor if src_type == "elasticsearch" else None)
    redis_scan_state = checkpoint.redis_scan_state or (ddb_cursor if src_type == "redis" else None)
    qdrant_offset = checkpoint.qdrant_offset if src_type == "qdrant" else None
    # Preserve probe continuation tokens when checkpoint has none (fresh job).
    probe_continuation = ddb_cursor
    # Keyset (seek) pagination — Phase F2.
    # Bookmark the full primary key (including composite) so page boundaries
    # cannot skip tied leading-key values. Without a PK, fall back to OFFSET —
    # quadratic and slow, but it cannot skip rows.
    from services.keyset_pagination import (
        KEYSET_CAPABLE_SOURCES,
        decide_keyset_pagination,
        max_keyset_bookmark,
        safe_keyset_unique_columns,
    )

    keyset_pk_cols = [c for c in pk_source_cols if c and c in columns]
    pagination_warning = ""
    _pk_introspect_types = _PK_INTROSPECT_TYPES
    # Property 6 — source catalog for create-new fidelity (any SQL sink that
    # consumes it). Introspected once ahead of the COPY fast path (whose CREATE
    # answers to the same planner); the paged read reuses that catalog here.
    source_schema_catalog: dict[str, Any] | None = None
    _src_schema_types, _src_schema_nulls, _src_keys = _src_rich_catalog
    if src_read_type in _pk_introspect_types:
        try:
            if not keyset_pk_cols:
                keyset_pk_cols = [
                    c for c in (_src_keys.get("primary_key_columns") or []) if c in columns
                ]
            if not keyset_pk_cols:
                keyset_pk_cols = safe_keyset_unique_columns(
                    _src_keys.get("unique_keys") or [],
                    columns,
                    _src_schema_nulls,
                )
                if not keyset_pk_cols and (_src_keys.get("unique_keys") or []):
                    pagination_warning = (
                        "Unique key is nullable, advisory, or expression-based — "
                        "snapshot scan (one SELECT + fetchmany), not keyset. "
                        "Nullable UK seek skips NULL rows."
                    )
        except Exception as exc:
            logger.debug("source schema introspection failed: %s", exc, exc_info=exc)
    # MongoDB ``_id`` is the collection primary key. Without this, a full
    # snapshot fell through to ``.skip(offset)`` (O(n²) + concurrent-insert
    # drift). Debezium/Fivetran seek ``_id``; we do the same.
    if src_type == "mongodb" and columns and "_id" in columns:
        if "_id" not in keyset_pk_cols:
            keyset_pk_cols = ["_id"] + [c for c in keyset_pk_cols if c != "_id"]
    if _src_keys or _src_schema_nulls or _src_schema_types:
        try:
            from services.schema_fidelity import (
                build_catalog_from_introspect,
                catalog_to_payload,
            )

            source_schema_catalog = catalog_to_payload(
                build_catalog_from_introspect(
                    dialect=src_read_type,
                    columns=list(columns or []),
                    column_types=_src_schema_types or dict(schema or {}),
                    nullable=_src_schema_nulls,
                    keys=_src_keys,
                )
            )
        except Exception as exc:
            logger.debug("source schema catalog build failed: %s", exc, exc_info=exc)
    # Ordered composite key for seek (all PK parts). Legacy single + tie-break
    # still work when only one/two columns are available.
    keyset_order_cols = list(keyset_pk_cols)
    keyset_col = checkpoint.cursor_column or (
        keyset_order_cols[0]
        if keyset_order_cols
        else (columns[0] if columns and not incremental else "")
    )
    if incremental and cursor_source_col:
        # An incremental run seeks on the column its watermark was taken on.
        # Seeking a different column silently drops the watermark: the seek
        # branch carries the bookmark *as* the read filter, so a source paged
        # by seek (SQL Server / Oracle) re-read the whole table every run and
        # an append destination died on its own primary key.
        keyset_col = cursor_source_col
        keyset_order_cols = [cursor_source_col] + [
            c for c in keyset_order_cols if c != cursor_source_col
        ]
    keyset_tiebreak = next((c for c in keyset_order_cols if c != keyset_col), "")
    keyset_after = None if sdk_source else checkpoint.cursor_value
    keyset_run_seed = False
    if keyset_after in (None, "") and incremental and keyset_col == cursor_source_col:
        # D60/D98 — a run seeded from the watermark re-reads the whole tie
        # group at the watermark (cursor >= watermark). Page edges advanced
        # inside the run keep the exclusive seek — only this seed is inclusive.
        keyset_after = watermark
        keyset_run_seed = True
    # The bookmark this run started from — the digest window's lower bound.
    # ``keyset_after`` itself advances per committed page, so it must be
    # captured here; the seed is checkpoint.resume or the stored watermark.
    incremental_seed_bookmark = keyset_after if keyset_after not in (None, "") else watermark
    # Captured once: ``checkpoint.cursor_value`` advances as batches commit. A
    # run seeded from the stored watermark (not a resume from a committed page
    # edge) owns the whole tie group at that watermark.
    seeded_from_watermark = bool(
        incremental
        and cursor_source_col
        and watermark not in (None, "")
        and checkpoint.cursor_value in (None, "")
    )
    # D134 — the window Gate-8 digests and the watermark a failed run rolls
    # back to belong to the *job*, not to this pass. A resume after a cancel
    # seeks from its committed page edge, and the route watermark has already
    # advanced with every committed batch; digesting only the resumed tail
    # against a destination holding the whole job's rows read as a checksum
    # mismatch on a correct load. The first pass records the seed it started
    # from ("" = the route had no watermark) and every resume reuses it.
    window_seed_bookmark = incremental_seed_bookmark
    window_seed_inclusive = seeded_from_watermark
    job_start_watermark = watermark
    if incremental and cursor_source_col:
        if resumed_pass and checkpoint.incremental_seed is not None:
            job_start_watermark = checkpoint.incremental_seed or None
            window_seed_bookmark = job_start_watermark
            window_seed_inclusive = job_start_watermark is not None
        elif not resumed_pass:
            checkpoint.incremental_seed = str(watermark or "")
    # D124/D60/D98 — every read seeded from the watermark re-reads its whole
    # tie group, whichever branch pages the source. Peers an earlier run
    # already delivered are dropped against the destination's pre-run rows at
    # the watermark cursor. A keyed append matches by mapped key (a row
    # restamped to the watermark from an older cursor is a new version and
    # still lands — COPY's boundary_append_insert_sql rule). An upsert or an
    # unkeyed append matches full-row content: an upsert still rewrites a peer
    # whose content changed, and a quiet poll stays a no-op, so its covered-
    # content proof still runs. The verify re-read drops the same rows.
    _tie_dedupe = seeded_from_watermark
    _boundary_fps_state: dict[str, Any] = {
        "fps": None,
        "match_cols": [],
        "new_cols": [],
    }

    def _boundary_rows_already_landed(headers: list[str], rows: list[list[str]]) -> set[int]:
        """Indices of re-read tie-group rows the destination already holds."""
        if not (_tie_dedupe and rows):
            return set()
        cidx = header_column_index(headers or [], cursor_source_col)
        if cidx is None:
            return set()
        try:
            from services.keyset_pagination import split_cursor_bookmark
            from services.target_sample import read_target_sample
            from services.value_serializer import present_cell_text

            cur_val, _ = split_cursor_bookmark(str(watermark), has_tiebreak=True)
            boundary_idx = [
                i
                for i, row in enumerate(rows)
                if cidx < len(row) and present_cell_text(row[cidx]) == str(cur_val)
            ]
            if not boundary_idx:
                return set()
            all_cols = list(target_cols or [])
            cursor_dest_col = str(
                next(
                    (
                        m.get("target")
                        for m in mappings
                        if m.get("source") == cursor_source_col
                    ),
                    cursor_source_col,
                )
            )
            match_cols = all_cols
            if (
                not requires_upsert(effective_sync)
                and pk_target_cols
                and all(c in all_cols for c in pk_target_cols)
            ):
                match_cols = [cursor_dest_col] + [
                    c for c in pk_target_cols if c != cursor_dest_col
                ]
            if _boundary_fps_state["fps"] is None:
                # A run that ADDs a column (approved schema evolution) re-reads
                # tie peers whose new column the destination does not have yet.
                # Selecting it failed the read, which fails open: every peer was
                # delivered twice. Compare on the columns the table holds, and
                # keep a peer only when its new columns carry a value.
                landed = read_target_sample(
                    dest_bind_dialect or dest_type,
                    dest_cfg,
                    schema=str(dest_cfg.get("schema") or "public"),
                    table_name=dest_table,
                    columns=None,
                    limit=1,
                    sort_key=cursor_dest_col,
                    key_values=[cur_val],
                )
                live_cols = {str(k).lower() for k in (landed[0] if landed else {})}
                read_cols = [c for c in all_cols if c.lower() in live_cols] or all_cols
                _boundary_fps_state["new_cols"] = [
                    c for c in all_cols if c.lower() not in live_cols
                ] if landed else []
                match_cols = [c for c in match_cols if c in read_cols] or match_cols
                _boundary_fps_state["match_cols"] = match_cols
                dest_rows = read_target_sample(
                    dest_bind_dialect or dest_type,
                    dest_cfg,
                    schema=str(dest_cfg.get("schema") or "public"),
                    table_name=dest_table,
                    columns=read_cols,
                    limit=10_000,
                    sort_key=cursor_dest_col,
                    key_values=[cur_val],
                )
                dest_tups = []
                for dr in dest_rows or []:
                    low = {str(k).lower(): v for k, v in dr.items()}
                    dest_tups.append(tuple(low.get(str(c).lower()) for c in match_cols))
                _boundary_fps_state["fps"] = {
                    fp
                    for _k, fp in row_fingerprints(
                        dest_tups,
                        match_cols,
                        dest_db_type=dest_bind_dialect,
                        dest_types=digest_dest_types,
                    )
                }
            dest_fps = _boundary_fps_state["fps"]
            if not dest_fps:
                return set()
            match_cols = _boundary_fps_state["match_cols"]
            drop = _boundary_append_dedupe(
                list(headers or []),
                [rows[i] for i in boundary_idx],
                dest_fps,
                match_cols=match_cols,
                new_cols=_boundary_fps_state["new_cols"],
                mappings=mappings,
                target_cols=all_cols,
                column_types=column_types,
                stream_error_policy=stream_error_policy,
                digest_dest_types=digest_dest_types,
                dest_bind_dialect=dest_bind_dialect,
            )
            if drop:
                logger.info(
                    "Boundary re-read dedupe dropped %d already-present row(s) "
                    "at watermark %r.",
                    len(drop),
                    str(cur_val),
                )
            return {boundary_idx[i] for i in drop}
        except Exception as exc:  # fail open — dedupe must never lose a row
            logger.warning("Boundary dedupe skipped (fail-open): %s", exc)
            return set()

    decision = decide_keyset_pagination(
        src_type=src_read_type,
        keyset_order_cols=keyset_order_cols,
        keyset_col=keyset_col,
        keyset_tiebreak=keyset_tiebreak,
        incremental=bool(incremental),
        offset=offset,
        chunk_index=chunk_idx,
        cursor_after=keyset_after,
        snapshot_scan=bool(src_scan),
        cursor_is_unique=_cursor_is_unique,
        callable_source=bool(is_callable_source(source) or is_callable_source(src_cfg)),
    )
    use_keyset = decision.use_keyset
    if _filtered_scan_reason and use_keyset:
        raise RuntimeError(
            "pagination owners disagree: the read opened a filtered snapshot scan "
            "but the keyset decision would seek on "
            f"{keyset_order_cols!r} — refusing rather than risk skipped rows"
        )
    keyset_order_cols = decision.order_cols
    _resume_scan_aligned = False
    if decision.resume_fallback:
        if src_scan.get("started"):
            logger.warning(
                "Resume checkpoint offset=%s chunk_index=%s has no keyset cursor_value — "
                "continuing the snapshot scan past the committed prefix (not OFFSET pages).",
                offset,
                chunk_idx,
            )
        else:
            logger.warning(
                "Resume checkpoint offset=%s chunk_index=%s has no keyset cursor_value — "
                "falling back to OFFSET pagination to avoid re-reading committed rows.",
                offset,
                chunk_idx,
            )
    pagination_mode = decision.pagination_mode
    if use_keyset and src_scan.get("started"):
        from connectors.sql_snapshot_scan import (
            close_table_scan,
            scan_order_supports_seek,
        )

        if scan_order_supports_seek(src_scan.get("order_cols"), keyset_order_cols):
            # First page already landed from the snapshot scan; later pages seek.
            close_table_scan(src_scan)
        else:
            # The scan's ORDER BY is not the seek order (or is unpublished):
            # seeking past its first page would skip every row the scan order
            # placed before the page edge. Keep paging the held snapshot.
            logger.warning(
                "Snapshot scan on %s.%s is ordered by %r but the keyset seek would "
                "run on %r — continuing the held scan instead of seeking.",
                src_type,
                table,
                src_scan.get("order_cols"),
                keyset_order_cols,
            )
            use_keyset = False
            pagination_mode = "scan"
    elif (
        _is_resume
        and not use_keyset
        and src_scan.get("started")
        and offset > 0
    ):
        from connectors.sql_snapshot_scan import align_snapshot_resume

        def _resume_scan_next():
            nxt, _extra = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    offset,
                    _batch_limit(offset),
                    database=src_db,
                    known_total_rows=total_rows,
                    scan_state=src_scan,
                )
            )
            return nxt

        probe = align_snapshot_resume(probe, offset, _resume_scan_next)
        _resume_scan_aligned = True
    # Phase F3 — PostgreSQL COPY TO STDOUT bulk export (full refresh, no filter).
    bulk_export_iter = None
    try:
        from connectors.bulk_export import (
            bulk_export_enabled,
            bulk_export_forced,
            bulk_export_implemented,
            iter_postgresql_copy_batches,
        )

        if (
            bulk_export_enabled()
            and bulk_export_implemented(src_read_type)
            and not incremental
            and not _is_resume
            and not source_filter
            and not limit
            and not is_callable_source(source)
            and not is_callable_source(src_cfg)
        ):
            try:
                bulk_export_iter = iter_postgresql_copy_batches(
                    host=src_cfg.get("host", ""),
                    port=int(src_cfg.get("port") or 5432),
                    database=src_cfg.get("database", ""),
                    username=src_cfg.get("username", ""),
                    password=src_cfg.get("password", ""),
                    schema=src_cfg.get("schema", "public"),
                    connection_string=src_cfg.get("connection_string", ""),
                    ssl=bool(src_cfg.get("ssl", False)),
                    table=table,
                    columns=columns,
                    batch_rows=max(1, int(chunk_size)),
                )
                pagination_mode = "bulk_copy"
                use_keyset = False
                logger.info(
                    "Bulk export (COPY TO STDOUT) enabled for %s.%s — skipping OFFSET/keyset pages",
                    src_type,
                    table,
                )
            except Exception as exc:
                if bulk_export_forced():
                    raise
                logger.warning(
                    "Bulk export unavailable for %s.%s (%s) — falling back to %s",
                    src_type,
                    table,
                    exc,
                    "keyset" if use_keyset else "offset",
                )
                bulk_export_iter = None
        elif (
            bulk_export_forced()
            and src_read_type in ("snowflake", "bigquery")
            and not incremental
        ):
            raise NotImplementedError(
                f"Bulk export forced for {src_type} but unload/Storage Read is not "
                "wired in this build — unset DATAFLOW_BULK_EXPORT or wait for F3 complete."
            )
    except ImportError:
        bulk_export_iter = None

    if keyset_col and not use_keyset and src_read_type in KEYSET_CAPABLE_SOURCES:
        logger.info(
            "Keyset pagination disabled for %s.%s — no unique bookmark (declare a "
            "primary_key on the stream contract to re-enable seek reads). Falling "
            "back to %s.",
            src_type,
            table,
            "snapshot scan" if src_scan else "OFFSET pagination",
        )
    elif not use_keyset:
        logger.info(
            "%s pagination for %s.%s — no keyset-capable unique bookmark "
            "(quadratic cost on large tables when OFFSET).",
            "Snapshot-scan" if src_scan else "OFFSET",
            src_type,
            table,
        )
    ddb_cursor = checkpoint.dynamodb_cursor or (probe_continuation if src_type == "dynamodb" else None)
    kafka_cursor = checkpoint.kafka_cursor or (probe_continuation if src_type == "kafka" else None)

    retry = retry_budget or RetryBudget()

    # Separate fetch cursors from committed checkpoint cursors so we can read
    # ahead in parallel while only persisting durable offsets after a batch is
    # successfully written.
    fetch_cursor = running_cursor
    # True until the first page read from the stored watermark has been taken:
    # that seed read re-reads the tie group (``>=``), every later page seeks
    # strictly past the edge this run advanced to.
    fetch_from_seed = seeded_from_watermark
    # `fetch_offset` doubles as the source-row budget counter (`limit` /
    # `total_rows`). A keyset resume seeks by bookmark, and that bookmark sits at
    # or behind the committed offset, so the rows it re-reads would be charged to
    # the budget twice and the run would stop short of the tail — silent loss the
    # operator only sees as a reconciliation mismatch. Keyset pages terminate
    # naturally on a short/empty page, so the budget restarts at zero instead.
    keyset_resume = bool(use_keyset and keyset_after and offset > 0)
    fetch_offset = 0 if keyset_resume else offset
    committed_offset = offset
    population_resume_offset = int(offset or 0)
    # Keyset bookmark of the last *committed* batch. `keyset_after` belongs to
    # the reader, which runs batches ahead of the writer; persisting it would
    # resume past rows that were read but never written.
    committed_keyset = checkpoint.cursor_value or ""

    def _fetch_next_batch(last_batch):
        """Timed wrapper — source read time is the first thing to rule out."""
        started = time.perf_counter()
        try:
            result = _fetch_source_batch(last_batch)
        except BaseException:
            phase_profile.add(PHASE_READ, time.perf_counter() - started)
            raise
        phase_profile.add(
            PHASE_READ,
            time.perf_counter() - started,
            rows=len(getattr(result, "rows", None) or []),
        )
        return result

    def _fetch_source_batch(last_batch):
        nonlocal ddb_cursor, es_search_after, redis_scan_state, keyset_after, kafka_cursor, qdrant_offset, keyset_run_seed
        if bulk_export_iter is not None:
            try:
                return next(bulk_export_iter)
            except StopIteration:
                return None
        if limit > 0 and fetch_offset >= limit:
            return None
        if (
            total_rows is not None
            and fetch_offset >= total_rows
            and src_type != "dynamodb"
            and not sdk_source
        ):
            return None
        if sdk_source:
            last_meta = (
                last_batch.meta
                if last_batch is not None and isinstance(last_batch.meta, dict)
                else {}
            )
            if last_batch is not None and last_meta.get("sdk_done"):
                return None
            sdk_state_for_read = (
                last_meta.get("sdk_state") if last_batch is not None else initial_sdk_state
            )
            batch, _ = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    _batch_limit(fetch_offset),
                    database=src_db,
                    sdk_state=sdk_state_for_read or None,
                )
            )
            return batch
        # A zero-row page is drained. Short *non-empty* pages may be driver
        # vectors (DuckDB 2048) and must continue — that is ``page_may_be_partial``
        # below. An empty fetchmany after ``close_table_scan`` used to fall
        # through and reopen the snapshot from row 0 because ``while batch:``
        # is true for any ReadBatch object, so SQL Server (no keyset bookmark)
        # checkpoint-looped forever on a 2-row table.
        if last_batch is not None and _raw_page_rows(last_batch) == 0:
            return None
        batch_limit = _batch_limit(fetch_offset)
        # A short page only proves the source is drained when the reader owned
        # the page size. A held snapshot scan hands back whatever ``fetchmany``
        # buffered (DuckDB returns one vector), and a catalog count that still
        # exceeds the rows read says more remain — stopping there drops rows
        # while every count and checksum agrees on the truncated set.
        page_may_be_partial = bool(
            last_batch is not None and _raw_page_rows(last_batch)
        ) and (
            bool(src_scan.get("started"))
            or (total_rows is not None and fetch_offset < total_rows)
        )
        if (
            last_batch is not None
            and _raw_page_rows(last_batch) < chunk_size
            and not page_may_be_partial
        ):
            if (
                src_read_type
                in (
                    "postgresql",
                    "redshift",
                    "mysql",
                    "snowflake",
                    "mongodb",
                    "bigquery",
                    "sqlserver",
                    "oracle",
                    "sqlite",
                    "generic_sql",
                )
                and (incremental or use_keyset)
            ) or src_read_type in ("elasticsearch", "redis", "qdrant"):
                return None
        if src_type == "dynamodb":
            if not ddb_cursor:
                return None
            batch, ddb_cursor = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    dynamodb_cursor=ddb_cursor,
                    dynamodb_total=ddb_total,
                )
            )
            return batch
        if src_type == "kafka":
            if last_batch is not None and (
                kafka_cursor is None or kafka_cursor.get("exhausted")
            ):
                return None
            batch, kafka_cursor = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    kafka_cursor=kafka_cursor,
                    known_total_rows=total_rows,
                )
            )
            return batch
        if src_type == "qdrant":
            if qdrant_offset is None and last_batch is not None:
                return None
            batch, qdrant_offset = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    qdrant_offset=qdrant_offset,
                    known_total_rows=total_rows,
                )
            )
            return batch
        elif incremental and cursor_source_col and (
            is_callable_source(src_cfg)
        ):
            # Spool is not cursor-sorted. Filter against the *run* watermark
            # then OFFSET — never the advancing page max (that drops rows).
            batch, _ = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    known_total_rows=total_rows,
                    **_cursor_read_args(watermark, run_seed=True),
                )
            )
            return batch
        elif (
            incremental
            and cursor_source_col
            and not _filtered_scan_reason
            and source_bounds_cursor_reads(src_read_type)
        ):
            batch, _ = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    known_total_rows=total_rows,
                    **_cursor_read_args(fetch_cursor, run_seed=fetch_from_seed),
                )
            )
            return batch
        elif use_keyset:
            if last_batch is not None and _raw_page_rows(last_batch):
                # Advance on the max of the page across the full composite key
                # (Phase F2) — max_keyset_bookmark does not assume page sort. A
                # shaped page carries the bookmark of the rows it was read as,
                # taken before the recipe could drop the highest one.
                advanced = _raw_page_keyset(last_batch)
                if not advanced:
                    order_cols = keyset_order_cols or (
                        [keyset_col] + ([keyset_tiebreak] if keyset_tiebreak else [])
                    )
                    advanced = max_keyset_bookmark(
                        last_batch.rows, last_batch.headers, order_cols
                    )
                    if advanced is None and keyset_col in (last_batch.headers or []):
                        advanced = max_cursor_value(
                            last_batch.rows,
                            last_batch.headers,
                            keyset_col,
                            keyset_tiebreak or None,
                        )
                keyset_after = advanced or keyset_after
                if advanced:
                    keyset_run_seed = False
            _key_cols = keyset_order_cols or (
                [keyset_col] + ([keyset_tiebreak] if keyset_tiebreak else [])
            )
            batch, extra = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    cursor_column=keyset_col or (_key_cols[0] if _key_cols else ""),
                    cursor_after=keyset_after,
                    cursor_inclusive=keyset_run_seed,
                    cursor_type=column_types.get(
                        keyset_col or (_key_cols[0] if _key_cols else ""), "VARCHAR"
                    ),
                    known_total_rows=total_rows,
                    cursor_primary_key=(
                        keyset_tiebreak or None if len(_key_cols) <= 2 else None
                    ),
                    cursor_key_columns=_key_cols if len(_key_cols) >= 2 else None,
                )
            )
            if src_type == "elasticsearch":
                es_search_after = extra
            elif src_type == "redis":
                redis_scan_state = extra
            return batch
        elif src_type == "elasticsearch":
            batch, es_search_after = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    known_total_rows=total_rows,
                    es_search_after=es_search_after,
                )
            )
            return batch
        elif src_type == "qdrant":
            if qdrant_offset is None and last_batch is not None:
                return None
            batch, qdrant_offset = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    known_total_rows=total_rows,
                    qdrant_offset=qdrant_offset,
                )
            )
            return batch
        elif src_type == "redis":
            batch, redis_scan_state = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    known_total_rows=total_rows,
                    redis_scan_state=redis_scan_state,
                )
            )
            return batch
        elif total_rows is not None and fetch_offset >= total_rows and not sdk_source:
            return None
        elif _filtered_scan_reason:
            # One held scan bound to the *run* watermark; the page max never
            # becomes the predicate, so rows tied at a page edge are kept.
            batch, _ = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    known_total_rows=total_rows,
                    **_scan_kw,
                )
            )
            return batch
        else:
            batch, extra = _unwrap_read(
                _read_batch(
                    src_read_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    known_total_rows=total_rows,
                    **(
                        {"scan_state": src_scan}
                        if src_read_type in _SNAPSHOT_SCAN_SOURCES and src_scan
                        else {}
                    ),
                )
            )
            if src_type == "elasticsearch":
                es_search_after = extra
            elif src_type == "redis":
                redis_scan_state = extra
            return batch

    shape_inputs = (
        frozenset(shape_runner.recipe.input_columns) if shape_runner is not None else frozenset()
    )

    def _rows_after_watermark_matrix(
        headers: list[str], rows: list[list[str]]
    ) -> tuple[list[list[str]], int]:
        """Bound a read page to this run's window past the watermark.

        The comparison goes through the same typed comparator the SQL readers
        bound with, so a Redis or Dynamo delta means the same thing the seed's
        ``WHERE cursor >= :watermark`` means — the tie group at the watermark
        included (D124). Returns the kept rows and how many carried no cursor
        value at all.
        """
        cursor_idx = header_column_index(headers, cursor_source_col)
        if cursor_idx is None:
            # The cursor column is not on the page: nothing here can be proven
            # historical, and dropping the page would lose rows.
            return list(rows), 0
        kept: list[list[str]] = []
        unbounded = 0
        for row in rows:
            verdict = row_in_run_window(
                {cursor_source_col: row[cursor_idx]}, cursor_source_col, watermark
            )
            if verdict is None:
                unbounded += 1
                continue
            if verdict:
                kept.append(row)
        return kept, unbounded

    # ``fk_orphan_guard`` was built ahead of the COPY decision (one guard per
    # stream); the row path below applies it to every page.

    def _filter_batch(batch):
        """Source filter, then the approved recipe — once per page.

        The page carries its own pre-filter, pre-shape row count: the source
        OFFSET must advance by the rows the source handed over, not by the rows
        that survived a filter or a recipe, or every dropped row shifts the next
        page and the tail of the table is never read.

        That mark is also what makes this idempotent, and why both rewrites are
        recorded by one mark: they happen together, in this call, so the mark
        means "this page has already been rewritten". The first page passes
        through here twice (its DDL is committed before any worker starts), and a
        page already shaped is returned untouched: shaping it again would round a
        rounded value and count the page's effects into the run's ledger twice.
        """
        if not batch:
            return batch
        if _raw_page_marked(batch):
            return batch
        raw_rows = len(batch.rows or [])
        if raw_rows and (
            source_filter
            or shape_runner is not None
            or client_cursor_bound
            or _tie_dedupe
            or fk_orphan_guard is not None
        ):
            # Pagination bookmarks belong to the rows the source handed over. A
            # recipe that drops the page's highest key would otherwise bookmark a
            # lower one and the next read would hand those rows over again.
            order_cols = keyset_order_cols or (
                [keyset_col] + ([keyset_tiebreak] if keyset_tiebreak else [])
            )
            keyset_mark = max_keyset_bookmark(batch.rows, batch.headers, order_cols)
            if keyset_mark is None and header_column_index(
                batch.headers or [], keyset_col
            ) is not None:
                keyset_mark = max_cursor_value(
                    batch.rows, batch.headers, keyset_col, keyset_tiebreak or None
                )
            cursor_mark = ""
            if incremental and cursor_source_col:
                cursor_mark = (
                    max_cursor_value(
                        batch.rows,
                        batch.headers,
                        cursor_source_col,
                        cursor_pk_source or None,
                    )
                    or ""
                )
            if not _mark_raw_page(batch, raw_rows, cursor_mark, keyset_mark or ""):
                raise ValueError(
                    "this source's read page cannot record the page as it was read, "
                    "so a filter or a transform recipe cannot be proven to be applied "
                    "to it exactly once, nor the next page proven to resume where "
                    "this one ended — refusing rather than risk skipping source rows"
                )
        if client_cursor_bound and batch.rows:
            # The source could not push the cursor bound into its read call, so
            # it handed the whole keyspace over. Honouring the bound here is
            # what makes "incremental" mean incremental for it: without this the
            # second run re-offered every row and an append destination held 2N.
            before = len(batch.rows)
            kept, unbounded = _rows_after_watermark_matrix(
                batch.headers, batch.rows
            )
            if unbounded:
                # A row with no cursor value cannot be proven new or already at
                # rest. Skipping it loses data and sending it duplicates data,
                # so the run refuses instead of choosing silently.
                raise ValueError(
                    f"{unbounded} row(s) carry no value for cursor "
                    f"'{cursor_source_col}' — an incremental read of "
                    f"{src_type} cannot prove whether they already landed. "
                    "Fill the cursor column at the source, or run this sync as "
                    "full refresh."
                )
            batch.rows = kept
            excluded = before - len(kept)
            if excluded:
                try:
                    batch.raw_page_cursor_bounded = excluded
                except AttributeError as exc:
                    raise ValueError(
                        "this source's read page cannot record how many rows the "
                        "incremental cursor bound excluded, so the run's source "
                        "population cannot be stated — refusing rather than "
                        "counting rows this run was never asked to move"
                    ) from exc
        if _tie_dedupe and batch.rows:
            # Tie peers an earlier run already delivered are outside this
            # run's scope, exactly like rows below the watermark.
            landed = _boundary_rows_already_landed(batch.headers, batch.rows)
            if landed:
                batch.rows = [r for i, r in enumerate(batch.rows) if i not in landed]
                try:
                    batch.raw_page_cursor_bounded = (
                        int(getattr(batch, "raw_page_cursor_bounded", 0) or 0)
                        + len(landed)
                    )
                except AttributeError as exc:
                    raise ValueError(
                        "this source's read page cannot record the re-read tie "
                        "rows it dropped, so the run's source population cannot "
                        "be stated — refusing rather than miscounting them"
                    ) from exc
        if source_filter and batch.rows:
            before = len(batch.rows)
            batch.rows = apply_row_filter_to_matrix(
                batch.headers, batch.rows, source_filter
            )
            removed = before - len(batch.rows)
            try:
                batch.raw_page_filtered = removed
            except AttributeError as exc:
                # Without this the removal would be charged to the recipe, and
                # proof would name the wrong authority for a missing row.
                raise ValueError(
                    "this source's read page cannot record how many rows the "
                    "declared source filter removed, so proof cannot say whether "
                    "the filter or the transform recipe removed a row — refusing "
                    "rather than attributing it to the wrong authority"
                ) from exc
        if fk_orphan_guard is not None and batch.rows:
            # fk__pg-maria/fk__pg-pg: a child row whose parent is absent at the
            # destination must quarantine, never land. The guard anti-joins
            # this page's FK tuples against the destination parent table.
            kept_rows, orphan_details = fk_orphan_guard.partition(
                list(batch.headers or []), list(batch.rows)
            )
            if orphan_details and _fk_orphans_fail_closed(validation_mode):
                raise ValueError(_fk_orphan_violation_message(orphan_details))
            if orphan_details:
                batch.rows = kept_rows
                try:
                    batch.fk_orphan_details = orphan_details
                except AttributeError as exc:
                    raise ValueError(
                        "this source's read page cannot carry quarantined "
                        "orphan-FK details, so the run cannot prove the rows "
                        "were held out — refusing rather than dropping them"
                    ) from exc
        if shape_runner is not None and batch.rows is not None:
            headers = list(batch.headers or [])
            if pin_ledger is not None:
                pin_ledger.add_page(headers, batch.rows)
            _shape_inputs_folded = {
                str(s).casefold() for s in (shape_inputs or [])
            }
            unknown = [
                h
                for h in headers
                if h and str(h).casefold() not in _shape_inputs_folded
            ]
            if unknown:
                # A column the recipe was never validated against cannot be
                # shaped, and writing it raw beside shaped columns would put two
                # different rules in one row. Fail closed, naming it.
                raise ValueError(
                    f"source column(s) {', '.join(sorted(unknown))} appeared on the read "
                    f"but the approved transform recipe ({shape_runner.recipe_hash}) was "
                    "validated against a different column set — re-validate the recipe "
                    "against the source as it is now"
                )
            batch.headers, batch.rows = shape_runner.matrix(headers, batch.rows)
            batch.raw_page_diverted = _persist_shape_diverts()
            # Rows still held by a buffering step are not removals — the
            # ledger subtracts them via this marker (DD-3).
            batch.raw_page_held = int(shape_runner.effect.rows_buffered or 0)
        return batch

    def _persist_shape_diverts() -> int:
        """Persist this page's shape-diverted rows to the quarantine DLQ.

        A row a recipe step diverted (on_error=divert, divert_rows, lookup
        unmatched=divert) is excluded from the destination — quarantine is what
        makes it recoverable. Persisting per page keeps the pending list
        bounded and keeps quarantine durable across a mid-stream abort.
        """
        if shape_runner is None:
            return 0
        diverted = shape_runner.take_diverted()
        shape_diverted_total[0] += len(diverted or [])
        # Keyed routed runs purge diverted keys from every route table —
        # a row that stopped matching any route must lose its stale copy
        # (P1-B). Records carry the shaped row; try the contract pk names
        # then the identity source names.
        for _d in diverted or []:
            _rec = getattr(_d, "record", None) or {}
            _key = None
            for _names in (pk_source_cols, pk_audit_cols):
                if _names and all(n in _rec for n in _names):
                    _key = tuple(_rec[n] for n in _names)
                    break
            if _key is not None and all(v is not None for v in _key):
                shape_divert_key_tuples.append(_key)
        if not diverted or not job_id:
            return len(diverted or [])
        from services.quarantine_dlq import persist_rejected_rows

        details = [
            {
                "row": d.record,
                "reason": (
                    f"transform divert — step {d.step} '{d.op}': {d.reason}"
                ),
                "failure_reason": d.reason,
                "transform_attempted": f"shape step {d.step} '{d.op}'",
                "shape_step": d.step,
                "shape_op": d.op,
                "source_row_index": d.row_index + 1,
                **(
                    {
                        "dedupe_key": list(d.dedupe_key),
                        "winner_row": d.winner_row_index + 1,
                    }
                    if getattr(d, "dedupe_key", None) is not None
                    and getattr(d, "winner_row_index", None) is not None
                    else {}
                ),
            }
            for d in diverted
        ]
        persist_rejected_rows(
            job_id=str(job_id),
            rejected_details=details,
            source="shape_divert",
            connector=str(
                getattr(destination, "format", None)
                or getattr(destination, "kind", None)
                or ""
            ),
        )
        return len(diverted)

    def _page_cursor_max(batch) -> str:
        """The page's highest cursor value, as the source handed it over.

        A row the recipe removed was still read, so the watermark must move past
        it — otherwise the next incremental run reads it again to remove it again,
        for ever.
        """
        return _raw_page_cursor(batch) or max_cursor_value(
            batch.rows, batch.headers, cursor_source_col, cursor_pk_source or None
        )

    if use_keyset and (offset > 0 or chunk_idx > 0):
        # Bookmark seek — the probe was schema-only (or the first page of a
        # scan we closed). Fetch the first uncommitted page from the key.
        first_page = _fetch_next_batch(None)
    elif _resume_scan_aligned:
        first_page = probe
    elif offset > 0 or chunk_idx > 0:
        first_page = _fetch_next_batch(None)
    else:
        first_page = probe
    batch = _filter_batch(first_page)
    batch_quality_enabled = validation_mode in ("strict", "maximum")
    drift_detector = BatchDriftDetector()
    # Per-route drift detectors — routed runs audit each split separately.
    route_drift_detectors: dict[str, Any] = {}
    # Identity key for duplicate audit — NEVER reuse the CDC cursor column.
    # Cursor fields (updated_at, id) are not the uniqueness contract; Mongo→Redis
    # must audit `_id`, not a business `id` that legitimately repeats.
    pk_source_col = ""
    if pk_target_cols and mappings:
        target_to_source = {m.get("target", "").lower(): m.get("source", "") for m in mappings if m.get("target")}
        for pk in pk_target_cols:
            src = target_to_source.get(pk.lower())
            if src:
                pk_source_col = src
                break
    if not pk_source_col:
        try:
            from services.primary_key import resolve_primary_key_source

            pk_source_col = (
                resolve_primary_key_source(
                    mappings,
                    list(columns or (batch.headers if batch else [])),
                    dest_type,
                    validation_mode=validation_mode,
                    purpose="uniqueness",
                )
                or ""
            )
        except Exception:
            pk_source_col = ""
    # Prefer live batch headers when available for the first chunk.
    if batch and getattr(batch, "headers", None) and pk_source_col:
        lower = {h.lower(): h for h in batch.headers}
        pk_source_col = lower.get(pk_source_col.lower(), pk_source_col)
    # Full composite identity for the batch audit (QA ACC-03). Auditing the
    # first component alone hard-blocked every (region, id) upsert at Execute.
    from services.primary_key import identity_source_columns

    pk_audit_cols = identity_source_columns(pk_target_cols, mappings)

    # Phase F6 — default per-transfer parallelism raised from 2 → min(4, CPUs).
    max_workers = int(
        getenv_brand("PARALLEL_WORKERS", str(min(4, os.cpu_count() or 1)))
    )
    # SQLite handles concurrency poorly with a single shared file, so keep it sequential.
    # Snowflake reuses one connection for the job — must stay serial.
    # Iceberg catalog commits are snapshot-isolated; concurrent writers conflict.
    # Public TCP proxies drop under concurrent bulk writers — force one connection.
    # PostgreSQL overwrite is one transaction: TRUNCATE and every insert
    # commit together (R4-D06). A worker pool opens a connection per batch,
    # and each of those connections truncates then commits its own slice, so
    # the last batch is the only one a reader still sees. Wide rows shrink
    # the chunk to one row and that is how a 3-row LOB copy landed as 1.
    pg_overwrite_one_txn = dest_type in ("postgresql", "redshift") and bool(
        (getattr(destination, "extra", None) or {}).get("pg_overwrite_in_transaction")
    )
    # A routed run writes several tables — on PostgreSQL it must run on the
    # single shared connection so one held transaction covers every route
    # (commit all or roll back all, RT-5). With a worker pool each batch gets
    # its own connection and each route commits independently, leaving
    # half-written route sets on failure.
    routed_pg = (
        dest_type in ("postgresql", "redshift")
        and shape_runner is not None
        and getattr(shape_runner, "recipe", None) is not None
        and shape_runner.recipe.router_step is not None
    )
    if (
        dest_type in ("sqlite", "snowflake", "iceberg")
        or pg_overwrite_one_txn
        or routed_pg
    ):
        max_workers = 1
    else:
        try:
            from connectors.write_resilience import is_public_proxy_host
        except ImportError:
            is_public_proxy_host = lambda _h: False  # noqa: E731
        proxy_host = str(dest_cfg.get("host") or "")
        proxy_cs = str(
            dest_cfg.get("connection_string")
            or dest_cfg.get("uri")
            or dest_cfg.get("url")
            or ""
        )
        if is_public_proxy_host(proxy_host) or is_public_proxy_host(proxy_cs):
            max_workers = 1

    # Shared destination connections for the life of this stream job (open once).
    sf_conn_state: dict[str, Any] = {"conn": None, "session_ready": False}
    pg_conn_state: dict[str, Any] = {"conn": None}
    # Per-route write-pass fingerprint accumulators — the value-level digest
    # each route table should show on read-back (Gate-8 compares it against
    # verify_target's checksum per route, tag column never written).
    route_fp_acc: dict[str, Any] = {}
    route_fp_cols: dict[str, list[str]] = {}
    batches_completed = 0
    # Only the distinct set matters (the summary asks "did any batch COPY?"),
    # so dedupe instead of appending one entry per batch.
    load_methods_seen: BoundedStrings = BoundedStrings(cap=MAX_LOAD_METHODS)
    # Bounded source sample for Gate-8 after streaming (records=[] at reconcile).
    reconcile_sample: list[dict[str, Any]] = []
    _RECONCILE_SAMPLE_CAP = 50

    def _ensure_snowflake_conn() -> Any:
        if sf_conn_state["conn"] is not None:
            return sf_conn_state["conn"]
        from connectors.snowflake_conn import get_connection, normalize_account

        from services.connector_auth import engine_login_role

        pem = str(dest_cfg.get("private_key") or "")
        sf_conn_state["conn"] = get_connection(
            account=normalize_account(dest_cfg.get("host", "")),
            username=dest_cfg.get("username", ""),
            password=dest_cfg.get("password", ""),
            database=dest_cfg.get("database", ""),
            schema=dest_cfg.get("schema", "PUBLIC"),
            warehouse=dest_cfg.get("warehouse", ""),
            connection_string=dest_cfg.get("connection_string", ""),
            role=engine_login_role(dest_cfg.get("auth_role"), dest_cfg.get("role")),
            private_key=pem,
            private_key_passphrase=str(dest_cfg.get("password") or "") if pem else "",
        )
        return sf_conn_state["conn"]

    def _ensure_pg_conn() -> Any:
        existing = pg_conn_state.get("conn")
        if existing is not None:
            try:
                if getattr(existing, "closed", 0) == 0:
                    return existing
            except Exception:
                pass
            pg_conn_state["conn"] = None
        from connectors.postgresql_conn import get_connection as pg_get_connection

        pg_port = int(
            dest_cfg.get("port")
            or (5439 if dest_type == "redshift" else 5432)
        )
        conn = pg_get_connection(
            host=dest_cfg.get("host", ""),
            port=pg_port,
            database=dest_cfg.get("database", ""),
            username=dest_cfg.get("username", ""),
            password=dest_cfg.get("password", ""),
            connection_string=dest_cfg.get("connection_string", ""),
            ssl=dest_cfg.get("ssl", False),
        )
        try:
            conn.autocommit = False
        except Exception:
            pass
        pg_conn_state["conn"] = conn
        return conn

    # The destination DDL rides on the first page that reaches the writer, which
    # is not necessarily the first page read: a source filter or a recipe can
    # empty page one entirely. Only the synchronous prologue below reads this as
    # False, so no worker thread can race another into a CREATE TABLE.
    ddl_state = {"created": False}

    def _process_db_chunk(idx: int, batch: Any) -> dict[str, Any]:
        """Timed wrapper around the transform + destination write for one chunk.

        Runs on the worker pool, so these totals overlap the reader's — that is
        what ``overlap_factor`` in the profile snapshot reports.
        """
        rows = len(getattr(batch, "rows", None) or [])
        started = time.perf_counter()
        try:
            return _process_db_chunk_inner(idx, batch)
        finally:
            phase_profile.add(
                PHASE_TRANSFORM_WRITE, time.perf_counter() - started, rows=rows
            )

    def _fk_held_out(batch: Any) -> list[dict[str, Any]]:
        return list(getattr(batch, "fk_orphan_details", None) or [])

    def _kafka_meta_rows(batch: Any) -> tuple[list[dict[str, Any]], int]:
        """Broker-side rejects (poison messages) and tombstone count the Kafka
        reader carried on ``batch.meta`` — folded into this page's rejected
        ledger so poison lands in the DLQ, never as an all-NULL row."""
        meta = getattr(batch, "meta", None)
        if not isinstance(meta, dict):
            return [], 0
        return (
            list(meta.get("rejected_rows") or []),
            int(meta.get("kafka_tombstones_seen") or 0),
        )

    def _chunks_for_page(idx: int, batch: Any) -> int:
        """The batch count the writer is told while writing page ``idx``.

        A Kafka topic has no row count to size the run by, so the estimate
        can undershoot; a staged overwrite publishes on the last page and
        would never publish (or publish early). The reader flags the page that
        reaches the run's frozen log end, which is the last one.
        """
        if src_type != "kafka":
            return chunks
        meta = getattr(batch, "meta", None)
        if isinstance(meta, dict) and meta.get("final_page"):
            return idx
        return max(chunks, idx + 1)

    def _process_db_chunk_inner(idx: int, batch: Any) -> dict[str, Any]:
        nonlocal rejected_total
        batch_sdk_state = None
        if sdk_source and isinstance(getattr(batch, "meta", None), dict):
            batch_sdk_state = batch.meta.get("sdk_state", "")
        kafka_rejects, kafka_tombstones = _kafka_meta_rows(batch)
        if kafka_rejects or kafka_tombstones:
            dest_summary["rejected_details"] = list(
                dest_summary.get("rejected_details") or []
            ) + kafka_rejects
            dest_summary["rejected_rows"] = int(
                dest_summary.get("rejected_rows") or 0
            ) + len(kafka_rejects)
            dest_summary["kafka_tombstones_seen"] = int(
                dest_summary.get("kafka_tombstones_seen") or 0
            ) + kafka_tombstones
            rejected_total += len(kafka_rejects)
            if kafka_rejects and job_id:
                from services.quarantine_dlq import persist_rejected_rows

                persist_rejected_rows(
                    job_id=job_id,
                    rejected_details=kafka_rejects,
                    workspace_id=workspace_id,
                    source="transfer",
                    connector=src_type,
                )
        if not batch or not getattr(batch, "rows", None):
            # A page a filter or a recipe emptied still consumed source rows, so
            # the offset, the watermark and the keyset bookmark advance past it.
            # Reporting zero here would re-read that page on resume for ever.
            # A page the FK orphan guard emptied is *quarantine*, not removal:
            # its rows must reach the DLQ and the ledger as rejects.
            orphans = _fk_held_out(batch)
            return {
                "batch_written": 0,
                "last_checksum": "",
                "dest_summary": (
                    {
                        "rejected_details": orphans,
                        "rejected_rows": len(orphans),
                        "orphan_fk_rows": len(orphans),
                    }
                    if orphans
                    else {}
                ),
                "rejected": len(orphans),
                "coerced_null": 0,
                "warnings": [],
                "batch_max": _raw_page_cursor(batch) or None,
                "batch_keyset": _raw_page_keyset(batch) or None,
                "batch_rows": _raw_page_rows(batch) + len(kafka_rejects),
                "filter_page_removed": _raw_page_filtered(batch),
                "cursor_bounded_page": _raw_page_cursor_bounded(batch),
                "shape_page_removed": max(
                    0,
                    _raw_page_rows(batch)
                    - _raw_page_filtered(batch)
                    - _raw_page_cursor_bounded(batch)
                    - len(orphans),
                ),
                "shape_page_diverted": int(
                    getattr(batch, "raw_page_diverted", 0) or 0
                ),
                "shape_page_held": int(
                    getattr(batch, "raw_page_held", 0) or 0
                ),
                "reconcile_sample_rows": [],
                "fingerprints": [],
                "sdk_state_after": batch_sdk_state,
            }
        # Absorb sparse schemaless attributes discovered on this page.
        _absorb_schemaless_discovered_attrs(batch)
        local_warnings: list[str] = []
        sample_rows: list[dict[str, Any]] = [
            dict(zip(batch.headers, row)) for row in batch.rows[:_RECONCILE_SAMPLE_CAP]
        ]
        # Per-batch data-quality / anomaly gate for database streams.
        # Routed batches are audited PER ROUTE inside the split loop — each
        # route table is a separate destination; auditing the mixed batch
        # reads one route's mapped/diverted population as another's NULLs
        # and shrinkage (RT-3).
        _batch_routed = (
            shape_runner is not None
            and getattr(shape_runner, "recipe", None) is not None
            and shape_runner.recipe.router_step is not None
        )
        if batch_quality_enabled and not _batch_routed:
            # Audit uniqueness must match the *write* posture, not only the sync
            # label. Resume upgrades insert→upsert while effective_sync stays
            # append — inventing a soft uniqueness check would last-row-wins.
            audit_sync = (
                "upsert" if write_mode == "upsert" else effective_sync
            )
            audit = run_integrity_audit(
                headers=batch.headers,
                rows=batch.rows,
                column_types=column_types,
                mappings=mappings,
                required_targets=pk_target_cols or [],
                primary_key=pk_source_col if pk_source_col in batch.headers else None,
                primary_key_columns=pk_audit_cols,
                validation_mode=validation_mode,
                dest_kind=dest_type,
                # Validate↔Run parity: Full append must not invent a uniqueness
                # hard-block after G9 unlocked Execute for the same sync mode.
                sync_mode=audit_sync,
            )
            if audit.issues:
                local_warnings.extend(audit.issues[:10])
            if audit.warnings:
                local_warnings.extend(audit.warnings[:10])
            if not audit.passed:
                raise ValueError(f"Batch {idx} failed data-quality audit: {'; '.join(audit.issues[:5])}")
            drift_warnings = drift_detector.check(audit.stats or {})
            if drift_warnings:
                if validation_mode == "maximum":
                    raise ValueError(
                        f"Batch {idx} drift detected: {'; '.join(drift_warnings[:3])}"
                    )
                local_warnings.extend(drift_warnings[:3])

        # Both bookmarks describe the page as the source handed it over: a recipe
        # may have dropped the row carrying the page's highest key, and
        # committing a lower bookmark would hand those rows over again.
        batch_max = None
        if incremental and cursor_source_col:
            batch_max = _raw_page_cursor(batch) or max_cursor_value(
                batch.rows, batch.headers, cursor_source_col, cursor_pk_source or None
            )
        batch_keyset = None
        # Persist a PK bookmark even on a snapshot scan so the next resume
        # seeks instead of draining the prefix again. Never invent a bookmark
        # from an arbitrary first column — that skips tied peers.
        if use_keyset or keyset_order_cols:
            batch_keyset = _raw_page_keyset(batch) or max_keyset_bookmark(
                batch.rows,
                batch.headers,
                keyset_order_cols
                or ([keyset_col] + ([keyset_tiebreak] if keyset_tiebreak else [])),
            )

        from services.preflight_source_kind import source_is_record_batch

        # Object-store and file extracts spell a blank numeric cell as "".
        # That blank is SQL NULL on a nullable typed column (R8-003). A
        # database integer cannot store "" either, but only when the source
        # carrier itself has no empty-string domain — that path stays in the
        # blank-cell contract and is not flipped on here.
        write_kwargs: dict[str, Any] = {
            # File uploads reaching this stream path must opt in by their
            # real kind — hardcoding "database" made a csv upload fall back
            # to the extract rule and reject blanks a file preflight had
            # already promised as SQL NULL (D1).
            "empty_cells_as_null": source_is_record_batch(
                getattr(source, "kind", "") or "database", src_type
            ),
        }
        if dest_type == "snowflake":
            write_kwargs["connection"] = _ensure_snowflake_conn()
            write_kwargs["close_connection"] = False
            write_kwargs["skip_session_setup"] = bool(sf_conn_state["session_ready"])
        elif dest_type in ("postgresql", "redshift") and max_workers == 1:
            # One connection for the job. Later batches stay on this thread
            # (drive_ordered_batches). psycopg2 deadlocks once that handle
            # is used from the pool, which is how a 100k load stopped after
            # the pages that had already committed.
            write_kwargs["connection"] = _ensure_pg_conn()
            write_kwargs["close_connection"] = False
            write_kwargs["connection_holder"] = pg_conn_state

        router_step = (
            shape_runner.recipe.router_step
            if shape_runner is not None and shape_runner.recipe is not None
            else None
        )
        _rconn = None
        if (
            router_step is not None
            and dest_type in ("postgresql", "redshift")
            and write_kwargs.get("connection") is not None
        ):
            # A routed run writes several tables; a failure must not leave
            # some routes committed and others not (RT-5). On the shared PG
            # connection, arm a held transaction for ANY sync mode — every
            # route's commits defer until finish_held_overwrite at stream end
            # (commit all or roll back all). The overwrite path already binds
            # one on truncate; begin() is idempotent.
            from connectors.write_resilience import (
                PgOverwriteTransaction,
                bind_overwrite_transaction,
                overwrite_transaction_for,
            )

            _rconn = write_kwargs["connection"]
            _rheld = overwrite_transaction_for(_rconn)
            if _rheld is None:
                _rheld = PgOverwriteTransaction()
                bind_overwrite_transaction(_rconn, _rheld)
            _rheld.begin(job_id=job_id or "")
            # Failed-Gate-8 restore guard: a replace-mode routed run into
            # non-empty route tables takes a row-level COPY of each occupied
            # table inside this transaction (CREATE TABLE bak (LIKE t
            # INCLUDING ALL) + INSERT SELECT). The live table object is never
            # renamed or replaced — the customer's DDL, indexes, grants and
            # dependent views stay bound to it (P0-G1). A write failure rolls
            # the copies back with the rows; a post-write proof failure lets
            # _settle_overwrite_backup DELETE+INSERT the prior rows back and
            # drop the copies (the txn commits before Gate-8, so the backup
            # is the only way to restore prior content).
            _rsync_now = str(effective_sync or "")
            if is_overwrite_sync(_rsync_now) and not route_backup_state.get(
                "armed"
            ):
                route_backup_state["armed"] = True
                from connectors.sql_identifiers import (
                    quote_table_ref,
                    split_qualified_table,
                )
                from connectors.table_manager import pg_route_backup_name

                _rprecount = route_backup_state.get("precount") or {}
                _rbaks: dict[str, dict[str, str]] = {}
                _rtargets_map = dict(router_step.options.get("targets") or {})
                for _rr in router_step.options.get("routes") or []:
                    _rrn = str(_rr.get("name") or "")
                    if not _rrn or int(_rprecount.get(_rrn) or 0) <= 0:
                        continue
                    _rrt = str(
                        _rtargets_map.get(_rrn) or f"{dest_table}__{_rrn}"
                    )
                    _rschema, _rtbl = split_qualified_table(
                        _rrt, str(dest_cfg.get("schema") or "public")
                    )
                    _rbak = pg_route_backup_name(_rtbl, job_id or "")
                    try:
                        _live_ref = quote_table_ref(
                            _rtbl, _rschema, dialect="postgresql"
                        )
                        _bak_ref = quote_table_ref(
                            _rbak, _rschema, dialect="postgresql"
                        )
                        with _rconn.cursor() as _rcur:
                            # Backup existence is schema-qualified — a
                            # same-named table in another schema must not
                            # match (P0-G2).
                            _rcur.execute(
                                "SELECT 1 FROM information_schema.tables "
                                "WHERE table_schema = %s AND table_name = %s",
                                (_rschema, _rtbl),
                            )
                            if not _rcur.fetchone():
                                raise LookupError(
                                    f"route table {_rschema}.{_rtbl} not found"
                                )
                            _rcur.execute(
                                f"CREATE TABLE {_bak_ref} "
                                f"(LIKE {_live_ref} INCLUDING ALL)"
                            )
                            _rcur.execute(
                                f"INSERT INTO {_bak_ref} SELECT * FROM {_live_ref}"
                            )
                        _rbaks[_rrn] = {
                            "table": _rtbl,
                            "backup": _rbak,
                            "schema": _rschema,
                        }
                    except Exception as _rbak_exc:
                        logger.warning(
                            "Route %s backup copy skipped (%s); "
                            "post-write proof failure cannot restore it",
                            _rrn,
                            _rbak_exc,
                        )
                if _rbaks:
                    route_backup_state["backups"] = dict(_rbaks)
                    _dextra = dict(getattr(destination, "extra", None) or {})
                    _dextra["route_backups"] = dict(_rbaks)
                    destination.extra = _dextra
        write_calls: list[tuple[str | None, str, Any]] = []
        if row_apply_plan is not None:
            if router_step is not None:
                raise ValueError(
                    "router transform cannot write through a destination "
                    "stored-procedure plan — route outputs are separate tables"
                )
            from .stream_dest_procedure import write_procedure_chunk

            records = [dict(zip(batch.headers, row)) for row in batch.rows]
            write_calls.append(
                (
                    None,
                    dest_table,
                    partial(write_procedure_chunk, destination, records, row_apply_plan),
                )
            )
        else:
            from .stream_write import _write_batch

            splits = (
                _split_routed_batch(batch, shape_runner.recipe, dest_table)
                if router_step is not None
                else [(None, dest_table, batch.headers, batch.rows)]
            )
            if (
                router_step is not None
                and write_mode == "upsert"
                and _rconn is not None
                and pk_source_cols
            ):
                # Dedupe-by-partition (P1-B): a keyed row that changed route
                # must not keep its old copy — delete every batch/diverted
                # key from EVERY route table, inside this held transaction,
                # before the route's upsert lands its current copy.
                _purge_route_key_tuples(
                    _rconn,
                    router_step=router_step,
                    recipe=shape_runner.recipe,
                    dest_table=dest_table,
                    dest_cfg=dest_cfg,
                    batch=batch,
                    pk_source_cols=pk_source_cols,
                    pk_target_cols=pk_target_cols,
                    pk_audit_cols=pk_audit_cols,
                    diverted_keys=shape_divert_key_tuples,
                )
            created_tables = ddl_state.setdefault("created_tables", set())
            _rmap_decls = (
                {
                    str(_rd.get("name") or ""): (_rd.get("map") or {})
                    for _rd in (router_step.options.get("routes") or [])
                }
                if router_step is not None
                else {}
            )
            for _rname, _rtable, _rheaders, _rrows in splits:
                create_this = (
                    _rtable not in created_tables
                    if _rname is not None
                    else not ddl_state["created"]
                )
                if _rname is not None:
                    # The route table declares only its own columns — the
                    # ROUTE_COLUMN tag and other routes' map targets must not
                    # reach the writer (a "mapped source not in headers" risk
                    # contract would refuse the batch as a NULL-invent).
                    _rheader_set = set(_rheaders)
                    if _rmap_decls.get(_rname):
                        # A mapped route's headers are already the TARGET
                        # names, so source->target lookup against the shared
                        # mappings finds nothing — build identity mappings
                        # keyed by target name and carry the SOURCE column's
                        # type through the map so the route DDL keeps INT
                        # instead of degrading to TEXT (RT-8).
                        _rsrc_of = {
                            str(t): str(s)
                            for s, t in _rmap_decls[_rname].items()
                        }
                        _rmappings = [
                            {"source": h, "target": h, "confidence": 0.95}
                            for h in _rheaders
                        ]
                        _rtargets = set(_rheaders)
                        _rcol_types = {
                            h: column_types[_rsrc_of[h]]
                            for h in _rheaders
                            if _rsrc_of.get(h) in column_types
                        }
                    else:
                        _rmappings = [
                            m for m in mappings
                            if str(m.get("source") or "") in _rheader_set
                        ]
                        _rtargets = {
                            str(m.get("target") or m.get("source") or "")
                            for m in _rmappings
                        }
                        _rcol_types = {
                            k: v for k, v in column_types.items() if k in _rtargets
                        }
                    if True:
                        # Value-level proof per route: fingerprint this route's
                        # mapped rows exactly as the non-routed path does, so
                        # Gate-8 can compare each route table's read-back
                        # checksum against what this run wrote (RT accuracy).
                        # Always fingerprint for routes — a whole-source
                        # re-read cannot produce per-route digests.
                        try:
                            _r_mapped_fp, _ = map_rows_for_fingerprint(
                                headers=_rheaders,
                                data_rows=_rrows,
                                mappings=_rmappings,
                                target_cols=sorted(_rtargets),
                                column_types=_rcol_types,
                                error_policy=stream_error_policy,
                                dest_types=fingerprint_dest_types,
                                preserve_case=True,
                                dest_kind=dest_bind_dialect,
                                destination_pk_columns=list(pk_target_cols or []) or None,
                                empty_cells_as_null=bool(
                                    write_kwargs.get("empty_cells_as_null")
                                ),
                            )
                            if _r_mapped_fp:
                                from services.fingerprint_accumulator import (
                                    FingerprintAccumulator,
                                )

                                acc = route_fp_acc.setdefault(
                                    _rname, FingerprintAccumulator()
                                )
                                route_fp_cols.setdefault(
                                    _rname, sorted(_rtargets)
                                )
                                acc.add_many(
                                    row_fingerprints(
                                        _r_mapped_fp,
                                        sorted(_rtargets),
                                        dest_db_type=dest_bind_dialect,
                                        dest_types=digest_dest_types,
                                    )
                                )
                        except Exception as exc:
                            logger.warning(
                                "Route fingerprint skipped for %s: %s", _rname, exc
                            )
                else:
                    _rmappings = mappings
                    _rcol_types = column_types
                if _rname is not None and batch_quality_enabled:
                    # Each route table is its own destination — audit the
                    # route's rows against its own columns, never the mixed
                    # batch (RT-3).
                    try:
                        audit = run_integrity_audit(
                            headers=_rheaders,
                            rows=_rrows,
                            column_types=_rcol_types,
                            mappings=_rmappings,
                            required_targets=pk_target_cols or [],
                            primary_key=pk_source_col
                            if pk_source_col in _rheaders
                            else None,
                            primary_key_columns=[
                                c for c in (pk_audit_cols or []) if c in _rheaders
                            ],
                            validation_mode=validation_mode,
                            dest_kind=dest_type,
                            sync_mode=(
                                "upsert"
                                if write_mode == "upsert"
                                else effective_sync
                            ),
                        )
                        if audit.issues:
                            local_warnings.extend(
                                f"route {_rname}: {i}" for i in audit.issues[:10]
                            )
                        if audit.warnings:
                            local_warnings.extend(
                                f"route {_rname}: {w}" for w in audit.warnings[:10]
                            )
                        if not audit.passed:
                            raise ValueError(
                                f"Batch {idx} route {_rname} failed "
                                f"data-quality audit: "
                                f"{'; '.join(audit.issues[:5])}"
                            )
                        _rdet = route_drift_detectors.setdefault(
                            _rname, BatchDriftDetector()
                        )
                        drift_warnings = _rdet.check(audit.stats or {})
                        if drift_warnings:
                            if validation_mode == "maximum":
                                raise ValueError(
                                    f"Batch {idx} route {_rname} drift "
                                    f"detected: "
                                    f"{'; '.join(drift_warnings[:3])}"
                                )
                            local_warnings.extend(
                                f"route {_rname}: {w}"
                                for w in drift_warnings[:3]
                            )
                    except ValueError:
                        raise
                    except Exception as exc:
                        logger.warning(
                            "Route %s integrity audit skipped: %s", _rname, exc
                        )
                write_calls.append(
                    (
                        _rname,
                        _rtable,
                        partial(
                            _write_batch,
                            dest_type,
                            destination,
                            dest_cfg,
                            _rtable,
                            _rheaders,
                            _rrows,
                            _rmappings,
                            _rcol_types,
                            create_table=create_this,
                            on_checkpoint=None,
                            chunk_idx=idx,
                            total_chunks=_chunks_for_page(idx, batch),
                            rows_so_far=0,
                            write_mode=write_mode,
                            conflict_columns=pk_target_cols or None,
                            sync_mode=effective_sync,
                            backfill_new_fields=backfill_new_fields,
                            error_policy=stream_error_policy,
                            job_id=job_id,
                            skip_preflight=skip_preflight,
                            source_schema_catalog=source_schema_catalog,
                            **write_kwargs,
                        ),
                    )
                )
        # Keyed census and the inline write-pass fingerprint are single-table
        # mechanisms — routed rows land in per-route tables, verified per
        # route at Gate-8 instead.
        if keyed_census_acc is not None and router_step is None:
            observe_keyed_batch(
                keyed_census_acc,
                headers=batch.headers,
                rows=batch.rows,
                mappings=mappings,
                key_columns=pk_target_cols,
                db_type=dest_type,
                cfg=dest_cfg,
                schema=schema_from_cfg(dest_type, dest_cfg),
                table_name=dest_table,
            )
        # Phase F1 — fingerprint the mapped rows from this chunk (same policy as
        # post-write re-read) so the write pass can own the source checksum.
        # Tombstones are already stripped: hashing deleted keys as writes would
        # fail Gate-8 after a correct hard DELETE.
        inline_fps: list[Any] = []
        if inline_fingerprints_used and router_step is None:
            try:
                mapped_fp, _ = map_rows_for_fingerprint(
                    headers=batch.headers,
                    data_rows=batch.rows,
                    mappings=mappings,
                    target_cols=target_cols,
                    column_types=column_types,
                    error_policy=stream_error_policy,
                    dest_types=fingerprint_dest_types,
                    preserve_case=True,
                    dest_kind=dest_bind_dialect,
                    destination_pk_columns=list(pk_target_cols or []) or None,
                    empty_cells_as_null=bool(write_kwargs.get("empty_cells_as_null")),
                )
                if mapped_fp and pk_target_cols:
                    from services.primary_key import sync_collapses_in_file_duplicates

                    if sync_collapses_in_file_duplicates(effective_sync):
                        from connectors.writer_common import dedupe_rows

                        mapped_fp = dedupe_rows(
                            list(mapped_fp), list(pk_target_cols), list(target_cols)
                        )
                if mapped_fp:
                    inline_fps = row_fingerprints(
                        mapped_fp,
                        target_cols,
                        dest_db_type=dest_bind_dialect,
                        dest_types=digest_dest_types,
                    )
            except Exception as exc:
                logger.warning(
                    "Inline write-pass fingerprint skipped for chunk %s: %s", idx, exc
                )

        if job_id:
            # Not the throttled progress callback: that skips calls inside
            # its interval, and a cancel during a long batch was ignored
            # until the next flush. The in-flight statement still ends on
            # its lock wait; this stops the next batch.
            from services.cancel_check import raise_if_job_cancelled
            from services.mongodb_service import get_mongodb_service

            raise_if_job_cancelled(
                get_mongodb_service(), str(job_id), rows_written=written
            )
        try:
            batch_written = 0
            last_checksum = ""
            chunk_summary: dict[str, Any] = {}
            routes_wire: dict[str, dict[str, Any]] = {}
            for _rname, _rtable, _wop in write_calls:
                _w, _cs, _ds = with_retry(
                    _wop,
                    budget=RetryBudget(
                        max_attempts=retry.max_attempts,
                        base_delay_seconds=retry.base_delay_seconds,
                        max_delay_seconds=retry.max_delay_seconds,
                        exponential_base=retry.exponential_base,
                        jitter=retry.jitter,
                    ),
                    replay_safety=replay_safety,
                )
                batch_written += int(_w or 0)
                if _cs:
                    last_checksum = _cs
                chunk_summary = _merge_chunk_write_summaries(chunk_summary, _ds)
                if _rname is not None:
                    ddl_state["created_tables"].add(_rtable)
                    routes_wire[_rname] = {
                        "table": _rtable,
                        "rows": int(_w or 0),
                        # The route's rename map — downstream evidence probes
                        # (identity watermark) translate source pk names to
                        # the route table's actual columns through this.
                        "map": dict(_rmap_decls.get(_rname) or {}),
                    }
            if routes_wire:
                chunk_summary["routes"] = routes_wire
            # Fold orphan-FK holdouts into the batch's own rejected ledger so
            # they persist to the quarantine DLQ and balance Gate-8
            # conservation (read = written + rejected + filtered).
            fk_orphans = getattr(batch, "fk_orphan_details", None) or []
            if fk_orphans:
                det = list(chunk_summary.get("rejected_details") or [])
                det.extend(fk_orphans)
                chunk_summary["rejected_details"] = det
                chunk_summary["rejected_rows"] = int(
                    chunk_summary.get("rejected_rows") or 0
                ) + len(fk_orphans)
                chunk_summary["orphan_fk_rows"] = int(
                    chunk_summary.get("orphan_fk_rows") or 0
                ) + len(fk_orphans)
        except WriteBatchBlocked as blocked:
            if keyed_census_acc is not None:
                keyed_census_acc.reverse_last_live_batch()
            # Persist this batch's quarantine before aborting the stream —
            # bare RuntimeError would drop mid-write FAIL_JOB details.
            details = list(blocked.rejected_details or [])
            if details and job_id:
                from services.quarantine_dlq import persist_rejected_rows

                persist_rejected_rows(
                    job_id=str(job_id),
                    rejected_details=details,
                    source="stream_batch_abort",
                    connector=str(
                        getattr(destination, "format", None)
                        or getattr(destination, "kind", None)
                        or ""
                    ),
                )
            raise
        except Exception:
            if keyed_census_acc is not None:
                keyed_census_acc.reverse_last_live_batch()
            raise
        if keyed_census_acc is not None:
            if int(batch_written or 0) == 0:
                keyed_census_acc.reverse_last_live_batch()
            else:
                keyed_census_acc.keep_last_live_batch()
        if dest_type == "snowflake":
            sf_conn_state["session_ready"] = True
        return {
            "batch_written": batch_written,
            "last_checksum": last_checksum,
            "dest_summary": chunk_summary,
            "rejected": int(chunk_summary.get("rejected_rows", 0) or 0),
            "coerced_null": int(chunk_summary.get("coerced_null_rows", 0) or 0),
            "warnings": (chunk_summary.get("warnings") or [])[:10] + local_warnings,
            "batch_max": batch_max,
            "batch_keyset": batch_keyset,
            # Source rows this page consumed — the offset and the run's read
            # population count them, including the ones a recipe removed and a
            # Kafka poison message (read, then quarantined).
            "batch_rows": _raw_page_rows(batch) + len(kafka_rejects),
            "filter_page_removed": _raw_page_filtered(batch),
            "cursor_bounded_page": _raw_page_cursor_bounded(batch),
            "shape_page_removed": max(
                0,
                _raw_page_rows(batch)
                - _raw_page_filtered(batch)
                - _raw_page_cursor_bounded(batch)
                - len(batch.rows)
                # FK orphans are rejects (already in this batch's ledger), not
                # recipe removals — counting both made a correct run look lossy.
                - len(_fk_held_out(batch)),
            ),
            # Quarantine-diverted and still-buffered rows left the page for
            # named reasons — neither is a recipe removal (DD-3).
            "shape_page_diverted": int(
                getattr(batch, "raw_page_diverted", 0) or 0
            ),
            "shape_page_held": int(
                getattr(batch, "raw_page_held", 0) or 0
            ),
            "reconcile_sample_rows": sample_rows,
            "fingerprints": inline_fps,
            "overwrite_keys": (
                matrix_to_key_tuples(
                    batch.headers, batch.rows, mappings, pk_target_cols
                )
                if overwrite_keys_acc is not None
                else None
            ),
            "sdk_state_after": batch_sdk_state,
        }

    def _apply_result(idx: int, result: dict[str, Any]) -> None:
        nonlocal written, rejected_total, coerced_null_total, removed_on_read_total, filtered_on_read_total, cursor_bounded_total, expanded_total, last_checksum, running_cursor, committed_offset, committed_keyset, dest_summary, batches_completed, kafka_cursor, reconcile_sample
        if overwrite_keys_acc is not None:
            if "overwrite_keys" in result:
                overwrite_keys_acc.observe_tuples(result.get("overwrite_keys"))
            elif result.get("batch_rows"):
                overwrite_keys_acc.observe_tuples(None)
        written += result["batch_written"]
        rejected_total += result["rejected"]
        coerced_null_total += result.get("coerced_null", 0)
        filter_removed = int(result.get("filter_page_removed") or 0)
        filtered_on_read_total += filter_removed
        cursor_bounded_total += int(result.get("cursor_bounded_page") or 0)
        # Held-buffered rows and quarantine diverts left the page for named
        # reasons — subtracting them keeps removed_on_read_total at the true
        # recipe-removal count. rows_buffered is cumulative, so the page's
        # held contribution is the delta since the last committed page
        # (drained rows re-emit with raw_page_rows=0 and never recount).
        _held_now = int(result.get("shape_page_held") or 0)
        _held_delta = max(0, _held_now - shape_held_seen[0])
        shape_held_seen[0] = _held_now
        removed_on_read_total += filter_removed + max(
            0,
            int(result.get("shape_page_removed") or 0)
            - int(result.get("shape_page_diverted") or 0)
            - _held_delta,
        )
        last_checksum = result["last_checksum"] or last_checksum
        fps = result.get("fingerprints") or []
        if fps:
            write_pass_fp.add_many(fps)
        warning_samples.extend(result["warnings"])  # bounded + deduped
        if len(reconcile_sample) < _RECONCILE_SAMPLE_CAP:
            for row in result.get("reconcile_sample_rows") or []:
                if not isinstance(row, dict):
                    continue
                reconcile_sample.append(row)
                if len(reconcile_sample) >= _RECONCILE_SAMPLE_CAP:
                    break
        if result["batch_max"] is not None:
            if running_cursor is None or compare_cursor_values(result["batch_max"], running_cursor) > 0:
                running_cursor = result["batch_max"]
        # Results arrive in commit order, so the newest committed page carries
        # the furthest safe keyset bookmark.
        if result.get("batch_keyset"):
            committed_keyset = result["batch_keyset"]
        if sdk_source and "sdk_state_after" in result:
            running_cursor = result["sdk_state_after"]
        # Absolute source-row offset for this batch (0-based start before commit).
        batch_start = int(committed_offset or 0)
        committed_offset += result["batch_rows"]
        if result["dest_summary"]:
            incoming = dict(result["dest_summary"])
            method = incoming.get("load_method")
            if method:
                load_methods_seen.append(str(method))
            from .stream_dest_procedure import append_procedure_warnings

            append_procedure_warnings(ddl_log, incoming)
            # Merge quarantine findings across batches — never replace with last batch only.
            prev = dest_summary if isinstance(dest_summary, dict) else {}
            merged_usage = _merge_embedding_usage(
                prev.get("embedding_usage"), incoming.get("embedding_usage")
            )
            prev_details = list(prev.get("rejected_details") or [])
            new_details: list[dict[str, Any]] = []
            for raw in incoming.get("rejected_details") or []:
                if not isinstance(raw, dict):
                    continue
                detail = dict(raw)
                local_row = int(detail.get("row") or 0)
                if local_row > 0:
                    detail["batch_row"] = local_row
                    detail["batch_offset"] = batch_start
                    detail["row"] = batch_start + local_row  # 1-based absolute across the transfer
                    if int(result.get("shape_page_removed") or 0):
                        # The recipe removed rows earlier in this page, so this
                        # number counts the rows that reached the writer, not the
                        # source's own numbering. Say so rather than name a source
                        # line the operator would not find.
                        detail["row_basis"] = "shaped_read"
                new_details.append(detail)
            # GA: keep full rejected_details across batches — never drop for a
            # UI sample cap (sample is stamped separately at finalize).
            merged_details = prev_details + new_details
            # Drop the writer's per-batch source_row_count stamp: it counts only
            # this batch's mapped rows and would clobber the aggregate. The full
            # reader-side population is committed_offset, applied at finalize.
            incoming.pop("source_row_count", None)
            incoming.pop("source_row_count_source", None)
            expanded_total += int(incoming.get("rows_expanded") or 0)
            page_skips["skipped"] += int(incoming.get("rows_skipped") or 0)
            page_skips["collapsed"] += int(incoming.get("collapsed_duplicate_rows") or 0)
            dest_summary = {
                **prev,
                **incoming,
                "rejected_details": merged_details,
                "rejected_rows": rejected_total,
                "coerced_null_rows": coerced_null_total,
                # Per-batch STRUCT explode deltas sum — the writer stamps only
                # what its own batch expanded (X-30).
                "rows_expanded": expanded_total,
                # A collapsed image is skipped once, as the file path counts it.
                "rows_skipped": page_skips["skipped"] + page_skips["collapsed"],
            }
            if page_skips["collapsed"]:
                dest_summary["collapsed_duplicate_rows"] = page_skips["collapsed"]
            if merged_usage is not None:
                dest_summary["embedding_usage"] = merged_usage
            # Property 6 — keep first-batch fidelity certificate across chunks.
            fid = incoming.get("schema_fidelity") or prev.get("schema_fidelity")
            if fid:
                dest_summary["schema_fidelity"] = fid
            batches_completed += 1
            # Persist rejected rows before continuing — crash must not lose quarantine.
            if new_details and job_id:
                try:
                    from services.quarantine_dlq import persist_rejected_rows

                    persist_rejected_rows(
                        job_id=str(job_id),
                        rejected_details=new_details,
                        source="stream_batch",
                        connector=str(
                            getattr(destination, "format", None)
                            or getattr(destination, "kind", None)
                            or ""
                        ),
                    )
                    dest_summary["quarantine_stream_durable"] = True
                    dest_summary["quarantine_durable"] = True
                    # Prefix already on control-plane DLQ — finalize must not re-append.
                    dest_summary["quarantine_dlq_persisted_count"] = len(merged_details)
                except Exception as qexc:
                    dest_summary["quarantine_durable"] = False
                    dest_summary["quarantine_dlq_error"] = str(qexc)[:300]
                    raise RuntimeError(
                        "Quarantine DLQ persist failed after batch — refuse to "
                        f"continue (rows cannot disappear): {qexc}"
                    ) from qexc

        # Persist durable checkpoint after the batch is committed.  We use the
        # ordered result stream so checkpoint offsets/cursors can never skip
        # ahead of an in-flight batch.
        checkpoint.chunk_index = idx
        checkpoint.offset = committed_offset
        checkpoint.rows_processed = written
        # Persist cumulative quarantine counts so a crash-resume restores them and
        # Gate-8 conservation balances across passes.
        checkpoint.rejected_rows = rejected_total
        checkpoint.coerced_null_rows = coerced_null_total
        checkpoint.rows_expanded = expanded_total
        checkpoint.rows_removed_on_read = removed_on_read_total
        checkpoint.rows_source_filtered = filtered_on_read_total
        checkpoint.rows_cursor_bounded = cursor_bounded_total
        checkpoint.cursor_value = (
            running_cursor if sdk_source else running_cursor or committed_keyset or ""
        )
        checkpoint.cursor_column = cursor_source_col if incremental else keyset_col
        checkpoint.es_search_after = es_search_after
        checkpoint.redis_scan_state = redis_scan_state
        checkpoint.dynamodb_cursor = ddb_cursor
        checkpoint.kafka_cursor = kafka_cursor
        checkpoint.qdrant_offset = qdrant_offset
        checkpoint.checksum = last_checksum
        checkpoint.phase = "writing"
        checkpoint.cdc_stream = table or checkpoint.cdc_stream or ""
        if dest_summary.get(PRECOUNT_KEY) is not None:
            checkpoint.target_rows_before = int(dest_summary[PRECOUNT_KEY])
        checkpoint.chunk_total = chunks
        checkpoint.status = "running"
        # ACC-05 — advance the *route* watermark with every committed batch.
        # These rows are durable at the destination the moment the batch
        # returns; a cancelled run's next job_id has no access to this
        # job-scoped checkpoint, so without a per-chunk watermark the next
        # run re-reads from the beginning and re-writes committed rows
        # (observed: 40 duplicate documents on PG→Mongo incremental_append).
        # Ordering: watermark first — if the checkpoint save below fails the
        # route watermark still reflects committed rows, never behind them.
        if incremental and running_cursor and cursor_key:
            try:
                set_watermark(
                    cursor_key,
                    running_cursor,
                    metadata={
                        "job_id": job_id,
                        "sync_mode": effective_sync,
                        # A watermark is a value of one column; record which one
                        # so a later run on a different cursor cannot inherit it.
                        "cursor_column": cursor_source_col,
                        # The column a composite watermark is decodable on (QA ACC-02).
                        "tiebreak_column": cursor_pk_source,
                    },
                )
                # D110 — batch undo restores the watermark to this pre-run
                # value when it removes the rows this advance stood for.
                dest_summary.setdefault(
                    "watermark_restore_point",
                    {"cursor_key": cursor_key, "watermark": job_start_watermark},
                )
            except Exception as wm_exc:
                raise RuntimeError(
                    "Cursor watermark persist failed after committed batch — "
                    "refuse to continue (next run would re-read and re-write "
                    f"committed rows): {wm_exc}"
                ) from wm_exc
        # Fail-closed: no durable resume point ⇒ abort (do not keep writing).
        checkpoint_service.require_save(checkpoint)
        if src_type == "kafka" and kafka_cursor:
            # D85 — never commit consumer offsets inside the run. Accumulate the
            # per-partition end offsets in the checkpoint cursor so a crash
            # resumes the full uncommitted range; the engine commits the group
            # offsets exactly once, only after the job is accepted (reconcile
            # passed + destination committed). A failure/cancel leaves the
            # broker position untouched, so the next run re-reads every
            # uncommitted message (at-least-once).
            kafka_cursor = _kafka_accumulate_offsets(dest_summary, kafka_cursor)
            checkpoint.kafka_cursor = kafka_cursor
            checkpoint_service.require_save(checkpoint)
        if on_checkpoint:
            on_checkpoint(idx, chunks, written, checkpoint.to_dict())

    first_idx = chunk_idx + 1

    def _advance_fetch_cursor(page: Any) -> None:
        """Move the read edge to the page's high-water mark.

        The seed page is taken whole: an inclusive tie-group read can end
        *below* the stored ``(cursor, last key)``, and keeping the stored mark
        then would skip the tie rows between the page edge and it.
        """
        nonlocal fetch_cursor, fetch_from_seed
        if not (incremental and cursor_source_col and _raw_page_rows(page)):
            return
        batch_max = _page_cursor_max(page)
        if batch_max and (
            fetch_from_seed
            or fetch_cursor is None
            or compare_cursor_values(batch_max, fetch_cursor) > 0
        ):
            fetch_cursor = batch_max
            fetch_from_seed = False

    def _prepare_batch(idx: int, raw_batch: Any) -> Any:
        nonlocal fetch_offset
        prepared = _filter_batch(raw_batch)
        _advance_fetch_cursor(prepared)
        fetch_offset += _raw_page_rows(prepared)
        return prepared

    from .stream_dest_procedure import run_session_after, run_session_before

    run_after = False
    row_path_ok = False
    try:
        run_after = run_session_before(destination, dest_proc_plan, ddl_log)
        # Process the first batch synchronously so DDL (table/index creation) is
        # committed before any parallel workers try to insert into the new table.
        while batch:
            batch = _filter_batch(batch)
            _advance_fetch_cursor(batch)
            _apply_result(first_idx, _process_db_chunk(first_idx, batch))
            fetch_offset += _raw_page_rows(batch)
            if batch.rows:
                ddl_state["created"] = True
                break
            # Every row on this page was removed on the read, so it never
            # reached the writer and the table does not exist yet. Keep reading
            # here rather than hand the DDL to the worker pool.
            batch = _fetch_next_batch(batch)
            first_idx += 1

        # The first page committed above. A single shared connection (proxy,
        # Snowflake, SQLite, Iceberg) finishes the rest on this thread.
        # Parallel destinations still overlap the next read with the write.
        drive_ordered_batches(
            max_workers=max_workers,
            initial=batch,
            fetch_next=_fetch_next_batch,
            prepare=_prepare_batch,
            process=_process_db_chunk,
            apply_result=_apply_result,
            start_idx=first_idx + 1,
        )
        # A buffering recipe step (dedupe keep-last, aggregate, sort, rank)
        # holds rows until the population is fully read — this is that moment.
        # Drained rows run the same write path and DDL gate as every other page.
        if shape_runner is not None:
            tail_headers, tail_rows = shape_runner.drain_matrix()
            if tail_rows:
                tail_batch = SimpleNamespace(
                    headers=tail_headers,
                    rows=tail_rows,
                    meta={},
                    fk_orphan_details=[],
                    # Every drained row was already counted in the page that
                    # held it — the tail is a re-emit, not new source rows
                    # (DD-3). raw_page_rows=0 keeps committed_offset honest.
                    raw_page_rows=0,
                )
                tail_idx = first_idx + 1 + 10_000_000
                _apply_result(tail_idx, _process_db_chunk(tail_idx, tail_batch))
            # Winners can still hit a divert step downstream of the buffer.
            _persist_shape_diverts()
        row_path_ok = True
    except BaseException as _row_exc:
        # X-9: same undo as the file path — a transform refusal / writer
        # reject must not leave committed batches.
        # D102: a user cancel is NOT a failed batch — the committed prefix
        # stays in the destination and cancel_outcome reports it verbatim.
        # Undoing it here deleted committed rows while the job still claimed
        # them (D42: "committed rows into a table that doesn't exist").
        from services.error_handling import TransferCancelled

        if not isinstance(_row_exc, TransferCancelled):
            try:
                from services.batch_undo import (
                    undo_failed_batch_if_dest_was_empty,
                )

                _undo_note = undo_failed_batch_if_dest_was_empty(
                    destination=destination,
                    dest_summary=dest_summary,
                    sync_mode=effective_sync,
                )
                if _undo_note:
                    setattr(_row_exc, "failed_batch_undo_note", _undo_note)
            except Exception:  # noqa: BLE001 - undo must not hide the write failure
                pass
        _kafka_failure_offset_note(src_type, dest_summary, _row_exc)
        raise
    finally:
        if run_after:
            run_session_after(
                destination, dest_proc_plan, ddl_log, write_ok=row_path_ok
            )
        if src_scan:
            try:
                from connectors.sql_snapshot_scan import close_table_scan

                close_table_scan(src_scan)
            except Exception as exc:
                logging.getLogger(__name__).debug(
                    "Source snapshot scan close skipped: %s", exc, exc_info=exc
                )
        for state in (sf_conn_state, pg_conn_state):
            conn = state.get("conn")
            if conn is not None:
                if state is pg_conn_state:
                    from connectors.write_resilience import (
                        finish_held_overwrite,
                        overwrite_transaction_for,
                    )

                    _rtx = overwrite_transaction_for(conn)
                    # G-1i: record whether this run's write transaction
                    # cleared the destination ("emptied" an existing table
                    # or "absent"/create-new via note_truncated) BEFORE
                    # finish() resets the flags. The transform proof fails
                    # a leftover-delete only when the run recorded this
                    # clear — anything reaching the heal afterwards can
                    # only have been written mid-gap. When no clear ran
                    # (clear skipped, non-overwrite, resume arming a bare
                    # txn) heal-removal stays "removed by overwrite".
                    _run_cleared = bool(
                        _rtx is not None and _rtx.was_truncated()
                    )
                    finish_held_overwrite(conn, success=row_path_ok)
                    if _run_cleared:
                        dest_summary["overwrite_clear_recorded"] = True
                    _rtables = sorted(
                        {
                            str(v.get("table") or "")
                            for v in (dest_summary.get("routes") or {}).values()
                            if isinstance(v, dict) and v.get("table")
                        }
                    )
                    if row_path_ok and _rtables:
                        # Stamp the transaction id that wrote this run's
                        # routed rows (xmin = txid) so a failed Gate-8 can
                        # undo exactly this run's rows — never pre-existing
                        # ones, including byte-identical copies (P2). All of
                        # this run's writes share one committed xid, which is
                        # the newest xmin in each table it wrote. Each route
                        # table commits under its own xid, so keep the map
                        # table -> xid and undo per table (P2).
                        try:
                            from connectors.sql_identifiers import (
                                quote_table_ref as _qtr,
                                split_qualified_table as _sqt,
                            )

                            _rschema = str(
                                dest_cfg.get("schema") or "public"
                            )
                            _txids: dict[str, int] = {}
                            with conn.cursor() as _txcur:
                                for _t in _rtables:
                                    _ts, _tt = _sqt(_t, _rschema)
                                    # xid has no ordering ops — max client-side.
                                    _txcur.execute(
                                        "SELECT DISTINCT xmin FROM "
                                        + _qtr(
                                            _tt,
                                            _ts,
                                            dialect="postgresql",
                                        )
                                    )
                                    _vals = [
                                        int(_r[0])
                                        for _r in _txcur.fetchall()
                                        if _r[0] is not None
                                    ]
                                    if _vals:
                                        _txids[_t] = max(_vals)
                            if _txids:
                                dest_summary["route_run_txid"] = _txids
                                dest_summary["route_run_tables"] = _rtables
                        except Exception as exc:  # noqa: BLE001
                            logger.warning(
                                "Route run txid capture failed: %s", exc
                            )
                try:
                    conn.close()
                except Exception as exc:
                    logging.getLogger(__name__).debug(
                        "Exception suppressed: %s", exc, exc_info=exc
                    )
                state["conn"] = None

    if written == 0 and incremental:
        ddl_log.append("INCREMENTAL — no new rows since last watermark")
        dest_summary["sync_mode"] = effective_sync
        # No batch ran, so no writer summary named the table; the job record
        # read an empty destination and fell back to the database (QA MX3-01).
        dest_summary.setdefault("table", dest_table)
        stamp_incremental_no_op(dest_summary)
        if resume_key_resolved:
            # Honest delivery label: the read side re-delivers the interrupted
            # batch, the write side resolves it on the identity key.
            dest_summary["delivery_semantics"] = "at_least_once_idempotent_apply"
            dest_summary["resume_write_mode"] = write_mode
        dest_summary["watermark"] = watermark
        try:
            from services.source_snapshot import get_source_snapshot_meta

            _snap = get_source_snapshot_meta()
            if _snap:
                dest_summary["source_snapshot"] = _snap
                dest_summary.setdefault(
                    "source_snapshot_guarantee",
                    _snap.get("guarantee") or "unknown",
                )
        except Exception:
            pass
        return 0, ddl_log, dest_summary, columns

    if written == 0:
        details = "; ".join(filter(None, warning_samples[:10]))
        if details:
            raise ValueError(f"No rows were written to the destination: {details}")
        if rejected_total:
            # Quarantine is not an empty source. Reporting "empty" here sent
            # operators to look at data that was in fact read, rejected and
            # held — the reason is already recorded, so name it.
            reasons: list[str] = []
            for detail in (dest_summary.get("rejected_details") or [])[:200]:
                reason = str((detail or {}).get("reason") or "").strip()
                if reason and reason not in reasons:
                    reasons.append(reason)
                if len(reasons) >= 3:
                    break
            raise ValueError(
                f"All {rejected_total} row(s) were quarantined, so nothing was "
                "written to the destination"
                + (f" — {'; '.join(reasons)}" if reasons else "")
            )
        if columns and (getattr(destination, "extra", None) or {}).get(
            "create_empty_source"
        ):
            written_n, checksum, empty_summary = _stream._write_batch(
                dest_type,
                destination,
                dest_cfg,
                dest_table,
                list(columns),
                [],
                mappings,
                column_types,
                create_table=True,
                on_checkpoint=None,
                chunk_idx=0,
                total_chunks=1,
                rows_so_far=0,
                write_mode=write_mode,
                conflict_columns=pk_target_cols or None,
                sync_mode=effective_sync,
                backfill_new_fields=backfill_new_fields,
                error_policy=stream_error_policy,
                job_id=job_id,
                skip_preflight=skip_preflight,
                source_schema_catalog=source_schema_catalog,
            )
            dest_summary.update(empty_summary or {})
            dest_summary["source_row_count"] = 0
            dest_summary["empty_source_table"] = True
            dest_summary["checksum"] = checksum
            ddl_log.append(f"CREATE TABLE {dest_table} — source has columns and 0 rows")
            return int(written_n or 0), ddl_log, dest_summary, columns
        raise ValueError("Source table is empty")

    if incremental and running_cursor and cursor_key and running_cursor != watermark:
        set_watermark(
            cursor_key,
            running_cursor,
            metadata={
                "job_id": job_id,
                "sync_mode": effective_sync,
                # A watermark is a value of one column; record which one so a
                # later run on a different cursor cannot inherit it.
                "cursor_column": cursor_source_col,
                # The column a composite watermark is decodable on (QA ACC-02).
                "tiebreak_column": cursor_pk_source,
            },
        )
        # D110 — batch undo restores the watermark to this pre-run value
        # when it removes the rows this advance stood for.
        dest_summary.setdefault(
            "watermark_restore_point",
            {"cursor_key": cursor_key, "watermark": job_start_watermark},
        )

    # Phase F1 — write-pass fingerprints hash the same remapped rows just written.
    # Warehouse full refresh auto-re-reads, including same-engine
    # (DATAFLOW_RECONCILE_SOURCE_REREAD=auto), so Gate-8 can earn
    # independent_source_reread / full_checksum from a second source scan.
    # Force off with =0; force on with =1. Partial write-pass (resume tail) always
    # re-reads — a session digest vs a full destination is a false mismatch.
    # Incremental and CDC stay on the write-pass.
    #
    # Resume: the write pass only fingerprints this session's rows while Gate-8
    # compares the full destination — force a source re-read. This holds for
    # every sync mode, not just overwrite: a resumed keyed load lands the whole
    # population too, so a session-only digest reads as a checksum mismatch on a
    # destination that is in fact complete.
    # A recipe that removed rows makes the write pass smaller than the source
    # population by design. Counting that as a partial pass would force a second
    # source scan and then compare unshaped source values against shaped
    # destination values, so the removed rows come off the expectation.
    _shaped_removed = int(removed_on_read_total)
    _expected_population = max(
        (int(total_rows or 0) or int(written or 0)) - _shaped_removed, 0
    )
    _partial_write_pass = bool(
        write_pass_fp.total > 0
        and _expected_population > 0
        and write_pass_fp.total < _expected_population
        and (is_overwrite_sync(effective_sync) or resumed_pass)
    )
    _reread = should_reread_source(
        src_type=src_read_type,
        dest_type=dest_type,
        incremental=incremental,
        partial_write_pass=_partial_write_pass,
    )
    if incremental and _reread and not (
        cursor_source_col and running_cursor and pk_target_cols
    ):
        # M3 — an incremental re-read is only honest when it is bounded to
        # the [watermark, max_seen] window this run covered AND the
        # destination read-back can be scoped to the run's keys (X-OW1).
        # No cursor bound or no key scope → keep the write-pass digest and
        # its honest write-confirmed label.
        _reread = False
    if write_pass_fp.total > 0 and not _reread:
        checksum_started = time.perf_counter()
        final_checksum = write_pass_fp.digest()
        phase_profile.add(
            PHASE_CHECKSUM, time.perf_counter() - checksum_started, rows=write_pass_fp.total
        )
        dest_summary["checksum_mode"] = "inline_write_pass"
        dest_summary["source_independently_reread"] = False
        dest_summary["identity_hash_aligned"] = False
        _unbounded = incremental and not (
            cursor_source_col and running_cursor and pk_target_cols
        )
        dest_summary["checksum_note"] = (
            "Source fingerprints accumulated during the write pass (Phase F1) — "
            "no second source scan. Warehouse routes re-read by default, "
            "including same-engine; incremental runs re-read the bounded "
            "[watermark, max_seen] window. This run stayed on the write-pass "
            + (
                "(incremental — the re-read window or the destination key "
                "scope could not be bounded). "
                if _unbounded
                else "(non-warehouse route, CDC, or "
                     "DATAFLOW_RECONCILE_SOURCE_REREAD=0). "
            )
            + "Set DATAFLOW_RECONCILE_SOURCE_REREAD=1 to force a second scan."
        )
    elif _reread and src_read_type in REREAD_SCAN_SOURCES:
        # Independent re-read — snapshot scan (no OFFSET) on warehouse sources.
        from connectors.sql_snapshot_scan import close_table_scan

        checksum_started = time.perf_counter()
        fp_accumulator = FingerprintAccumulator()
        reread_plan = reread_pagination_plan(
            src_type=src_read_type,
            incremental=incremental,
            callable_source=bool(is_callable_source(source) or is_callable_source(src_cfg)),
        )
        reread_scan = reread_plan.get("scan_state")
        use_reread_offset = bool(reread_plan.get("use_offset"))
        cursor_type_for_read: str | None = None
        if incremental and cursor_source_col:
            cursor_type_for_read = normalize_inferred(schema.get(cursor_source_col, "string")).upper()
        checksum_cursor_col = cursor_source_col if incremental else ""
        checksum_cursor_after = window_seed_bookmark if incremental else None
        checksum_cursor_max = running_cursor if incremental else None
        _seed_tiebreak = bool(
            cursor_pk_source and cursor_pk_source != checksum_cursor_col
        )
        _seed_cursor_part: str | None = None
        if incremental and checksum_cursor_after not in (None, ""):
            from services.keyset_pagination import split_cursor_bookmark as _split_bm

            _seed_cursor_part = _split_bm(
                str(checksum_cursor_after), has_tiebreak=_seed_tiebreak
            )[0]
        # M3 — publish the exact window the digest covers so Gate-8 scopes
        # every other population the same way (the ladder's own source
        # re-read and the destination key filter are unbounded without it —
        # a whole-table digest compared to a window digest reads as a
        # checksum mismatch on a correct run).
        if incremental and checksum_cursor_col:
            dest_summary["incremental_window_cursor"] = checksum_cursor_col
            dest_summary["incremental_window_pk"] = cursor_pk_source or ""
            if checksum_cursor_after not in (None, ""):
                dest_summary["incremental_window_seed"] = str(checksum_cursor_after)
                dest_summary["incremental_window_seed_inclusive"] = window_seed_inclusive
            if checksum_cursor_max not in (None, ""):
                dest_summary["incremental_window_max"] = str(checksum_cursor_max)
        use_checksum_keyset = False
        if incremental and checksum_cursor_col:
            use_checksum_keyset = True
        read_offset = 0
        checksum_rows_read = 0
        # The re-read covers the population the write pass read: its first
        # page re-reads the seed's tie group exactly when the write pass did.
        checksum_seed_read = window_seed_inclusive
        # The digest has to describe the rows this run wrote, so the second scan
        # replays the same approved recipe. Its own runner: the run's shaping
        # effects were counted on the write pass and must not be counted twice.
        reread_shaper = (
            ShapeRunner(shape_runner.recipe, typed_values=shape_runner.typed_values) if shape_runner is not None else None
        )
        reread_holdouts: list[dict[str, Any]] = []
        reread_tombstones_excluded = 0
        try:
            while True:
                read_limit = _batch_limit(
                    read_offset if use_reread_offset else checksum_rows_read,
                    default=chunk_size,
                )
                if read_limit <= 0:
                    break
                _scan_kw = {"scan_state": reread_scan} if reread_scan is not None else {}
                batch, _ = _unwrap_read(
                    _read_batch(
                        src_read_type,
                        src_cfg,
                        table,
                        columns,
                        read_offset if use_reread_offset else 0,
                        read_limit,
                        database=src_db,
                        cursor_column=checksum_cursor_col,
                        cursor_after=checksum_cursor_after,
                        cursor_inclusive=checksum_seed_read,
                        cursor_type=cursor_type_for_read,
                        # A tie-broken incremental watermark is composite
                        # (cursor + pk). Every read of this run has to name
                        # the same primary key or the bookmark decodes as
                        # one part and the page dies on arity — this re-read
                        # is bound by the same contract as the write pass.
                        cursor_primary_key=cursor_pk_source or None,
                        known_total_rows=total_rows,
                        **_scan_kw,
                    )
                )
                if not batch or not batch.rows:
                    break
                # A keyed upsert hard-DELETEs tombstoned keys, so the destination
                # does not hold those rows: hashing them here would compare N
                # source rows against N-deletes destination rows and fail Gate-8
                # on a correct run. Same owner the write pass strips with.
                # ``batch.rows`` stays intact — the keyset cursor advances on the
                # last row actually read, not on the digest scope.
                digest_rows = batch.rows
                if (
                    incremental
                    and use_checksum_keyset
                    and header_column_index(batch.headers or [], checksum_cursor_col)
                    is not None
                ):
                    # M3 — bound the digest to the [watermark, max_seen]
                    # window this run covered. The upper bound drops rows
                    # that arrived after the run started; the lower bound is
                    # the write pass's own watermark predicate — a snapshot
                    # or OFFSET scan does not seek, so below-watermark rows
                    # would otherwise hash against a destination population
                    # this run never wrote.
                    _win_rows = []
                    for _r in digest_rows:
                        _rc = max_cursor_value(
                            [_r], batch.headers, checksum_cursor_col,
                            cursor_pk_source or None,
                        )
                        if _seed_cursor_part is not None:
                            # Lower bound: the whole tie group at the seed —
                            # same inclusive ``cursor >= seed`` semantics the
                            # write pass used, so compare cursor parts only.
                            _rc_part, _rc_pk = _split_bm(
                                str(_rc or ""), has_tiebreak=_seed_tiebreak
                            )
                            if compare_cursor_values(_rc_part, _seed_cursor_part) < 0:
                                continue
                        if compare_cursor_values(_rc, checksum_cursor_max) <= 0:
                            _win_rows.append(_r)
                    # No early break: OFFSET pages do not guarantee cursor
                    # ordering, so a straddling page does not end the window.
                    digest_rows = _win_rows
                if _boundary_fps_state["fps"] is not None and digest_rows:
                    # The write pass dropped these tie peers as already at the
                    # destination (judged against its pre-run rows, cached):
                    # the digest covers what was written, not re-delivered.
                    _landed = _boundary_rows_already_landed(batch.headers, digest_rows)
                    if _landed:
                        digest_rows = [
                            r for i, r in enumerate(digest_rows) if i not in _landed
                        ]
                if keyed_upsert_scope:
                    digest_rows, _excluded_now = live_rows_for_digest(
                        batch.headers,
                        digest_rows,
                        key_columns=pk_target_cols,
                        mappings=mappings,
                    )
                    reread_tombstones_excluded += _excluded_now
                digest_headers = batch.headers
                if source_filter and digest_rows:
                    # The write pass only wrote the rows the filter kept, so the
                    # digest has to cover the same population — hashing the rows
                    # it removed would fail Gate-8 on a correct run.
                    digest_rows = apply_row_filter_to_matrix(
                        batch.headers, digest_rows, source_filter
                    )
                if fk_orphan_guard is not None and digest_rows:
                    # Orphans were quarantined, not written: the digest covers
                    # the rows the write pass let through, in the same order of
                    # owners (filter → FK guard → recipe).
                    digest_rows, _orphans_now = fk_orphan_guard.partition(
                        list(digest_headers or []), list(digest_rows)
                    )
                if reread_shaper is not None:
                    # Shaped for the digest only: pagination and the keyset
                    # cursor still advance on the page the source handed over.
                    digest_headers, digest_rows = reread_shaper.matrix(
                        list(batch.headers or []), digest_rows
                    )
                mapped, reread_rejected = map_rows_for_fingerprint(
                    headers=digest_headers,
                    data_rows=digest_rows,
                    mappings=mappings,
                    target_cols=target_cols,
                    column_types=column_types,
                    error_policy=stream_error_policy,
                    dest_types=fingerprint_dest_types,
                    preserve_case=True,
                    dest_kind=dest_bind_dialect,
                    destination_pk_columns=list(pk_target_cols or []) or None,
                )
                reread_holdouts.extend(reread_rejected or [])
                if mapped:
                    fp_accumulator.add_many(
                        row_fingerprints(
                            mapped,
                            target_cols,
                            dest_db_type=dest_bind_dialect,
                            dest_types=digest_dest_types,
                        )
                    )
                checksum_rows_read += len(batch.rows)
                if limit > 0 and checksum_rows_read >= limit:
                    break
                if len(batch.rows) < read_limit:
                    break
                if (
                    use_checksum_keyset
                    and header_column_index(batch.headers or [], checksum_cursor_col)
                    is not None
                ):
                    # Advance on the composite (cursor, pk) max — the same
                    # bookmark shape the write pass seeds and the reader
                    # seeks with. A bare cursor value would drop tie rows on
                    # the next page and undercount the window.
                    checksum_cursor_after = max_cursor_value(
                        batch.rows,
                        batch.headers,
                        checksum_cursor_col,
                        cursor_pk_source or None,
                    )
                    checksum_seed_read = False
                elif use_reread_offset:
                    read_offset += len(batch.rows)
        finally:
            if reread_scan is not None:
                close_table_scan(reread_scan)
        if reread_shaper is not None:
            # Buffering recipe steps emitted their winners on the write pass at
            # end-of-stream; the digest population must include the same tail
            # or the write-pass checksum cannot align.
            tail_headers, tail_rows = reread_shaper.drain_matrix()
            if tail_rows:
                tail_mapped, _tail_rejected = map_rows_for_fingerprint(
                    headers=tail_headers,
                    data_rows=tail_rows,
                    mappings=mappings,
                    target_cols=target_cols,
                    column_types=column_types,
                    error_policy=stream_error_policy,
                )
                if tail_mapped:
                    fp_accumulator.add_many(
                        row_fingerprints(
                            tail_mapped,
                            target_cols,
                            dest_db_type=dest_bind_dialect,
                            dest_types=digest_dest_types,
                        )
                    )
        phase_profile.add(
            PHASE_CHECKSUM, time.perf_counter() - checksum_started, rows=fp_accumulator.total
        )
        dest_summary["reread_pagination"] = reread_plan.get("mode")
        if route_fp_acc:
            # Per-route write-pass digests — the value-level expectation
            # Gate-8 compares against each route table's independent
            # read-back (route_verification). Keyed by route name.
            dest_summary["route_expected"] = {
                _rn: {
                    "rows": _acc.total,
                    "checksum": _acc.digest(),
                    # Additive multiset digest — proves an append into an
                    # occupied route table: post-read-back sum minus the
                    # pre-write sum must equal this value (RT-6).
                    "additive": _acc.additive_digest(),
                    # The columns the fingerprint covers — the read-back is
                    # projected to exactly these so customer-owned extra
                    # columns (defaults, generated) never enter the digest.
                    "columns": route_fp_cols.get(_rn) or [],
                }
                for _rn, _acc in route_fp_acc.items()
                if _acc.total > 0
            }
        if route_backup_state.get("backups"):
            # Failed-Gate-8 restore guard armed: the renamed-aside route
            # tables land on the job summary so a failed post-write proof
            # can restore every route to pre-run content.
            dest_summary["route_backups"] = dict(route_backup_state["backups"])
        if fp_accumulator.total:
            if write_pass_fp.total:
                alignment = align_source_populations(write_pass_fp, fp_accumulator)
                dest_summary["write_pass_checksum"] = write_pass_fp.digest()
            else:
                alignment = alignment_when_write_pass_empty(
                    int(fp_accumulator.total),
                    inline_fingerprints_used=inline_fingerprints_used,
                )
            dest_summary["identity_alignment"] = alignment
            if alignment.get("identity_hash_aligned") is not None:
                dest_summary["identity_hash_aligned"] = bool(
                    alignment.get("identity_hash_aligned")
                )
            final_checksum = fp_accumulator.digest()
            dest_summary["checksum_mode"] = "source_reread"
            dest_summary["source_independently_reread"] = True
            note = (
                "Independent source re-read after the write pass "
                f"({reread_plan.get('mode')} pagination) — dest read-back can earn "
                "full_checksum / migration_proven."
            )
            dest_summary["checksum_note"] = record_tombstone_digest_scope(
                dest_summary, reread_tombstones_excluded, note
            )
        else:
            # The re-read fingerprinted nothing. Falling back to the writer's own
            # digest here — while still stamping source_reread — presented a
            # writer-basis hash as an independent source scan and produced a
            # mismatch no operator could localize. Decline the digest instead.
            final_checksum = ""
            dest_summary["checksum_mode"] = "source_reread_unavailable"
            dest_summary["source_independently_reread"] = False
            dest_summary["identity_hash_aligned"] = False
            dest_summary["checksum_note"] = (
                "Independent source re-read produced no comparable fingerprints "
                f"({checksum_rows_read:,} row(s) read, "
                f"{len(reread_holdouts):,} held out by the remap) — no source "
                "digest for this run rather than a writer-basis hash presented "
                "as a source scan."
            )
    else:
        final_checksum = write_pass_fp.digest() if write_pass_fp.total else last_checksum
        dest_summary["checksum_mode"] = "inline_write_pass" if write_pass_fp.total else "writer_last_batch"
        dest_summary["source_independently_reread"] = False
        dest_summary["identity_hash_aligned"] = False

    dest_summary["checksum"] = (
        final_checksum
        if dest_summary.get("checksum_mode") == "source_reread_unavailable"
        else (final_checksum or last_checksum)
    )
    if not isinstance(dest_summary.get("connector_versions"), dict):
        from services.connector_versions import capture_route_versions

        dest_summary["connector_versions"] = capture_route_versions(
            source_engine=src_type,
            dest_type=dest_type,
        )
    # Phase F2 — operator-visible pagination honesty (OFFSET cliff vs keyset).
    dest_summary["pagination_mode"] = pagination_mode
    if decision.resume_fallback:
        dest_summary["resume_pagination"] = (
            "snapshot_prefix" if pagination_mode == "scan" else "offset_legacy"
        )
    if pagination_warning:
        dest_summary["pagination_warning"] = pagination_warning
    elif pagination_mode == "offset":
        dest_summary["pagination_warning"] = (
            "OFFSET pagination — quadratic cost and full scans on some warehouses; "
            "declare a primary key to enable keyset seek reads."
        )
    elif pagination_mode == "bulk_copy":
        dest_summary["bulk_export"] = "postgresql_copy_stdout"
    elif keyset_order_cols:
        dest_summary["pagination_key_columns"] = list(keyset_order_cols)
    # Where the time actually went. Without this the only signal was a single
    # end-of-run rows/second figure, which cannot distinguish a slow source scan
    # from a slow destination write from a doubled cost in strict verification.
    phase_snapshot = phase_profile.snapshot()
    dest_summary["phase_profile"] = phase_snapshot
    if phase_snapshot.get("phases"):
        logger.info("Transfer phase breakdown — %s", phase_profile.summary())
    # Connection and metadata reuse for this run. Without these numbers the
    # operator cannot tell a transfer that reused one pool across N chunks from
    # one that rebuilt a pool per chunk — and that distinction is the whole
    # point of the engine/schema caches.
    if _pool_baseline is not None or _schema_baseline is not None:
        try:
            from services.engine_pool import stats as _engine_stats
            from services.reflection_cache import stats as _schema_stats

            def _delta(now: dict, base: dict | None) -> dict:
                if not base:
                    return now
                # Counters are differenced so the run summary shows what *this*
                # transfer reused. Gauges (``live``) and ratios stay absolute —
                # subtracting a gauge produces a signed nonsense number.
                gauges = {"live", "reuse_ratio"}
                out = {}
                for key, value in now.items():
                    if (
                        key not in gauges
                        and isinstance(value, (int, float))
                        and key in base
                    ):
                        out[key] = type(value)(value - base[key])  # type: ignore[operator]
                    else:
                        out[key] = value
                return out

            pool_delta = _delta(_engine_stats().to_dict(), _pool_baseline)
            schema_delta = _delta(_schema_stats().to_dict(), _schema_baseline)
            # Recompute ratios from the deltas so a process with prior hits
            # does not inflate this run's reuse figure.
            pool_total = int(pool_delta.get("hits", 0)) + int(pool_delta.get("misses", 0))
            schema_total = int(schema_delta.get("hits", 0)) + int(schema_delta.get("misses", 0))
            pool_delta["reuse_ratio"] = (
                round(pool_delta["hits"] / pool_total, 4) if pool_total else 0.0
            )
            schema_delta["reuse_ratio"] = (
                round(schema_delta["hits"] / schema_total, 4) if schema_total else 0.0
            )
            dest_summary["connection_reuse"] = {
                "engine_pool": pool_delta,
                "schema_cache": schema_delta,
            }
            if pool_delta.get("hits") or schema_delta.get("hits"):
                logger.info(
                    "Connection reuse — engines saved=%s (ratio=%s), "
                    "metadata queries saved=%s (ratio=%s)",
                    pool_delta.get("connections_saved", 0),
                    pool_delta.get("reuse_ratio"),
                    schema_delta.get("metadata_queries_saved", 0),
                    schema_delta.get("reuse_ratio"),
                )
        except Exception as exc:
            logger.debug("connection_reuse summary skipped: %s", exc)
    # Tell the operator whether an interrupted write could have been retried in
    # place. This is the difference between "resume is automatic" and "resume
    # needs a decision", so it belongs in the summary, not only in the logs.
    dest_summary["replay_safety"] = replay_safety.to_dict()
    dest_summary["rejected_rows"] = rejected_total
    dest_summary["coerced_null_rows"] = coerced_null_total
    all_details = list(dest_summary.get("rejected_details") or [])
    # GA: keep full rejected_details for DLQ durability — never truncate bodies
    # before quarantine persist. UI may use rejected_details_sample.
    dest_summary["rejected_details"] = all_details
    dest_summary["rejected_details_sample"] = all_details[:2000]
    dest_summary["rejected_details_total"] = len(all_details)
    # Writer source-minus-kept can exceed the findings the quarantine table
    # stores (reported 7, table 5). The headline is the stored count.
    align_reported_rejects(dest_summary)
    if len(all_details) > 2000:
        dest_summary["rejected_details_sample_capped"] = True
    dest_summary["warnings"] = warning_samples[:10]
    suppressed_warnings = max(len(warning_samples) - 10, 0) + getattr(
        warning_samples, "dropped", 0
    )
    if suppressed_warnings:
        dest_summary["warnings_suppressed"] = suppressed_warnings
    dest_summary["error_policy"] = "quarantine" if (rejected_total or coerced_null_total) else "none"
    dest_summary["sync_mode"] = effective_sync
    stamp_overwrite_source_keys(dest_summary, overwrite_keys_acc)
    if keyed_census_acc is not None:
        census = keyed_census_acc.to_census()
        if census is not None:
            _resume_held = dest_summary.get("resume_rows_before")
            _job_before = dest_summary.get(PRECOUNT_KEY)
            if isinstance(_resume_held, int) and isinstance(_job_before, int):
                census = dataclasses.replace(
                    census, prior_pass_inserts=max(_resume_held - _job_before, 0)
                )
            dest_summary[CENSUS_KEY] = census.to_dict()
    if pk_target_cols:
        dest_summary.setdefault("conflict_columns", list(pk_target_cols))
        dest_summary.setdefault("primary_key_columns", list(pk_target_cols))
    dest_summary["watermark"] = running_cursor
    dest_summary["chunk_size"] = chunk_size
    dest_summary["chunk_policy"] = chunk_policy
    dest_summary["batches"] = batches_completed
    # Gate-8 conservation must come from the *reader*, never written+held_out.
    # Inventing source_row_count from writer ack circularly balances short reads.
    # committed_offset is the authoritative full-stream population (sum of every
    # committed batch's source rows), so it overrides any per-batch writer stamp
    # that may have merged into the summary — the last batch's mapped-row count is
    # not the population.
    # Rows the incremental cursor bound excluded are outside this run's read
    # scope — a pushdown source would never have read them at all — so the
    # population is the delta, not the whole keyspace the page arrived in.
    counted_population = reader_population(
        committed_offset=int(committed_offset or 0),
        resume_offset=population_resume_offset,
        total_rows=total_rows,
        cursor_bounded=int(cursor_bounded_total or 0),
    )
    stamp_source_row_count(
        dest_summary, reader_count=counted_population, rows_written=int(written or 0)
    )
    # RT-7: routed upsert re-runs grow the destination by 0 but still landed
    # their rows — the ledger must read the measured writer tally, not
    # "unmeasured".
    dest_summary["rows_written"] = int(written or 0)
    if cursor_bounded_total:
        dest_summary["rows_cursor_bounded"] = int(cursor_bounded_total)
        ddl_log.append(
            f"INCREMENTAL client-side cursor bound on {cursor_source_col} — "
            f"{cursor_bounded_total:,} row(s) at or before the watermark were "
            f"not in this run's read scope ({src_type} cannot push the bound down)"
        )
    _diverted_total = int(shape_diverted_total[0] or 0)
    if removed_on_read_total or _diverted_total:
        # committed_offset counts the rows the source handed over, so the rows a
        # declared filter or an approved recipe removed are inside that population
        # and have to be named for Gate-8 — otherwise a filtered or shaped run
        # reads as short delivery against the table's own count. A run whose only
        # movement was diverts still has to charge them (DD-3).
        if removed_on_read_total:
            dest_summary["rows_removed_on_read"] = int(removed_on_read_total)
        # Named apart: the operator's declared scope removed these before the
        # recipe was offered them, so they are not the recipe's effect.
        dest_summary["rows_source_filtered"] = int(filtered_on_read_total)
        diverted = _diverted_total
        shaped_removed = max(
            0, removed_on_read_total - filtered_on_read_total - diverted
        )
        if shaped_removed:
            # Every committed page's recipe removals, this pass and the passes
            # before it — the live runner only shaped the pages of this pass.
            dest_summary["rows_shaped_out"] = shaped_removed
        else:
            # The batch writer stamps its own rows_shaped_out including
            # diverted rows; a fully-diverted removal count must be cleared,
            # not left stale (the row is charged to rejected_rows instead).
            dest_summary.pop("rows_shaped_out", None)
        if diverted:
            # A diverted row sits in the DLQ with a reason — it is quarantine,
            # not removal (R2). Charge it to rejected_rows so the ledger's
            # in = out + diverted holds literally.
            dest_summary["rows_diverted"] = diverted
            dest_summary["rejected_rows"] = int(
                dest_summary.get("rejected_rows") or 0
            ) + diverted
    if reconcile_sample:
        dest_summary["reconcile_sample"] = reconcile_sample[:_RECONCILE_SAMPLE_CAP]
    if load_methods_seen:
        if "copy_into" in load_methods_seen:
            dest_summary["load_method"] = "copy_into"
        elif "merge_batch" in load_methods_seen:
            dest_summary["load_method"] = "merge_batch"
        else:
            dest_summary["load_method"] = load_methods_seen[-1]
    ddl_log.insert(1, f"CREATE TABLE IF NOT EXISTS {dest_table}")
    try:
        from services.source_snapshot import get_source_snapshot_meta

        _snap = get_source_snapshot_meta()
        if _snap:
            dest_summary["source_snapshot"] = _snap
            dest_summary.setdefault(
                "source_snapshot_guarantee",
                _snap.get("guarantee") or "unknown",
            )
        elif "source_snapshot" not in dest_summary:
            # Honest weaker guarantee when no engine snapshot was bound.
            dest_summary["source_snapshot"] = {
                "engine": src_type,
                "isolation": "none",
                "guarantee": "not_guaranteed",
                "note": (
                    "No transfer-scoped source snapshot for this engine/mode. "
                    "PostgreSQL/SQLite full-refresh bind REPEATABLE READ / "
                    "transaction snapshots; other engines and incremental sync "
                    "document weaker page-local visibility."
                ),
            }
    except Exception as exc:
        logger.debug("source_snapshot stamp skipped: %s", exc)
    if pin_ledger is not None:
        try:
            _pin_meta = pin_ledger.summary()
            if _pin_meta:
                dest_summary["shape_raw_ledger"] = _pin_meta
        except Exception as exc:
            logger.debug("raw-pin ledger stamp skipped: %s", exc)
    _carry_single_table_foreign_keys(
        source, destination, table, dest_table, mappings, dest_summary, ddl_log
    )
    # The exact type basis the write-pass/re-read fingerprinted on — the dest
    # read-back must hash against it or a correct load mismatches (SQLite TEXT
    # degrade: write pass hashes DECIMAL-declared, naive read-back hashes the
    # TEXT carrier literally).
    dest_summary.setdefault("gate8_digest_types", dict(digest_dest_types))
    from services.null_cursor_probe import stamp_skipped_null_cursor_rows

    stamp_skipped_null_cursor_rows(
        dest_summary, null_cursor_rows_skipped, cursor_source_col
    )
    return written, ddl_log, dest_summary, columns


def _null_cursor_rows_skipped(src_cfg: Any, table: str, cursor_column: str) -> int | None:
    """Source rows a ``cursor > watermark`` read can never see (None = unmeasured)."""
    try:
        from services.null_cursor_probe import probe_null_cursor_count

        probe = probe_null_cursor_count(
            source_config=src_cfg,
            source_table=table,
            source_collection=str((src_cfg or {}).get("collection") or ""),
            cursor_column=cursor_column,
        )
    except Exception:  # noqa: BLE001 — unmeasured is stamped as unmeasured
        return None
    return int(probe.null_count or 0) if probe.ran else None


def _typed_value_source(fmt: str) -> bool:
    try:
        from services.shape_apply import is_typed_value_source

        return is_typed_value_source(fmt)
    except Exception:
        return False


class _NoOpCheckpointService:
    """A checkpoint service that does not persist anything.

    Used for temporary staging transfers so the parent job's checkpoint is not
    overwritten by an internal phase.

    Module 14 honesty: this phase is not crash-recoverable on its own.
    """

    failed_saves = 0
    durable = False
    resume_supported = False

    @property
    def has_failed_saves(self) -> bool:
        return False

    def save(self, checkpoint: Any) -> bool:  # noqa: ARG002
        return True

    def require_save(self, checkpoint: Any) -> None:  # noqa: ARG002
        return None

    def load(self, job_id: str) -> Any | None:  # noqa: ARG002
        return None

    def honesty(self) -> dict:
        from services.execution_engine_contract import noop_checkpoint_posture

        return noop_checkpoint_posture()


def supports_streaming(source: EndpointConfig, destination: EndpointConfig) -> bool:
    if source.kind != "database" or destination.kind != "database":
        return False
    from connectors.sdk import get_descriptor

    source_driver = _stream_source_read_driver(source.format)
    descriptor = get_descriptor(source_driver)
    sdk_source = bool(
        descriptor
        and "source" in descriptor.roles
        and descriptor.catalog_ids
    )
    return (
        (source_driver in _STREAMING_SOURCES or sdk_source)
        and resolve_driver_type(destination.format) in _STREAMING_DESTINATIONS
    )


# Preflight's look-ahead lives in its own module (size budget); re-exported here
# because callers import it from this module.
from .source_peek import peek_stream_source  # noqa: E402,F401 — re-export

# Sequential multi-stream runs live in their own module (size budget) and are
# re-exported here because callers import them from this module.
from .stream_multi import (  # noqa: E402,F401 — re-export
    _drop_destination_endpoint,
    run_non_cdc_multi_stream_sequential,
)


# --------------------------------------------------------------------------- #
# The SCD2 mirror path lives in its own module (Phase F8 size freeze) and is
# re-exported here so every existing import keeps working.
# --------------------------------------------------------------------------- #
from .stream_scd2 import (  # noqa: E402,F401
    _qualified,
    _read_staging_batches,
    _staging_endpoint,
    stream_scd2_mirror_transfer,
)

_STREAM_WRITE_EXPORTS = frozenset(
    {"_raise_write_failure", "_write_batch", "_writer_diagnostics"}
)


def __getattr__(name: str):
    if name in _STREAM_WRITE_EXPORTS:
        from src.transfer import stream_write

        value = getattr(stream_write, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


from src.transfer.stream_kafka import (  # noqa: E402,F401 - re-export
    _kafka_accumulate_offsets,
    _kafka_failure_offset_note,
)


from src.transfer.stream_routes import (  # noqa: E402,F401 - re-export
    _split_routed_batch,
    _purge_route_key_tuples,
)


from src.transfer.stream_pin_ledger import (  # noqa: E402,F401 - re-export
    _StepRawLedger,
    _discard_unconsumed_pin_ledgers,
    _pin_norm,
)
from src.transfer.stream_boundary import (  # noqa: E402,F401 - re-export
    _boundary_append_dedupe,
)
