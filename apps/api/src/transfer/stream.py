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
    record_tombstone_digest_scope,
)
from services.resilience import (  # noqa: E402, F401
    ResilientBatcher,
    adaptive_chunk_size,
)

from .adapters import (
    WriteBatchBlocked,  # noqa: F401 - re-export (stream_write)
    _introspect_table_schema,
    _introspect_table_schema_rich,
    resolve_connector_config,
    resolve_dest_table,
)
from .connector_capabilities import resolve_driver_type, source_read_driver
from .job_quarantine import align_reported_rejects, rebase_batch_rejects
from .stream_row_accounting import (
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
    from services.data_quality import BatchDriftDetector
    from services.error_handling import RetryBudget, with_retry
    from services.parallel_chunks import drive_ordered_batches
    from services.reconciliation import FingerprintAccumulator
    from services.replay_safety import classify_replay_safety
    from services.resilience import adaptive_chunk_size
    from services.row_filter import apply_row_filter_to_matrix
    from services.shape_apply import ShapeRunner
except ImportError:  # pragma: no cover - tests with api root on path
    from src.services.checkpoint_service import Checkpoint, CheckpointService
    from src.services.data_quality import BatchDriftDetector
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
    run = DbRunContext(
        cfg=RunConfig(
            source=source,
            destination=destination,
            job_id=job_id,
            limit=limit,
            schema=schema,
            mappings=mappings,
            shape_runner=shape_runner,
            source_filter=source_filter,
            validation_mode=validation_mode,
            backfill_new_fields=backfill_new_fields,
            skip_preflight=skip_preflight,
            workspace_id=workspace_id,
        ),
        cur=ReadCursors(),
        tally=RunTally(),
    )
    # A recipe that arrives as steps is built after this stream's columns are
    # known. Until then it still has to keep the run off a server-side COPY,
    # which would write the raw rows the operator asked to change.
    pending_shape = run.cfg.shape_runner is not None or bool(shape_steps)
    from .connector_capabilities import resolve_bind_dialect
    run.cfg.src_type = resolve_driver_type(run.cfg.source.format)
    run.cfg.src_read_type = _stream_source_read_driver(run.cfg.source.format)
    run.cfg.dest_type = resolve_driver_type(run.cfg.destination.format)
    from connectors.sdk import get_descriptor

    sdk_descriptor = get_descriptor(run.cfg.src_type)
    run.cfg.sdk_source = bool(
        sdk_descriptor
        and "source" in sdk_descriptor.roles
        and sdk_descriptor.catalog_ids
    )
    run.cfg.initial_sdk_state = (
        getattr(checkpoint, "cursor_value", None) if run.cfg.sdk_source else None
    )
    run.cfg.src_cfg = resolve_connector_config(run.cfg.source)
    run.cfg.dest_cfg = resolve_connector_config(run.cfg.destination)
    # Gate-8 fingerprints must name the dialect the writer binds against, not
    # the driver family: generic_sql hides DuckDB/ClickHouse carrier rules.
    run.cfg.dest_bind_dialect = resolve_bind_dialect(
        run.cfg.destination.format, config_type=str(run.cfg.dest_cfg.get("type") or "")
    )
    from services.procedure_source import is_callable_source, stamp_callable_job_id

    run.cfg.src_cfg = stamp_callable_job_id(run.cfg.src_cfg, run.cfg.job_id)

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
        set_watermark,
        source_bounds_cursor_reads,
    )

    contract = resolve_sync_contract(stream_contracts)
    run.cfg.effective_sync = resolve_effective_sync_mode(
        sync_mode,
        contract.sync_mode if contract else None,
    )
    run.cfg.incremental = requires_incremental(run.cfg.effective_sync)
    run.cfg.cursor_source_col = contract.cursor_field if contract else ""
    run.cfg.pk_target_cols: list[str] = []
    run.cfg.pk_source_cols: list[str] = []
    run.cfg.cursor_pk_source = ""
    if contract and contract.primary_key:
        run.cfg.pk_source_cols = contract.primary_key_columns()
        run.cfg.pk_target_cols = [
            map_source_to_target(col, run.cfg.mappings) for col in run.cfg.pk_source_cols
        ]
    if run.cfg.incremental and run.cfg.cursor_source_col:
        # Tie-break incremental watermarks with a stable source PK (Airbyte
        # gap), or with the engine's own row identity (Mongo ``_id``) when the
        # contract declares none — rows sharing a cursor value across a page
        # edge are never skipped. Same owner as the preflight read scope.
        from services.keyset_pagination import incremental_tiebreak_column

        run.cfg.cursor_pk_source = incremental_tiebreak_column(
            run.cfg.src_read_type, run.cfg.cursor_source_col, run.cfg.pk_source_cols
        )
    if not run.cfg.pk_target_cols:
        from services.copy_kafka_common import kafka_family_name
        if kafka_family_name(run.cfg.dest_type) == "kafka":
            # A Kafka key is partitioning/compaction identity, not a merge
            # constraint — any declared identity (primary_key, upsert_key,
            # dedupe_key, merge_key) is the right produce key (D88).
            from services.sync_cursor import declared_identity_columns
            run.cfg.pk_source_cols = declared_identity_columns(
                SimpleNamespace(
                    stream_contracts=stream_contracts or [],
                    read_options={},
                    source_filter=run.cfg.source_filter or {},
                )
            )
            run.cfg.pk_target_cols = [
                map_source_to_target(c, run.cfg.mappings) for c in run.cfg.pk_source_cols
            ]
    if requires_upsert(run.cfg.effective_sync) and not run.cfg.pk_target_cols:
        # No contract key, but the destination table may declare one. That is
        # catalog evidence, not a guess, so an upsert keyed on it resolves rows
        # on the same identity the destination already enforces.
        run.cfg.pk_source_cols, run.cfg.pk_target_cols = _declared_destination_key_columns(
            run.cfg.dest_type,
            run.cfg.dest_cfg,
            resolve_dest_table(run.cfg.dest_type, run.cfg.destination, _source_name(run.cfg.source)),
            run.cfg.mappings,
        )
    # Source-catalog primary key is read with the schema below. Refusing here
    # made incremental_upsert fail before that evidence existed.
    run.cfg.write_mode = "insert"

    from .stream_dest_procedure import open_destination_row_plan

    dest_proc_plan, run.cfg.row_apply_plan = open_destination_row_plan(run.cfg.destination)

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
    if run.cfg.src_read_type in _PK_INTROSPECT_TYPES:
        try:
            _src_rich_catalog = _introspect_table_schema_rich(
                run.cfg.src_read_type,
                run.cfg.src_cfg,
                _source_name(run.cfg.source),
                [str(m.get("source") or "") for m in run.cfg.mappings if m.get("source")],
            )
        except Exception as exc:
            logger.debug("source schema introspection failed: %s", exc, exc_info=exc)
    if run.cfg.incremental and run.cfg.cursor_source_col and not run.cfg.cursor_pk_source:
        # One tie-break for every read path of this run (QA ACC-02). The COPY
        # fast path and the pre-copy watermark scope run before the row path,
        # so deriving the catalog tie-break only there let run 1 write a
        # composite watermark that run 2's COPY then decoded single-column.
        from services.keyset_pagination import catalog_incremental_tiebreak

        _tb_types, _tb_nulls, _tb_keys = _src_rich_catalog
        run.cfg.cursor_pk_source = catalog_incremental_tiebreak(
            run.cfg.src_read_type,
            run.cfg.cursor_source_col,
            contract_pk=run.cfg.pk_source_cols,
            catalog_pk=list(_tb_keys.get("primary_key_columns") or []),
            unique_keys=list(_tb_keys.get("unique_keys") or []),
            nullable=_tb_nulls,
        )
    if requires_upsert(run.cfg.effective_sync) and not run.cfg.pk_target_cols:
        from services.primary_key import mapped_catalog_upsert_key

        run.cfg.pk_source_cols, run.cfg.pk_target_cols = mapped_catalog_upsert_key(
            (_src_rich_catalog[2] or {}).get("primary_key_columns"),
            run.cfg.mappings,
        )
        if run.cfg.pk_source_cols and run.cfg.incremental and run.cfg.cursor_source_col and not run.cfg.cursor_pk_source:
            run.cfg.cursor_pk_source = incremental_tiebreak_column(
                run.cfg.src_read_type, run.cfg.cursor_source_col, run.cfg.pk_source_cols
            )
    if not run.cfg.pk_target_cols:
        from services.copy_kafka_common import kafka_family_name
        from services.primary_key import mapped_catalog_upsert_key

        if kafka_family_name(run.cfg.dest_type) == "kafka":
            # D88: with no declared identity, the source table's own primary
            # key is the message key; append runs went out key=None, which
            # breaks per-key ordering and log compaction downstream.
            run.cfg.pk_source_cols, run.cfg.pk_target_cols = mapped_catalog_upsert_key(
                (_src_rich_catalog[2] or {}).get("primary_key_columns"),
                run.cfg.mappings,
            )
    if requires_upsert(run.cfg.effective_sync) and not run.cfg.pk_target_cols:
        raise ValueError(
            f"Sync mode `{run.cfg.effective_sync}` requires primary_key for upsert; "
            "refuse silent insert fallback (set primary_key on the stream contract)"
        )
    run.cfg.write_mode = (
        "upsert" if requires_upsert(run.cfg.effective_sync) and run.cfg.pk_target_cols else "insert"
    )
    _create_scope_token, _create_scope = begin_fast_path_create_scope(
        _fast_path_source_catalog(run.cfg.src_read_type, run.cfg.mappings, run.cfg.schema, _src_rich_catalog),
        run.cfg.mappings,
        resolve_dest_table(run.cfg.dest_type, run.cfg.destination, _source_name(run.cfg.source)),
    )
    pre_copy_cursor_key = ""
    pre_copy_watermark = None
    if run.cfg.incremental and run.cfg.cursor_source_col and not pending_shape:
        _tbl = _source_name(run.cfg.source)
        if _tbl and not (is_callable_source(run.cfg.source) or is_callable_source(run.cfg.src_cfg)):
            from services.preflight_cursor_gate import refuse_unusable_cursor_state

            _dest_obj = resolve_dest_table(run.cfg.dest_type, run.cfg.destination, _tbl)
            _scope = resolve_incremental_read_scope(
                sync_mode=run.cfg.effective_sync,
                stream_contracts=stream_contracts,
                source_type=run.cfg.src_read_type,
                source_database=run.cfg.source.database or run.cfg.src_cfg.get("database", ""),
                source_object=_tbl,
                dest_type=run.cfg.dest_type,
                dest_database=run.cfg.destination.database or run.cfg.dest_cfg.get("database", ""),
                dest_object=_dest_obj,
                source=run.cfg.src_cfg,
                destination=run.cfg.destination,
                destination_config=run.cfg.dest_cfg,
            )
            pre_copy_cursor_key = _scope.cursor_key
            pre_copy_watermark = _scope.watermark
            from services.sync_cursor import reconcile_cursor_tiebreak

            run.cfg.cursor_pk_source = reconcile_cursor_tiebreak(_scope, run.cfg.cursor_pk_source)
            refuse_unusable_cursor_state(
                _scope, run.cfg.dest_type, run.cfg.dest_cfg, _dest_obj
            )
    # Gate-8 append proof needs the destination cardinality from before the
    # first write, whichever path performs it. Measured once here, ahead of the
    # COPY fast path, and reused by the row path below. A resumed run already
    # holds rows this job wrote, so its live COUNT is not a "before".
    pre_write_rows_before: int | None = None
    pinned_before = (getattr(run.cfg.destination, "extra", None) or {}).get("overwrite_rows_before")
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
            run.cfg.dest_type,
            run.cfg.dest_cfg,
            resolve_dest_table(run.cfg.dest_type, run.cfg.destination, _source_name(run.cfg.source)),
        )
    # Schema evolution (ADD COLUMN / widen under backfill_new_fields) is owned
    # by the destination writer; a server-to-server copy would write into the
    # destination's current shape and fail on the column the source just grew.
    # An occupied destination under backfill therefore stays on the row path.
    writer_owns_evolution = bool(run.cfg.backfill_new_fields) and pre_write_rows_before is not None
    if writer_owns_evolution and not pending_shape:
        logger.info(
            "COPY fast path declined: backfill_new_fields on an existing %s "
            "destination — schema evolution runs on the writer path",
            run.cfg.dest_type,
        )
    # Write-path referential-integrity guard (QA fk__pg-maria / fk__pg-pg):
    # child rows whose destination parent does not exist are quarantined, not
    # committed. Built before the COPY decision: a server-to-server copy never
    # brings a row into this process, so it cannot hold an orphan back — a
    # guarded route takes the row path instead of landing orphans unchecked.
    # Strict/maximum fail the run on the first orphan before it is committed;
    # balanced/warn quarantine it and Gate-8 G22 reports the count as a warn.
    run.cfg.fk_orphan_guard = None
    try:
        from .stream_foreign_keys import build_fk_orphan_guard

        run.cfg.fk_orphan_guard = build_fk_orphan_guard(
            run.cfg.source,
            run.cfg.destination,
            _source_name(run.cfg.source),
            dest_type=run.cfg.dest_type,
            dest_cfg=run.cfg.dest_cfg,
        )
    except Exception as exc:
        logger.debug("FK orphan guard not built for %s: %s", _source_name(run.cfg.source), exc)
    if run.cfg.fk_orphan_guard is not None:
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
        for m in (run.cfg.mappings or [])
        if isinstance(m, dict) and m.get("risk_contract")
    ]
    if risk_contract_columns:
        logger.info(
            "COPY fast path declined for %s: Risk Contract on %s needs per-row "
            "disposition on the writer path",
            _source_name(run.cfg.source),
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
        run.cfg.write_mode == "upsert"
        and run.cfg.pk_target_cols
        and run.cfg.dest_type == "postgresql"
        and not pending_shape
        and str(run.cfg.validation_mode or "").strip().lower() in {"strict", "maximum", ""}
        and int(pre_write_rows_before or 0) > 0
    ):
        copy_route_backups = _arm_strict_keyed_backup(
            run.cfg.destination,
            dest_type=run.cfg.dest_type,
            dest_cfg=run.cfg.dest_cfg,
            dest_table=resolve_dest_table(run.cfg.dest_type, run.cfg.destination, _source_name(run.cfg.source)),
            job_id=str(run.cfg.job_id or ""),
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
            or run.cfg.fk_orphan_guard is not None
            or bool(risk_contract_columns)
        ) else _try_copy_fast_path(
            source=run.cfg.source,
            destination=run.cfg.destination,
            mappings=run.cfg.mappings,
            schema=run.cfg.schema,
            src_type=run.cfg.src_read_type,
            dest_type=run.cfg.dest_type,
            src_cfg=run.cfg.src_cfg,
            dest_cfg=run.cfg.dest_cfg,
            effective_sync=run.cfg.effective_sync,
            incremental=run.cfg.incremental,
            source_filter=run.cfg.source_filter,
            limit=run.cfg.limit,
            checkpoint=checkpoint,
            incremental_cursor=run.cfg.cursor_source_col if run.cfg.incremental else "",
            incremental_watermark=pre_copy_watermark,
            incremental_pk=(run.cfg.cursor_pk_source or "") if run.cfg.incremental else "",
            incremental_key=(
                (run.cfg.pk_source_cols[0] if run.cfg.pk_source_cols else "")
                if run.cfg.incremental else ""
            ),
            job_id=run.cfg.job_id,
        )
    finally:
        reset_copy_decline_capture(_decline_token)
        reset_fast_path_create_scope(_create_scope_token)
    if fast is not None:
        rows_copied, run.cfg.ddl_log, run.tally.dest_summary, run.cfg.columns = fast
        run.tally.dest_summary["copy_fast_path"] = "used"
        if copy_route_backups:
            run.tally.dest_summary["route_backups"] = dict(copy_route_backups)
        _copy_profile.add(
            PHASE_BULK_COPY,
            time.perf_counter() - _copy_started,
            rows=int(rows_copied or 0),
        )
        run.tally.dest_summary.setdefault("phase_profile", _copy_profile.snapshot())
        _certificate = _create_scope.certificate()
        if _certificate is not None:
            run.tally.dest_summary.setdefault("schema_fidelity", _certificate)
        _fast_census = (run.tally.dest_summary.get("source_snapshot") or {}).get(CENSUS_KEY)
        if _fast_census:
            run.tally.dest_summary.setdefault(CENSUS_KEY, _fast_census)
        _fast_table = _source_name(run.cfg.source)
        _carry_single_table_foreign_keys(
            run.cfg.source,
            run.cfg.destination,
            _fast_table,
            resolve_dest_table(run.cfg.dest_type, run.cfg.destination, _fast_table),
            run.cfg.mappings,
            run.tally.dest_summary,
            run.cfg.ddl_log,
        )
        # Bulk COPY never pages the source; say so instead of leaving the
        # pagination fields absent (which reads as "unknown" to the operator).
        run.tally.dest_summary.setdefault("pagination_mode", "bulk_copy")
        if pre_write_rows_before is not None:
            run.tally.dest_summary.setdefault(PRECOUNT_KEY, int(pre_write_rows_before))
        if run.cfg.incremental:
            run.tally.dest_summary["sync_mode"] = run.cfg.effective_sync
            wm = str(run.tally.dest_summary.get("incremental_watermark") or "").strip()
            if int(rows_copied or 0) == 0:
                stamp_incremental_no_op(run.tally.dest_summary)
                run.tally.dest_summary["watermark"] = pre_copy_watermark
            elif wm and pre_copy_cursor_key:
                set_watermark(
                    pre_copy_cursor_key,
                    wm,
                    metadata={
                        "job_id": run.cfg.job_id,
                        "sync_mode": run.cfg.effective_sync,
                        "cursor_column": run.cfg.cursor_source_col,
                        # The column a composite watermark is decodable on (QA ACC-02).
                        "tiebreak_column": run.cfg.cursor_pk_source,
                    },
                )
                run.tally.dest_summary["watermark"] = wm
                # D110 — if verification removes this run's batch, the route
                # watermark must roll back to this pre-run value.
                run.tally.dest_summary.setdefault(
                    "watermark_restore_point",
                    {
                        "cursor_key": pre_copy_cursor_key,
                        "watermark": pre_copy_watermark,
                    },
                )
            run.tally.dest_summary["cursor_key"] = pre_copy_cursor_key
            if pre_copy_watermark not in (None, "") and run.cfg.cursor_source_col:
                # D99 — the COPY seed drops NULL-cursor rows exactly as the
                # row path's seek does; the job says so on both paths.
                from services.null_cursor_probe import stamp_skipped_null_cursor_rows

                stamp_skipped_null_cursor_rows(
                    run.tally.dest_summary,
                    _null_cursor_rows_skipped(
                        run.cfg.src_cfg, _source_name(run.cfg.source), run.cfg.cursor_source_col
                    ),
                    run.cfg.cursor_source_col,
                )
        return rows_copied, run.cfg.ddl_log, run.tally.dest_summary, run.cfg.columns
    # Parallel/chunked resume is only safe with idempotent writes.
    resuming = bool(checkpoint and getattr(checkpoint, "chunk_index", 0) > 0)
    resume_key_resolved = False
    from services.copy_kafka_common import kafka_family_name as _kafka_family
    if resuming and run.cfg.write_mode == "insert" and _kafka_family(run.cfg.dest_type) != "kafka":
        resume_key_resolved = True
        if not run.cfg.pk_target_cols:
            # No stream contract, but the destination itself may enforce a key.
            # Resuming an append re-delivers the interrupted batch, so honouring
            # that key turns an unavoidable duplicate-key abort into an
            # idempotent apply instead of stranding a half-loaded destination.
            run.cfg.pk_source_cols, run.cfg.pk_target_cols = _declared_destination_key_columns(
                run.cfg.dest_type,
                run.cfg.dest_cfg,
                resolve_dest_table(run.cfg.dest_type, run.cfg.destination, _source_name(run.cfg.source)),
                run.cfg.mappings,
            )
        if run.cfg.pk_target_cols:
            run.cfg.write_mode = "upsert"
        else:
            raise ValueError(
                "Cannot resume a streaming insert without a primary key. "
                "Re-run this transfer with Full refresh · Overwrite to reload it "
                "safely, or set a primary key to make the replay idempotent."
            )
    # Decide up front whether an interrupted batch may be re-sent. Append-only
    # destinations get retries only for failures that prove nothing landed;
    # everything else keeps the full retry budget.
    run.cfg.replay_safety = classify_replay_safety(
        dest_type=run.cfg.dest_type,
        write_mode=run.cfg.write_mode,
        conflict_columns=run.cfg.pk_target_cols or None,
        job_id=run.cfg.job_id,
        has_primary_key=bool(run.cfg.pk_target_cols),
    )
    if run.cfg.src_read_type not in _STREAMING_SOURCES and not run.cfg.sdk_source:
        raise ValueError(f"Streaming source '{run.cfg.src_type}' not supported")
    if run.cfg.dest_type not in _STREAMING_DESTINATIONS:
        raise ValueError(f"Streaming destination '{run.cfg.dest_type}' not supported")

    run.cfg.table = _source_name(run.cfg.source)
    if is_callable_source(run.cfg.source) or is_callable_source(run.cfg.src_cfg):
        from services.procedure_source import (
            parse_callable_source,
            procedure_params_of,
            procedure_text_of,
            source_object_for_cursor,
            source_read_mode_of,
            stream_name_for_callable,
        )

        if not run.cfg.table:
            try:
                spec = parse_callable_source(
                    procedure_text_of(run.cfg.source) or procedure_text_of(run.cfg.src_cfg),
                    dialect=run.cfg.src_read_type,
                    mode=source_read_mode_of(run.cfg.source) or source_read_mode_of(run.cfg.src_cfg),
                    params=procedure_params_of(run.cfg.source) or procedure_params_of(run.cfg.src_cfg),
                )
                run.cfg.table = stream_name_for_callable(spec)
            except Exception:
                run.cfg.table = "procedure_result"
        cursor_source_object = (
            source_object_for_cursor(run.cfg.source)
            or source_object_for_cursor(run.cfg.src_cfg)
            or run.cfg.table
        )
    else:
        cursor_source_object = run.cfg.table
    if not run.cfg.table:
        raise ValueError("Source table/collection name required for streaming transfer")

    run.cfg.watermark = None
    cursor_key = ""
    if run.cfg.incremental and run.cfg.cursor_source_col:
        # One resolver for the whole product: Validate's gates read this route's
        # watermark through the same call, so what they judge is what this run
        # will read. Callable identity includes SQL+binds, not just the stream label.
        scope = resolve_incremental_read_scope(
            sync_mode=run.cfg.effective_sync,
            stream_contracts=stream_contracts,
            source_type=run.cfg.src_read_type,
            source_database=run.cfg.source.database or run.cfg.src_cfg.get("database", ""),
            source_object=cursor_source_object,
            dest_type=run.cfg.dest_type,
            dest_database=run.cfg.destination.database or run.cfg.dest_cfg.get("database", ""),
            dest_object=resolve_dest_table(run.cfg.dest_type, run.cfg.destination, run.cfg.table),
            source=run.cfg.source if is_callable_source(run.cfg.source) or is_callable_source(run.cfg.src_cfg) else run.cfg.src_cfg,
            destination=run.cfg.destination,
            destination_config=run.cfg.dest_cfg,
        )
        cursor_key = scope.cursor_key
        run.cfg.watermark = scope.read_watermark
        from services.preflight_cursor_gate import refuse_unusable_cursor_state
        from services.sync_cursor import reconcile_cursor_tiebreak

        run.cfg.cursor_pk_source = reconcile_cursor_tiebreak(scope, run.cfg.cursor_pk_source)

        refuse_unusable_cursor_state(
            scope, run.cfg.dest_type, run.cfg.dest_cfg, resolve_dest_table(run.cfg.dest_type, run.cfg.destination, run.cfg.table)
        )
    # A source whose read call cannot carry the cursor bound (Redis SCAN, Dynamo
    # Scan, an ES search) hands its whole population over, so the bound is
    # applied to each page instead. Without it "incremental" degraded into a
    # full re-read and the second run appended the population again.
    run.cfg.client_cursor_bound = bool(
        run.cfg.incremental
        and run.cfg.cursor_source_col
        and run.cfg.watermark
        and not source_bounds_cursor_reads(run.cfg.src_read_type)
    )

    # D99: when the source bounds the delta in SQL, ``cursor > watermark``
    # silently drops every NULL-cursor row — the job must say how many it
    # skipped rather than report verified. Not only the pushdown family:
    # SQLite / SQL Server / Oracle reads are SQL-bounded as well (their seek or
    # filtered scan drops NULLs before the client-side bound could refuse
    # them), so the probe runs for every source it can measure; a source that
    # hands NULL-cursor rows over is refused by the client-side bound instead.
    null_cursor_rows_skipped: int | None = None
    if run.cfg.incremental and run.cfg.cursor_source_col and run.cfg.watermark:
        null_cursor_rows_skipped = _null_cursor_rows_skipped(
            run.cfg.src_cfg, cursor_source_object or run.cfg.table, run.cfg.cursor_source_col
        )

    run.cfg.src_db = run.cfg.source.database or run.cfg.src_cfg.get("database") or ("test" if run.cfg.src_type == "mongodb" else "")

    if run.cfg.src_type == "gcs" and not run.cfg.src_cfg.get("database"):
        raise ValueError("GCS source requires bucket name in the database field")
    if run.cfg.src_type == "s3" and not run.cfg.src_cfg.get("database"):
        raise ValueError("S3 source requires bucket name in the database field")

    if run.cfg.src_type in ("s3", "gcs", "adls", "sftp"):
        from services.object_streaming import (
            download_for_object_store,
            download_object,
            stream_spilled_file_to_database,
        )

        bucket = run.cfg.source.database or run.cfg.src_cfg.get("database", "")
        key = run.cfg.table
        if run.cfg.src_type in ("s3", "gcs") and not bucket:
            raise ValueError(f"{run.cfg.src_type.upper()} source requires bucket name in the database field")
        from services.file_pickup import is_file_pattern

        extra = getattr(run.cfg.source, "extra", None) or {}
        wait_seconds = int(extra.get("file_wait_seconds") or 0)
        if is_file_pattern(key) or wait_seconds:
            from services.file_pickup_load import load_picked_files

            return load_picked_files(
                src_type=run.cfg.src_type,
                src_cfg=run.cfg.src_cfg,
                bucket=bucket,
                key=key,
                dest_type=run.cfg.dest_type,
                dest_cfg=run.cfg.dest_cfg,
                source=run.cfg.source,
                destination=run.cfg.destination,
                mappings=run.cfg.mappings,
                schema=run.cfg.schema,
                effective_sync=run.cfg.effective_sync,
                stream_contracts=stream_contracts,
                job_id=run.cfg.job_id,
                checkpoint=checkpoint,
                checkpoint_service=checkpoint_service,
                retry_budget=retry_budget,
                backfill_new_fields=run.cfg.backfill_new_fields,
                validation_mode=run.cfg.validation_mode,
                source_filter=run.cfg.source_filter,
                skip_preflight=run.cfg.skip_preflight,
                shape_runner=run.cfg.shape_runner,
                pre_write_rows_before=pre_write_rows_before,
            )
        cache_key = f"{run.cfg.src_type}:{bucket}:{key}"
        # Always fetch a fresh copy for a transfer run; atomic download protects
        # against partial/corrupt reuse.
        path = download_object(
            cache_key,
            lambda p: download_for_object_store(run.cfg.src_type, p, run.cfg.src_cfg, bucket, key),
            force=True,
        )
        return stream_spilled_file_to_database(
            path=path,
            filename=key,
            destination=run.cfg.destination,
            mappings=run.cfg.mappings,
            schema=run.cfg.schema,
            sync_mode=run.cfg.effective_sync,
            stream_contracts=stream_contracts,
            job_id=run.cfg.job_id,
            checkpoint=checkpoint,
            checkpoint_service=checkpoint_service,
            retry_budget=retry_budget,
            backfill_new_fields=run.cfg.backfill_new_fields,
            validation_mode=run.cfg.validation_mode,
            source_filter=run.cfg.source_filter,
            skip_preflight=run.cfg.skip_preflight,
            shape_runner=run.cfg.shape_runner,
        )

    # Property 3 — one MVCC/transaction snapshot for the whole full-refresh read
    # (sample probe + pages + optional checksum re-read). Incremental syncs
    # intentionally follow watermarks and do not bind a frozen snapshot.
    if not run.cfg.incremental and run.cfg.src_read_type in ("postgresql", "redshift"):
        try:
            from services.source_snapshot import (
                activate_snapshot,
                begin_postgresql_repeatable_read,
                end_postgresql_snapshot,
            )

            _snap_conn, _snap_meta = begin_postgresql_repeatable_read(
                host=str(run.cfg.src_cfg.get("host") or ""),
                port=int(run.cfg.src_cfg.get("port") or (5439 if run.cfg.src_type == "redshift" else 5432)),
                database=str(run.cfg.src_cfg.get("database") or ""),
                username=str(run.cfg.src_cfg.get("username") or ""),
                password=str(run.cfg.src_cfg.get("password") or ""),
                connection_string=str(run.cfg.src_cfg.get("connection_string") or ""),
                ssl=bool(run.cfg.src_cfg.get("ssl", False)),
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
    elif not run.cfg.incremental and run.cfg.src_type == "sqlite":
        try:
            from services.source_snapshot import (
                activate_snapshot,
                begin_sqlite_snapshot,
                end_sqlite_snapshot,
            )

            _snap_conn, _snap_meta = begin_sqlite_snapshot(
                database=str(run.cfg.src_cfg.get("database") or ""),
                connection_string=str(
                    run.cfg.src_cfg.get("connection_string") or run.cfg.src_cfg.get("uri") or ""
                ),
                host=str(run.cfg.src_cfg.get("host") or ""),
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
    elif not run.cfg.incremental and run.cfg.src_type == "snowflake":
        try:
            from services.connector_auth import snowflake_session_kwargs
            from services.source_snapshot import (
                activate_snapshot,
                begin_snowflake_time_travel,
                end_snowflake_time_travel,
            )

            session = snowflake_session_kwargs(run.cfg.src_cfg)
            _snap_conn, _snap_meta = begin_snowflake_time_travel(
                host=str(run.cfg.src_cfg.get("host") or ""),
                database=str(run.cfg.src_cfg.get("database") or ""),
                username=str(run.cfg.src_cfg.get("username") or ""),
                password=str(run.cfg.src_cfg.get("password") or ""),
                schema=str(run.cfg.src_cfg.get("schema") or "PUBLIC"),
                warehouse=str(run.cfg.src_cfg.get("warehouse") or ""),
                connection_string=str(run.cfg.src_cfg.get("connection_string") or ""),
                role=str(session.get("role") or run.cfg.src_cfg.get("role") or ""),
                private_key=str(session.get("private_key") or run.cfg.src_cfg.get("private_key") or ""),
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
    sample_limit = 100 if run.cfg.limit == 0 else min(100, run.cfg.limit)
    # Created before the first read so the sizing probe and the first data page
    # are attributed too. Both are real source reads; leaving them out reported
    # "Reading source 0.0s" on any transfer that fit in a single batch.
    run.cfg.phase_profile = PhaseProfile()
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
    if run.cfg.incremental and run.cfg.cursor_source_col and not run.cfg.cursor_pk_source:
        # The keyset decision later seeks on the catalog primary key. The
        # filtered-scan check has to see that same column or the two owners
        # disagree on the first incremental run (E3-002).
        run.cfg.cursor_pk_source = catalog_incremental_tiebreak(
            run.cfg.src_read_type,
            run.cfg.cursor_source_col,
            contract_pk=run.cfg.pk_source_cols,
            catalog_pk=list(_cat_keys.get("primary_key_columns") or []),
            unique_keys=list(_cat_keys.get("unique_keys") or []),
            nullable=_cat_nulls,
        )
    _cursor_is_unique = cursor_unique_evidence(
        run.cfg.cursor_source_col,
        primary_key_columns=run.cfg.pk_source_cols or (_cat_keys.get("primary_key_columns") or []),
        unique_keys=_cat_keys.get("unique_keys") or [],
        nullable=_cat_nulls,
    )
    run.cfg._filtered_scan_reason = incremental_read_needs_filtered_scan(
        src_type=run.cfg.src_read_type,
        incremental=bool(run.cfg.incremental),
        cursor_column=run.cfg.cursor_source_col,
        tiebreak_column=run.cfg.cursor_pk_source,
        cursor_is_unique=_cursor_is_unique,
        callable_source=bool(is_callable_source(run.cfg.source) or is_callable_source(run.cfg.src_cfg)),
    )
    if run.cfg._filtered_scan_reason:
        logger.info("%s.%s: %s", run.cfg.src_type, run.cfg.table, run.cfg._filtered_scan_reason)

    _sample_started = time.perf_counter()
    # Snowflake snapshot scan opens one warehouse session on the first write
    # page. A separate OFFSET sizing login resumes the warehouse twice — that
    # is how a 150k TPC-H extract spent minutes before the first real page.
    skip_sf_sizing_probe = (run.cfg.src_type == "snowflake" and not run.cfg.incremental) or run.cfg.sdk_source
    if skip_sf_sizing_probe:
        sample_probe = type("SizingSkip", (), {"rows": [], "headers": [], "total_rows": None})()
        sample_rows = []
        avg_row_size = 256
        run.cfg.phase_profile.add(PHASE_READ, time.perf_counter() - _sample_started, rows=0)
    else:
        sample_probe, _ = _unwrap_read(
            _read_batch(
                run.cfg.src_read_type, run.cfg.src_cfg, run.cfg.table, None, 0, sample_limit, database=run.cfg.src_db,
                **_cursor_read_args(run, run.cfg.watermark, run_seed=True),
            )
        )
        run.cfg.phase_profile.add(
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
    if run.cfg.dest_type in _warehouse_dests:
        target_memory_bytes = 64 * 1024 * 1024
    elif run.cfg.dest_type in _sql_tight and avg_row_size >= 4096:
        target_memory_bytes = 4 * 1024 * 1024  # wide-row PG/MySQL ceiling
    elif run.cfg.dest_type in _sql_tight:
        target_memory_bytes = 8 * 1024 * 1024
    else:
        target_memory_bytes = 8 * 1024 * 1024
    requested_chunk = int(CHUNK_SIZE)
    run.cfg.chunk_size = adaptive_chunk_size(CHUNK_SIZE, avg_row_size, max_size=CHUNK_SIZE, target_memory_bytes=target_memory_bytes)
    adaptive_chunk = int(run.cfg.chunk_size)
    proxy_capped = False
    object_store_single_shot = False
    # Cap stream batches on public TCP proxies so one socket never holds a 20k-row window.
    try:
        from connectors.write_resilience import proxy_stream_batch_size
    except ImportError:
        proxy_stream_batch_size = None  # type: ignore
    if proxy_stream_batch_size is not None:
        before_proxy = int(run.cfg.chunk_size)
        run.cfg.chunk_size = proxy_stream_batch_size(
            run.cfg.dest_cfg.get("host"),
            connection_string=run.cfg.dest_cfg.get("connection_string") or run.cfg.dest_cfg.get("uri") or "",
            default=run.cfg.chunk_size,
        )
        proxy_capped = int(run.cfg.chunk_size) < before_proxy
    # Object-store writers (S3/GCS/ADLS) emit a single destination object per call.
    # Chunked writes would overwrite the same key and silently lose data.
    # Always force a single shot — gating on truthy total_rows failed open after
    # honesty waves set total_rows=None for unknown cardinality.
    if run.cfg.dest_type in ("s3", "gcs", "adls"):
        run.cfg.chunk_size = max(1, int(sample_probe.total_rows or 10_000_000))
        object_store_single_shot = True
    chunk_policy = {
        "requested": requested_chunk,
        "adaptive": adaptive_chunk,
        "final": int(run.cfg.chunk_size),
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

    _probe_started = time.perf_counter()
    # Warehouse/SQL snapshot without a keyset must not OFFSET-page (O(n²) +
    # new login per chunk). One SELECT + fetchmany, started on this first page.
    from connectors.sql_snapshot_scan import SNAPSHOT_SCAN_SOURCES as _SNAPSHOT_SCAN_SOURCES

    run.cfg.src_scan: dict[str, Any] = {}
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
    _hold_snapshot = run.cfg.src_read_type in _SNAPSHOT_SCAN_SOURCES and (
        bool(run.cfg._filtered_scan_reason)
        or (
            not run.cfg.incremental
            and not (_is_resume and _resume_bookmark not in (None, ""))
        )
    )
    run.cfg._scan_kw: dict[str, Any] = {"scan_state": run.cfg.src_scan} if _hold_snapshot else {}
    if run.cfg._filtered_scan_reason:
        run.cfg._scan_kw["scan_filter"] = (run.cfg.cursor_source_col, run.cfg.watermark)
        _probe_cursor_kw: dict[str, Any] = {}
    else:
        # The probe page is the run's first written batch, read from the seed.
        _probe_cursor_kw = _cursor_read_args(run, run.cfg.watermark, run_seed=True)
    if run.cfg.sdk_source and initial_batch is not None:
        probe, run.cur.ddb_cursor = initial_batch, None
    else:
        probe, run.cur.ddb_cursor = _unwrap_read(
            _read_batch(
                run.cfg.src_read_type, run.cfg.src_cfg, run.cfg.table, None, 0, _batch_limit(run, 0), database=run.cfg.src_db,
                **_probe_cursor_kw,
                **run.cfg._scan_kw,
                **(
                    {"sdk_state": run.cfg.initial_sdk_state}
                    if run.cfg.sdk_source and run.cfg.initial_sdk_state
                    else {}
                ),
            )
        )
    # This page is not thrown away — it becomes the first written batch.
    run.cfg.phase_profile.add(
        PHASE_READ, time.perf_counter() - _probe_started, rows=len(probe.rows or [])
    )
    run.cfg.columns = probe.headers
    if not run.cfg.columns and run.cfg.src_type == "dynamodb":
        # Empty or key-only Dynamo tables still have a KeySchema — seed from
        # describe so Map/Validate can proceed instead of aborting the demo.
        try:
            from connectors.dynamodb_reader import describe_table_schema

            key_names, key_types = describe_table_schema(run.cfg.src_cfg, run.cfg.table)
            if key_names:
                run.cfg.columns = list(key_names)
                for name, lt in (key_types or {}).items():
                    run.cfg.schema.setdefault(name, lt)
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "DynamoDB empty-probe KeySchema seed failed for %s: %s", run.cfg.table, exc
            )
    if not run.cfg.columns and run.cfg.incremental and run.cfg.schema:
        # A schemaless source names its columns from the documents it returns, so
        # an incremental page with nothing past the watermark comes back with no
        # headers. That is a zero-row run at the current mark, not an empty
        # source: the declared shape carries the run to the no-op close.
        run.cfg.columns = list(run.cfg.schema.keys())
    if not run.cfg.columns:
        raise ValueError(f"Source table `{run.cfg.table}` has no columns or is empty")

    # Schemaless sources — absorb sparse attrs discovered mid-transfer so they
    # are never silently dropped from destination writes (Dynamo/Mongo/ES/Redis).
    #
    # This runs on chunk-writer threads, and it rebinds state the reader thread
    # and sibling chunks read (columns, mappings, column_types, schema,
    # target_cols). Two pages that each discover a new attribute used to
    # interleave their read-modify-write and lose one of the attributes — a
    # silent column drop, which is exactly what this function exists to prevent.
    run.cfg._schema_absorb_lock = threading.Lock()

    if not run.cfg.schema:
        if run.cfg.src_type in ("s3", "gcs", "adls"):
            try:
                from services.object_store_introspect import profile_object_batch
                profiled = profile_object_batch(run.cfg.columns, probe.rows)
                run.cfg.schema = profiled.get("schema") or {c: "string" for c in run.cfg.columns}
            except Exception:
                run.cfg.schema = {c: "string" for c in run.cfg.columns}
        elif run.cfg.src_type == "redis":
            try:
                from services.object_store_introspect import (
                    profile_schemaless_source_schema,
                )

                run.cfg.schema = profile_schemaless_source_schema(
                    run.cfg.columns, probe.rows, source_format="redis"
                ) or {c: "string" for c in run.cfg.columns}
            except Exception:
                run.cfg.schema = {c: "string" for c in run.cfg.columns}
        else:
            # Prefer reader-declared native_types (Kafka, thin SaaS typed flatten,
            # Dynamo) over all-string fiction from a failed SQL introspect.
            probe_meta = getattr(probe, "meta", None) or {}
            native = probe_meta.get("native_types") or probe_meta.get("schema") or {}
            if isinstance(native, dict) and native:
                run.cfg.schema = {c: str(native.get(c) or "string") for c in run.cfg.columns}
            elif run.cfg.src_type == "kafka":
                try:
                    from services.file_parser import FileParser

                    sample_recs = [
                        dict(zip(run.cfg.columns, row)) for row in (probe.rows or [])[:50]
                    ]
                    inferred = FileParser.infer_schema(sample_recs) if sample_recs else {}
                    run.cfg.schema = {c: inferred.get(c, "TEXT") for c in run.cfg.columns}
                except Exception:
                    run.cfg.schema = {c: "TEXT" for c in run.cfg.columns}
            else:
                run.cfg.schema = _introspect_table_schema(run.cfg.src_read_type, run.cfg.src_cfg, run.cfg.table, run.cfg.columns)
                if not run.cfg.schema:
                    run.cfg.schema = {c: "string" for c in run.cfg.columns}

    # Built after schema resolution so ordering steps compare under the
    # source column's declared type, never wire text (DD-1).
    if shape_steps and run.cfg.shape_runner is None:
        from .stream_dest_procedure import runner_for_stream_steps

        run.cfg.shape_runner = runner_for_stream_steps(
            shape_steps,
            columns=list(run.cfg.columns),
            table=run.cfg.table,
            effective_sync=run.cfg.effective_sync,
            cursor_field=run.cfg.cursor_source_col or "",
            key_columns=run.cfg.pk_source_cols,
            workspace_id=run.cfg.workspace_id,
            source_types=dict(run.cfg.schema or {}),
            typed_values=_typed_value_source(run.cfg.src_read_type),
        )
    # G-1d: pin every keyed raw row the write consumed so the post-write
    # proof can pin source stability per key instead of inferring drift.
    # The ledger key resolves the same way the engine keys elsewhere:
    # the declared contract PK, then the source catalog PRIMARY KEY, then
    # a fully non-null enforced unique key. Keyless sources keep the
    # proof's multiset fallback (G-1f).
    ledger_key_cols = [c for c in (run.cfg.pk_source_cols or []) if c in run.cfg.columns]
    if not ledger_key_cols:
        ledger_key_cols = [
            c for c in (_cat_keys.get("primary_key_columns") or [])
            if c in run.cfg.columns
        ]
    if not ledger_key_cols:
        ledger_key_cols = list(
            safe_keyset_unique_columns(
                _cat_keys.get("unique_keys") or [],
                list(run.cfg.columns or []),
                _cat_nulls,
            )
            or []
        )
    run.cfg.pin_ledger = (
        _StepRawLedger(
            ledger_key_cols,
            col_ddl={c: str(run.cfg.schema.get(c, "")) for c in run.cfg.columns},
        )
        if run.cfg.shape_runner is not None and ledger_key_cols
        else None
    )

    # ``columns`` stays the source's own column list: it is what every read
    # projects on. Under a recipe the writer sees different columns, so the
    # carriers, the mappings and the DDL are decided from the recipe's output.
    write_columns = list(run.cfg.shape_runner.output_columns or run.cfg.columns) if run.cfg.shape_runner else run.cfg.columns
    run.cfg.column_types = {c: ddl_carrier_type(run.cfg.schema.get(c, "string")) for c in write_columns}
    from .stream_dest_procedure import adopt_stream_column_map

    run.cfg.mappings, inherit_note = adopt_stream_column_map(
        run.cfg.mappings, write_columns, inherited=mappings_inherited
    )
    if not run.cfg.mappings:
        run.cfg.mappings = [{"source": c, "target": c, "confidence": 0.95} for c in write_columns]
    from services.shape_contract import write_ready_mappings

    # Pending extras and intentional omits stay off INSERT/MERGE (name-addressed).
    run.cfg.mappings = write_ready_mappings(run.cfg.mappings)
    # Schemaless sources: always allow ADD COLUMN when attrs appear mid-transfer.
    if run.cfg.src_type in ("dynamodb", "mongodb", "elasticsearch", "redis", "kafka"):
        run.cfg.backfill_new_fields = True
        probe_meta = getattr(probe, "meta", None) or {}
        for name, lt in (probe_meta.get("native_types") or {}).items():
            if name in run.cfg.schema and run.cfg.schema[name] in {"string", "VARCHAR", "TEXT", "S"}:
                run.cfg.schema[name] = str(lt)
                run.cfg.column_types[name] = ddl_carrier_type(str(lt))
    run.cfg.target_cols, logical_types = resolve_target_columns(
        run.cfg.mappings, run.cfg.column_types, preserve_case=True
    )
    # Write-path dest_types for Gate-8 fingerprint remap parity — including the
    # drift widen the backfill write applies, so the digest is taken against the
    # carrier the rows actually land in.
    from connectors.writer_common import effective_dest_types_under_backfill

    run.cfg.fingerprint_dest_types = effective_dest_types_under_backfill(
        {run.cfg.target_cols[i]: logical_types[i] for i in range(len(run.cfg.target_cols))},
        list(run.cfg.mappings or []),
        backfill=bool(run.cfg.backfill_new_fields),
    )

    run.cfg.total_rows = probe.total_rows
    if run.cfg.total_rows is not None and run.cfg.limit > 0:
        run.cfg.total_rows = min(run.cfg.total_rows, run.cfg.limit)

    checkpoint_service = checkpoint_service or CheckpointService()
    checkpoint = checkpoint or Checkpoint(job_id=run.cfg.job_id or "")
    checkpoint.source_type = run.cfg.src_type
    checkpoint.dest_type = run.cfg.dest_type
    checkpoint.write_mode = run.cfg.write_mode
    checkpoint.conflict_columns = run.cfg.pk_target_cols or []

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
    remaining_rows = max(0, (run.cfg.total_rows or 0) - rows_accounted_for)
    remaining_chunks = (remaining_rows + run.cfg.chunk_size - 1) // run.cfg.chunk_size if remaining_rows else 0
    if not run.cfg.total_rows and (run.cur.ddb_cursor or (run.cfg.src_type == "kafka" and probe.rows)):
        remaining_chunks = max(remaining_chunks, 1)
    run.cfg.chunks = max(1, completed_chunks + (1 if chunk_idx == 0 and resume_offset == 0 else 0) + remaining_chunks)
    run.cfg.dest_table = resolve_dest_table(run.cfg.dest_type, run.cfg.destination, run.cfg.table)
    # An existing destination column decides what an instant keeps, not the plan,
    # and this digest must be taken at that granularity or the destination re-read
    # disagrees with it and a correct load fails Gate-8 on two opaque hashes.
    # Kept separate from the remap types on purpose: those drive the write-path
    # quarantine matrix, where a declared precision means "refuse to truncate" —
    # feeding the carrier's precision into it would hold out every row whose
    # fractional seconds the carrier rounds, which is a narrowing to disclose,
    # not a row to reject.
    run.cfg.digest_dest_types = dict(run.cfg.fingerprint_dest_types)
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
        run.cfg.digest_dest_types = overlay_physical_dest_types(
            run.cfg.digest_dest_types,
            physical_column_types(
                run.cfg.dest_type, run.cfg.dest_cfg, table=str(run.cfg.dest_table), columns=list(run.cfg.target_cols)
            ),
        )
        run.cfg.digest_dest_types = apply_physical_temporal_precision(
            run.cfg.digest_dest_types, run.cfg.dest_type, run.cfg.dest_cfg, table=str(run.cfg.dest_table)
        )
    except Exception as exc:
        logger.warning(
            "physical destination temporal precision unavailable for the write-pass "
            "digest: %s",
            exc,
            exc_info=exc,
        )

    run.cfg.ddl_log: list[str] = [
        f"STREAM {run.cfg.src_type}.{run.cfg.table} → {run.cfg.dest_type}.{run.cfg.dest_table} "
        f"({('unknown' if run.cfg.total_rows is None else f'{run.cfg.total_rows:,}')} rows est., "
        f"{run.cfg.chunks} batches, sync={run.cfg.effective_sync})"
    ]
    if run.cfg.incremental and run.cfg.watermark:
        run.cfg.ddl_log.append(f"INCREMENTAL cursor {run.cfg.cursor_source_col} > {run.cfg.watermark}")
    for col in write_columns:
        run.cfg.ddl_log.append(f"{run.cfg.dest_type.upper()} COLUMN {col} {ddl_type(run.cfg.dest_type, run.cfg.schema.get(col, 'string'))}")
    if run.cfg.shape_runner is not None:
        run.cfg.ddl_log.append(
            f"SHAPE recipe {run.cfg.shape_runner.recipe_hash} applied on the read: "
            f"{run.cfg.shape_runner.recipe.describe()}"
        )
    if inherit_note:
        run.cfg.ddl_log.append(inherit_note)

    run.tally.written = checkpoint.rows_processed or 0
    offset = checkpoint.offset or 0
    # A resumed pass only ever fingerprints the rows it writes, while Gate-8
    # compares against the whole destination — the write-pass digest is not a
    # population checksum for this run.
    resumed_pass = bool(run.tally.written or offset)
    run.tally.dest_summary: dict[str, Any] = {}
    # Failed-Gate-8 restore guard: renamed-aside route tables (armed once,
    # inside the held txn) — stamped onto dest_summary at stream end.
    run.tally.route_backup_state: dict[str, Any] = {}
    # R2: shape-diverted rows are quarantine, not removal — counted here and
    # stamped as rejected_rows so the ledger reads in = out + diverted.
    run.tally.shape_diverted_total: list[int] = [0]
    # Keys of rows the recipe diverted (unmatched=divert etc.): keyed routed
    # runs delete these from every route table so a key that stopped matching
    # loses its stale copy (P1-B).
    run.tally.shape_divert_key_tuples: list[tuple] = []
    # Defensive early binding — _persist_shape_diverts is a closure that can
    # run before the pk-audit assignment later in this function
    # (pk_source_cols is already bound unconditionally near line 630).
    run.cfg.pk_audit_cols: list[str] = []
    if copy_decline:
        run.tally.dest_summary["copy_fast_path"] = "declined"
        run.tally.dest_summary["copy_decline_reason"] = copy_decline[0]
        run.tally.dest_summary["copy_decline_reasons"] = copy_decline[:8]
    # Gate-8 append proof needs the cardinality from before the first batch. A
    # resumed run already appended rows, so its count is not a "before" and the
    # delta stays unproven rather than being reported wrong.
    if not (run.tally.written or offset):
        rows_before = pre_write_rows_before
        _rstep0 = (
            run.cfg.shape_runner.recipe.router_step
            if run.cfg.shape_runner is not None and getattr(run.cfg.shape_runner, "recipe", None) is not None
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
            _rbase0 = resolve_dest_table(run.cfg.dest_type, run.cfg.destination, _source_name(run.cfg.source))
            _rsum0 = 0
            _rper0: dict[str, int] = {}
            for _rroute in _rstep0.options.get("routes") or []:
                _rname0 = str(_rroute.get("name") or "")
                if not _rname0:
                    continue
                _rtable0 = str(_rtargets0.get(_rname0) or f"{_rbase0}__{_rname0}")
                _rn0 = precount_table(run.cfg.dest_type, run.cfg.dest_cfg, _rtable0)
                if _rn0 is None:
                    # One unreadable route table keeps the sum honest: report
                    # no "before" at all rather than a wrong one.
                    _rsum0 = None
                    break
                _rsum0 += int(_rn0)
                _rper0[_rname0] = int(_rn0)
            rows_before = _rsum0
            if _rper0:
                run.tally.dest_summary["route_precount"] = dict(_rper0)
                run.tally.route_backup_state["precount"] = dict(_rper0)
                # RT-6: an append into an occupied route table cannot be
                # proven by the canonical whole-table digest, but it can by
                # the additive multiset digest: post − pre must equal this
                # run's digest. Measure the pre-write additive sum for each
                # occupied route table now (postgres only — other engines
                # stay honestly "not comparable").
                if run.cfg.dest_type == "postgresql" and not is_overwrite_sync(
                    str(run.cfg.effective_sync or "")
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
                            host=str(run.cfg.dest_cfg.get("host") or ""),
                            port=int(run.cfg.dest_cfg.get("port") or 5432),
                            database=str(run.cfg.dest_cfg.get("database") or ""),
                            username=str(
                                run.cfg.dest_cfg.get("username") or run.cfg.dest_cfg.get("user") or ""
                            ),
                            password=str(run.cfg.dest_cfg.get("password") or ""),
                            schema=str(run.cfg.dest_cfg.get("schema") or "public"),
                            connection_string=str(
                                run.cfg.dest_cfg.get("connection_string") or ""
                            ),
                            ssl=bool(run.cfg.dest_cfg.get("ssl") or run.cfg.dest_cfg.get("sslmode")),
                            table_name=_ptable,
                            # Digest only the columns the run writes — a
                            # customer column (default/generated) must not
                            # enter the additive delta.
                            target_columns=list(
                                run.cfg.shape_runner.recipe.route_output_columns(_rname1)
                            ),
                        )
                        if _padd:
                            _rpred[_rname1] = {
                                "rows": int(_rn1),
                                "additive": _padd,
                            }
                    if _rpred:
                        run.tally.dest_summary["route_predigest"] = _rpred
        if rows_before is not None:
            run.tally.dest_summary[PRECOUNT_KEY] = int(rows_before)
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
                    run.cfg.dest_type,
                    run.cfg.dest_cfg,
                    schema=schema_from_cfg(run.cfg.dest_type, run.cfg.dest_cfg),
                    table_name=str(run.cfg.dest_table),
                )
                if _existed is not None:
                    run.tally.dest_summary["dest_table_existed_before"] = bool(_existed)
                    _dextra = getattr(run.cfg.destination, "extra", None)
                    if isinstance(_dextra, dict):
                        _dextra["dest_table_existed_before"] = bool(_existed)
                if rows_before is not None:
                    _dextra = getattr(run.cfg.destination, "extra", None)
                    if isinstance(_dextra, dict):
                        _dextra["dest_rows_before"] = int(rows_before)
            except Exception:  # noqa: BLE001 - probe failure must not abort the run
                pass
    elif checkpoint.target_rows_before is not None:
        # Resume: restore the first-pass dest-before. A live COUNT now includes
        # rows this job already wrote and is not a pre-write cardinality.
        run.tally.dest_summary[PRECOUNT_KEY] = int(checkpoint.target_rows_before)
    if resumed_pass:
        # D134 — but it is what a failed *resumed* pass may not undo: rows an
        # earlier pass committed (reported as kept when it was cancelled) are
        # not this pass's batch, so the undo must never clear or drop them.
        try:
            _resume_live = precount_table(run.cfg.dest_type, run.cfg.dest_cfg, str(run.cfg.dest_table))
        except Exception:  # noqa: BLE001 - unknown stays unknown (undo retains)
            _resume_live = None
        run.tally.dest_summary["resume_rows_before"] = _resume_live
    # A key-addressed destination replaces the value held at the key on every
    # write, so even an append needs the dest-engine census (new keys vs
    # replaced keys) to close conservation — COUNT(*) growth alone reads a
    # correct re-write of the same keys as silent loss.
    dest_key_addressed = dest_is_key_addressed(run.cfg.dest_type, run.cfg.pk_target_cols)
    if dest_key_addressed:
        run.tally.dest_summary[KEY_ADDRESSED_KEY] = True
    keyed_upsert_scope = run.cfg.write_mode == "upsert" and bool(run.cfg.pk_target_cols)
    # D111 — strict keyed upsert on a populated PG destination: row-copy the
    # pre-run table to __dfbak_<job> so a failed verification restores the
    # exact prior state (DELETE + INSERT FROM backup) instead of leaving the
    # rejected batch in place. Armed only on the first pass — a resume must
    # not snapshot mid-run state — and only where undo is contractually
    # allowed (strict family). Other engines/modes keep the honest
    # "left in place" report.
    if (
        keyed_upsert_scope
        and run.cfg.dest_type == "postgresql"
        and not resumed_pass
        and str(run.cfg.validation_mode or "").strip().lower() in {"strict", "maximum", ""}
        and int(run.tally.dest_summary.get(PRECOUNT_KEY) or 0) > 0
        and (run.cfg.shape_runner is None or run.cfg.shape_runner.recipe.router_step is None)
    ):
        _route_backups = _arm_strict_keyed_backup(
            run.cfg.destination,
            dest_type=run.cfg.dest_type,
            dest_cfg=run.cfg.dest_cfg,
            dest_table=str(run.cfg.dest_table),
            job_id=str(run.cfg.job_id or ""),
        )
        if _route_backups:
            run.tally.dest_summary["route_backups"] = dict(_route_backups)
    run.tally.keyed_census_acc = (
        KeyCensusAccumulator()
        if run.cfg.pk_target_cols and (keyed_upsert_scope or dest_key_addressed)
        else None
    )
    run.tally.overwrite_keys_acc = begin_overwrite_source_keys(
        run.cfg.effective_sync, run.cfg.pk_target_cols, resumed=resumed_pass
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
    run.tally.inline_fingerprints_used = not (
        should_reread_source(
            src_type=run.cfg.src_read_type,
            dest_type=run.cfg.dest_type,
            incremental=run.cfg.incremental,
            partial_write_pass=False,
        )
        and run.cfg.src_read_type in REREAD_SCAN_SOURCES
        # M3 — an incremental write pass is batch-sized, so keeping its
        # fingerprints is cheap and they are the fallback digest when the
        # re-read window cannot be bounded (no cursor column / no observed
        # max). Whether they are used is decided at reconcile time.
        and not run.cfg.incremental
    )
    # Restore cumulative quarantine counts on resume — Gate-8 conservation is
    # source - (rejected - coerced_null) - skipped. committed_offset already spans
    # the full population on resume, so these counters must be cumulative too or a
    # correct resumed load that quarantined rows in the first pass fails Gate-8.
    run.tally.rejected_total = int(getattr(checkpoint, "rejected_rows", 0) or 0) if resumed_pass else 0
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
    run.cfg.stream_error_policy = transform_error_policy_for_validation_mode(run.cfg.validation_mode)
    # Bounded: warnings arrive per batch, so an uncapped list grows with the
    # batch count on large transfers. Truncating at emit time bounded the output
    # but never the memory.
    warning_samples: BoundedStrings = BoundedStrings(cap=MAX_WARNING_SAMPLES)
    run.cur.ddb_total = probe.total_rows if run.cfg.src_type == "dynamodb" else None
    running_cursor = checkpoint.cursor_value if checkpoint.cursor_value is not None else run.cfg.watermark
    run.cur.es_search_after = checkpoint.es_search_after or (run.cur.ddb_cursor if run.cfg.src_type == "elasticsearch" else None)
    run.cur.redis_scan_state = checkpoint.redis_scan_state or (run.cur.ddb_cursor if run.cfg.src_type == "redis" else None)
    run.cur.qdrant_offset = checkpoint.qdrant_offset if run.cfg.src_type == "qdrant" else None
    # Preserve probe continuation tokens when checkpoint has none (fresh job).
    probe_continuation = run.cur.ddb_cursor
    # Keyset (seek) pagination — Phase F2.
    # Bookmark the full primary key (including composite) so page boundaries
    # cannot skip tied leading-key values. Without a PK, fall back to OFFSET —
    # quadratic and slow, but it cannot skip rows.
    from services.keyset_pagination import (
        KEYSET_CAPABLE_SOURCES,
        decide_keyset_pagination,
        safe_keyset_unique_columns,
    )

    keyset_pk_cols = [c for c in run.cfg.pk_source_cols if c and c in run.cfg.columns]
    pagination_warning = ""
    _pk_introspect_types = _PK_INTROSPECT_TYPES
    # Property 6 — source catalog for create-new fidelity (any SQL sink that
    # consumes it). Introspected once ahead of the COPY fast path (whose CREATE
    # answers to the same planner); the paged read reuses that catalog here.
    run.cfg.source_schema_catalog: dict[str, Any] | None = None
    _src_schema_types, _src_schema_nulls, _src_keys = _src_rich_catalog
    if run.cfg.src_read_type in _pk_introspect_types:
        try:
            if not keyset_pk_cols:
                keyset_pk_cols = [
                    c for c in (_src_keys.get("primary_key_columns") or []) if c in run.cfg.columns
                ]
            if not keyset_pk_cols:
                keyset_pk_cols = safe_keyset_unique_columns(
                    _src_keys.get("unique_keys") or [],
                    run.cfg.columns,
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
    if run.cfg.src_type == "mongodb" and run.cfg.columns and "_id" in run.cfg.columns:
        if "_id" not in keyset_pk_cols:
            keyset_pk_cols = ["_id"] + [c for c in keyset_pk_cols if c != "_id"]
    if _src_keys or _src_schema_nulls or _src_schema_types:
        try:
            from services.schema_fidelity import (
                build_catalog_from_introspect,
                catalog_to_payload,
            )

            run.cfg.source_schema_catalog = catalog_to_payload(
                build_catalog_from_introspect(
                    dialect=run.cfg.src_read_type,
                    columns=list(run.cfg.columns or []),
                    column_types=_src_schema_types or dict(run.cfg.schema or {}),
                    nullable=_src_schema_nulls,
                    keys=_src_keys,
                )
            )
        except Exception as exc:
            logger.debug("source schema catalog build failed: %s", exc, exc_info=exc)
    # Ordered composite key for seek (all PK parts). Legacy single + tie-break
    # still work when only one/two columns are available.
    run.cfg.keyset_order_cols = list(keyset_pk_cols)
    run.cfg.keyset_col = checkpoint.cursor_column or (
        run.cfg.keyset_order_cols[0]
        if run.cfg.keyset_order_cols
        else (run.cfg.columns[0] if run.cfg.columns and not run.cfg.incremental else "")
    )
    if run.cfg.incremental and run.cfg.cursor_source_col:
        # An incremental run seeks on the column its watermark was taken on.
        # Seeking a different column silently drops the watermark: the seek
        # branch carries the bookmark *as* the read filter, so a source paged
        # by seek (SQL Server / Oracle) re-read the whole table every run and
        # an append destination died on its own primary key.
        run.cfg.keyset_col = run.cfg.cursor_source_col
        run.cfg.keyset_order_cols = [run.cfg.cursor_source_col] + [
            c for c in run.cfg.keyset_order_cols if c != run.cfg.cursor_source_col
        ]
    run.cfg.keyset_tiebreak = next((c for c in run.cfg.keyset_order_cols if c != run.cfg.keyset_col), "")
    run.cur.keyset_after = None if run.cfg.sdk_source else checkpoint.cursor_value
    run.cur.keyset_run_seed = False
    if run.cur.keyset_after in (None, "") and run.cfg.incremental and run.cfg.keyset_col == run.cfg.cursor_source_col:
        # D60/D98 — a run seeded from the watermark re-reads the whole tie
        # group at the watermark (cursor >= watermark). Page edges advanced
        # inside the run keep the exclusive seek — only this seed is inclusive.
        run.cur.keyset_after = run.cfg.watermark
        run.cur.keyset_run_seed = True
    # The bookmark this run started from — the digest window's lower bound.
    # ``keyset_after`` itself advances per committed page, so it must be
    # captured here; the seed is checkpoint.resume or the stored watermark.
    incremental_seed_bookmark = run.cur.keyset_after if run.cur.keyset_after not in (None, "") else run.cfg.watermark
    # Captured once: ``checkpoint.cursor_value`` advances as batches commit. A
    # run seeded from the stored watermark (not a resume from a committed page
    # edge) owns the whole tie group at that watermark.
    seeded_from_watermark = bool(
        run.cfg.incremental
        and run.cfg.cursor_source_col
        and run.cfg.watermark not in (None, "")
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
    job_start_watermark = run.cfg.watermark
    if run.cfg.incremental and run.cfg.cursor_source_col:
        if resumed_pass and checkpoint.incremental_seed is not None:
            job_start_watermark = checkpoint.incremental_seed or None
            window_seed_bookmark = job_start_watermark
            window_seed_inclusive = job_start_watermark is not None
        elif not resumed_pass:
            checkpoint.incremental_seed = str(run.cfg.watermark or "")
    # D124/D60/D98 — every read seeded from the watermark re-reads its whole
    # tie group, whichever branch pages the source. Peers an earlier run
    # already delivered are dropped against the destination's pre-run rows at
    # the watermark cursor. A keyed append matches by mapped key (a row
    # restamped to the watermark from an older cursor is a new version and
    # still lands — COPY's boundary_append_insert_sql rule). An upsert or an
    # unkeyed append matches full-row content: an upsert still rewrites a peer
    # whose content changed, and a quiet poll stays a no-op, so its covered-
    # content proof still runs. The verify re-read drops the same rows.
    run.cfg._tie_dedupe = seeded_from_watermark
    run.cur._boundary_fps_state: dict[str, Any] = {
        "fps": None,
        "match_cols": [],
        "new_cols": [],
    }

    decision = decide_keyset_pagination(
        src_type=run.cfg.src_read_type,
        keyset_order_cols=run.cfg.keyset_order_cols,
        keyset_col=run.cfg.keyset_col,
        keyset_tiebreak=run.cfg.keyset_tiebreak,
        incremental=bool(run.cfg.incremental),
        offset=offset,
        chunk_index=chunk_idx,
        cursor_after=run.cur.keyset_after,
        snapshot_scan=bool(run.cfg.src_scan),
        cursor_is_unique=_cursor_is_unique,
        callable_source=bool(is_callable_source(run.cfg.source) or is_callable_source(run.cfg.src_cfg)),
    )
    run.cfg.use_keyset = decision.use_keyset
    if run.cfg._filtered_scan_reason and run.cfg.use_keyset:
        raise RuntimeError(
            "pagination owners disagree: the read opened a filtered snapshot scan "
            "but the keyset decision would seek on "
            f"{run.cfg.keyset_order_cols!r} — refusing rather than risk skipped rows"
        )
    run.cfg.keyset_order_cols = decision.order_cols
    _resume_scan_aligned = False
    if decision.resume_fallback:
        if run.cfg.src_scan.get("started"):
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
    if run.cfg.use_keyset and run.cfg.src_scan.get("started"):
        from connectors.sql_snapshot_scan import (
            close_table_scan,
            scan_order_supports_seek,
        )

        if scan_order_supports_seek(run.cfg.src_scan.get("order_cols"), run.cfg.keyset_order_cols):
            # First page already landed from the snapshot scan; later pages seek.
            close_table_scan(run.cfg.src_scan)
        else:
            # The scan's ORDER BY is not the seek order (or is unpublished):
            # seeking past its first page would skip every row the scan order
            # placed before the page edge. Keep paging the held snapshot.
            logger.warning(
                "Snapshot scan on %s.%s is ordered by %r but the keyset seek would "
                "run on %r — continuing the held scan instead of seeking.",
                run.cfg.src_type,
                run.cfg.table,
                run.cfg.src_scan.get("order_cols"),
                run.cfg.keyset_order_cols,
            )
            run.cfg.use_keyset = False
            pagination_mode = "scan"
    elif (
        _is_resume
        and not run.cfg.use_keyset
        and run.cfg.src_scan.get("started")
        and offset > 0
    ):
        from connectors.sql_snapshot_scan import align_snapshot_resume

        def _resume_scan_next():
            nxt, _extra = _unwrap_read(
                _read_batch(
                    run.cfg.src_read_type,
                    run.cfg.src_cfg,
                    run.cfg.table,
                    run.cfg.columns,
                    offset,
                    _batch_limit(run, offset),
                    database=run.cfg.src_db,
                    known_total_rows=run.cfg.total_rows,
                    scan_state=run.cfg.src_scan,
                )
            )
            return nxt

        probe = align_snapshot_resume(probe, offset, _resume_scan_next)
        _resume_scan_aligned = True
    # Phase F3 — PostgreSQL COPY TO STDOUT bulk export (full refresh, no filter).
    run.cfg.bulk_export_iter = None
    try:
        from connectors.bulk_export import (
            bulk_export_enabled,
            bulk_export_forced,
            bulk_export_implemented,
            iter_postgresql_copy_batches,
        )

        if (
            bulk_export_enabled()
            and bulk_export_implemented(run.cfg.src_read_type)
            and not run.cfg.incremental
            and not _is_resume
            and not run.cfg.source_filter
            and not run.cfg.limit
            and not is_callable_source(run.cfg.source)
            and not is_callable_source(run.cfg.src_cfg)
        ):
            try:
                run.cfg.bulk_export_iter = iter_postgresql_copy_batches(
                    host=run.cfg.src_cfg.get("host", ""),
                    port=int(run.cfg.src_cfg.get("port") or 5432),
                    database=run.cfg.src_cfg.get("database", ""),
                    username=run.cfg.src_cfg.get("username", ""),
                    password=run.cfg.src_cfg.get("password", ""),
                    schema=run.cfg.src_cfg.get("schema", "public"),
                    connection_string=run.cfg.src_cfg.get("connection_string", ""),
                    ssl=bool(run.cfg.src_cfg.get("ssl", False)),
                    table=run.cfg.table,
                    columns=run.cfg.columns,
                    batch_rows=max(1, int(run.cfg.chunk_size)),
                )
                pagination_mode = "bulk_copy"
                run.cfg.use_keyset = False
                logger.info(
                    "Bulk export (COPY TO STDOUT) enabled for %s.%s — skipping OFFSET/keyset pages",
                    run.cfg.src_type,
                    run.cfg.table,
                )
            except Exception as exc:
                if bulk_export_forced():
                    raise
                logger.warning(
                    "Bulk export unavailable for %s.%s (%s) — falling back to %s",
                    run.cfg.src_type,
                    run.cfg.table,
                    exc,
                    "keyset" if run.cfg.use_keyset else "offset",
                )
                run.cfg.bulk_export_iter = None
        elif (
            bulk_export_forced()
            and run.cfg.src_read_type in ("snowflake", "bigquery")
            and not run.cfg.incremental
        ):
            raise NotImplementedError(
                f"Bulk export forced for {run.cfg.src_type} but unload/Storage Read is not "
                "wired in this build — unset DATAFLOW_BULK_EXPORT or wait for F3 complete."
            )
    except ImportError:
        run.cfg.bulk_export_iter = None

    if run.cfg.keyset_col and not run.cfg.use_keyset and run.cfg.src_read_type in KEYSET_CAPABLE_SOURCES:
        logger.info(
            "Keyset pagination disabled for %s.%s — no unique bookmark (declare a "
            "primary_key on the stream contract to re-enable seek reads). Falling "
            "back to %s.",
            run.cfg.src_type,
            run.cfg.table,
            "snapshot scan" if run.cfg.src_scan else "OFFSET pagination",
        )
    elif not run.cfg.use_keyset:
        logger.info(
            "%s pagination for %s.%s — no keyset-capable unique bookmark "
            "(quadratic cost on large tables when OFFSET).",
            "Snapshot-scan" if run.cfg.src_scan else "OFFSET",
            run.cfg.src_type,
            run.cfg.table,
        )
    run.cur.ddb_cursor = checkpoint.dynamodb_cursor or (probe_continuation if run.cfg.src_type == "dynamodb" else None)
    run.cur.kafka_cursor = checkpoint.kafka_cursor or (probe_continuation if run.cfg.src_type == "kafka" else None)

    run.cfg.retry = retry_budget or RetryBudget()

    # Separate fetch cursors from committed checkpoint cursors so we can read
    # ahead in parallel while only persisting durable offsets after a batch is
    # successfully written.
    run.cur.fetch_cursor = running_cursor
    # True until the first page read from the stored watermark has been taken:
    # that seed read re-reads the tie group (``>=``), every later page seeks
    # strictly past the edge this run advanced to.
    run.cur.fetch_from_seed = seeded_from_watermark
    # `fetch_offset` doubles as the source-row budget counter (`limit` /
    # `total_rows`). A keyset resume seeks by bookmark, and that bookmark sits at
    # or behind the committed offset, so the rows it re-reads would be charged to
    # the budget twice and the run would stop short of the tail — silent loss the
    # operator only sees as a reconciliation mismatch. Keyset pages terminate
    # naturally on a short/empty page, so the budget restarts at zero instead.
    keyset_resume = bool(run.cfg.use_keyset and run.cur.keyset_after and offset > 0)
    run.cur.fetch_offset = 0 if keyset_resume else offset
    committed_offset = offset
    population_resume_offset = int(offset or 0)
    # Keyset bookmark of the last *committed* batch. `keyset_after` belongs to
    # the reader, which runs batches ahead of the writer; persisting it would
    # resume past rows that were read but never written.
    committed_keyset = checkpoint.cursor_value or ""

    run.cfg.shape_inputs = (
        frozenset(run.cfg.shape_runner.recipe.input_columns) if run.cfg.shape_runner is not None else frozenset()
    )

    # ``fk_orphan_guard`` was built ahead of the COPY decision (one guard per
    # stream); the row path below applies it to every page.

    if run.cfg.use_keyset and (offset > 0 or chunk_idx > 0):
        # Bookmark seek — the probe was schema-only (or the first page of a
        # scan we closed). Fetch the first uncommitted page from the key.
        first_page = _fetch_next_batch(run, None)
    elif _resume_scan_aligned:
        first_page = probe
    elif offset > 0 or chunk_idx > 0:
        first_page = _fetch_next_batch(run, None)
    else:
        first_page = probe
    batch = _filter_batch(run, first_page)
    run.cfg.batch_quality_enabled = run.cfg.validation_mode in ("strict", "maximum")
    run.cfg.drift_detector = BatchDriftDetector()
    # Per-route drift detectors — routed runs audit each split separately.
    run.cfg.route_drift_detectors: dict[str, Any] = {}
    # Identity key for duplicate audit — NEVER reuse the CDC cursor column.
    # Cursor fields (updated_at, id) are not the uniqueness contract; Mongo→Redis
    # must audit `_id`, not a business `id` that legitimately repeats.
    run.cfg.pk_source_col = ""
    if run.cfg.pk_target_cols and run.cfg.mappings:
        target_to_source = {m.get("target", "").lower(): m.get("source", "") for m in run.cfg.mappings if m.get("target")}
        for pk in run.cfg.pk_target_cols:
            src = target_to_source.get(pk.lower())
            if src:
                run.cfg.pk_source_col = src
                break
    if not run.cfg.pk_source_col:
        try:
            from services.primary_key import resolve_primary_key_source

            run.cfg.pk_source_col = (
                resolve_primary_key_source(
                    run.cfg.mappings,
                    list(run.cfg.columns or (batch.headers if batch else [])),
                    run.cfg.dest_type,
                    validation_mode=run.cfg.validation_mode,
                    purpose="uniqueness",
                )
                or ""
            )
        except Exception:
            run.cfg.pk_source_col = ""
    # Prefer live batch headers when available for the first chunk.
    if batch and getattr(batch, "headers", None) and run.cfg.pk_source_col:
        lower = {h.lower(): h for h in batch.headers}
        run.cfg.pk_source_col = lower.get(run.cfg.pk_source_col.lower(), run.cfg.pk_source_col)
    # Full composite identity for the batch audit (QA ACC-03). Auditing the
    # first component alone hard-blocked every (region, id) upsert at Execute.
    from services.primary_key import identity_source_columns

    run.cfg.pk_audit_cols = identity_source_columns(run.cfg.pk_target_cols, run.cfg.mappings)

    # Phase F6 — default per-transfer parallelism raised from 2 → min(4, CPUs).
    run.cfg.max_workers = int(
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
    pg_overwrite_one_txn = run.cfg.dest_type in ("postgresql", "redshift") and bool(
        (getattr(run.cfg.destination, "extra", None) or {}).get("pg_overwrite_in_transaction")
    )
    # A routed run writes several tables — on PostgreSQL it must run on the
    # single shared connection so one held transaction covers every route
    # (commit all or roll back all, RT-5). With a worker pool each batch gets
    # its own connection and each route commits independently, leaving
    # half-written route sets on failure.
    routed_pg = (
        run.cfg.dest_type in ("postgresql", "redshift")
        and run.cfg.shape_runner is not None
        and getattr(run.cfg.shape_runner, "recipe", None) is not None
        and run.cfg.shape_runner.recipe.router_step is not None
    )
    if (
        run.cfg.dest_type in ("sqlite", "snowflake", "iceberg")
        or pg_overwrite_one_txn
        or routed_pg
    ):
        run.cfg.max_workers = 1
    else:
        try:
            from connectors.write_resilience import is_public_proxy_host
        except ImportError:
            is_public_proxy_host = lambda _h: False  # noqa: E731
        proxy_host = str(run.cfg.dest_cfg.get("host") or "")
        proxy_cs = str(
            run.cfg.dest_cfg.get("connection_string")
            or run.cfg.dest_cfg.get("uri")
            or run.cfg.dest_cfg.get("url")
            or ""
        )
        if is_public_proxy_host(proxy_host) or is_public_proxy_host(proxy_cs):
            run.cfg.max_workers = 1

    # Shared destination connections for the life of this stream job (open once).
    run.tally.sf_conn_state: dict[str, Any] = {"conn": None, "session_ready": False}
    run.tally.pg_conn_state: dict[str, Any] = {"conn": None}
    # Per-route write-pass fingerprint accumulators — the value-level digest
    # each route table should show on read-back (Gate-8 compares it against
    # verify_target's checksum per route, tag column never written).
    run.tally.route_fp_acc: dict[str, Any] = {}
    run.tally.route_fp_cols: dict[str, list[str]] = {}
    batches_completed = 0
    # Only the distinct set matters (the summary asks "did any batch COPY?"),
    # so dedupe instead of appending one entry per batch.
    load_methods_seen: BoundedStrings = BoundedStrings(cap=MAX_LOAD_METHODS)
    # Bounded source sample for Gate-8 after streaming (records=[] at reconcile).
    reconcile_sample: list[dict[str, Any]] = []
    run.cfg._RECONCILE_SAMPLE_CAP = 50

    # The destination DDL rides on the first page that reaches the writer, which
    # is not necessarily the first page read: a source filter or a recipe can
    # empty page one entirely. Only the synchronous prologue below reads this as
    # False, so no worker thread can race another into a CREATE TABLE.
    run.tally.ddl_state = {"created": False}

    def _apply_result(idx: int, result: dict[str, Any]) -> None:
        nonlocal coerced_null_total, removed_on_read_total, filtered_on_read_total, cursor_bounded_total, expanded_total, last_checksum, running_cursor, committed_offset, committed_keyset, batches_completed, reconcile_sample
        if run.tally.overwrite_keys_acc is not None:
            if "overwrite_keys" in result:
                run.tally.overwrite_keys_acc.observe_tuples(result.get("overwrite_keys"))
            elif result.get("batch_rows"):
                run.tally.overwrite_keys_acc.observe_tuples(None)
        run.tally.written += result["batch_written"]
        run.tally.rejected_total += result["rejected"]
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
        if len(reconcile_sample) < run.cfg._RECONCILE_SAMPLE_CAP:
            for row in result.get("reconcile_sample_rows") or []:
                if not isinstance(row, dict):
                    continue
                reconcile_sample.append(row)
                if len(reconcile_sample) >= run.cfg._RECONCILE_SAMPLE_CAP:
                    break
        if result["batch_max"] is not None:
            if running_cursor is None or compare_cursor_values(result["batch_max"], running_cursor) > 0:
                running_cursor = result["batch_max"]
        # Results arrive in commit order, so the newest committed page carries
        # the furthest safe keyset bookmark.
        if result.get("batch_keyset"):
            committed_keyset = result["batch_keyset"]
        if run.cfg.sdk_source and "sdk_state_after" in result:
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

            append_procedure_warnings(run.cfg.ddl_log, incoming)
            # Merge quarantine findings across batches — never replace with last batch only.
            prev = run.tally.dest_summary if isinstance(run.tally.dest_summary, dict) else {}
            merged_usage = _merge_embedding_usage(
                prev.get("embedding_usage"), incoming.get("embedding_usage")
            )
            prev_details = list(prev.get("rejected_details") or [])
            new_details = rebase_batch_rejects(
                incoming.get("rejected_details"),
                batch_start=batch_start,
                row_basis=(
                    "shaped_read" if int(result.get("shape_page_removed") or 0) else ""
                ),
            )
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
            run.tally.dest_summary = {
                **prev,
                **incoming,
                "rejected_details": merged_details,
                "rejected_rows": run.tally.rejected_total,
                "coerced_null_rows": coerced_null_total,
                # Per-batch STRUCT explode deltas sum — the writer stamps only
                # what its own batch expanded (X-30).
                "rows_expanded": expanded_total,
                # A collapsed image is skipped once, as the file path counts it.
                "rows_skipped": page_skips["skipped"] + page_skips["collapsed"],
            }
            if page_skips["collapsed"]:
                run.tally.dest_summary["collapsed_duplicate_rows"] = page_skips["collapsed"]
            if merged_usage is not None:
                run.tally.dest_summary["embedding_usage"] = merged_usage
            # Property 6 — keep first-batch fidelity certificate across chunks.
            fid = incoming.get("schema_fidelity") or prev.get("schema_fidelity")
            if fid:
                run.tally.dest_summary["schema_fidelity"] = fid
            batches_completed += 1
            # Persist rejected rows before continuing — crash must not lose quarantine.
            if new_details and run.cfg.job_id:
                try:
                    from services.quarantine_dlq import persist_rejected_rows

                    persist_rejected_rows(
                        job_id=str(run.cfg.job_id),
                        rejected_details=new_details,
                        source="stream_batch",
                        connector=str(
                            getattr(run.cfg.destination, "format", None)
                            or getattr(run.cfg.destination, "kind", None)
                            or ""
                        ),
                    )
                    run.tally.dest_summary["quarantine_stream_durable"] = True
                    run.tally.dest_summary["quarantine_durable"] = True
                    # Prefix already on control-plane DLQ — finalize must not re-append.
                    run.tally.dest_summary["quarantine_dlq_persisted_count"] = len(merged_details)
                except Exception as qexc:
                    run.tally.dest_summary["quarantine_durable"] = False
                    run.tally.dest_summary["quarantine_dlq_error"] = str(qexc)[:300]
                    raise RuntimeError(
                        "Quarantine DLQ persist failed after batch — refuse to "
                        f"continue (rows cannot disappear): {qexc}"
                    ) from qexc

        # Persist durable checkpoint after the batch is committed.  We use the
        # ordered result stream so checkpoint offsets/cursors can never skip
        # ahead of an in-flight batch.
        checkpoint.chunk_index = idx
        checkpoint.offset = committed_offset
        checkpoint.rows_processed = run.tally.written
        # Persist cumulative quarantine counts so a crash-resume restores them and
        # Gate-8 conservation balances across passes.
        checkpoint.rejected_rows = run.tally.rejected_total
        checkpoint.coerced_null_rows = coerced_null_total
        checkpoint.rows_expanded = expanded_total
        checkpoint.rows_removed_on_read = removed_on_read_total
        checkpoint.rows_source_filtered = filtered_on_read_total
        checkpoint.rows_cursor_bounded = cursor_bounded_total
        checkpoint.cursor_value = (
            running_cursor if run.cfg.sdk_source else running_cursor or committed_keyset or ""
        )
        checkpoint.cursor_column = run.cfg.cursor_source_col if run.cfg.incremental else run.cfg.keyset_col
        checkpoint.es_search_after = run.cur.es_search_after
        checkpoint.redis_scan_state = run.cur.redis_scan_state
        checkpoint.dynamodb_cursor = run.cur.ddb_cursor
        checkpoint.kafka_cursor = run.cur.kafka_cursor
        checkpoint.qdrant_offset = run.cur.qdrant_offset
        checkpoint.checksum = last_checksum
        checkpoint.phase = "writing"
        checkpoint.cdc_stream = run.cfg.table or checkpoint.cdc_stream or ""
        if run.tally.dest_summary.get(PRECOUNT_KEY) is not None:
            checkpoint.target_rows_before = int(run.tally.dest_summary[PRECOUNT_KEY])
        checkpoint.chunk_total = run.cfg.chunks
        checkpoint.status = "running"
        # ACC-05 — advance the *route* watermark with every committed batch.
        # These rows are durable at the destination the moment the batch
        # returns; a cancelled run's next job_id has no access to this
        # job-scoped checkpoint, so without a per-chunk watermark the next
        # run re-reads from the beginning and re-writes committed rows
        # (observed: 40 duplicate documents on PG→Mongo incremental_append).
        # Ordering: watermark first — if the checkpoint save below fails the
        # route watermark still reflects committed rows, never behind them.
        if run.cfg.incremental and running_cursor and cursor_key:
            try:
                set_watermark(
                    cursor_key,
                    running_cursor,
                    metadata={
                        "job_id": run.cfg.job_id,
                        "sync_mode": run.cfg.effective_sync,
                        # A watermark is a value of one column; record which one
                        # so a later run on a different cursor cannot inherit it.
                        "cursor_column": run.cfg.cursor_source_col,
                        # The column a composite watermark is decodable on (QA ACC-02).
                        "tiebreak_column": run.cfg.cursor_pk_source,
                    },
                )
                # D110 — batch undo restores the watermark to this pre-run
                # value when it removes the rows this advance stood for.
                run.tally.dest_summary.setdefault(
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
        if run.cfg.src_type == "kafka" and run.cur.kafka_cursor:
            # D85 — never commit consumer offsets inside the run. Accumulate the
            # per-partition end offsets in the checkpoint cursor so a crash
            # resumes the full uncommitted range; the engine commits the group
            # offsets exactly once, only after the job is accepted (reconcile
            # passed + destination committed). A failure/cancel leaves the
            # broker position untouched, so the next run re-reads every
            # uncommitted message (at-least-once).
            run.cur.kafka_cursor = _kafka_accumulate_offsets(run.tally.dest_summary, run.cur.kafka_cursor)
            checkpoint.kafka_cursor = run.cur.kafka_cursor
            checkpoint_service.require_save(checkpoint)
        if on_checkpoint:
            on_checkpoint(idx, run.cfg.chunks, run.tally.written, checkpoint.to_dict())

    first_idx = chunk_idx + 1

    from .stream_dest_procedure import run_session_after, run_session_before

    run_after = False
    row_path_ok = False
    try:
        run_after = run_session_before(run.cfg.destination, dest_proc_plan, run.cfg.ddl_log)
        # Process the first batch synchronously so DDL (table/index creation) is
        # committed before any parallel workers try to insert into the new table.
        while batch:
            batch = _filter_batch(run, batch)
            _advance_fetch_cursor(run, batch)
            _apply_result(first_idx, _process_db_chunk(run, first_idx, batch))
            run.cur.fetch_offset += _raw_page_rows(batch)
            if batch.rows:
                run.tally.ddl_state["created"] = True
                break
            # Every row on this page was removed on the read, so it never
            # reached the writer and the table does not exist yet. Keep reading
            # here rather than hand the DDL to the worker pool.
            batch = _fetch_next_batch(run, batch)
            first_idx += 1

        # The first page committed above. A single shared connection (proxy,
        # Snowflake, SQLite, Iceberg) finishes the rest on this thread.
        # Parallel destinations still overlap the next read with the write.
        drive_ordered_batches(
            max_workers=run.cfg.max_workers,
            initial=batch,
            fetch_next=partial(_fetch_next_batch, run),
            prepare=partial(_prepare_batch, run),
            process=partial(_process_db_chunk, run),
            apply_result=_apply_result,
            start_idx=first_idx + 1,
        )
        # A buffering recipe step (dedupe keep-last, aggregate, sort, rank)
        # holds rows until the population is fully read — this is that moment.
        # Drained rows run the same write path and DDL gate as every other page.
        if run.cfg.shape_runner is not None:
            tail_headers, tail_rows = run.cfg.shape_runner.drain_matrix()
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
                _apply_result(tail_idx, _process_db_chunk(run, tail_idx, tail_batch))
            # Winners can still hit a divert step downstream of the buffer.
            _persist_shape_diverts(run)
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
                    destination=run.cfg.destination,
                    dest_summary=run.tally.dest_summary,
                    sync_mode=run.cfg.effective_sync,
                )
                if _undo_note:
                    setattr(_row_exc, "failed_batch_undo_note", _undo_note)
            except Exception:  # noqa: BLE001 - undo must not hide the write failure
                pass
        _kafka_failure_offset_note(run.cfg.src_type, run.tally.dest_summary, _row_exc)
        raise
    finally:
        if run_after:
            run_session_after(
                run.cfg.destination, dest_proc_plan, run.cfg.ddl_log, write_ok=row_path_ok
            )
        if run.cfg.src_scan:
            try:
                from connectors.sql_snapshot_scan import close_table_scan

                close_table_scan(run.cfg.src_scan)
            except Exception as exc:
                logging.getLogger(__name__).debug(
                    "Source snapshot scan close skipped: %s", exc, exc_info=exc
                )
        for state in (run.tally.sf_conn_state, run.tally.pg_conn_state):
            conn = state.get("conn")
            if conn is not None:
                if state is run.tally.pg_conn_state:
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
                        run.tally.dest_summary["overwrite_clear_recorded"] = True
                    _rtables = sorted(
                        {
                            str(v.get("table") or "")
                            for v in (run.tally.dest_summary.get("routes") or {}).values()
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
                                run.cfg.dest_cfg.get("schema") or "public"
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
                                run.tally.dest_summary["route_run_txid"] = _txids
                                run.tally.dest_summary["route_run_tables"] = _rtables
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

    if run.tally.written == 0 and run.cfg.incremental:
        run.cfg.ddl_log.append("INCREMENTAL — no new rows since last watermark")
        run.tally.dest_summary["sync_mode"] = run.cfg.effective_sync
        # No batch ran, so no writer summary named the table; the job record
        # read an empty destination and fell back to the database (QA MX3-01).
        run.tally.dest_summary.setdefault("table", run.cfg.dest_table)
        stamp_incremental_no_op(run.tally.dest_summary)
        if resume_key_resolved:
            # Honest delivery label: the read side re-delivers the interrupted
            # batch, the write side resolves it on the identity key.
            run.tally.dest_summary["delivery_semantics"] = "at_least_once_idempotent_apply"
            run.tally.dest_summary["resume_write_mode"] = run.cfg.write_mode
        run.tally.dest_summary["watermark"] = run.cfg.watermark
        try:
            from services.source_snapshot import get_source_snapshot_meta

            _snap = get_source_snapshot_meta()
            if _snap:
                run.tally.dest_summary["source_snapshot"] = _snap
                run.tally.dest_summary.setdefault(
                    "source_snapshot_guarantee",
                    _snap.get("guarantee") or "unknown",
                )
        except Exception:
            pass
        return 0, run.cfg.ddl_log, run.tally.dest_summary, run.cfg.columns

    if run.tally.written == 0:
        details = "; ".join(filter(None, warning_samples[:10]))
        if details:
            raise ValueError(f"No rows were written to the destination: {details}")
        if run.tally.rejected_total:
            # Quarantine is not an empty source. Reporting "empty" here sent
            # operators to look at data that was in fact read, rejected and
            # held — the reason is already recorded, so name it.
            reasons: list[str] = []
            for detail in (run.tally.dest_summary.get("rejected_details") or [])[:200]:
                reason = str((detail or {}).get("reason") or "").strip()
                if reason and reason not in reasons:
                    reasons.append(reason)
                if len(reasons) >= 3:
                    break
            raise ValueError(
                f"All {run.tally.rejected_total} row(s) were quarantined, so nothing was "
                "written to the destination"
                + (f" — {'; '.join(reasons)}" if reasons else "")
            )
        if run.cfg.columns and (getattr(run.cfg.destination, "extra", None) or {}).get(
            "create_empty_source"
        ):
            written_n, checksum, empty_summary = _stream._write_batch(
                run.cfg.dest_type,
                run.cfg.destination,
                run.cfg.dest_cfg,
                run.cfg.dest_table,
                list(run.cfg.columns),
                [],
                run.cfg.mappings,
                run.cfg.column_types,
                create_table=True,
                on_checkpoint=None,
                chunk_idx=0,
                total_chunks=1,
                rows_so_far=0,
                write_mode=run.cfg.write_mode,
                conflict_columns=run.cfg.pk_target_cols or None,
                sync_mode=run.cfg.effective_sync,
                backfill_new_fields=run.cfg.backfill_new_fields,
                error_policy=run.cfg.stream_error_policy,
                job_id=run.cfg.job_id,
                skip_preflight=run.cfg.skip_preflight,
                source_schema_catalog=run.cfg.source_schema_catalog,
            )
            run.tally.dest_summary.update(empty_summary or {})
            run.tally.dest_summary["source_row_count"] = 0
            run.tally.dest_summary["empty_source_table"] = True
            run.tally.dest_summary["checksum"] = checksum
            run.cfg.ddl_log.append(f"CREATE TABLE {run.cfg.dest_table} — source has columns and 0 rows")
            return int(written_n or 0), run.cfg.ddl_log, run.tally.dest_summary, run.cfg.columns
        raise ValueError("Source table is empty")

    if run.cfg.incremental and running_cursor and cursor_key and running_cursor != run.cfg.watermark:
        set_watermark(
            cursor_key,
            running_cursor,
            metadata={
                "job_id": run.cfg.job_id,
                "sync_mode": run.cfg.effective_sync,
                # A watermark is a value of one column; record which one so a
                # later run on a different cursor cannot inherit it.
                "cursor_column": run.cfg.cursor_source_col,
                # The column a composite watermark is decodable on (QA ACC-02).
                "tiebreak_column": run.cfg.cursor_pk_source,
            },
        )
        # D110 — batch undo restores the watermark to this pre-run value
        # when it removes the rows this advance stood for.
        run.tally.dest_summary.setdefault(
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
        (int(run.cfg.total_rows or 0) or int(run.tally.written or 0)) - _shaped_removed, 0
    )
    _partial_write_pass = bool(
        write_pass_fp.total > 0
        and _expected_population > 0
        and write_pass_fp.total < _expected_population
        and (is_overwrite_sync(run.cfg.effective_sync) or resumed_pass)
    )
    _reread = should_reread_source(
        src_type=run.cfg.src_read_type,
        dest_type=run.cfg.dest_type,
        incremental=run.cfg.incremental,
        partial_write_pass=_partial_write_pass,
    )
    if run.cfg.incremental and _reread and not (
        run.cfg.cursor_source_col and running_cursor and run.cfg.pk_target_cols
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
        run.cfg.phase_profile.add(
            PHASE_CHECKSUM, time.perf_counter() - checksum_started, rows=write_pass_fp.total
        )
        run.tally.dest_summary["checksum_mode"] = "inline_write_pass"
        run.tally.dest_summary["source_independently_reread"] = False
        run.tally.dest_summary["identity_hash_aligned"] = False
        _unbounded = run.cfg.incremental and not (
            run.cfg.cursor_source_col and running_cursor and run.cfg.pk_target_cols
        )
        run.tally.dest_summary["checksum_note"] = (
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
    elif _reread and run.cfg.src_read_type in REREAD_SCAN_SOURCES:
        # Independent re-read — snapshot scan (no OFFSET) on warehouse sources.
        from connectors.sql_snapshot_scan import close_table_scan

        checksum_started = time.perf_counter()
        fp_accumulator = FingerprintAccumulator()
        reread_plan = reread_pagination_plan(
            src_type=run.cfg.src_read_type,
            incremental=run.cfg.incremental,
            callable_source=bool(is_callable_source(run.cfg.source) or is_callable_source(run.cfg.src_cfg)),
        )
        reread_scan = reread_plan.get("scan_state")
        use_reread_offset = bool(reread_plan.get("use_offset"))
        cursor_type_for_read: str | None = None
        if run.cfg.incremental and run.cfg.cursor_source_col:
            cursor_type_for_read = normalize_inferred(run.cfg.schema.get(run.cfg.cursor_source_col, "string")).upper()
        checksum_cursor_col = run.cfg.cursor_source_col if run.cfg.incremental else ""
        checksum_cursor_after = window_seed_bookmark if run.cfg.incremental else None
        checksum_cursor_max = running_cursor if run.cfg.incremental else None
        _seed_tiebreak = bool(
            run.cfg.cursor_pk_source and run.cfg.cursor_pk_source != checksum_cursor_col
        )
        _seed_cursor_part: str | None = None
        if run.cfg.incremental and checksum_cursor_after not in (None, ""):
            from services.keyset_pagination import split_cursor_bookmark as _split_bm

            _seed_cursor_part = _split_bm(
                str(checksum_cursor_after), has_tiebreak=_seed_tiebreak
            )[0]
        # M3 — publish the exact window the digest covers so Gate-8 scopes
        # every other population the same way (the ladder's own source
        # re-read and the destination key filter are unbounded without it —
        # a whole-table digest compared to a window digest reads as a
        # checksum mismatch on a correct run).
        if run.cfg.incremental and checksum_cursor_col:
            run.tally.dest_summary["incremental_window_cursor"] = checksum_cursor_col
            run.tally.dest_summary["incremental_window_pk"] = run.cfg.cursor_pk_source or ""
            if checksum_cursor_after not in (None, ""):
                run.tally.dest_summary["incremental_window_seed"] = str(checksum_cursor_after)
                run.tally.dest_summary["incremental_window_seed_inclusive"] = window_seed_inclusive
            if checksum_cursor_max not in (None, ""):
                run.tally.dest_summary["incremental_window_max"] = str(checksum_cursor_max)
        use_checksum_keyset = False
        if run.cfg.incremental and checksum_cursor_col:
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
            ShapeRunner(run.cfg.shape_runner.recipe, typed_values=run.cfg.shape_runner.typed_values) if run.cfg.shape_runner is not None else None
        )
        reread_holdouts: list[dict[str, Any]] = []
        reread_tombstones_excluded = 0
        try:
            while True:
                read_limit = _batch_limit(run,
                    read_offset if use_reread_offset else checksum_rows_read,
                    default=run.cfg.chunk_size,
                )
                if read_limit <= 0:
                    break
                run.cfg._scan_kw = {"scan_state": reread_scan} if reread_scan is not None else {}
                batch, _ = _unwrap_read(
                    _read_batch(
                        run.cfg.src_read_type,
                        run.cfg.src_cfg,
                        run.cfg.table,
                        run.cfg.columns,
                        read_offset if use_reread_offset else 0,
                        read_limit,
                        database=run.cfg.src_db,
                        cursor_column=checksum_cursor_col,
                        cursor_after=checksum_cursor_after,
                        cursor_inclusive=checksum_seed_read,
                        cursor_type=cursor_type_for_read,
                        # A tie-broken incremental watermark is composite
                        # (cursor + pk). Every read of this run has to name
                        # the same primary key or the bookmark decodes as
                        # one part and the page dies on arity — this re-read
                        # is bound by the same contract as the write pass.
                        cursor_primary_key=run.cfg.cursor_pk_source or None,
                        known_total_rows=run.cfg.total_rows,
                        **run.cfg._scan_kw,
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
                    run.cfg.incremental
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
                            run.cfg.cursor_pk_source or None,
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
                if run.cur._boundary_fps_state["fps"] is not None and digest_rows:
                    # The write pass dropped these tie peers as already at the
                    # destination (judged against its pre-run rows, cached):
                    # the digest covers what was written, not re-delivered.
                    _landed = _boundary_rows_already_landed(run, batch.headers, digest_rows)
                    if _landed:
                        digest_rows = [
                            r for i, r in enumerate(digest_rows) if i not in _landed
                        ]
                if keyed_upsert_scope:
                    digest_rows, _excluded_now = live_rows_for_digest(
                        batch.headers,
                        digest_rows,
                        key_columns=run.cfg.pk_target_cols,
                        mappings=run.cfg.mappings,
                    )
                    reread_tombstones_excluded += _excluded_now
                digest_headers = batch.headers
                if run.cfg.source_filter and digest_rows:
                    # The write pass only wrote the rows the filter kept, so the
                    # digest has to cover the same population — hashing the rows
                    # it removed would fail Gate-8 on a correct run.
                    digest_rows = apply_row_filter_to_matrix(
                        batch.headers, digest_rows, run.cfg.source_filter
                    )
                if run.cfg.fk_orphan_guard is not None and digest_rows:
                    # Orphans were quarantined, not written: the digest covers
                    # the rows the write pass let through, in the same order of
                    # owners (filter → FK guard → recipe).
                    digest_rows, _orphans_now = run.cfg.fk_orphan_guard.partition(
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
                    mappings=run.cfg.mappings,
                    target_cols=run.cfg.target_cols,
                    column_types=run.cfg.column_types,
                    error_policy=run.cfg.stream_error_policy,
                    dest_types=run.cfg.fingerprint_dest_types,
                    preserve_case=True,
                    dest_kind=run.cfg.dest_bind_dialect,
                    destination_pk_columns=list(run.cfg.pk_target_cols or []) or None,
                )
                reread_holdouts.extend(reread_rejected or [])
                if mapped:
                    fp_accumulator.add_many(
                        row_fingerprints(
                            mapped,
                            run.cfg.target_cols,
                            dest_db_type=run.cfg.dest_bind_dialect,
                            dest_types=run.cfg.digest_dest_types,
                        )
                    )
                checksum_rows_read += len(batch.rows)
                if run.cfg.limit > 0 and checksum_rows_read >= run.cfg.limit:
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
                        run.cfg.cursor_pk_source or None,
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
                    mappings=run.cfg.mappings,
                    target_cols=run.cfg.target_cols,
                    column_types=run.cfg.column_types,
                    error_policy=run.cfg.stream_error_policy,
                )
                if tail_mapped:
                    fp_accumulator.add_many(
                        row_fingerprints(
                            tail_mapped,
                            run.cfg.target_cols,
                            dest_db_type=run.cfg.dest_bind_dialect,
                            dest_types=run.cfg.digest_dest_types,
                        )
                    )
        run.cfg.phase_profile.add(
            PHASE_CHECKSUM, time.perf_counter() - checksum_started, rows=fp_accumulator.total
        )
        run.tally.dest_summary["reread_pagination"] = reread_plan.get("mode")
        if run.tally.route_fp_acc:
            # Per-route write-pass digests — the value-level expectation
            # Gate-8 compares against each route table's independent
            # read-back (route_verification). Keyed by route name.
            run.tally.dest_summary["route_expected"] = {
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
                    "columns": run.tally.route_fp_cols.get(_rn) or [],
                }
                for _rn, _acc in run.tally.route_fp_acc.items()
                if _acc.total > 0
            }
        if run.tally.route_backup_state.get("backups"):
            # Failed-Gate-8 restore guard armed: the renamed-aside route
            # tables land on the job summary so a failed post-write proof
            # can restore every route to pre-run content.
            run.tally.dest_summary["route_backups"] = dict(run.tally.route_backup_state["backups"])
        if fp_accumulator.total:
            if write_pass_fp.total:
                alignment = align_source_populations(write_pass_fp, fp_accumulator)
                run.tally.dest_summary["write_pass_checksum"] = write_pass_fp.digest()
            else:
                alignment = alignment_when_write_pass_empty(
                    int(fp_accumulator.total),
                    inline_fingerprints_used=run.tally.inline_fingerprints_used,
                )
            run.tally.dest_summary["identity_alignment"] = alignment
            if alignment.get("identity_hash_aligned") is not None:
                run.tally.dest_summary["identity_hash_aligned"] = bool(
                    alignment.get("identity_hash_aligned")
                )
            final_checksum = fp_accumulator.digest()
            run.tally.dest_summary["checksum_mode"] = "source_reread"
            run.tally.dest_summary["source_independently_reread"] = True
            note = (
                "Independent source re-read after the write pass "
                f"({reread_plan.get('mode')} pagination) — dest read-back can earn "
                "full_checksum / migration_proven."
            )
            run.tally.dest_summary["checksum_note"] = record_tombstone_digest_scope(
                run.tally.dest_summary, reread_tombstones_excluded, note
            )
        else:
            # The re-read fingerprinted nothing. Falling back to the writer's own
            # digest here — while still stamping source_reread — presented a
            # writer-basis hash as an independent source scan and produced a
            # mismatch no operator could localize. Decline the digest instead.
            final_checksum = ""
            run.tally.dest_summary["checksum_mode"] = "source_reread_unavailable"
            run.tally.dest_summary["source_independently_reread"] = False
            run.tally.dest_summary["identity_hash_aligned"] = False
            run.tally.dest_summary["checksum_note"] = (
                "Independent source re-read produced no comparable fingerprints "
                f"({checksum_rows_read:,} row(s) read, "
                f"{len(reread_holdouts):,} held out by the remap) — no source "
                "digest for this run rather than a writer-basis hash presented "
                "as a source scan."
            )
    else:
        final_checksum = write_pass_fp.digest() if write_pass_fp.total else last_checksum
        run.tally.dest_summary["checksum_mode"] = "inline_write_pass" if write_pass_fp.total else "writer_last_batch"
        run.tally.dest_summary["source_independently_reread"] = False
        run.tally.dest_summary["identity_hash_aligned"] = False

    run.tally.dest_summary["checksum"] = (
        final_checksum
        if run.tally.dest_summary.get("checksum_mode") == "source_reread_unavailable"
        else (final_checksum or last_checksum)
    )
    if not isinstance(run.tally.dest_summary.get("connector_versions"), dict):
        from services.connector_versions import capture_route_versions

        run.tally.dest_summary["connector_versions"] = capture_route_versions(
            source_engine=run.cfg.src_type,
            dest_type=run.cfg.dest_type,
        )
    # Phase F2 — operator-visible pagination honesty (OFFSET cliff vs keyset).
    run.tally.dest_summary["pagination_mode"] = pagination_mode
    if decision.resume_fallback:
        run.tally.dest_summary["resume_pagination"] = (
            "snapshot_prefix" if pagination_mode == "scan" else "offset_legacy"
        )
    if pagination_warning:
        run.tally.dest_summary["pagination_warning"] = pagination_warning
    elif pagination_mode == "offset":
        run.tally.dest_summary["pagination_warning"] = (
            "OFFSET pagination — quadratic cost and full scans on some warehouses; "
            "declare a primary key to enable keyset seek reads."
        )
    elif pagination_mode == "bulk_copy":
        run.tally.dest_summary["bulk_export"] = "postgresql_copy_stdout"
    elif run.cfg.keyset_order_cols:
        run.tally.dest_summary["pagination_key_columns"] = list(run.cfg.keyset_order_cols)
    # Where the time actually went. Without this the only signal was a single
    # end-of-run rows/second figure, which cannot distinguish a slow source scan
    # from a slow destination write from a doubled cost in strict verification.
    phase_snapshot = run.cfg.phase_profile.snapshot()
    run.tally.dest_summary["phase_profile"] = phase_snapshot
    if phase_snapshot.get("phases"):
        logger.info("Transfer phase breakdown — %s", run.cfg.phase_profile.summary())
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
            run.tally.dest_summary["connection_reuse"] = {
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
    run.tally.dest_summary["replay_safety"] = run.cfg.replay_safety.to_dict()
    run.tally.dest_summary["rejected_rows"] = run.tally.rejected_total
    run.tally.dest_summary["coerced_null_rows"] = coerced_null_total
    all_details = list(run.tally.dest_summary.get("rejected_details") or [])
    # GA: keep full rejected_details for DLQ durability — never truncate bodies
    # before quarantine persist. UI may use rejected_details_sample.
    run.tally.dest_summary["rejected_details"] = all_details
    run.tally.dest_summary["rejected_details_sample"] = all_details[:2000]
    run.tally.dest_summary["rejected_details_total"] = len(all_details)
    # Writer source-minus-kept can exceed the findings the quarantine table
    # stores (reported 7, table 5). The headline is the stored count.
    align_reported_rejects(run.tally.dest_summary)
    if len(all_details) > 2000:
        run.tally.dest_summary["rejected_details_sample_capped"] = True
    run.tally.dest_summary["warnings"] = warning_samples[:10]
    suppressed_warnings = max(len(warning_samples) - 10, 0) + getattr(
        warning_samples, "dropped", 0
    )
    if suppressed_warnings:
        run.tally.dest_summary["warnings_suppressed"] = suppressed_warnings
    run.tally.dest_summary["error_policy"] = "quarantine" if (run.tally.rejected_total or coerced_null_total) else "none"
    run.tally.dest_summary["sync_mode"] = run.cfg.effective_sync
    stamp_overwrite_source_keys(run.tally.dest_summary, run.tally.overwrite_keys_acc)
    if run.tally.keyed_census_acc is not None:
        census = run.tally.keyed_census_acc.to_census()
        if census is not None:
            _resume_held = run.tally.dest_summary.get("resume_rows_before")
            _job_before = run.tally.dest_summary.get(PRECOUNT_KEY)
            if isinstance(_resume_held, int) and isinstance(_job_before, int):
                census = dataclasses.replace(
                    census, prior_pass_inserts=max(_resume_held - _job_before, 0)
                )
            run.tally.dest_summary[CENSUS_KEY] = census.to_dict()
    if run.cfg.pk_target_cols:
        run.tally.dest_summary.setdefault("conflict_columns", list(run.cfg.pk_target_cols))
        run.tally.dest_summary.setdefault("primary_key_columns", list(run.cfg.pk_target_cols))
    run.tally.dest_summary["watermark"] = running_cursor
    run.tally.dest_summary["chunk_size"] = run.cfg.chunk_size
    run.tally.dest_summary["chunk_policy"] = chunk_policy
    run.tally.dest_summary["batches"] = batches_completed
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
        total_rows=run.cfg.total_rows,
        cursor_bounded=int(cursor_bounded_total or 0),
    )
    stamp_source_row_count(
        run.tally.dest_summary, reader_count=counted_population, rows_written=int(run.tally.written or 0)
    )
    # RT-7: routed upsert re-runs grow the destination by 0 but still landed
    # their rows — the ledger must read the measured writer tally, not
    # "unmeasured".
    run.tally.dest_summary["rows_written"] = int(run.tally.written or 0)
    if cursor_bounded_total:
        run.tally.dest_summary["rows_cursor_bounded"] = int(cursor_bounded_total)
        run.cfg.ddl_log.append(
            f"INCREMENTAL client-side cursor bound on {run.cfg.cursor_source_col} — "
            f"{cursor_bounded_total:,} row(s) at or before the watermark were "
            f"not in this run's read scope ({run.cfg.src_type} cannot push the bound down)"
        )
    _diverted_total = int(run.tally.shape_diverted_total[0] or 0)
    if removed_on_read_total or _diverted_total:
        # committed_offset counts the rows the source handed over, so the rows a
        # declared filter or an approved recipe removed are inside that population
        # and have to be named for Gate-8 — otherwise a filtered or shaped run
        # reads as short delivery against the table's own count. A run whose only
        # movement was diverts still has to charge them (DD-3).
        if removed_on_read_total:
            run.tally.dest_summary["rows_removed_on_read"] = int(removed_on_read_total)
        # Named apart: the operator's declared scope removed these before the
        # recipe was offered them, so they are not the recipe's effect.
        run.tally.dest_summary["rows_source_filtered"] = int(filtered_on_read_total)
        diverted = _diverted_total
        shaped_removed = max(
            0, removed_on_read_total - filtered_on_read_total - diverted
        )
        if shaped_removed:
            # Every committed page's recipe removals, this pass and the passes
            # before it — the live runner only shaped the pages of this pass.
            run.tally.dest_summary["rows_shaped_out"] = shaped_removed
        else:
            # The batch writer stamps its own rows_shaped_out including
            # diverted rows; a fully-diverted removal count must be cleared,
            # not left stale (the row is charged to rejected_rows instead).
            run.tally.dest_summary.pop("rows_shaped_out", None)
        if diverted:
            # A diverted row sits in the DLQ with a reason — it is quarantine,
            # not removal (R2). Charge it to rejected_rows so the ledger's
            # in = out + diverted holds literally.
            run.tally.dest_summary["rows_diverted"] = diverted
            run.tally.dest_summary["rejected_rows"] = int(
                run.tally.dest_summary.get("rejected_rows") or 0
            ) + diverted
    if reconcile_sample:
        run.tally.dest_summary["reconcile_sample"] = reconcile_sample[:run.cfg._RECONCILE_SAMPLE_CAP]
    if load_methods_seen:
        if "copy_into" in load_methods_seen:
            run.tally.dest_summary["load_method"] = "copy_into"
        elif "merge_batch" in load_methods_seen:
            run.tally.dest_summary["load_method"] = "merge_batch"
        else:
            run.tally.dest_summary["load_method"] = load_methods_seen[-1]
    run.cfg.ddl_log.insert(1, f"CREATE TABLE IF NOT EXISTS {run.cfg.dest_table}")
    try:
        from services.source_snapshot import get_source_snapshot_meta

        _snap = get_source_snapshot_meta()
        if _snap:
            run.tally.dest_summary["source_snapshot"] = _snap
            run.tally.dest_summary.setdefault(
                "source_snapshot_guarantee",
                _snap.get("guarantee") or "unknown",
            )
        elif "source_snapshot" not in run.tally.dest_summary:
            # Honest weaker guarantee when no engine snapshot was bound.
            run.tally.dest_summary["source_snapshot"] = {
                "engine": run.cfg.src_type,
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
    if run.cfg.pin_ledger is not None:
        try:
            _pin_meta = run.cfg.pin_ledger.summary()
            if _pin_meta:
                run.tally.dest_summary["shape_raw_ledger"] = _pin_meta
        except Exception as exc:
            logger.debug("raw-pin ledger stamp skipped: %s", exc)
    _carry_single_table_foreign_keys(
        run.cfg.source, run.cfg.destination, run.cfg.table, run.cfg.dest_table, run.cfg.mappings, run.tally.dest_summary, run.cfg.ddl_log
    )
    # The exact type basis the write-pass/re-read fingerprinted on — the dest
    # read-back must hash against it or a correct load mismatches (SQLite TEXT
    # degrade: write pass hashes DECIMAL-declared, naive read-back hashes the
    # TEXT carrier literally).
    run.tally.dest_summary.setdefault("gate8_digest_types", dict(run.cfg.digest_dest_types))
    from services.null_cursor_probe import stamp_skipped_null_cursor_rows

    stamp_skipped_null_cursor_rows(
        run.tally.dest_summary, null_cursor_rows_skipped, run.cfg.cursor_source_col
    )
    return run.tally.written, run.cfg.ddl_log, run.tally.dest_summary, run.cfg.columns


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


from .stream_db_context import (  # noqa: E402
    DbRunContext,
    ReadCursors,
    RunConfig,
    RunTally,
)
from .stream_db_read import (  # noqa: E402
    _batch_limit,
    _cursor_read_args,
    _fetch_next_batch,
)
from .stream_db_page import (  # noqa: E402
    _advance_fetch_cursor,
    _boundary_rows_already_landed,
    _filter_batch,
    _persist_shape_diverts,
    _prepare_batch,
)
from .stream_db_chunk import (  # noqa: E402
    _process_db_chunk,
)
