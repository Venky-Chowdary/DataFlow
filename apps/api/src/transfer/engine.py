"""Universal transfer orchestrator — routes any source to any destination."""

from __future__ import annotations

import logging
import os
from services.brand_env import getenv_brand
import sys
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Iterator, Optional

try:
    import resource  # Unix-only; unavailable on Windows
except ImportError:  # pragma: no cover - Windows
    resource = None  # type: ignore[assignment]

# Ensure the API root (parent of the `src` package) is first on sys.path so the
# `services` intelligence package resolves to `apps/api/services`, not an
# accidentally-shadowing `apps/api/src/services` that may be on PYTHONPATH.
_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

_engine_module = sys.modules[__name__]

try:
    from services import lineage_telemetry as lineage
    from services.error_handling import (
        FullRefreshDropFailed,  # noqa: F401
        RetryBudget,
        TransferCancelled,
        with_retry,
    )
    from services.mirror_engine import apply_inferred_soft_deletes
    from services.mongodb_service import get_mongodb_service
    from services.pipeline_explanation import build_pipeline_explanation
    from services.transform_engine import (
        assumed_number_locale,
        decimal_wire_value,
        infer_date_locale,
        infer_number_locale,
        reset_active_date_locale,
        reset_active_number_locale,
        set_active_date_locale,
        set_active_number_locale,
    )
    from services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        probe_destination,
        run_file_preflight,
    )
    from services.row_filter import apply_row_filter
    from services.scd2_engine import apply_scd2
    from services.shape_apply import shaped_schema
    from services.sync_cursor import (
        destination_exists_for_shape,  # noqa: F401
        is_overwrite_sync,  # noqa: F401
        map_source_to_target,
        requires_upsert,
        resolve_effective_sync_mode,
        resolve_selected_sync_contracts,
        resolve_sync_contract,
        should_drop_destination_for_sync,
    )
except (
    ImportError
):  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services import lineage_telemetry as lineage
    from src.services.error_handling import (
        RetryBudget,
        TransferCancelled,
        with_retry,
    )
    from src.services.mirror_engine import apply_inferred_soft_deletes
    from src.services.mongodb_service import get_mongodb_service
    from src.services.pipeline_explanation import build_pipeline_explanation
    from src.services.transform_engine import (
        assumed_number_locale,
        decimal_wire_value,
        infer_date_locale,
        infer_number_locale,
        reset_active_date_locale,
        reset_active_number_locale,
        set_active_date_locale,
        set_active_number_locale,
    )
    from src.services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        probe_destination,
        run_file_preflight,
    )
    from src.services.row_filter import apply_row_filter
    from src.services.scd2_engine import apply_scd2
    from src.services.shape_apply import shaped_schema
    from src.services.sync_cursor import (
        map_source_to_target,
        requires_upsert,
        resolve_effective_sync_mode,
        resolve_selected_sync_contracts,
        resolve_sync_contract,
        should_drop_destination_for_sync,
    )
try:
    from services import pii_guard
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services import pii_guard

from services.file_export_append import land_export_bytes

from .adapters import (
    FileExportMapBlocked,
    WriteBatchBlocked,
    parse_file_content,
    read_source_database,
    resolve_connector_config,
    write_destination_database,
    write_destination_file,
)
from .cdc_transfer import run_cdc_database_transfer
from .incremental_no_op import incremental_no_op_result
from .file_stream import (
    peek_file_source,
    prepare_stream_content,
    should_stream_file,
    stream_file_to_database,
)
from .models import (
    EndpointConfig,
    TransferRequest,
    TransferResult,
    endpoint_to_dict,
    transfer_request_to_dict,
)
from .reconcile_step import run_reconciliation
from .registry import validate_transfer
from .stream import (
    _discard_unconsumed_pin_ledgers,
    _kafka_accumulate_offsets,
    run_non_cdc_multi_stream_sequential,
    stream_database_transfer,
    stream_scd2_mirror_transfer,
    supports_streaming,
)

try:
    from services.data_contract import ContractViolation

    from .contract_engine import enforce_or_create_contract, finalize_contract
except ImportError:  # pragma: no cover - compatibility for tests
    from src.services.data_contract import ContractViolation
    from src.transfer.contract_engine import (
        enforce_or_create_contract,
        finalize_contract,
    )
try:
    from ai.training.training_scheduler import schedule_training_on_transfer
except (
    ImportError
):  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.ai.training.training_scheduler import schedule_training_on_transfer

def _bounded_incremental_read(request: "TransferRequest") -> bool:
    """True when this run's read narrows to a delta — a stored watermark is
    bound to the declared cursor, so the opening message must not quote the
    full-table row count (M4)."""
    try:
        from services.sync_cursor import resolve_incremental_read_scope

        scope = resolve_incremental_read_scope(
            sync_mode=getattr(request, "sync_mode", "") or "",
            stream_contracts=getattr(request, "stream_contracts", None),
            source_type=getattr(request.source, "format", "") or "",
            source_database=getattr(request.source, "database", "") or "",
            source_object=(
                getattr(request.source, "table", "")
                or getattr(request.source, "collection", "")
                or ""
            ),
            dest_type=getattr(request.destination, "format", "") or "",
            dest_database=getattr(request.destination, "database", "") or "",
            dest_object=(
                getattr(request.destination, "table", "")
                or getattr(request.destination, "collection", "")
                or ""
            ),
            source=request.source,
            destination=request.destination,
        )
        return bool(getattr(scope, "bounded", False))
    except Exception:
        return False


from services.batch_progress import (
    ThrottledCheckpoint,
    batch_write_message,
    compute_transfer_progress_pct,
    effective_backfill_new_fields,
    opening_analysis_message,
    opening_batch_message,
    row_count_label,
)
from services.read_options import ReadOptions, ReadOptionsError

try:
    from services import schema_registry  # noqa: F401
except (
    ImportError
):  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    pass
from services.checkpoint_service import (
    Checkpoint,
    CheckpointService,
)
from src.transfer.resume_state import resolve_resume_checkpoint

from . import job_submission, lifecycle_events

logger = logging.getLogger("dataflow.transfer")


from src.transfer.reconcile_heartbeat import (  # noqa: E402
    reconcile_heartbeat_scope as _reconcile_heartbeat_scope,
    reconcile_phase_heartbeat as _reconcile_phase_heartbeat,
    job_phase_heartbeat as _job_phase_heartbeat,
)


def _persist_load_history_profile(
    request: TransferRequest,
    rows: list[dict],
    schema: dict[str, str] | None,
    *,
    job_id: str,
    dest_summary: dict[str, Any],
    row_count: int,
    mappings: list[dict] | None = None,
) -> None:
    """Persist this load to the route ring buffer (same job_id replaces)."""
    try:
        from services import pii_guard
        from services.data_quality_history import profile_batch, save_profile

        route = transfer_request_to_dict(request)
        redacted_rows = (
            pii_guard.redact_records(rows, mappings or []) if mappings else rows
        )
        save_profile(
            route["source"],
            route["destination"],
            profile_batch(redacted_rows, schema),
            job_id=job_id,
            rejected_details=dest_summary.get("rejected_details") or [],
            rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
            row_count=row_count,
        )
    except Exception as exc:
        logger.debug("load-history save_profile skipped: %s", exc, exc_info=exc)


def _coalesce_sort_value(value: Any) -> Any:
    """Return a tuple that sorts None/empty values last regardless of direction.

    Numeric strings use ``decimal_wire_value`` — the write-path parser — so
    Auto ``1,234`` / ``1.234`` stay lexical instead of inventing 1.234.
    """
    if value is None or value == "":
        return (1, "")
    if isinstance(value, (int, float)):
        return (0, value)
    parsed = decimal_wire_value(value)
    if parsed is not None:
        return (0, parsed)
    return (1, str(value).lower())


def _apply_priority_and_limit(
    records: list[dict[str, Any]],
    priority_column: str,
    direction: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Sort source rows by a priority column and optionally cap the row count."""
    if not priority_column or not records:
        if limit > 0:
            return records[:limit]
        return records

    reverse = direction != "asc"
    sorted_records = sorted(
        records,
        key=lambda r: _coalesce_sort_value(r.get(priority_column)),
        reverse=reverse,
    )
    if limit > 0:
        return sorted_records[:limit]
    return sorted_records


def _snapshot_rejected_details(dest_summary: Any) -> list[dict[str, Any]] | None:
    """Unmasked quarantine cells, captured before operator redaction.

    Replay writes whatever the DLQ stored. Redaction runs first today, so the
    durable copy has to be taken from the writer payload, not the preview.
    """
    if not isinstance(dest_summary, dict):
        return None
    details = dest_summary.get("rejected_details")
    if not isinstance(details, list) or not details:
        return None
    import copy

    return copy.deepcopy(details)


def _redacted_endpoint(ep: EndpointConfig) -> dict[str, Any]:
    """Return endpoint metadata without credentials for lineage and logging."""
    d = {
        "kind": ep.kind,
        "format": ep.format,
        "connector_id": ep.connector_id,
        "host": ep.host,
        "port": ep.port,
        "database": ep.database,
        "schema": ep.schema,
        "table": ep.table,
        "collection": ep.collection,
        "warehouse": ep.warehouse,
    }
    return {k: v for k, v in d.items() if v}


def _build_explanation(
    request: TransferRequest,
    columns: list[str],
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]],
    recon: dict[str, Any],
    dest_summary: dict[str, Any],
    pf: dict[str, Any] | None,
    rows_written: int,
) -> str:
    rejected = int(dest_summary.get("rejected_rows", 0) or 0)
    return build_pipeline_explanation(
        request=request,
        columns=columns,
        source_schema=schema,
        mappings=mappings,
        reconciliation=recon,
        destination_summary=dest_summary,
        validation_plan=_validation_plan_for_result(pf) or None,
        rows_written=rows_written,
        rejected_rows=rejected,
    )


def _mapping_proof_for_request(request: TransferRequest) -> dict[str, Any]:
    """Durable per-mapping evidence for Theater/Jobs — rebuilt from the run request."""
    from services.mapping_proof import build_mapping_proof, mappings_from_request

    mappings = mappings_from_request(request)
    if not mappings:
        return {}
    dest_extra = getattr(request.destination, "extra", None) or {}
    raw_exists = dest_extra.get("table_exists") if isinstance(dest_extra, dict) else None
    # Existence SSOT is introspect/parity only. Never invent False from create_new
    # / identity_passthrough stamps — that forged "Projected CREATE" when the
    # destination table already existed (e.g. railway.users).
    table_exists = raw_exists if isinstance(raw_exists, bool) else None
    return build_mapping_proof(
        mappings,
        destination_db_type=(request.destination.format or "").lower(),
        source_kind=request.source.kind or "",
        dest_kind=request.destination.kind or "",
        sync_mode=request.sync_mode or "",
        destination_table_exists=table_exists,
        source_db_type=request.source.format or "",
    )


def _schemaless_live_is_placeholder(fmt: str, live: dict[str, str]) -> bool:
    """True when a schemaless probe answered only untyped string placeholders.

    Redis / document introspect without ``native_types`` reports ``string`` per
    header. That is not DDL. Relational TEXT (SQLite ``id TEXT``) is a real
    declaration and must still overlay.
    """
    if not live:
        return False
    try:
        from services.data_profiler import _is_weak_declared
        from services.type_system import destination_carriers_are_inferred
    except Exception:
        return False
    if not destination_carriers_are_inferred(fmt):
        return False
    return all(_is_weak_declared(str(t or "")) for t in live.values())


def _authoritative_source_schema(
    source: EndpointConfig,
    schema: dict[str, str],
    columns: list[str] | None = None,
) -> dict[str, str]:
    """Merge the source's declared types over the reader's decoded shape.

    The reader names the Python value it decoded, so a MySQL ``CHAR(36)`` is
    reported ``VARCHAR``. Mapping ran off that shape while the coercion
    validator read the declaration, so create-new invented ``TEXT`` and the same
    engine then blocked its own invent as a ``CHAR(36) → TEXT`` fidelity
    collapse. Map, Validate and the writer must read one schema.
    """
    try:
        from services.source_schema_authority import (
            endpoint_source_column_types,
            reconcile_source_types,
        )

        live = endpoint_source_column_types(source)
        if not live:
            return _rekey_to_read_columns(schema, columns)
        # A keyspace / document probe that still answers every header
        # ``string`` is a placeholder catalog, not DDL. Overlaying it on a
        # profiled peek (INTEGER/DECIMAL) made Execute invent TEXT while
        # Validate hashed NUMERIC — redis→SQL cartesian DDL identity.
        fmt = str(getattr(source, "format", "") or "").strip().lower()
        if _schemaless_live_is_placeholder(fmt, live):
            return _rekey_to_read_columns(schema, columns)
        merged, _drift = reconcile_source_types(schema, live)
        return _rekey_to_read_columns(merged, columns)
    except Exception as exc:
        logger.debug("source schema authority merge failed: %s", exc, exc_info=exc)
        return schema


def resolve_read_options(request: TransferRequest) -> ReadOptions:
    """The declared source read window, or a refusal naming what is wrong.

    Raises :class:`ReadOptionsError` for an unusable declaration and for a
    window declared against a source that has no read window to narrow — a
    silently ignored ``sheet`` would read a different population than the one
    the operator approved.
    """
    options = ReadOptions.from_dict(request.read_options)
    if options.is_default:
        return options
    if request.source.kind != "file":
        raise ReadOptionsError(
            "Source read options (sheet/header row/skips) apply to uploaded "
            f"files only; this source is a {request.source.kind}. Filter a "
            "table source with source_filter instead."
        )
    from services.file_parser import FileParser, unsupported_read_options

    file_type = FileParser.detect_file_type(
        request.source_filename or "upload.csv",
        request.source_content or None,
    )
    refusal = unsupported_read_options(options, file_type)
    if refusal:
        raise ReadOptionsError(refusal)
    return options


def _sync_mode_is_cdc(request: TransferRequest) -> bool:
    from services.sync_cursor import normalize_sync_mode

    return normalize_sync_mode(getattr(request, "sync_mode", "") or "", default="") == "cdc"


# Back-compat alias for tests / callers that imported the identity-only helper.
def _execute_preflight_identity_kwargs(request: TransferRequest) -> dict[str, Any]:
    return _engine_module._execute_preflight_parity_kwargs(
        request, destination_connected=True
    )


def _run_number_locale(request: TransferRequest) -> str:
    """The grouping contract this run reads numbers under.

    An operator declaration always wins. With none, a source that hands us
    text it rendered from a typed carrier reads as WIRE: ``NUMERIC(12,3)``
    arrives as ``10.129`` and must not be refused as US/EU-ambiguous, while a
    CSV keeps Auto and still fails closed on ``10.129`` — a human wrote that
    one, and it is 10.129 or 10129 depending on where they wrote it.
    """
    declared = str(getattr(request, "number_locale", "") or "").strip()
    if declared:
        return declared
    from .connector_capabilities import typed_wire_number_locale

    src = getattr(request, "source", None)
    return typed_wire_number_locale(
        str(getattr(src, "kind", "") or ""),
        str(getattr(src, "format", "") or ""),
    )


def _settle_locales(
    request: TransferRequest,
    rows: list[dict[str, Any]],
    columns: list[str],
) -> None:
    """Pin how this run reads dates and numbers, once, before anything parses.

    Order is the operator's declaration, then the typed-wire contract, then
    unambiguous evidence in the read, and only then the US assumption Validate
    reports — the same order preflight applies, so a route Validate cleared
    cannot read differently on Run. Called on the raw read: grouping is a
    property of the source text, and a Shape recipe's own numeric steps parse
    it, so settling afterwards leaves those steps guessing.
    """
    if not rows or not columns:
        return
    if not getattr(request, "date_locale", ""):
        inferred_dates = infer_date_locale(rows, columns)
        if inferred_dates:
            request.date_locale = inferred_dates
            set_active_date_locale(inferred_dates)
    if _run_number_locale(request):
        return
    settled = infer_number_locale(rows, columns)
    assumed = "" if settled else assumed_number_locale(rows, columns)
    settled = settled or assumed
    if settled:
        request.number_locale = settled
        request.number_locale_assumed = bool(assumed)
        set_active_number_locale(settled)


from services.post_load_transform import apply_post_load_transforms  # noqa: E402


# Quarantine durability + rollback plan attachment live in ``job_quarantine``;
# re-exported for the historical ``engine`` import surface.
from .job_quarantine import (  # noqa: E402,F401 — re-export
    _attach_job_rollback_plan,
    _persist_checkpoint_quarantine_delta,
    _persist_job_quarantine,
    checkpoint_quarantine_summary,
    fail_closed_on_silent_loss,
)
from .engine_kafka import (  # noqa: E402
    commit_kafka_offsets_on_accept,
    kafka_nothing_past_committed,
    stamp_kafka_source,
)
from .engine_identity import (  # noqa: E402,F401 — re-export
    _enforce_ddl_identity,
    _enforce_decision_artifact,
    _inline_stamp_ddl_identity,
    _operator_contract_maps,
    _request_decision_artifact_payload,
    execute_preflight_progress_message,
    reuse_approved_validate_population_fit,
)
from .engine_shape import (  # noqa: E402,F401 — re-export
    _open_shape_runner,
    _shape_materialized_read,
    _shape_stream_refusal,
    _shaped_population_rows,
    _stamp_shape_evidence,
)


# Failure/CDC job-field stamping lives in ``job_failure``; re-exported for the
# historical ``engine`` import surface.
from .job_failure import (  # noqa: E402,F401 — re-export
    _CDC_JOB_FIELDS,
    _blank_cells_as_null_from_preflight,
    _cdc_fields_from_summary,
    _fail_job_preflight,
    _fail_runtime_job,
    _job_failure_fields,
    _promote_cdc_job_fields,
    _validation_plan_for_result,
)


#: Terminal-status write attempts after a verified load. The control plane can
#: be slow under load (QA observed 600s timeouts); the data outcome cannot.
_TERMINAL_WRITE_ATTEMPTS = 4


from .mapping_write_stamp import (  # noqa: E402
    enrich_mappings_with_types as _enrich_mappings_with_types,
    prefer_approved_column_types as _prefer_approved_column_types,
    schema_for_endpoint as _schema_for_endpoint,
    stamp_additive_mappings_for_write as _stamp_additive_mappings_for_write,
)


#: Placeholder holder id used while claiming a slot before the job document
#: exists. Claim-then-create is deliberate: creating the job first would leave a
#: window where a duplicate submit sees no claim and starts a second writer.
_PENDING_CLAIM_ID = "__pending__"


class DuplicateTransferSubmission(Exception):
    """An equivalent transfer is already running.

    Carries the in-flight job so the caller can point the operator at it instead
    of reporting a bare conflict. Raised rather than returning the existing id so
    no caller can accidentally treat a deduplicated submit as a fresh run.
    """

    def __init__(self, claim: Any) -> None:
        self.claim = claim
        self.existing_job_id = getattr(claim, "existing_job_id", "") or ""
        self.existing_status = getattr(claim, "existing_status", "") or ""
        self.existing_created_at = getattr(claim, "existing_created_at", "") or ""
        super().__init__(
            "An equivalent transfer is already "
            f"{self.existing_status or 'in progress'}"
            + (f" (job {self.existing_job_id})" if self.existing_job_id else "")
            + ". Wait for it to finish or cancel it before starting another run."
        )


class DestinationBusy(DuplicateTransferSubmission):
    """Another live job is already writing this destination object (D36).

    A subclass so every submit path answers it as the same 409 conflict that
    names the job holding the write.
    """

    def __init__(self, claim: Any, destination: str) -> None:
        super().__init__(claim)
        self.args = (
            f"Destination {destination!r} is already being written by job "
            f"{self.existing_job_id or '(unknown)'} "
            f"({self.existing_status or 'in progress'}). Two jobs writing one "
            "table interleave their rows — one writer at a time. Wait for that "
            "job to finish or cancel it, then run this transfer.",
        )


def _job_destination_name(dest_summary: dict[str, Any], destination: Any) -> str:
    """The object a run wrote, as the job record names it.

    A file export has no table: the writer names the file it produced, and
    without it ``get_job`` / ``list_jobs`` showed the format (``csv``) and
    ``dest_table=null``.
    """
    return str(
        dest_summary.get("collection")
        or dest_summary.get("table")
        or getattr(destination, "collection", "")
        or getattr(destination, "table", "")
        or dest_summary.get("filename")
        or ""
    )


def _failed_run_records_transferred(
    dest_summary: dict[str, Any], rows_written: int
) -> int:
    """Rows that actually remain after a failed run's undo.

    ``records_transferred`` must reflect what the destination still holds:
    when the failed-batch undo cleared the batch (or dropped the run-created
    table) the write was rolled back and reporting ``rows_written`` would
    claim committed rows that no longer exist. A retained batch still holds
    those rows, so the written count stays honest there.
    """
    if (dest_summary or {}).get("partial_batch_undo") == "cleared":
        return 0
    return int(rows_written or 0)


class SdkResumeNotSupportedError(ValueError):
    """SDK source resume is not supported by the engine contract."""


def _sdk_source_descriptor_for_format(source_format: str) -> Any | None:
    from connectors.sdk import get_descriptor
    from .connector_capabilities import resolve_driver_type

    descriptor = get_descriptor(resolve_driver_type(source_format))
    if (
        descriptor is None
        or "source" not in descriptor.roles
        or not descriptor.catalog_ids
    ):
        return None
    return descriptor


def _job_store() -> Any:
    """Module-level job store lookup (tests patch ``get_mongodb_service``)."""
    return get_mongodb_service()

class UniversalTransferEngine:
    """
    Orchestrates universal data movement:
    - File → Database (MongoDB, PostgreSQL, Snowflake)
    - File → File (CSV, JSON, JSONL)
    - Database → Database
    - Database → File
    Auto-creates tables, collections, and schemas as needed.
    """

    def execute(self, request: TransferRequest) -> TransferResult:
        """Synchronous transfer — creates job record on completion."""
        self._resolve_saved_connectors(request)
        job_id = self._create_pending_job(request)
        return self.execute_tracked(request, job_id)

    @staticmethod
    def _peak_memory_bytes() -> int:
        """Return the maximum resident set size (bytes) for this process so far."""
        if resource is None:
            return 0
        try:
            # Linux reports KB; macOS reports bytes — keep prior Linux-oriented scale.
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        except Exception as exc:
            logger.debug("peak memory read failed: %s", exc, exc_info=exc)
            return 0

    def _resolve_saved_connectors(self, request: TransferRequest) -> None:
        """Expand connector_id references into full host/port/credentials before execution."""
        try:
            from .adapters import resolve_endpoint
        except Exception as exc:
            logger.warning("resolve_endpoint import failed: %s", exc, exc_info=exc)
            return
        request.source = resolve_endpoint(
            request.source, workspace_id=request.workspace_id
        )
        from services.pgvector_layout import stamp_destination_layout

        # D209: decide a pgvector destination's layout once, before resolving.
        request.destination = resolve_endpoint(
            stamp_destination_layout(request.destination, request.workspace_id),
            workspace_id=request.workspace_id,
        )

    def execute_tracked(
        self, request: TransferRequest, job_id: str, resume: bool = False
    ) -> TransferResult:
        """Run a tracked transfer under the workspace's connector egress policy."""
        from services.egress_guard import egress_workspace

        from services.endpoint_session import endpoint_session_scope

        # One SFTP login per endpoint for the whole job (D189): listing, stat,
        # spill, preview and probes share it. Database sessions stay the job's.
        with egress_workspace(str(request.workspace_id or "")), endpoint_session_scope(
            engines=("sftp",)
        ):
            return self._execute_tracked_in_workspace(request, job_id, resume)

    def _execute_tracked_in_workspace(
        self, request: TransferRequest, job_id: str, resume: bool = False
    ) -> TransferResult:
        """Timed wrapper around the core transfer engine.

        Also the OpenTelemetry root span for the transfer. Every phase span
        opened inside the stream nests under this one, and the resulting
        ``trace_id`` is folded into ``destination_summary`` so the UI can
        deep-link an operator from a job card into their APM.

        Auto-created destination shells with zero durable writes are rolled
        back on failure (audit §2.3 orphan DDL).
        """
        from services.auto_create_lifecycle import (
            bind_auto_create_job,
            clear_auto_create_job,
            mark_auto_create_committed,
            rollback_uncommitted_auto_creates,
        )

        from services.source_engine_scope import bind_source_engine

        # Resolve first: a connector_id reference carries the engine id that
        # create-new invention needs, and an inline format may be empty.
        self._resolve_saved_connectors(request)
        stamp_kafka_source(request)
        from connectors.sql_identifiers import unsafe_destination_identifier_reason

        ident_err = unsafe_destination_identifier_reason(
            str(request.destination.format or ""),
            str(request.destination.table or request.destination.collection or ""),
        )
        if ident_err:
            try:
                from services.mongodb_service import get_mongodb_service

                get_mongodb_service().update_job_status(
                    job_id,
                    "failed",
                    phase="validate",
                    error=ident_err,
                    message=ident_err,
                )
            except Exception:
                logger.exception("Could not record destination-name refusal on %s", job_id)
            return TransferResult(
                success=False,
                job_id=job_id,
                error=ident_err,
                operation=request.operation,
            )
        if request.source.kind == "file":
            from services.transfer_file_staging import hydrate_file_source

            hydrate_file_source(request)
        from services.append_continuity import check_before_write, record_after_run

        violation = check_before_write(request, resume=resume)
        if violation is not None:
            try:
                _job_store().update_job_status(
                    job_id,
                    "failed",
                    phase="verify",
                    error=violation.message,
                    message=violation.message,
                    error_details=violation.details,
                )
            except Exception:
                logger.exception("Could not record continuity failure on %s", job_id)
            return TransferResult(
                success=False,
                job_id=job_id,
                error=violation.message,
                error_details=dict(violation.details),
                operation=request.operation,
            )
        # Create-new invention needs the source engine to keep Unicode polarity
        # (PostgreSQL VARCHAR → SQL Server NVARCHAR, not code-page VARCHAR).
        started = time.monotonic()
        fields = lifecycle_events.emit_run_started(logger, request, job_id, resume)
        # D102: publish this job's cancel probe so every batch boundary —
        # row writers and the COPY fast paths alike — stops on cancel even
        # where no on_checkpoint plumbing reaches.
        from services.cancel_check import (
            begin_run_attempt,
            cancel_check_for,
            ProgressMeter,
            end_run_attempt,
            set_cancel_probe,
            set_progress_meter,
        )

        try:
            # _job_store() resolves get_mongodb_service at module level — a
            # local `from ... import` in this function would bypass the
            # test seam every other engine lookup goes through.
            _probe_mongo = _job_store()
        except Exception:  # noqa: BLE001 - a store miss must not hide the transfer
            _probe_mongo = None
        set_cancel_probe(cancel_check_for(_probe_mongo, job_id))
        begin_run_attempt(_probe_mongo, job_id)
        set_progress_meter(
            ProgressMeter(_probe_mongo, job_id) if _probe_mongo is not None else None
        )
        try:
            with bind_auto_create_job(job_id), bind_source_engine(
                request.source.format or ""
            ):
                result = self._execute_tracked_inner(request, job_id, resume=resume)
        except BaseException as exc:
            lifecycle_events.emit_run_exception(logger, fields, started, exc)
            raise
        finally:
            set_cancel_probe(None)
            set_progress_meter(None)
            end_run_attempt()
        try:
            record_after_run(request, job_id, result)
        except Exception:
            logger.warning("Continuity baseline not recorded for %s", job_id, exc_info=True)
        try:
            written = int(
                getattr(result, "rows_written", 0)
                or getattr(result, "records_transferred", 0)
                or 0
            )
            if getattr(result, "success", False) and written > 0:
                mark_auto_create_committed(job_id)
                clear_auto_create_job(job_id)
            elif not getattr(result, "success", False) and written == 0:
                # D102/D42: a cancelled run whose chunks already committed to
                # the auto-created table must keep it — dropping it deletes
                # the committed prefix while the job report says the rows
                # landed, and the write ledger would skip them on resume
                # (silent data loss).
                _cancelled_with_rows = False
                try:
                    _job = _job_store().get_job(job_id) or {}
                    _cancelled_with_rows = (
                        _job.get("cancel_requested")
                        or str(_job.get("status") or "") == "cancelled"
                    ) and int(_job.get("records_processed") or 0) > 0
                except Exception:  # noqa: BLE001 - a store miss keeps the safe rollback
                    _cancelled_with_rows = False
                if _cancelled_with_rows:
                    mark_auto_create_committed(job_id)
                    clear_auto_create_job(job_id)
                    dropped = []
                else:
                    dropped = rollback_uncommitted_auto_creates(job_id)
                if dropped:
                    details = dict(getattr(result, "error_details", None) or {})
                    details["auto_create_rolled_back"] = dropped
                    try:
                        result.error_details = details
                    except Exception:
                        pass
            else:
                clear_auto_create_job(job_id)
        except Exception:
            logger.debug("auto_create finalize failed", exc_info=True)
        lifecycle_events.emit_run_finished(logger, fields, started, result)
        return result

    def _execute_tracked_inner(
        self, request: TransferRequest, job_id: str, resume: bool = False
    ) -> TransferResult:
        """Core transfer engine body (see :meth:`execute_tracked`)."""
        self._resolve_saved_connectors(request)
        # Programmatic callers (tests/CLI/fleet) may pass a job_id that is not yet
        # in the store. Mint a pending shell so checkpoint persistence cannot
        # fail-closed solely because the document is missing.
        try:
            mongo_boot = get_mongodb_service()
            if not mongo_boot.get_job(job_id):
                mongo_boot.create_transfer_job(
                    {
                        "_id": job_id,
                        "status": "pending",
                        "message": "Execute started (job shell)",
                    }
                )
            elif resume:
                # Resume is the one sanctioned exit from a terminal status, and
                # it belongs here rather than only in the HTTP router: every
                # resume caller (fleet worker, scheduler retry, CLI) otherwise
                # aborts at its first checkpoint save, which the job store
                # rejects as a terminal-status regression — after the writer has
                # already committed rows.
                mongo_boot.clear_job_cancel(job_id)
                mongo_boot.update_job_status(
                    job_id,
                    "running",
                    phase="resuming",
                    message="Resume from last committed checkpoint",
                    allow_terminal_exit=True,
                )
        except Exception:
            logger.debug("job shell bootstrap skipped for %s", job_id, exc_info=True)
        # Staging can set the ack on the request, the job root, or both.
        # Execute uses whichever survived (R10-001).
        try:
            from services.confirmed_transfer import apply_stored_schema_drift_ack

            stored_job = get_mongodb_service().get_job(job_id) or {}
            if apply_stored_schema_drift_ack(request, stored_job):
                repaired = transfer_request_to_dict(request)
                get_mongodb_service().update_job_fields(
                    job_id,
                    {
                        "schema_drift_acknowledged": True,
                        "acknowledgment_actor": request.acknowledgment_actor,
                        "acknowledgment_reason": request.acknowledgment_reason,
                        "transfer_request": repaired,
                    },
                )
        except Exception:
            logger.debug("schema-drift ack restore skipped for %s", job_id, exc_info=True)
        # Hard-block Execute when Map still has unresolved requires_review rows —
        # skip_preflight must never green-path ambiguous remaps into a write.
        # auto selects dest-owned exactly-once on an eligible CDC route.
        # An explicit at_least_once pin stays. Ineligible exactly_once fails closed.
        from services.cdc_exactly_once import (
            ExactlyOnceRouteError,
            dest_allow_append_only,
            operator_pinned_delivery,
            route_declares_log_position,
            route_has_cdc_pk,
            select_route_delivery,
        )
        from services.execution_engine_contract import DeliveryGuaranteeError
        from services.mapping_pipeline import assert_mappings_executable
        from services.procedure_source import is_callable_source
        from .models import endpoint_to_dict

        try:
            if resume:
                descriptor = _sdk_source_descriptor_for_format(
                    request.source.format or ""
                )
                if descriptor is not None:
                    raise SdkResumeNotSupportedError(
                        f"Resume is not supported for {descriptor.display_name} yet; "
                        "rerun the job — rows are upserted by primary key."
                    )
            requested_delivery = getattr(request, "delivery_guarantee", None) or "auto"
            request.delivery_pinned = operator_pinned_delivery(requested_delivery)
            request.delivery_guarantee = select_route_delivery(
                requested_delivery,
                sync_mode=getattr(request, "sync_mode", "") or "",
                dest_type=str(getattr(request.destination, "format", "") or ""),
                source_type=str(getattr(request.source, "format", "") or ""),
                has_primary_key=route_has_cdc_pk(
                    getattr(request, "stream_contracts", None),
                ),
                has_lsn_column=route_declares_log_position(
                    getattr(request, "stream_contracts", None),
                ),
                allow_append_only=dest_allow_append_only(request.destination),
                callable_source=is_callable_source(request.source),
                dest_cfg=endpoint_to_dict(request.destination),
            )
            from services.procedure_source import assert_callable_sync_allowed

            assert_callable_sync_allowed(
                getattr(request, "sync_mode", "") or "",
                getattr(request, "source", None),
            )
            from src.transfer.engine_schema_propagation import (
                _mapping_review_waits_for_evolution,
            )

            # Approved evolution: reviewed on the evolved plan (D200).
            assert_mappings_executable(
                request.mappings,
                review=not _mapping_review_waits_for_evolution(request),
            )
        except (ValueError, DeliveryGuaranteeError, ExactlyOnceRouteError) as mapping_exc:
            mongo = get_mongodb_service()
            mongo.update_job_status(
                job_id,
                "failed",
                phase="failed",
                message=str(mapping_exc),
                error=str(mapping_exc),
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=str(mapping_exc),
                records_transferred=0,
            )
        # Persist accepted Risk Contracts on the job for Proof Pack / audit
        # (mappings alone can be dropped from transfer_request redaction paths).
        from services.signed_proof_pack import collect_accepted_risks_from_job

        accepted = collect_accepted_risks_from_job(
            {"mappings": list(request.mappings or [])}
        )
        if accepted:
            mongo = get_mongodb_service()
            stamped = mongo.update_job_fields(
                job_id,
                {
                    "accepted_risks": accepted,
                    "delivery_guarantee": getattr(
                        request, "delivery_guarantee", "at_least_once"
                    )
                    or "at_least_once",
                },
            )
            if not stamped:
                mongo.update_job_status(
                    job_id,
                    "failed",
                    phase="failed",
                    message=(
                        "Failed to persist accepted Migration Risk Contracts "
                        "before Execute — refuse write"
                    ),
                    error="accepted_risks_persist_failed",
                )
                return TransferResult(
                    success=False,
                    job_id=job_id,
                    error="accepted_risks_persist_failed",
                    records_transferred=0,
                )
            try:
                from services.audit_log import append_audit_event

                append_audit_event(
                    action="migration_risk_contract.execute_bound",
                    resource=f"job:{job_id}",
                    actor=str(getattr(request, "triggered_by", None) or "system"),
                    details={
                        "job_id": job_id,
                        "risk_ids": [r.get("risk_id") for r in accepted if isinstance(r, dict)],
                        "count": len(accepted),
                    },
                )
            except Exception:
                # Audit best-effort — contracts are already on the job document.
                pass
        # Business-rule evidence: one executed event per job, linked back to the
        # compile event by report fingerprint. Best-effort — never fails a run.
        try:
            _br_mappings = [
                m
                for m in (request.mappings or [])
                if isinstance(m, dict)
                and (
                    m.get("rule_contracts")
                    or (
                        isinstance(m.get("business_rule"), dict)
                        and str((m.get("business_rule") or {}).get("report_fingerprint") or "")
                    )
                )
            ]
            if _br_mappings:
                from datetime import datetime, timezone

                from services.audit_log import append_audit_event
                from services.rule_compiler.audit import find_rule_report_event

                _br_ws = str(getattr(request, "workspace_id", "") or "")
                _fps = sorted(
                    {
                        str((m.get("business_rule") or {}).get("report_fingerprint") or "")
                        for m in _br_mappings
                    }
                    - {""}
                )
                _compiled = {
                    fp: str((find_rule_report_event(fp, _br_ws) or {}).get("event_hash") or "")
                    or None
                    for fp in _fps
                }
                _contract_ids = sorted(
                    {
                        str(c.get("rule_id") or "")
                        for m in _br_mappings
                        for c in (m.get("rule_contracts") or [])
                        if isinstance(c, dict) and c.get("rule_id")
                    }
                )
                _shape_ids = sorted(
                    {
                        str((m.get("business_rule") or {}).get("rule_id") or "")
                        for m in _br_mappings
                    }
                    - {""}
                )
                append_audit_event(
                    action="business_rules.executed",
                    resource=f"job:{job_id}",
                    actor=str(getattr(request, "triggered_by", "") or "unknown"),
                    level="info",
                    correlation_id=job_id,
                    workspace_id=_br_ws or None,
                    details={
                        "report_fingerprints": _fps,
                        "compiled_event_hash": (
                            _compiled[_fps[0]] if len(_fps) == 1 else (_compiled or None)
                        ),
                        "rule_ids_contracts": _contract_ids,
                        "rule_ids_transforms": _shape_ids,
                        # Job submit time = this execute call; TransferRequest
                        # carries no earlier timestamp field.
                        "approved_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
        except Exception:
            logger.warning(
                "business_rules audit event unavailable for %s", job_id, exc_info=True
            )
            try:
                get_mongodb_service().update_job_fields(
                    job_id, {"business_rules_audit": "unavailable"}
                )
            except Exception:
                pass
        # Stamp declared mask/hash/redact so the certificate still lists them
        # after transfer_request mappings are redacted away. Best-effort —
        # harvest at export time if this write does not land.
        try:
            from services.governance_ops import harvest_governance_operations

            gov = harvest_governance_operations(
                {"mappings": list(request.mappings or [])}
            )
            get_mongodb_service().update_job_fields(
                job_id, {"governance_operations": gov}
            )
        except Exception:
            logger.warning(
                "governance_operations stamp skipped for %s", job_id, exc_info=True
            )
        locale_token = set_active_date_locale(request.date_locale)
        number_token = set_active_number_locale(
            _run_number_locale(request)
        )
        from services.transform_engine import (
            reset_strict_number_reading,
            set_strict_number_reading,
        )

        strict_number_token = set_strict_number_reading(
            str(getattr(request, "validation_mode", "") or "strict").strip().lower()
            == "strict"
            and not str(getattr(request, "number_locale", "") or "").strip()
        )
        try:
            from services.tracing import (
                current_trace_id,
                get_correlation_id,
                set_span_attribute,
                set_span_error,
                start_span,
            )
        except Exception:
            start_span = None  # type: ignore[assignment]

        src = getattr(request, "source", None)
        dst = getattr(request, "destination", None)
        span_attrs = {
            "dataflow.job_id": job_id,
            "dataflow.resume": bool(resume),
            "dataflow.source.type": str(getattr(src, "type", "") or getattr(src, "kind", "") or ""),
            "dataflow.destination.type": str(
                getattr(dst, "type", "") or getattr(dst, "kind", "") or ""
            ),
            "dataflow.sync_mode": str(getattr(request, "sync_mode", "") or ""),
            "dataflow.workspace_id": str(getattr(request, "workspace_id", "") or ""),
        }
        span_cm = (
            start_span("transfer.execute", attributes=span_attrs, kind="internal")
            if start_span is not None
            else nullcontext()
        )

        # Bind the job id for the whole transfer so every log line emitted by the
        # engine, the connectors, and reconciliation identifies its job. Those
        # modules have no notion of a job, which is why "what happened to job X?"
        # was previously unanswerable from logs alone.
        try:
            from services.logging_config import job_log_context

            log_cm: Any = job_log_context(job_id)
        except Exception:
            log_cm = nullcontext()

        try:
            with log_cm, span_cm as span:
                start = time.monotonic()
                start_mem = self._peak_memory_bytes()
                try:
                    result = self._execute_tracked_core(request, job_id, resume=resume)
                except BaseException as exc:
                    if start_span is not None:
                        set_span_error(span, exc)
                    raise
                elapsed = time.monotonic() - start
                result.elapsed_seconds = round(elapsed, 3)
                result.records_per_second = (
                    round(result.records_transferred / elapsed, 3) if elapsed > 0 else 0.0
                )
                result.peak_memory_bytes = max(self._peak_memory_bytes() - start_mem, 0)
                # Surface SLA metrics in the destination summary for the UI / API consumers.
                result.destination_summary["elapsed_seconds"] = result.elapsed_seconds
                result.destination_summary["records_per_second"] = result.records_per_second
                result.destination_summary["peak_memory_bytes"] = result.peak_memory_bytes
                try:
                    from services.row_conservation import (
                        PopulationConservationError,
                        assert_population_conservation_closed,
                        ledger_from_transfer_result,
                    )

                    result.row_accounting = ledger_from_transfer_result(
                        result,
                        sync_mode=str(getattr(request, "sync_mode", "") or ""),
                    )
                    if result.success:
                        ledger = assert_population_conservation_closed(
                            {
                                "records_processed": result.records_transferred,
                                "sync_mode": str(
                                    getattr(request, "sync_mode", "") or ""
                                ),
                                "reconciliation": dict(
                                    result.reconciliation or {}
                                ),
                                "destination_summary": dict(
                                    result.destination_summary or {}
                                ),
                                "rejected_rows": (
                                    result.destination_summary or {}
                                ).get("rejected_rows"),
                                "coerced_null_rows": (
                                    result.destination_summary or {}
                                ).get("coerced_null_rows"),
                            },
                            validation_mode=str(
                                getattr(request, "validation_mode", "") or ""
                            ),
                        )
                        result.row_accounting = ledger.to_dict()
                except PopulationConservationError as exc:
                    # Must never fall through to the generic stamp handler —
                    # that would wipe the ledger and leave a green job.
                    result.success = False
                    result.error = str(exc)
                    dest_summary = dict(result.destination_summary or {})
                    dest_summary["silent_loss"] = True
                    dest_summary["conservation_error"] = str(exc)[:500]
                    result.destination_summary = dest_summary
                    try:
                        get_mongodb_service().update_job_status(
                            job_id,
                            "failed",
                            error=str(exc),
                            phase="failed",
                            message=str(exc),
                            reconciliation=result.reconciliation,
                            destination_summary=dest_summary,
                        )
                    except Exception:
                        logger.warning(
                            "silent-loss fail-closed persist failed for %s",
                            job_id,
                            exc_info=True,
                        )
                except Exception:
                    logger.warning(
                        "Conservation ledger stamp failed for job %s",
                        job_id,
                        exc_info=True,
                    )
                    if not result.row_accounting:
                        result.row_accounting = {}
                if start_span is not None:
                    set_span_attribute(span, "dataflow.records_transferred", result.records_transferred)
                    set_span_attribute(span, "dataflow.elapsed_seconds", result.elapsed_seconds)
                    set_span_attribute(
                        span,
                        "dataflow.rejected_rows",
                        int(result.destination_summary.get("rejected_rows") or 0),
                    )
                    trace_id = current_trace_id()
                    correlation = get_correlation_id()
                    if trace_id:
                        result.destination_summary["trace_id"] = trace_id
                    if correlation:
                        result.destination_summary["correlation_id"] = correlation
                self._notify_job_status(request, result)
                return result
        finally:
            reset_strict_number_reading(strict_number_token)
            reset_active_number_locale(number_token)
            reset_active_date_locale(locale_token)
            # The run is over however it ended, so the slot must be freed here.
            # Releasing only on success would leave a failed job's claim in place
            # and block the operator's retry until the TTL expired. A superseded
            # attempt must not free the claim the newer attempt now holds.
            from services.cancel_check import attempt_superseded

            if not attempt_superseded(_job_store(), job_id):
                self._release_idempotency(job_id)

    def _notify_job_status(
        self, request: TransferRequest, result: TransferResult
    ) -> None:
        """Fire workspace notifications for failed or partially-quarantined jobs."""
        rejected = result.destination_summary.get("rejected_rows", 0) or 0
        coerced = result.destination_summary.get("coerced_null_rows", 0) or 0
        if result.success and not rejected and not coerced:
            return
        try:
            from services.notification_service import (
                build_job_payload,
                log_job_notifications,
                notify_workspace,
            )
            from services.platform_config import public_url, web_url

            status = "failed"
            if result.success and (rejected or coerced):
                # Successful terminal run that altered/dropped data — consistent
                # with the persisted job status.
                status = "completed_with_quarantine"
            elif result.success:
                status = "completed"
            payload = build_job_payload(
                job_id=result.job_id,
                status=status,
                source=request.source.kind or "unknown",
                destination=request.destination.kind or "unknown",
                records_transferred=result.records_transferred or 0,
                rejected_rows=int(rejected),
                error=result.error or "",
                retry_url=f"/api/v1/connectors/jobs/{result.job_id}/resume",
                workspace_id=request.workspace_id or "",
                base_url=public_url(),
                web_url=web_url(),
            )
            results = notify_workspace(request.workspace_id or "", payload)
            log_job_notifications(result.job_id, results)
        except Exception as exc:
            # Notifications must never fail a transfer.
            logger.debug("job status notification suppressed: %s", exc, exc_info=exc)

    def _execute_tracked_core(
        self, request: TransferRequest, job_id: str, resume: bool = False
    ) -> TransferResult:
        mongo = get_mongodb_service()
        checkpoint_service = CheckpointService(mongo)
        _forget_cached_table_shapes(request.source, request.destination)
        if resume:
            contract = resolve_sync_contract(request.stream_contracts)
            checkpoint = resolve_resume_checkpoint(
                job_id=job_id,
                mongo=mongo,
                checkpoint_service=checkpoint_service,
                has_progress=_checkpoint_has_progress,
                sync_mode=resolve_effective_sync_mode(
                    request.sync_mode,
                    contract.sync_mode if contract else None,
                ),
            )
        else:
            checkpoint = Checkpoint(job_id=job_id)

        # Module 7: AUDIT / DISCOVERY never write — fail closed before mutation.
        try:
            from services.validation_mode_contract import assert_mode_allows_write

            assert_mode_allows_write(getattr(request, "validation_mode", None))
        except Exception as mode_exc:
            from services.validation_mode_contract import ValidationModeWriteRefused

            if isinstance(mode_exc, ValidationModeWriteRefused):
                msg = str(mode_exc)
                mongo.update_job_status(
                    job_id, "failed", error=msg, phase="failed", progress_pct=0
                )
                lineage.emit_run_failed(
                    run_id=job_id,
                    job_id=job_id,
                    error=msg,
                    error_details={"reason": "validation_mode_write_refused"},
                )
                return TransferResult(
                    success=False,
                    error=msg,
                    operation=request.operation,
                    job_id=job_id,
                )
            raise

        lineage.emit_run_started(
            run_id=job_id,
            job_id=job_id,
            source=_redacted_endpoint(request.source),
            destination=_redacted_endpoint(request.destination),
            validation_mode=request.validation_mode,
            write_semantics=request.sync_mode,
        )
        try:
            resolve_read_options(request)
        except ReadOptionsError as exc:
            msg = f"Source read options refused: {exc}"
            mongo.update_job_status(
                job_id, "failed", error=msg, phase="failed", progress_pct=0
            )
            lineage.emit_run_failed(
                run_id=job_id,
                job_id=job_id,
                error=msg,
                error_details={"reason": "Invalid source read options"},
            )
            return TransferResult(
                success=False, error=msg, operation=request.operation, job_id=job_id
            )

        src_fmt = request.source.format or "csv"
        dst_fmt = request.destination.format or "mongodb"
        ok, msg = validate_transfer(
            request.source.kind,
            src_fmt,
            request.destination.kind,
            dst_fmt,
        )
        if not ok:
            from .connector_capabilities import transfer_live_driver_types

            live = ", ".join(transfer_live_driver_types())
            msg = f"{msg}. Transfer-live drivers: {live}."
            mongo.update_job_status(
                job_id, "failed", error=msg, phase="failed", progress_pct=0
            )
            lineage.emit_run_failed(
                run_id=job_id,
                job_id=job_id,
                error=msg,
                error_details={"reason": "Unsupported route", "supported": live},
            )
            return TransferResult(
                success=False, error=msg, operation=request.operation, job_id=job_id
            )

        if (
            supports_streaming(request.source, request.destination)
            and not request.priority_column
            and not getattr(request, "write_via_staging", False)
        ):
            try:
                return self._execute_streaming(
                    request,
                    job_id,
                    mongo,
                    src_fmt,
                    resume=resume,
                    checkpoint=checkpoint,
                    checkpoint_service=checkpoint_service,
                )
            except NotImplementedError:
                # Streaming transfer is not implemented for this sync-mode/destination
                # combination (e.g. SCD2/mirror to a non-SQL destination). Fall through
                # to the buffered path which supports all destinations.
                pass

        non_streaming_mode = request.sync_mode.lower() in (
            "full_refresh_mirror",
            "mirror",
            "scd2",
        )
        if (
            request.source.kind == "file"
            and request.destination.kind == "database"
            and (request.source_content or request.source_path)
            and not non_streaming_mode
            and not request.priority_column
            and request.limit == 0
            and not getattr(request, "write_via_staging", False)
            and should_stream_file(
                request.source_path or request.source_content,
                request.source_filename or "upload.csv",
                request.destination,
                resolve_read_options(request),
            )
        ):
            return self._execute_file_streaming(
                request,
                job_id,
                mongo,
                src_fmt,
                resume=resume,
                checkpoint=checkpoint,
                checkpoint_service=checkpoint_service,
            )

        pf = None
        contract_id = ""
        # Set once Gate-8 passed with no silent loss: from there the run's data
        # outcome is final and no later exception may fail or roll it back.
        verified = False
        try:
            mongo.update_job_status(
                job_id,
                "running",
                phase="reading",
                progress_pct=5,
                message="Reading source data…",
            )
            read_budget = RetryBudget(
                max_attempts=3,
                base_delay_seconds=0.5,
                max_delay_seconds=5.0,
            )
            with _job_phase_heartbeat(
                mongo,
                job_id,
                phase="reading",
                message="Reading source data…",
                progress_pct=5,
            ):
                source_stamp: dict[str, Any] = {}
                records, columns, schema = with_retry(
                    lambda: self._read_source(request, source_stamp),
                    budget=read_budget,
                    on_transient=lifecycle_events.source_read_retry_callback(
                        request, read_budget
                    ),
                )
            schema = _authoritative_source_schema(request.source, schema, columns)
            if request.source_filter:
                records = apply_row_filter(records, request.source_filter)
            # The batch path reads the source whole, so an incremental mode has
            # to be bounded here or it re-delivers rows already at rest.
            try:
                from services.batch_incremental import bind_transfer_request

                incremental_bound = bind_transfer_request(request, src_fmt)
                records = incremental_bound.bound(records)
            except ValueError as cursor_refusal:
                mongo.update_job_status(
                    job_id, "failed", error=str(cursor_refusal), phase="failed"
                )
                return TransferResult(
                    success=False,
                    error=str(cursor_refusal),
                    operation=request.operation,
                    job_id=job_id,
                )
            if incremental_bound.active and not records:
                # The cursor bound left nothing: every source row is already at
                # rest. That is a correct no-op run, not an empty source.
                return incremental_no_op_result(
                    request, job_id, incremental_bound.scope.watermark
                )
            _settle_locales(request, records, columns)
            shape_runner = _open_shape_runner(request, columns, schema)
            if shape_runner is not None:
                records, columns, schema = _shape_materialized_read(
                    shape_runner, records, columns, schema
                )
            records = _apply_priority_and_limit(
                records,
                request.priority_column,
                request.priority_direction,
                request.limit,
            )
            if not records and request.source.kind != "database":
                if columns and str(getattr(request.source, "kind", "") or "") == "file":
                    from .incremental_no_op import empty_file_success_result

                    return empty_file_success_result(request, job_id)
                mongo.update_job_status(
                    job_id, "failed", error="No records to transfer", phase="failed"
                )
                return TransferResult(
                    success=False,
                    error="No records to transfer",
                    operation=request.operation,
                    job_id=job_id,
                )

            total_rows = len(records)
            mongo.update_job_status(
                job_id, "running", total_rows=total_rows, records_processed=0
            )

            dest_schema_types, dest_table_exists_flag = _engine_module._connect_destination(
                request.destination,
                sync_mode=request.sync_mode,
                mongo=mongo,
                job_id=job_id,
            )
            schema = _prefer_approved_column_types(request, schema)
            mappings = _enrich_mappings_with_types(
                _engine_module._auto_map(
                    request, columns, schema, sample_rows=records[:100], job_id=job_id
                ),
                column_types=schema,
                dest_types=dest_schema_types,
            )
            # Schema auto-propagate (Validate≡Execute): extend mappings for new
            # additive columns under propagate_* before conflict/identity resolve.
            mappings = _apply_schema_auto_propagate(
                request=request,
                columns=columns,
                schema=schema,
                mappings=mappings,
                dest_schema_types=dest_schema_types,
            )
            _record_schema_propagation(
                mongo, job_id, columns, mappings, dest_schema_types
            )
            mappings = _stamp_additive_mappings_for_write(
                request,
                mappings,
                column_types=schema,
                dest_types=dest_schema_types,
                sample_rows=records[:100] if isinstance(records, list) else None,
                dest_table_exists=dest_table_exists_flag,
            )
            dest_schema_types = _apply_sampled_dest_profile(
                request.destination, mappings, dest_schema_types
            )
            # Resolve upsert mode for non-streaming database writes.
            contract = resolve_sync_contract(request.stream_contracts)
            effective_sync = resolve_effective_sync_mode(
                request.sync_mode,
                contract.sync_mode if contract else None,
            )
            effective_sync_lower = (effective_sync or "").lower()
            write_mode = "insert"
            conflict_columns: list[str] = []
            if contract and contract.primary_key:
                conflict_columns = [
                    map_source_to_target(col, mappings)
                    for col in contract.primary_key_columns()
                ]
            if not conflict_columns and effective_sync_lower in (
                "full_refresh_mirror",
                "mirror",
                "scd2",
            ):
                inferred_pk = _infer_primary_key(columns, mappings)
                if inferred_pk:
                    conflict_columns = [inferred_pk]
            # Key-addressed sinks (Redis/Dynamo/ES/vectors) always need identity —
            # append still collides on the key. Infer from natural keys; never invent
            # weak attrs (countries:capital bug).
            from services.primary_key import KEY_ADDRESSED_DESTS
            from .connector_capabilities import resolve_driver_type

            dest_driver = resolve_driver_type(request.destination.format or "")
            if not conflict_columns and dest_driver in KEY_ADDRESSED_DESTS:
                inferred_pk = _infer_primary_key(columns, mappings)
                if inferred_pk:
                    conflict_columns = [inferred_pk]
            if not conflict_columns and dest_driver == "kafka":
                # Kafka's key is partitioning/compaction identity, not a merge
                # constraint — any declared identity is the right produce key
                # (D88: keyed messages went out key=None).
                from services.sync_cursor import declared_identity_columns
                conflict_columns = [
                    map_source_to_target(c, mappings)
                    for c in declared_identity_columns(request)
                ]
            if requires_upsert(effective_sync):
                if not conflict_columns:
                    # The destination's own declared key is catalog evidence, not
                    # a guess — use it when every key column is mapped. Refusing
                    # here while the table itself declares the key made upsert
                    # unusable on any route where the operator had not also typed
                    # the key into the stream contract.
                    conflict_columns = _declared_destination_key(
                        request.destination, mappings
                    )
                if not conflict_columns:
                    raise ValueError(
                        f"Sync mode `{effective_sync}` requires primary_key for upsert; "
                        "refuse silent insert fallback (set primary_key on the stream contract)"
                    )
                write_mode = "upsert"

            activation_notes: list[str] = []
            if effective_sync_lower == "reverse_etl":
                from services.reverse_etl import plan_activation

                if not conflict_columns:
                    raise ValueError(
                        "reverse_etl requires primary_key for activation — "
                        "refuse inventing default 'id'"
                    )
                plan = plan_activation(
                    destination_kind=request.destination.format or "",
                    object_name=request.destination.table
                    or request.destination.collection
                    or "",
                    primary_key=conflict_columns,
                    field_map={
                        str(m.get("source") or ""): str(
                            m.get("target") or m.get("source") or ""
                        )
                        for m in (mappings or [])
                        if m.get("source")
                    },
                    mode="upsert",
                )
                write_mode = plan.mode or "upsert"
                activation_notes = list(plan.notes or [])
                if not conflict_columns:
                    conflict_columns = list(plan.primary_key)
                # Apply planner object name so SaaS writers hit the intended CRM object.
                if plan.object_name:
                    request.destination.table = plan.object_name
                if plan.batch_size:
                    request.destination.extra = dict(request.destination.extra or {})
                    request.destination.extra["activation_batch_size"] = plan.batch_size
                activation_notes.append(
                    f"Activation apply: mode={write_mode} pk={','.join(conflict_columns)} "
                    f"object={plan.object_name} batch={plan.batch_size}"
                )

            reuse_fit = reuse_approved_validate_population_fit(request)
            mongo.update_job_status(
                job_id,
                "running",
                phase="preflight",
                progress_pct=compute_transfer_progress_pct(phase="preflight"),
                message=execute_preflight_progress_message(request),
            )
            # Preflight for every destination kind (database, file_export, object
            # store). Skipping Validate for file sinks was an honesty hole —
            # PRODUCTION_SKU and Studio Execute must prove mapping gates first.
            if not request.skip_preflight:
                dest_ok, dest_msg = probe_destination(request.destination)
                parity = _engine_module._execute_preflight_parity_kwargs(
                    request,
                    destination_connected=dest_ok,
                    destination_table_exists_fallback=dest_table_exists_flag,
                )
                pf = run_file_preflight(
                    columns=columns,
                    column_types=schema,
                    row_count=len(records),
                    mappings=mappings,
                    destination_connected=dest_ok,
                    destination_error=None if dest_ok else dest_msg,
                    source_kind=request.source.kind,
                    source_format=request.source.format,
                    sync_mode=request.sync_mode,
                    sample_rows=_preflight_sample_rows(records),
                    # The whole batch is already in memory — decide bounded
                    # destination carriers here instead of letting the writer
                    # discover an unfit value mid-load. A just-approved Studio
                    # Validate already asked that question; write-time fit
                    # still binds every row.
                    # An empty batch stays the population even on a reused
                    # Validate: it is what proves a header-only file is empty.
                    population_rows=None if reuse_fit and records else records,
                    rows_are_population=not reuse_fit or not records,
                    skip_population_fit=reuse_fit,
                    confidence_threshold=confidence_threshold_for_mode(
                        request.validation_mode
                    ),
                    destination_column_types=dest_schema_types,
                    column_nullability=_source_nullability_probe(request.source),
                    destination_column_nullability=(
                        (request.destination.extra or {}).get("schema_nullability") or {}
                    ),
                    **_destination_filler_metadata(request.destination.extra),
                    destination_db_type=dst_fmt.lower(),
                    validation_mode=request.validation_mode,
                    source_table=(
                        request.source.table
                        or request.source.collection
                        or request.source_filename
                        or ""
                    ),
                    source_connector_id=request.source.connector_id or "",
                    source_config=endpoint_to_dict(request.source),
                    destination_table=(
                        request.destination.table
                        or request.destination.collection
                        or ""
                    ),
                    source_filename=request.source_filename or "",
                    source_file_id=str(
                        (getattr(request.source, "extra", None) or {}).get("file_id")
                        or getattr(request, "source_file_id", "")
                        or ""
                    ).strip(),
                    schema_policy=request.schema_policy,
                    backfill_new_fields=request.backfill_new_fields,
                    date_locale=request.date_locale,
                    number_locale=getattr(request, "number_locale", "") or "",
                    resume=resume,
                    **parity,
                )
                pf = apply_policy_gates(
                    pf,
                    _engine_module._execute_policy_gates_for_request(
                        request, source_columns=columns
                    ),
                    validation_mode=request.validation_mode,
                    destination_db_type=dst_fmt.lower(),
                )
                if not dest_ok:
                    mongo.update_job_status(
                        job_id,
                        "failed",
                        error=f"Destination unreachable: {dest_msg}",
                        phase="failed",
                        progress_pct=0,
                    )
                    line_msg = f"Destination unreachable: {dest_msg}"
                    lineage.emit_run_failed(
                        run_id=job_id,
                        job_id=job_id,
                        error=line_msg,
                        error_details={"reason": "Destination unreachable"},
                    )
                    return TransferResult(
                        success=False,
                        error=line_msg,
                        operation=request.operation,
                        job_id=job_id,
                    )
                if not pf["passed"]:
                    error_message, error_details = _fail_job_preflight(
                        mongo,
                        job_id,
                        pf,
                        lineage=lineage,
                        rows_read=total_rows,
                        sync_mode=str(getattr(request, "sync_mode", "") or ""),
                    )
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=_validation_plan_for_result(pf),
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
                        row_accounting=dict(error_details.get("row_accounting") or {}),
                    )

            # A stamped hash/artifact is always checked against the operator
            # Map contract it was taken over — never against post-enrich stamps.
            identity_maps = _operator_contract_maps(request, mappings)
            approved_hash = str(
                getattr(request, "approved_ddl_identity_hash", "") or ""
            )
            dest_db_fmt = str(getattr(request.destination, "format", None) or "")
            ddl_err = _enforce_ddl_identity(
                pf,
                identity_maps,
                dest_db=dest_db_fmt,
                approved_ddl_identity_hash=approved_hash,
                skip_preflight=bool(getattr(request, "skip_preflight", False)),
                preflight_mappings=mappings,
            )
            if ddl_err:
                mongo.update_job_status(
                    job_id, "failed", error=ddl_err, phase="failed", progress_pct=0
                )
                return TransferResult(
                    success=False,
                    error=ddl_err,
                    error_details={
                        "reason": "ddl_identity_mismatch",
                        "remediation": "Re-run Validate after Map/DDL changes.",
                    },
                    operation=request.operation,
                    job_id=job_id,
                )
            art_err, art_dict = _enforce_decision_artifact(
                pf,
                identity_maps,
                dest_db=dest_db_fmt,
                approved_decision_artifact_hash=str(
                    getattr(request, "approved_decision_artifact_hash", "") or ""
                ),
                decision_artifact=_request_decision_artifact_payload(request),
                skip_preflight=bool(getattr(request, "skip_preflight", False)),
                sync_mode=str(getattr(request, "sync_mode", "") or ""),
                error_policy="quarantine",
                destination_column_types=dest_schema_types,
                destination_table_exists=dest_table_exists_flag,
                source_column_types=schema,
                source_kind=str(getattr(request.source, "kind", "") or ""),
                source_format=str(getattr(request.source, "format", None) or src_fmt or ""),
                source_connector_id=str(getattr(request.source, "connector_id", "") or ""),
            )
            if art_err:
                mongo.update_job_status(
                    job_id, "failed", error=art_err, phase="failed", progress_pct=0
                )
                return TransferResult(
                    success=False,
                    error=art_err,
                    error_details={
                        "reason": "decision_artifact_mismatch",
                        "remediation": "Re-run Validate to stamp a Decision Artifact.",
                    },
                    operation=request.operation,
                    job_id=job_id,
                )
            if art_dict:
                try:
                    mongo.update_job_fields(
                        job_id,
                        {
                            "decision_artifact": art_dict,
                            "decision_artifact_hash": art_dict.get("content_hash"),
                        },
                    )
                except Exception:
                    pass
            if pf:
                mongo.update_job_status(
                    job_id, "running", phase="preflight", progress_pct=15, preflight=pf
                )

            # Data contract / circuit breaker enforcement.
            try:
                contract_id = enforce_or_create_contract(request, schema, mappings, pf)
            except ContractViolation as cv:
                msg = cv.message
                mongo.update_job_status(
                    job_id, "failed", error=msg, phase="failed", progress_pct=0
                )
                return TransferResult(
                    success=False,
                    error=msg,
                    error_details={"violations": cv.violations},
                    operation=request.operation,
                    job_id=job_id,
                )

            # History-aware data quality: compare this load to the last N runs
            # for the same source→destination route (null-rate, volume, mean MAD).
            load_history_report = _compare_and_publish_load_history(
                mongo,
                job_id,
                records,
                request,
                schema,
                validation_mode=request.validation_mode,
                row_count_hint=len(records),
            )
            if load_history_report.get("strict_blocked"):
                anomalies = list(load_history_report.get("anomalies") or [])
                msg = "Data quality anomaly: " + "; ".join(anomalies[:8])
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=msg,
                    phase="failed",
                    progress_pct=0,
                    load_history_report=load_history_report,
                )
                return TransferResult(
                    success=False,
                    error=msg,
                    operation=request.operation,
                    job_id=job_id,
                    destination_summary={"load_history_report": load_history_report},
                    error_details={"load_history_report": load_history_report},
                )

            ddl_log: list[str] = []
            dest_summary: dict = {}
            rows_written = 0

            mongo.update_job_status(
                job_id,
                "running",
                phase="writing",
                progress_pct=compute_transfer_progress_pct(
                    phase="writing", rows_processed=0, total_rows=total_rows
                )
                or 5,
                message=f"Writing {row_count_label(total_rows)} rows…",
            )

            def _check_cancelled(committed_rows: int | None = None) -> None:
                # After the chunk commits. The count has to travel with the
                # cancel: the status was often already "cancelled" at 0 rows
                # while this table held the batch.
                _raise_if_job_cancelled(
                    mongo, job_id, rows_written=committed_rows
                )

            _quarantine_persisted = [0]

            def on_checkpoint(
                chunk: int, chunks: int, rows: int, checkpoint: dict | None = None
            ) -> None:
                _check_cancelled(rows)
                pct = compute_transfer_progress_pct(
                    phase="writing",
                    rows_processed=rows,
                    total_rows=total_rows,
                    chunk=chunk,
                    chunks=chunks,
                )
                update = dict(
                    records_processed=rows,
                    chunk_current=chunk,
                    chunk_total=chunks,
                    message=batch_write_message(
                        chunk,
                        chunks,
                        rows,
                        checkpoint=checkpoint,
                        stream_contracts=request.stream_contracts,
                    ),
                )
                if pct is not None:
                    update["progress_pct"] = pct
                if checkpoint:
                    _persist_checkpoint_quarantine_delta(
                        job_id,
                        checkpoint if isinstance(checkpoint, dict) else None,
                        request=request,
                        last_persisted=_quarantine_persisted,
                    )
                    from services.job_document_budget import (
                        slim_checkpoint_for_job_store,
                        slim_rejected_details,
                    )

                    details = list(checkpoint.get("rejected_details") or [])
                    preview, total, truncated = slim_rejected_details(details)
                    # Never embed the full writer checkpoint (unbounded quarantine)
                    # into transfer_jobs — that is the DocumentTooLarge failure mode.
                    update["checkpoint"] = slim_checkpoint_for_job_store(checkpoint)
                    update["destination_summary"] = checkpoint_quarantine_summary(
                        checkpoint, details, preview, total, truncated
                    )
                    _promote_cdc_job_fields(checkpoint, update)
                _progress_write_or_cancel(mongo, job_id, **update)

            _throttled_progress = ThrottledCheckpoint(on_checkpoint)

            def throttled_checkpoint(
                chunk: int, chunks: int, rows: int, checkpoint: dict | None = None
            ) -> None:
                # D102: check cancel on EVERY batch boundary; the throttled
                # progress write below may drop intermediate calls.
                _check_cancelled(rows)
                _throttled_progress(chunk, chunks, rows, checkpoint=checkpoint)
            backfill_fields = _run_backfill_new_fields(
                request, mappings, dest_schema_types, dest_table_exists_flag
            )

            # Reconciliation must weigh the destination against the whole source
            # population, not the tail a resumed pass happens to write.
            resume_full_records: list[dict] = []
            resume_skipped_rows = 0

            if request.destination.kind == "database":
                # Buffered path reloads the in-memory source. Positional
                # ``records[skip_n:]`` is unsafe under source reorder — silent
                # wrong-row skip. Idempotent resume re-applies the full
                # population via upsert when a key exists (MERGE-class).
                checkpoint_has_progress = _checkpoint_has_progress(checkpoint)
                should_drop_full_refresh = should_drop_destination_for_sync(
                    request_sync_mode=request.sync_mode,
                    contract_sync_mode=contract.sync_mode if contract else None,
                ) and not checkpoint_has_progress
                if resume and checkpoint_has_progress:
                    skip_n = max(
                        int(getattr(checkpoint, "rows_processed", 0) or 0),
                        int(getattr(checkpoint, "offset", 0) or 0),
                    )
                    if skip_n > 0:
                        if skip_n >= len(records):
                            mongo.update_job_status(
                                job_id,
                                "completed",
                                phase="completed",
                                progress_pct=100,
                                message=(
                                    f"Resume: checkpoint already at {skip_n:,} row(s); "
                                    "nothing left to write."
                                ),
                                records_processed=skip_n,
                            )
                            return TransferResult(
                                success=True,
                                operation=request.operation,
                                job_id=job_id,
                                records_transferred=skip_n,
                                destination_summary={
                                    "resumed_from": skip_n,
                                    "rows_written": 0,
                                    "note": "checkpoint ahead of or equal to source size",
                                },
                            )
                        if dest_driver == "kafka" and write_mode == "insert":
                            # No merge exists on a topic — resume is a
                            # positional skip of the produced prefix, then
                            # append the remainder (at-least-once).
                            records = records[skip_n:]
                            dest_summary_resume_note = (
                                f"Resume after {skip_n:,} produced row(s) — "
                                "publishing the remainder (Kafka append)."
                            )
                        elif write_mode == "insert" and not conflict_columns:
                            raise ValueError(
                                "Cannot safely resume a buffered insert without primary key; "
                                "use upsert sync mode or restart with full_refresh_overwrite"
                            )
                        elif conflict_columns:
                            # Force upsert so re-applying prior rows is idempotent.
                            write_mode = "upsert"
                            dest_summary_resume_note = (
                                f"Idempotent resume after {skip_n:,} prior row(s) — "
                                "re-applying full population via upsert (key-addressed)."
                            )
                        else:
                            dest_summary_resume_note = (
                                f"Resume after {skip_n:,} prior row(s)."
                            )
                        # Keep full population — no positional slice (Kafka's
                        # positional skip already applied above).
                        # Engine record spill owns the population; do not
                        # duplicate the dict list for resume.
                        resume_full_records = []
                        resume_skipped_rows = 0
                        total_rows = len(records)
                        mongo.update_job_status(
                            job_id,
                            "running",
                            total_rows=total_rows,
                            records_processed=0,
                            message=dest_summary_resume_note,
                        )

                def _write_destination_with_drop():
                    # Drop inside the retry boundary so a failed full-refresh write
                    # retries from an empty table and cannot duplicate already-loaded rows.
                    use_staging = bool(getattr(request, "write_via_staging", False))
                    # Mirror/SCD2 already use their own staging algorithm.
                    if use_staging and effective_sync_lower not in (
                        "scd2",
                        "full_refresh_mirror",
                        "mirror",
                    ):
                        from services.pre_ingestion_staging import (
                            write_via_pre_ingestion_staging,
                        )

                        return write_via_pre_ingestion_staging(
                            request.destination,
                            records,
                            columns,
                            schema,
                            mappings,
                            validation_mode=request.validation_mode,
                            backfill_new_fields=backfill_fields,
                            write_mode=write_mode,
                            conflict_columns=conflict_columns,
                            job_id=job_id,
                            on_checkpoint=throttled_checkpoint,
                            drop_primary=should_drop_full_refresh,
                        )
                    if should_drop_full_refresh:
                        mongo.update_job_status(
                            job_id,
                            "running",
                            phase="writing",
                            message=(
                                "Preparing destination — clearing table for full refresh…"
                            ),
                        )
                        _engine_module._drop_destination_table(
                            request.destination,
                            mappings=mappings,
                            checkpoint=checkpoint,
                            checkpoint_service=checkpoint_service,
                            mongo=mongo,
                            job_id=job_id,
                        )
                        mongo.update_job_status(
                            job_id,
                            "running",
                            phase="writing",
                            message="Connecting to destination and creating table…",
                        )
                    from services.preflight_source_kind import (
                        source_is_record_batch,
                    )

                    with _job_phase_heartbeat(
                        mongo,
                        job_id,
                        phase="writing",
                        message="Connecting to destination and writing…",
                    ):
                        return write_destination_database(
                            request.destination,
                            records,
                            columns,
                            schema,
                            mappings,
                            on_checkpoint=throttled_checkpoint,
                            validation_mode=request.validation_mode,
                            backfill_new_fields=backfill_fields,
                            write_mode=write_mode,
                            conflict_columns=conflict_columns,
                            job_id=job_id,
                            skip_preflight=request.skip_preflight,
                            sync_mode=request.sync_mode,
                            # File/object-store blanks into typed columns are SQL
                            # NULL (D1): the buffered path dropped the flag, so
                            # Execute rejected blanks preflight accepted.
                            empty_cells_as_null=source_is_record_batch(
                                getattr(request.source, "kind", "") or "database",
                                request.source.format,
                            ),
                            upsert_ddl_approved=bool(
                                getattr(
                                    request, "ddl_changes_acknowledged", False
                                )
                            ),
                            release_records=True,
                            retain_engine_spill=True,
                            collect_mirror_keys=effective_sync_lower
                            in ("full_refresh_mirror", "mirror"),
                        )

                _scd2_opts = None
                if effective_sync_lower == "scd2":
                    # A recipe ``scd2`` step overrides the destination PK as
                    # the natural key and declares stamp column names,
                    # tracked columns, Type-1 untracked refresh, version
                    # stamping, and the soft-close contract (B1). Keys and
                    # tracked are declared in SOURCE column space; translate
                    # to the mapped target names the merge writes.
                    try:
                        from services.shape_models import ShapeRecipe

                        # The runner parsed it against the SOURCE columns;
                        # ``columns`` is now the shaped image (a rename
                        # before the scd2 step would not parse against it).
                        _rcp = (
                            shape_runner.recipe
                            if shape_runner is not None
                            else ShapeRecipe.parse(
                                getattr(request, "shape_recipe", None) or {},
                                source_columns=columns,
                            )
                        )
                        _st = _rcp.scd2_step
                        if _st is not None:
                            _scd2_opts = _rcp.scd2_column_names() or {}
                            _src2tgt = {
                                str(m.get("source") or ""): str(
                                    m.get("target") or ""
                                )
                                for m in (mappings or [])
                                if isinstance(m, dict)
                            }
                            _rkeys = [
                                str(_src2tgt.get(k) or k)
                                for k in _scd2_opts.get("keys") or []
                            ]
                            if _rkeys:
                                conflict_columns = _rkeys
                            if _scd2_opts.get("tracked"):
                                _scd2_opts["tracked"] = [
                                    str(_src2tgt.get(t) or t)
                                    for t in _scd2_opts["tracked"]
                                ]
                    except Exception as exc:  # noqa: BLE001
                        raise RuntimeError(
                            "SCD2 recipe step refused — the declared history "
                            f"contract does not parse against the source "
                            f"columns: {exc}"
                        ) from exc
                if effective_sync_lower == "scd2" and conflict_columns:
                    from connectors.engine_record_spill import (
                        ENGINE_SPILL_SUMMARY_KEY,
                        spill_engine_write_records,
                    )

                    dest_extra = getattr(request.destination, "extra", None)
                    from services.dest_precount import PRECOUNT_KEY, precount_destination
                    from src.transfer.adapters import resolve_connector_config

                    try:
                        scd2_rows_before = precount_destination(
                            request.destination,
                            resolve_connector_config(request.destination),
                        )
                    except Exception:
                        scd2_rows_before = None
                    scd2_spill = spill_engine_write_records(
                        records,
                        columns,
                        mappings,
                        extra=dest_extra if isinstance(dest_extra, dict) else {},
                        clear_records=True,
                    )
                    try:
                        scd2_summary = with_retry(
                            lambda: apply_scd2(
                                request.destination,
                                records,
                                columns,
                                schema,
                                mappings,
                                conflict_columns,
                                validation_mode=request.validation_mode,
                                source_spool=scd2_spill.spool,
                                # SCD2 never narrows its read (bind_batch_incremental),
                                # so an unlimited read is the whole source population.
                                complete_snapshot=not int(request.limit or 0),
                                scd2_opts=_scd2_opts,
                            ),
                            budget=RetryBudget(
                                max_attempts=3,
                                base_delay_seconds=0.5,
                                max_delay_seconds=5.0,
                            ),
                        )
                    except Exception:
                        scd2_spill.close()
                        raise
                    dest_summary = {
                        "table": request.destination.table
                        or request.destination.collection,
                        "schema": _schema_for_endpoint(request.destination),
                        "checksum": scd2_summary.get("active_checksum", ""),
                        "scd2": scd2_summary,
                        "scd2_opts": _scd2_opts,
                        "rejected_details": list(
                            scd2_summary.get("rejected_details") or []
                        ),
                        "rejected_rows": int(scd2_summary.get("rejected_rows") or 0),
                        "source_row_count": int(scd2_spill.unexpanded_row_count),
                        "source_row_count_source": "engine_record_spill",
                        "engine_record_spill": {
                            "spilled": scd2_spill.spilled,
                            "source_row_count": scd2_spill.source_row_count,
                            "unexpanded_row_count": scd2_spill.unexpanded_row_count,
                        },
                        ENGINE_SPILL_SUMMARY_KEY: scd2_spill,
                    }
                    if isinstance(scd2_rows_before, int):
                        dest_summary[PRECOUNT_KEY] = int(scd2_rows_before)
                    if scd2_summary.get("ok") is False:
                        _fail_spill = dest_summary.pop(ENGINE_SPILL_SUMMARY_KEY, None)
                        if _fail_spill is not None:
                            _fail_spill.close()
                        block_msg = str(
                            scd2_summary.get("error")
                            or "SCD2 map/Risk Contract blocked history merge"
                        )
                        _persist_job_quarantine(
                            job_id,
                            dest_summary,
                            request,
                            already_persisted=_quarantine_persisted,
                        )
                        mongo.update_job_status(
                            job_id,
                            "failed",
                            phase="failed",
                            error=block_msg,
                            message=block_msg,
                            records_processed=0,
                            rejected_rows=int(dest_summary.get("rejected_rows") or 0),
                            rejected_details=(
                                dest_summary.get("rejected_details") or []
                            )[:2000],
                            destination_summary=dest_summary,
                            ddl_log=list(ddl_log or [])[:500],
                        )
                        return TransferResult(
                            success=False,
                            error=block_msg,
                            job_id=job_id,
                            operation=request.operation,
                            destination_summary=dest_summary,
                            ddl_executed=list(ddl_log or []),
                        )
                    rows_written = scd2_summary.get("rows_written", 0)
                    ddl_log.append(
                        f"SCD2 merge: {scd2_summary.get('active_rows', 0)} active, "
                        f"{scd2_summary.get('updated_rows', 0)} expired"
                        + (
                            f", {dest_summary['rejected_rows']} quarantined"
                            if dest_summary.get("rejected_rows")
                            else ""
                        )
                    )
                else:
                    rows_written, ddl_log, dest_summary = with_retry(
                        _write_destination_with_drop,
                        budget=RetryBudget(
                            max_attempts=3,
                            base_delay_seconds=0.5,
                            max_delay_seconds=5.0,
                        ),
                    )
                    if dest_summary.get("promote_blocked"):
                        from connectors.engine_record_spill import ENGINE_SPILL_SUMMARY_KEY

                        _blocked_spill = dest_summary.pop(ENGINE_SPILL_SUMMARY_KEY, None)
                        if _blocked_spill is not None:
                            _blocked_spill.close()
                        # Strict/maximum + staging: primary untouched; persist DLQ then fail.
                        _persist_job_quarantine(
                            job_id,
                            dest_summary,
                            request,
                            already_persisted=_quarantine_persisted,
                        )
                        _attach_job_rollback_plan(job_id, dest_summary, request)
                        block_msg = (
                            (dest_summary.get("pre_ingestion_staging") or {}).get(
                                "blocked_reason"
                            )
                            or "Pre-ingestion staging blocked promote due to validation failures"
                        )
                        mongo.update_job_status(
                            job_id,
                            "failed",
                            phase="failed",
                            error=block_msg,
                            message=block_msg,
                            records_processed=0,
                            rejected_rows=int(dest_summary.get("rejected_rows") or 0),
                            rejected_details=(
                                dest_summary.get("rejected_details") or []
                            )[:2000],
                            destination_summary=dest_summary,
                            ddl_log=list(ddl_log or [])[:500],
                        )
                        return TransferResult(
                            success=False,
                            error=block_msg,
                            job_id=job_id,
                            operation=request.operation,
                            destination_summary=dest_summary,
                            ddl_executed=list(ddl_log or []),
                        )
                    if activation_notes:
                        ddl_log = list(ddl_log or []) + [
                            f"reverse-ETL: {n}" for n in activation_notes
                        ]
                        dest_summary["reverse_etl"] = {"notes": activation_notes}
                    if (
                        effective_sync_lower in ("full_refresh_mirror", "mirror")
                        and conflict_columns
                    ):
                        from connectors.engine_record_spill import (
                            ENGINE_SPILL_SUMMARY_KEY,
                            MIRROR_PK_SUMMARY_KEY,
                        )

                        spill_holder = (
                            dest_summary.get(ENGINE_SPILL_SUMMARY_KEY)
                            if isinstance(dest_summary, dict)
                            else None
                        )
                        legacy_keys = (
                            dest_summary.pop(MIRROR_PK_SUMMARY_KEY, None)
                            if isinstance(dest_summary, dict)
                            else None
                        )
                        from services.sync_cursor import (
                            resolve_sync_contract as _resolve_mirror_contract,
                        )

                        _mirror_contract = _resolve_mirror_contract(
                            list(getattr(request, "stream_contracts", None) or [])
                        )
                        # Soft delete is the default; hard only when declared.
                        from services.mirror_engine import resolve_mirror_delete_mode

                        _mirror_delete_mode = resolve_mirror_delete_mode(_mirror_contract)
                        mirror_summary = apply_inferred_soft_deletes(
                            request.destination,
                            records,
                            columns,
                            schema,
                            mappings,
                            conflict_columns,
                            delete_mode=_mirror_delete_mode,
                            source_spool=getattr(spill_holder, "spool", None),
                            source_key_spool=getattr(spill_holder, "key_spool", None),
                            pk_sources=getattr(spill_holder, "pk_sources", None),
                            source_pk_tuples=legacy_keys
                            if getattr(spill_holder, "key_spool", None) is None
                            and getattr(spill_holder, "spool", None) is None
                            else None,
                        )
                        dest_summary["mirror"] = mirror_summary
                        rows_written = mirror_summary.get("active_rows", rows_written)
                write_done_pct = compute_transfer_progress_pct(
                    phase="writing",
                    rows_processed=rows_written,
                    total_rows=total_rows,
                )
                _progress_write_or_cancel(
                    mongo,
                    job_id,
                    records_processed=rows_written,
                    **(
                        {"progress_pct": write_done_pct}
                        if write_done_pct is not None
                        else {}
                    ),
                )
            elif request.destination.kind == "file_export":
                try:
                    export_bytes, export_name, dest_summary = with_retry(
                        lambda: write_destination_file(
                            request.destination,
                            records,
                            columns,
                            source_format=src_fmt,
                            mappings=mappings,
                            column_types=request.column_types or schema,
                            validation_mode=request.validation_mode,
                        ),
                        budget=RetryBudget(
                            max_attempts=3,
                            base_delay_seconds=0.5,
                            max_delay_seconds=5.0,
                        ),
                    )
                except FileExportMapBlocked as blocked:
                    dest_summary = {
                        "rejected_details": list(blocked.rejected_details),
                        "rejected_rows": int(blocked.rejected_rows),
                        "format": request.destination.format or "",
                    }
                    _persist_job_quarantine(
                        job_id,
                        dest_summary,
                        request,
                        already_persisted=_quarantine_persisted,
                    )
                    block_msg = str(blocked)
                    mongo.update_job_status(
                        job_id,
                        "failed",
                        phase="failed",
                        error=block_msg,
                        message=block_msg,
                        records_processed=0,
                        rejected_rows=int(dest_summary.get("rejected_rows") or 0),
                        rejected_details=(
                            dest_summary.get("rejected_details") or []
                        )[:2000],
                        destination_summary=dest_summary,
                    )
                    return TransferResult(
                        success=False,
                        error=block_msg,
                        job_id=job_id,
                        operation=request.operation,
                        destination_summary=dest_summary,
                    )
                # Honesty: count exported mapped rows, not source batch size.
                rows_written = int(dest_summary.get("rows") or 0)
                try:
                    dest_summary.update(
                        land_export_bytes(
                            request.destination,
                            export_bytes,
                            job_id=job_id,
                            sync_mode=request.sync_mode,
                            export_name=export_name,
                        )
                    )
                except ValueError as refusal:
                    mongo.update_job_status(
                        job_id, "failed", error=str(refusal), phase="failed"
                    )
                    return TransferResult(
                        success=False,
                        error=str(refusal),
                        job_id=job_id,
                        operation=request.operation,
                    )
                ddl_log.append(
                    f"Exported {rows_written} rows to {dest_summary['filename']}"
                )
            else:
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=f"Unknown destination: {request.destination.kind}",
                    phase="failed",
                )
                return TransferResult(
                    success=False,
                    error=f"Unknown destination kind: {request.destination.kind}",
                    job_id=job_id,
                )

            with _reconcile_phase_heartbeat(
                mongo,
                job_id,
                processed=int(rows_written or 0),
                total=int(rows_written or 0),
                **_reconcile_heartbeat_scope(dest_summary),
            ):
                if isinstance(dest_summary, dict):
                    dest_summary.setdefault("sync_mode", effective_sync)
                    dest_summary.setdefault("job_id", job_id)
                    # Gate-8 keyed upsert proof needs PK on the summary even when
                    # the writer omits written_ids (SQLite/PG historically did).
                    if conflict_columns:
                        dest_summary.setdefault(
                            "conflict_columns", list(conflict_columns)
                        )
                        dest_summary.setdefault(
                            "primary_key_columns", list(conflict_columns)
                        )
                    # Stamped before reconciliation so the recipe identity is on
                    # the summary the ladder and the ledger read, not only on the
                    # record written afterwards.
                    _stamp_shape_evidence(dest_summary, shape_runner)
                from connectors.engine_record_spill import ENGINE_SPILL_SUMMARY_KEY

                spill_holder = (
                    dest_summary.pop(ENGINE_SPILL_SUMMARY_KEY, None)
                    if isinstance(dest_summary, dict)
                    else None
                )
                if resume_skipped_rows and isinstance(dest_summary, dict):
                    dest_summary["resumed_from"] = resume_skipped_rows
                    dest_summary["resume_full_source_rows"] = int(
                        (dest_summary.get("engine_record_spill") or {}).get(
                            "unexpanded_row_count"
                        )
                        or len(resume_full_records)
                        or len(records)
                    )
                try:
                    recon = run_reconciliation(
                        endpoint=request.destination,
                        # Full population via spool when the engine released
                        # the dict list; otherwise the in-memory records.
                        records=resume_full_records or records,
                        columns=columns,
                        rows_written=rows_written,
                        writer_checksum=dest_summary.get("checksum")
                        or dest_summary.get("active_checksum", ""),
                        dest_summary=dest_summary,
                        mappings=mappings,
                        source_schema=schema,
                        validation_mode=request.validation_mode,
                        source_endpoint=request.source,
                        source_spool=getattr(spill_holder, "spool", None),
                        shape_recipe=getattr(request, "shape_recipe", None),
                    )
                finally:
                    if spill_holder is not None:
                        spill_holder.close()
            canonical_rejected_details = _snapshot_rejected_details(dest_summary)
            dest_summary = pii_guard.redact_destination_summary(dest_summary, mappings)
            recon = pii_guard.redact_reconciliation(recon, mappings)
            if not recon.get("passed"):
                fail_message = _engine_module._note_failed_batch_undo(
                    request,
                    dest_summary,
                    recon.get("message", "Reconciliation failed"),
                    recon=recon,
                )
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=fail_message,
                    phase="failed",
                    progress_pct=99,
                    message=fail_message,
                    reconciliation=recon,
                    destination_summary=dest_summary,
                    rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                    coerced_null_rows=int(
                        dest_summary.get("coerced_null_rows", 0) or 0
                    ),
                )
                return TransferResult(
                    success=False,
                    error=fail_message,
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=_failed_run_records_transferred(
                        dest_summary, rows_written
                    ),
                    destination_summary=dest_summary,
                    reconciliation=recon,
                )
            lost = fail_closed_on_silent_loss(
                job_id=job_id,
                request=request,
                dest_summary=dest_summary,
                recon=recon,
                rows_written=rows_written,
                operation=request.operation,
            )
            if lost is not None:
                return lost
            verified = True

            # Reconciliation passed: the delta is at rest, so the watermark may
            # move. Persisting it earlier would skip these rows after a failed run.
            incremental_bound.commit(job_id=job_id)

            explanation = _build_explanation(
                request,
                columns,
                schema,
                mappings,
                recon,
                dest_summary,
                pf,
                rows_written,
            )
            from services.job_status import terminal_status_for

            terminal_status = terminal_status_for(
                dest_summary.get("rejected_rows", 0),
                dest_summary.get("coerced_null_rows", 0),
            )
            if load_history_report:
                dest_summary["load_history_report"] = load_history_report
            _persist_job_quarantine(
                job_id,
                dest_summary,
                request,
                already_persisted=_quarantine_persisted,
                canonical_details=canonical_rejected_details,
            )
            _attach_job_rollback_plan(job_id, dest_summary, request)
            apply_post_load_transforms(request, dest_summary)
            _settle_overwrite_backup(request.destination, restore=False)
            if source_stamp.get("kafka_cursor"):
                # The buffered read took one poll batch; its offsets commit
                # with the job, never earlier (D85).
                _kafka_accumulate_offsets(dest_summary, source_stamp["kafka_cursor"])
            commit_kafka_offsets_on_accept(request, dest_summary)
            _write_success_status(
                mongo,
                job_id,
                terminal_status,
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get(
                    "message", f"Transferred {rows_written:,} rows successfully"
                ),
                # The scan heartbeat paints 0–99 while rows hash; a passed
                # Gate-8 is the 100 — verified jobs must not freeze at 99 (M1).
                reconcile_pct=100,
                destination_database=dest_summary.get(
                    "database", request.destination.database or ""
                ),
                destination_collection=_job_destination_name(
                    dest_summary, request.destination
                ),
                rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                coerced_null_rows=int(dest_summary.get("coerced_null_rows", 0) or 0),
                rejected_details=(dest_summary.get("rejected_details") or [])[:2000],
                destination_summary=dest_summary,
                explanation=explanation,
                mapping_proof=_mapping_proof_for_request(request),
                reconciliation=recon,
                load_history_report=dest_summary.get("load_history_report") or {},
                ddl_executed=list(ddl_log or [])[:500],
                ddl_log=list(ddl_log or [])[:500],
                sync_mode=request.sync_mode,
                schema_policy=request.schema_policy,
                validation_mode=request.validation_mode,
            )
            try:
                from services.usage_metering import record_transfer_usage

                record_transfer_usage(
                    job_id=job_id,
                    workspace_id=str(getattr(request, "workspace_id", "") or ""),
                    rows_written=rows_written,
                    source_type=request.source.format,
                    dest_type=request.destination.format,
                )
            except Exception as exc:
                logger.debug("usage metering suppressed: %s", exc, exc_info=exc)
            if getenv_brand("POST_TRANSFER_TRAINING", "").lower() in {
                "1",
                "true",
                "on",
            }:
                try:
                    from services.value_serializer import wire_cell_text

                    redacted_training = pii_guard.redact_records(records[:5], mappings)
                    samples = {
                        c: [
                            wire_cell_text(r.get(c, ""))
                            for r in redacted_training
                            if r.get(c) is not None
                        ]
                        for c in columns
                    }
                    schedule_training_on_transfer(
                        request.source_filename
                        or dest_summary.get("table", "transfer"),
                        columns,
                        len(records),
                        samples,
                    )
                except Exception as exc:
                    logger.debug(
                        "post-transfer training suppressed: %s", exc, exc_info=exc
                    )

            lineage.emit_preflight_completed(
                run_id=job_id,
                passed=True,
                readiness_score=pf.get("readiness_score", 100) if pf else 100,
                validation_plan=_validation_plan_for_result(pf),
            )
            lineage.emit_lineage(
                run_id=job_id,
                source_dataset=f"{request.source.kind}/{src_fmt}/{request.source.table or request.source.collection}",
                target_dataset=f"{request.destination.kind}/{dst_fmt}/{request.destination.table or request.destination.collection}",
                mappings=[
                    {
                        "source": m.get("source"),
                        "target": m.get("target"),
                        "confidence": m.get("confidence"),
                    }
                    for m in mappings
                ],
            )
            lineage.emit_run_completed(
                run_id=job_id,
                job_id=job_id,
                records_transferred=rows_written,
                source_summary={
                    "kind": request.source.kind,
                    "format": src_fmt,
                    "columns": len(columns),
                    "rows": len(records),
                },
                destination_summary=dest_summary,
            )
            # Append this load to the route ring buffer (last-N multi-load intelligence).
            if load_history_report:
                dest_summary["load_history_report"] = load_history_report
            _persist_load_history_profile(
                request,
                records,
                schema,
                job_id=job_id,
                dest_summary=dest_summary,
                row_count=len(records),
                mappings=mappings,
            )
            finalize_contract(contract_id, success=True)
            return TransferResult(
                success=True,
                job_id=job_id,
                records_transferred=rows_written,
                operation=request.operation,
                source_summary={
                    "kind": request.source.kind,
                    "format": src_fmt,
                    "columns": len(columns),
                    "rows": len(records),
                },
                destination_summary=dest_summary,
                ddl_executed=ddl_log,
                columns=columns,
                reconciliation=recon,
                validation_plan=_validation_plan_for_result(pf),
                payload_shape=pf.get("payload_shape") if pf else {},
                contract_id=contract_id,
                explanation=explanation,
                mapping_proof=_mapping_proof_for_request(request),
            )
        except WriteBatchBlocked as blocked:
            _settle_overwrite_backup(request.destination, restore=True)
            dest_summary = {
                **(blocked.dest_summary or {}),
                "rejected_details": list(blocked.rejected_details),
                "rejected_rows": int(blocked.rejected_rows),
                "rows_written": int(blocked.rows_written),
                "ok": False,
                "error": str(blocked),
            }
            _persist_job_quarantine(
                job_id,
                dest_summary,
                request,
                already_persisted=_quarantine_persisted,
            )
            from src.transfer.job_failure import account_blocked_write

            kept, block_msg = account_blocked_write(mongo, job_id, blocked)
            mongo.update_job_status(
                job_id,
                "failed",
                phase="failed",
                error=block_msg,
                message=block_msg,
                records_processed=kept,
                rejected_rows=int(dest_summary.get("rejected_rows") or 0),
                rejected_details=(
                    dest_summary.get("rejected_details") or []
                )[:2000],
                destination_summary=dest_summary,
            )
            return TransferResult(
                success=False,
                error=block_msg,
                job_id=job_id,
                operation=request.operation,
                records_transferred=kept,
                destination_summary=dest_summary,
            )
        except Exception as e:
            if verified:
                # QA MX2-15: proven rows must not be failed or rolled back by a
                # bookkeeping step that raised after Gate-8 passed.
                return _finish_verified_run(
                    mongo,
                    job_id,
                    e,
                    request=request,
                    rows_written=int(rows_written or 0),
                    dest_summary=dest_summary,
                    recon=recon,
                )
            _settle_backup_after_error(request.destination, e)
            finalize_contract(contract_id, success=False)
            display, error_details = _fail_runtime_job(
                mongo, job_id, e, lineage=lineage, request=request
            )
            _undo_note = getattr(e, "failed_batch_undo_note", "")
            if _undo_note and _undo_note not in display:
                display = f"{display} {_undo_note}"
            return TransferResult(
                success=False,
                job_id=job_id,
                error=display,
                error_details=error_details,
                operation=request.operation,
                contract_id=contract_id,
            )

    def _execute_streaming(
        self,
        request: TransferRequest,
        job_id: str,
        mongo,
        src_fmt: str,
        resume: bool = False,
        checkpoint: Any = None,
        checkpoint_service: Any = None,
    ) -> TransferResult:
        """Batched DB→DB path — never loads full table into memory."""
        dst_fmt = request.destination.format or "mongodb"
        pf: dict | None = None
        contract_id = ""
        load_history_report: dict[str, Any] = {}
        verified = False
        number_token = set_active_number_locale(_run_number_locale(request))
        from services.transform_engine import (
            reset_strict_number_reading,
            set_strict_number_reading,
        )

        strict_number_token = set_strict_number_reading(
            str(getattr(request, "validation_mode", "") or "strict").strip().lower()
            == "strict"
            and not str(getattr(request, "number_locale", "") or "").strip()
        )
        try:
            if getattr(request, "ddl_changes_acknowledged", False):
                # D101: the writers gate the unique index an upsert needs on an
                # existing table on this flag. Only the buffered path passed it,
                # so a streamed run refused DDL the operator had approved.
                request.destination.extra = {
                    **(request.destination.extra or {}),
                    "upsert_ddl_approved": True,
                }
            mongo.update_job_status(
                job_id,
                "running",
                phase="reading",
                progress_pct=5,
                message=opening_analysis_message(request.stream_contracts),
            )
            # Several tables: the primary sample is that stream's CALL or
            # SELECT when it has one. Peeking the table would map columns the
            # writer never reads.
            from services.execute_shape_route import peek_declared_source
            sdk_descriptor = _sdk_source_descriptor_for_format(src_fmt)
            sdk_source = sdk_descriptor is not None
            sdk_state = (
                getattr(checkpoint, "cursor_value", None) if sdk_source else None
            )
            peeked = peek_declared_source(
                request.source,
                request.stream_contracts,
                sdk_state=sdk_state,
                include_batch=sdk_source,
            )
            if sdk_source:
                columns, schema, total_rows, sample_rows, initial_batch = peeked
            else:
                columns, schema, total_rows, sample_rows = peeked
                initial_batch = None
            if request.limit > 0 and total_rows is not None:
                total_rows = min(total_rows, request.limit)
            if kafka_nothing_past_committed(request, total_rows):
                return incremental_no_op_result(request, job_id, None)
            if total_rows == 0 and not columns:
                mongo.update_job_status(
                    job_id, "failed", error="Source table is empty", phase="failed"
                )
                return TransferResult(
                    success=False,
                    error="Source table is empty",
                    operation=request.operation,
                    job_id=job_id,
                )
            if total_rows == 0 and columns:
                # A table with columns and no rows is a real object. The stream
                # creates the destination and completes with a measured 0.
                extra = dict(request.destination.extra or {})
                extra["create_empty_source"] = True
                request.destination.extra = extra

            read_columns = list(columns)
            raw_sample_size = len(sample_rows)
            if request.source_filter:
                sample_rows = apply_row_filter(sample_rows, request.source_filter)

            # The recipe runs on the read, so Map, the narrowing scan and the DDL
            # identity are decided from the shaped columns and values the writer
            # will receive. A separate throwaway runner shapes the design-time
            # sample: those effects are not the population's.
            _settle_locales(request, sample_rows, columns)
            # design_runner shapes the primary sample. The sequential writer
            # builds one runner per stream from shape_by_stream.
            from services.execute_shape_route import open_execute_shape

            shape_plan = open_execute_shape(
                request, columns, job_id=job_id, schema=schema
            )
            if shape_plan.failure is not None:
                return shape_plan.failure
            design_runner = shape_plan.design_runner
            shape_runner = shape_plan.shape_runner
            shape_by_stream = shape_plan.shape_by_stream
            approved_shape_hash = shape_plan.approved_shape_hash
            sample_probe = (
                design_runner.fork() if design_runner is not None else None
            )
            if sample_probe is not None:
                sample_rows = sample_probe.records(sample_rows)
            if (
                not sample_rows
                and raw_sample_size
                and (request.source_filter or sample_probe is not None)
            ):
                # The rewrites emptied the first page, not the table. Read on
                # rather than let the gates judge a run on no rows at all.
                widened_probe = (
                    design_runner.fork() if design_runner is not None else None
                )
                sample_rows = _widen_design_sample(
                    request.source,
                    read_columns,
                    request.source_filter,
                    widened_probe,
                    wanted=raw_sample_size,
                )
                if widened_probe is not None:
                    sample_probe = widened_probe
            if sample_probe is not None:
                columns = list(sample_probe.output_columns or columns)
                schema = shaped_schema(sample_probe, sample_rows, schema)

            mongo.update_job_status(
                job_id, "running", total_rows=total_rows, records_processed=0
            )
            dest_schema_types, dest_table_exists_flag = _engine_module._connect_destination(
                request.destination,
                sync_mode=request.sync_mode,
                mongo=mongo,
                job_id=job_id,
            )
            schema = _prefer_approved_column_types(request, schema)
            mappings = _enrich_mappings_with_types(
                _engine_module._auto_map(
                    request, columns, schema, sample_rows=sample_rows, job_id=job_id
                ),
                column_types=schema,
                dest_types=dest_schema_types,
            )
            mappings = _apply_schema_auto_propagate(
                request=request,
                columns=columns,
                schema=schema,
                mappings=mappings,
                dest_schema_types=dest_schema_types,
            )
            _record_schema_propagation(
                mongo, job_id, columns, mappings, dest_schema_types
            )
            mappings = _stamp_additive_mappings_for_write(
                request,
                mappings,
                column_types=schema,
                dest_types=dest_schema_types,
                sample_rows=sample_rows[:100] if sample_rows else None,
                dest_table_exists=dest_table_exists_flag,
            )
            dest_schema_types = _apply_sampled_dest_profile(
                request.destination, mappings, dest_schema_types
            )
            reuse_fit = reuse_approved_validate_population_fit(request)
            mongo.update_job_status(
                job_id,
                "running",
                phase="preflight",
                progress_pct=compute_transfer_progress_pct(phase="preflight"),
                message=execute_preflight_progress_message(request),
            )
            if not request.skip_preflight:
                dest_ok, dest_msg = probe_destination(request.destination)
                parity = _engine_module._execute_preflight_parity_kwargs(
                    request,
                    destination_connected=dest_ok,
                    destination_table_exists_fallback=dest_table_exists_flag,
                )
                pf = run_file_preflight(
                    columns=columns,
                    column_types=schema,
                    row_count=total_rows,
                    mappings=mappings,
                    destination_connected=dest_ok,
                    destination_error=None if dest_ok else dest_msg,
                    source_kind=request.source.kind,
                    source_format=request.source.format,
                    sync_mode=request.sync_mode,
                    sample_rows=_preflight_sample_rows(sample_rows),
                    # Read-only projected re-read of the same table the write
                    # loop is about to stream, so a narrowing carrier is decided
                    # before the first batch. A just-approved Studio Validate
                    # already asked that; write-time fit still binds every row.
                    # CDC never walks here (the write loop owns the cursor).
                    population_rows=(
                        None
                        if reuse_fit or _sync_mode_is_cdc(request)
                        else _table_population_rows(
                            request,
                            mappings,
                            column_types=schema,
                            dest_types=dest_schema_types,
                            dest_db=dst_fmt.lower(),
                            shape_runner=design_runner,
                        )
                    ),
                    population_replay=lambda: _table_population_rows(
                        request,
                        mappings,
                        column_types=schema,
                        dest_types=dest_schema_types,
                        dest_db=dst_fmt.lower(),
                        shape_runner=design_runner,
                    ),
                    rows_are_population=not reuse_fit and not _sync_mode_is_cdc(request),
                    skip_population_fit=reuse_fit,
                    source_filter=request.source_filter or None,
                    confidence_threshold=confidence_threshold_for_mode(
                        request.validation_mode
                    ),
                    destination_column_types=dest_schema_types,
                    column_nullability=_source_nullability_probe(request.source),
                    destination_column_nullability=(
                        (request.destination.extra or {}).get("schema_nullability") or {}
                    ),
                    **_destination_filler_metadata(request.destination.extra),
                    destination_db_type=dst_fmt.lower(),
                    validation_mode=request.validation_mode,
                    source_table=(
                        request.source.table
                        or request.source.collection
                        or request.source_filename
                        or ""
                    ),
                    source_connector_id=request.source.connector_id or "",
                    source_config=endpoint_to_dict(request.source),
                    destination_table=(
                        request.destination.table
                        or request.destination.collection
                        or ""
                    ),
                    source_filename=request.source_filename or "",
                    source_file_id=str(
                        (getattr(request.source, "extra", None) or {}).get("file_id")
                        or getattr(request, "source_file_id", "")
                        or ""
                    ).strip(),
                    schema_policy=request.schema_policy,
                    backfill_new_fields=request.backfill_new_fields,
                    date_locale=request.date_locale,
                    number_locale=getattr(request, "number_locale", "") or "",
                    resume=resume,
                    **parity,
                )
                pf = apply_policy_gates(
                    pf,
                    _engine_module._execute_policy_gates_for_request(
                        request, source_columns=columns
                    ),
                    validation_mode=request.validation_mode,
                    destination_db_type=dst_fmt.lower(),
                )
                if not dest_ok:
                    mongo.update_job_status(
                        job_id,
                        "failed",
                        error=f"Destination unreachable: {dest_msg}",
                        phase="failed",
                        progress_pct=0,
                    )
                    line_msg = f"Destination unreachable: {dest_msg}"
                    lineage.emit_run_failed(
                        run_id=job_id,
                        job_id=job_id,
                        error=line_msg,
                        error_details={"reason": "Destination unreachable"},
                    )
                    return TransferResult(
                        success=False,
                        error=line_msg,
                        operation=request.operation,
                        job_id=job_id,
                    )
                if not pf["passed"]:
                    error_message, error_details = _fail_job_preflight(
                        mongo,
                        job_id,
                        pf,
                        lineage=lineage,
                        rows_read=total_rows,
                        sync_mode=str(getattr(request, "sync_mode", "") or ""),
                    )
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=_validation_plan_for_result(pf),
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
                        row_accounting=dict(error_details.get("row_accounting") or {}),
                    )

            # A stamped hash/artifact is always checked against the operator
            # Map contract it was taken over — never against post-enrich stamps.
            identity_maps = _operator_contract_maps(request, mappings)
            approved_hash = str(
                getattr(request, "approved_ddl_identity_hash", "") or ""
            )
            dest_db_fmt = str(getattr(request.destination, "format", None) or "")
            ddl_err = _enforce_ddl_identity(
                pf,
                identity_maps,
                dest_db=dest_db_fmt,
                approved_ddl_identity_hash=approved_hash,
                skip_preflight=bool(getattr(request, "skip_preflight", False)),
                preflight_mappings=mappings,
            )
            if ddl_err:
                mongo.update_job_status(
                    job_id, "failed", error=ddl_err, phase="failed", progress_pct=0
                )
                return TransferResult(
                    success=False,
                    error=ddl_err,
                    error_details={
                        "reason": "ddl_identity_mismatch",
                        "remediation": "Re-run Validate after Map/DDL changes.",
                    },
                    operation=request.operation,
                    job_id=job_id,
                )
            art_err, art_dict = _enforce_decision_artifact(
                pf,
                identity_maps,
                dest_db=dest_db_fmt,
                approved_decision_artifact_hash=str(
                    getattr(request, "approved_decision_artifact_hash", "") or ""
                ),
                decision_artifact=_request_decision_artifact_payload(request),
                skip_preflight=bool(getattr(request, "skip_preflight", False)),
                sync_mode=str(getattr(request, "sync_mode", "") or ""),
                error_policy="quarantine",
                destination_column_types=dest_schema_types,
                destination_table_exists=dest_table_exists_flag,
                source_column_types=schema,
                source_kind=str(getattr(request.source, "kind", "") or ""),
                source_format=str(getattr(request.source, "format", None) or src_fmt or ""),
                source_connector_id=str(getattr(request.source, "connector_id", "") or ""),
            )
            if art_err:
                mongo.update_job_status(
                    job_id, "failed", error=art_err, phase="failed", progress_pct=0
                )
                return TransferResult(
                    success=False,
                    error=art_err,
                    error_details={
                        "reason": "decision_artifact_mismatch",
                        "remediation": "Re-run Validate to stamp a Decision Artifact.",
                    },
                    operation=request.operation,
                    job_id=job_id,
                )
            if art_dict:
                try:
                    mongo.update_job_fields(
                        job_id,
                        {
                            "decision_artifact": art_dict,
                            "decision_artifact_hash": art_dict.get("content_hash"),
                        },
                    )
                except Exception:
                    pass
            if pf:
                mongo.update_job_status(
                    job_id, "running", phase="preflight", progress_pct=15, preflight=pf
                )
                from services.population_fit_scan import stamp_create_widens

                stamp_create_widens(mappings, pf.get("population_fit"))

            # Data contract / circuit breaker enforcement.
            try:
                contract_id = enforce_or_create_contract(request, schema, mappings, pf)
            except ContractViolation as cv:
                msg = cv.message
                mongo.update_job_status(
                    job_id, "failed", error=msg, phase="failed", progress_pct=0
                )
                return TransferResult(
                    success=False,
                    error=msg,
                    error_details={"violations": cv.violations},
                    operation=request.operation,
                    job_id=job_id,
                )

            # Sample-based history compare (full table never loaded in streaming path).
            load_history_report = _compare_and_publish_load_history(
                mongo,
                job_id,
                sample_rows or [],
                request,
                schema,
                validation_mode=request.validation_mode,
                row_count_hint=total_rows,
            )
            if load_history_report.get("strict_blocked"):
                anomalies = list(load_history_report.get("anomalies") or [])
                msg = "Data quality anomaly: " + "; ".join(anomalies[:8])
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=msg,
                    phase="failed",
                    progress_pct=0,
                    load_history_report=load_history_report,
                )
                return TransferResult(
                    success=False,
                    error=msg,
                    operation=request.operation,
                    job_id=job_id,
                    destination_summary={"load_history_report": load_history_report},
                    error_details={"load_history_report": load_history_report},
                )

            def _check_cancelled(committed_rows: int | None = None) -> None:
                # After the chunk commits. The count has to travel with the
                # cancel: the status was often already "cancelled" at 0 rows
                # while this table held the batch.
                _raise_if_job_cancelled(
                    mongo, job_id, rows_written=committed_rows
                )

            _quarantine_persisted = [0]

            def on_checkpoint(
                chunk: int, chunks: int, rows: int, checkpoint: dict | None = None
            ) -> None:
                _check_cancelled(rows)
                # CDC has no finite denominator — never invent a percentage.
                # cdc_incremental is the same mode. A raw equality check treated
                # that alias as a finite batch load and drew a percent mid-snapshot.
                from services.sync_cursor import normalize_sync_mode

                sync_l = normalize_sync_mode(request.sync_mode or "", default="")
                contracts = request.stream_contracts or []
                is_cdc = sync_l == "cdc" or any(
                    normalize_sync_mode(
                        str((c or {}).get("sync_mode") or ""), default=""
                    )
                    == "cdc"
                    for c in contracts
                    if isinstance(c, dict)
                )
                pct = compute_transfer_progress_pct(
                    phase="writing",
                    rows_processed=rows,
                    total_rows=0 if is_cdc else total_rows,
                    chunk=chunk,
                    chunks=chunks,
                )
                update = dict(
                    records_processed=rows,
                    chunk_current=chunk,
                    chunk_total=chunks,
                    message=batch_write_message(
                        chunk,
                        chunks,
                        rows,
                        is_cdc=is_cdc,
                        checkpoint=checkpoint,
                        stream_contracts=request.stream_contracts,
                    ),
                )
                if is_cdc:
                    update["progress_indeterminate"] = True
                if pct is not None:
                    update["progress_pct"] = pct
                if checkpoint:
                    _persist_checkpoint_quarantine_delta(
                        job_id,
                        checkpoint if isinstance(checkpoint, dict) else None,
                        request=request,
                        last_persisted=_quarantine_persisted,
                    )
                    from services.job_document_budget import (
                        slim_checkpoint_for_job_store,
                        slim_rejected_details,
                    )

                    details = list(checkpoint.get("rejected_details") or [])
                    preview, total, truncated = slim_rejected_details(details)
                    # Never embed the full writer checkpoint (unbounded quarantine)
                    # into transfer_jobs — that is the DocumentTooLarge failure mode.
                    update["checkpoint"] = slim_checkpoint_for_job_store(checkpoint)
                    update["destination_summary"] = checkpoint_quarantine_summary(
                        checkpoint, details, preview, total, truncated
                    )
                    _promote_cdc_job_fields(checkpoint, update)
                _progress_write_or_cancel(mongo, job_id, **update)

            _throttled_progress = ThrottledCheckpoint(on_checkpoint)

            def throttled_checkpoint(
                chunk: int, chunks: int, rows: int, checkpoint: dict | None = None
            ) -> None:
                # D102: check cancel on EVERY batch boundary; the throttled
                # progress write below may drop intermediate calls.
                _check_cancelled(rows)
                _throttled_progress(chunk, chunks, rows, checkpoint=checkpoint)
            backfill_fields = _run_backfill_new_fields(
                request, mappings, dest_schema_types, dest_table_exists_flag
            )

            mongo.update_job_status(
                job_id,
                "running",
                phase="writing",
                progress_pct=compute_transfer_progress_pct(
                    phase="writing", rows_processed=0, total_rows=total_rows
                )
                or 5,
                message=opening_batch_message(
                    total_rows,
                    request.stream_contracts,
                    delta=_bounded_incremental_read(request),
                ),
            )

            stream_contract = resolve_sync_contract(request.stream_contracts)
            selected_streams = resolve_selected_sync_contracts(request.stream_contracts)
            multi_non_cdc = len(selected_streams) > 1
            from services.execute_shape_route import procedure_replay_failure

            procedure_blocked = procedure_replay_failure(
                request, selected_streams, job_id=job_id
            )
            if procedure_blocked is not None:
                return procedure_blocked
            # Overwrite DROP once on primary is wrong for multi-stream — sequential
            # path drops each remapped destination instead.
            if not multi_non_cdc and should_drop_destination_for_sync(
                request_sync_mode=request.sync_mode,
                contract_sync_mode=stream_contract.sync_mode
                if stream_contract
                else None,
            ):
                if not _checkpoint_has_progress(checkpoint):
                    mongo.update_job_status(
                        job_id,
                        "running",
                        phase="writing",
                        progress_pct=compute_transfer_progress_pct(
                            phase="writing", rows_processed=0, total_rows=total_rows
                        )
                        or 5,
                        message=(
                            "Preparing destination — clearing table for full refresh…"
                        ),
                    )
                    _engine_module._drop_destination_table(
                        request.destination,
                        mappings=mappings,
                        checkpoint=checkpoint,
                        checkpoint_service=checkpoint_service,
                        mongo=mongo,
                        job_id=job_id,
                    )
                    mongo.update_job_status(
                        job_id,
                        "running",
                        phase="writing",
                        message="Connecting to destination and creating table…",
                    )

            effective_sync = resolve_effective_sync_mode(
                request.sync_mode,
                stream_contract.sync_mode if stream_contract else None,
            ).lower()
            if effective_sync in ("full_refresh_mirror", "mirror", "scd2"):
                if multi_non_cdc:
                    raise RuntimeError(
                        "Multi-stream SCD2/mirror is not supported yet — "
                        "select a single stream or use full/incremental/CDC"
                    )
                _scd2_opts_stream = None
                _scd2_keys_stream: list[str] | None = None
                if effective_sync == "scd2" and getattr(
                    request, "shape_recipe", None
                ):
                    # The recipe scd2 step IS the history contract (B1):
                    # keys/tracked are source-space, translated to target
                    # names through the mappings. A malformed contract is a
                    # refusal — silently falling back to default stamps
                    # would run history the operator did not approve.
                    from services.shape_models import ShapeRecipe

                    _rcp = (
                        shape_runner.recipe
                        if shape_runner is not None
                        else ShapeRecipe.parse(
                            request.shape_recipe,
                            source_columns=columns,
                        )
                    )
                    if _rcp.scd2_step is not None:
                        _scd2_opts_stream = _rcp.scd2_column_names() or {}
                        _src2tgt = {
                            str(m.get("source") or ""): str(
                                m.get("target") or ""
                            )
                            for m in (mappings or [])
                            if isinstance(m, dict)
                        }
                        _rkeys = [
                            str(_src2tgt.get(k) or k)
                            for k in _scd2_opts_stream.get("keys") or []
                        ]
                        if _rkeys:
                            _scd2_keys_stream = _rkeys
                        if _scd2_opts_stream.get("tracked"):
                            _scd2_opts_stream["tracked"] = [
                                str(_src2tgt.get(t) or t)
                                for t in _scd2_opts_stream["tracked"]
                            ]
                rows_written, ddl_log, dest_summary, _ = stream_scd2_mirror_transfer(
                    request.source,
                    request.destination,
                    mappings,
                    schema,
                    on_checkpoint=throttled_checkpoint,
                    sync_mode=request.sync_mode,
                    stream_contracts=request.stream_contracts,
                    job_id=job_id,
                    checkpoint=checkpoint,
                    checkpoint_service=checkpoint_service,
                    backfill_new_fields=backfill_fields,
                    validation_mode=request.validation_mode,
                    limit=request.limit,
                    scd2_opts=_scd2_opts_stream,
                    key_columns=_scd2_keys_stream,
                    shape_runner=shape_runner,
                )
                if isinstance(dest_summary, dict) and dest_summary.get("ok") is False:
                    block_msg = str(
                        dest_summary.get("error")
                        or "SCD2 map/Risk Contract blocked history merge"
                    )
                    _persist_job_quarantine(
                        job_id,
                        dest_summary,
                        request,
                        already_persisted=_quarantine_persisted,
                    )
                    mongo.update_job_status(
                        job_id,
                        "failed",
                        phase="failed",
                        error=block_msg,
                        message=block_msg,
                        records_processed=0,
                        rejected_rows=int(dest_summary.get("rejected_rows") or 0),
                        rejected_details=(
                            dest_summary.get("rejected_details") or []
                        )[:2000],
                        destination_summary=dest_summary,
                        ddl_log=list(ddl_log or [])[:500],
                    )
                    return TransferResult(
                        success=False,
                        error=block_msg,
                        job_id=job_id,
                        operation=request.operation,
                        destination_summary=dest_summary,
                        ddl_executed=list(ddl_log or []),
                    )
            elif effective_sync == "cdc":
                rows_written, ddl_log, dest_summary, _ = run_cdc_database_transfer(
                    request.source,
                    request.destination,
                    mappings,
                    schema,
                    on_checkpoint=throttled_checkpoint,
                    sync_mode=request.sync_mode,
                    stream_contracts=request.stream_contracts,
                    job_id=job_id,
                    checkpoint=checkpoint,
                    checkpoint_service=checkpoint_service,
                    backfill_new_fields=backfill_fields,
                    validation_mode=request.validation_mode,
                    limit=request.limit,
                    delivery_guarantee=getattr(request, "delivery_guarantee", None)
                    or "at_least_once",
                    delivery_pinned=bool(getattr(request, "delivery_pinned", False)),
                    workspace_id=str(getattr(request, "workspace_id", "") or ""),
                    schedule_id=str(getattr(request, "schedule_id", "") or ""),
                )
            elif multi_non_cdc:
                rows_written, ddl_log, dest_summary, _ = (
                    run_non_cdc_multi_stream_sequential(
                        request.source,
                        request.destination,
                        mappings,
                        schema,
                        on_checkpoint=throttled_checkpoint,
                        sync_mode=request.sync_mode,
                        stream_contracts=request.stream_contracts,
                        selected=selected_streams,
                        job_id=job_id,
                        checkpoint=checkpoint,
                        checkpoint_service=checkpoint_service,
                        backfill_new_fields=backfill_fields,
                        validation_mode=request.validation_mode,
                        source_filter=request.source_filter,
                        limit=request.limit,
                        skip_preflight=request.skip_preflight,
                        shape_by_stream=shape_by_stream or None,
                        approved_shape_hash=approved_shape_hash,
                        workspace_id=str(getattr(request, "workspace_id", "") or ""),
                    )
                )
            else:
                rows_written, ddl_log, dest_summary, _ = stream_database_transfer(
                    request.source,
                    request.destination,
                    mappings,
                    schema,
                    on_checkpoint=throttled_checkpoint,
                    sync_mode=request.sync_mode,
                    stream_contracts=request.stream_contracts,
                    job_id=job_id,
                    checkpoint=checkpoint,
                    checkpoint_service=checkpoint_service,
                    backfill_new_fields=backfill_fields,
                    validation_mode=request.validation_mode,
                    source_filter=request.source_filter,
                    limit=request.limit,
                    skip_preflight=request.skip_preflight,
                    shape_runner=shape_runner,
                    initial_batch=initial_batch,
                    workspace_id=str(getattr(request, "workspace_id", "") or ""),
                )

            with _reconcile_phase_heartbeat(
                mongo,
                job_id,
                processed=int(rows_written or 0),
                total=int(rows_written or 0),
                **_reconcile_heartbeat_scope(dest_summary),
            ):
                if isinstance(dest_summary, dict):
                    dest_summary.setdefault("sync_mode", effective_sync)
                    dest_summary.setdefault("job_id", job_id)
                    dest_summary.setdefault("streaming", True)
                    # The locale the run bound number text under — Gate-8's
                    # source-side fingerprint interprets source cells under
                    # the same locale (EU "1,234" → 1.234), never WIRE.
                    dest_summary.setdefault(
                        "number_locale", _run_number_locale(request)
                    )
                    # Stamped before reconciliation: the ladder and the ledger
                    # read the recipe identity and the rows it removed off this
                    # summary, so a shaped run balances instead of reporting a
                    # destination population short by the filtered rows.
                    _stamp_shape_evidence(dest_summary, shape_runner)
                recon = run_reconciliation(
                    endpoint=request.destination,
                    records=[],
                    columns=columns,
                    rows_written=rows_written,
                    writer_checksum=dest_summary.get("checksum")
                    or dest_summary.get("active_checksum", ""),
                    dest_summary=dest_summary,
                    mappings=mappings,
                    source_schema=schema,
                    validation_mode=request.validation_mode,
                    source_endpoint=request.source,
                    shape_recipe=getattr(request, "shape_recipe", None),
                )
            canonical_rejected_details = _snapshot_rejected_details(dest_summary)
            dest_summary = pii_guard.redact_destination_summary(dest_summary, mappings)
            recon = pii_guard.redact_reconciliation(recon, mappings)
            from services.file_pickup_load import apply_pickup_job_outcome

            recon, pickup_status, _pickup_message = apply_pickup_job_outcome(
                recon, dest_summary, rows_written
            )
            if not recon.get("passed"):
                fail_message = _engine_module._note_failed_batch_undo(
                    request,
                    dest_summary,
                    recon.get("message", "Reconciliation failed"),
                    recon=recon,
                )
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=fail_message,
                    phase="failed",
                    progress_pct=99,
                    message=fail_message,
                    reconciliation=recon,
                    destination_summary=dest_summary,
                    rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                    coerced_null_rows=int(
                        dest_summary.get("coerced_null_rows", 0) or 0
                    ),
                )
                return TransferResult(
                    success=False,
                    error=fail_message,
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=_failed_run_records_transferred(
                        dest_summary, rows_written
                    ),
                    destination_summary=dest_summary,
                    reconciliation=recon,
                )
            lost = fail_closed_on_silent_loss(
                job_id=job_id,
                request=request,
                dest_summary=dest_summary,
                recon=recon,
                rows_written=rows_written,
                operation=request.operation,
            )
            if lost is not None:
                return lost
            verified = True


            if effective_sync == "cdc" and isinstance(dest_summary, dict):
                from services.cdc_catchup import unread_postgres_change

                unread = unread_postgres_change(request.source, dest_summary)
                if unread:
                    mongo.update_job_status(
                        job_id,
                        "failed",
                        error=unread,
                        phase="failed",
                        progress_pct=99,
                        message=unread,
                        reconciliation=recon,
                        destination_summary=dest_summary,
                    )
                    return TransferResult(
                        success=False,
                        error=unread,
                        operation=request.operation,
                        job_id=job_id,
                        records_transferred=_failed_run_records_transferred(
                        dest_summary, rows_written
                    ),
                        destination_summary=dest_summary,
                        reconciliation=recon,
                    )

            explanation = _build_explanation(
                request,
                columns,
                schema,
                mappings,
                recon,
                dest_summary,
                pf,
                rows_written,
            )
            from services.job_status import terminal_status_for

            terminal_status = pickup_status or terminal_status_for(
                dest_summary.get("rejected_rows", 0),
                dest_summary.get("coerced_null_rows", 0),
            )
            if load_history_report:
                dest_summary["load_history_report"] = load_history_report
            _persist_load_history_profile(
                request,
                sample_rows or [],
                schema,
                job_id=job_id,
                dest_summary=dest_summary,
                row_count=rows_written or total_rows,
                mappings=mappings,
            )
            _persist_job_quarantine(
                job_id,
                dest_summary,
                request,
                already_persisted=_quarantine_persisted,
                canonical_details=canonical_rejected_details,
            )
            _attach_job_rollback_plan(job_id, dest_summary, request)
            apply_post_load_transforms(request, dest_summary)
            if effective_sync == "cdc" and isinstance(dest_summary, dict):
                from services.cdc_catchup import release_finished_cdc_slot

                try:
                    job_doc = mongo.get_job(job_id) or {}
                except Exception as exc:
                    logger.warning(
                        "CDC slot release could not read job %s: %s", job_id, exc
                    )
                    job_doc = {}
                job_doc = dict(job_doc)
                job_doc.setdefault("cdc_slot_name", dest_summary.get("cdc_slot_name"))
                job_doc.setdefault(
                    "cdc_publication_name", dest_summary.get("cdc_publication_name")
                )
                if not str(job_doc.get("cursor_key") or "").strip():
                    named = dest_summary.get("cursor_key")
                    nested_cdc = dest_summary.get("cdc")
                    if not named and isinstance(nested_cdc, dict):
                        named = nested_cdc.get("cursor_key")
                    if named:
                        job_doc["cursor_key"] = named
                try:
                    from src.transfer.adapters import resolve_connector_config

                    source_cfg = resolve_connector_config(request.source)
                except Exception as exc:
                    logger.debug("CDC completion source config unread: %s", exc)
                    source_cfg = None
                dest_cfg = None
                dest_type = ""
                try:
                    from src.transfer.adapters import resolve_connector_config

                    dest_cfg = resolve_connector_config(request.destination)
                    dest_type = str(
                        getattr(request.destination, "format", "")
                        or getattr(request.destination, "type", "")
                        or ""
                    )
                except Exception as exc:
                    logger.debug("CDC completion dest config unread: %s", exc)
                    dest_cfg = None
                extra_lsns = [
                    dest_summary.get("eos_committed_lsn"),
                    (dest_summary.get("cdc") or {}).get("eos_committed_lsn")
                    if isinstance(dest_summary.get("cdc"), dict)
                    else None,
                ]
                # Drop the slot only after the job document leaves "running".
                # Releasing first left the capture gone when this write failed
                # (DEF-B2-007).
                release_args = (
                    job_doc,
                    source_cfg,
                    dest_type,
                    dest_cfg,
                    [value for value in extra_lsns if value],
                )
            else:
                release_args = None
            _settle_overwrite_backup(request.destination, restore=False)
            # D85: Kafka is a streaming source, so this is the path that must
            # commit the group offsets; without it every run re-read the topic.
            commit_kafka_offsets_on_accept(request, dest_summary)
            status_written = _write_success_status(
                mongo,
                job_id,
                terminal_status,
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get(
                    "message", f"Transferred {rows_written:,} rows successfully"
                ),
                # The scan heartbeat paints 0–99 while rows hash; a passed
                # Gate-8 is the 100 — verified jobs must not freeze at 99 (M1).
                reconcile_pct=100,
                destination_database=dest_summary.get(
                    "database", request.destination.database or ""
                ),
                destination_collection=_job_destination_name(
                    dest_summary, request.destination
                ),
                rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                coerced_null_rows=int(dest_summary.get("coerced_null_rows", 0) or 0),
                rejected_details=(dest_summary.get("rejected_details") or [])[:2000],
                destination_summary=dest_summary,
                explanation=explanation,
                mapping_proof=_mapping_proof_for_request(request),
                reconciliation=recon,
                load_history_report=load_history_report or {},
                ddl_executed=list(ddl_log or [])[:500],
                ddl_log=list(ddl_log or [])[:500],
                sync_mode=request.sync_mode,
                schema_policy=request.schema_policy,
                validation_mode=request.validation_mode,
                **_cdc_fields_from_summary(dest_summary),
            )
            if release_args is not None and status_written:
                job_doc, source_cfg, dest_type, dest_cfg, extra_lsns = release_args
                job_doc["status"] = terminal_status
                dest_summary["cdc_slot_release"] = release_finished_cdc_slot(
                    job_doc,
                    reason="completed",
                    schedule_id=str(getattr(request, "schedule_id", "") or ""),
                    source_cfg=source_cfg,
                    job_id=job_id,
                    dest_type=dest_type,
                    dest_cfg=dest_cfg,
                    extra_lsns=extra_lsns,
                )
                mongo.update_job_status(
                    job_id,
                    terminal_status,
                    destination_summary=dest_summary,
                    **_cdc_fields_from_summary(dest_summary),
                )

            lineage.emit_preflight_completed(
                run_id=job_id,
                passed=True,
                readiness_score=pf.get("readiness_score", 100) if pf else 100,
                validation_plan=_validation_plan_for_result(pf),
            )
            lineage.emit_lineage(
                run_id=job_id,
                source_dataset=f"{request.source.kind}/{src_fmt}/{request.source.table or request.source.collection}",
                target_dataset=f"{request.destination.kind}/{dst_fmt}/{request.destination.table or request.destination.collection}",
                mappings=[
                    {
                        "source": m.get("source"),
                        "target": m.get("target"),
                        "confidence": m.get("confidence"),
                    }
                    for m in mappings
                ],
            )
            lineage.emit_run_completed(
                run_id=job_id,
                job_id=job_id,
                records_transferred=rows_written,
                source_summary={
                    "kind": request.source.kind,
                    "format": src_fmt,
                    "columns": len(columns),
                    "rows": total_rows,
                    "streaming": True,
                },
                destination_summary=dest_summary,
            )
            finalize_contract(contract_id, success=True)
            return TransferResult(
                success=True,
                job_id=job_id,
                records_transferred=rows_written,
                operation=request.operation,
                source_summary={
                    "kind": request.source.kind,
                    "format": src_fmt,
                    "columns": len(columns),
                    "rows": total_rows,
                    "streaming": True,
                },
                destination_summary=dest_summary,
                ddl_executed=ddl_log,
                columns=columns,
                reconciliation=recon,
                validation_plan=_validation_plan_for_result(pf),
                payload_shape=pf.get("payload_shape") if pf else {},
                contract_id=contract_id,
                explanation=explanation,
                mapping_proof=_mapping_proof_for_request(request),
            )
        except WriteBatchBlocked as blocked:
            _settle_overwrite_backup(request.destination, restore=True)
            dest_summary = {
                **(blocked.dest_summary or {}),
                "rejected_details": list(blocked.rejected_details),
                "rejected_rows": int(blocked.rejected_rows),
                "rows_written": int(blocked.rows_written),
                "ok": False,
                "error": str(blocked),
            }
            _persist_job_quarantine(
                job_id,
                dest_summary,
                request,
                already_persisted=_quarantine_persisted,
            )
            from src.transfer.job_failure import account_blocked_write

            kept, block_msg = account_blocked_write(mongo, job_id, blocked)
            from services.cdc_catchup import capture_identity_from

            capture = capture_identity_from(blocked)
            mongo.update_job_status(
                job_id,
                "failed",
                phase="failed",
                error=block_msg,
                message=block_msg,
                records_processed=kept,
                rejected_rows=int(dest_summary.get("rejected_rows") or 0),
                rejected_details=(
                    dest_summary.get("rejected_details") or []
                )[:2000],
                destination_summary=dest_summary,
                **capture,
            )
            if capture:
                from .job_failure import release_cdc_capture_after_failure

                release_cdc_capture_after_failure(
                    mongo, job_id, request, blocked, retriable=False
                )
            return TransferResult(
                success=False,
                error=block_msg,
                job_id=job_id,
                operation=request.operation,
                records_transferred=kept,
                destination_summary=dest_summary,
                contract_id=contract_id,
            )
        except Exception as e:
            if verified:
                # QA MX2-15: proven rows must not be failed or rolled back by a
                # bookkeeping step that raised after Gate-8 passed.
                return _finish_verified_run(
                    mongo,
                    job_id,
                    e,
                    request=request,
                    rows_written=int(rows_written or 0),
                    dest_summary=dest_summary,
                    recon=recon,
                )
            _settle_backup_after_error(request.destination, e)
            finalize_contract(contract_id, success=False)
            display, error_details = _fail_runtime_job(
                mongo, job_id, e, lineage=lineage, request=request
            )
            _undo_note = getattr(e, "failed_batch_undo_note", "")
            if _undo_note and _undo_note not in display:
                display = f"{display} {_undo_note}"
            return TransferResult(
                success=False,
                job_id=job_id,
                error=display,
                error_details=error_details,
                operation=request.operation,
                contract_id=contract_id,
            )
        finally:
            reset_strict_number_reading(strict_number_token)
            reset_active_number_locale(number_token)
            # Same pin-ledger cleanup as the file path: a ledger that died
            # inside _stream before the proof consumed it is still
            # registered and must not leave df_pin_*.tsv behind.
            _discard_unconsumed_pin_ledgers()

    def _execute_file_streaming(
        self,
        request: TransferRequest,
        job_id: str,
        mongo,
        src_fmt: str,
        resume: bool = False,
        checkpoint: Any = None,
        checkpoint_service: Any = None,
    ) -> TransferResult:
        """Batched file → database path for large CSV/TSV/JSONL uploads."""
        dst_fmt = request.destination.format or "mongodb"
        pf: dict | None = None
        contract_id = ""
        load_history_report: dict[str, Any] = {}
        verified = False
        # Bound before the stream runs so the finally block never raises
        # UnboundLocalError on runs refused before the stream returns
        # (preflight / mapping / risk refusals) — that masked the real
        # refusal reason behind a bookkeeping crash.
        dest_summary: dict[str, Any] = {}
        number_token = set_active_number_locale(_run_number_locale(request))
        from services.transform_engine import (
            reset_strict_number_reading,
            set_strict_number_reading,
        )

        strict_number_token = set_strict_number_reading(
            str(getattr(request, "validation_mode", "") or "strict").strip().lower()
            == "strict"
            and not str(getattr(request, "number_locale", "") or "").strip()
        )
        try:
            filename = request.source_filename or "upload.csv"
            content = prepare_stream_content(
                content=request.source_content or b"",
                filename=filename,
                source_path=request.source_path or "",
            )

            mongo.update_job_status(
                job_id,
                "running",
                phase="reading",
                progress_pct=compute_transfer_progress_pct(phase="reading"),
                message="Analyzing uploaded file…",
            )
            read_options = resolve_read_options(request)
            # A guessed text encoding is recorded on the job and judged by
            # preflight (warn when it holds, refuse when the bytes contradict it).
            from services.source_encoding_guess import record_file_encoding

            source_encoding = record_file_encoding(
                mongo, job_id, content, filename, read_options
            )
            columns, schema, total_rows, sample_rows = peek_file_source(
                content, filename, read_options
            )
            if total_rows == 0:
                mongo.update_job_status(
                    job_id, "failed", error="File contains no records", phase="failed"
                )
                return TransferResult(
                    success=False,
                    error="File contains no records",
                    operation=request.operation,
                    job_id=job_id,
                )

            if request.source_filter:
                sample_rows = apply_row_filter(sample_rows, request.source_filter)

            # The recipe runs before Map on the sample as well as on the stream:
            # Map, the narrowing scan and the DDL identity are all decided from
            # the columns and values shown here, so an unshaped sample would
            # approve carriers for data the writer never receives. Two runners —
            # the sample's effects are design-time and must not be added to the
            # population accounting the ledger publishes.
            _settle_locales(request, sample_rows, columns)
            shape_runner = _open_shape_runner(request, columns, schema)
            if shape_runner is not None:
                sample_probe = shape_runner.fork()
                sample_rows = sample_probe.records(sample_rows)
                columns = list(sample_probe.output_columns or columns)
                schema = shaped_schema(sample_probe, sample_rows, schema)

            mongo.update_job_status(
                job_id, "running", total_rows=total_rows, records_processed=0
            )
            dest_schema_types, dest_table_exists_flag = _engine_module._connect_destination(
                request.destination,
                sync_mode=request.sync_mode,
                mongo=mongo,
                job_id=job_id,
            )
            schema = _prefer_approved_column_types(request, schema)
            mappings = _enrich_mappings_with_types(
                _engine_module._auto_map(
                    request, columns, schema, sample_rows=sample_rows, job_id=job_id
                ),
                column_types=schema,
                dest_types=dest_schema_types,
            )
            mappings = _apply_schema_auto_propagate(
                request=request,
                columns=columns,
                schema=schema,
                mappings=mappings,
                dest_schema_types=dest_schema_types,
            )
            _record_schema_propagation(
                mongo, job_id, columns, mappings, dest_schema_types
            )
            mappings = _stamp_additive_mappings_for_write(
                request,
                mappings,
                column_types=schema,
                dest_types=dest_schema_types,
                sample_rows=sample_rows[:100] if sample_rows else None,
                dest_table_exists=dest_table_exists_flag,
            )
            dest_schema_types = _apply_sampled_dest_profile(
                request.destination, mappings, dest_schema_types
            )
            reuse_fit = reuse_approved_validate_population_fit(request)
            mongo.update_job_status(
                job_id,
                "running",
                phase="preflight",
                progress_pct=compute_transfer_progress_pct(phase="preflight"),
                message=execute_preflight_progress_message(request),
            )
            if not request.skip_preflight:
                dest_ok, dest_msg = probe_destination(request.destination)
                parity = _engine_module._execute_preflight_parity_kwargs(
                    request,
                    destination_connected=dest_ok,
                    destination_table_exists_fallback=dest_table_exists_flag,
                )
                pf = run_file_preflight(
                    columns=columns,
                    column_types=schema,
                    row_count=total_rows,
                    mappings=mappings,
                    destination_connected=dest_ok,
                    destination_error=None if dest_ok else dest_msg,
                    source_kind=request.source.kind,
                    source_format=request.source.format,
                    sync_mode=request.sync_mode,
                    sample_rows=_preflight_sample_rows(sample_rows),
                    # Second read-only pass over the same bytes the writer is
                    # about to stream: a narrowing destination carrier is decided
                    # before the first batch, not at row 431 with the load
                    # half-done. Lazy — nothing is re-read when no mapped column
                    # can exceed its destination carrier by declaration.
                    # A just-approved Studio Validate already walked this file;
                    # do not spend minutes asking the same question again.
                    population_rows=(
                        None
                        if reuse_fit and sample_rows
                        else _shaped_population_rows(
                            shape_runner,
                            _file_population_rows(content, filename, read_options),
                        )
                    ),
                    population_replay=lambda: _shaped_population_rows(
                        shape_runner,
                        _file_population_rows(content, filename, read_options),
                    ),
                    rows_are_population=not reuse_fit or not sample_rows,
                    skip_population_fit=reuse_fit,
                    source_filter=request.source_filter or None,
                    confidence_threshold=confidence_threshold_for_mode(
                        request.validation_mode
                    ),
                    destination_column_types=dest_schema_types,
                    column_nullability=_source_nullability_probe(request.source),
                    destination_column_nullability=(
                        (request.destination.extra or {}).get("schema_nullability") or {}
                    ),
                    **_destination_filler_metadata(request.destination.extra),
                    destination_db_type=dst_fmt.lower(),
                    validation_mode=request.validation_mode,
                    source_table=(
                        request.source.table
                        or request.source.collection
                        or request.source_filename
                        or ""
                    ),
                    source_connector_id=request.source.connector_id or "",
                    source_config=endpoint_to_dict(request.source),
                    destination_table=(
                        request.destination.table
                        or request.destination.collection
                        or ""
                    ),
                    source_filename=request.source_filename or "",
                    source_file_id=str(
                        (getattr(request.source, "extra", None) or {}).get("file_id")
                        or getattr(request, "source_file_id", "")
                        or ""
                    ).strip(),
                    schema_policy=request.schema_policy,
                    backfill_new_fields=request.backfill_new_fields,
                    date_locale=request.date_locale,
                    number_locale=getattr(request, "number_locale", "") or "",
                    resume=resume,
                    source_encoding=source_encoding,
                    **parity,
                )
                pf = apply_policy_gates(
                    pf,
                    _engine_module._execute_policy_gates_for_request(
                        request, source_columns=columns
                    ),
                    validation_mode=request.validation_mode,
                    destination_db_type=dst_fmt.lower(),
                )
                if not dest_ok:
                    mongo.update_job_status(
                        job_id,
                        "failed",
                        error=f"Destination unreachable: {dest_msg}",
                        phase="failed",
                        progress_pct=0,
                    )
                    line_msg = f"Destination unreachable: {dest_msg}"
                    lineage.emit_run_failed(
                        run_id=job_id,
                        job_id=job_id,
                        error=line_msg,
                        error_details={"reason": "Destination unreachable"},
                    )
                    return TransferResult(
                        success=False,
                        error=line_msg,
                        operation=request.operation,
                        job_id=job_id,
                    )
                if not pf["passed"]:
                    error_message, error_details = _fail_job_preflight(
                        mongo,
                        job_id,
                        pf,
                        lineage=lineage,
                        rows_read=total_rows,
                        sync_mode=str(getattr(request, "sync_mode", "") or ""),
                    )
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=_validation_plan_for_result(pf),
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
                        row_accounting=dict(error_details.get("row_accounting") or {}),
                    )

            # A stamped hash/artifact is always checked against the operator
            # Map contract it was taken over — never against post-enrich stamps.
            identity_maps = _operator_contract_maps(request, mappings)
            approved_hash = str(
                getattr(request, "approved_ddl_identity_hash", "") or ""
            )
            dest_db_fmt = str(getattr(request.destination, "format", None) or "")
            ddl_err = _enforce_ddl_identity(
                pf,
                identity_maps,
                dest_db=dest_db_fmt,
                approved_ddl_identity_hash=approved_hash,
                skip_preflight=bool(getattr(request, "skip_preflight", False)),
                preflight_mappings=mappings,
            )
            if ddl_err:
                mongo.update_job_status(
                    job_id, "failed", error=ddl_err, phase="failed", progress_pct=0
                )
                return TransferResult(
                    success=False,
                    error=ddl_err,
                    error_details={
                        "reason": "ddl_identity_mismatch",
                        "remediation": "Re-run Validate after Map/DDL changes.",
                    },
                    operation=request.operation,
                    job_id=job_id,
                )
            art_err, art_dict = _enforce_decision_artifact(
                pf,
                identity_maps,
                dest_db=dest_db_fmt,
                approved_decision_artifact_hash=str(
                    getattr(request, "approved_decision_artifact_hash", "") or ""
                ),
                decision_artifact=_request_decision_artifact_payload(request),
                skip_preflight=bool(getattr(request, "skip_preflight", False)),
                sync_mode=str(getattr(request, "sync_mode", "") or ""),
                error_policy="quarantine",
                destination_column_types=dest_schema_types,
                destination_table_exists=dest_table_exists_flag,
                source_column_types=schema,
                source_kind=str(getattr(request.source, "kind", "") or ""),
                source_format=str(getattr(request.source, "format", None) or src_fmt or ""),
                source_connector_id=str(getattr(request.source, "connector_id", "") or ""),
            )
            if art_err:
                mongo.update_job_status(
                    job_id, "failed", error=art_err, phase="failed", progress_pct=0
                )
                return TransferResult(
                    success=False,
                    error=art_err,
                    error_details={
                        "reason": "decision_artifact_mismatch",
                        "remediation": "Re-run Validate to stamp a Decision Artifact.",
                    },
                    operation=request.operation,
                    job_id=job_id,
                )
            if art_dict:
                try:
                    mongo.update_job_fields(
                        job_id,
                        {
                            "decision_artifact": art_dict,
                            "decision_artifact_hash": art_dict.get("content_hash"),
                        },
                    )
                except Exception:
                    pass
            if pf:
                mongo.update_job_status(
                    job_id, "running", phase="preflight", progress_pct=15, preflight=pf
                )
                from services.population_fit_scan import stamp_create_widens

                stamp_create_widens(mappings, pf.get("population_fit"))

            # Data contract / circuit breaker enforcement.
            try:
                contract_id = enforce_or_create_contract(request, schema, mappings, pf)
            except ContractViolation as cv:
                msg = cv.message
                mongo.update_job_status(
                    job_id, "failed", error=msg, phase="failed", progress_pct=0
                )
                return TransferResult(
                    success=False,
                    error=msg,
                    error_details={"violations": cv.violations},
                    operation=request.operation,
                    job_id=job_id,
                )

            # Sample-based history compare (file is streamed; full table never loaded).
            load_history_report = _compare_and_publish_load_history(
                mongo,
                job_id,
                sample_rows or [],
                request,
                schema,
                validation_mode=request.validation_mode,
                row_count_hint=total_rows,
            )
            if load_history_report.get("strict_blocked"):
                anomalies = list(load_history_report.get("anomalies") or [])
                msg = "Data quality anomaly: " + "; ".join(anomalies[:8])
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=msg,
                    phase="failed",
                    progress_pct=0,
                    load_history_report=load_history_report,
                )
                return TransferResult(
                    success=False,
                    error=msg,
                    operation=request.operation,
                    job_id=job_id,
                    destination_summary={"load_history_report": load_history_report},
                    error_details={"load_history_report": load_history_report},
                )

            def _check_cancelled(committed_rows: int | None = None) -> None:
                # After the chunk commits. The count has to travel with the
                # cancel: the status was often already "cancelled" at 0 rows
                # while this table held the batch.
                _raise_if_job_cancelled(
                    mongo, job_id, rows_written=committed_rows
                )

            _quarantine_persisted = [0]

            def on_checkpoint(
                chunk: int, chunks: int, rows: int, checkpoint: dict | None = None
            ) -> None:
                _check_cancelled(rows)
                pct = compute_transfer_progress_pct(
                    phase="writing",
                    rows_processed=rows,
                    total_rows=total_rows,
                    chunk=chunk,
                    chunks=chunks,
                )
                update = dict(
                    records_processed=rows,
                    chunk_current=chunk,
                    chunk_total=chunks,
                    message=batch_write_message(
                        chunk,
                        chunks,
                        rows,
                        checkpoint=checkpoint,
                        stream_contracts=request.stream_contracts,
                    ),
                )
                if pct is not None:
                    update["progress_pct"] = pct
                if checkpoint:
                    _persist_checkpoint_quarantine_delta(
                        job_id,
                        checkpoint if isinstance(checkpoint, dict) else None,
                        request=request,
                        last_persisted=_quarantine_persisted,
                    )
                    from services.job_document_budget import (
                        slim_checkpoint_for_job_store,
                        slim_rejected_details,
                    )

                    details = list(checkpoint.get("rejected_details") or [])
                    preview, total, truncated = slim_rejected_details(details)
                    # Never embed the full writer checkpoint (unbounded quarantine)
                    # into transfer_jobs — that is the DocumentTooLarge failure mode.
                    update["checkpoint"] = slim_checkpoint_for_job_store(checkpoint)
                    update["destination_summary"] = checkpoint_quarantine_summary(
                        checkpoint, details, preview, total, truncated
                    )
                    _promote_cdc_job_fields(checkpoint, update)
                _progress_write_or_cancel(mongo, job_id, **update)

            _throttled_progress = ThrottledCheckpoint(on_checkpoint)

            def throttled_checkpoint(
                chunk: int, chunks: int, rows: int, checkpoint: dict | None = None
            ) -> None:
                # D102: check cancel on EVERY batch boundary; the throttled
                # progress write below may drop intermediate calls.
                _check_cancelled(rows)
                _throttled_progress(chunk, chunks, rows, checkpoint=checkpoint)
            backfill_fields = _run_backfill_new_fields(
                request, mappings, dest_schema_types, dest_table_exists_flag
            )

            mongo.update_job_status(
                job_id,
                "running",
                phase="writing",
                progress_pct=compute_transfer_progress_pct(
                    phase="writing", rows_processed=0, total_rows=total_rows
                )
                or 5,
                message=opening_batch_message(
                    total_rows,
                    request.stream_contracts,
                    delta=_bounded_incremental_read(request),
                ),
            )

            stream_contract = resolve_sync_contract(request.stream_contracts)
            effective_sync = resolve_effective_sync_mode(
                request.sync_mode,
                stream_contract.sync_mode if stream_contract else None,
            )
            if should_drop_destination_for_sync(
                request_sync_mode=request.sync_mode,
                contract_sync_mode=stream_contract.sync_mode
                if stream_contract
                else None,
            ):
                if not _checkpoint_has_progress(checkpoint):
                    mongo.update_job_status(
                        job_id,
                        "running",
                        phase="writing",
                        progress_pct=compute_transfer_progress_pct(
                            phase="writing", rows_processed=0, total_rows=total_rows
                        )
                        or 5,
                        message=(
                            "Preparing destination — clearing table for full refresh…"
                        ),
                    )
                    _engine_module._drop_destination_table(
                        request.destination,
                        mappings=mappings,
                        checkpoint=checkpoint,
                        checkpoint_service=checkpoint_service,
                        mongo=mongo,
                        job_id=job_id,
                    )
                    mongo.update_job_status(
                        job_id,
                        "running",
                        phase="writing",
                        message="Connecting to destination and creating table…",
                    )

            rows_written, ddl_log, dest_summary, _ = stream_file_to_database(
                content,
                filename,
                request.destination,
                mappings,
                schema,
                on_checkpoint=throttled_checkpoint,
                sync_mode=request.sync_mode,
                stream_contracts=request.stream_contracts,
                job_id=job_id,
                checkpoint=checkpoint,
                checkpoint_service=checkpoint_service,
                backfill_new_fields=backfill_fields,
                validation_mode=request.validation_mode,
                source_filter=request.source_filter,
                skip_preflight=request.skip_preflight,
                date_locale=request.date_locale,
                number_locale=_run_number_locale(request),
                read_options=read_options,
                shape_runner=shape_runner,
                duplicate_policy=str(getattr(request, "duplicate_policy", "") or ""),
            )

            with _reconcile_phase_heartbeat(
                mongo,
                job_id,
                processed=int(rows_written or 0),
                total=int(rows_written or 0),
                **_reconcile_heartbeat_scope(dest_summary),
            ):
                if isinstance(dest_summary, dict):
                    dest_summary.setdefault("sync_mode", effective_sync)
                    dest_summary.setdefault("job_id", job_id)
                    dest_summary.setdefault("streaming", True)
                    # File-stream upsert needs PK stamps for keyed Gate-8.
                    file_pk: list[str] = []
                    if stream_contract and stream_contract.primary_key:
                        file_pk = [
                            map_source_to_target(col, mappings)
                            for col in stream_contract.primary_key_columns()
                        ]
                    if file_pk:
                        dest_summary.setdefault("conflict_columns", list(file_pk))
                        dest_summary.setdefault("primary_key_columns", list(file_pk))
                    # Before reconciliation, not after: the cell ladder decides
                    # whether a source re-read is still comparable from this stamp.
                    _stamp_shape_evidence(dest_summary, shape_runner)
                recon = run_reconciliation(
                    endpoint=request.destination,
                    records=[],
                    columns=columns,
                    rows_written=rows_written,
                    writer_checksum=dest_summary.get("checksum")
                    or dest_summary.get("active_checksum", ""),
                    dest_summary=dest_summary,
                    mappings=mappings,
                    source_schema=schema,
                    validation_mode=request.validation_mode,
                    source_endpoint=request.source,
                    shape_recipe=getattr(request, "shape_recipe", None),
                )
            canonical_rejected_details = _snapshot_rejected_details(dest_summary)
            dest_summary = pii_guard.redact_destination_summary(dest_summary, mappings)
            recon = pii_guard.redact_reconciliation(recon, mappings)
            from services.file_pickup_load import apply_pickup_job_outcome

            recon, pickup_status, _pickup_message = apply_pickup_job_outcome(
                recon, dest_summary, rows_written
            )
            if not recon.get("passed"):
                fail_message = _engine_module._note_failed_batch_undo(
                    request,
                    dest_summary,
                    recon.get("message", "Reconciliation failed"),
                    recon=recon,
                )
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=fail_message,
                    phase="failed",
                    progress_pct=99,
                    message=fail_message,
                    reconciliation=recon,
                    destination_summary=dest_summary,
                    rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                    coerced_null_rows=int(
                        dest_summary.get("coerced_null_rows", 0) or 0
                    ),
                )
                return TransferResult(
                    success=False,
                    error=fail_message,
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=_failed_run_records_transferred(
                        dest_summary, rows_written
                    ),
                    destination_summary=dest_summary,
                    reconciliation=recon,
                )
            lost = fail_closed_on_silent_loss(
                job_id=job_id,
                request=request,
                dest_summary=dest_summary,
                recon=recon,
                rows_written=rows_written,
                operation=request.operation,
            )
            if lost is not None:
                return lost
            verified = True
            if isinstance(dest_summary, dict) and dest_summary.get("file_digest"):
                from services.file_load_ledger import record_successful_file_load

                record_successful_file_load(dest_summary)

            explanation = _build_explanation(
                request,
                columns,
                schema,
                mappings,
                recon,
                dest_summary,
                pf,
                rows_written,
            )
            from services.job_status import terminal_status_for

            terminal_status = pickup_status or terminal_status_for(
                dest_summary.get("rejected_rows", 0),
                dest_summary.get("coerced_null_rows", 0),
            )
            if load_history_report:
                dest_summary["load_history_report"] = load_history_report
            _persist_load_history_profile(
                request,
                sample_rows or [],
                schema,
                job_id=job_id,
                dest_summary=dest_summary,
                row_count=rows_written or total_rows,
                mappings=mappings,
            )
            _persist_job_quarantine(
                job_id,
                dest_summary,
                request,
                already_persisted=_quarantine_persisted,
                canonical_details=canonical_rejected_details,
            )
            _attach_job_rollback_plan(job_id, dest_summary, request)
            apply_post_load_transforms(request, dest_summary)
            _settle_overwrite_backup(request.destination, restore=False)
            commit_kafka_offsets_on_accept(request, dest_summary)
            _write_success_status(
                mongo,
                job_id,
                terminal_status,
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get(
                    "message", f"Transferred {rows_written:,} rows successfully"
                ),
                # The scan heartbeat paints 0–99 while rows hash; a passed
                # Gate-8 is the 100 — verified jobs must not freeze at 99 (M1).
                reconcile_pct=100,
                destination_database=dest_summary.get(
                    "database", request.destination.database or ""
                ),
                destination_collection=_job_destination_name(
                    dest_summary, request.destination
                ),
                rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                coerced_null_rows=int(dest_summary.get("coerced_null_rows", 0) or 0),
                rejected_details=(dest_summary.get("rejected_details") or [])[:2000],
                destination_summary=dest_summary,
                explanation=explanation,
                mapping_proof=_mapping_proof_for_request(request),
                reconciliation=recon,
                load_history_report=load_history_report or {},
                ddl_executed=list(ddl_log or [])[:500],
                ddl_log=list(ddl_log or [])[:500],
                sync_mode=request.sync_mode,
                schema_policy=request.schema_policy,
                validation_mode=request.validation_mode,
            )

            lineage.emit_preflight_completed(
                run_id=job_id,
                passed=True,
                readiness_score=pf.get("readiness_score", 100) if pf else 100,
                validation_plan=_validation_plan_for_result(pf),
            )
            lineage.emit_lineage(
                run_id=job_id,
                source_dataset=f"{request.source.kind}/{src_fmt}/{request.source_filename}",
                target_dataset=f"{request.destination.kind}/{dst_fmt}/{request.destination.table or request.destination.collection}",
                mappings=[
                    {
                        "source": m.get("source"),
                        "target": m.get("target"),
                        "confidence": m.get("confidence"),
                    }
                    for m in mappings
                ],
            )
            lineage.emit_run_completed(
                run_id=job_id,
                job_id=job_id,
                records_transferred=rows_written,
                source_summary={
                    "kind": "file",
                    "format": src_fmt,
                    "columns": len(columns),
                    "rows": total_rows,
                    "streaming": True,
                },
                destination_summary=dest_summary,
            )
            finalize_contract(contract_id, success=True)
            return TransferResult(
                success=True,
                job_id=job_id,
                records_transferred=rows_written,
                operation=request.operation,
                source_summary={
                    "kind": "file",
                    "format": src_fmt,
                    "columns": len(columns),
                    "rows": total_rows,
                    "streaming": True,
                },
                destination_summary=dest_summary,
                ddl_executed=ddl_log,
                columns=columns,
                reconciliation=recon,
                validation_plan=_validation_plan_for_result(pf),
                payload_shape=pf.get("payload_shape") if pf else {},
                contract_id=contract_id,
                explanation=explanation,
                mapping_proof=_mapping_proof_for_request(request),
            )
        except WriteBatchBlocked as blocked:
            _settle_overwrite_backup(request.destination, restore=True)
            dest_summary = {
                **(blocked.dest_summary or {}),
                "rejected_details": list(blocked.rejected_details),
                "rejected_rows": int(blocked.rejected_rows),
                "rows_written": int(blocked.rows_written),
                "ok": False,
                "error": str(blocked),
            }
            _persist_job_quarantine(
                job_id,
                dest_summary,
                request,
                already_persisted=_quarantine_persisted,
            )
            from src.transfer.job_failure import account_blocked_write

            kept, block_msg = account_blocked_write(mongo, job_id, blocked)
            mongo.update_job_status(
                job_id,
                "failed",
                phase="failed",
                error=block_msg,
                message=block_msg,
                records_processed=kept,
                rejected_rows=int(dest_summary.get("rejected_rows") or 0),
                rejected_details=(
                    dest_summary.get("rejected_details") or []
                )[:2000],
                destination_summary=dest_summary,
            )
            return TransferResult(
                success=False,
                error=block_msg,
                job_id=job_id,
                operation=request.operation,
                records_transferred=kept,
                destination_summary=dest_summary,
                contract_id=contract_id,
            )
        except Exception as e:
            if verified:
                # QA MX2-15: proven rows must not be failed or rolled back by a
                # bookkeeping step that raised after Gate-8 passed.
                return _finish_verified_run(
                    mongo,
                    job_id,
                    e,
                    request=request,
                    rows_written=int(rows_written or 0),
                    dest_summary=dest_summary,
                    recon=recon,
                )
            _settle_backup_after_error(request.destination, e)
            finalize_contract(contract_id, success=False)
            display, error_details = _fail_runtime_job(
                mongo, job_id, e, lineage=lineage, request=request
            )
            _undo_note = getattr(e, "failed_batch_undo_note", "")
            if _undo_note and _undo_note not in display:
                display = f"{display} {_undo_note}"
            return TransferResult(
                success=False,
                job_id=job_id,
                error=display,
                error_details=error_details,
                operation=request.operation,
                contract_id=contract_id,
            )
        finally:
            reset_strict_number_reading(strict_number_token)
            reset_active_number_locale(number_token)
            # The write-time pin ledger's spill file outlives _stream and is
            # normally consumed (and unlinked) by the transform proof; a run
            # that fails before reconcile — e.g. the dedupe failure path —
            # would leak it otherwise. Unconsumed ledgers discard
            # deterministically via the registry; dest_summary is bound at
            # the top of this function so a refusal cannot mask its reason.
            _discard_unconsumed_pin_ledgers()
            _ledger = (dest_summary or {}).get("shape_raw_ledger")
            _pin_file = (
                _ledger.get("file") if isinstance(_ledger, dict) else None
            )
            if _pin_file:
                try:
                    os.unlink(str(_pin_file))
                except OSError:
                    pass

    def _create_pending_job(self, request: TransferRequest) -> str:
        return job_submission.create_pending_job(self, request)

    def _idempotency_key(self, request: TransferRequest) -> str:
        """Full claim key for a request, or '' when the guard is disabled."""
        if getenv_brand("JOB_IDEMPOTENCY", "1").strip().lower() in {
            "0",
            "false",
            "off",
        }:
            return ""
        from services.job_idempotency import (
            claim_key,
            normalize_client_key,
            request_fingerprint,
        )

        explicit = normalize_client_key(getattr(request, "idempotency_key", ""))
        return claim_key(
            workspace_id=request.workspace_id or "",
            key=explicit or request_fingerprint(request),
        )

    def _destination_claim_key(self, request: TransferRequest) -> str:
        """Claim key for the destination object, or '' when not claimable."""
        if not self._idempotency_key(request):
            return ""
        from services.job_idempotency import claim_key, destination_fingerprint

        dest = destination_fingerprint(request)
        if not dest:
            return ""
        return claim_key(workspace_id=request.workspace_id or "", key=dest)

    def _claim_idempotency(
        self, request: TransferRequest, job_doc: dict[str, Any]
    ) -> Any | None:
        """Reserve this transfer's slot. Returns the claim, or None if unavailable."""
        return self._claim_slot(
            self._idempotency_key(request), job_doc, field="idempotency_key"
        )

    def _claim_destination(
        self, request: TransferRequest, job_doc: dict[str, Any]
    ) -> Any | None:
        """Reserve the destination object for this job (D36: one writer at a time)."""
        return self._claim_slot(
            self._destination_claim_key(request), job_doc, field="destination_claim_key"
        )

    def _claim_slot(
        self, key: str, job_doc: dict[str, Any], *, field: str
    ) -> Any | None:
        from services.job_idempotency import JobClaim

        if not key:
            return None
        mongo = get_mongodb_service()
        claimer = getattr(mongo, "claim_job_idempotency", None)
        if not callable(claimer):
            return None
        try:
            acquired, existing_id, existing_status = claimer(
                key=key, job_id=_PENDING_CLAIM_ID
            )
        except Exception as exc:
            # The guard is a safety net, not a gate. If it cannot be consulted,
            # run the transfer rather than refusing all work.
            logger.warning("idempotency claim unavailable: %s", exc)
            return None
        job_doc[field] = key
        created_at = ""
        if not acquired and existing_id and existing_id != _PENDING_CLAIM_ID:
            try:
                from services.job_status import job_timestamp_iso

                held = mongo.get_job(existing_id) or {}
                created_at = job_timestamp_iso(held.get("created_at"))
            except Exception as exc:
                logger.debug("idempotency holder timestamp skipped: %s", exc)
        return JobClaim(
            key=key,
            job_id="",
            acquired=acquired,
            existing_job_id=existing_id,
            existing_status=existing_status,
            existing_created_at=created_at,
        )

    def _bind_idempotency_claim(self, key: str, job_id: str) -> None:
        """Replace the placeholder holder id with the real job id.

        Done as an in-place update, never as release-then-reclaim. The latter
        opens a window where a concurrent submit can take the freed key and
        start a second writer before this run reclaims it.
        """
        if not key or not job_id:
            return
        mongo = get_mongodb_service()
        try:
            binder = getattr(mongo, "bind_job_idempotency", None)
            if callable(binder):
                binder(key, _PENDING_CLAIM_ID, job_id)
        except Exception as exc:
            logger.debug("idempotency claim bind skipped: %s", exc)

    def _release_idempotency(self, job_id: str) -> None:
        """Free the claim so the same transfer can be run again later.

        Called on every terminal path. A claim that outlived its job would block
        legitimate re-runs until the TTL expired.
        """
        if not job_id:
            return
        mongo = get_mongodb_service()
        try:
            job = mongo.get_job(job_id) or {}
            release = getattr(mongo, "release_job_idempotency", None)
            if not callable(release):
                return
            for field in ("idempotency_key", "destination_claim_key"):
                key = str(job.get(field) or "")
                if key:
                    release(key, job_id)
        except Exception as exc:
            logger.debug("idempotency release skipped for %s: %s", job_id, exc)

    def _read_source(
        self,
        request: TransferRequest,
        source_stamp: dict[str, Any] | None = None,
    ) -> tuple[list, list[str], dict[str, str]]:
        if request.source.kind == "file":
            from services.transfer_file_staging import hydrate_file_source

            hydrate_file_source(request)
            content = request.source_content or b""
            if not content and request.source_path:
                from services.dataset_file import confine_source_path

                p = confine_source_path(request.source_path)
                if p.is_file():
                    content = p.read_bytes()
            if not content:
                raise ValueError("File content required for file source")
            enable_ocr = bool((request.source.extra or {}).get("enable_ocr"))
            return parse_file_content(
                content,
                request.source_filename or "upload.csv",
                enable_ocr=enable_ocr,
                read_options=resolve_read_options(request),
            )
        if request.source.kind == "database":
            cursor_column = ""
            cursor_after = None
            try:
                from services.batch_incremental import bind_transfer_request

                bound = bind_transfer_request(request, self._resolved_format(request.source))
                # First incremental run has a cursor column but no watermark —
                # that is a full extract. Only seek when a dest-owned mark exists.
                if bound.active and bound.scope.watermark:
                    cursor_column = bound.scope.cursor_column
                    cursor_after = bound.scope.read_watermark
            except ValueError:
                # Cursor-identity refusal is owned by execute_tracked after the
                # read; do not turn it into a retried source error here.
                cursor_column = ""
                cursor_after = None
            return read_source_database(
                request.source,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                stamp_total=source_stamp,
            )
        raise ValueError(f"Unsupported source kind: {request.source.kind}")

    def analyze_compatibility(
        self,
        source: EndpointConfig,
        destination: EndpointConfig,
        sample_content: bytes | None = None,
        filename: str = "",
        source_columns: list[str] | None = None,
        source_schema: dict[str, str] | None = None,
    ) -> dict:
        """Understand source + destination and recommend auto-creation plan."""
        from services.universal_router import analyze_route

        from .endpoint_intelligence import build_transfer_plan, introspect_endpoint

        source.extra = {
            **(source.extra or {}),
            "introspect_purpose": "source",
            "introspect_strict": True,
        }
        source_info = introspect_endpoint(source, sample_content, filename)
        if source_columns and not source_info.get("columns"):
            source_info["columns"] = source_columns
            source_info["schema"] = source_schema or {}
            source_info["connected"] = True
            source_info["message"] = f"Schema ready — {len(source_columns)} columns"
        elif source_columns:
            source_info["columns"] = source_columns
            if source_schema:
                source_info["schema"] = source_schema
        plan = build_transfer_plan(source, destination, source_info)
        if source_info.get("columns"):
            plan["source_columns"] = source_info["columns"]
            plan["source_schema"] = source_info["schema"]
        src_fmt = self._resolved_format(source)
        dst_fmt = self._resolved_format(destination)
        plan["route_analysis"] = analyze_route(
            source.kind, src_fmt, destination.kind, dst_fmt
        )
        return plan

    def _resolved_format(self, endpoint: EndpointConfig) -> str:
        """Return the canonical driver format, preferring saved connector type."""
        if endpoint.connector_id:
            try:
                cfg = resolve_connector_config(endpoint)
                return (cfg.get("type") or endpoint.format or "").lower()
            except Exception as exc:
                logger.debug("resolved connector config failed: %s", exc, exc_info=exc)
        if endpoint.kind == "file":
            return (endpoint.format or "csv").lower()
        if endpoint.kind == "file_export":
            return (endpoint.format or "json").lower()
        return (endpoint.format or "").lower()


_engine: Optional[UniversalTransferEngine] = None


def get_transfer_engine() -> UniversalTransferEngine:
    global _engine
    if _engine is None:
        _engine = UniversalTransferEngine()
    return _engine

_EXTRACTED_ENGINE_EXPORTS = {
    "_catalog_pk_for_policy": "engine_preflight",
    "_execute_policy_gates_for_request": "engine_preflight",
    "_execute_preflight_parity_kwargs": "engine_preflight",
    "_destination_schema_probe": "engine_destination",
    "_connect_destination": "engine_destination",
    "_drop_destination_table": "engine_destination",
    "_note_failed_batch_undo": "engine_destination",
    "_pin_overwrite_rows_before": "engine_destination",
    "_remember_preserved_columns": "engine_destination",
    "_auto_map": "engine_automap",
    "_record_schema_and_lineage": "engine_automap",
}


def __getattr__(name: str):
    module_name = _EXTRACTED_ENGINE_EXPORTS.get(name)
    if module_name is not None:
        from importlib import import_module

        module = import_module(f"src.transfer.{module_name}")
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


from src.transfer.engine_history import (  # noqa: E402,F401 - re-export
    _shape_recipe_removes_rows,
    _compare_and_publish_load_history,
)


from src.transfer.engine_finish import (  # noqa: E402,F401 - re-export
    _checkpoint_has_progress,
    _raise_if_job_cancelled,
    _progress_write_or_cancel,
    _write_success_status,
    _finish_verified_run,
    _settle_backup_after_error,
    _settle_overwrite_backup,
)


from src.transfer.engine_schema_propagation import (  # noqa: E402,F401 - re-export
    _destination_schema_types,
    _record_schema_propagation,
    _forget_cached_table_shapes,
    _run_backfill_new_fields,
    _apply_schema_auto_propagate,
    _infer_primary_key,
    _declared_destination_key,
)


from src.transfer.engine_population import (  # noqa: E402,F401 - re-export
    _widen_design_sample,
    _rekey_to_read_columns,
    _source_nullability_probe,
    _apply_sampled_dest_profile,
    _destination_filler_metadata,
    _preflight_sample_rows,
    _file_population_rows,
    _table_population_rows,
)
