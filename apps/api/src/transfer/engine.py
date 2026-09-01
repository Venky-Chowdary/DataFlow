"""Universal transfer orchestrator — routes any source to any destination."""

from __future__ import annotations

import logging
import os
from services.brand_env import getenv_brand
import sys
import threading
import time
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

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

try:
    from services import lineage_telemetry as lineage
    from services.error_handling import (
        FullRefreshDropFailed,
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
    from services.value_serializer import cell_to_string
    from services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        probe_destination,
        run_file_preflight,
        run_transfer_policy_gates,
    )
    from services.row_filter import apply_row_filter
    from services.scd2_engine import apply_scd2
    from services.shape_apply import (
        ShapeError,
        ShapeRowError,
        ShapeRunner,
        shaped_schema,
    )
    from services.sync_cursor import (
        destination_exists_for_shape,
        is_overwrite_sync,
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
        FullRefreshDropFailed,
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
    from src.services.value_serializer import cell_to_string
    from src.services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        probe_destination,
        run_file_preflight,
        run_transfer_policy_gates,
    )
    from src.services.row_filter import apply_row_filter
    from src.services.scd2_engine import apply_scd2
    from src.services.shape_apply import (
        ShapeError,
        ShapeRowError,
        ShapeRunner,
        shaped_schema,
    )
    from src.services.sync_cursor import (
        destination_exists_for_shape,
        is_overwrite_sync,
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
    peek_stream_source,
    run_non_cdc_multi_stream_sequential,
    stream_database_transfer,
    stream_scd2_mirror_transfer,
    supports_streaming,
)
from .type_mapper import default_mappings

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

from services.batch_progress import (
    ThrottledCheckpoint,
    compute_transfer_progress_pct,
    effective_backfill_new_fields,
    row_count_label,
)
from services.read_options import ReadOptions, ReadOptionsError

try:
    from services import schema_registry
except (
    ImportError
):  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services import schema_registry
from services.checkpoint_service import (
    Checkpoint,
    CheckpointService,
)
from src.transfer.resume_state import resolve_resume_checkpoint

logger = logging.getLogger("dataflow.transfer")

from src.transfer.reconcile_heartbeat import (  # noqa: E402
    reconcile_phase_heartbeat as _reconcile_phase_heartbeat,
)


def _compare_and_publish_load_history(
    mongo: Any,
    job_id: str,
    rows: list[dict],
    request: TransferRequest,
    schema: dict[str, str] | None,
    *,
    validation_mode: str,
    row_count_hint: int | None = None,
) -> dict[str, Any]:
    """Compare current sample/batch to last-N route history; publish on the job.

    Streaming paths pass a bounded ``rows`` sample plus ``row_count_hint`` for
    volume drift. Never raises — history must not abort writes.
    """
    load_history_report: dict[str, Any] = {}
    try:
        from services.data_quality_history import compare_route_to_history

        route = transfer_request_to_dict(request)
        load_history_report = compare_route_to_history(
            rows,
            route["source"],
            route["destination"],
            schema=schema,
            current_row_count=row_count_hint,
        )
        quality_anomalies = list(load_history_report.get("anomalies") or [])
        if quality_anomalies:
            mongo.update_job_status(
                job_id,
                "running",
                phase="quality_check",
                progress_pct=20,
                message="; ".join(quality_anomalies[:5]),
                load_history_report=load_history_report,
            )
            if validation_mode == "strict":
                load_history_report = {
                    **load_history_report,
                    "passed": False,
                    "strict_blocked": True,
                }
        else:
            mongo.update_job_status(
                job_id,
                "running",
                load_history_report=load_history_report,
            )
    except Exception as hist_exc:
        load_history_report = {
            "passed": True,
            "anomalies": [],
            "warning": f"Load-history compare unavailable: {hist_exc!s}"[:240],
            "prior_load_count": 0,
        }
        try:
            mongo.update_job_status(
                job_id,
                "running",
                load_history_report=load_history_report,
                message=load_history_report["warning"],
            )
        except Exception as exc:
            logger.warning("load-history status update failed: %s", exc, exc_info=exc)
    return load_history_report


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


def _validation_plan_for_result(pf: dict | None) -> dict:
    """Checklist plus live gate outcomes so operators see float→decimal etc. warnings."""
    if not pf:
        return {}
    plan = dict(pf.get("validation_plan") or {})
    if pf.get("gates") is not None:
        plan["gates"] = pf.get("gates") or []
    if "passed" in pf:
        plan["passed"] = pf.get("passed")
    if pf.get("warnings") is not None:
        plan["warnings"] = pf.get("warnings") or []
    if pf.get("blockers") is not None:
        plan["blockers"] = pf.get("blockers") or []
    if pf.get("readiness_score") is not None:
        plan["readiness_score"] = pf.get("readiness_score")
    return plan




def _fail_job_preflight(mongo, job_id: str, pf: dict, *, lineage) -> tuple[str, dict]:
    """Mark job failed at preflight and persist inspectable quarantine rows."""
    from services.quarantine_from_preflight import quarantine_rows_from_preflight

    decision = (pf.get("proof_bundle") or {}).get("transfer_decision", {}) or {}
    blocker_reasons = [
        b.get("message") for b in pf.get("blockers", []) if isinstance(b, dict)
    ]
    qrows = quarantine_rows_from_preflight(pf)
    row_ids = {d.get("row") for d in qrows if d.get("row") is not None}
    rejected_rows = len(row_ids) if row_ids else len(qrows)
    error_details = {
        "reason": "Preflight blocked transfer",
        "blockers": blocker_reasons,
        "guidance": [
            {
                "gate": b.get("id"),
                "message": b.get("message"),
                "why": (b.get("guidance") or {}).get("why", ""),
                "fix": (b.get("guidance") or {}).get("fix", ""),
            }
            for b in pf.get("blockers", [])
            if isinstance(b, dict) and b.get("guidance")
        ],
        "proof_bundle": {
            "decision": decision.get("decision"),
            "reason": decision.get("reason"),
            "semantic_mapping_score": pf.get("proof_bundle", {}).get(
                "semantic_mapping_score"
            ),
            "min_confidence": pf.get("proof_bundle", {}).get("min_confidence"),
            "quality_score": pf.get("proof_bundle", {}).get("quality_score"),
            "compliance_risk": (pf.get("proof_bundle", {}).get("compliance") or {}).get(
                "risk_score"
            ),
        },
        "readiness_score": pf.get("readiness_score"),
        "validation_plan": _validation_plan_for_result(pf),
        "payload_shape": pf.get("payload_shape"),
        "quarantine_issue_count": len(qrows),
        "quarantine_row_count": rejected_rows,
    }
    error_message = (
        decision.get("reason")
        or "; ".join(str(x) for x in blocker_reasons if x)
        or "Preflight blocked transfer"
    )
    lineage.emit_preflight_completed(
        run_id=job_id,
        passed=False,
        readiness_score=pf.get("readiness_score", 0),
        blockers=pf.get("blockers", []),
        validation_plan=_validation_plan_for_result(pf),
    )
    lineage.emit_run_failed(
        run_id=job_id,
        job_id=job_id,
        error=error_message,
        error_details=error_details,
    )
    mongo.update_job_status(
        job_id,
        "failed",
        error=error_message,
        phase="failed",
        progress_pct=0,
        error_details=error_details,
        preflight=pf,
        rejected_details=qrows,
        rejected_rows=rejected_rows,
    )
    return error_message, error_details


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
    from services.mapping_proof import build_mapping_proof

    mappings = list(request.mappings or [])
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
    )


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
        merged, _drift = reconcile_source_types(schema, live)
        return _rekey_to_read_columns(merged, columns)
    except Exception as exc:
        logger.debug("source schema authority merge failed: %s", exc, exc_info=exc)
        return schema




def _widen_design_sample(
    source: "EndpointConfig",
    columns: list[str],
    source_filter: dict[str, Any] | None,
    runner: "ShapeRunner | None",
    *,
    wanted: int,
    scan_budget: int = 50_000,
) -> list[dict[str, Any]]:
    """Read further into the source until ``wanted`` rows survive the read rewrites.

    A declared source filter and an approved recipe both remove rows *before*
    Map, the type inference and the gates ever see them, so a selective one can
    empty the first page and leave preflight with no rows to judge — which reads
    as "no sample" and blocks a run that is in fact correct. Reading on is the
    only honest answer: the rows exist, they are just deeper than one page.

    Bounded on purpose. A filter that keeps one row in a million must not turn
    design time into a full table scan, so the walk stops at ``scan_budget`` raw
    rows and returns what survived — the caller then judges a thinner sample,
    which is what it would have done for a small table anyway.
    """
    from services.row_filter import apply_row_filter

    from .source_peek import iter_stream_source_column_rows

    survivors: list[dict[str, Any]] = []
    scanned = 0
    for row in iter_stream_source_column_rows(source, columns):
        scanned += 1
        kept = [row]
        if source_filter:
            kept = apply_row_filter(kept, source_filter)
        if kept and runner is not None:
            kept = runner.records(kept)
        survivors.extend(kept)
        if len(survivors) >= wanted or scanned >= scan_budget:
            break
    return survivors[:wanted]


def _rekey_to_read_columns(
    schema: dict[str, str], columns: list[str] | None
) -> dict[str, str]:
    """Re-key declared types onto the spelling the reader gave the rows.

    Oracle/DB2/Snowflake introspection reports the folded catalog name
    (``AMOUNT``) while the same read hands rows back as ``amount``. Every
    consumer keyed on the row's own column name then missed the declaration and
    fell back to a sampled guess — ``NUMBER(12,2)`` money was scored as
    ``DECIMAL(8,4)``. Only an unambiguous single fold match is renamed.
    """
    if not schema or not columns:
        return schema
    out = dict(schema)
    for col in columns:
        name = str(col)
        if name in out:
            continue
        folded = name.casefold()
        hits = [k for k in out if str(k).casefold() == folded]
        if len(hits) == 1:
            out[name] = out.pop(hits[0])
    return out


def _source_nullability_probe(source: EndpointConfig) -> dict[str, bool]:
    """Real NOT NULL facts for a database source.

    G3 enforces the destination's NOT NULL contract, and a source column with
    unknown nullability is assumed nullable. That is right for files, but for
    an introspected database it made every NOT NULL destination column look
    unsafe — copying a table onto an identical table with a PRIMARY KEY blocked
    as a "type coercion issue". Returning {} keeps the old assume-nullable
    behaviour whenever the probe cannot speak.
    """
    if source.kind != "database":
        return {}
    try:
        from .endpoint_intelligence import introspect_endpoint

        source.extra = {**(source.extra or {}), "introspect_purpose": "source"}
        info = introspect_endpoint(source)
        return {
            str(k): bool(v)
            for k, v in dict(info.get("schema_nullability") or {}).items()
        }
    except Exception as exc:
        logger.debug("source nullability probe failed: %s", exc, exc_info=exc)
        return {}


def _destination_schema_probe(
    destination: EndpointConfig,
    sync_mode: str = "",
) -> tuple[dict[str, str], bool | None]:
    """Return (column_types, table_exists).

    ``table_exists`` is independent of whether columns loaded — a listed table
    with empty column metadata must not be treated as missing / create-new.

    Overwrite sync still needs a proven existence boolean for preflight DDL
    gates; stale destination *types* are cleared so recreate is not typed
    against the old table shape.
    """
    if destination.kind != "database":
        return {}, None
    try:
        from .endpoint_intelligence import introspect_endpoint

        # Match Studio Destination/Validate: destination probes stay in the
        # operator-chosen namespace (no cross-DB/schema "heal").
        destination.extra = {
            **(destination.extra or {}),
            "introspect_purpose": "destination",
        }
        info = introspect_endpoint(destination)
        schema = dict(info.get("schema") or {})
        nullability = {
            str(k): bool(v)
            for k, v in dict(info.get("schema_nullability") or {}).items()
        }
        if "table_exists" in info:
            raw = info.get("table_exists")
            # Preserve explicit None (unknown) — bool(None) is False and would
            # dishonestly trigger create-new on Execute after a soft-fail probe.
            exists = None if raw is None else bool(raw)
        elif schema:
            exists = True
        else:
            exists = None
        # Stamp probe SSOT for Map/Validate CTAs. Sticky None is an error;
        # create-new (exists=False) messages are informational, not failures.
        probe_err = str(info.get("sample_error") or info.get("error") or "").strip()
        probe_msg = str(info.get("message") or "").strip()
        extra = dict(destination.extra or {})
        if exists is None:
            detail = probe_err or probe_msg or (
                "Destination schema unknown — existence not proven (fail-closed)"
            )
            extra["schema_probe_error"] = detail[:500]
            extra["schema_probe_message"] = detail[:500]
        elif probe_err:
            extra["schema_probe_error"] = probe_err[:500]
            extra["schema_probe_message"] = (probe_msg or probe_err)[:500]
        else:
            extra.pop("schema_probe_error", None)
            if str(extra.get("schema_probe_message") or "").startswith(
                "Destination schema unknown"
            ):
                extra.pop("schema_probe_message", None)
            if probe_msg and exists is False:
                extra["schema_probe_message"] = probe_msg[:500]
        # Stamp PK/UNIQUE/FK catalog for Execute preflight SSOT with Validate
        # (preflight_router passes dest_meta.primary_key_columns / unique_keys).
        extra["primary_key_columns"] = list(info.get("primary_key_columns") or [])
        extra["unique_keys"] = list(info.get("unique_keys") or [])
        extra["foreign_keys"] = list(
            info.get("foreign_keys") or info.get("destination_foreign_keys") or []
        )
        # Create-new overwrite has no dest contract — clear stale types.
        if is_overwrite_sync(sync_mode) and exists is not True:
            extra["schema_nullability"] = {}
            extra["schema_defaults"] = {}
            extra["identity_columns"] = []
            extra["generated_columns"] = []
            extra["schema_carrier_authority"] = {}
            extra.pop("schema_types", None)
            destination.extra = extra
            return {}, exists
        # Dest-exists overwrite keeps nullability/defaults so G14/G15 write by
        # dest column name and never invent create-new on a listed table, but
        # its *types* are stale too: the table is dropped and recreated from
        # the source shape, so stamping the doomed carrier onto the mappings
        # refused a route for loss it cannot suffer (``TEXT → VARCHAR(64)`` on
        # a run whose own CREATE declares ``LONGTEXT``).
        overwrite_recreates_existing = is_overwrite_sync(sync_mode) and exists is True
        # Kept for G19 alone: the carrier the operator declared, which this run
        # is about to drop. No typing decision may read it — that is the bug
        # clearing the types above fixes — but the operator is owed the fact
        # that their declaration is being replaced.
        extra["overwrite_replaced_column_types"] = (
            dict(schema) if overwrite_recreates_existing else {}
        )
        extra["schema_nullability"] = nullability
        # Sampled vs declared — D1: a page-measured DECIMAL(2,2) is a profile,
        # not a catalog. Writers and Map both read this stamp.
        extra["schema_carrier_authority"] = dict(info.get("schema_authority") or {})
        if overwrite_recreates_existing:
            extra.pop("schema_types", None)
        else:
            extra["schema_types"] = dict(schema)
        # Who fills a required column when the mapping does not (G14).
        extra["schema_defaults"] = dict(info.get("schema_defaults") or {})
        extra["identity_columns"] = list(info.get("identity_columns") or [])
        extra["generated_columns"] = list(info.get("generated_columns") or [])
        destination.extra = extra
        return ({} if overwrite_recreates_existing else schema), exists
    except Exception as exc:
        logger.warning(
            "Destination schema probe failed for %s: %s",
            getattr(destination, "format", ""),
            exc,
            exc_info=exc,
        )
        destination.extra = {
            **(destination.extra or {}),
            "schema_probe_error": str(exc)[:500],
            "schema_probe_message": str(exc)[:500],
            "schema_nullability": {},
            "schema_defaults": {},
            "identity_columns": [],
            "generated_columns": [],
            "primary_key_columns": [],
            "unique_keys": [],
            "foreign_keys": [],
            "schema_carrier_authority": {},
            "schema_types": {},
        }
        return {}, None


def _apply_sampled_dest_profile(
    destination: EndpointConfig,
    mappings: list[dict[str, Any]],
    dest_schema_types: dict[str, str],
) -> dict[str, str]:
    """Widen a sampled dest profile so writers and preflight share one shape.

    Object-store / Redis rereads measure DECIMAL(2,2) from the first page.
    Treating that as DDL made run 2 refuse (and quarantine) the source's
    declared DECIMAL(12,2). Declared catalogs (SQL, Elasticsearch mappings)
    are left untouched.
    """
    from services.dest_schema_authority import apply_sampled_profile_to_dest_types

    extra = dict(getattr(destination, "extra", None) or {})
    authority = extra.get("schema_carrier_authority")
    widened = apply_sampled_profile_to_dest_types(
        dest_schema_types,
        mappings,
        dest_db=str(getattr(destination, "format", "") or ""),
        authority_map=authority if isinstance(authority, dict) else None,
    )
    extra["schema_types"] = dict(widened)
    destination.extra = extra
    return widened


def _destination_filler_metadata(extra: dict[str, Any] | None) -> dict[str, Any]:
    """Catalog facts about who fills a destination column when no mapping does (G14)."""
    meta = dict(extra or {})
    return {
        "destination_column_defaults": dict(meta.get("schema_defaults") or {}),
        "destination_identity_columns": list(meta.get("identity_columns") or []),
        "destination_generated_columns": list(meta.get("generated_columns") or []),
        "destination_live_column_types": dict(
            meta.get("overwrite_replaced_column_types") or {}
        ),
    }


def _preflight_sample_rows(records: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Execute must use the same sample cap as Validate (not a thinner head)."""
    from services.coercion_probe import PREFLIGHT_SAMPLE_LIMIT

    rows = list(records or [])
    if len(rows) > PREFLIGHT_SAMPLE_LIMIT:
        return rows[:PREFLIGHT_SAMPLE_LIMIT]
    return rows


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


def _file_population_rows(
    content: bytes | str | os.PathLike[str],
    filename: str,
    read_options: ReadOptions | None = None,
) -> Iterator[dict[str, Any]]:
    """Every source row of a file, for the pre-write population fit scan.

    Read-only and lazy: the preflight scan pulls rows only for columns whose
    declared source type can exceed a bounded destination carrier, so a file
    with no narrowing mapping is never re-read. A source that cannot be
    re-streamed yields nothing, which the scan reports as unmeasured evidence
    rather than as proof of fit.
    """
    from .file_stream import iter_source_rows

    try:
        yield from iter_source_rows(content, filename, read_options=read_options)
    except Exception as exc:  # noqa: BLE001 - unreadable source is unmeasured, never "fits"
        logger.warning(
            "population fit scan could not re-read %s: %s", filename, exc
        )


def _sync_mode_is_cdc(request: TransferRequest) -> bool:
    from services.sync_cursor import normalize_sync_mode

    return normalize_sync_mode(getattr(request, "sync_mode", "") or "", default="") == "cdc"


def _table_population_rows(
    request: TransferRequest,
    mappings: list[dict[str, Any]],
    *,
    column_types: dict[str, str],
    dest_types: dict[str, str],
    dest_db: str,
    shape_runner: "ShapeRunner | None" = None,
) -> Iterator[dict[str, Any]]:
    """Every source row of a table, projected to the bounded columns only.

    Same pre-write question as the file path, asked of a relational/object
    source: which declared narrowing can this population actually not survive.
    The projection is resolved from the mappings themselves, so a table with no
    bounded destination carrier issues no query at all. A reader that cannot be
    paged independently yields nothing, which the scan reports as preview
    evidence rather than as proof of fit.
    """
    from services.preflight_cursor_gate import read_scope_for_transfer_request
    from services.sync_cursor import incremental_read_narrows, normalize_sync_mode
    from .source_peek import iter_bounded_table_population_rows

    if normalize_sync_mode(getattr(request, "sync_mode", "") or "", default="") == "cdc":
        return
    scope = None
    if incremental_read_narrows(getattr(request, "sync_mode", "") or ""):
        try:
            scope = read_scope_for_transfer_request(request)
        except Exception:
            scope = None

    try:
        rows = iter_bounded_table_population_rows(
            request.source,
            mappings,
            column_types=column_types,
            dest_types=dest_types,
            dest_db=dest_db,
            source_kind=getattr(request.source, "kind", "") or "",
            source_format=getattr(request.source, "format", "") or "",
            limit=int(request.limit or 0),
            shape_runner=shape_runner,
            read_scope=scope if scope is not None and getattr(scope, "bounded", False) else None,
            source_filter=getattr(request, "source_filter", None) or None,
        )
        if rows is None:
            return
        yield from rows
    except Exception as exc:  # noqa: BLE001 - unreadable source is unmeasured, never "fits"
        logger.warning("population fit scan could not re-read source table: %s", exc)


def _execute_preflight_parity_kwargs(
    request: TransferRequest,
    *,
    destination_connected: bool,
    destination_table_exists_fallback: bool | None = None,
) -> dict[str, Any]:
    """Validate≡Execute preflight kwargs — privilege, FK, identity, operator acks.

    Never invent ``can_create``/``can_write`` from connectivity alone when a
    privilege probe is available. Contract PK prefers the destination stream name
    (same as ``preflight_router``).

    Owns ``destination_table_exists`` so callers must not also pass that kwarg
    into ``run_file_preflight`` (duplicate keyword → hard Execute failure).
    """
    from services.primary_key import extract_contract_primary_key_columns
    from services.preflight_service import inspect_destination_for_preflight

    dest = request.destination
    dest_table = str(dest.table or dest.collection or "")
    stream_contracts = list(getattr(request, "stream_contracts", None) or [])
    # Full composite contract PK — Validate≡Execute must not truncate to first col.
    # Prefer exact stream match; only fall back to first when a single contract is selected.
    contract_pk_cols = extract_contract_primary_key_columns(
        stream_contracts, stream_name=dest_table, fallback_first=False
    )
    if not contract_pk_cols:
        selected = [
            c
            for c in stream_contracts
            if isinstance(c, dict) and c.get("selected", True)
        ]
        if len(selected) == 1:
            contract_pk_cols = extract_contract_primary_key_columns(selected)
    contract_pk = ",".join(contract_pk_cols) if contract_pk_cols else ""

    extra = dict(getattr(dest, "extra", None) or {})
    dest_meta: dict[str, Any] = {}
    try:
        dest_meta = inspect_destination_for_preflight(
            connector_id=dest.connector_id or None,
            dest_type=dest.format or "",
            dest_host=dest.host or None,
            dest_port=int(dest.port or 0) or None,
            dest_database=dest.database or None,
            dest_table=dest.table or None,
            dest_collection=dest.collection or None,
            dest_schema=getattr(dest, "schema", None) or None,
            dest_username=dest.username or None,
            dest_password=dest.password or None,
            dest_connection_string=dest.connection_string or None,
            dest_warehouse=getattr(dest, "warehouse", None) or None,
            dest_auth_source=getattr(dest, "auth_source", None) or None,
            dest_auth_mode=getattr(dest, "auth_mode", None) or None,
            dest_auth_role=getattr(dest, "auth_role", None) or None,
            dest_api_key=getattr(dest, "api_key", None) or None,
            dest_service_account=getattr(dest, "service_account", None) or None,
            dest_kind=dest.kind or "database",
            dest_extra=dict(getattr(dest, "extra", None) or {}),
        )
    except Exception as exc:
        logger.warning(
            "Execute destination inspect for preflight parity failed: %s",
            exc,
            exc_info=exc,
        )

    pk_cols = list(
        dest_meta.get("primary_key_columns")
        or dest_meta.get("pk_columns")
        or extra.get("primary_key_columns")
        or []
    )
    unique_keys = list(dest_meta.get("unique_keys") or extra.get("unique_keys") or [])
    foreign_keys = list(
        dest_meta.get("foreign_keys")
        or extra.get("foreign_keys")
        or []
    )
    privilege_probe = dict(
        dest_meta.get("privilege_probe") or extra.get("privilege_probe") or {}
    )

    can_create = dest_meta.get("can_create_table")
    if can_create is None and privilege_probe:
        if "can_create_table" in privilege_probe:
            can_create = privilege_probe.get("can_create_table")
        elif "create" in privilege_probe:
            can_create = privilege_probe.get("create")
    if can_create is None:
        # Fail-closed: unknown privilege must not invent create-new DDL.
        can_create = False

    can_write = dest_meta.get("can_write")
    if can_write is None and privilege_probe:
        if "can_write" in privilege_probe:
            can_write = privilege_probe.get("can_write")
        elif "write" in privilege_probe:
            can_write = privilege_probe.get("write")
    if can_write is None:
        # Connectivity ≠ write grant. Unknown → assume writeable only for
        # append paths that still hit live driver errors; never invent create.
        can_write = bool(destination_connected)

    table_exists = dest_meta.get("table_exists")
    if table_exists is None:
        table_exists = destination_table_exists_fallback

    # Keep destination.extra stamped for later gates / theater honesty.
    extra["primary_key_columns"] = pk_cols
    extra["unique_keys"] = unique_keys
    extra["foreign_keys"] = foreign_keys
    if isinstance(table_exists, bool):
        extra["table_exists"] = table_exists
    if privilege_probe:
        extra["privilege_probe"] = privilege_probe
    dest.extra = extra

    return {
        "destination_pk_columns": pk_cols,
        "destination_unique_keys": unique_keys,
        "destination_foreign_keys": foreign_keys,
        "destination_config": dest_meta.get("_probe_cfg") or None,
        "contract_primary_key": contract_pk,
        "stream_contracts": stream_contracts,
        "privilege_probe": privilege_probe or None,
        "redshift_staging_probe": dest_meta.get("redshift_staging_probe") or None,
        "destination_can_create": bool(can_create),
        "destination_can_write": bool(can_write),
        # Always owned here — never also pass at the call site with **parity.
        "destination_table_exists": table_exists,
        "compliance_acknowledged": bool(
            getattr(request, "compliance_acknowledged", False)
        ),
        "schema_drift_acknowledged": bool(
            getattr(request, "schema_drift_acknowledged", False)
        ),
        "fk_risk_acknowledged": bool(getattr(request, "fk_risk_acknowledged", False)),
        "acknowledgment_actor": str(
            getattr(request, "acknowledgment_actor", "") or ""
        ).strip(),
        "acknowledgment_reason": str(
            getattr(request, "acknowledgment_reason", "") or ""
        ).strip(),
    }


# Back-compat alias for tests / callers that imported the identity-only helper.
def _execute_preflight_identity_kwargs(request: TransferRequest) -> dict[str, Any]:
    return _execute_preflight_parity_kwargs(request, destination_connected=True)


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


def _execute_policy_gates_for_request(
    request: TransferRequest,
    *,
    source_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Validate≡Execute policy gates — never default source_kind to file.

    Studio Validate passes dest_type / source_type / source_kind / write_via_staging.
    Execute must match or CDC/SCD2/staging falsely block (or skip) after Approve.
    """
    from services.preflight_cursor_gate import read_scope_for_transfer_request
    from services.preflight_service import run_transfer_policy_gates

    dest = getattr(request, "destination", None)
    src = getattr(request, "source", None)
    src_extra = getattr(src, "extra", None) or {}
    return run_transfer_policy_gates(
        sync_mode=str(getattr(request, "sync_mode", "") or ""),
        schema_policy=str(getattr(request, "schema_policy", "") or "manual_review"),
        validation_mode=str(getattr(request, "validation_mode", "") or "strict"),
        stream_contracts=list(getattr(request, "stream_contracts", None) or []),
        backfill_new_fields=bool(getattr(request, "backfill_new_fields", False)),
        source_columns=list(source_columns or []),
        dest_type=str(getattr(dest, "format", None) or getattr(dest, "kind", None) or ""),
        source_type=str(getattr(src, "format", None) or getattr(src, "kind", None) or ""),
        source_kind=str(getattr(src, "kind", None) or "file"),
        write_via_staging=bool(getattr(request, "write_via_staging", False)),
        source_read_mode=str(src_extra.get("source_read_mode") or ""),
        # Best-effort here; the read side refuses authoritatively with the exact
        # cursor key. Both refuse a watermark measured on another column.
        read_scope=read_scope_for_transfer_request(request),
        # Advanced write knobs — Validate already emits g17_row_cap. Execute
        # must name the same cap/sort or Approve hides a leftover filter.
        priority_column=str(getattr(request, "priority_column", "") or ""),
        priority_direction=str(getattr(request, "priority_direction", "") or "desc"),
        row_limit=max(0, int(getattr(request, "limit", 0) or 0)),
    )


def _destination_schema_types(
    destination: EndpointConfig, sync_mode: str = ""
) -> dict[str, str]:
    """Introspect destination column types for schema-aware preflight and transforms.

    For full-refresh overwrite sync modes the destination table will be dropped
    and recreated, so any existing schema is irrelevant and should not influence
    mapping or preflight decisions.
    """
    schema, _exists = _destination_schema_probe(destination, sync_mode=sync_mode)
    return schema


def _apply_schema_auto_propagate(
    *,
    request: Any,
    columns: list[str],
    schema: dict[str, str],
    mappings: list[dict[str, Any]],
    dest_schema_types: dict[str, str] | None,
) -> list[dict[str, Any]]:
    """Validate≡Execute schema auto-propagate — pause on hard-break, extend maps."""
    from services.schema_drift import apply_propagate_mappings, detect_schema_drift

    contract = resolve_sync_contract(getattr(request, "stream_contracts", None))
    dest_recreated = should_drop_destination_for_sync(
        request_sync_mode=getattr(request, "sync_mode", None),
        contract_sync_mode=contract.sync_mode if contract else None,
    )
    drift = detect_schema_drift(
        source_columns=columns,
        source_schema=schema,
        target_columns=list((dest_schema_types or {}).keys()),
        target_schema=dest_schema_types or {},
        mappings=mappings,
        destination_db_type=(getattr(request.destination, "format", "") or "").lower(),
        schema_policy=getattr(request, "schema_policy", None) or "manual_review",
        dest_recreated=dest_recreated,
    )
    evolution = drift.get("schema_evolution") or {}
    if evolution.get("should_pause"):
        raise ValueError(
            (drift.get("issues") or ["Breaking schema change — sync paused"])[0]
        )
    if not evolution.get("should_propagate"):
        return mappings
    mappings, applied = apply_propagate_mappings(
        mappings,
        source_columns=columns,
        source_schema=schema,
        evolution=evolution,
        schema_policy=getattr(request, "schema_policy", None) or "manual_review",
    )
    if not applied:
        return mappings
    return _enrich_mappings_with_types(
        mappings,
        column_types=schema,
        dest_types=dest_schema_types,
    )


def _infer_primary_key(columns: list[str], mappings: list[dict[str, Any]]) -> str:
    """Infer the primary key target column for mirror/upsert/key-addressed transfers.

    Prefers ``id`` / ``*_id`` / ``uuid``, then natural keys (``code``, ``iso``,
    ``name``, ``sku``, …). Never invents a weak attribute like ``capital`` when
    no strong candidate exists — that caused Redis keys such as
    ``countries:Abu_Dhabi`` when column order put ``capital`` first.
    """
    if not columns:
        return ""
    from services.primary_key import infer_redis_conflict_columns

    target_cols = [
        str(m.get("target") or m.get("source") or "")
        for m in (mappings or [])
        if (m.get("target") or m.get("source"))
    ]
    if not target_cols:
        target_cols = list(columns)
    inferred = infer_redis_conflict_columns(target_cols, mappings or [], None)
    if inferred:
        return inferred[0]
    # Refuse first-column fallback — weak attrs must be set explicitly on Map.
    return ""


def _declared_destination_key(
    destination: EndpointConfig,
    mappings: list[dict[str, Any]],
) -> list[str]:
    """The destination table's declared primary key, when the write covers it.

    Read from the destination catalog probe, so this is evidence rather than the
    name-shape inference used for key-addressed sinks. Returns ``[]`` when the
    table declares no key or when any key column is unmapped — an upsert keyed on
    a column this write never supplies would silently insert duplicates.
    """
    declared = [
        str(col).strip()
        for col in ((destination.extra or {}).get("primary_key_columns") or [])
        if str(col or "").strip()
    ]
    if not declared:
        return []
    written = {
        str(m.get("target") or "").strip().lower()
        for m in (mappings or [])
        if str(m.get("target") or "").strip() and not m.get("intentional_omit")
    }
    if any(col.lower() not in written for col in declared):
        return []
    return declared


def _checkpoint_has_progress(checkpoint: Any) -> bool:
    """True when the checkpoint has durable resume tokens (parity with Module 14).

    Must match ``job_has_durable_progress`` / ``evaluate_resume_safety`` — cursor /
    file_offset alone are enough to resume. Narrow row-only checks wiped those
    tokens on reclaim and restarted append from zero (silent duplicates).
    """
    if not checkpoint:
        return False
    try:
        return bool(
            int(getattr(checkpoint, "chunk_index", 0) or 0) > 0
            or int(getattr(checkpoint, "offset", 0) or 0) > 0
            or int(getattr(checkpoint, "rows_processed", 0) or 0) > 0
            or int(getattr(checkpoint, "file_offset", 0) or 0) > 0
            or getattr(checkpoint, "cursor_value", None) is not None
            or getattr(checkpoint, "dynamodb_cursor", None)
            or getattr(checkpoint, "kafka_cursor", None)
            or getattr(checkpoint, "es_search_after", None) is not None
        )
    except (TypeError, ValueError):
        return False


def _apply_post_load_transforms(request: Any, dest_summary: dict[str, Any]) -> None:
    """Run configured transformation models and fold the result into the summary.

    Called from all three completion paths (buffered, DB-to-DB streaming, file
    streaming). They are near-identical blocks, and a post-load step wired into
    only one of them would run after a CSV load but not after a Postgres
    stream — worse than not running at all, because the operator cannot tell
    which happened.

    Never raises. The rows are already at the destination by this point, so a
    transformation failure must be reported inside the summary rather than
    turning a successful load into a failed job.
    """
    try:
        from services.post_load_transform import run_post_load_transforms

        destination = getattr(request, "destination", None)
        if destination is None:
            return
        landed = str(
            getattr(destination, "table", "")
            or getattr(destination, "collection", "")
            or ""
        )
        outcome = run_post_load_transforms(
            destination=destination,
            landed_table=landed,
            workspace_id=str(getattr(request, "workspace_id", "") or ""),
        )
        # Surface anything the operator needs to act on. A clean "nothing
        # configured" skip is omitted so the job summary stays dense; a
        # mismatch, load failure, or partial run always lands in the summary
        # so it cannot be mistaken for "nothing was configured".
        if (
            outcome.get("ran")
            or outcome.get("status") in {"failed", "partial"}
            or (
                outcome.get("status") == "skipped"
                and outcome.get("message")
                and "No transformation project" not in str(outcome.get("message"))
            )
        ):
            dest_summary["transformations"] = outcome
    except Exception as exc:
        logger.warning("Post-load transformations skipped: %s", exc, exc_info=exc)
        dest_summary["transformations"] = {
            "ran": False,
            "status": "failed",
            "projects": [],
            "message": f"Post-load transformations could not start: {exc}",
        }


# Quarantine durability + rollback plan attachment live in ``job_quarantine``;
# re-exported for the historical ``engine`` import surface.
from .job_quarantine import (  # noqa: E402,F401 — re-export
    _attach_job_rollback_plan,
    _persist_checkpoint_quarantine_delta,
    _persist_job_quarantine,
    checkpoint_quarantine_summary,
    fail_closed_on_silent_loss,
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
    _cdc_fields_from_summary,
    _fail_runtime_job,
    _job_failure_fields,
    _promote_cdc_job_fields,
)



def _drop_destination_table(destination: EndpointConfig) -> bool:
    """Drop the destination object for full-refresh overwrite sync modes.

    Raises :class:`FullRefreshDropFailed` when a drop was attempted and failed.
    Swallowing it is the difference between an overwrite and an append: the old
    rows survive, the new rows land on top, and the job reports success with a
    doubled destination. A ``full_refresh`` that cannot clear the destination
    must fail loudly instead of quietly changing its own semantics.

    Returns ``False`` only when the driver has no drop support, which is a
    genuine "nothing to do" and not an error.
    """
    if destination.kind != "database":
        return False

    from connectors.table_manager import TableDropError, drop_table

    from .adapters import (
        carry_dest_spelling_across_drop,
        resolve_connector_config,
        resolve_dest_table,
    )
    from .connector_capabilities import resolve_driver_type

    try:
        db_type = resolve_driver_type(destination.format)
        cfg = resolve_connector_config(destination)
        table_name = resolve_dest_table(db_type, destination)
        schema = cfg.get("schema")
    except Exception as exc:
        # Could not even work out what to drop. Treated as fatal for the same
        # reason: proceeding would append onto an uncleared table.
        raise FullRefreshDropFailed(
            "unknown", f"could not resolve destination for drop: {exc}"
        ) from exc

    carry_dest_spelling_across_drop(destination, db_type, cfg, table_name, schema)
    try:
        return drop_table(db_type, cfg, table_name, schema)
    except TableDropError as exc:
        logger.error("full_refresh drop failed for %s: %s", table_name, exc)
        raise FullRefreshDropFailed(table_name, str(exc.cause)) from exc
    except Exception as exc:
        logger.error("full_refresh drop failed for %s: %s", table_name, exc, exc_info=exc)
        raise FullRefreshDropFailed(table_name, str(exc)) from exc


from .mapping_write_stamp import (  # noqa: E402
    enrich_mappings_with_types as _enrich_mappings_with_types,
    schema_for_endpoint as _schema_for_endpoint,
    stamp_additive_mappings_for_write as _stamp_additive_mappings_for_write,
)


def _auto_map(
    request: TransferRequest,
    columns: list[str],
    schema: dict[str, str],
    sample_rows: list[dict] | None = None,
    job_id: str = "",
) -> list[dict]:
    """Generate destination-aware mappings when no mapping contract was supplied.

    For append/upsert/merge into an existing target, the destination schema is
    introspected and the semantic mapper aligns source columns to target columns.
    For full-refresh/overwrites into a new target, identity mappings are used so
    the destination can be created from the source shape.
    """
    mappings: list[dict] | None = None

    if request.mappings:
        mappings = request.mappings
    elif request.destination.kind != "database":
        mappings = default_mappings(columns)
    else:
        sync_mode = resolve_effective_sync_mode(request.sync_mode)
        if is_overwrite_sync(sync_mode):
            from services.overwrite_typed_mapping import (
                build_overwrite_create_new_mappings,
            )

            mappings = build_overwrite_create_new_mappings(
                columns=columns,
                schema=schema,
                sample_rows=sample_rows,
                request=request,
            )
            # Dest-exists overwrite must not invent extra dest columns. Extra
            # source stays unaccounted so G13 blocks instead of silent drop
            # or ADD COLUMN from source position.
            target_schema, probe_exists = _destination_schema_probe(
                request.destination,
                sync_mode=sync_mode,
            )
            dest_exists = destination_exists_for_shape(
                probe_exists,
                dest_format=str(getattr(request.destination, "format", "") or ""),
            )
            if dest_exists is True and target_schema:
                from services.mapping_constraints import retain_dest_exists_write_mappings

                mappings = retain_dest_exists_write_mappings(
                    mappings, list(target_schema)
                )
        else:
            target_schema, probe_exists = _destination_schema_probe(
                request.destination,
                sync_mode=sync_mode,
            )
            # Shape/existence is the raw probe. Typing may collapse
            # exists+empty-catalog to "no types to bind" — that must not
            # invent create-new on a listed dest table (G15 pending).
            dest_exists = destination_exists_for_shape(
                probe_exists,
                dest_format=str(getattr(request.destination, "format", "") or ""),
            )
            if not target_schema:
                # Empty columns: only invent identity create-new when the object
                # is confirmed missing. Existing/unknown → pending via mapper.
                try:
                    from services.mapping_pipeline import run_mapping_pipeline

                    source_schemas = [
                        {
                            "name": c,
                            "inferred_type": schema.get(c, "string"),
                            "samples": [
                                cell_to_string(r.get(c, ""))
                                for r in (sample_rows or [])[:8]
                            ],
                        }
                        for c in columns
                    ]
                    source_samples = {
                        c: [
                            cell_to_string(r.get(c, ""))
                            for r in (sample_rows or [])[:8]
                        ]
                        for c in columns
                    }
                    from services.data_profiler import source_types_are_authoritative

                    result = run_mapping_pipeline(
                        source_columns=columns,
                        target_columns=[],
                        source_schemas=source_schemas,
                        target_schemas=None,
                        file_format=request.source.format,
                        confidence_threshold=confidence_threshold_for_mode(
                            request.validation_mode
                        ),
                        source_samples=source_samples,
                        validation_mode=request.validation_mode,
                        use_llm=False,
                        schema_policy=request.schema_policy,
                        destination_db_type=(request.destination.format or "").lower(),
                        source_db_type=(request.source.format or "").lower(),
                        destination_table_exists=dest_exists,
                        source_types_authoritative=source_types_are_authoritative(
                            request.source.kind or "",
                            request.source.format or "",
                        ),
                    )
                    auto = result.get("mappings")
                    if (
                        auto
                        and isinstance(auto, list)
                        and any(m.get("source") for m in auto)
                    ):
                        # New tables should keep source column names; the semantic
                        # mapper may canonicalize (e.g. birth_date -> date_of_birth)
                        # which is only safe when the target schema is known.
                        # Preserve the mapper's inferred target_type.
                        if dest_exists is False:
                            for m in auto:
                                m["target"] = m.get("source") or m.get("target", "")
                                m["create_new"] = True
                        mappings = auto
                    elif dest_exists is False:
                        mappings = default_mappings(columns)
                except Exception as exc:
                    logger.warning(
                        "Empty-dest schema map failed: %s; fallback identity only if missing",
                        exc,
                    )
                    if dest_exists is False:
                        mappings = default_mappings(columns)
            else:
                # For MongoDB append/upsert, do not let the semantic mapper overwrite _id
                # unless the source literally contains an _id column or the user supplied a mapping.
                if (
                    request.destination.format == "mongodb"
                    and not is_overwrite_sync(sync_mode)
                    and "_id" not in columns
                ):
                    target_schema = {
                        k: v for k, v in target_schema.items() if k != "_id"
                    }

                try:
                    from services.mapping_pipeline import run_mapping_pipeline

                    source_schemas = [
                        {
                            "name": c,
                            "inferred_type": schema.get(c, "string"),
                            "samples": [
                                cell_to_string(r.get(c, ""))
                                for r in (sample_rows or [])[:8]
                            ],
                        }
                        for c in columns
                    ]
                    target_columns = list(target_schema.keys())
                    target_schemas = [
                        {
                            "name": c,
                            "inferred_type": target_schema.get(c, "string"),
                            "samples": [],
                        }
                        for c in target_columns
                    ]
                    source_samples = {
                        c: [
                            cell_to_string(r.get(c, ""))
                            for r in (sample_rows or [])[:8]
                        ]
                        for c in columns
                    }
                    from services.data_profiler import source_types_are_authoritative

                    result = run_mapping_pipeline(
                        source_columns=columns,
                        target_columns=target_columns,
                        source_schemas=source_schemas,
                        target_schemas=target_schemas,
                        file_format=request.source.format,
                        confidence_threshold=confidence_threshold_for_mode(
                            request.validation_mode
                        ),
                        source_samples=source_samples,
                        validation_mode=request.validation_mode,
                        use_llm=False,
                        schema_policy=request.schema_policy,
                        # Without the dialect the fidelity verdict is judged in a
                        # vacuum: DECIMAL→TEXT reads as loss, when TEXT is the
                        # exact-digit carrier our own DDL picks on SQLite. Every
                        # existing-destination route shares this call.
                        destination_db_type=(request.destination.format or "").lower(),
                        source_db_type=(request.source.format or "").lower(),
                        destination_table_exists=dest_exists,
                        source_types_authoritative=source_types_are_authoritative(
                            request.source.kind or "",
                            request.source.format or "",
                        ),
                    )
                    auto = result.get("mappings")
                    if (
                        auto
                        and isinstance(auto, list)
                        and any(m.get("source") for m in auto)
                    ):
                        mapped_sources = {str(m.get("source")) for m in auto}
                        if request.backfill_new_fields:
                            for c in columns:
                                if c not in mapped_sources:
                                    auto.append(
                                        {"source": c, "target": c, "confidence": 0.95}
                                    )
                        mappings = auto
                except Exception as exc:
                    logger.warning(
                        "Auto-mapping failed: %s; falling back to identity mappings",
                        exc,
                    )

    if mappings is None:
        mappings = default_mappings(columns)

    _record_schema_and_lineage(request, mappings, schema, job_id)
    return mappings


def _record_schema_and_lineage(
    request: TransferRequest,
    mappings: list[dict[str, Any]],
    schema: dict[str, str],
    job_id: str,
) -> None:
    """Register source/target schemas and column-level lineage for this job."""
    src_id = (
        request.source.connector_id
        or request.source.host
        or (request.source_filename if request.source.kind == "file" else "")
        or f"{request.source.kind}:{request.source.format}"
    )
    src_object = (
        request.source.table
        or request.source.collection
        or (request.source_filename if request.source.kind == "file" else "")
        or "source"
    )
    dst_id = (
        request.destination.connector_id
        or request.destination.host
        or f"{request.destination.kind}:{request.destination.format}"
    )
    dst_object = (
        request.destination.table
        or request.destination.collection
        or f"{request.destination.format}_export"
    )

    source_columns = [
        {"name": name, "type": (schema.get(name) or "string"), "primary_key": False}
        for name in schema.keys()
    ]
    schema_registry.register_schema(
        columns=source_columns,
        connector_type=request.source.format,
        connector_id=src_id,
        object_name=src_object,
        job_id=job_id,
        source_of_truth=True,
    )

    # Build a best-effort target schema from the mappings.
    target_columns = []
    for m in mappings:
        src = str(m.get("source", "")).strip()
        tgt = str(m.get("target", "")).strip()
        if not src or not tgt:
            continue
        target_columns.append(
            {
                "name": tgt,
                "type": schema.get(src, "string"),
                "primary_key": False,
            }
        )
    if target_columns:
        schema_registry.register_schema(
            columns=target_columns,
            connector_type=request.destination.format,
            connector_id=dst_id,
            object_name=dst_object,
            job_id=job_id,
        )
        schema_registry.record_lineage(
            source={
                "connector_type": request.source.format,
                "connector_id": src_id,
                "object_name": src_object,
            },
            target={
                "connector_type": request.destination.format,
                "connector_id": dst_id,
                "object_name": dst_object,
            },
            mappings=mappings,
            job_id=job_id,
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
        super().__init__(
            "An equivalent transfer is already "
            f"{self.existing_status or 'in progress'}"
            + (f" (job {self.existing_job_id})" if self.existing_job_id else "")
            + ". Wait for it to finish or cancel it before starting another run."
        )


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
        request.destination = resolve_endpoint(
            request.destination, workspace_id=request.workspace_id
        )

    def execute_tracked(
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
        if request.source.kind == "file":
            from services.transfer_file_staging import hydrate_file_source

            hydrate_file_source(request)
        # Create-new invention needs the source engine to keep Unicode polarity
        # (PostgreSQL VARCHAR → SQL Server NVARCHAR, not code-page VARCHAR).
        with bind_auto_create_job(job_id), bind_source_engine(
            request.source.format or ""
        ):
            result = self._execute_tracked_inner(request, job_id, resume=resume)
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
        # Hard-block Execute when Map still has unresolved requires_review rows —
        # skip_preflight must never green-path ambiguous remaps into a write.
        # Delivery: at_least_once default; exactly_once opt-in and fail-closed
        # on ineligible routes. at_most_once is never offered.
        from services.cdc_exactly_once import (
            ExactlyOnceRouteError,
            assert_requested_cdc_delivery,
            dest_allow_append_only,
            route_has_cdc_pk,
        )
        from services.execution_engine_contract import DeliveryGuaranteeError
        from services.mapping_pipeline import assert_mappings_executable
        from services.procedure_source import is_callable_source

        try:
            assert_requested_cdc_delivery(
                getattr(request, "delivery_guarantee", None) or "at_least_once",
                sync_mode=getattr(request, "sync_mode", "") or "",
                dest_type=str(getattr(request.destination, "format", "") or ""),
                source_type=str(getattr(request.source, "format", "") or ""),
                has_primary_key=route_has_cdc_pk(
                    getattr(request, "stream_contracts", None),
                ),
                allow_append_only=dest_allow_append_only(request.destination),
                callable_source=is_callable_source(request.source),
            )
            from services.procedure_source import assert_callable_sync_allowed

            assert_callable_sync_allowed(
                getattr(request, "sync_mode", "") or "",
                getattr(request, "source", None),
            )
            assert_mappings_executable(request.mappings)
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
        locale_token = set_active_date_locale(request.date_locale)
        number_token = set_active_number_locale(
            _run_number_locale(request)
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
            reset_active_number_locale(number_token)
            reset_active_date_locale(locale_token)
            # The run is over however it ended, so the slot must be freed here.
            # Releasing only on success would leave a failed job's claim in place
            # and block the operator's retry until the TTL expired.
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
        try:
            mongo.update_job_status(
                job_id,
                "running",
                phase="reading",
                progress_pct=5,
                message="Reading source data…",
            )
            records, columns, schema = with_retry(
                lambda: self._read_source(request),
                budget=RetryBudget(
                    max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0
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
            shape_runner = _open_shape_runner(request, columns)
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

            dest_schema_types, dest_table_exists_flag = _destination_schema_probe(
                request.destination,
                sync_mode=request.sync_mode,
            )
            mappings = _enrich_mappings_with_types(
                _auto_map(
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
                parity = _execute_preflight_parity_kwargs(
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
                    population_rows=None if reuse_fit else records,
                    rows_are_population=not reuse_fit,
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
                    schema_policy=request.schema_policy,
                    backfill_new_fields=request.backfill_new_fields,
                    date_locale=request.date_locale,
                    number_locale=getattr(request, "number_locale", "") or "",
                    resume=resume,
                    **parity,
                )
                pf = apply_policy_gates(
                    pf,
                    _execute_policy_gates_for_request(
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
                        mongo, job_id, pf, lineage=lineage
                    )
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=_validation_plan_for_result(pf),
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
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

            def _check_cancelled() -> None:
                try:
                    job = mongo.get_job(job_id)
                    # Honour the durable cancel flag as well as the status. The
                    # status field is rewritten by this very loop on every
                    # chunk, so a cancel that landed mid-chunk could be
                    # overwritten before it was ever read.
                    if job and (
                        job.get("cancel_requested") or job.get("status") == "cancelled"
                    ):
                        raise TransferCancelled("Transfer cancelled by user")
                except TransferCancelled:
                    raise
                except Exception as exc:
                    logger.warning("Cancellation check failed: %s", exc, exc_info=exc)

            _quarantine_persisted = [0]

            def on_checkpoint(
                chunk: int, chunks: int, rows: int, checkpoint: dict | None = None
            ) -> None:
                _check_cancelled()
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
                    message=f"Writing batch {chunk}/{chunks} ({rows:,} rows)…",
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
                mongo.update_job_status(job_id, "running", **update)

            throttled_checkpoint = ThrottledCheckpoint(on_checkpoint)
            backfill_fields = effective_backfill_new_fields(
                backfill_new_fields=request.backfill_new_fields,
                schema_policy=request.schema_policy,
                mappings=getattr(request, "mappings", None),
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
                ) and not (resume and checkpoint_has_progress)
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
                        if write_mode == "insert" and not conflict_columns:
                            raise ValueError(
                                "Cannot safely resume a buffered insert without primary key; "
                                "use upsert sync mode or restart with full_refresh_overwrite"
                            )
                        # Force upsert so re-applying prior rows is idempotent.
                        if conflict_columns:
                            write_mode = "upsert"
                        # Keep full population — no positional slice.
                        # Engine record spill owns the population; do not
                        # duplicate the dict list for resume.
                        resume_full_records = []
                        resume_skipped_rows = 0
                        dest_summary_resume_note = (
                            f"Idempotent resume after {skip_n:,} prior row(s) — "
                            "re-applying full population via upsert (key-addressed)."
                        )
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
                        _drop_destination_table(request.destination)
                        mongo.update_job_status(
                            job_id,
                            "running",
                            phase="writing",
                            message="Connecting to destination and creating table…",
                        )
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
                        release_records=True,
                        retain_engine_spill=True,
                        collect_mirror_keys=effective_sync_lower
                        in ("full_refresh_mirror", "mirror"),
                    )

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
                        mirror_summary = apply_inferred_soft_deletes(
                            request.destination,
                            records,
                            columns,
                            schema,
                            mappings,
                            conflict_columns,
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
                mongo.update_job_status(
                    job_id,
                    "running",
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
                proof_kind=str((dest_summary or {}).get("checksum_mode") or "full")
                if isinstance(dest_summary, dict)
                else "full",
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
                    )
                finally:
                    if spill_holder is not None:
                        spill_holder.close()
            dest_summary = pii_guard.redact_destination_summary(dest_summary, mappings)
            recon = pii_guard.redact_reconciliation(recon, mappings)
            if not recon.get("passed"):
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=recon.get("message", "Reconciliation failed"),
                    phase="failed",
                    progress_pct=99,
                    message=recon.get("message"),
                    reconciliation=recon,
                    destination_summary=dest_summary,
                    rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                    coerced_null_rows=int(
                        dest_summary.get("coerced_null_rows", 0) or 0
                    ),
                )
                return TransferResult(
                    success=False,
                    error=recon.get("message", "Reconciliation failed"),
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=rows_written,
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

            # Reconciliation passed: the delta is at rest, so the watermark may
            # move. Persisting it earlier would skip these rows after a failed run.
            incremental_bound.commit()

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
            )
            _attach_job_rollback_plan(job_id, dest_summary, request)
            _apply_post_load_transforms(request, dest_summary)
            mongo.update_job_status(
                job_id,
                terminal_status,
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get(
                    "message", f"Transferred {rows_written:,} rows successfully"
                ),
                destination_database=dest_summary.get(
                    "database", request.destination.database or ""
                ),
                destination_collection=dest_summary.get("collection")
                or dest_summary.get("table", ""),
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
                    redacted_training = pii_guard.redact_records(records[:5], mappings)
                    samples = {
                        c: [
                            cell_to_string(r.get(c, ""))
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
            block_msg = str(blocked)
            mongo.update_job_status(
                job_id,
                "failed",
                phase="failed",
                error=block_msg,
                message=block_msg,
                records_processed=int(blocked.rows_written or 0),
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
                records_transferred=int(blocked.rows_written or 0),
                destination_summary=dest_summary,
            )
        except Exception as e:
            finalize_contract(contract_id, success=False)
            display, error_details = _fail_runtime_job(
                mongo, job_id, e, lineage=lineage
            )
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
        try:
            mongo.update_job_status(
                job_id,
                "running",
                phase="reading",
                progress_pct=5,
                message="Analyzing source table…",
            )
            columns, schema, total_rows, sample_rows = peek_stream_source(
                request.source
            )
            schema = _authoritative_source_schema(request.source, schema, columns)
            if request.limit > 0:
                total_rows = min(total_rows, request.limit)
            if total_rows == 0:
                mongo.update_job_status(
                    job_id, "failed", error="Source table is empty", phase="failed"
                )
                return TransferResult(
                    success=False,
                    error="Source table is empty",
                    operation=request.operation,
                    job_id=job_id,
                )

            read_columns = list(columns)
            raw_sample_size = len(sample_rows)
            if request.source_filter:
                sample_rows = apply_row_filter(sample_rows, request.source_filter)

            # The recipe runs on the read, so Map, the narrowing scan and the DDL
            # identity are decided from the shaped columns and values the writer
            # will receive. A separate throwaway runner shapes the design-time
            # sample: those effects are not the population's.
            _settle_locales(request, sample_rows, columns)
            shape_runner = _open_shape_runner(request, columns)
            declared_contract = resolve_sync_contract(request.stream_contracts)
            shape_refusal = _shape_stream_refusal(
                shape_runner,
                effective_sync=resolve_effective_sync_mode(
                    request.sync_mode,
                    declared_contract.sync_mode if declared_contract else None,
                ),
                multi_stream=len(
                    resolve_selected_sync_contracts(request.stream_contracts)
                ) > 1,
                cursor_field=(
                    declared_contract.cursor_field if declared_contract else ""
                ),
                key_columns=(
                    declared_contract.primary_key_columns()
                    if declared_contract and declared_contract.primary_key
                    else []
                ),
            )
            if shape_refusal:
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=shape_refusal,
                    phase="failed",
                    progress_pct=0,
                )
                return TransferResult(
                    success=False,
                    error=shape_refusal,
                    error_details={
                        "reason": "shape_route_unsupported",
                        "remediation": (
                            "Remove the Shape recipe for this sync mode, or shape "
                            "on a full-refresh / incremental-append route."
                        ),
                    },
                    operation=request.operation,
                    job_id=job_id,
                )
            sample_probe = (
                ShapeRunner(shape_runner.recipe) if shape_runner is not None else None
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
                    ShapeRunner(shape_runner.recipe)
                    if shape_runner is not None
                    else None
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
            dest_schema_types, dest_table_exists_flag = _destination_schema_probe(
                request.destination,
                sync_mode=request.sync_mode,
            )
            mappings = _enrich_mappings_with_types(
                _auto_map(
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
                parity = _execute_preflight_parity_kwargs(
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
                            shape_runner=shape_runner,
                        )
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
                    schema_policy=request.schema_policy,
                    backfill_new_fields=request.backfill_new_fields,
                    date_locale=request.date_locale,
                    number_locale=getattr(request, "number_locale", "") or "",
                    resume=resume,
                    **parity,
                )
                pf = apply_policy_gates(
                    pf,
                    _execute_policy_gates_for_request(
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
                        mongo, job_id, pf, lineage=lineage
                    )
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=_validation_plan_for_result(pf),
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
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

            def _check_cancelled() -> None:
                try:
                    job = mongo.get_job(job_id)
                    # Honour the durable cancel flag as well as the status. The
                    # status field is rewritten by this very loop on every
                    # chunk, so a cancel that landed mid-chunk could be
                    # overwritten before it was ever read.
                    if job and (
                        job.get("cancel_requested") or job.get("status") == "cancelled"
                    ):
                        raise TransferCancelled("Transfer cancelled by user")
                except TransferCancelled:
                    raise
                except Exception as exc:
                    logger.warning("Cancellation check failed: %s", exc, exc_info=exc)

            _quarantine_persisted = [0]

            def on_checkpoint(
                chunk: int, chunks: int, rows: int, checkpoint: dict | None = None
            ) -> None:
                _check_cancelled()
                # CDC has no finite denominator — never invent a percentage.
                sync_l = (request.sync_mode or "").lower()
                contracts = request.stream_contracts or []
                is_cdc = sync_l == "cdc" or any(
                    str((c or {}).get("sync_mode") or "").lower() == "cdc"
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
                    message=(
                        f"CDC applied {rows:,} change(s)…"
                        if is_cdc
                        else f"Writing batch {chunk}/{chunks} ({rows:,} rows)…"
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
                mongo.update_job_status(job_id, "running", **update)

            throttled_checkpoint = ThrottledCheckpoint(on_checkpoint)
            backfill_fields = effective_backfill_new_fields(
                backfill_new_fields=request.backfill_new_fields,
                schema_policy=request.schema_policy,
                mappings=getattr(request, "mappings", None),
            )

            mongo.update_job_status(
                job_id,
                "running",
                phase="writing",
                progress_pct=compute_transfer_progress_pct(
                    phase="writing", rows_processed=0, total_rows=total_rows
                )
                or 5,
                message=f"Streaming {row_count_label(total_rows)} rows in batches…",
            )

            is_streaming = True
            stream_contract = resolve_sync_contract(request.stream_contracts)
            selected_streams = resolve_selected_sync_contracts(request.stream_contracts)
            multi_non_cdc = len(selected_streams) > 1
            # Overwrite DROP once on primary is wrong for multi-stream — sequential
            # path drops each remapped destination instead.
            if not multi_non_cdc and should_drop_destination_for_sync(
                request_sync_mode=request.sync_mode,
                contract_sync_mode=stream_contract.sync_mode
                if stream_contract
                else None,
            ):
                if (
                    not resume
                    or not is_streaming
                    or not _checkpoint_has_progress(checkpoint)
                ):
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
                    _drop_destination_table(request.destination)
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
                )

            with _reconcile_phase_heartbeat(
                mongo,
                job_id,
                processed=int(rows_written or 0),
                total=int(rows_written or 0),
                proof_kind=str((dest_summary or {}).get("checksum_mode") or "full")
                if isinstance(dest_summary, dict)
                else "full",
            ):
                if isinstance(dest_summary, dict):
                    dest_summary.setdefault("sync_mode", effective_sync)
                    dest_summary.setdefault("job_id", job_id)
                    dest_summary.setdefault("streaming", True)
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
                )
            dest_summary = pii_guard.redact_destination_summary(dest_summary, mappings)
            recon = pii_guard.redact_reconciliation(recon, mappings)
            if not recon.get("passed"):
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=recon.get("message", "Reconciliation failed"),
                    phase="failed",
                    progress_pct=99,
                    message=recon.get("message"),
                    reconciliation=recon,
                    destination_summary=dest_summary,
                    rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                    coerced_null_rows=int(
                        dest_summary.get("coerced_null_rows", 0) or 0
                    ),
                )
                return TransferResult(
                    success=False,
                    error=recon.get("message", "Reconciliation failed"),
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=rows_written,
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
            )
            _attach_job_rollback_plan(job_id, dest_summary, request)
            _apply_post_load_transforms(request, dest_summary)
            mongo.update_job_status(
                job_id,
                terminal_status,
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get(
                    "message", f"Transferred {rows_written:,} rows successfully"
                ),
                destination_database=dest_summary.get(
                    "database", request.destination.database or ""
                ),
                destination_collection=dest_summary.get("collection")
                or dest_summary.get("table", ""),
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
            block_msg = str(blocked)
            mongo.update_job_status(
                job_id,
                "failed",
                phase="failed",
                error=block_msg,
                message=block_msg,
                records_processed=int(blocked.rows_written or 0),
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
                records_transferred=int(blocked.rows_written or 0),
                destination_summary=dest_summary,
                contract_id=contract_id,
            )
        except Exception as e:
            finalize_contract(contract_id, success=False)
            display, error_details = _fail_runtime_job(
                mongo, job_id, e, lineage=lineage
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=display,
                error_details=error_details,
                operation=request.operation,
                contract_id=contract_id,
            )

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
            shape_runner = _open_shape_runner(request, columns)
            if shape_runner is not None:
                sample_probe = ShapeRunner(shape_runner.recipe)
                sample_rows = sample_probe.records(sample_rows)
                columns = list(sample_probe.output_columns or columns)
                schema = shaped_schema(sample_probe, sample_rows, schema)

            mongo.update_job_status(
                job_id, "running", total_rows=total_rows, records_processed=0
            )
            dest_schema_types, dest_table_exists_flag = _destination_schema_probe(
                request.destination,
                sync_mode=request.sync_mode,
            )
            mappings = _enrich_mappings_with_types(
                _auto_map(
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
                parity = _execute_preflight_parity_kwargs(
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
                        if reuse_fit
                        else _shaped_population_rows(
                            shape_runner,
                            _file_population_rows(content, filename, read_options),
                        )
                    ),
                    rows_are_population=not reuse_fit,
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
                    schema_policy=request.schema_policy,
                    backfill_new_fields=request.backfill_new_fields,
                    date_locale=request.date_locale,
                    number_locale=getattr(request, "number_locale", "") or "",
                    resume=resume,
                    **parity,
                )
                pf = apply_policy_gates(
                    pf,
                    _execute_policy_gates_for_request(
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
                        mongo, job_id, pf, lineage=lineage
                    )
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=_validation_plan_for_result(pf),
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
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

            def _check_cancelled() -> None:
                try:
                    job = mongo.get_job(job_id)
                    # Honour the durable cancel flag as well as the status. The
                    # status field is rewritten by this very loop on every
                    # chunk, so a cancel that landed mid-chunk could be
                    # overwritten before it was ever read.
                    if job and (
                        job.get("cancel_requested") or job.get("status") == "cancelled"
                    ):
                        raise TransferCancelled("Transfer cancelled by user")
                except TransferCancelled:
                    raise
                except Exception as exc:
                    logger.warning("Cancellation check failed: %s", exc, exc_info=exc)

            _quarantine_persisted = [0]

            def on_checkpoint(
                chunk: int, chunks: int, rows: int, checkpoint: dict | None = None
            ) -> None:
                _check_cancelled()
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
                    message=f"Writing batch {chunk}/{chunks} ({rows:,} rows)…",
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
                mongo.update_job_status(job_id, "running", **update)

            throttled_checkpoint = ThrottledCheckpoint(on_checkpoint)
            backfill_fields = effective_backfill_new_fields(
                backfill_new_fields=request.backfill_new_fields,
                schema_policy=request.schema_policy,
                mappings=getattr(request, "mappings", None),
            )

            mongo.update_job_status(
                job_id,
                "running",
                phase="writing",
                progress_pct=compute_transfer_progress_pct(
                    phase="writing", rows_processed=0, total_rows=total_rows
                )
                or 5,
                message=f"Streaming {row_count_label(total_rows)} rows in batches…",
            )

            is_streaming = True
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
                if (
                    not resume
                    or not is_streaming
                    or not _checkpoint_has_progress(checkpoint)
                ):
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
                    _drop_destination_table(request.destination)
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
            )

            with _reconcile_phase_heartbeat(
                mongo,
                job_id,
                processed=int(rows_written or 0),
                total=int(rows_written or 0),
                proof_kind=str((dest_summary or {}).get("checksum_mode") or "full")
                if isinstance(dest_summary, dict)
                else "full",
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
                )
            dest_summary = pii_guard.redact_destination_summary(dest_summary, mappings)
            recon = pii_guard.redact_reconciliation(recon, mappings)
            if not recon.get("passed"):
                mongo.update_job_status(
                    job_id,
                    "failed",
                    error=recon.get("message", "Reconciliation failed"),
                    phase="failed",
                    progress_pct=99,
                    message=recon.get("message"),
                    reconciliation=recon,
                    destination_summary=dest_summary,
                    rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                    coerced_null_rows=int(
                        dest_summary.get("coerced_null_rows", 0) or 0
                    ),
                )
                return TransferResult(
                    success=False,
                    error=recon.get("message", "Reconciliation failed"),
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=rows_written,
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
            )
            _attach_job_rollback_plan(job_id, dest_summary, request)
            _apply_post_load_transforms(request, dest_summary)
            mongo.update_job_status(
                job_id,
                terminal_status,
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get(
                    "message", f"Transferred {rows_written:,} rows successfully"
                ),
                destination_database=dest_summary.get(
                    "database", request.destination.database or ""
                ),
                destination_collection=dest_summary.get("collection")
                or dest_summary.get("table", ""),
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
            block_msg = str(blocked)
            mongo.update_job_status(
                job_id,
                "failed",
                phase="failed",
                error=block_msg,
                message=block_msg,
                records_processed=int(blocked.rows_written or 0),
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
                records_transferred=int(blocked.rows_written or 0),
                destination_summary=dest_summary,
                contract_id=contract_id,
            )
        except Exception as e:
            finalize_contract(contract_id, success=False)
            display, error_details = _fail_runtime_job(
                mongo, job_id, e, lineage=lineage
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=display,
                error_details=error_details,
                operation=request.operation,
                contract_id=contract_id,
            )

    def _create_pending_job(self, request: TransferRequest) -> str:
        self._resolve_saved_connectors(request)
        # Claim-queue / HA: spill file bytes before Mongo serialize so workers
        # can hydrate source_path (never mark requires_file_reupload on fresh submit).
        if request.source.kind == "file":
            from services.transfer_file_staging import hydrate_file_source, persist_file_source

            hydrate_file_source(request)
            if request.source_content:
                persist_file_source(request)
        mongo = get_mongodb_service()
        from services.procedure_source import source_read_mode_of

        source_name = (
            request.source_filename
            or request.source.table
            or request.source.collection
            or "database"
        )
        dest_label = (
            request.destination.collection
            or request.destination.table
            or request.destination.database
            or request.destination.format
            or "destination"
        )
        job_doc: dict[str, Any] = {
            "source_type": request.source.kind,
            "source_name": source_name,
            "name": f"{source_name} → {dest_label}",
            "name_key": f"{source_name} → {dest_label}".strip().casefold(),
            "source_format": request.source.format,
            "source_connector_id": str(getattr(request.source, "connector_id", None) or "").strip() or None,
            "dest_connector_id": str(getattr(request.destination, "connector_id", None) or "").strip() or None,
            "destination_type": request.destination.format,
            "destination_kind": request.destination.kind,
            "destination_database": request.destination.database or "",
            "destination_collection": request.destination.collection
            or request.destination.table
            or "",
            "operation": request.operation,
            "records_processed": 0,
            "total_rows": 0,
            "progress_pct": 0,
            "phase": "queued",
            "message": "Transfer queued",
            "workspace_id": request.workspace_id or "",
            "data_region": request.data_region or "",
            "transfer_request": transfer_request_to_dict(request),
            "sync_mode": request.sync_mode,
            "schema_policy": request.schema_policy,
            "validation_mode": request.validation_mode,
            "source_read_mode": source_read_mode_of(request.source),
            "triggered_by": (request.triggered_by or "").strip(),
            "retry_of": None,
        }
        proof = _mapping_proof_for_request(request)
        if proof:
            job_doc["mapping_proof"] = proof

        # Claim the right to run this transfer before the job exists, so a
        # double submit is told about the in-flight job instead of starting a
        # second writer against the same destination table.
        claim = self._claim_idempotency(request, job_doc)
        if claim is not None and claim.duplicate:
            raise DuplicateTransferSubmission(claim)

        try:
            job_id = mongo.create_transfer_job(job_doc)
        except Exception:
            # Creating the job failed after we reserved the slot. Free it so the
            # next submit is not blocked by a claim that points at nothing.
            if claim is not None:
                try:
                    release = getattr(mongo, "release_job_idempotency", None)
                    if callable(release):
                        release(claim.key, _PENDING_CLAIM_ID)
                except Exception as release_exc:
                    logger.debug(
                        "idempotency release after create failure skipped: %s",
                        release_exc,
                    )
            raise
        if claim is not None:
            # The claim was inserted with a placeholder id because the real one
            # only exists after the job document is written. Point it at the
            # job now so a later release, or a duplicate check, names the right
            # run rather than the placeholder.
            self._bind_idempotency_claim(claim.key, job_id)
        try:
            from services.connector_store import mark_used

            mark_used(job_doc.get("source_connector_id"), job_doc.get("dest_connector_id"))
        except Exception as used_exc:
            logger.debug("mark_used after job create skipped: %s", used_exc)
        return job_id

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

    def _claim_idempotency(
        self, request: TransferRequest, job_doc: dict[str, Any]
    ) -> Any | None:
        """Reserve this transfer's slot. Returns the claim, or None if unavailable."""
        from services.job_idempotency import JobClaim

        key = self._idempotency_key(request)
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
        job_doc["idempotency_key"] = key
        return JobClaim(
            key=key,
            job_id="",
            acquired=acquired,
            existing_job_id=existing_id,
            existing_status=existing_status,
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
            key = str(job.get("idempotency_key") or "")
            if not key:
                return
            release = getattr(mongo, "release_job_idempotency", None)
            if callable(release):
                release(key, job_id)
        except Exception as exc:
            logger.debug("idempotency release skipped for %s: %s", job_id, exc)

    def _read_source(
        self, request: TransferRequest
    ) -> tuple[list, list[str], dict[str, str]]:
        if request.source.kind == "file":
            from services.transfer_file_staging import hydrate_file_source

            hydrate_file_source(request)
            content = request.source_content or b""
            if not content and request.source_path:
                from pathlib import Path as _Path

                p = _Path(request.source_path)
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
                    cursor_after = bound.scope.watermark
            except ValueError:
                # Cursor-identity refusal is owned by execute_tracked after the
                # read; do not turn it into a retried source error here.
                cursor_column = ""
                cursor_after = None
            return read_source_database(
                request.source,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
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
