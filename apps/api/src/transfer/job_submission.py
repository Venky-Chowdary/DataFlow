"""Pending transfer job creation."""

from __future__ import annotations

from typing import Any


def create_pending_job(engine: Any, request: Any) -> str:
    from src.transfer import engine as engine_module

    engine._resolve_saved_connectors(request)
    if request.source.kind == "file":
        from services.transfer_file_staging import (
            hydrate_file_source,
            persist_file_source,
        )

        hydrate_file_source(request)
        if request.source_content:
            persist_file_source(request)
    mongo = engine_module.get_mongodb_service()
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
        "source_connector_id": (
            str(getattr(request.source, "connector_id", None) or "").strip() or None
        ),
        "dest_connector_id": (
            str(getattr(request.destination, "connector_id", None) or "").strip()
            or None
        ),
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
        "transfer_request": engine_module.transfer_request_to_dict(request),
        "sync_mode": request.sync_mode,
        "schema_policy": request.schema_policy,
        "validation_mode": request.validation_mode,
        "source_read_mode": source_read_mode_of(request.source),
        "triggered_by": (request.triggered_by or "").strip(),
        "retry_of": None,
        "parent_job_id": str(getattr(request, "parent_job_id", "") or "").strip(),
        # Job-root copy. get_job reads this when a rewrite of transfer_request
        # drops the nested flag (R10-001 / R8-002).
        "schema_drift_acknowledged": bool(
            getattr(request, "schema_drift_acknowledged", False)
        ),
        "acknowledgment_actor": str(
            getattr(request, "acknowledgment_actor", "") or ""
        ).strip(),
        "acknowledgment_reason": str(
            getattr(request, "acknowledgment_reason", "") or ""
        ).strip(),
    }
    proof = engine_module._mapping_proof_for_request(request)
    if proof:
        job_doc["mapping_proof"] = proof

    claim = engine._claim_idempotency(request, job_doc)
    request_fields = engine_module.lifecycle_events.job_requested_fields(request)
    if claim is not None and claim.duplicate:
        engine_module.lifecycle_events.emit_job_requested_skipped(
            engine_module.logger, request_fields
        )
        raise engine_module.DuplicateTransferSubmission(claim)

    try:
        job_id = mongo.create_transfer_job(job_doc)
    except Exception:
        if claim is not None:
            try:
                release = getattr(mongo, "release_job_idempotency", None)
                if callable(release):
                    release(claim.key, engine_module._PENDING_CLAIM_ID)
            except Exception as release_exc:
                engine_module.logger.debug(
                    "idempotency release after create failure skipped: %s",
                    release_exc,
                )
        raise
    if claim is not None:
        engine._bind_idempotency_claim(claim.key, job_id)
    try:
        from services.connector_store import mark_used

        mark_used(
            job_doc.get("source_connector_id"), job_doc.get("dest_connector_id")
        )
    except Exception as used_exc:
        engine_module.logger.debug("mark_used after job create skipped: %s", used_exc)
    engine_module.lifecycle_events.emit_job_requested_succeeded(
        engine_module.logger,
        str(job_id),
        bool(job_doc.get("triggered_by")),
        request_fields,
    )
    return job_id
