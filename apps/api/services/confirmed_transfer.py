"""Build the TransferRequest a confirmed ack is allowed to run.

Database acks keep requiring a connector on each end. A file ack may name an
uploaded dataset instead of a source connector. ``skip_preflight`` is forced
off here so a rewritten ack cannot bypass Validate.
"""

from __future__ import annotations

from typing import Any

from services.dataset_file import assert_dataset_file
from src.transfer.models import EndpointConfig, TransferRequest

_SOURCE_KINDS = {"database", "file"}
_DEST_KINDS = {"database", "file_export"}


def _kind(raw: dict[str, Any], default: str) -> str:
    token = str(raw.get("kind") or default).strip().lower()
    return token or default


def transfer_request_from_ack(payload: dict[str, Any]) -> TransferRequest:
    """Raise ``ValueError`` when the ack cannot name a safe transfer."""
    src = dict(payload.get("source") or {})
    dst = dict(payload.get("destination") or {})
    src_kind = _kind(src, "database")
    dst_kind = _kind(dst, "database")
    if src_kind not in _SOURCE_KINDS:
        raise ValueError(f"Unsupported source kind: {src_kind}")
    if dst_kind not in _DEST_KINDS:
        raise ValueError(f"Unsupported destination kind: {dst_kind}")
    if src_kind == "database" and not str(src.get("connector_id") or "").strip():
        raise ValueError("Transfer approval is missing its source connector.")
    if dst_kind == "database" and not str(dst.get("connector_id") or "").strip():
        raise ValueError("Transfer approval is missing its destination connector.")

    source_path = str(payload.get("source_path") or src.get("path") or "").strip()
    extra = dict(src.get("extra") or {}) if isinstance(src.get("extra"), dict) else {}
    file_id = str(extra.get("file_id") or payload.get("source_file_id") or "").strip()
    if src_kind == "file":
        if source_path:
            source_path = str(assert_dataset_file(source_path))
        elif not file_id:
            raise ValueError("File transfer approval is missing the uploaded file.")
        if file_id:
            extra["file_id"] = file_id
        src = {**src, "extra": extra}

    request = TransferRequest(
        source=EndpointConfig.from_dict(src_kind, src),
        destination=EndpointConfig.from_dict(dst_kind, dst),
        mappings=list(payload.get("mappings") or []),
        column_types=dict(payload.get("column_types") or {}),
        sync_mode=str(payload.get("sync_mode") or "full_refresh_append"),
        schema_policy=str(payload.get("schema_policy") or "manual_review"),
        validation_mode=str(payload.get("validation_mode") or "balanced"),
        limit=max(0, int(payload.get("limit") or 0)),
        source_filter=dict(payload.get("source_filter") or {}),
        stream_contracts=list(payload.get("stream_contracts") or []),
        # A multi-step column transform staged from chat is a recipe; Execute
        # runs it only under the hash Validate approved.
        shape_recipe=dict(payload.get("shape_recipe") or {}),
        approved_shape_recipe_hash=str(payload.get("approved_shape_recipe_hash") or ""),
        source_filename=str(payload.get("source_filename") or ""),
        source_path=source_path,
        source_object_uri=str(payload.get("source_object_uri") or ""),
        # The declared read window (an uploaded file's encoding): Execute
        # reads the bytes the way staging did, strictly.
        read_options=dict(payload.get("read_options") or {}),
        skip_preflight=False,
        triggered_by="data-pilot",
        # The operator's PII acknowledgement from staging: Execute re-runs
        # Validate with it, and the job keeps the approver + reason for audit.
        compliance_acknowledged=bool(payload.get("compliance_acknowledged")),
        # Staging clears schema drift when the operator attests. Confirm used
        # to drop the flag, so Execute re-ran Validate without it and job
        # governance read schema_drift as false (R8-002).
        schema_drift_acknowledged=bool(payload.get("schema_drift_acknowledged")),
        fk_risk_acknowledged=bool(payload.get("fk_risk_acknowledged")),
        # The operator's approval of the disclosed destination DDL (the
        # upsert's unique index, D101). Execute's preflight and the writer
        # both gate on it; dropping it here re-refused the approved run.
        ddl_changes_acknowledged=bool(payload.get("ddl_changes_acknowledged")),
        acknowledgment_actor=str(payload.get("acknowledgment_actor") or "").strip(),
        acknowledgment_reason=str(payload.get("acknowledgment_reason") or "").strip(),
    )
    stamp_schema_drift_ack(request)
    return request


def stamp_schema_drift_ack(request: TransferRequest) -> None:
    """A bare boolean still has an actor and a reason on the job.

    MCP staging sets ``schema_drift_acknowledged`` without the Studio
    signature. Execute honors the boolean; governance and the audit trail
    also need a name. Hard-breaking drift is not cleared by this stamp.
    """
    if not bool(getattr(request, "schema_drift_acknowledged", False)):
        return
    if not str(getattr(request, "acknowledgment_actor", "") or "").strip():
        request.acknowledgment_actor = "operator"
    if not str(getattr(request, "acknowledgment_reason", "") or "").strip():
        request.acknowledgment_reason = "Accept drift for this run"


def apply_stored_schema_drift_ack(request: TransferRequest, job: dict[str, Any] | None) -> bool:
    """Restore an ack the job document has and the in-memory request lost.

    Returns True when the nested transfer request did not already carry the
    flag, so the caller can write it back before Execute. Either side being
    true is enough: staging, confirm, and the job root agree.
    """
    doc = job or {}
    stored_req = doc.get("transfer_request") if isinstance(doc.get("transfer_request"), dict) else {}
    stored = bool(
        stored_req.get("schema_drift_acknowledged") or doc.get("schema_drift_acknowledged")
    )
    if not stored and not bool(getattr(request, "schema_drift_acknowledged", False)):
        return False
    request.schema_drift_acknowledged = True
    if not str(getattr(request, "acknowledgment_actor", "") or "").strip():
        request.acknowledgment_actor = (
            str(
                stored_req.get("acknowledgment_actor")
                or doc.get("acknowledgment_actor")
                or "operator"
            ).strip()
            or "operator"
        )
    if not str(getattr(request, "acknowledgment_reason", "") or "").strip():
        request.acknowledgment_reason = (
            str(
                stored_req.get("acknowledgment_reason")
                or doc.get("acknowledgment_reason")
                or "Accept drift for this run"
            ).strip()
            or "Accept drift for this run"
        )
    return not bool(stored_req.get("schema_drift_acknowledged"))


def mapped_sources_missing_from(
    request: TransferRequest, live_columns: set[str] | list[str]
) -> list[str]:
    """Mapped source columns the live source no longer has (case-insensitive)."""
    from services.mapping_constraints import write_mappings

    live = {str(c).strip().casefold() for c in live_columns if str(c).strip()}
    if not live:
        return []
    missing: list[str] = []
    for mapping in write_mappings(list(request.mappings or [])):
        source = str(mapping.get("source") or "").strip()
        if source and source.casefold() not in live and source not in missing:
            missing.append(source)
    return missing


def assert_source_schema_unchanged(request: TransferRequest) -> None:
    """Re-read the source's columns at Confirm and refuse a stale approval (D21).

    The approval mapped columns as they were when it was staged. A column
    dropped or renamed since then used to surface only as a raw driver error
    mid-run. An unreadable source is left to Execute's own preflight, which
    reports connectivity itself.
    """
    source = request.source
    if source.kind != "database" or not (source.table or source.collection):
        return
    from services.procedure_source import is_callable_source

    if is_callable_source(source):
        return
    from src.transfer.adapters import resolve_endpoint
    from src.transfer.endpoint_intelligence import introspect_endpoint

    try:
        resolved = resolve_endpoint(source, workspace_id=request.workspace_id)
        info = introspect_endpoint(resolved) or {}
    except Exception:  # noqa: BLE001 - connectivity is Execute's to report
        return
    live = {str(k) for k in (info.get("schema") or {})} | {
        str(c) for c in (info.get("columns") or []) if isinstance(c, str)
    }
    missing = mapped_sources_missing_from(request, live)
    if missing:
        listed = ", ".join(missing[:8])
        raise ValueError(
            f"The source changed since this transfer was staged: mapped column(s) "
            f"{listed} no longer exist on {source.table or source.collection}. "
            "Re-stage the transfer so the mapping is rebuilt from the current "
            "schema."
        )
