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
        source_filename=str(payload.get("source_filename") or ""),
        source_path=source_path,
        source_object_uri=str(payload.get("source_object_uri") or ""),
        skip_preflight=False,
        triggered_by="data-pilot",
    )
    return request
