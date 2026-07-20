"""Resolve CDC incremental-snapshot context from a transfer job.

Operators enqueue snapshots from Theater/Jobs without inventing ``source_key``.
Fingerprint must match what CDC adapters use (``connection_fingerprint``).
"""

from __future__ import annotations

from typing import Any

from services.cdc_schema_history import connection_fingerprint


def _endpoint_dict(job: dict[str, Any]) -> dict[str, Any]:
    req = job.get("transfer_request") if isinstance(job.get("transfer_request"), dict) else {}
    src = req.get("source") if isinstance(req.get("source"), dict) else {}
    return src


def _stream_defaults(job: dict[str, Any]) -> tuple[str, str]:
    """Return (table, primary_key) defaults from the job request."""
    req = job.get("transfer_request") if isinstance(job.get("transfer_request"), dict) else {}
    src = _endpoint_dict(job)
    table = str(src.get("table") or src.get("collection") or "").strip()
    contracts = req.get("stream_contracts") or []
    primary_key = "id"
    if isinstance(contracts, list) and contracts:
        first = contracts[0] if isinstance(contracts[0], dict) else {}
        primary_key = str(first.get("primary_key") or primary_key)
        if not table:
            table = str(first.get("name") or first.get("table") or "").strip()
    if not table:
        table = str(job.get("source_table") or job.get("source_collection") or "").strip()
    return table, primary_key


def resolve_job_cdc_snapshot_context(job: dict[str, Any]) -> dict[str, Any]:
    """Build source_key(s), table, primary_key for job-scoped snapshot ops.

    Returns keys tried in order: connector fingerprint first (when connector_id
    is present), then host/port/db fingerprint — so list/cancel still finds
    signals enqueued against either form.
    """
    from src.transfer.models import transfer_request_from_dict
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    req_raw = job.get("transfer_request") if isinstance(job.get("transfer_request"), dict) else {}
    if not req_raw:
        raise ValueError("Job has no transfer_request — cannot resolve CDC source")

    try:
        treq = transfer_request_from_dict(req_raw)
    except Exception as exc:
        raise ValueError(f"Invalid transfer_request on job: {exc}") from exc

    sync_mode = str(job.get("sync_mode") or treq.sync_mode or "").lower()
    if sync_mode and sync_mode not in {"cdc", "cdc_log", "change_data_capture"}:
        # Allow enqueue when job was CDC even if status field drifted.
        if not (job.get("cdc_plugin") or job.get("watermark") or job.get("cdc_delivery")):
            raise ValueError(
                f"Incremental snapshots require a CDC job (sync_mode={sync_mode or 'unknown'})"
            )

    source = treq.source
    workspace_id = str(job.get("workspace_id") or "") or None
    cfg = resolve_connector_config(source, workspace_id=workspace_id)
    # Stamp connector_id so fingerprint matches CDC adapters that read cfg.
    if source.connector_id and not cfg.get("connector_id"):
        cfg["connector_id"] = source.connector_id

    driver = resolve_driver_type(str(cfg.get("type") or source.format or ""))
    cfg["type"] = driver or cfg.get("type") or source.format

    keys: list[str] = []
    connector_id = str(cfg.get("connector_id") or source.connector_id or "").strip()
    if connector_id:
        keys.append(connection_fingerprint(cfg, connector_id=connector_id))
    host_key = connection_fingerprint({**cfg, "type": cfg.get("type") or driver}, connector_id="")
    if host_key not in keys:
        keys.append(host_key)

    table, primary_key = _stream_defaults(job)
    return {
        "source_keys": keys,
        "source_key": keys[0],
        "table": table,
        "primary_key": primary_key,
        "driver": driver,
        "connector_id": connector_id or None,
        "cfg_summary": {
            "host": cfg.get("host"),
            "port": cfg.get("port"),
            "database": cfg.get("database"),
            "type": cfg.get("type"),
        },
        "honesty": (
            "Incremental snapshot backfills PK-ordered chunks interleaved with CDC "
            "(stream-wins within a chunk). Delivery remains at-least-once upsert — "
            "not exactly-once, not transfer undo."
        ),
    }


def list_signals_for_job(job: dict[str, Any], *, status: str = "") -> list[dict[str, Any]]:
    from services.cdc_incremental_snapshot import list_signals

    ctx = resolve_job_cdc_snapshot_context(job)
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for key in ctx["source_keys"]:
        for sig in list_signals(key, status=status):
            if sig.id in seen:
                continue
            seen.add(sig.id)
            row = sig.to_dict()
            row["resolved_source_key"] = key
            out.append(row)
    out.sort(key=lambda r: float(r.get("created_at") or 0), reverse=True)
    return out


def request_snapshot_for_job(
    job: dict[str, Any],
    *,
    table: str = "",
    primary_key: str = "",
    chunk_size: int = 1000,
) -> dict[str, Any]:
    from services.cdc_incremental_snapshot import request_incremental_snapshot

    ctx = resolve_job_cdc_snapshot_context(job)
    tbl = (table or ctx["table"] or "").strip()
    if not tbl:
        raise ValueError("table is required (job has no source table/collection)")
    pk = (primary_key or ctx["primary_key"] or "id").strip() or "id"
    sig = request_incremental_snapshot(
        ctx["source_key"],
        tbl,
        primary_key=pk,
        chunk_size=chunk_size,
    )
    row = sig.to_dict()
    row["resolved_source_key"] = ctx["source_key"]
    row["context"] = {
        "source_keys": ctx["source_keys"],
        "driver": ctx["driver"],
        "honesty": ctx["honesty"],
    }
    return row
