"""Discard-staging binding for ``POST /transfer/{job_id}/rollback/execute``.

The route must drop the staging table on the destination the run wrote to.
The run is stored on the job under ``transfer_request`` (``job_submission``)
with its endpoint secrets encrypted; older/other paths stored ``request`` or a
bare ``destination``. Reading only ``request`` rebuilt an endpoint with no
engine, connector or schema and the drop failed closed on every job.
"""

from __future__ import annotations

from typing import Any, Mapping


def job_destination_config(job: Mapping[str, Any] | None) -> dict[str, Any]:
    """The destination endpoint dict of the job's run, secrets decrypted."""
    if not isinstance(job, Mapping):
        return {}
    raw: dict[str, Any] = {}
    for key in ("transfer_request", "request"):
        req = job.get(key)
        if isinstance(req, Mapping) and isinstance(req.get("destination"), Mapping):
            raw = dict(req["destination"])
            break
    if not raw and isinstance(job.get("destination"), Mapping):
        raw = dict(job["destination"])
    if not raw:
        return {}
    try:
        from src.transfer.models import _decrypt_endpoint_secrets

        return _decrypt_endpoint_secrets(raw)
    except Exception:  # noqa: BLE001 — undecryptable secret: the drop fails closed
        return raw


def drop_staging_table(destination: Any, staging_table: str) -> None:
    """Drop ``staging_table`` next to the run's destination; raise with the reason.

    ``pre_ingestion_staging._drop_table`` swallows the cause and answers
    ``False``, which the route could only report as "fail closed". The operator
    needs to see why (no engine on the job, permission denied, ...).
    """
    from dataclasses import replace

    from connectors.table_manager import drop_table
    from src.transfer.adapters import resolve_connector_config, resolve_dest_table
    from src.transfer.connector_capabilities import resolve_driver_type

    db_type = resolve_driver_type(getattr(destination, "format", "") or "")
    if not db_type:
        raise StagingDiscardError(
            "The job does not record its destination engine — cannot reach the "
            "staging table."
        )
    ep = replace(destination, table=staging_table, collection=staging_table)
    cfg = resolve_connector_config(ep)
    table_name = resolve_dest_table(db_type, ep)
    try:
        dropped = drop_table(db_type, cfg, table_name, cfg.get("schema"))
    except Exception as exc:  # noqa: BLE001 — reported to the operator, redacted by the route
        raise StagingDiscardError(f"The destination refused the drop: {exc}") from exc
    if not dropped:
        raise StagingDiscardError(
            f"Dropping a table is not supported for destination '{db_type}'."
        )


class StagingDiscardError(RuntimeError):
    """Why the staging table could not be dropped (operator-facing)."""
