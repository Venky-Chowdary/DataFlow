"""Plan blockers that Execute re-checks with the same function (R8).

A plan that says ``safe_to_start=false`` is a contract: Execute must refuse the
same request for the same reason before anything is read or written, on every
destination and on every Execute path (streaming, buffered, file streaming,
multi-stream). Each refusal here is asked twice, by the preflight gate that
Validate / plan_transfer / start_transfer / create_schedule show, and by
Execute's up-front request check (``source_filter_gate.request_refusal``):

* ``g_shape_route`` (D417): a pre-load transform recipe on a route that merges
  against stored history (mirror, cdc, scd2 without its contract step). The
  streaming runner refused it; the buffered runner that SQL Server, Oracle and
  file destinations take applied it anyway, so the plan said "blocked" and the
  run wrote.
* ``g_dest_schema_unread`` (D431): Map could not read the destination schema,
  so its rows are ``pending_dest_schema`` with no destination type. The plan
  reported ``safe_to_start=true`` beside them and the run ended in a raw
  database error or "No column mappings".
* ``g9_sync_contract`` (D404/D422): a Kafka incremental read paged on a payload
  column. The plan refused it; a schedule saved anyway failed every run.
* ``g_dest_schema_missing`` (D431 AC-3, :mod:`services.dest_namespace`): the
  configured Oracle owner / SQL Server schema does not exist, so the run
  failed with a raw ORA-01918 after a green plan.

Schedules ask the same function when they are saved and before each run
(:func:`schedule_plan_refusal`, D422), so a schedule every run would refuse is
never saved, and one whose route gained a blocker parks before a job exists.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

SHAPE_ROUTE_GATE_ID = "g_shape_route"
SYNC_CONTRACT_GATE_ID = "g9_sync_contract"
DEST_SCHEMA_UNREAD_GATE_ID = "g_dest_schema_unread"
PENDING_DEST_SCHEMA = "pending_dest_schema"


class BlockerRefusal(str):
    """An Execute refusal that names the plan blocker it repeats."""

    gate_id: str

    def __new__(cls, message: str, gate_id: str) -> "BlockerRefusal":
        obj = super().__new__(cls, message)
        obj.gate_id = gate_id
        return obj


def _as_dict(row: Any) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    return getattr(row, "__dict__", None) or {}


def pending_dest_schema_columns(mappings: Iterable[Any] | None) -> list[str]:
    """Written columns whose destination type Map could not read, in Map order."""
    from services.mapping_constraints import is_intentional_omit

    out: list[str] = []
    for raw in mappings or []:
        m = _as_dict(raw)
        if not m or is_intentional_omit(dict(m)):
            continue
        if str(m.get("assignment_strategy") or "") != PENDING_DEST_SCHEMA:
            continue
        name = str(m.get("target") or m.get("source") or "").strip()
        if name and name not in out:
            out.append(name)
    return out


def dest_schema_unread_refusal(mappings: Iterable[Any] | None) -> str:
    """Why these Map rows cannot be written, or ``""`` (the same text everywhere)."""
    cols = pending_dest_schema_columns(mappings)
    if not cols:
        return ""
    sample = ", ".join(cols[:5]) + (f" (+{len(cols) - 5} more)" if len(cols) > 5 else "")
    return (
        f"The destination schema could not be read when the columns were mapped, so "
        f"{len(cols)} column(s) have no destination type: {sample}. Nothing was "
        "created or written. Check that the destination connection and its schema "
        "are reachable, then plan the transfer again."
    )


def dest_schema_unread_gate(mappings: Iterable[Any] | None) -> dict[str, Any] | None:
    """Preflight gate: block a plan whose Map rows wait on an unread destination."""
    refusal = dest_schema_unread_refusal(mappings)
    if not refusal:
        return None
    return {
        "id": DEST_SCHEMA_UNREAD_GATE_ID,
        "status": "block",
        "message": refusal,
        "duration_ms": 0,
        "details": {
            "kind": "destination_schema_unread",
            "columns": pending_dest_schema_columns(mappings)[:20],
            "retriable": True,
        },
    }


def _effective_sync_modes(sync_mode: str | None, stream_contracts: Iterable[Any] | None) -> list[str]:
    from services.sync_cursor import resolve_effective_sync_mode

    selected = [
        _as_dict(c) for c in (stream_contracts or [])
        if _as_dict(c) and _as_dict(c).get("selected", True)
    ]
    if not selected:
        return [resolve_effective_sync_mode(sync_mode).lower()]
    modes: list[str] = []
    for c in selected:
        mode = resolve_effective_sync_mode(sync_mode, str(c.get("sync_mode") or "")).lower()
        if mode not in modes:
            modes.append(mode)
    return modes


def recipe_route_refusal(
    shape_recipe: Any, sync_mode: str | None, stream_contracts: Iterable[Any] | None = None
) -> str:
    """Execute's refusal for a pre-load recipe on this route (per selected stream), or ``""``."""
    from services.history_route_recipe import history_recipe_refusal

    if not shape_recipe:
        return ""
    for mode in _effective_sync_modes(sync_mode, stream_contracts):
        refusal = history_recipe_refusal(shape_recipe, mode)
        if refusal:
            return refusal
    return ""


def kafka_cursor_refusal(
    source_format: Any, sync_mode: str | None, stream_contracts: Iterable[Any] | None
) -> str:
    """The g9 refusal of a Kafka column cursor (D404), or ``""``.

    The plan's g9 gate and Execute both call this, so each selected stream is
    judged on the mode its run uses: the stream's own mode when it declares one,
    else the request's (``resolve_effective_sync_mode``, as the engine resolves
    it). A full-refresh stream under an incremental request is not paged.
    """
    from services.copy_kafka_common import kafka_family_name

    if kafka_family_name(str(source_format or "")) != "kafka":
        return ""
    from services.preflight_cursor_gate import MODES_REQUIRING_CURSOR
    from services.sync_cursor import contract_cursor_field, resolve_effective_sync_mode
    from src.transfer.engine_kafka import kafka_unpageable_cursor

    selected = [
        _as_dict(c) for c in (stream_contracts or [])
        if _as_dict(c) and _as_dict(c).get("selected", True)
    ]
    for c in selected:
        mode = resolve_effective_sync_mode(sync_mode, str(c.get("sync_mode") or ""))
        if mode not in MODES_REQUIRING_CURSOR:
            continue
        refusal = kafka_unpageable_cursor(source_format, contract_cursor_field(c))
        if refusal:
            return refusal
    return ""


def _request_mapping_sets(request: Any) -> list[list[Any]]:
    """The Map rows each stream of this request writes."""
    from services.target_name_legacy import selected_streams

    own = list(getattr(request, "mappings", None) or [])
    streams = selected_streams(request)
    if not streams:
        return [own]
    sets: list[list[Any]] = []
    for contract in streams:
        rows = contract.get("mappings")
        sets.append(list(rows) if isinstance(rows, list) and rows else own)
    return sets


def execute_plan_refusal(request: Any) -> str:
    """The plan blocker this request still carries, as Execute's refusal, or ``""``.

    Checked before any read, DDL or write, whether or not the caller skipped
    preflight: the plan computed the same verdict from the same inputs.
    """
    recipe = recipe_route_refusal(
        getattr(request, "shape_recipe", None),
        getattr(request, "sync_mode", None),
        getattr(request, "stream_contracts", None),
    )
    if recipe:
        return BlockerRefusal(recipe, SHAPE_ROUTE_GATE_ID)
    kafka = kafka_cursor_refusal(
        getattr(getattr(request, "source", None), "format", ""),
        getattr(request, "sync_mode", None),
        getattr(request, "stream_contracts", None),
    )
    if kafka:
        return BlockerRefusal(kafka, SYNC_CONTRACT_GATE_ID)
    for rows in _request_mapping_sets(request):
        unread = dest_schema_unread_refusal(rows)
        if unread:
            return BlockerRefusal(unread, DEST_SCHEMA_UNREAD_GATE_ID)
    missing = _destination_namespace_refusal(getattr(request, "destination", None))
    if missing:
        from services.dest_namespace import MISSING_NAMESPACE_GATE_ID

        return BlockerRefusal(missing, MISSING_NAMESPACE_GATE_ID)
    return ""


def _destination_namespace_refusal(destination: Any) -> str:
    """D431 AC-3 at Execute: the schema the run would create its table in is missing.

    The same probe Validate's gate runs, before any DDL: an existing table cannot
    live in a missing schema, so this is a refusal whatever the table's state.
    """
    if destination is None or str(getattr(destination, "kind", "") or "database") != "database":
        return ""
    from services.dest_namespace import namespace_refusal

    try:
        from src.transfer.adapters import resolve_connector_config

        cfg = resolve_connector_config(destination)
    except Exception:  # noqa: BLE001 - unresolvable here is the connector gate's finding
        return ""
    label = str(getattr(destination, "host", "") or "") or "the destination"
    return namespace_refusal(cfg, label)


def refusal_details(refusal: Any) -> dict[str, Any]:
    """``error_details`` for a job refused up front: the blocker id when there is one."""
    gate_id = str(getattr(refusal, "gate_id", "") or "")
    if not gate_id:
        return {}
    return {
        "reason": "plan_blocker",
        "blocker_id": gate_id,
        "retriable": gate_id == DEST_SCHEMA_UNREAD_GATE_ID,
        "rows_written": 0,
    }


def _schedule_source_format(payload: Mapping[str, Any]) -> str:
    connector_id = str(payload.get("source_connector_id") or "").strip()
    if not connector_id:
        return ""
    try:
        from services.connector_store import get_connector

        conn = get_connector(connector_id, workspace_id=str(payload.get("workspace_id") or "") or None)
    except Exception:  # noqa: BLE001 - the store's own connector check reports a missing one
        return ""
    data = conn.to_dict() if conn is not None and hasattr(conn, "to_dict") else {}
    return str(data.get("type") or data.get("format") or "").strip().lower()


def schedule_plan_refusal(payload: Mapping[str, Any]) -> str:
    """The plan blocker a schedule would carry into every run, or ``""`` (D422).

    Judged on the request the schedule replays: its sync mode, stream contracts
    (the schedule's ``cursor_column`` is the primary stream's cursor), recipe
    and stored Map rows, with the same function Execute asks.
    """
    from types import SimpleNamespace

    sync_mode = str(payload.get("sync_mode") or "")
    contracts = [dict(_as_dict(c)) for c in (payload.get("stream_contracts") or []) if _as_dict(c)]
    cursor = str(payload.get("cursor_column") or "").strip()
    if not contracts and cursor:
        contracts = [{
            "name": str(payload.get("source_table") or ""), "sync_mode": sync_mode,
            "cursor_field": cursor, "selected": True,
        }]
    request = SimpleNamespace(
        source=SimpleNamespace(format=_schedule_source_format(payload)),
        sync_mode=sync_mode,
        stream_contracts=contracts,
        shape_recipe=payload.get("shape_recipe"),
        mappings=list(payload.get("mappings") or []),
        destination=None,
    )
    return execute_plan_refusal(request)
