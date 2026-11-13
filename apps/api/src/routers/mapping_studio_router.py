"""Map step remapping: destination columns, live type grades, auto-map, memory.

Map lets the operator point any source column at any existing destination
column (``order_id → legacy_ref``, ``first_name → given_name``). These routes
give it what that needs without a second rulebook:

``POST /destination-columns``  the destination table's columns with type,
    nullability, default, key and the "required" verdict G14 enforces — read
    off the event loop under the connector deadline.
``POST /type-compatibility``   safe / lossy / incompatible per pair, graded by
    the conversion contract and fidelity verdict Validate uses.
``POST /auto-map``             one-to-one name suggestions (exact → normalized
    → learned → synonym) with a confidence, for the operator to preview.
``GET|POST|DELETE /memory``    the workspace's confirmed pairs (the learned tier).

Every route is workspace-scoped: a saved connector is authorized against its
own workspace before it is dialled, and the memory read or written is the
caller's workspace's only.
"""

from __future__ import annotations

import logging
from typing import Any, Literal, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

from services.mapping_type_compat import MAX_PAIRS
from services.safe_errors import safe_http_exception
from services.workspace_access import (
    actor_email,
    assert_connector_accessible,
    resolve_read_workspace,
    resolve_write_workspace,
)

from .transfer_router import EndpointDTO

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/transfer/mapping", tags=["Mapping Studio"])

#: The connector-deadline tool name: a catalog read dials the customer system.
_CATALOG_TOOL = "introspect_connector_schema"
_MAX_COLUMNS = 5000
_NAME = Field(..., min_length=1, max_length=256)


class DestinationColumnsRequest(BaseModel):
    destination: EndpointDTO
    schema_policy: str = Field("", max_length=64)


class TypePair(BaseModel):
    source_type: str = Field("", max_length=200)
    target_type: str = Field("", max_length=200)
    transform: str = Field("none", max_length=120)


class TypeCompatibilityRequest(BaseModel):
    pairs: list[TypePair] = Field(default_factory=list, max_length=MAX_PAIRS)
    dest_type: str = Field("", max_length=64)
    dest_table_exists: Optional[bool] = True


class NamedColumn(BaseModel):
    name: str = _NAME
    type: str = Field("", max_length=200)


class MappedPair(BaseModel):
    source: str = Field("", max_length=256)
    target: str = Field("", max_length=256)


class AutoMapRequest(BaseModel):
    source_columns: list[NamedColumn] = Field(..., max_length=_MAX_COLUMNS)
    destination_columns: list[NamedColumn] = Field(..., max_length=_MAX_COLUMNS)
    #: Pairs the operator already chose: kept, never re-suggested ("remaining").
    mapped: list[MappedPair] = Field(default_factory=list, max_length=_MAX_COLUMNS)
    mode: Literal["all", "remaining"] = "remaining"
    source_table: str = Field("", max_length=256)
    dest_table: str = Field("", max_length=256)
    dest_type: str = Field("", max_length=64)
    use_learned: bool = True


class MemoryRecordRequest(BaseModel):
    pairs: list[MappedPair] = Field(..., min_length=1, max_length=2000)


def _workspace(request: Request, header: str, *, write: bool) -> str:
    return (
        resolve_write_workspace(request, header)
        if write
        else resolve_read_workspace(request, header)
    )


@router.post("/destination-columns")
async def destination_columns(body: DestinationColumnsRequest, request: Request) -> dict[str, Any]:
    """The destination table's column catalog for the Map picker and panel."""
    from src.ai.copilot.tool_deadline import deadline_seconds, run_off_event_loop
    from src.transfer.models import EndpointConfig

    from services.mapping_dest_catalog import (
        build_destination_column_catalog,
        read_destination_column_catalog,
    )

    # A connector id expands into stored host and credentials: authorize it
    # against its own workspace before anything is dialled (404 elsewhere).
    assert_connector_accessible(request, body.destination.connector_id)
    endpoint = EndpointConfig.from_dict(
        body.destination.kind, body.destination.model_dump(by_alias=True)
    )
    policy = body.schema_policy
    if endpoint.kind != "database":
        # A file export is written fresh: there is no existing column to pick.
        return build_destination_column_catalog(
            {"connected": True, "table_exists": False},
            dest_type=str(endpoint.format or endpoint.kind or ""),
            schema_policy=policy,
        )
    limit = deadline_seconds(_CATALOG_TOOL)

    def _timed_out(_message: str) -> Any:
        # The executor's message names host:port; the operator gets a fixed one.
        raise HTTPException(
            status_code=504,
            detail=(
                f"The destination did not answer within {int(limit)} s. Check that it is "
                "reachable from Datawrap, then reload the destination columns."
            ),
        )

    try:
        return await run_off_event_loop(
            _CATALOG_TOOL,
            lambda: read_destination_column_catalog(endpoint, schema_policy=policy),
            _timed_out,
            seconds=limit,
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 — driver errors carry paths and hosts
        raise safe_http_exception(
            exc,
            event="mapping.destination_columns.errored",
            public_message="Could not read the destination's columns.",
            status_code=502,
            logger=logger,
        ) from exc


@router.post("/type-compatibility")
def type_compatibility(body: TypeCompatibilityRequest) -> dict[str, Any]:
    """Grade each source/target type pair with Validate's own rules."""
    from services.mapping_type_compat import grade_type_pairs

    results = grade_type_pairs(
        [p.model_dump() for p in body.pairs],
        dest_db=body.dest_type,
        dest_table_exists=body.dest_table_exists,
    )
    return {"results": results, "rules": "conversion_contract+mapping_fidelity"}


@router.post("/auto-map")
def auto_map(
    body: AutoMapRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    """Preview one-to-one suggestions; nothing is applied or stored here."""
    from services.mapping_memory import learned_index
    from services.mapping_name_match import suggest_assignments
    from services.mapping_type_compat import grade_type_pairs

    ws = _workspace(request, workspace_id, write=False)
    learned = learned_index(workspace_id=ws) if body.use_learned else {}
    locked = [p for p in body.mapped if p.source and p.target] if body.mode == "remaining" else []
    suggestions = suggest_assignments(
        [c.name for c in body.source_columns],
        [c.name for c in body.destination_columns],
        locked_sources=[p.source for p in locked],
        locked_targets=[p.target for p in locked],
        learned=learned,
        source_table=body.source_table,
        dest_table=body.dest_table,
    )
    src_types = {c.name: c.type for c in body.source_columns}
    dst_types = {c.name: c.type for c in body.destination_columns}
    grades = grade_type_pairs(
        [
            {"source_type": src_types.get(s["source"], ""), "target_type": dst_types.get(s["target"], "")}
            for s in suggestions
        ],
        dest_db=body.dest_type,
    )
    for suggestion, grade in zip(suggestions, grades):
        suggestion["compat"] = {"verdict": grade["verdict"], "reasons": grade["reasons"]}
    return {
        "suggestions": suggestions,
        "learned_pairs_used": sum(1 for s in suggestions if s["kind"] == "learned"),
        "mode": body.mode,
    }


@router.get("/memory")
def list_memory(
    request: Request,
    limit: int = 500,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    from services.mapping_memory import list_learned

    ws = _workspace(request, workspace_id, write=False)
    return {"pairs": list_learned(workspace_id=ws, limit=max(1, min(limit, 5000)))}


@router.post("/memory")
def record_memory(
    body: MemoryRecordRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    """Remember pairs the operator confirmed (Approve on Map)."""
    from services.audit_log import append_audit_event
    from services.mapping_memory import record_confirmed

    ws = _workspace(request, workspace_id, write=True)
    result = record_confirmed(
        [p.model_dump() for p in body.pairs], workspace_id=ws, actor=actor_email(request)
    )
    append_audit_event(
        action="mapping_memory.recorded",
        resource=f"workspace/{ws or 'default'}/mapping-memory",
        details={"recorded": result["recorded"], "total": result["total"]},
    )
    return result


@router.delete("/memory")
def forget_memory(
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    """Erase the workspace's learned pairs (governance: forget on request)."""
    from services.audit_log import append_audit_event
    from services.mapping_memory import forget_all

    ws = _workspace(request, workspace_id, write=True)
    removed = forget_all(workspace_id=ws)
    append_audit_event(
        action="mapping_memory.cleared",
        resource=f"workspace/{ws or 'default'}/mapping-memory",
        details={"removed": removed},
    )
    return {"removed": removed}
