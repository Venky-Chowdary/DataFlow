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
``POST /export`` ``/import``    mapping-as-code: a versioned, checksummed YAML/JSON
    document (:mod:`services.mapping_document`).
``/templates``                 workspace mapping templates, re-bound onto another
    table's columns on apply (:mod:`services.mapping_templates`).

Every route is workspace-scoped: a saved connector is authorized against its
own workspace before it is dialled, and the memory read or written is the
caller's workspace's only.
"""

from __future__ import annotations

import logging
from typing import Any, Literal, Optional

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field

from services.mapping_type_compat import MAX_PAIRS
from services.safe_errors import safe_http_exception
from services.workspace_access import (
    actor_email,
    assert_connector_accessible,
    assert_resource_workspace,
    resolve_read_workspace,
    resolve_write_workspace,
    tenant_scope,
)

from .transfer_router import EndpointDTO, _refuse_unavailable_destination

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
    #: False for a column the destination generates: never a suggestion target.
    writable: bool = True


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
    #: Scopes the learned tier to this destination (no dial: identity only).
    destination: Optional[EndpointDTO] = None


class MemoryRecordRequest(BaseModel):
    #: The destination the pairs were confirmed for: it scopes the memory, and
    #: its catalog is read so only pairs onto real columns are remembered.
    destination: EndpointDTO
    pairs: list[MappedPair] = Field(..., min_length=1, max_length=200)


def _workspace(request: Request, header: str, *, write: bool) -> str:
    return (
        resolve_write_workspace(request, header)
        if write
        else resolve_read_workspace(request, header)
    )


async def _read_catalog(request: Request, destination: EndpointDTO, schema_policy: str) -> dict[str, Any]:
    """The destination's column catalog: authorized, off the loop, under the deadline."""
    from src.ai.copilot.tool_deadline import deadline_seconds, run_off_event_loop
    from src.transfer.models import EndpointConfig

    from services.mapping_dest_catalog import (
        build_destination_column_catalog,
        read_destination_column_catalog,
    )

    # A connector id expands into stored host and credentials: authorize it
    # against its own workspace before anything is dialled (404 elsewhere).
    assert_connector_accessible(request, destination.connector_id)
    # The host's destination policy (D276: SQLite / local files on the hosted
    # cloud) — the same refusal the transfer routes apply, so a refused local
    # file is not opened just to read its columns.
    _refuse_unavailable_destination(
        destination, resolve_read_workspace(request, request.headers.get("X-Workspace-Id", ""))
    )
    endpoint = EndpointConfig.from_dict(destination.kind, destination.model_dump(by_alias=True))
    if endpoint.kind != "database":
        # A file export is written fresh: there is no existing column to pick.
        return build_destination_column_catalog(
            {"connected": True, "table_exists": False},
            dest_type=str(endpoint.format or endpoint.kind or ""),
            schema_policy=schema_policy,
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
            lambda: read_destination_column_catalog(endpoint, schema_policy=schema_policy),
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


@router.post("/destination-columns")
async def destination_columns(body: DestinationColumnsRequest, request: Request) -> dict[str, Any]:
    """The destination table's column catalog for the Map picker and panel."""
    return await _read_catalog(request, body.destination, body.schema_policy)


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
    learned = (
        learned_index(
            workspace_id=ws,
            destination=body.destination.model_dump(by_alias=True) if body.destination else None,
        )
        if body.use_learned
        else {}
    )
    locked = [p for p in body.mapped if p.source and p.target] if body.mode == "remaining" else []
    suggestions = suggest_assignments(
        [c.name for c in body.source_columns],
        [c.name for c in body.destination_columns if c.writable],
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
        if grade["verdict"] == "incompatible":
            # A name match onto a type the write cannot carry is never pre-applied.
            suggestion["selected"] = False
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
async def record_memory(
    body: MemoryRecordRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    """Remember pairs the operator confirmed (Continue on Map) for this destination.

    Only pairs whose target is a writable column of the stated destination are
    recorded (its catalog is read, as for /destination-columns); the rest are
    returned as ``refused``, so the learned tier cannot be filled with names
    the destination does not have.
    """
    from services.audit_log import append_audit_event
    from services.mapping_memory import record_confirmed

    ws = _workspace(request, workspace_id, write=True)
    catalog = await _read_catalog(request, body.destination, "")
    columns = {
        str(c.get("name") or "").lower()
        for c in catalog.get("columns") or []
        if c.get("writable", True)
    }
    valid = [p for p in body.pairs if p.source and p.target.lower() in columns]
    refused = [p.target for p in body.pairs if p.target.lower() not in columns]
    result = record_confirmed(
        [p.model_dump() for p in valid],
        workspace_id=ws,
        destination=body.destination.model_dump(by_alias=True),
        actor=actor_email(request),
    ) if valid else {"recorded": 0, "total": 0}
    append_audit_event(
        action="mapping_memory.recorded",
        resource=f"workspace/{ws or 'default'}/mapping-memory",
        details={"recorded": result["recorded"], "refused": len(refused)},
    )
    return {**result, "refused": refused}


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


# --------------------------------------------------------------------------- mapping-as-code

_TEMPLATE_NOT_FOUND = "Mapping template not found"


class MappingDocumentBody(BaseModel):
    mappings: list[dict] = Field(..., max_length=5000)
    computed: list[dict] = Field(default_factory=list, max_length=200)
    name: str = Field("", max_length=120)
    source: dict = Field(default_factory=dict)
    destination: dict = Field(default_factory=dict)


class ExportRequest(MappingDocumentBody):
    format: Literal["yaml", "json"] = "yaml"


class ImportRequest(BaseModel):
    document: str = Field(..., min_length=1, max_length=2 * 1024 * 1024)


class TemplateCreate(MappingDocumentBody):
    description: str = Field("", max_length=1000)


class TemplateApply(BaseModel):
    source_columns: list[str] = Field(..., max_length=_MAX_COLUMNS)
    destination_columns: list[str] = Field(..., max_length=_MAX_COLUMNS)


def _document(body: MappingDocumentBody) -> dict[str, Any]:
    from services.mapping_document import MappingDocumentError, build_mapping_document

    try:
        return build_mapping_document(
            mappings=body.mappings,
            computed=body.computed,
            name=body.name,
            source=body.source,
            destination=body.destination,
        )
    except MappingDocumentError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/export")
def export_mapping(body: ExportRequest) -> Response:
    """The mapping as a downloadable YAML/JSON document (version + checksum)."""
    import re

    from services.mapping_document import dump_mapping_document

    doc = _document(body)
    text = dump_mapping_document(doc, fmt=body.format)
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", body.name or "mapping").strip("-") or "mapping"
    return Response(
        content=text,
        media_type="application/yaml" if body.format == "yaml" else "application/json",
        headers={"Content-Disposition": f'attachment; filename="{stem[:80]}.mapping.{body.format}"'},
    )


@router.post("/import")
def import_mapping(body: ImportRequest) -> dict[str, Any]:
    """Validate a document (version, schema, checksum, Transform steps) and return it."""
    from services.mapping_document import MappingDocumentError, load_mapping_document_with_status

    try:
        doc, verified = load_mapping_document_with_status(body.document)
    except MappingDocumentError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {
        "metadata": doc["metadata"],
        "mappings": doc["spec"]["mappings"],
        "computed": doc["spec"]["computed"],
        "checksum": doc["checksum"],
        "checksum_verified": verified,
        "warnings": [] if verified else [
            "The document has no checksum: it was imported as hand-authored and unverified."
        ],
    }


@router.get("/templates")
def list_mapping_templates(request: Request) -> dict[str, Any]:
    from services.mapping_templates import list_templates

    scope = tenant_scope(request)
    return {"templates": list_templates(allows=scope.allows)}


@router.post("/templates", status_code=201)
def create_mapping_template(
    body: TemplateCreate,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    from services.audit_log import append_audit_event
    from services.mapping_templates import TemplateLimitReached, create_template

    if not body.name.strip():
        raise HTTPException(status_code=422, detail="A template needs a name")
    ws = _workspace(request, workspace_id, write=True)
    doc = _document(body)
    try:
        record = create_template(
            name=body.name, description=body.description, document=doc,
            workspace_id=ws, actor=actor_email(request),
        )
    except TemplateLimitReached as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    append_audit_event(
        action="mapping_template.created",
        resource=f"mapping-template/{record['id']}",
        details={"name": record["name"], "mappings": len(doc["spec"]["mappings"])},
    )
    return record


def _owned_template(request: Request, template_id: str, *, write: bool = False) -> dict[str, Any]:
    from services.mapping_templates import get_template

    record = get_template(template_id)
    if not record:
        raise HTTPException(status_code=404, detail=_TEMPLATE_NOT_FOUND)
    assert_resource_workspace(
        request, record.get("workspace_id") or "", write=write, not_found_detail=_TEMPLATE_NOT_FOUND
    )
    return record


@router.get("/templates/{template_id}")
def get_mapping_template(template_id: str, request: Request) -> dict[str, Any]:
    return _owned_template(request, template_id)


@router.delete("/templates/{template_id}")
def delete_mapping_template(template_id: str, request: Request) -> dict[str, Any]:
    from services.audit_log import append_audit_event
    from services.mapping_templates import delete_template

    record = _owned_template(request, template_id, write=True)
    delete_template(template_id)
    append_audit_event(
        action="mapping_template.deleted",
        resource=f"mapping-template/{template_id}",
        details={"name": record.get("name")},
    )
    return {"deleted": template_id}


@router.post("/templates/{template_id}/apply")
def apply_mapping_template(template_id: str, body: TemplateApply, request: Request) -> dict[str, Any]:
    """Re-bind the template onto these columns; a preview, nothing is stored."""
    from services.mapping_templates import rebind_template_pairs

    record = _owned_template(request, template_id)
    spec = (record.get("document") or {}).get("spec") or {}
    pairs = [
        (str(m.get("source") or ""), str(m.get("target") or ""))
        for m in spec.get("mappings") or []
        if m.get("target") and not m.get("intentional_omit")
    ]
    out = rebind_template_pairs(
        pairs, source_columns=body.source_columns, destination_columns=body.destination_columns
    )
    return {**out, "computed": spec.get("computed") or [], "template": record.get("name")}
