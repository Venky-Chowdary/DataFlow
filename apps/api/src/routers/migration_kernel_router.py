"""Migration Decision Kernel API.

Single authoritative endpoint for migration decisions. The kernel returns one
immutable Decision Artifact that Map, Validate, Execute, Proof, Audit, and the
UI all consume.
"""

from __future__ import annotations

import dataclasses
from typing import Any

from fastapi import APIRouter, HTTPException

from services.migration_kernel import (
    ColumnModel,
    MigrationKernel,
    SchemaModel,
    TypeCarrier,
)

router = APIRouter(tags=["migration"])


def _carrier_from_dict(t: dict[str, Any]) -> TypeCarrier:
    return TypeCarrier(
        logical=t.get("logical") or "",
        native=t.get("native") or t.get("inferred_type") or "",
        precision=t.get("precision"),
        scale=t.get("scale"),
        length=t.get("length"),
        timezone=t.get("timezone"),
        metadata=t.get("metadata") or {},
    )


def _schema_from_dict(s: dict[str, Any]) -> SchemaModel:
    columns = s.get("columns") or []
    return SchemaModel(
        kind=s.get("kind") or "database",
        format=s.get("format") or "",
        name=s.get("name") or "",
        columns=tuple(
            ColumnModel(
                name=c.get("name") or "",
                carrier=_carrier_from_dict(c.get("carrier") or {}),
            )
            for c in columns
        ),
    )


@router.post("/migration/decision")
def migration_decision(payload: dict[str, Any]) -> dict[str, Any]:
    """Build an immutable MigrationDecision for a source → destination pair."""
    source = _schema_from_dict(payload.get("source") or {})
    destination = _schema_from_dict(payload.get("destination") or {})
    dest_db = str(payload.get("dest_db") or "")
    if not dest_db:
        raise HTTPException(status_code=400, detail="dest_db is required")

    kernel = MigrationKernel()
    decision = kernel.build_decision(
        source,
        destination,
        dest_db=dest_db,
        validation_mode=payload.get("validation_mode", "strict"),
    )
    return dataclasses.asdict(decision)
