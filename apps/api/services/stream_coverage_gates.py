"""Validate's per-table G13 and ADD COLUMN verdict for a multi-table request.

Validate ran G13 once, for the primary table, with the request's map. Each
other table of a multi-table job writes with its own stored map (or, without
one, an identity map of its own columns), and a column that map left out was
dropped with no gate saying so. This applies to every table the verdict
Execute applies before writing it (``source_column_coverage.stream_coverage_verdict``),
from the same inputs: the table's catalog columns, its map, its destination
columns and the run's schema policy (R8).
"""

from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)

GATE_ID = "g13_source_coverage"


def _stream_catalog(
    *, connector_id: str, stream: str, source_type: str, source_config: Any
) -> tuple[list[str], dict[str, str]]:
    """(columns, flatten lineage) of one source table, as Execute's reader sees it."""
    from services.flatten_protection import source_flattens
    from services.source_schema_authority import _endpoint_catalog_info, _saved_source_endpoint

    cfg = source_config if isinstance(source_config, dict) else {}
    endpoint = _saved_source_endpoint(
        connector_id,
        source_table=stream,
        source_collection="",
        stream=stream,
        source_schema=str(cfg.get("schema") or ""),
        source_database=str(cfg.get("database") or ""),
        source_config=None,
    )
    info = _endpoint_catalog_info(endpoint)
    schema = info.get("schema") if isinstance(info.get("schema"), dict) else {}
    lineage = info.get("flatten_lineage") if source_flattens(source_type, cfg) else None
    return [str(c) for c in schema], dict(lineage or {})


def per_stream_coverage_gates(
    *,
    stream_contracts: list[dict[str, Any]] | None,
    request_mappings: list[dict[str, Any]] | None,
    source_connector_id: str,
    source_type: str,
    source_config: Any,
    dest_type: str,
    dest_column_types: Callable[[str], dict[str, str]],
    schema_policy: str | None,
    backfill_new_fields: bool,
    acknowledged: bool,
) -> list[dict[str, Any]]:
    """One gate per table of a multi-table request (``[]`` for a single table)."""
    from services.flatten_protection import stamped_lineage
    from services.multi_stream_plan import adopt_inherited_mappings
    from services.schema_drift import effective_schema_policy
    from src.transfer.source_column_coverage import stream_coverage_verdict

    selected = [c for c in stream_contracts or [] if isinstance(c, dict) and c.get("selected", True)]
    if len(selected) < 2:
        return []
    policy = effective_schema_policy(schema_policy, backfill_new_fields=bool(backfill_new_fields))
    gates: list[dict[str, Any]] = []
    for contract in selected:
        stream = str(contract.get("name") or "").strip()
        if not stream:
            continue
        columns: list[str] = []
        lineage: dict[str, str] = {}
        if (source_connector_id or "").strip():
            try:
                columns, lineage = _stream_catalog(
                    connector_id=source_connector_id, stream=stream,
                    source_type=source_type, source_config=source_config,
                )
            except Exception:  # noqa: BLE001 - an unread table is reported, and Execute judges it
                logger.debug("per-table catalog read failed for %s", stream, exc_info=True)
        if not columns:
            gates.append({
                "id": GATE_ID, "status": "warn", "duration_ms": 0,
                "message": (
                    f"Table {stream}: Validate could not read its source columns. Execute "
                    "checks this table's columns against its map before writing it."
                ),
                "details": {"stream": stream, "rule_id": f"{GATE_ID}.table_unread"},
            })
            continue
        own = contract.get("mappings")
        if isinstance(own, list) and own:
            mappings = list(own)
        else:
            mappings, _note = adopt_inherited_mappings(request_mappings, columns)
        lineage = {**stamped_lineage(mappings), **lineage}
        try:
            dest_types = dict(dest_column_types(stream) or {})
        except Exception:  # noqa: BLE001 - unread destination is judged as absent (a CREATE)
            dest_types = {}
        verdict = stream_coverage_verdict(
            stream=stream, source_columns=columns, mappings=mappings, lineage=lineage,
            dest_types=dest_types, dest_type=dest_type, policy=policy, acknowledged=acknowledged,
        )
        gates.append({
            "id": GATE_ID,
            "status": verdict["status"],
            "duration_ms": 0,
            "message": verdict["message"]
            or f"Table {stream}: all {len(columns)} source column(s) accounted for",
            "details": {
                "stream": stream,
                "rule_id": verdict["rule_id"] or f"{GATE_ID}.table",
                "unaccounted_sources": verdict["columns"] if verdict["gate"] == GATE_ID else [],
                "add_columns": verdict["columns"] if verdict["gate"] and verdict["gate"] != GATE_ID else [],
                "record": verdict["record"],
            },
        })
    return gates


def apply_per_stream_coverage(result: dict[str, Any], gates: list[dict[str, Any]]) -> None:
    """Add the per-table gates to a preflight result; a block makes it not pass."""
    if not gates:
        return
    result["gates"] = [*(result.get("gates") or []), *gates]
    blocks = [g for g in gates if g.get("status") == "block"]
    if not blocks:
        return
    result["blockers"] = [
        *(result.get("blockers") or []),
        *({"id": g["id"], "message": g["message"], "details": g["details"]} for g in blocks),
    ]
    result["passed"] = False
