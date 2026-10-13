"""Capability catalog for the recipe canvas.

The row engine publishes the operations it will run. Set-engine nodes (join,
lookup, aggregate, router, and the rest) are described here with their ports
so the canvas can hold them, and ``executable`` stays false until a set engine
is certified. Nothing in this module parses a recipe or refuses a transfer.
``set_hash`` is the identity of a set document; an empty document hashes to
``""``, matching the set-graph contract.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

__all__ = [
    "canvas_capability",
    "set_document_hash",
]


def _row(port_id: str, role: str, label: str) -> dict[str, Any]:
    return {"id": port_id, "role": role, "kind": "row", "label": label, "visible": True}


def _field(port_id: str, role: str, label: str) -> dict[str, Any]:
    return {"id": port_id, "role": role, "kind": "field", "label": label, "visible": False}


def _node(
    op: str,
    summary: str,
    family: str,
    placement: str,
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    field_ports: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "op": op,
        "summary": summary,
        "family": family,
        "executable": False,
        "placement": placement,
        "inputs": inputs,
        "outputs": outputs,
        "field_ports": field_ports or [],
    }


# Ports are the plug-in contract. Field ports stay in the model and are not
# drawn until a catalog entry sets visible true.
_CANVAS_NODES: tuple[dict[str, Any], ...] = (
    _node("source", "Source object. The wizard owns this step.", "object", "wizard", [], [_row("out", "out", "Rows")]),
    _node("target", "Target object. The wizard owns this step.", "object", "wizard", [_row("in", "in", "Rows")], []),
    _node(
        "join",
        "Combine two inputs on a key",
        "set",
        "palette",
        [_row("left", "in", "Left"), _row("right", "in", "Right")],
        [_row("out", "out", "Joined")],
        [_field("left-key", "in", "Left key"), _field("right-key", "in", "Right key")],
    ),
    _node(
        "lookup",
        "Attach columns from a reference input",
        "set",
        "palette",
        [_row("main", "in", "Main"), _row("reference", "in", "Reference")],
        [_row("out", "out", "Looked up")],
        [_field("key", "in", "Lookup key")],
    ),
    _node(
        "aggregate",
        "Group rows and compute measures",
        "set",
        "palette",
        [_row("in", "in", "Rows")],
        [_row("out", "out", "Groups")],
        [_field("group", "in", "Group by"), _field("measure", "out", "Measure")],
    ),
    _node("sort", "Order rows by one or more columns", "set", "palette", [_row("in", "in", "Rows")], [_row("out", "out", "Ordered")]),
    _node("rank", "Number rows inside a partition", "set", "palette", [_row("in", "in", "Rows")], [_row("out", "out", "Ranked")]),
    _node(
        "window",
        "Compute a value over a frame of rows",
        "set",
        "palette",
        [_row("in", "in", "Rows")],
        [_row("out", "out", "Windowed")],
        [_field("frame", "in", "Frame")],
    ),
    _node("dedupe", "Keep one row per key", "set", "palette", [_row("in", "in", "Rows")], [_row("out", "out", "Unique"), _row("duplicates", "out", "Duplicates")]),
    _node(
        "router",
        "Send each row to one or more outputs",
        "set",
        "palette",
        [_row("in", "in", "Rows")],
        [_row("out-1", "out", "Group 1"), _row("out-2", "out", "Group 2"), _row("default", "out", "Default")],
    ),
    _node(
        "union",
        "Stack compatible inputs",
        "set",
        "palette",
        [_row("in-1", "in", "Input 1"), _row("in-2", "in", "Input 2")],
        [_row("out", "out", "Union")],
    ),
    _node("normalizer", "Turn repeating columns into rows", "set", "palette", [_row("in", "in", "Wide")], [_row("out", "out", "Normalized")]),
    _node("mapplet", "A reusable group of steps", "set", "palette", [_row("in", "in", "In")], [_row("out", "out", "Out")]),
    _node("pivot", "Turn row values into columns", "set", "palette", [_row("in", "in", "Rows")], [_row("out", "out", "Wide")]),
    _node("unpivot", "Turn columns into row values", "set", "palette", [_row("in", "in", "Wide")], [_row("out", "out", "Tall")]),
    _node("mask", "Hide part of a value", "cleanse", "palette", [_row("in", "in", "Rows")], [_row("out", "out", "Masked")], [_field("column", "in", "Column")]),
    _node("sql", "Warehouse SQL. It runs after the load, not on this canvas.", "sql", "plan", [], []),
)


def canvas_capability() -> dict[str, Any]:
    """Catalog fragment merged into GET /shape/catalog.

    ``set_operations`` lists set ops the engine can run. It is empty until one
    is certified. ``set_hash`` on the engine block is the empty-document hash.
    """
    return {
        "set_operations": [],
        "set_engine": {
            "available": False,
            "reference": "duckdb",
            "pushdown_dialects": [],
            "set_hash": "",
        },
        "canvas_nodes": [dict(node) for node in _CANVAS_NODES],
    }


def _canonical_document(document: Mapping[str, Any]) -> dict[str, Any]:
    """Key order and op spelling are not meaning. Node order is."""
    nodes: list[Any] = []
    for node in document.get("nodes") or []:
        if not isinstance(node, Mapping):
            continue
        item = json.loads(json.dumps(dict(node), sort_keys=True, default=str))
        if isinstance(item.get("op"), str):
            item["op"] = item["op"].strip().casefold()
        nodes.append(item)
    body: dict[str, Any] = {"nodes": nodes}
    main = document.get("main_output")
    if main not in (None, ""):
        body["main_output"] = main
    for key, value in document.items():
        if key in {"nodes", "main_output"}:
            continue
        body[key] = json.loads(json.dumps(value, sort_keys=True, default=str))
    return body


def set_document_hash(document: Mapping[str, Any] | None) -> str:
    """Identity of a set document. No nodes → ``""``."""
    if not document:
        return ""
    nodes = document.get("nodes") or []
    if not isinstance(nodes, (list, tuple)) or len(nodes) == 0:
        return ""
    payload = json.dumps(
        _canonical_document(document),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
