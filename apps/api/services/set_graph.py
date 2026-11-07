"""Set-graph contract: preserve the document, hash it, and refuse to run it.

The row recipe (``ShapeRecipe.steps``) is a stream program. A join, lookup, or
aggregate is a second document, ``set_graph``, because a linear step list cannot
name two inputs. Phase A publishes that document and does not execute it.

Nothing in ``set_operations`` is executable. A node that names an operation is
kept on the recipe and reported as ``set_errors``. Execute, preview, and
Validate's image all refuse before any row step runs, so a trim cannot succeed
while the join is dropped.

An empty graph hashes to ``""``. ``recipe_hash`` does not include the graph, so
approvals taken before this field existed still match.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, NoReturn

__all__ = [
    "POST_LOAD_ONLY_OPERATIONS",
    "POST_LOAD_ONLY_REASON",
    "SetGraph",
    "SetNodeError",
    "attach_set_graph",
    "block_message",
    "catalog_set_fields",
    "executable_set_ops",
    "set_hash_refusal",
]


# Published on GET /shape/catalog as post_load_only.operations. Aliases the
# step parser refuses (group_by, order_by, distinct, …) stay parser-only so
# this list does not grow a second vocabulary the wizard would have to render.
# Ops that graduated to real in-flight steps (join/union/lookup/aggregate/
# sort_rows/rank_rows/dedupe_rows) are removed — they are no longer
# post-load-only.
POST_LOAD_ONLY_OPERATIONS: tuple[str, ...] = (
    "pivot",
    "unpivot",
    "window",
)

POST_LOAD_ONLY_REASON = (
    "These need the whole population in one place, which a streaming "
    "read cannot provide; model them on the Transforms page after the load."
)


def executable_set_ops() -> frozenset[str]:
    """Operations the set engine can run.

    Empty until a node is certified. Graduating an op adds it here, puts an
    ``executable: true`` entry in ``catalog_set_fields``, and removes it from
    ``POST_LOAD_ONLY_OPERATIONS``. It does not add the op to the row catalog.
    """
    return frozenset()


def catalog_set_fields() -> dict[str, Any]:
    """Catalog fragment for GET /shape/catalog. The row ``operations`` list is separate."""
    return {
        "set_operations": [],
        "set_engine": {
            "available": False,
            "reference": "duckdb",
            "pushdown_dialects": [],
        },
        "post_load_only": {
            "operations": list(POST_LOAD_ONLY_OPERATIONS),
            "reason": POST_LOAD_ONLY_REASON,
        },
    }


@dataclass(frozen=True, slots=True)
class SetNodeError:
    """One node the engine will not run. The document itself is kept."""

    node: str
    message: str
    op: str = ""

    def to_wire(self) -> dict[str, str]:
        return {"node": self.node, "op": self.op, "message": self.message}


@dataclass(frozen=True, slots=True)
class SetGraph:
    """A preserved set document plus the reasons it cannot run."""

    document: Mapping[str, Any]
    errors: tuple[SetNodeError, ...] = ()

    @property
    def nodes(self) -> tuple[Mapping[str, Any], ...]:
        raw = self.document.get("nodes") or ()
        if not isinstance(raw, (list, tuple)):
            return ()
        return tuple(node for node in raw if isinstance(node, Mapping))

    @property
    def is_empty(self) -> bool:
        """No nodes and no other payload. Omitted from ``to_wire`` so a steps-only recipe stays one."""
        if self.nodes:
            return False
        for key, value in self.document.items():
            if key == "nodes":
                continue
            if value not in (None, "", [], {}):
                return False
        return True

    @property
    def set_hash(self) -> str:
        """Identity of the graph. No nodes → ``""``, so today's approvals stay valid."""
        if not self.nodes:
            return ""
        payload = json.dumps(
            _canonical_document(self.document),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def to_wire(self) -> dict[str, Any]:
        return json.loads(json.dumps(dict(self.document), default=str))


def block_message(graph: SetGraph | None) -> str:
    """'' when there is nothing to run. Otherwise the first reason the graph cannot run."""
    if graph is None or graph.is_empty:
        return ""
    if graph.errors:
        return graph.errors[0].message
    return ""


def set_hash_refusal(actual: str, approved: str) -> str:
    """'' when the approved set identity matches. Both empty is a match."""
    have = str(actual or "")
    want = str(approved or "").strip()
    if have == want:
        return ""
    shown = have or "(none)"
    if not want:
        return (
            f"set graph {shown} has no approval — re-validate before running"
        )
    return (
        f"set graph {shown} is not the one approved at Validate "
        f"({want}) — re-validate the changed graph before running"
    )


def attach_set_graph(payload: Any) -> SetGraph | None:
    """The graph on a recipe payload, or None when the key is absent.

    A list payload is a steps list and has no graph. A malformed graph raises
    ``ShapeError`` so the document is not dropped on the floor.
    """
    data = _coerce(payload)
    if data is None:
        return None
    if _is_recipe(data):
        graph = getattr(data, "set_graph", None)
        return graph if isinstance(graph, SetGraph) else None
    if not isinstance(data, Mapping) or "set_graph" not in data:
        return None
    raw = data.get("set_graph")
    if raw is None:
        return None
    return _parse_document(raw)


def _is_recipe(payload: Any) -> bool:
    from services.shape_models import ShapeRecipe

    return isinstance(payload, ShapeRecipe)


def _refuse(message: str) -> NoReturn:
    from services.shape_models import ShapeError

    raise ShapeError(message)


def _coerce(payload: Any) -> Any:
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except ValueError as exc:
            _refuse(f"shape recipe is not valid JSON: {exc}")
    return payload


def _parse_document(raw: Any) -> SetGraph:
    if not isinstance(raw, Mapping):
        _refuse("set_graph must be an object")
    if "nodes" in raw and not isinstance(raw.get("nodes"), (list, tuple)):
        _refuse("set_graph nodes must be a list")
    nodes = raw.get("nodes") or []
    if not isinstance(nodes, (list, tuple)):
        _refuse("set_graph nodes must be a list")
    for index, node in enumerate(nodes):
        if not isinstance(node, Mapping):
            _refuse(f"set_graph node {index + 1} must be an object")
    document = json.loads(json.dumps(dict(raw), default=str))
    if "nodes" not in document:
        document["nodes"] = []
    return SetGraph(document=document, errors=tuple(_node_errors(document["nodes"])))


def _node_errors(nodes: list[Any]) -> list[SetNodeError]:
    errors: list[SetNodeError] = []
    seen: set[str] = set()
    runnable = executable_set_ops()
    for index, node in enumerate(nodes):
        if not isinstance(node, Mapping):
            continue
        node_id = str(node.get("id") or "").strip()
        label = node_id or f"node {index + 1}"
        op = str(node.get("op") or node.get("type") or "").strip()
        folded = op.casefold()
        if not node_id:
            errors.append(SetNodeError(
                node=label,
                op=folded,
                message=f"set node {label} has no id, so it cannot be pinned",
            ))
        elif node_id in seen:
            errors.append(SetNodeError(
                node=label,
                op=folded,
                message=f"set node {label} duplicates an id already in the graph",
            ))
        else:
            seen.add(node_id)
        if not folded:
            errors.append(SetNodeError(
                node=label,
                op="",
                message=f"set node {label} has no op",
            ))
            continue
        if folded not in runnable:
            errors.append(SetNodeError(
                node=label,
                op=folded,
                message=(
                    f"set node {label}: '{folded}' is not an executable set operation. "
                    "The set engine is not available, so this graph cannot run and "
                    "was not applied. Row steps in the same recipe were not run either."
                ),
            ))
    return errors


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
