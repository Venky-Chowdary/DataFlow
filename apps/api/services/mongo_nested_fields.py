"""The field paths an existing MongoDB collection stores, with their BSON types.

The Map destination picker described an existing collection by its top-level
fields only (``address`` as one JSON value), so mapping a source column onto
``address.city`` meant typing the path blind. This reads a bounded, recent
sample of documents, lists every field path it holds as dotted text
(``address.city``, ``address.geo.lat``) and asks the server for each path's
stored BSON type with ``$type``, so the picker shows what is really there.

Writes into these paths go through ``connectors.mongo_field_paths``: a mapped
target ``address.city`` lands inside the ``address`` sub-document.

Bounds: the newest ``sample`` documents (by ``_id``), at most ``max_paths``
paths, at most ``max_depth`` levels. Arrays are leaves (``document_paths``
never addresses array elements by position). A key the path rules refuse
(``$x``, a dot inside a key, NUL, a nested ``_id``) is not offered.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from services.document_paths import path_issues

_logger = logging.getLogger(__name__)

DEFAULT_SAMPLE = 100
DEFAULT_MAX_PATHS = 300
DEFAULT_MAX_DEPTH = 6


def _collect_paths(
    doc: Mapping[str, Any], prefix: str, depth: int, max_depth: int, out: dict[str, None], max_paths: int
) -> None:
    for key, value in doc.items():
        if len(out) >= max_paths:
            return
        name = str(key)
        path = f"{prefix}.{name}" if prefix else name
        if "." in name or path_issues(path, allow_id=True):
            continue
        out.setdefault(path, None)
        if isinstance(value, Mapping) and value and depth < max_depth:
            _collect_paths(value, path, depth + 1, max_depth, out, max_paths)


def sample_field_paths(
    docs: list[Mapping[str, Any]], *, max_paths: int = DEFAULT_MAX_PATHS, max_depth: int = DEFAULT_MAX_DEPTH
) -> list[str]:
    """Every field path the documents hold, in first-seen order (parents before children)."""
    out: dict[str, None] = {}
    for doc in docs:
        if isinstance(doc, Mapping):
            _collect_paths(doc, "", 1, max_depth, out, max_paths)
        if len(out) >= max_paths:
            break
    return list(out)


def _type_label(types: set[str]) -> str:
    present = sorted(t for t in types if t and t != "missing")
    if not present:
        return ""
    non_null = [t for t in present if t != "null"]
    if len(non_null) == 1:
        return non_null[0]
    if not non_null:
        return "null"
    return "mixed: " + " | ".join(non_null)


def collection_field_types(
    collection: Any,
    *,
    sample: int = DEFAULT_SAMPLE,
    max_paths: int = DEFAULT_MAX_PATHS,
    max_depth: int = DEFAULT_MAX_DEPTH,
) -> list[dict[str, Any]]:
    """``[{"path", "bson_type", "parent", "depth"}]`` for the collection's newest documents.

    ``bson_type`` is the server's own ``$type`` name (``string``, ``int``,
    ``long``, ``double``, ``decimal``, ``bool``, ``date``, ``objectId``,
    ``object``, ``array`` ...); a path holding several types reads
    ``mixed: int | string``. Returns ``[]`` when the collection cannot be read.
    """
    pipeline_head: list[dict[str, Any]] = [{"$sort": {"_id": -1}}, {"$limit": int(sample)}]
    try:
        docs = list(collection.aggregate(pipeline_head))
    except Exception as exc:  # noqa: BLE001 - a catalog read must not fail the picker
        _logger.info("MongoDB field sample failed: %s", type(exc).__name__)
        return []
    paths = sample_field_paths(docs, max_paths=max_paths, max_depth=max_depth)
    if not paths:
        return []
    projection = {f"f{i}": {"$type": f"${path}"} for i, path in enumerate(paths)}
    projection["_id"] = 0
    seen: dict[str, set[str]] = {path: set() for path in paths}
    try:
        for row in collection.aggregate([*pipeline_head, {"$project": projection}]):
            for i, path in enumerate(paths):
                seen[path].add(str(row.get(f"f{i}") or ""))
    except Exception as exc:  # noqa: BLE001
        _logger.info("MongoDB field type read failed: %s", type(exc).__name__)
        return []
    out: list[dict[str, Any]] = []
    for path in paths:
        label = _type_label(seen[path])
        if not label:
            continue
        parent = path.rsplit(".", 1)[0] if "." in path else ""
        out.append({"path": path, "bson_type": label, "parent": parent, "depth": path.count(".") + 1})
    return out
