"""Mapped MongoDB targets are field paths: place, read and check them by path.

A flat mapping target ``address.city`` names ``city`` inside the sub-document
``address`` (``services.document_paths``). The MongoDB writer uses these
helpers so a mapping into an existing nested schema lands inside it:

* :func:`insert_document` nests dotted keys for ``insert_many`` (insert and
  overwrite); upserts keep the dotted keys in ``$set`` so only the mapped
  nested field changes and its siblings survive;
* :func:`path_view` reads mapped paths back out of a stored document (the BSON
  type sample and the Gate-8 read-back);
* :func:`target_refusal` refuses targets the path rules reject, before any
  write, instead of renaming them.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from services.document_paths import SCALAR, PathError, check_paths, flatten, nest

_MISSING = object()


def insert_document(doc: Mapping[str, Any]) -> dict[str, Any]:
    """The document to insert: dotted keys become sub-documents, key order kept."""
    if not any("." in key for key in doc):
        return dict(doc)
    return nest(doc)


def value_at_path(doc: Any, path: str) -> Any:
    """The value stored at ``path``, or the module's missing marker.

    Read through sub-documents only. A literal top-level key ``"a.b"`` is a
    different field (an old COPY shape), not the mapped path: reading it
    would let Gate-8 pass a document the row path never writes.
    """
    node = doc
    for seg in path.split("."):
        if not isinstance(node, Mapping) or seg not in node:
            return _MISSING
        node = node[seg]
    return node


def path_view(doc: Mapping[str, Any], paths: Iterable[str]) -> dict[str, Any]:
    """``{path: value}`` for each of ``paths`` the document holds (absent paths left out)."""
    out: dict[str, Any] = {}
    for path in paths:
        value = value_at_path(doc, str(path))
        if value is not _MISSING:
            out[str(path)] = value
    return out


def readback_record(doc: Mapping[str, Any], columns: Iterable[str] | None = None) -> dict[str, Any]:
    """A stored document as Gate-8 compares it: keyed by dotted field path.

    With ``columns`` (the mapped targets), each is read at its path, so a
    target holding a sub-document (a ``nest_json`` object) is one value.
    Without, the document is flattened to its leaf paths
    (``document_paths.flatten``, non-strict); a key the flat form cannot name
    (a dot inside a key an application wrote) keeps the document as stored.
    """
    if columns:
        return path_view(doc, columns)
    try:
        return flatten(doc)
    except PathError:
        return dict(doc)


def target_refusal(target_cols: Iterable[str]) -> str:
    """Why these targets cannot be written as MongoDB field paths, or ``""``."""
    problems = check_paths((str(c), SCALAR) for c in target_cols if str(c or ""))
    if not problems:
        return ""
    return "MongoDB field paths refused: " + "; ".join(p.message for p in problems[:3])


__all__ = [
    "insert_document", "path_view", "readback_record", "target_refusal", "value_at_path",
]
