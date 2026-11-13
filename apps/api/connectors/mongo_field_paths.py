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

from services.document_paths import SCALAR, check_paths, nest

_MISSING = object()


def insert_document(doc: Mapping[str, Any]) -> dict[str, Any]:
    """The document to insert: dotted keys become sub-documents, key order kept."""
    if not any("." in key for key in doc):
        return dict(doc)
    return nest(doc)


def value_at_path(doc: Any, path: str) -> Any:
    """The value stored at ``path``, or the module's missing marker."""
    if isinstance(doc, Mapping) and path in doc:
        return doc[path]
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


def target_refusal(target_cols: Iterable[str]) -> str:
    """Why these targets cannot be written as MongoDB field paths, or ``""``."""
    problems = check_paths((str(c), SCALAR) for c in target_cols if str(c or ""))
    if not problems:
        return ""
    return "MongoDB field paths refused: " + "; ".join(p.message for p in problems[:3])


__all__ = ["insert_document", "path_view", "target_refusal", "value_at_path"]
