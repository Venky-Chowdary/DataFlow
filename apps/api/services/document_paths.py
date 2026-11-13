"""Field paths inside a MongoDB document: the rules every writer shares.

A path is dotted text: ``address.city`` names ``city`` inside the
sub-document ``address``. The document model (``services.document_model``)
and the flat Mongo writer both place values by path, so both read the rules
from here, and a path that is refused by one is refused by the other.

Rules:

* dots separate segments, and no segment is empty;
* no segment starts with ``$`` (MongoDB reads those as operators);
* no segment contains NUL;
* ``_id`` is allowed only as the first segment of a top-level path; inside an
  array element or a nested container it is refused (``allow_id=False``);
* two paths collide when they are the same path, when one is a scalar and the
  other writes inside it (``a`` and ``a.b``), or when one writes inside an
  array path (``orders`` is an array and ``orders.total`` is a field).

``flatten`` and ``nest`` convert between a document and its dotted leaf
paths. Arrays are leaf values: their elements are compared per relationship,
never by position through a dotted path.

This module has no dependencies outside the standard library, on purpose:
writers, the model, the builder and the reconciler all import it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

SCALAR = "scalar"
ARRAY = "array"
_KINDS = frozenset({SCALAR, ARRAY})

ID_FIELD = "_id"


@dataclass(frozen=True)
class PathIssue:
    """One reason a path cannot be used. ``path`` is the path at fault."""

    code: str
    path: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "path": self.path, "message": self.message}


class PathError(ValueError):
    """A path that cannot be used, named by ``code`` (see :class:`PathIssue`)."""

    def __init__(self, issue: PathIssue):
        super().__init__(issue.message)
        self.issue = issue
        self.code = issue.code
        self.path = issue.path


def _issue(code: str, path: str, message: str) -> PathIssue:
    return PathIssue(code=code, path=path, message=message)


def path_issues(path: Any, *, allow_id: bool = True) -> list[PathIssue]:
    """Every rule ``path`` breaks on its own (collisions are checked separately).

    ``allow_id=False`` is for paths inside an array element or another
    nested container, where ``_id`` would not be the document's id.
    """
    if not isinstance(path, str):
        return [_issue("path_not_text", str(path), f"Field path {path!r} must be text.")]
    if not path.strip():
        return [_issue("path_empty", path, "A field path cannot be empty.")]
    issues: list[PathIssue] = []
    if "\x00" in path:
        issues.append(
            _issue("path_nul", path, f"Field path {path!r} contains a NUL character.")
        )
    segments = path.split(".")
    if any(seg == "" for seg in segments):
        issues.append(
            _issue(
                "path_empty_segment",
                path,
                f"Field path {path!r} has an empty part: remove the extra dot.",
            )
        )
    dollar = [seg for seg in segments if seg.startswith("$")]
    if dollar:
        issues.append(
            _issue(
                "path_dollar_segment",
                path,
                f"Field path {path!r} has a part starting with '$' ({dollar[0]!r}); "
                "MongoDB reserves those for operators.",
            )
        )
    id_at = [i for i, seg in enumerate(segments) if seg == ID_FIELD]
    if any(i > 0 for i in id_at) or (id_at and not allow_id):
        issues.append(
            _issue(
                "path_id_not_root",
                path,
                f"Field path {path!r} uses '_id' below the top of the document; "
                "'_id' is only allowed as the document's own id.",
            )
        )
    return issues


def split_path(path: Any, *, allow_id: bool = True) -> tuple[str, ...]:
    """The segments of a valid path; :class:`PathError` names the first problem."""
    issues = path_issues(path, allow_id=allow_id)
    if issues:
        raise PathError(issues[0])
    return tuple(path.split("."))


def collision_issues(entries: Iterable[tuple[str, str]]) -> list[PathIssue]:
    """Collisions among ``(path, kind)`` entries placed in one document.

    ``kind`` is :data:`SCALAR` (a value, which may itself be a sub-document
    the caller writes whole) or :data:`ARRAY`. Sub-documents implied by dotted
    paths are not entries: ``address.city`` and ``address.zip`` share
    ``address`` without colliding. Paths are expected to be valid already
    (see :func:`check_paths`); each collision is reported once, on the later
    or longer path.
    """
    seen: dict[str, str] = {}
    issues: list[PathIssue] = []
    ordered: list[tuple[str, str]] = []
    for path, kind in entries:
        if kind not in _KINDS:
            raise ValueError(f"unknown path kind {kind!r}")
        if path in seen:
            issues.append(
                _issue(
                    "path_duplicate",
                    path,
                    f"Field path {path!r} is written twice in the same document.",
                )
            )
            continue
        seen[path] = kind
        ordered.append((path, kind))
    for path, _kind in ordered:
        parts = path.split(".")
        for cut in range(1, len(parts)):
            prefix = ".".join(parts[:cut])
            prefix_kind = seen.get(prefix)
            if prefix_kind is None:
                continue
            if prefix_kind == ARRAY:
                issues.append(
                    _issue(
                        "path_array_collision",
                        path,
                        f"Field path {path!r} writes inside {prefix!r}, which holds "
                        "an array; put the field inside the array's elements instead.",
                    )
                )
            else:
                issues.append(
                    _issue(
                        "path_prefix_collision",
                        path,
                        f"Field path {path!r} writes inside {prefix!r}, which already "
                        "holds a single value; a path cannot be both a value and a "
                        "sub-document.",
                    )
                )
            break
    return issues


def check_paths(
    entries: Iterable[tuple[str, str]], *, allow_id: bool = True
) -> list[PathIssue]:
    """Every problem with a set of paths placed in one document.

    Invalid paths are reported and left out of the collision check, so one
    bad path is not also reported as colliding with its neighbours.
    """
    issues: list[PathIssue] = []
    valid: list[tuple[str, str]] = []
    for path, kind in entries:
        bad = path_issues(path, allow_id=allow_id)
        if bad:
            issues.extend(bad)
        else:
            valid.append((path, kind))
    issues.extend(collision_issues(valid))
    return issues


def flatten(doc: Mapping[str, Any], *, prefix: str = "") -> dict[str, Any]:
    """``{dotted path: leaf value}`` for a document.

    Sub-documents are walked; arrays, scalars and empty sub-documents are
    leaves. A key that would not survive the round trip (not text, empty, or
    containing a dot) raises :class:`PathError`.
    """
    out: dict[str, Any] = {}
    _flatten_into(doc, prefix, out)
    return out


def _flatten_into(doc: Mapping[str, Any], prefix: str, out: dict[str, Any]) -> None:
    for key, value in doc.items():
        if not isinstance(key, str) or key == "":
            raise PathError(
                _issue(
                    "path_empty_segment" if key == "" else "path_not_text",
                    f"{prefix}.{key}" if prefix else str(key),
                    f"Document key {key!r} cannot be used as a field path.",
                )
            )
        if "." in key:
            raise PathError(
                _issue(
                    "path_dotted_key",
                    f"{prefix}.{key}" if prefix else key,
                    f"Document key {key!r} contains a dot, so its dotted path "
                    "would be ambiguous.",
                )
            )
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, Mapping) and value:
            _flatten_into(value, path, out)
        else:
            out[path] = value


def nest(flat: Mapping[str, Any]) -> dict[str, Any]:
    """The document a ``{dotted path: value}`` mapping describes.

    Paths are checked with :func:`check_paths`; the first problem raises
    :class:`PathError`. ``nest(flatten(doc)) == doc`` for any document whose
    keys contain no dots.
    """
    entries = [(path, SCALAR) for path in flat]
    problems = check_paths(entries)
    if problems:
        raise PathError(problems[0])
    out: dict[str, Any] = {}
    for path, value in flat.items():
        parts = path.split(".")
        node = out
        for seg in parts[:-1]:
            node = node.setdefault(seg, {})
        node[parts[-1]] = value
    return out


__all__ = [
    "ARRAY",
    "ID_FIELD",
    "SCALAR",
    "PathError",
    "PathIssue",
    "check_paths",
    "collision_issues",
    "flatten",
    "nest",
    "path_issues",
    "split_path",
]
