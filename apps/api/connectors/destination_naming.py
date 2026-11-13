"""How each destination names a mapped target: kept, refused, never silently changed.

One rule per destination family, shared by the writers (``writer_identifier``),
Validate (``target_name_gate``) and Execute (``target_names_refusal``), so a
name Validate accepts is the name the writer stores (G11).

* **SQL engines** sanitize illegal characters as before
  (:func:`connectors.sql_identifiers.sanitize_identifier`), then a name longer
  than the engine's own limit is refused with that limit, never cut. Every
  engine used to be cut at PostgreSQL's 63, so a 64-character MySQL name or a
  100-character SQL Server name was silently shortened, and two long names
  sharing their first 63 characters became one column.
* **BigQuery** keeps the target's case (column names are case-insensitive but
  case-preserving); two targets that differ only in case are one column there,
  so they are refused.
* **Document and search stores** (MongoDB, Elasticsearch, OpenSearch) keep the
  name exactly. SQL rules renamed ``@timestamp`` to ``_timestamp`` and
  ``address.city`` to ``address_city``. A MongoDB target is a field path
  (:mod:`services.document_paths`); a bad path is refused, not renamed.
* **File exports** keep the target's case (``CustomerName`` stayed
  ``customername`` in a local CSV/JSONL/Parquet/XLSX export).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from connectors.sql_identifiers import sanitize_identifier

#: Longest column name each SQL engine stores. PostgreSQL counts bytes
#: (NAMEDATALEN-1); Redshift loads through the PostgreSQL writer, so it keeps
#: that writer's limit; Oracle 12.2+ counts 128 bytes.
SQL_NAME_LIMITS: dict[str, int] = {
    "postgresql": 63,
    "redshift": 63,
    "mysql": 64,
    "sqlserver": 128,
    "synapse": 128,
    "oracle": 128,
    "snowflake": 255,
    "bigquery": 300,
}
#: SQL engines with no practical column-name limit: never cut.
SQL_UNLIMITED = frozenset({"sqlite", "duckdb"})
_BYTE_LIMITED = frozenset({"postgresql", "redshift", "oracle"})
_NO_CUT = 1 << 20

DOCUMENT_STORES = frozenset({"mongodb"})
SEARCH_STORES = frozenset({"elasticsearch", "opensearch"})
FILE_EXPORTS = frozenset({
    "csv", "tsv", "json", "jsonl", "ndjson", "excel", "xlsx", "xls", "parquet",
    "avro", "orc", "xml", "yaml", "fixed_width", "file", "file_export",
})
#: Elasticsearch metadata fields a document body cannot carry. ``_id`` is the
#: document id: the writer moves a mapped ``_id`` out of the body.
_ES_METADATA = frozenset({
    "_index", "_source", "_routing", "_field_names", "_ignored", "_seq_no",
    "_primary_term", "_version", "_tier", "_doc_count", "_size", "_meta",
    "_data_stream_timestamp", "_type", "_uid", "_all", "_parent",
})


def dest_kind(dest_type: str | None) -> str:
    """The canonical destination kind (``mssql`` -> ``sqlserver``), or ``""``."""
    raw = str(dest_type or "").strip().lower()
    if not raw:
        return ""
    try:
        from services.db_type_utils import normalize_dest_kind

        return normalize_dest_kind(raw)
    except ImportError:  # pragma: no cover - connectors used without services
        return raw


def keeps_exact_names(dest_type: str | None) -> bool:
    """True for stores whose field names are kept exactly as mapped."""
    kind = dest_kind(dest_type)
    return kind in DOCUMENT_STORES or kind in SEARCH_STORES


def name_limit(dest_type: str | None) -> int | None:
    """The engine's column-name limit, or ``None`` when names are not cut."""
    return SQL_NAME_LIMITS.get(dest_kind(dest_type))


def _exact(name: Any) -> str:
    return str(name or "").strip()


def writer_identifier(
    dest_type: str | None, *, preserve_case: bool = False
) -> Callable[[Any], str]:
    """The function a writer names each mapped target with.

    An unknown or empty destination keeps the historical rule (sanitize, cut at
    63) so callers that do not say where they write are unchanged.
    """
    kind = dest_kind(dest_type)
    if kind in DOCUMENT_STORES or kind in SEARCH_STORES:
        return _exact
    keep_case = preserve_case or kind == "bigquery" or kind in FILE_EXPORTS
    if kind in SQL_NAME_LIMITS or kind in SQL_UNLIMITED or kind in FILE_EXPORTS:
        cap = SQL_NAME_LIMITS.get(kind, _NO_CUT)
        return lambda name: sanitize_identifier(name, preserve_case=keep_case, max_len=cap)
    return lambda name: sanitize_identifier(name, preserve_case=keep_case)


def target_identifier(
    dest_type: str | None, *, preserve_case: bool = True
) -> Callable[[Any], str] | None:
    """``resolve_target_columns(identifier=)`` for code that names targets the
    way the destination's writer does (the stream's target columns, Gate-8's
    source digest); ``None`` (no destination) keeps the historical SQL rule."""
    if dest_kind(dest_type):
        return writer_identifier(dest_type, preserve_case=preserve_case)
    return None


def name_cap(dest_type: str | None) -> int:
    """How long a column name the destination's writer keeps (63 when unknown)."""
    kind = dest_kind(dest_type)
    if kind in SQL_NAME_LIMITS:
        return SQL_NAME_LIMITS[kind]
    if kind in SQL_UNLIMITED or kind in FILE_EXPORTS or keeps_exact_names(kind):
        return _NO_CUT
    return 63


def capped(func: Callable[..., str], dest_type: str | None) -> Callable[..., str]:
    """``func`` (an identifier helper taking ``max_len``) at the engine's own limit."""
    cap = name_cap(dest_type)

    def call(name: Any, *args: Any, **kwargs: Any) -> str:
        kwargs.setdefault("max_len", cap)
        return func(name, *args, **kwargs)

    return call


def _name_size(kind: str, name: str) -> int:
    return len(name.encode("utf-8")) if kind in _BYTE_LIMITED else len(name)


def target_name_issues(dest_type: str | None, targets: Iterable[Any]) -> list[dict[str, str]]:
    """Every mapped target the destination would refuse or silently change.

    Each issue is ``{code, target, message}`` (plus ``other`` for a collision).
    """
    kind = dest_kind(dest_type)
    names = [str(t) for t in targets if str(t or "").strip()]
    if kind in DOCUMENT_STORES:
        from services.document_paths import SCALAR, check_paths

        return [
            {"code": f"target_{p.code}", "target": p.path, "message": p.message,
             **({"other": p.other} if p.other else {})}
            for p in check_paths((_exact(n), SCALAR) for n in dict.fromkeys(names))
        ]
    if kind in SEARCH_STORES:
        return _search_name_issues(names)
    limit = SQL_NAME_LIMITS.get(kind)
    issues: list[dict[str, str]] = []
    if limit is not None:
        for raw in dict.fromkeys(names):
            stored = sanitize_identifier(raw, preserve_case=True, max_len=_NO_CUT)
            size = _name_size(kind, stored)
            if size > limit:
                unit = "bytes" if kind in _BYTE_LIMITED else "characters"
                issues.append({
                    "code": "target_name_too_long",
                    "target": raw,
                    "message": (
                        f"Column name {raw!r} is {size} {unit}; {kind} allows at most "
                        f"{limit}. Datawrap will not shorten it silently: rename it in Map."
                    ),
                })
    if kind == "bigquery":
        seen: dict[str, str] = {}
        for raw in dict.fromkeys(names):
            stored = sanitize_identifier(raw, preserve_case=True, max_len=_NO_CUT)
            folded = stored.casefold()
            if folded in seen and seen[folded] != raw:
                issues.append({
                    "code": "target_name_case_collision",
                    "target": raw,
                    "other": seen[folded],
                    "message": (
                        f"Columns {seen[folded]!r} and {raw!r} differ only in case; "
                        "BigQuery column names are case-insensitive, so they would be "
                        "one column. Rename one of them in Map."
                    ),
                })
            else:
                seen.setdefault(folded, raw)
    return issues


def _search_name_issues(names: list[str]) -> list[dict[str, str]]:
    from services.document_paths import SCALAR, collision_issues

    issues: list[dict[str, str]] = []
    valid: list[tuple[str, str]] = []
    for raw in dict.fromkeys(names):
        name = _exact(raw)
        segments = name.split(".")
        if any(not seg.strip() for seg in segments):
            issues.append({
                "code": "target_path_empty_segment",
                "target": raw,
                "message": f"Field name {raw!r} has an empty part: remove the extra dot.",
            })
            continue
        if segments[0] in _ES_METADATA:
            issues.append({
                "code": "target_name_reserved",
                "target": raw,
                "message": (
                    f"Field name {raw!r} is an Elasticsearch metadata field and "
                    "cannot be written inside a document. Rename it in Map."
                ),
            })
            continue
        valid.append((name, SCALAR))
    for p in collision_issues(valid):
        issues.append({"code": f"target_{p.code}", "target": p.path,
                       "message": p.message, **({"other": p.other} if p.other else {})})
    return issues


def _as_dict(mapping: Any) -> dict[str, Any]:
    """A Map row as a dict (the Validate API hands over pydantic models)."""
    if isinstance(mapping, dict):
        return mapping
    dump = getattr(mapping, "model_dump", None)
    return dump() if callable(dump) else {}


def _mapped_targets(mappings: Iterable[Any] | None) -> list[str]:
    from services.mapping_constraints import write_mappings

    rows = [_as_dict(m) for m in (mappings or [])]
    return [
        str(m.get("target") or "")
        for m in write_mappings(rows)
        if str(m.get("target") or "").strip()
    ]


def target_names_refusal(dest_type: str | None, mappings: Iterable[Any] | None) -> str:
    """Why Execute must not write these targets, or ``""`` (same rule as Validate)."""
    issues = target_name_issues(dest_type, _mapped_targets(mappings))
    if not issues:
        return ""
    head = "; ".join(i["message"] for i in issues[:3])
    more = f" (and {len(issues) - 3} more)" if len(issues) > 3 else ""
    return f"Destination field names refused: {head}{more}"


def target_name_gate(dest_type: str | None, mappings: Iterable[Any] | None) -> dict[str, Any] | None:
    """The Validate gate for mapped target names, or ``None`` when every name lands as mapped."""
    issues = target_name_issues(dest_type, _mapped_targets(mappings))
    if not issues:
        return None
    return {
        "id": "g23c_target_names",
        "status": "block",
        "message": target_names_refusal(dest_type, mappings),
        "duration_ms": 0,
        "details": {"dest_type": dest_kind(dest_type), "issues": issues},
    }


__all__ = [
    "DOCUMENT_STORES",
    "FILE_EXPORTS",
    "SEARCH_STORES",
    "SQL_NAME_LIMITS",
    "SQL_UNLIMITED",
    "dest_kind",
    "keeps_exact_names",
    "name_limit",
    "target_name_gate",
    "target_name_issues",
    "target_identifier",
    "target_names_refusal",
    "writer_identifier",
]
