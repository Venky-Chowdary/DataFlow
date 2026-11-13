"""Existing SQL tables keep the column names earlier runs gave them (G11 compatibility).

``connectors.destination_naming`` stops renaming targets: SQL engines keep a
name up to their real limit instead of PostgreSQL's 63 and BigQuery keeps
case. A table an earlier run created still holds the old spelling, so a
target whose new spelling is not a column there, but whose legacy spelling
(sanitized, cut at 63, lowercased on BigQuery) is exactly one column, keeps
writing to that column. Validate shows an info note
(``g23d_existing_target_columns``); Execute retargets the mapping before
anything reads or writes, so the writer, Gate-8 and the read-back all use that
column. A new table gets the full name. A name past the engine's limit is
refused only when no existing column matches.

MongoDB / Elasticsearch: a target ``a.b`` / ``@timestamp`` whose old
flattened spelling (``a_b`` / ``_timestamp``) is already a field of the
existing collection or index gets a Validate warning
(``target_path_legacy_flat_field``) naming both spellings and the remedies,
so a run does not silently mix two shapes.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

from connectors.destination_naming import (
    SQL_NAME_LIMITS,
    SQL_UNLIMITED,
    DOCUMENT_STORES,
    SEARCH_STORES,
    _NO_CUT,
    _as_dict,
    dest_kind,
)
from connectors.sql_identifiers import sanitize_identifier

logger = logging.getLogger(__name__)

#: Engines whose column names compare case-insensitively.
_CASE_INSENSITIVE = frozenset({"mysql", "sqlserver", "synapse", "sqlite", "bigquery", "snowflake"})
_LEGACY_CUT = 63


def legacy_spelling(dest_type: str | None, name: Any) -> str:
    """The column name a writer produced for ``name`` before G11."""
    kind = dest_kind(dest_type)
    return sanitize_identifier(
        str(name or ""), preserve_case=kind != "bigquery", max_len=_LEGACY_CUT
    )


def _full_spelling(name: Any) -> str:
    """The name without the old cut (what Validate measures against the limit)."""
    return sanitize_identifier(str(name or ""), preserve_case=True, max_len=_NO_CUT)


def _applies(kind: str) -> bool:
    return kind in SQL_NAME_LIMITS or kind in SQL_UNLIMITED


def existing_column_for(
    dest_type: str | None, target: str, existing: Iterable[str]
) -> str | None:
    """The existing legacy column ``target`` should keep writing to, or ``None``.

    ``None`` when the new spelling is itself a column (it is used as is), when
    the legacy spelling is the new one, or when no / several columns match it.
    """
    kind = dest_kind(dest_type)
    if not _applies(kind):
        return None
    cols = [str(c) for c in existing if str(c or "")]
    if not cols:
        return None
    fold = kind in _CASE_INSENSITIVE
    full = _full_spelling(target)
    if full in cols:
        return None
    if fold and kind != "bigquery" and any(c.casefold() == full.casefold() for c in cols):
        return None  # same column, already matched by the engine
    legacy = legacy_spelling(kind, target)
    hits = [c for c in cols if c == legacy or (fold and c.casefold() == legacy.casefold())]
    if kind == "bigquery" and not hits:
        hits = [c for c in cols if c.casefold() == full.casefold()]
    if len(hits) != 1 or hits[0] == full:
        return None
    return hits[0]


def adopt_existing_columns(
    dest_type: str | None, mappings: Iterable[Any] | None, existing: Iterable[str] | None
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Mappings retargeted to the legacy columns an existing table holds, and notes."""
    rows = [dict(_as_dict(m)) for m in (mappings or [])]
    cols = list(existing or [])
    notes: list[dict[str, str]] = []
    if not cols or not _applies(dest_kind(dest_type)):
        return rows, notes
    for row in rows:
        target = str(row.get("target") or "")
        if not target.strip():
            continue
        column = existing_column_for(dest_type, target, cols)
        if column is None:
            continue
        row["target"] = column
        notes.append({
            "code": "target_uses_existing_column",
            "target": target,
            "column": column,
            "message": (
                f"Map target {target!r} uses the existing column {column!r} created by "
                "an earlier run (Datawrap now keeps the full name only on new tables)."
            ),
        })
    return rows, notes


def _needs_probe(kind: str, mappings: list[dict[str, Any]]) -> bool:
    for m in mappings:
        target = str(m.get("target") or "")
        if target and legacy_spelling(kind, target) != _full_spelling(target):
            return True
    return False


def _bounded_probe(dest: Any) -> dict[str, Any]:
    """The destination's introspection, under the engine's connect deadline."""
    import copy

    from services.connector_executor import get_executor
    from services.workspace_filter import current_workspace_stamp
    from src.ai.copilot.tool_deadline import _Progress, deadline_scope, interrupt_connections
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.engine_destination import _destination_connect_deadline

    probe = copy.deepcopy(dest)
    probe.extra = {**(probe.extra or {}), "introspect_purpose": "destination"}
    seconds = _destination_connect_deadline()
    progress = _Progress(limit=seconds)

    def _run() -> dict[str, Any]:
        with deadline_scope(seconds, progress):
            return dict(introspect_endpoint(probe) or {})

    return get_executor().run(
        "destination_connect",
        _run,
        workspace=current_workspace_stamp(for_write=False),
        seconds=seconds,
        describe_timeout=lambda: "existing-column probe timed out",
        label=lambda: "destination_connect: existing target columns",
        interrupt=lambda: interrupt_connections(progress),
    )


def adopt_for_request(request: Any) -> list[dict[str, str]]:
    """Execute: retarget ``request.mappings`` onto an existing table's legacy columns.

    Probes the destination (bounded by the connect deadline) only when some
    target's spelling changed (a long name, a mixed-case BigQuery target) and
    adoption could matter: a refusal adoption cannot lift (a case collision)
    is left to Execute's own check. A failed probe adopts nothing.
    """
    from connectors.destination_naming import _mapped_targets, target_name_issues

    dest = getattr(request, "destination", None)
    kind = dest_kind(str(getattr(dest, "format", "") or ""))
    mappings = [dict(_as_dict(m)) for m in (getattr(request, "mappings", None) or [])]
    if dest is None or not _applies(kind) or not _needs_probe(kind, mappings):
        return []
    raw_issues = target_name_issues(kind, _mapped_targets(mappings))
    if any(i["code"] != "target_name_too_long" for i in raw_issues):
        return []
    try:
        info = _bounded_probe(dest)
    except Exception as exc:  # noqa: BLE001 - no adoption; Execute's own checks still run
        logger.warning("existing-column probe failed: %s", exc)
        return []
    if info.get("table_exists") is False:
        return []
    existing = list(dict(info.get("schema") or {}).keys())
    rows, notes = adopt_existing_columns(kind, mappings, existing)
    if notes:
        request.mappings = rows
        for note in notes:
            logger.info("G11: %s", note["message"])
    return notes


# -- document / search stores: warn about the old flattened spelling ---------


def legacy_flat_field_warnings(
    dest_type: str | None, mappings: Iterable[Any] | None, existing_fields: Iterable[str]
) -> list[dict[str, str]]:
    """``target_path_legacy_flat_field`` for each target whose old spelling exists."""
    kind = dest_kind(dest_type)
    if kind not in DOCUMENT_STORES and kind not in SEARCH_STORES:
        return []
    fields = {str(f) for f in existing_fields if str(f or "")}
    if not fields:
        return []
    store = "collection" if kind in DOCUMENT_STORES else "index"
    out: list[dict[str, str]] = []
    for m in mappings or []:
        target = str(_as_dict(m).get("target") or "").strip()
        old = sanitize_identifier(target, preserve_case=True)
        if not target or old == target or old not in fields or target in fields:
            continue
        rename = (
            f"updateMany({{}}, {{$rename: {{{old!r}: {target!r}}}}})"
            if kind in DOCUMENT_STORES
            else "a reindex that renames the field"
        )
        out.append({
            "code": "target_path_legacy_flat_field",
            "target": target,
            "other": old,
            "message": (
                f"The existing {store} already holds {old!r}, the name earlier runs "
                f"wrote for {target!r}; this run writes {target!r}, so the {store} "
                f"would mix both shapes. Rename the Map target to {old!r} to keep the "
                f"old layout, run one overwrite, or migrate first with {rename}."
            ),
        })
    return out


def mongo_top_level_fields(cfg: Mapping[str, Any], collection: str, sample: int = 20) -> set[str]:
    """Top-level field names in a few documents of an existing collection."""
    from connectors.mongodb_common import _mongo_client, normalize_mongodb_connection_string

    conn = normalize_mongodb_connection_string(
        str(cfg.get("connection_string") or ""),
        database=str(cfg.get("database") or ""),
        host=str(cfg.get("host") or ""),
        port=int(cfg.get("port") or 27017),
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
        ssl=bool(cfg.get("ssl", False)),
        auth_source=str(cfg.get("auth_source") or ""),
    )
    coll = _mongo_client(conn)[str(cfg.get("database") or "test")][collection]
    fields: set[str] = set()
    for doc in coll.find({}).limit(int(sample)):
        fields.update(str(k) for k in doc)
    return fields


def es_mapping_fields(cfg: Mapping[str, Any], index: str) -> set[str]:
    """Dotted field paths an existing Elasticsearch index mapping declares."""
    from connectors.elasticsearch_reader import _client

    raw = _client(dict(cfg)).indices.get_mapping(index=index)
    body = raw.body if hasattr(raw, "body") else raw
    fields: set[str] = set()

    def walk(props: Mapping[str, Any], prefix: str) -> None:
        for name, spec in (props or {}).items():
            path = f"{prefix}.{name}" if prefix else str(name)
            fields.add(path)
            if isinstance(spec, Mapping) and isinstance(spec.get("properties"), Mapping):
                walk(spec["properties"], path)

    for entry in (body or {}).values():
        walk(((entry or {}).get("mappings") or {}).get("properties") or {}, "")
    return fields


def existing_document_fields(
    dest_type: str | None, cfg: Mapping[str, Any] | None, table: str
) -> set[str]:
    """Fields an existing collection / index holds; empty when new, empty or unreadable."""
    kind = dest_kind(dest_type)
    if not cfg or not table:
        return set()
    try:
        if kind in DOCUMENT_STORES:
            return mongo_top_level_fields(cfg, table)
        if kind in SEARCH_STORES:
            return es_mapping_fields(cfg, table)
    except Exception as exc:  # noqa: BLE001 - a warning only; never blocks Validate
        logger.info("legacy field sample skipped: %s", exc)
    return set()


def target_name_gates(
    dest_type: str | None,
    mappings: Iterable[Any] | None,
    *,
    existing_columns: Iterable[str] | None = None,
    destination_config: Mapping[str, Any] | None = None,
    destination_table: str = "",
) -> list[dict[str, Any]]:
    """Validate's target-name gates.

    ``g23c_target_names`` (block) holds names the destination refuses, judged
    after targets adopt an existing table's legacy columns;
    ``g23d_existing_target_columns`` (info) lists those adoptions;
    ``g23e_target_legacy_fields`` (warn) lists MongoDB / Elasticsearch targets
    whose old flattened spelling the existing collection or index holds.
    """
    from connectors.destination_naming import _mapped_targets, target_name_issues

    rows, notes = adopt_existing_columns(dest_type, mappings, existing_columns)
    issues = target_name_issues(dest_type, _mapped_targets(rows))
    seen: dict[str, str] = {}
    for note in notes:
        column = note["column"].casefold()
        if column in seen:
            issues.append({
                "code": "target_name_collision",
                "target": note["target"],
                "other": seen[column],
                "message": (
                    f"Map targets {seen[column]!r} and {note['target']!r} would both "
                    f"write the existing column {note['column']!r}. Rename one in Map."
                ),
            })
        seen.setdefault(column, note["target"])
    gates: list[dict[str, Any]] = []
    kind = dest_kind(dest_type)
    if issues:
        head = "; ".join(i["message"] for i in issues[:3])
        more = f" (and {len(issues) - 3} more)" if len(issues) > 3 else ""
        gates.append({
            "id": "g23c_target_names",
            "status": "block",
            "message": f"Destination field names refused: {head}{more}",
            "duration_ms": 0,
            "details": {"dest_type": kind, "issues": issues},
        })
    if notes:
        gates.append({
            "id": "g23d_existing_target_columns",
            "status": "pass",
            "severity": "info",
            "message": " ".join(n["message"] for n in notes[:3]),
            "duration_ms": 0,
            "details": {"dest_type": kind, "notes": notes},
        })
    fields = existing_document_fields(dest_type, destination_config, destination_table)
    warnings = legacy_flat_field_warnings(dest_type, mappings, fields)
    if warnings:
        gates.append({
            "id": "g23e_target_legacy_fields",
            "status": "warn",
            "message": " ".join(w["message"] for w in warnings[:3]),
            "duration_ms": 0,
            "details": {"dest_type": kind, "issues": warnings},
        })
    return gates


__all__ = [
    "adopt_existing_columns",
    "adopt_for_request",
    "existing_column_for",
    "es_mapping_fields",
    "existing_document_fields",
    "legacy_flat_field_warnings",
    "legacy_spelling",
    "mongo_top_level_fields",
    "target_name_gates",
]
