"""One pgvector destination contract: what the plan shows is what the job creates (D209).

A pgvector destination is a PostgreSQL database with the ``vector`` extension.
It can hold two different shapes:

* ``table`` — the ordinary typed table the Map promises: one column per mapped
  field with its declared type, SQL NULL kept as NULL. Written, introspected
  and verified exactly like PostgreSQL.
* ``document`` — the embedding / RAG layout: ``id TEXT`` (the source key),
  ``content`` (the embedded text, from one declared rule), ``embedding
  vector(n)``, ``metadata JSONB`` (every mapped value, NULL as JSON null),
  ``source_id``, ``chunk_index``, ``created_at``.

Before D209 the plan, preflight and Map treated every pgvector destination as
plain PostgreSQL (typed columns) while the writer always built the document
layout, so the promised columns did not exist and values sat only in a JSON
blob that dropped NULLs.

The layout is decided once, here, and carried on the endpoint as
``extra.vector_layout`` so plan, preflight, writer and Gate-8 read the same
answer:

1. an explicit ``vector_layout`` on the run or the saved connector wins;
2. otherwise an existing table keeps the shape it already has (run 2 into a
   table DataFlow created keeps working);
3. otherwise a new table gets the document layout — production behaviour —
   unless ``DATAFLOW_PGVECTOR_TYPED_TABLE_DEFAULT=1`` makes it a typed table.
   With the flag off, each new table that would have been typed is recorded
   once an hour as ``pgvector.would_create_typed_table``.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterable, Mapping
from typing import Any

logger = logging.getLogger(__name__)

LAYOUT_TABLE = "table"
LAYOUT_DOCUMENT = "document"
TYPED_DEFAULT_FLAG = "DATAFLOW_PGVECTOR_TYPED_TABLE_DEFAULT"

_ALIASES = {
    "table": LAYOUT_TABLE,
    "typed": LAYOUT_TABLE,
    "typed_table": LAYOUT_TABLE,
    "relational": LAYOUT_TABLE,
    "document": LAYOUT_DOCUMENT,
    "documents": LAYOUT_DOCUMENT,
    "vector_document": LAYOUT_DOCUMENT,
    "embedding": LAYOUT_DOCUMENT,
    "rag": LAYOUT_DOCUMENT,
}
# Columns the document writer creates. A table holding all of the first six is
# one DataFlow created as a vector-document table.
DOCUMENT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("id", "TEXT"),
    ("content", "TEXT"),
    ("embedding", "vector"),
    ("metadata", "JSONB"),
    ("source_id", "TEXT"),
    ("chunk_index", "INTEGER"),
    ("created_at", "TIMESTAMP"),
)
_DOCUMENT_SIGNATURE = frozenset(
    {"id", "content", "embedding", "metadata", "source_id", "chunk_index"}
)


def normalize_layout(value: Any) -> str | None:
    """``table`` / ``document`` for a recognised spelling, else ``None``."""
    return _ALIASES.get(str(value or "").strip().lower())


def declared_layout(cfg: Mapping[str, Any] | None) -> str | None:
    """The layout an operator declared on the run (flat) or saved connector (nested)."""
    if not isinstance(cfg, Mapping):
        return None
    flat = normalize_layout(cfg.get("vector_layout"))
    if flat:
        return flat
    nested = cfg.get("extra")
    if isinstance(nested, Mapping):
        return normalize_layout(nested.get("vector_layout"))
    return None


def is_document_shaped(columns: Iterable[Any]) -> bool:
    names = {str(c or "").strip().lower() for c in columns or ()}
    return _DOCUMENT_SIGNATURE <= names


def typed_default_enabled() -> bool:
    from services.brand_env import getenv_brand_bool

    return getenv_brand_bool(TYPED_DEFAULT_FLAG, False)


def decide_layout(
    cfg: Mapping[str, Any] | None,
    *,
    existing_columns: Iterable[Any] | None,
    table: str = "",
    workspace_id: str = "",
) -> tuple[str, str]:
    """``(layout, reason)`` for a pgvector destination.

    ``existing_columns`` is ``None`` when the table does not exist.
    """
    declared = declared_layout(cfg)
    if declared:
        return declared, "declared"
    if existing_columns is not None:
        cols = list(existing_columns)
        if cols:
            if is_document_shaped(cols):
                return LAYOUT_DOCUMENT, "existing_document_table"
            return LAYOUT_TABLE, "existing_typed_table"
    if typed_default_enabled():
        return LAYOUT_TABLE, "typed_default_flag"
    from services.would_apply_audit import audit_would_apply

    audit_would_apply(
        "pgvector.would_create_typed_table",
        resource=f"pgvector:{table}",
        dedupe_key=(str((cfg or {}).get("host") or ""), str((cfg or {}).get("database") or ""), table),
        details={
            "table": table,
            "today": "vector-document table (id, content, embedding, metadata)",
            "with_flag": "typed table with the mapped columns",
        },
        flag=TYPED_DEFAULT_FLAG,
        workspace_id=workspace_id,
    )
    return LAYOUT_DOCUMENT, "default_document"


def is_pgvector_cfg(cfg: Mapping[str, Any] | None) -> bool:
    from src.transfer.connector_capabilities import resolve_driver_type

    return resolve_driver_type(str((cfg or {}).get("type") or "")) == "pgvector"


def apply_layout_to_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    """A pgvector cfg whose layout is ``table`` is driven as PostgreSQL.

    Called by ``resolve_connector_config`` so every reader of the resolved
    config — writer dispatch, introspection, Gate-8 — sees one engine.
    """
    if is_pgvector_cfg(cfg) and declared_layout(cfg) == LAYOUT_TABLE:
        cfg["type"] = "postgresql"
        cfg["vector_layout"] = LAYOUT_TABLE
    return cfg


def _existing_columns(cfg: Mapping[str, Any], schema: str, table: str) -> list[str] | None:
    from connectors.postgresql_conn import get_connection

    conn = get_connection(
        host=str(cfg.get("host") or ""),
        port=int(cfg.get("port") or 5432),
        database=str(cfg.get("database") or ""),
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
        connection_string=str(cfg.get("connection_string") or ""),
        ssl=bool(cfg.get("ssl")),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            )
            names = [str(r[0]) for r in cur.fetchall()]
        return names or None
    finally:
        conn.close()


def stamp_destination_layout(endpoint: Any, workspace_id: str | None = None) -> Any:
    """Decide a pgvector destination's layout once and carry it on ``extra``.

    A run staged by the Pilot already carries the planned layout. Anything
    else (Studio, API, schedules) is decided here, before the endpoint is
    resolved, so the rest of the job reads the same answer.
    """
    from src.transfer.adapters import resolve_connector_config

    if normalize_layout((getattr(endpoint, "extra", None) or {}).get("vector_layout")):
        return endpoint
    fmt = str(getattr(endpoint, "format", "") or "").strip()
    if fmt:
        from src.transfer.connector_capabilities import resolve_driver_type

        if resolve_driver_type(fmt) != "pgvector":
            return endpoint  # no extra connector resolution for other engines
    try:
        cfg = resolve_connector_config(endpoint, workspace_id=workspace_id)
    except Exception:  # noqa: BLE001 — resolution errors surface on the real resolve
        return endpoint
    # A saved connector that declares ``table`` already resolves as postgresql.
    if not is_pgvector_cfg(cfg) and cfg.get("vector_layout") != LAYOUT_TABLE:
        return endpoint
    table = str(getattr(endpoint, "table", "") or "")
    schema = str(cfg.get("schema") or "public")
    existing: list[str] | None = None
    if not declared_layout(cfg) and table:
        try:
            existing = _existing_columns(cfg, schema, table)
        except Exception as exc:  # noqa: BLE001 — the writer reports the real failure
            logger.info("pgvector layout probe failed: %s", type(exc).__name__)
    layout, reason = decide_layout(
        cfg, existing_columns=existing, table=table, workspace_id=str(workspace_id or "")
    )
    extra = {**(endpoint.extra or {}), "vector_layout": layout, "vector_layout_reason": reason}
    return dataclasses.replace(endpoint, extra=extra)


# --------------------------------------------------------------------------- #
# What the plan shows
# --------------------------------------------------------------------------- #


def embedded_text_rule(options: Mapping[str, Any] | None) -> dict[str, Any]:
    """The one rule every document's embedded text comes from."""
    opts = options or {}
    template = opts.get("text_template")
    if template is not None:
        return {"kind": "template", "template": str(template)}
    column = str(opts.get("content_column") or "").strip()
    if column:
        return {
            "kind": "column",
            "column": column,
            "null": "SQL NULL or '' → empty content, no embedding",
        }
    if str(opts.get("embedding_column") or "").strip():
        return {"kind": "embedding_column", "column": str(opts["embedding_column"])}
    return {
        "kind": "row_json",
        "description": "every mapped field of the row as canonical JSON (sorted keys, NULL as null)",
    }


def destination_contract(
    layout: str,
    reason: str,
    *,
    mappings: list[dict[str, Any]],
    column_types: Mapping[str, str] | None,
    options: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """The physical shape the job will create, column by column."""
    rows = [
        (
            str(m.get("target_column") or m.get("target") or m.get("source") or ""),
            str(m.get("source_column") or m.get("source") or ""),
            str(m.get("target_type") or m.get("dest_type") or ""),
        )
        for m in mappings or []
    ]
    if layout == LAYOUT_TABLE:
        return {
            "layout": LAYOUT_TABLE,
            "layout_reason": reason,
            "columns": [
                {"name": target, "type": ttype, "source": source}
                for target, source, ttype in rows
                if target
            ],
            "null_policy": "SQL NULL stays NULL",
        }
    opts = dict(options or {})
    from connectors.pgvector_writer import _pgvector_typed_extras
    from services.vector_sync import vector_dimension_hint

    dim = vector_dimension_hint(opts.get("embedding_model"), opts)
    storage = str(opts.get("vector_storage") or "vector").strip().lower() or "vector"
    columns = []
    for name, ctype in DOCUMENT_COLUMNS:
        if name == "embedding":
            ctype = f"{storage}({dim})" if dim else storage
        columns.append({"name": name, "type": ctype})
    for name, ddl in _pgvector_typed_extras(mappings, dict(column_types or {})):
        columns.append({"name": name, "type": ddl, "source": name})
    excluded = {str(c) for c in (opts.get("exclude_pii_columns") or []) if c}
    only = {str(c) for c in (opts.get("metadata_columns") or []) if c}
    embed_col = str(opts.get("embedding_column") or "").strip()
    carried, declared_out = [], []
    for target, source, ttype in rows:
        if not target:
            continue
        if target in excluded or source in excluded or (only and target not in only and source not in only):
            declared_out.append(target)
        elif embed_col and embed_col in {target, source}:
            declared_out.append(target)
        else:
            carried.append({"source": source, "key": target, "source_type": ttype})
    return {
        "layout": LAYOUT_DOCUMENT,
        "layout_reason": reason,
        "columns": columns,
        "id": "TEXT — the source key verbatim for a one-chunk document; a key-derived hash per chunk otherwise (source_id holds the key)",
        "metadata_fields": carried,
        "declared_not_in_metadata": declared_out,
        "embedded_text_rule": embedded_text_rule(opts),
        "null_policy": "SQL NULL is JSON null in metadata",
    }


def document_options(cfg: Mapping[str, Any] | None) -> dict[str, Any]:
    """Vector options from a resolved cfg (flat run extras over saved extras)."""
    cfg = cfg or {}
    nested = cfg.get("extra") if isinstance(cfg.get("extra"), Mapping) else {}
    keys = (
        "content_column",
        "text_template",
        "embedding_column",
        "embedding_model",
        "embedding_dimensions",
        "metadata_columns",
        "exclude_pii_columns",
        "vector_storage",
    )
    out: dict[str, Any] = {}
    for key in keys:
        if key in cfg and cfg.get(key) is not None:
            out[key] = cfg[key]
        elif key in nested and nested.get(key) is not None:
            out[key] = nested[key]
    return out


# --------------------------------------------------------------------------- #
# Plan_transfer hooks (kept here so the plan module only calls them)
# --------------------------------------------------------------------------- #


def plan_layout(
    dst_conn: Mapping[str, Any] | None,
    dst_info: Mapping[str, Any] | None,
    table: str,
    dest_exists: Any,
    *,
    requested: str = "",
) -> tuple[str, str] | None:
    """``(layout, reason)`` for a planned pgvector destination, else ``None``.

    ``requested`` is the run's own ``vector_layout``; it beats the connector.
    """
    from src.transfer.connector_capabilities import resolve_driver_type

    conn = dict(dst_conn or {})
    if resolve_driver_type(str(conn.get("type") or conn.get("db_type") or "")) != "pgvector":
        if str(requested or "").strip():
            raise ValueError("vector_layout applies to pgvector destinations only.")
        return None
    if str(requested or "").strip():
        chosen = normalize_layout(requested)
        if not chosen:
            raise ValueError(
                f"vector_layout {requested!r} is not one of 'table' or 'document'."
            )
        conn["vector_layout"] = chosen
    existing = None
    if dest_exists is True:
        existing = [
            (c.get("name") if isinstance(c, Mapping) else c)
            for c in ((dst_info or {}).get("columns") or [])
        ]
    return decide_layout(conn, existing_columns=existing, table=table)


def plan_destination_contract(
    decided: tuple[str, str] | None,
    dst_conn: Mapping[str, Any] | None,
    mappings: list[dict[str, Any]],
    column_types: Mapping[str, str] | None,
) -> dict[str, Any] | None:
    if not decided:
        return None
    layout, reason = decided
    return destination_contract(
        layout,
        reason,
        mappings=mappings,
        column_types=column_types,
        options=document_options(dst_conn),
    )


def attach_contract_to_preflight(
    preflight: dict[str, Any], contract: Mapping[str, Any]
) -> dict[str, Any]:
    """Preflight states the shape it validated, in words and as data."""
    out = dict(preflight or {})
    cols = ", ".join(f"{c['name']} {c['type']}" for c in contract.get("columns") or [])
    if contract.get("layout") == LAYOUT_DOCUMENT:
        rule = contract.get("embedded_text_rule") or {}
        rule_text = {
            "column": f"column {rule.get('column')}",
            "template": "the declared template",
            "embedding_column": f"precomputed vectors in {rule.get('column')}",
        }.get(str(rule.get("kind")), "the whole row as JSON")
        message = (
            f"pgvector vector-document table ({contract.get('layout_reason')}): {cols}. "
            f"Every mapped value is carried in metadata (NULL as JSON null); "
            f"embedded text comes from {rule_text} for every row."
        )
    else:
        message = f"pgvector typed table ({contract.get('layout_reason')}): {cols}."
    out["warnings"] = [
        *list(out.get("warnings") or []),
        {"id": "pgvector_destination_layout", "message": message},
    ]
    out["destination_contract"] = dict(contract)
    return out


# --------------------------------------------------------------------------- #
# Preflight / Execute view of an existing document table
# --------------------------------------------------------------------------- #

_DOC_TABLE_NOTE = (
    "Existing pgvector vector-document table: mapped fields are metadata "
    "members, not physical columns — validated as members the writer adds."
)
_CLEARED = {
    "column_types": {},
    "column_nullability": {},
    "column_defaults": {},
    "identity_columns": [],
    "generated_columns": [],
    "primary_key_columns": [],
    "pk_columns": [],
    "unique_keys": [],
    "foreign_keys": [],
    "check_constraints": [],
}


def document_table_preflight_view(probe: dict[str, Any]) -> dict[str, Any]:
    """Validate's destination probe of a document table, seen as the Map sees it.

    The physical id/content/embedding/metadata columns are not the Map's
    targets: every mapped field is a metadata member. Gating the run against
    them refused run 2 into a table DataFlow itself created (D209).
    """
    if str(probe.get("db_type") or "").lower() != "pgvector" or probe.get("table_exists") is not True:
        return probe
    names = list((probe.get("column_types") or {}).keys()) or [
        (c.get("name") if isinstance(c, Mapping) else c) for c in probe.get("columns") or []
    ]
    if not is_document_shaped(names):
        return probe
    return {
        **probe,
        **_CLEARED,
        "table_exists": False,
        "vector_document_table": True,
        "message": _DOC_TABLE_NOTE,
    }


def document_table_engine_view(destination: Any, info: dict[str, Any]) -> dict[str, Any]:
    """Execute's destination probe of a document table (same rule as Validate)."""
    fmt = str(getattr(destination, "format", "") or "").strip().lower()
    if fmt != "pgvector" or info.get("table_exists") is False:
        return info
    if not is_document_shaped((info.get("schema") or {}).keys()):
        return info
    return {
        **info,
        "schema": {},
        "schema_nullability": {},
        "table_exists": False,
        "primary_key_columns": [],
        "unique_keys": [],
        "foreign_keys": [],
        "message": _DOC_TABLE_NOTE,
    }
