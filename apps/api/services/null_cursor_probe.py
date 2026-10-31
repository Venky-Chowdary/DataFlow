"""Probe how many source rows carry a NULL incremental cursor value.

An incremental read bounds the delta with ``WHERE cursor > :watermark`` — and
``NULL > x`` is never true, so every NULL-cursor row is silently absent from
the delta forever (D99). Validate measures the count up front so the gate can
warn (or block under strict validation) and state exactly which rows only ever
arrive through a full refresh.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from services.source_duplicate_probe import is_sqlish_source


@dataclass
class NullCursorProbeResult:
    status: str = "skipped_no_source"
    ran: bool = False
    null_count: int | None = None
    message: str = ""
    cursor_column: str = ""
    db_type: str = ""


def _resolve_source_cfg(
    source_connector_id: str,
    source_config: dict[str, Any] | None,
    workspace_id: str | None,
) -> tuple[dict[str, Any] | None, str]:
    cfg: dict[str, Any] | None = None
    db_type = ""
    if source_connector_id:
        from services.connector_store import get_connector

        conn = get_connector(source_connector_id, workspace_id=workspace_id)
        if conn:
            from services.connector_probe import probe_cfg_from_saved

            cfg = probe_cfg_from_saved(conn)
            db_type = (conn.type or "").lower()
    if cfg is None and source_config:
        cfg = dict(source_config)
        db_type = (
            cfg.get("type") or cfg.get("db_type") or cfg.get("format") or ""
        ).lower()
    if cfg and db_type:
        cfg = dict(cfg)
        cfg.setdefault("type", db_type)
        nested_extra = cfg.get("extra")
        if isinstance(nested_extra, dict) and nested_extra:
            for extra_key, extra_value in nested_extra.items():
                cfg.setdefault(extra_key, extra_value)
    return cfg, db_type


def _sql_null_count(
    cfg: dict[str, Any], table: str, cursor_column: str
) -> int:
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table
    from services.sql_object_identity import resolve_object_identity

    engine = _engine(cfg)
    schema, table = split_qualified_table(
        table, (cfg.get("schema") or "").strip() or None
    )
    ident = resolve_object_identity(
        engine, table, schema, columns=[cursor_column]
    )
    if not ident.exists:
        raise ValueError(
            f"source object '{table}' not found in catalog — NULL-cursor "
            "count is unmeasured, not zero"
        )
    if cursor_column not in ident.columns:
        raise ValueError(
            f"cursor column '{cursor_column}' not found on '{table}' — "
            "NULL-cursor count is unmeasured, not zero (SQLite-family engines "
            "treat an unknown quoted identifier as a string literal and would "
            "report a false-clean 0)"
        )
    table = sa.sql.quoted_name(ident.table, True)
    schema = sa.sql.quoted_name(ident.schema, True) if ident.schema else None
    cursor_column = ident.columns[cursor_column]

    tbl = sa.table(table, schema=schema)
    col = sa.column(sa.sql.quoted_name(str(cursor_column), True))
    stmt = sa.select(sa.func.count()).select_from(tbl).where(col.is_(None))
    with engine.connect() as conn:
        return int(conn.execute(stmt).scalar() or 0)


def _mongo_null_count(
    cfg: dict[str, Any], collection: str, cursor_column: str
) -> int:
    from services.mongodb_service import get_mongodb_service

    svc = get_mongodb_service()
    client = svc.client_from_config(cfg)
    db_name = str(cfg.get("database") or "")
    coll = client[db_name][collection]
    # Mongo "null" matches both missing and explicit-null fields — the same
    # set an incremental bound over this field can never see.
    return int(coll.count_documents({cursor_column: None}))


def stamp_skipped_null_cursor_rows(
    dest_summary: dict[str, Any],
    null_count: int | None,
    cursor_column: str,
) -> None:
    """Report NULL-cursor rows the incremental delta skipped on the job.

    ``dest_summary`` is the record the job result, REST, and MCP surfaces read
    — a run that skipped rows must say so instead of reporting verified (D99).
    """
    if null_count is None:
        return
    dest_summary["skipped_null_cursor_rows"] = int(null_count)
    if null_count:
        dest_summary["null_cursor_note"] = (
            f"{int(null_count)} source row(s) carry a NULL value in the "
            f"incremental cursor column '{cursor_column}' — they are absent "
            "from every incremental delta and only arrive via a full refresh."
        )


def probe_null_cursor_count(
    *,
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    source_table: str = "",
    source_collection: str = "",
    cursor_column: str = "",
    workspace_id: str | None = None,
) -> NullCursorProbeResult:
    """Count source rows whose incremental cursor column is NULL."""
    col = (cursor_column or "").strip()
    if not col:
        return NullCursorProbeResult(
            status="skipped_no_cursor",
            message="No incremental cursor column resolved",
        )
    if not source_table and not source_collection:
        return NullCursorProbeResult(
            status="skipped_no_source",
            message="No source table/collection for the NULL-cursor probe",
            cursor_column=col,
        )
    if not source_connector_id and not source_config:
        return NullCursorProbeResult(
            status="skipped_no_source",
            message="No source connector id or inline config for the NULL-cursor probe",
            cursor_column=col,
        )
    try:
        cfg, db_type = _resolve_source_cfg(
            source_connector_id, source_config, workspace_id
        )
        if not cfg:
            return NullCursorProbeResult(
                status="skipped_no_source",
                message="Source connector could not be loaded for the NULL-cursor probe",
                cursor_column=col,
                db_type=db_type,
            )
        if is_sqlish_source(db_type):
            n = _sql_null_count(cfg, source_table, col)
            return NullCursorProbeResult(
                status="ran",
                ran=True,
                null_count=n,
                message=f"NULL-cursor probe on {source_table}.({col}): {n} row(s)",
                cursor_column=col,
                db_type=db_type,
            )
        if db_type in ("mongodb", "mongodb_atlas"):
            coll = source_collection or source_table
            n = _mongo_null_count(cfg, coll, col)
            return NullCursorProbeResult(
                status="ran",
                ran=True,
                null_count=n,
                message=f"NULL-cursor probe on {coll}.({col}): {n} document(s)",
                cursor_column=col,
                db_type=db_type,
            )
        return NullCursorProbeResult(
            status="skipped_unsupported",
            message=(
                f"NULL-cursor probe not supported for source type '{db_type}' — "
                "NULL-cursor rows cannot be counted"
            ),
            cursor_column=col,
            db_type=db_type,
        )
    except Exception as exc:  # noqa: BLE001 — a failed probe is unmeasured
        return NullCursorProbeResult(
            status="error",
            message=f"NULL-cursor probe failed: {exc}"[:400],
            cursor_column=col,
        )
