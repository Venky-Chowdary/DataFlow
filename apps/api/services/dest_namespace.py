"""Whether a destination's configured schema exists before a plan proposes CREATE (D431).

An Oracle connector whose schema (owner) does not exist tests green — the login
works — and its catalog read of ``NOSUCHSCHEMA.T`` answers "no such table". The
plan then treated the table as create-new, said ``safe_to_start=true`` with
create-new mappings, and the run failed with a raw ``ORA-01918: user ... does
not exist``. SQL Server is the same with a missing schema. PostgreSQL is not:
its writer runs ``CREATE SCHEMA IF NOT EXISTS``.

A create-new decision needs a successful read of the destination catalog, so
the plan asks the catalog whether the namespace exists before it proposes one.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

logger = logging.getLogger(__name__)

#: Engines whose writers cannot create a missing namespace: the SQL asked of each.
_NAMESPACE_SQL = {
    "oracle": "SELECT 1 FROM all_users WHERE username = :name",
    "sqlserver": "SELECT 1 FROM sys.schemas WHERE name = :name",
}


def _configured_namespace(db_type: str, cfg: Mapping[str, Any]) -> str:
    from services.dialect_profiles import normalize_schema

    raw = str(cfg.get("schema") or "").strip()
    if not raw:
        return ""  # the login's own default schema, which exists if the login does
    schema = normalize_schema(db_type, raw, username=str(cfg.get("username") or "")) or ""
    if db_type == "oracle":
        schema = schema if schema.startswith('"') else schema.upper()
        if schema == str(cfg.get("username") or "").strip().upper():
            return ""
    if db_type == "sqlserver" and schema.lower() == "dbo":
        return ""
    return schema.strip('"')


#: The namespace probe's own budget: one catalog row, never the run's deadline.
NAMESPACE_PROBE_SECONDS = 10.0
MISSING_NAMESPACE_GATE_ID = "g_dest_schema_missing"


def _ask_namespace(cfg: Mapping[str, Any], sql: str, name: str) -> bool:
    import sqlalchemy as sa

    from connectors.generic_sql import _engine

    with _engine(dict(cfg)).connect() as conn:
        return conn.execute(sa.text(sql), {"name": name}).first() is not None


def missing_destination_namespace(cfg: Mapping[str, Any] | None) -> str:
    """The configured schema the destination server does not have, or ``""``.

    ``""`` also when the question cannot be asked (the catalog read fails or
    does not answer within :data:`NAMESPACE_PROBE_SECONDS`): an unconfirmed
    namespace is never "missing", so an existing schema is never blocked; an
    unreachable destination is the destination gate's finding, not this one.
    One probe per namespace per read-only plan (the plan's catalog scope).
    """
    if not isinstance(cfg, Mapping):
        return ""
    from src.transfer.connector_capabilities import resolve_driver_type

    db_type = resolve_driver_type(str(cfg.get("type") or cfg.get("format") or ""))
    sql = _NAMESPACE_SQL.get(db_type)
    if not sql:
        return ""
    name = _configured_namespace(db_type, cfg)
    if not name:
        return ""
    from services.introspect_memo import memoized_plan_read

    key = ("dest_namespace", db_type, str(cfg.get("host") or ""), str(cfg.get("port") or ""),
           str(cfg.get("database") or ""), name)
    found = memoized_plan_read(key, lambda: _probe_namespace(cfg, sql, name))
    return name if found is False else ""


def _probe_namespace(cfg: Mapping[str, Any], sql: str, name: str) -> bool | None:
    """True / False from the catalog; ``None`` when it did not answer in budget."""
    from src.ai.copilot.tool_deadline import bounded_step, deadline_scope

    with deadline_scope(NAMESPACE_PROBE_SECONDS):
        step = bounded_step(
            f"checking that destination schema {name} exists",
            lambda: _ask_namespace(cfg, sql, name),
            seconds=NAMESPACE_PROBE_SECONDS,
        )
    if step.timed_out or step.error is not None:
        if step.error is not None:
            logger.info("destination namespace probe unavailable: %s", type(step.error).__name__)
        return None
    return bool(step.value)


def missing_namespace_refusal(connector_label: str, schema: str) -> str:
    """The refusal text every surface shows: the schema by name, and the remedies."""
    return (
        f"Destination schema {schema} does not exist on {connector_label}, so the "
        "table cannot be created there; nothing was created or written. Create the "
        "schema (on Oracle, the user) or point the connector at an existing schema, "
        "then plan the transfer again."
    )


def namespace_refusal(cfg: Mapping[str, Any] | None, label: str = "") -> str:
    """:func:`missing_namespace_refusal` for this destination config, or ``""``."""
    missing = missing_destination_namespace(cfg)
    if not missing:
        return ""
    return missing_namespace_refusal(_label(cfg, label), missing)


def _label(cfg: Mapping[str, Any] | None, label: str = "") -> str:
    return label or str((cfg or {}).get("host") or "") or "the destination"


def dest_namespace_gate(
    cfg: Mapping[str, Any] | None, table_exists: bool | None = None
) -> dict[str, Any] | None:
    """Preflight gate (Studio Validate, plan, Execute's preflight) for a missing schema.

    A table that was read proves its schema exists: no probe then.
    """
    if table_exists is True:
        return None
    missing = missing_destination_namespace(cfg)
    if not missing:
        return None
    return {
        "id": MISSING_NAMESPACE_GATE_ID,
        "status": "block",
        "message": missing_namespace_refusal(_label(cfg), missing),
        "duration_ms": 0,
        "details": {"kind": "destination_schema_missing", "schema": missing},
    }
