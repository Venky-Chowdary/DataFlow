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


def missing_destination_namespace(cfg: Mapping[str, Any] | None) -> str:
    """The configured schema the destination server does not have, or ``""``.

    ``""`` also when the question cannot be asked (the catalog read fails): an
    unreachable destination is the destination gate's finding, not this one.
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
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import _engine

        with _engine(dict(cfg)).connect() as conn:
            found = conn.execute(sa.text(sql), {"name": name}).first()
    except Exception as exc:  # noqa: BLE001 - unasked, never "missing"
        logger.info("destination namespace probe unavailable: %s", type(exc).__name__)
        return ""
    return "" if found else name


def missing_namespace_refusal(connector_label: str, schema: str) -> str:
    """The plan's refusal text: the schema by name, and the remedies."""
    return (
        f"Destination schema {schema} does not exist on {connector_label}, so the "
        "table cannot be created there; nothing was staged. Create the schema (on "
        "Oracle, the user) or point the connector at an existing schema, then plan "
        "the transfer again."
    )
