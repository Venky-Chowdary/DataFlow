"""Source-catalog reads Validate needs before a single row is written.

Split out of ``services.preflight_service``: reading a live source catalog is a
connector concern with its own failure modes (unreachable host, a role without
catalog rights, an engine with no probe), and preflight should consume the
answer rather than own the connection.
"""

from __future__ import annotations

import logging
from typing import Any

from services.foreign_key_metadata import SUPPORTED_DIALECTS
from services.foreign_key_orchestration import measure_source_foreign_keys

logger = logging.getLogger(__name__)


def _resolve_source(
    *,
    source_connector_id: str,
    source_config: dict[str, Any] | None,
    workspace_id: str | None,
) -> tuple[dict[str, Any] | None, str]:
    """The connector config and engine key behind a Validate request."""
    cfg: dict[str, Any] | None = None
    db_type = ""
    if source_connector_id:
        from services.connector_probe import probe_cfg_from_saved
        from services.connector_store import get_connector

        conn = get_connector(source_connector_id, workspace_id=workspace_id)
        if conn:
            cfg = probe_cfg_from_saved(conn)
            db_type = (conn.type or "").lower()
    if cfg is None and source_config:
        cfg = dict(source_config)
        db_type = (
            cfg.get("type") or cfg.get("db_type") or cfg.get("format") or ""
        ).lower()
    if not cfg or not db_type:
        return None, ""
    cfg = dict(cfg)
    cfg.setdefault("type", db_type)
    return cfg, db_type


def load_source_foreign_keys(
    *,
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    source_table: str = "",
    workspace_id: str | None = None,
) -> list[dict[str, Any]]:
    """The source table's foreign keys, as the catalog holds them.

    Reads through the canonical probe rather than the introspect payload, so
    SQL Server, Oracle and SQLite sources are measured too: the introspect path
    only carried keys for the PostgreSQL and MySQL families, and Validate's
    orphan probe therefore fell back to the *destination's* keys — silently
    checking a different set of relationships than the source enforces.

    Returns ``[]`` when the engine has no probe or the catalog cannot be read.
    Never invents a relationship.
    """
    table = (source_table or "").strip()
    if not table:
        return []
    if not source_connector_id and not source_config:
        return []
    cfg, db_type = _resolve_source(
        source_connector_id=source_connector_id,
        source_config=source_config,
        workspace_id=workspace_id,
    )
    if not cfg or db_type not in SUPPORTED_DIALECTS:
        return []
    measured = measure_source_foreign_keys(db_type, cfg, [table]).get(table)
    if measured is None or not measured.measured:
        if measured is not None and measured.detail:
            logger.debug("source FK catalog unreadable: %s", measured.detail)
        return []
    return [
        {
            "name": fk.name,
            "columns": list(fk.columns),
            "referenced_schema": fk.referenced_schema,
            "referenced_table": fk.referenced_table,
            "referenced_columns": list(fk.referenced_columns),
            "on_delete": fk.on_delete,
            "on_update": fk.on_update,
        }
        for fk in measured.items
    ]
