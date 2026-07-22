"""Canonical saved-connector probe config — shared by Connectors Test and Validate.

Transfer Studio must never re-assemble host/port/password from empty form fields
when a ``connector_id`` is selected. Connectors → Test and Validate → G2 must
call the same ``run_probe(type, cfg)`` with the same decrypted secrets.
"""

from __future__ import annotations

from typing import Any


def probe_cfg_from_saved(conn: Any) -> dict[str, Any]:
    """Build the exact probe kwargs used by ``POST /connectors/saved/{id}/test``.

    Accepts a ``SavedConnector`` dataclass or a plain dict from
    ``_lookup_saved_connector``.
    """
    if isinstance(conn, dict):
        get = conn.get
    else:
        def get(key: str, default: Any = "") -> Any:
            return getattr(conn, key, default)

    return {
        "host": get("host") or "",
        "port": int(get("port") or 0),
        "database": get("database") or "",
        "username": get("username") or "",
        "password": get("password") or "",
        "schema": get("schema") or "",
        "connection_string": get("connection_string") or "",
        "warehouse": get("warehouse") or "",
        "ssl": bool(get("ssl")),
        "auth_mode": get("auth_mode") or "",
        "auth_role": get("auth_role") or "",
        "role": get("auth_role") or get("role") or "",
        "api_key": get("api_key") or "",
        "service_account": get("service_account") or "",
        "private_key": get("private_key") or "",
        "endpoint_url": get("endpoint_url") or "",
        "path_style": bool(get("path_style")),
        "auth_source": get("auth_source") or "",
        "type": get("type") or "",
    }


def probe_saved_connector(
    connector_id: str,
    *,
    workspace_id: str | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    """Live connectivity probe identical to Connectors Test for a saved id.

    Returns ``(ok, message, cfg)``. On missing connector, ``ok`` is False and
    ``cfg`` is empty.
    """
    from services.connector_store import get_connector

    conn = get_connector(connector_id, workspace_id=workspace_id)
    if not conn:
        return False, f"Connector '{connector_id}' not found", {}

    password = conn.password or ""
    conn_str = conn.connection_string or ""
    if "[encrypted-secret-unavailable]" in password or "[decryption-failed]" in password:
        return (
            False,
            (
                "Saved credentials cannot be decrypted. Re-enter the password or "
                "connection string on the Connectors page, then Test again."
            ),
            {},
        )
    if "[encrypted-secret-unavailable]" in conn_str or "[decryption-failed]" in conn_str:
        return (
            False,
            (
                "Saved connection string cannot be decrypted. Re-enter it on the "
                "Connectors page, then Test again."
            ),
            {},
        )

    from src.transfer.connector_registry import run_probe

    cfg = probe_cfg_from_saved(conn)
    try:
        ok, message = run_probe(conn.type or "", cfg)
    except Exception as exc:
        return False, f"Connection failed: {exc}", cfg
    return ok, message, cfg


def endpoint_from_saved_connector(
    connector_id: str,
    *,
    table: str = "",
    collection: str = "",
    schema: str = "",
    database: str = "",
    workspace_id: str | None = None,
):
    """Build an EndpointConfig from the saved connector (credentials never empty)."""
    from services.connector_store import get_connector
    from src.transfer.models import EndpointConfig

    conn = get_connector(connector_id, workspace_id=workspace_id)
    if not conn:
        return None
    cfg = probe_cfg_from_saved(conn)
    from services.dialect_profiles import normalize_schema

    db_type = (conn.type or "").lower()
    return EndpointConfig(
        kind="database",
        format=db_type,
        connector_id=connector_id,
        host=cfg["host"],
        port=int(cfg["port"] or 0),
        database=database or cfg["database"] or "",
        schema=normalize_schema(
            db_type,
            schema or cfg.get("schema") or "",
            username=str(cfg.get("username") or "") or None,
        ) or "",
        table=table or "",
        collection=collection or table or "",
        username=cfg["username"],
        password=cfg["password"],
        connection_string=cfg["connection_string"],
        warehouse=cfg.get("warehouse") or "",
        ssl=bool(cfg.get("ssl")),
        auth_source=cfg.get("auth_source") or "",
        auth_mode=cfg.get("auth_mode") or "",
        auth_role=cfg.get("auth_role") or "",
        api_key=cfg.get("api_key") or "",
        service_account=cfg.get("service_account") or "",
        private_key=cfg.get("private_key") or "",
        endpoint_url=cfg.get("endpoint_url") or "",
        path_style=bool(cfg.get("path_style")),
    )