"""One save check for every path that creates or changes a saved connector (U1, R11).

REST create / PUT / PATCH / rotate-secrets, the legacy ``POST /connectors/``
and Pilot/MCP Confirm (create and update) all call :func:`connector_save_refusal`
before they write. It runs, in order:

1. the egress guard (``egress_guard.connector_save_refusal``): metadata and
   link-local hosts, the platform's own credentials (AWS default chain, Google
   ADC, MongoDB AWS auth), host files and sockets (``read_default_file``,
   ``local_infile``, libpq ``service``, ODBC ``DSN`` / driver libraries, TLS
   files) — 400, the existing API contract;
2. an SFTP URL host whose port disagrees with the port field (D208) — 400;
3. the store's own refusal (``connector_store.connector_config_refusal``):
   a server-side file read and SQLite on the hosted cloud (D276) — 422;
4. a SQLite connector with no path, ``:memory:`` or a URI filename (D425) — 422.

Before this, Pilot Confirm wrote through ``create_connector`` with none of 1,
2 or 4, so a staged ``?read_default_file=`` or ``169.254.169.254`` was saved.

This module is never reloaded by tests, so :class:`ConnectorRefused` stays one
class for the app's exception handler.
"""

from __future__ import annotations

from typing import Any


class ConnectorRefused(ValueError):
    """The connector configuration may not be saved; nothing was saved.

    ``status_code`` is 400 for the egress guard's refusals (the API contract
    those routes always had) and 422 for the store's and SQLite's.
    """

    status_code = 422

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        if status_code is not None:
            self.status_code = int(status_code)


def _field(data: dict[str, Any], existing: Any, name: str) -> Any:
    value = data.get(name)
    if value in (None, "") and existing is not None:
        value = getattr(existing, name, None)
    return value


def egress_refusal(data: dict[str, Any], existing: Any = None) -> ConnectorRefused | None:
    """Steps 1 and 2: the egress guard and the SFTP URL-port check (400)."""
    from connectors.sftp_common import sftp_edit_port_error
    from services.egress_guard import connector_save_refusal as egress_save_refusal

    refusal = egress_save_refusal(data, existing)
    if refusal:
        return ConnectorRefused(refusal, 400)
    conflict = sftp_edit_port_error(
        data.get("type") or getattr(existing, "type", ""),
        data.get("host"),
        data.get("port"),
        getattr(existing, "port", None) if existing is not None else None,
    )
    if conflict:
        return ConnectorRefused(conflict, 400)
    return None


def connector_save_refusal(data: dict[str, Any], existing: Any = None) -> ConnectorRefused | None:
    """Why ``data`` (merged over ``existing`` for an edit) may not be saved, or ``None``."""
    refused = egress_refusal(data, existing)
    if refused is not None:
        return refused
    from services.connector_store import connector_config_refusal
    from services.local_file_policy import sqlite_connector_field_error

    store = connector_config_refusal(data, existing)
    if store:
        return ConnectorRefused(store, 422)
    sqlite_error = sqlite_connector_field_error(
        str(_field(data, existing, "type") or ""),
        str(data.get("database") or ""),
        str(_field(data, existing, "connection_string") or ""),
    )
    if sqlite_error:
        return ConnectorRefused(sqlite_error, 422)
    return None


def check_connector_save(data: dict[str, Any], existing: Any = None) -> None:
    """Raise :class:`ConnectorRefused` when ``data`` may not be saved."""
    refused = connector_save_refusal(data, existing)
    if refused is not None:
        raise refused


def as_http(refused: ConnectorRefused) -> Any:
    """The route's ``HTTPException`` for a refusal: its status, its plain text."""
    from fastapi import HTTPException

    return HTTPException(status_code=refused.status_code, detail=str(refused))
