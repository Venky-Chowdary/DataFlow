"""Server settings continuous CDC needs, applied with the saved connector login.

The statements are fixed. A connector name never becomes SQL. PostgreSQL
reads ``wal_level``, ``max_replication_slots``, and ``max_wal_senders`` only
when the postmaster starts, so Confirm writes ``postgresql.auto.conf`` and
the host restarts the service. This session never signals the postmaster:
``pg_ctl restart`` and ``kill`` from inside the session stop a container
whose init is Postgres and cannot start it again. A role that is not
superuser (or a MySQL user without the grant) is reported; nothing is
invented in its place.
"""

from __future__ import annotations

import re
from typing import Any

_PG_ALTER = (
    "ALTER SYSTEM SET wal_level = 'logical'",
    "ALTER SYSTEM SET max_replication_slots = '10'",
    "ALTER SYSTEM SET max_wal_senders = '10'",
)
_PG_TARGET = {
    "wal_level": "logical",
    "max_replication_slots": "10",
    "max_wal_senders": "10",
}
_SHOW_NAMES = frozenset({"is_superuser", "data_directory", *_PG_TARGET})
_ACCOUNT = re.compile(r"^([A-Za-z0-9_]{1,64})@([%A-Za-z0-9._-]{1,255})$")
_HOST_RESTART = (
    "The settings are stored in postgresql.auto.conf. PostgreSQL reads "
    "wal_level, max_replication_slots, and max_wal_senders when the "
    "postmaster starts. Restart the PostgreSQL service from the host, "
    "then SHOW wal_level must read logical before a replication slot "
    "can be created."
)


def _cell(row: Any) -> str:
    if row is None:
        return ""
    if isinstance(row, (list, tuple)):
        return str(row[0] if row else "")
    return str(row)


def _show(cur: Any, name: str) -> str:
    if name not in _SHOW_NAMES:
        raise ValueError("refusing to read an unknown PostgreSQL setting")
    cur.execute(f"SHOW {name}")
    return _cell(cur.fetchone()).strip()


def apply_postgres_logical_decoding(conn: Any, *, restart: bool) -> dict[str, Any]:
    """Stage logical decoding on a superuser session.

    ``restart`` records that the operator wants the postmaster recycled.
    The session writes ``postgresql.auto.conf`` only. Stopping PID 1 from
    here exits a Docker Postgres and does not start it again.
    """
    if hasattr(conn, "autocommit"):
        conn.autocommit = True
    cur = conn.cursor()
    try:
        superuser = _show(cur, "is_superuser").lower() in {"on", "true", "t", "1"}
        before = {name: _show(cur, name) for name in _PG_TARGET}
        data_directory = _show(cur, "data_directory")
        if not superuser:
            return {
                "engine": "postgresql",
                "applied": False,
                "wal_level_before": before["wal_level"],
                "restarted": False,
                "restart_required": False,
                "error": (
                    "The saved PostgreSQL role is not a superuser, so wal_level "
                    "was left unchanged. A superuser must set wal_level=logical "
                    "and restart PostgreSQL from the host."
                ),
            }
        for statement in _PG_ALTER:
            cur.execute(statement)
        restart_required = any(
            before[name].lower() != target.lower() for name, target in _PG_TARGET.items()
        )
        return {
            "engine": "postgresql",
            "applied": True,
            "wal_level_before": before["wal_level"],
            "wal_level_staged": "logical",
            "max_replication_slots": "10",
            "max_wal_senders": "10",
            "data_directory": data_directory,
            "restarted": False,
            "restart_required": restart_required,
            "operator_requested_restart": bool(restart),
            "note": _HOST_RESTART if restart_required else (
                "The running server already has wal_level=logical with "
                "replication slots and WAL senders at 10. The same values "
                "were written to postgresql.auto.conf."
            ),
        }
    finally:
        close = getattr(cur, "close", None)
        if callable(close):
            close()


def _mysql_account(current_user: str) -> tuple[str, str]:
    match = _ACCOUNT.match((current_user or "").strip())
    if not match:
        raise ValueError("refusing to grant: the MySQL account name is not a plain user@host")
    return match.group(1), match.group(2)


def apply_mysql_replication_client(conn: Any, *, enable_gtid: bool) -> dict[str, Any]:
    """Grant binlog privileges to the logged-in user, then persist GTID when asked.

    The grant is the privilege the saved user does not have. If the server
    refuses it, GTID is left untouched — a half-applied gtid_mode is worse
    than staying OFF.
    """
    if hasattr(conn, "autocommit"):
        conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("SELECT CURRENT_USER()")
        account = _cell(cur.fetchone()).strip()
        user, host = _mysql_account(account)
        grant = (
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* "
            f"TO '{user}'@'{host}'"
        )
        try:
            cur.execute(grant)
            cur.execute("FLUSH PRIVILEGES")
        except Exception as exc:  # noqa: BLE001 — the driver error is the result
            return {
                "engine": "mysql",
                "applied": False,
                "account": f"{user}@{host}",
                "gtid_mode": "",
                "error": str(exc)[:400],
                "note": (
                    "This login cannot grant REPLICATION CLIENT to itself. "
                    "An administrator on this server must run "
                    f"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'{host}'; "
                    "FLUSH PRIVILEGES; "
                    "SET PERSIST enforce_gtid_consistency = ON; "
                    "SET PERSIST gtid_mode = OFF_PERMISSIVE; "
                    "SET PERSIST gtid_mode = ON_PERMISSIVE; "
                    "SET PERSIST gtid_mode = ON. "
                    "gtid_mode was not changed."
                ),
            }
        gtid = ""
        gtid_error = ""
        if enable_gtid:
            steps = (
                "SET PERSIST enforce_gtid_consistency = ON",
                "SET PERSIST gtid_mode = OFF_PERMISSIVE",
                "SET PERSIST gtid_mode = ON_PERMISSIVE",
                "SET PERSIST gtid_mode = ON",
            )
            try:
                for statement in steps:
                    cur.execute(statement)
                cur.execute("SELECT @@GLOBAL.gtid_mode")
                gtid = _cell(cur.fetchone()).strip()
            except Exception as exc:  # noqa: BLE001 — report the step that stopped
                gtid_error = str(exc)[:400]
        return {
            "engine": "mysql",
            "applied": True,
            "account": f"{user}@{host}",
            "grant": "REPLICATION SLAVE, REPLICATION CLIENT",
            "gtid_mode": gtid,
            "gtid_error": gtid_error,
        }
    finally:
        close = getattr(cur, "close", None)
        if callable(close):
            close()


def apply_saved_cdc_prereq(
    connector_id: str,
    *,
    restart: bool = True,
    enable_gtid: bool = True,
) -> dict[str, Any]:
    """Open the saved connector and apply the CDC server prerequisite for its engine."""
    from services.connector_probe import probe_cfg_from_saved
    from services.connector_store import get_connector

    saved = get_connector(connector_id)
    if not saved:
        raise ValueError(f"Connector '{connector_id}' was not found")
    cfg = probe_cfg_from_saved(saved)
    engine = str(cfg.get("type") or getattr(saved, "type", "") or "").strip().lower()
    if engine in {"postgresql", "postgres"}:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl")),
        )
        try:
            return apply_postgres_logical_decoding(conn, restart=restart)
        finally:
            conn.close()
    if engine in {"mysql", "mariadb"}:
        from connectors.mysql_conn import get_connection

        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl")),
            purpose="ddl",
        )
        try:
            result = apply_mysql_replication_client(conn, enable_gtid=enable_gtid and engine == "mysql")
        finally:
            conn.close()
        if engine == "mariadb":
            extra = "MariaDB does not use MySQL gtid_mode."
            result["note"] = f"{result.get('note') or ''} {extra}".strip()
        return result
    raise ValueError(f"CDC server prerequisites are not defined for {engine or 'this engine'}")
