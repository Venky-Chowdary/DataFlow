"""Server settings continuous CDC needs, applied with the saved connector login.

The statements are fixed. A connector name never becomes SQL, and the Postgres
restart command is ``pg_ctl`` with a data directory that contains only a plain
path. ``wal_level`` is read at postmaster start, so the restart is part of the
change. A role that is not superuser (or a MySQL user without the grant) is
reported; nothing is invented in its place.
"""

from __future__ import annotations

import re
from typing import Any

_PG_ALTER = (
    "ALTER SYSTEM SET wal_level = 'logical'",
    "ALTER SYSTEM SET max_replication_slots = '10'",
    "ALTER SYSTEM SET max_wal_senders = '10'",
)
_PGDATA = re.compile(r"^/[A-Za-z0-9_./-]{1,240}$")
_ACCOUNT = re.compile(r"^([A-Za-z0-9_]{1,64})@([%A-Za-z0-9._-]{1,255})$")
_RESTART_DISPATCHED = (
    "terminating connection",
    "server closed the connection",
    "connection already closed",
    "could not connect to server",
    "the database system is shutting down",
)


def _cell(row: Any) -> str:
    if row is None:
        return ""
    if isinstance(row, (list, tuple)):
        return str(row[0] if row else "")
    return str(row)


def _pgdata(raw: str) -> str:
    path = (raw or "").strip()
    if not _PGDATA.match(path) or ".." in path:
        raise ValueError("refusing to restart: data_directory is not a plain path")
    return path


def apply_postgres_logical_decoding(conn: Any, *, restart: bool) -> dict[str, Any]:
    """Set logical decoding on a superuser session. Restart when asked."""
    if hasattr(conn, "autocommit"):
        conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("SHOW is_superuser")
        superuser = _cell(cur.fetchone()).strip().lower() in {"on", "true", "t", "1"}
        cur.execute("SHOW wal_level")
        before = _cell(cur.fetchone()).strip()
        cur.execute("SHOW data_directory")
        data_directory = _cell(cur.fetchone()).strip()
        if not superuser:
            return {
                "engine": "postgresql",
                "applied": False,
                "wal_level_before": before,
                "restarted": False,
                "error": (
                    "The saved PostgreSQL role is not a superuser, so wal_level "
                    "was left unchanged. A superuser must set wal_level=logical "
                    "and restart PostgreSQL."
                ),
            }
        for statement in _PG_ALTER:
            cur.execute(statement)
        restarted = False
        restart_error = ""
        if restart:
            pgdata = _pgdata(data_directory)
            # The path was checked. It has no spaces or quotes, so it cannot
            # change the program pg_ctl runs. If pg_ctl cannot recycle the
            # postmaster from inside the session, signal PID 1. That stops a
            # container whose init is Postgres; a host login that does not own
            # PID 1 gets "operation not permitted" and the staged setting stays
            # for the next real restart.
            commands = (
                f"pg_ctl restart -D {pgdata} -m fast -t 90 -w",
                "kill -TERM 1",
            )
            for command in commands:
                try:
                    cur.execute(f"COPY (SELECT 1) TO PROGRAM '{command}'")
                    restarted = True
                    restart_error = ""
                    break
                except Exception as exc:  # noqa: BLE001 — a restart drops this session
                    restart_error = str(exc)[:400]
                    low = restart_error.lower()
                    if any(mark in low for mark in _RESTART_DISPATCHED):
                        restarted = True
                        break
        return {
            "engine": "postgresql",
            "applied": True,
            "wal_level_before": before,
            "wal_level_staged": "logical",
            "max_replication_slots": "10",
            "max_wal_senders": "10",
            "data_directory": data_directory,
            "restarted": restarted,
            "restart_error": restart_error,
            "note": (
                "wal_level is read at server start. After the restart, SHOW "
                "wal_level must read logical before a slot can be created."
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
                    "An administrator must run the GRANT. gtid_mode was not changed."
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
