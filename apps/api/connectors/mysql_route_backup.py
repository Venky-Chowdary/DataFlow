"""Pre-run row copy of a MySQL table for a strict keyed write (data audit DATA-5).

MySQL upsert and mirror commit each sub-batch, so an FK or lock-wait failure
at row 2,500 of 3,000 left 2,048 rows rewritten as v2 while the run failed;
PostgreSQL already restored from a ``__dfbak`` row copy (D111). This is the
MySQL twin: ``CREATE TABLE … LIKE`` + ``INSERT … SELECT`` before the write,
and on failure one transaction that deletes the live rows and copies the
pre-run rows back (foreign-key checks off for that transaction only, so the
delete neither cascades to child tables nor trips a reference), verified by
count and ``CHECKSUM TABLE`` before the copy is dropped.

A table with triggers is not backed up: replaying the rows would fire them
again and the "restored" rows would differ (the PostgreSQL path suspends
triggers; MySQL cannot), so that run keeps the honest "left in place" report.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

ENGINE = "mysql"


def mysql_route_backup_name(table: str, job_id: str) -> str:
    suffix = f"__dfbak_{str(job_id or '')[:8]}"
    return f"{str(table or 't')[: 64 - len(suffix)]}{suffix}"


def _q(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier

    return quote_sql_identifier(str(name), "`")


def create_mysql_table_backup(cfg: dict[str, Any], *, table: str, job_id: str) -> dict[str, str] | None:
    from connectors.table_manager import _mysql_ddl_connection, _mysql_stored_columns
    from services.dfbak_sweeper import backup_comment

    if not table:
        return None
    backup = mysql_route_backup_name(table, job_id)
    conn = _mysql_ddl_connection(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.TRIGGERS "
                "WHERE EVENT_OBJECT_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = %s",
                (table,),
            )
            if int((cur.fetchone() or [0])[0] or 0):
                logger.warning("MySQL strict-run backup of %s skipped: the table has triggers", table)
                return None
            cols = ", ".join(_q(c) for c in _mysql_stored_columns(cur, table))
            if not cols:
                return None
            cur.execute(f"DROP TABLE IF EXISTS {_q(backup)}")
            cur.execute(f"CREATE TABLE {_q(backup)} LIKE {_q(table)}")
            cur.execute(f"INSERT INTO {_q(backup)} ({cols}) SELECT {cols} FROM {_q(table)}")  # nosec B608
            comment = backup_comment(str(job_id or "")).replace("'", "")
            cur.execute(f"ALTER TABLE {_q(backup)} COMMENT = '{comment}'")
    finally:
        conn.close()
    return {"table": table, "backup": backup, "schema": "", "engine": ENGINE}


def restore_mysql_route_backups(cfg: dict[str, Any], backups: dict[str, dict[str, str]]) -> list[str]:
    from connectors.table_manager import _mysql_ddl_connection, _mysql_stored_columns

    restored: list[str] = []
    conn = _mysql_ddl_connection(cfg)
    try:
        for route, pair in (backups or {}).items():
            table, backup = str(pair.get("table") or ""), str(pair.get("backup") or "")
            if not table or not backup:
                continue
            try:
                with conn.cursor() as cur:
                    cols = ", ".join(_q(c) for c in _mysql_stored_columns(cur, table))
                    conn.begin()
                    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                    cur.execute(f"DELETE FROM {_q(table)}")  # nosec B608
                    cur.execute(f"INSERT INTO {_q(table)} ({cols}) SELECT {cols} FROM {_q(backup)}")  # nosec B608
                    cur.execute(f"SELECT (SELECT COUNT(*) FROM {_q(table)}), (SELECT COUNT(*) FROM {_q(backup)})")  # nosec B608
                    live_n, bak_n = cur.fetchone()
                    if int(live_n) != int(bak_n):
                        raise RuntimeError(f"restore count {live_n} != backup {bak_n}")
                    conn.commit()
                    cur.execute("SET FOREIGN_KEY_CHECKS = 1")
                    cur.execute(f"CHECKSUM TABLE {_q(table)}, {_q(backup)}")
                    sums = [row[1] for row in cur.fetchall()]
                    if len(sums) == 2 and sums[0] != sums[1]:
                        logger.warning("MySQL restore of %s: checksum differs from %s; backup kept", table, backup)
                        continue
                    cur.execute(f"DROP TABLE IF EXISTS {_q(backup)}")
                restored.append(str(route))
            except Exception as exc:  # noqa: BLE001 - report, keep the backup and the others
                try:
                    conn.rollback()
                    with conn.cursor() as cur:
                        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
                except Exception:  # noqa: BLE001 - the connection may be gone
                    logger.debug("MySQL restore cleanup failed", exc_info=True)
                logger.warning("MySQL route backup restore of %s failed: %s", table, exc)
    finally:
        conn.close()
    return restored


def discard_mysql_route_backups(cfg: dict[str, Any], backups: dict[str, dict[str, str]]) -> None:
    from connectors.table_manager import _mysql_ddl_connection

    conn = _mysql_ddl_connection(cfg)
    try:
        with conn.cursor() as cur:
            for pair in (backups or {}).values():
                if pair.get("backup"):
                    cur.execute(f"DROP TABLE IF EXISTS {_q(str(pair['backup']))}")
    finally:
        conn.close()
